use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex};

use burn::backend::NdArray;
use sqlx::PgPool;
use tokio::sync::Mutex;

use crate::models::tide::config::ModelParameters;
use crate::models::tide::data::{FeatureMappings, Scaler};
use crate::models::tide::model::TideModel;

pub struct ModelState {
    pub model: TideModel<NdArray>,
    pub parameters: ModelParameters,
    pub scaler: Scaler,
    pub mappings: FeatureMappings,
    pub continuous_columns: Vec<String>,
    pub categorical_columns: Vec<String>,
    pub static_categorical_columns: Vec<String>,
    pub artifact_key: String,
    pub load_timestamp: i64,
}

// SAFETY: TideModel<NdArray> is not Sync due to burn's Param<T> using an RwLock
// with a non-Sync FnOnce inside. We guard all access behind a Mutex, so this is safe.
unsafe impl Send for ModelState {}
unsafe impl Sync for ModelState {}

const HISTOGRAM_BUCKETS: [f64; 6] = [1.0, 5.0, 10.0, 30.0, 60.0, 120.0];

pub struct Metrics {
    prediction_requests_total: AtomicU64,
    prediction_errors: StdMutex<std::collections::HashMap<String, u64>>,
    histogram_counts: [AtomicU64; 6],
    histogram_sum: AtomicU64,
    histogram_count: AtomicU64,
    prediction_batch_count: AtomicU64,
    prediction_row_count: AtomicU64,
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}

impl Metrics {
    pub fn new() -> Self {
        Self {
            prediction_requests_total: AtomicU64::new(0),
            prediction_errors: StdMutex::new(std::collections::HashMap::new()),
            histogram_counts: [
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
            ],
            histogram_sum: AtomicU64::new(0),
            histogram_count: AtomicU64::new(0),
            prediction_batch_count: AtomicU64::new(0),
            prediction_row_count: AtomicU64::new(0),
        }
    }

    pub fn increment_requests(&self) {
        self.prediction_requests_total
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn increment_error(&self, stage: &str) {
        let mut errors = self.prediction_errors.lock().unwrap();
        *errors.entry(stage.to_string()).or_insert(0) += 1;
    }

    pub fn observe_duration(&self, seconds: f64) {
        for (index, bucket) in HISTOGRAM_BUCKETS.iter().enumerate() {
            if seconds <= *bucket {
                self.histogram_counts[index].fetch_add(1, Ordering::Relaxed);
            }
        }
        let mut current = self.histogram_sum.load(Ordering::Relaxed);
        loop {
            let current_f64 = f64::from_bits(current);
            let new_f64 = current_f64 + seconds;
            match self.histogram_sum.compare_exchange_weak(
                current,
                new_f64.to_bits(),
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(actual) => current = actual,
            }
        }
        self.histogram_count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn set_batch_count(&self, count: u64) {
        self.prediction_batch_count.store(count, Ordering::Relaxed);
    }

    pub fn set_row_count(&self, count: u64) {
        self.prediction_row_count.store(count, Ordering::Relaxed);
    }

    pub fn render_prometheus(&self, load_timestamp: i64, artifact_key: &str) -> String {
        let mut output = String::new();

        output.push_str(
            "# HELP ensemble_prediction_requests_total Total prediction requests received\n",
        );
        output.push_str("# TYPE ensemble_prediction_requests_total counter\n");
        let requests = self.prediction_requests_total.load(Ordering::Relaxed);
        output.push_str(&format!("ensemble_prediction_requests_total {requests}\n"));

        output.push_str(
            "# HELP ensemble_prediction_errors_total Total prediction requests that failed\n",
        );
        output.push_str("# TYPE ensemble_prediction_errors_total counter\n");
        let errors = self.prediction_errors.lock().unwrap();
        if errors.is_empty() {
            output.push_str("ensemble_prediction_errors_total 0\n");
        } else {
            for (stage, count) in errors.iter() {
                output.push_str(&format!(
                    "ensemble_prediction_errors_total{{stage=\"{stage}\"}} {count}\n"
                ));
            }
        }
        drop(errors);

        output.push_str(
            "# HELP ensemble_prediction_duration_seconds Time to generate predictions end-to-end\n",
        );
        output.push_str("# TYPE ensemble_prediction_duration_seconds histogram\n");
        let total_count = self.histogram_count.load(Ordering::Relaxed);
        #[allow(unused_assignments)]
        let mut cumulative = 0u64;
        for (index, bucket) in HISTOGRAM_BUCKETS.iter().enumerate() {
            cumulative += self.histogram_counts[index].load(Ordering::Relaxed);
            output.push_str(&format!(
                "ensemble_prediction_duration_seconds_bucket{{le=\"{bucket}\"}} {cumulative}\n"
            ));
        }
        output.push_str(&format!(
            "ensemble_prediction_duration_seconds_bucket{{le=\"+Inf\"}} {total_count}\n"
        ));
        let sum = f64::from_bits(self.histogram_sum.load(Ordering::Relaxed));
        output.push_str(&format!("ensemble_prediction_duration_seconds_sum {sum}\n"));
        output.push_str(&format!(
            "ensemble_prediction_duration_seconds_count {total_count}\n"
        ));

        output.push_str(
            "# HELP ensemble_prediction_batch_count Number of batches in last prediction run\n",
        );
        output.push_str("# TYPE ensemble_prediction_batch_count gauge\n");
        let batch_count = self.prediction_batch_count.load(Ordering::Relaxed);
        output.push_str(&format!("ensemble_prediction_batch_count {batch_count}\n"));

        output.push_str(
            "# HELP ensemble_prediction_row_count Number of prediction rows in last run\n",
        );
        output.push_str("# TYPE ensemble_prediction_row_count gauge\n");
        let row_count = self.prediction_row_count.load(Ordering::Relaxed);
        output.push_str(&format!("ensemble_prediction_row_count {row_count}\n"));

        output.push_str(
            "# HELP ensemble_model_load_timestamp Unix timestamp of last successful model load\n",
        );
        output.push_str("# TYPE ensemble_model_load_timestamp gauge\n");
        output.push_str(&format!("ensemble_model_load_timestamp {load_timestamp}\n"));

        output.push_str("# HELP ensemble_model_artifact_info Current model artifact\n");
        output.push_str("# TYPE ensemble_model_artifact_info gauge\n");
        output.push_str(&format!(
            "ensemble_model_artifact_info{{key=\"{artifact_key}\"}} 1\n"
        ));

        output
    }
}

#[derive(Clone)]
pub struct AppState {
    pub model_state: Arc<Mutex<Option<ModelState>>>,
    pub s3_client: aws_sdk_s3::Client,
    pub artifact_bucket: String,
    pub artifact_prefix: String,
    pub data_manager_base_url: String,
    pub model_version: String,
    pub metrics: Arc<Metrics>,
    pub local_artifact_dir: Option<std::path::PathBuf>,
    pub pool: Option<PgPool>,
}

impl AppState {
    pub async fn from_env() -> Self {
        let artifact_bucket = std::env::var("AWS_S3_MODEL_ARTIFACTS_BUCKET_NAME")
            .unwrap_or_else(|_| "fund-artifacts".to_string());
        let artifact_prefix = std::env::var("AWS_S3_MODEL_ARTIFACT_PATH")
            .unwrap_or_else(|_| "artifacts/tide/".to_string());
        let data_manager_base_url = std::env::var("FUND_DATA_MANAGER_BASE_URL")
            .unwrap_or_else(|_| "http://data-manager:8080".to_string());
        let model_version = std::env::var("MODEL_VERSION").unwrap_or_else(|_| "latest".to_string());
        let local_artifact_dir = std::env::var("FUND_LOCAL_ARTIFACT_DIR")
            .ok()
            .map(std::path::PathBuf::from);

        let s3_client = crate::common::aws::s3_client().await;

        let (pool, _) = crate::common::database::connect_optional_pool().await;

        AppState {
            model_state: Arc::new(Mutex::new(None)),
            s3_client,
            artifact_bucket,
            artifact_prefix,
            data_manager_base_url,
            model_version,
            metrics: Arc::new(Metrics::new()),
            local_artifact_dir,
            pool,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metrics_increment_requests() {
        let metrics = Metrics::new();
        metrics.increment_requests();
        metrics.increment_requests();
        assert_eq!(metrics.prediction_requests_total.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn test_metrics_increment_error() {
        let metrics = Metrics::new();
        metrics.increment_error("fetch_equity_bars");
        metrics.increment_error("fetch_equity_bars");
        metrics.increment_error("prediction");
        let errors = metrics.prediction_errors.lock().unwrap();
        assert_eq!(errors.get("fetch_equity_bars"), Some(&2));
        assert_eq!(errors.get("prediction"), Some(&1));
    }

    #[test]
    fn test_metrics_observe_duration() {
        let metrics = Metrics::new();
        metrics.observe_duration(3.0);
        metrics.observe_duration(15.0);

        assert_eq!(metrics.histogram_counts[0].load(Ordering::Relaxed), 0);
        assert_eq!(metrics.histogram_counts[1].load(Ordering::Relaxed), 1);
        assert_eq!(metrics.histogram_counts[2].load(Ordering::Relaxed), 1);
        assert_eq!(metrics.histogram_counts[3].load(Ordering::Relaxed), 2);
        assert_eq!(metrics.histogram_count.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn test_metrics_set_gauges() {
        let metrics = Metrics::new();
        metrics.set_batch_count(5);
        metrics.set_row_count(100);
        assert_eq!(metrics.prediction_batch_count.load(Ordering::Relaxed), 5);
        assert_eq!(metrics.prediction_row_count.load(Ordering::Relaxed), 100);
    }

    #[test]
    fn test_render_prometheus() {
        let metrics = Metrics::new();
        metrics.increment_requests();
        metrics.increment_error("fetch");
        metrics.observe_duration(2.5);
        metrics.set_batch_count(1);
        metrics.set_row_count(50);

        let output = metrics.render_prometheus(1700000000, "artifacts/tide/2024/model.tar.gz");
        assert!(output.contains("ensemble_prediction_requests_total 1"));
        assert!(output.contains("ensemble_prediction_errors_total{stage=\"fetch\"} 1"));
        assert!(output.contains("ensemble_prediction_duration_seconds_bucket"));
        assert!(output.contains("ensemble_prediction_batch_count 1"));
        assert!(output.contains("ensemble_prediction_row_count 50"));
        assert!(output.contains("ensemble_model_load_timestamp 1700000000"));
        assert!(output.contains("ensemble_model_artifact_info"));
    }
}
