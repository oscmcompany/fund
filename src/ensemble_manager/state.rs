use std::sync::Arc;

use burn::backend::NdArray;
use sqlx::PgPool;
use tokio::sync::Mutex;

use crate::models::tide::config::ModelParameters;
use crate::models::tide::data::{FeatureMappings, Scaler};
use crate::models::tide::model::TideModel;

pub struct ModelState {
    model: TideModel<NdArray>,
    parameters: ModelParameters,
    scaler: Scaler,
    mappings: FeatureMappings,
    continuous_columns: Vec<String>,
    categorical_columns: Vec<String>,
    static_categorical_columns: Vec<String>,
    artifact_key: String,
    /// Training run id (the timestamp segment of the artifact key). Written to
    /// `equity_predictions.model_run_id` so predictions join `model_runs.run_id`.
    run_id: String,
    load_timestamp: i64,
}

impl ModelState {
    /// Constructs a `ModelState` from a fully loaded artifact.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        model: TideModel<NdArray>,
        parameters: ModelParameters,
        scaler: Scaler,
        mappings: FeatureMappings,
        continuous_columns: Vec<String>,
        categorical_columns: Vec<String>,
        static_categorical_columns: Vec<String>,
        artifact_key: String,
        run_id: String,
        load_timestamp: i64,
    ) -> Self {
        Self {
            model,
            parameters,
            scaler,
            mappings,
            continuous_columns,
            categorical_columns,
            static_categorical_columns,
            artifact_key,
            run_id,
            load_timestamp,
        }
    }

    pub fn model(&self) -> &TideModel<NdArray> {
        &self.model
    }

    pub fn parameters(&self) -> &ModelParameters {
        &self.parameters
    }

    pub fn scaler(&self) -> &Scaler {
        &self.scaler
    }

    pub fn mappings(&self) -> &FeatureMappings {
        &self.mappings
    }

    pub fn continuous_columns(&self) -> &[String] {
        &self.continuous_columns
    }

    pub fn categorical_columns(&self) -> &[String] {
        &self.categorical_columns
    }

    pub fn static_categorical_columns(&self) -> &[String] {
        &self.static_categorical_columns
    }

    pub fn artifact_key(&self) -> &str {
        &self.artifact_key
    }

    pub fn run_id(&self) -> &str {
        &self.run_id
    }

    pub fn load_timestamp(&self) -> i64 {
        self.load_timestamp
    }
}

// SAFETY: TideModel<NdArray> is not Sync due to burn's Param<T> using an RwLock
// with a non-Sync FnOnce inside. We guard all access behind a Mutex, so this is safe.
unsafe impl Send for ModelState {}
unsafe impl Sync for ModelState {}

#[derive(Clone)]
pub struct AppState {
    model_state: Arc<Mutex<Option<ModelState>>>,
    s3_client: aws_sdk_s3::Client,
    artifact_bucket: String,
    artifact_prefix: String,
    data_manager_base_url: String,
    model_version: String,
    local_artifact_dir: Option<std::path::PathBuf>,
    pool: Option<PgPool>,
}

impl AppState {
    pub fn model_state(&self) -> &Arc<Mutex<Option<ModelState>>> {
        &self.model_state
    }

    pub fn s3_client(&self) -> &aws_sdk_s3::Client {
        &self.s3_client
    }

    pub fn artifact_bucket(&self) -> &str {
        &self.artifact_bucket
    }

    pub fn artifact_prefix(&self) -> &str {
        &self.artifact_prefix
    }

    pub fn data_manager_base_url(&self) -> &str {
        &self.data_manager_base_url
    }

    pub fn model_version(&self) -> &str {
        &self.model_version
    }

    pub fn local_artifact_dir(&self) -> Option<&std::path::Path> {
        self.local_artifact_dir.as_deref()
    }

    pub fn pool(&self) -> Option<&PgPool> {
        self.pool.as_ref()
    }

    /// Constructs an `AppState` for tests, with no model loaded and no pool.
    #[cfg(test)]
    pub fn for_tests(
        s3_client: aws_sdk_s3::Client,
        artifact_bucket: String,
        artifact_prefix: String,
        data_manager_base_url: String,
        model_version: String,
    ) -> Self {
        AppState {
            model_state: Arc::new(Mutex::new(None)),
            s3_client,
            artifact_bucket,
            artifact_prefix,
            data_manager_base_url,
            model_version,
            local_artifact_dir: None,
            pool: None,
        }
    }

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
            local_artifact_dir,
            pool,
        }
    }
}
