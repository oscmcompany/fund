//! Model output and training run record types.

use chrono::{DateTime, NaiveDate, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::FromRow;
use uuid::Uuid;

/// A single model prediction row for one ticker at one timestamp.
///
/// Identity is `(ticker, timestamp)` — matches the TimescaleDB primary key.
#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct EquityPrediction {
    pub correlation_id: Uuid,
    pub model_run_id: String,
    pub ticker: String,
    /// UTC timestamp for the day this prediction targets.
    pub timestamp: DateTime<Utc>,
    pub quantile_10: f64,
    pub quantile_50: f64,
    pub quantile_90: f64,
    /// Set by the database at insert time.
    pub created_at: DateTime<Utc>,
}

/// Training run metadata and evaluation metrics.
#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct ModelRun {
    pub id: i64,
    pub run_id: String,
    pub model_name: String,
    pub artifact_key: Option<String>,
    pub training_data_key: Option<String>,
    pub start_date: Option<NaiveDate>,
    pub end_date: Option<NaiveDate>,
    pub lookback_days: Option<i32>,
    pub status: String,
    pub continuous_ranked_probability_score: Option<f64>,
    pub directional_accuracy: Option<f64>,
    pub quantile_coverage: Option<f64>,
    pub drift_status: Option<String>,
    /// Per-stage prediction counts as a JSON object.
    pub stage_counts: Option<Value>,
    pub started_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{NaiveDate, Utc};
    use uuid::Uuid;

    #[test]
    fn test_equity_prediction_construction() {
        let prediction = EquityPrediction {
            correlation_id: Uuid::new_v4(),
            model_run_id: "run-abc123".to_string(),
            ticker: "AAPL".to_string(),
            timestamp: Utc::now(),
            quantile_10: -0.02,
            quantile_50: 0.01,
            quantile_90: 0.04,
            created_at: Utc::now(),
        };
        assert_eq!(prediction.ticker, "AAPL");
        assert_eq!(prediction.model_run_id, "run-abc123");
        assert!(prediction.quantile_10 < prediction.quantile_50);
        assert!(prediction.quantile_50 < prediction.quantile_90);
    }

    #[test]
    fn test_equity_prediction_clone() {
        let prediction = EquityPrediction {
            correlation_id: Uuid::new_v4(),
            model_run_id: "run-def456".to_string(),
            ticker: "MSFT".to_string(),
            timestamp: Utc::now(),
            quantile_10: -0.01,
            quantile_50: 0.005,
            quantile_90: 0.02,
            created_at: Utc::now(),
        };
        let cloned = prediction.clone();
        assert_eq!(cloned.ticker, "MSFT");
        assert_eq!(cloned.quantile_50, 0.005);
    }

    #[test]
    fn test_model_run_construction_minimal() {
        let model_run = ModelRun {
            id: 1,
            run_id: "run-tide-001".to_string(),
            model_name: "tide".to_string(),
            artifact_key: None,
            training_data_key: None,
            start_date: None,
            end_date: None,
            lookback_days: None,
            status: "started".to_string(),
            continuous_ranked_probability_score: None,
            directional_accuracy: None,
            quantile_coverage: None,
            drift_status: None,
            stage_counts: None,
            started_at: Utc::now(),
            completed_at: None,
        };
        assert_eq!(model_run.model_name, "tide");
        assert_eq!(model_run.status, "started");
    }

    #[test]
    fn test_model_run_construction_completed() {
        let model_run = ModelRun {
            id: 2,
            run_id: "run-tide-002".to_string(),
            model_name: "tide".to_string(),
            artifact_key: Some("models/tide/weights.safetensors".to_string()),
            training_data_key: Some("data/equity/bars/training.parquet".to_string()),
            start_date: Some(NaiveDate::from_ymd_opt(2026, 1, 1).unwrap()),
            end_date: Some(NaiveDate::from_ymd_opt(2026, 5, 1).unwrap()),
            lookback_days: Some(70),
            status: "completed".to_string(),
            continuous_ranked_probability_score: Some(0.42),
            directional_accuracy: Some(0.55),
            quantile_coverage: Some(0.88),
            drift_status: Some("stable".to_string()),
            stage_counts: Some(serde_json::json!({"stage_1": 100, "stage_2": 200})),
            started_at: Utc::now(),
            completed_at: Some(Utc::now()),
        };
        assert_eq!(model_run.status, "completed");
        assert_eq!(model_run.lookback_days, Some(70));
        assert!(model_run.continuous_ranked_probability_score.is_some());
        assert!(model_run.stage_counts.is_some());
    }

    #[test]
    fn test_model_run_clone() {
        let model_run = ModelRun {
            id: 3,
            run_id: "run-tide-003".to_string(),
            model_name: "tide".to_string(),
            artifact_key: None,
            training_data_key: None,
            start_date: None,
            end_date: None,
            lookback_days: Some(70),
            status: "failed".to_string(),
            continuous_ranked_probability_score: None,
            directional_accuracy: None,
            quantile_coverage: None,
            drift_status: None,
            stage_counts: None,
            started_at: Utc::now(),
            completed_at: None,
        };
        let cloned = model_run.clone();
        assert_eq!(cloned.run_id, "run-tide-003");
        assert_eq!(cloned.status, "failed");
    }
}
