//! Model output and training run record types.

use chrono::{DateTime, NaiveDate, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

use crate::domain::market::Ticker;

/// Lifecycle status of a training run in `model_runs`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, sqlx::Type)]
#[sqlx(type_name = "text", rename_all = "snake_case")]
#[serde(rename_all = "snake_case")]
pub enum ModelRunStatus {
    Started,
    Completed,
    Failed,
}

impl ModelRunStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Started => "started",
            Self::Completed => "completed",
            Self::Failed => "failed",
        }
    }

    /// Parses a stored database value. Returns `None` for unknown values.
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "started" => Some(Self::Started),
            "completed" => Some(Self::Completed),
            "failed" => Some(Self::Failed),
            _ => None,
        }
    }
}

/// A single model prediction row for one ticker at one timestamp.
///
/// Identity is `(ticker, timestamp)` — matches the TimescaleDB primary key.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EquityPrediction {
    correlation_id: Uuid,
    model_run_id: String,
    ticker: Ticker,
    /// UTC timestamp for the day this prediction targets.
    timestamp: DateTime<Utc>,
    quantile_10: f64,
    quantile_50: f64,
    quantile_90: f64,
    /// Set by the database at insert time.
    created_at: DateTime<Utc>,
}

impl EquityPrediction {
    /// Constructs an `EquityPrediction` from validated field values.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        correlation_id: Uuid,
        model_run_id: String,
        ticker: Ticker,
        timestamp: DateTime<Utc>,
        quantile_10: f64,
        quantile_50: f64,
        quantile_90: f64,
        created_at: DateTime<Utc>,
    ) -> Self {
        Self {
            correlation_id,
            model_run_id,
            ticker,
            timestamp,
            quantile_10,
            quantile_50,
            quantile_90,
            created_at,
        }
    }

    pub fn correlation_id(&self) -> Uuid {
        self.correlation_id
    }

    pub fn model_run_id(&self) -> &str {
        &self.model_run_id
    }

    pub fn ticker(&self) -> &Ticker {
        &self.ticker
    }

    pub fn timestamp(&self) -> DateTime<Utc> {
        self.timestamp
    }

    pub fn quantile_10(&self) -> f64 {
        self.quantile_10
    }

    pub fn quantile_50(&self) -> f64 {
        self.quantile_50
    }

    pub fn quantile_90(&self) -> f64 {
        self.quantile_90
    }

    pub fn created_at(&self) -> DateTime<Utc> {
        self.created_at
    }
}

/// Training run metadata and evaluation metrics.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelRun {
    id: i64,
    run_id: String,
    model_name: String,
    artifact_key: Option<String>,
    training_data_key: Option<String>,
    start_date: Option<NaiveDate>,
    end_date: Option<NaiveDate>,
    lookback_days: Option<i32>,
    status: ModelRunStatus,
    continuous_ranked_probability_score: Option<f64>,
    directional_accuracy: Option<f64>,
    quantile_coverage: Option<f64>,
    drift_status: Option<String>,
    /// Per-stage prediction counts as a JSON object.
    stage_counts: Option<Value>,
    started_at: DateTime<Utc>,
    completed_at: Option<DateTime<Utc>>,
}

impl ModelRun {
    /// Constructs a `ModelRun` from validated field values.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        id: i64,
        run_id: String,
        model_name: String,
        artifact_key: Option<String>,
        training_data_key: Option<String>,
        start_date: Option<NaiveDate>,
        end_date: Option<NaiveDate>,
        lookback_days: Option<i32>,
        status: ModelRunStatus,
        continuous_ranked_probability_score: Option<f64>,
        directional_accuracy: Option<f64>,
        quantile_coverage: Option<f64>,
        drift_status: Option<String>,
        stage_counts: Option<Value>,
        started_at: DateTime<Utc>,
        completed_at: Option<DateTime<Utc>>,
    ) -> Self {
        Self {
            id,
            run_id,
            model_name,
            artifact_key,
            training_data_key,
            start_date,
            end_date,
            lookback_days,
            status,
            continuous_ranked_probability_score,
            directional_accuracy,
            quantile_coverage,
            drift_status,
            stage_counts,
            started_at,
            completed_at,
        }
    }

    pub fn id(&self) -> i64 {
        self.id
    }

    pub fn run_id(&self) -> &str {
        &self.run_id
    }

    pub fn model_name(&self) -> &str {
        &self.model_name
    }

    pub fn artifact_key(&self) -> Option<&str> {
        self.artifact_key.as_deref()
    }

    pub fn training_data_key(&self) -> Option<&str> {
        self.training_data_key.as_deref()
    }

    pub fn start_date(&self) -> Option<NaiveDate> {
        self.start_date
    }

    pub fn end_date(&self) -> Option<NaiveDate> {
        self.end_date
    }

    pub fn lookback_days(&self) -> Option<i32> {
        self.lookback_days
    }

    pub fn status(&self) -> &ModelRunStatus {
        &self.status
    }

    pub fn continuous_ranked_probability_score(&self) -> Option<f64> {
        self.continuous_ranked_probability_score
    }

    pub fn directional_accuracy(&self) -> Option<f64> {
        self.directional_accuracy
    }

    pub fn quantile_coverage(&self) -> Option<f64> {
        self.quantile_coverage
    }

    pub fn drift_status(&self) -> Option<&str> {
        self.drift_status.as_deref()
    }

    pub fn stage_counts(&self) -> Option<&Value> {
        self.stage_counts.as_ref()
    }

    pub fn started_at(&self) -> DateTime<Utc> {
        self.started_at
    }

    pub fn completed_at(&self) -> Option<DateTime<Utc>> {
        self.completed_at
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{NaiveDate, Utc};
    use uuid::Uuid;

    #[test]
    fn test_model_run_status_round_trip() {
        for status in [
            ModelRunStatus::Started,
            ModelRunStatus::Completed,
            ModelRunStatus::Failed,
        ] {
            assert_eq!(ModelRunStatus::parse(status.as_str()), Some(status.clone()));
            let serialized = serde_json::to_string(&status).unwrap();
            assert_eq!(serialized, format!("\"{}\"", status.as_str()));
            let deserialized: ModelRunStatus = serde_json::from_str(&serialized).unwrap();
            assert_eq!(deserialized, status);
        }
    }

    #[test]
    fn test_model_run_status_parse_rejects_unknown() {
        assert_eq!(ModelRunStatus::parse("running"), None);
        assert_eq!(ModelRunStatus::parse("COMPLETED"), None);
    }

    #[test]
    fn test_equity_prediction_construction() {
        let prediction = EquityPrediction::new(
            Uuid::new_v4(),
            "run-abc123".to_string(),
            Ticker::new("AAPL").unwrap(),
            Utc::now(),
            -0.02,
            0.01,
            0.04,
            Utc::now(),
        );
        assert_eq!(prediction.ticker().as_str(), "AAPL");
        assert_eq!(prediction.model_run_id(), "run-abc123");
        assert!(prediction.quantile_10() < prediction.quantile_50());
        assert!(prediction.quantile_50() < prediction.quantile_90());
    }

    #[test]
    fn test_equity_prediction_clone() {
        let prediction = EquityPrediction::new(
            Uuid::new_v4(),
            "run-def456".to_string(),
            Ticker::new("MSFT").unwrap(),
            Utc::now(),
            -0.01,
            0.005,
            0.02,
            Utc::now(),
        );
        let cloned = prediction.clone();
        assert_eq!(cloned.ticker().as_str(), "MSFT");
        assert_eq!(cloned.quantile_50(), 0.005);
    }

    #[test]
    fn test_model_run_construction_minimal() {
        let model_run = ModelRun::new(
            1,
            "run-tide-001".to_string(),
            "tide".to_string(),
            None,
            None,
            None,
            None,
            None,
            ModelRunStatus::Started,
            None,
            None,
            None,
            None,
            None,
            Utc::now(),
            None,
        );
        assert_eq!(model_run.model_name(), "tide");
        assert_eq!(model_run.status(), &ModelRunStatus::Started);
    }

    #[test]
    fn test_model_run_construction_completed() {
        let model_run = ModelRun::new(
            2,
            "run-tide-002".to_string(),
            "tide".to_string(),
            Some("models/tide/weights.safetensors".to_string()),
            Some("data/equity/bars/training.parquet".to_string()),
            Some(NaiveDate::from_ymd_opt(2026, 1, 1).unwrap()),
            Some(NaiveDate::from_ymd_opt(2026, 5, 1).unwrap()),
            Some(70),
            ModelRunStatus::Completed,
            Some(0.42),
            Some(0.55),
            Some(0.88),
            Some("stable".to_string()),
            Some(serde_json::json!({"stage_1": 100, "stage_2": 200})),
            Utc::now(),
            Some(Utc::now()),
        );
        assert_eq!(model_run.status(), &ModelRunStatus::Completed);
        assert_eq!(model_run.lookback_days(), Some(70));
        assert!(model_run.continuous_ranked_probability_score().is_some());
        assert!(model_run.stage_counts().is_some());
    }

    #[test]
    fn test_model_run_clone() {
        let model_run = ModelRun::new(
            3,
            "run-tide-003".to_string(),
            "tide".to_string(),
            None,
            None,
            None,
            None,
            Some(70),
            ModelRunStatus::Failed,
            None,
            None,
            None,
            None,
            None,
            Utc::now(),
            None,
        );
        let cloned = model_run.clone();
        assert_eq!(cloned.run_id(), "run-tide-003");
        assert_eq!(cloned.status(), &ModelRunStatus::Failed);
    }
}
