use std::time::Instant;

use tracing::{error, info, warn};
use uuid::Uuid;

use crate::ensemble_manager::artifact;
use crate::ensemble_manager::database;
use crate::ensemble_manager::predict;
use crate::ensemble_manager::state::AppState;

/// Successful outcome of a prediction run.
pub struct PredictionRun {
    predictions: serde_json::Value,
    row_count: usize,
}

impl PredictionRun {
    /// Constructs a `PredictionRun` from the generated predictions and their row count.
    pub fn new(predictions: serde_json::Value, row_count: usize) -> Self {
        Self {
            predictions,
            row_count,
        }
    }

    pub fn predictions(&self) -> &serde_json::Value {
        &self.predictions
    }

    /// Consumes the run, returning the generated predictions.
    pub fn into_predictions(self) -> serde_json::Value {
        self.predictions
    }

    pub fn row_count(&self) -> usize {
        self.row_count
    }
}

/// A prediction-pipeline failure, tagged with the stage that failed so callers
/// can log it and emit `predictions_failed` with a reason.
pub struct PipelineError {
    stage: &'static str,
    message: String,
}

impl PipelineError {
    /// Constructs a `PipelineError` for the given pipeline stage.
    pub fn new(stage: &'static str, message: String) -> Self {
        Self { stage, message }
    }

    pub fn stage(&self) -> &'static str {
        self.stage
    }

    pub fn message(&self) -> &str {
        &self.message
    }
}

/// Run the full prediction pipeline once and persist the result.
///
/// Shared by the consolidated `fund` binary and the Postgres event consumer.
/// Predictions are inserted into `equity_predictions` and a
/// `predictions_completed` event is emitted on success; any stage failure emits
/// `predictions_errored` (with the stage as the reason) so downstream consumers
/// aren't left waiting.
pub async fn run_predictions(state: &AppState) -> Result<PredictionRun, PipelineError> {
    let start = Instant::now();

    let pool = match state.pool() {
        Some(pool) => pool,
        None => {
            let pipeline_error =
                PipelineError::new("no_pool", "Database pool required".to_string());
            error!(
                stage = pipeline_error.stage(),
                error = %pipeline_error.message(),
                duration_ms = start.elapsed().as_millis() as u64,
                "Prediction pipeline failed"
            );
            return Err(pipeline_error);
        }
    };

    let correlation_id = Uuid::new_v4();

    let result = run_pipeline_and_persist(state, pool, correlation_id).await;

    info!(
        duration_ms = start.elapsed().as_millis() as u64,
        succeeded = result.is_ok(),
        "Prediction run complete"
    );

    if let Err(error) = &result {
        error!(stage = error.stage(), error = %error.message(), "Prediction pipeline failed");
        if let Err(emit_error) = crate::common::events::emit_event(
            pool,
            crate::common::events::EventType::EquityPredictionsErrored,
            &serde_json::json!({
                "correlation_id": correlation_id.to_string(),
                "reason": error.stage(),
            }),
        )
        .await
        {
            warn!(error = %emit_error, "Failed to emit predictions_errored event");
        }
    }

    result
}

async fn run_pipeline_and_persist(
    state: &AppState,
    pool: &sqlx::PgPool,
    correlation_id: Uuid,
) -> Result<PredictionRun, PipelineError> {
    let (predictions, model_run_id) = {
        let guard = state.model_state().lock().await;
        let model_state = guard.as_ref().ok_or_else(|| {
            PipelineError::new("model_not_loaded", "Model not loaded".to_string())
        })?;

        let equity_bars = predict::fetch_equity_bars(pool)
            .await
            .map_err(|e| PipelineError::new("fetch_equity_bars", e.to_string()))?;

        let equity_details = predict::fetch_equity_details(pool)
            .await
            .map_err(|e| PipelineError::new("fetch_equity_details", e.to_string()))?;

        let consolidated = predict::consolidate_data(equity_bars, equity_details)
            .map_err(|e| PipelineError::new("data_consolidation", e.to_string()))?;

        let equity_filtered = predict::filter_equity_bars(
            consolidated,
            crate::domain::market::MINIMUM_CLOSE_PRICE,
            crate::domain::market::MINIMUM_VOLUME,
        )
        .map_err(|e| PipelineError::new("equity_bar_filtering", e.to_string()))?;

        let filtered = predict::filter_to_trained_tickers(equity_filtered, model_state)
            .map_err(|e| PipelineError::new("ticker_filtering", e.to_string()))?;

        let predictions = predict::generate_predictions(filtered, model_state)
            .map_err(|e| PipelineError::new("prediction", e.to_string()))?;

        (predictions, model_state.run_id().to_string())
    };

    if let Some(prediction_array) = predictions.as_array() {
        predict::validate_predictions(prediction_array)
            .map_err(|message| PipelineError::new("validation", message))?;
    }

    let row_count = predictions.as_array().map(|array| array.len()).unwrap_or(0);

    if let Some(prediction_array) = predictions.as_array() {
        let rows =
            database::insert_predictions(pool, prediction_array, correlation_id, &model_run_id)
                .await
                .map_err(|e| PipelineError::new("insert_predictions", e.to_string()))?;
        info!(rows = rows, "Predictions inserted into PostgreSQL");
        if let Err(e) = crate::common::events::emit_event(
            pool,
            crate::common::events::EventType::EquityPredictionsCompleted,
            &serde_json::json!({"correlation_id": correlation_id.to_string()}),
        )
        .await
        {
            warn!(error = %e, "Failed to emit predictions_completed event");
        }
    }

    Ok(PredictionRun::new(predictions, row_count))
}

/// Resolve the latest artifact and load it if it differs from the current
/// model, recording training lineage in `model_runs`. Called once at startup
/// (before the event consumer spawns, so a catch-up run has a model to use)
/// and then from the polling loop.
pub async fn poll_artifact_once(state: &AppState) {
    let latest_key = match artifact::resolve_artifact_key(
        state.s3_client(),
        state.artifact_bucket(),
        state.artifact_prefix(),
        state.model_version(),
        state.local_artifact_dir(),
    )
    .await
    {
        Ok(key) => key,
        Err(e) => {
            warn!(error = %e, "Failed to resolve artifact key");
            return;
        }
    };

    let current_key = {
        let guard = state.model_state().lock().await;
        guard.as_ref().map(|ms| ms.artifact_key().to_string())
    };

    if current_key.as_deref() == Some(&latest_key) {
        return;
    }

    info!(
        current = current_key.as_deref().unwrap_or("none"),
        latest = latest_key,
        "New model artifact detected"
    );

    match artifact::download_and_load_model(
        state.s3_client(),
        state.artifact_bucket(),
        &latest_key,
        state.local_artifact_dir(),
    )
    .await
    {
        Ok(new_model_state) => {
            // Record training lineage in model_runs so predictions written
            // with this run_id join back to its metrics. Best-effort.
            if let Some(pool) = state.pool() {
                let run_id = new_model_state.run_id().to_string();
                match artifact::fetch_run_metadata(
                    state.s3_client(),
                    state.artifact_bucket(),
                    &latest_key,
                    state.local_artifact_dir(),
                )
                .await
                {
                    Some(metadata) => {
                        let record = database::ModelRunRecord::from_metadata(
                            &run_id,
                            &latest_key,
                            &metadata,
                        );
                        if let Err(e) = database::upsert_model_run(pool, &record).await {
                            warn!(error = %e, "Failed to upsert model_runs row");
                        }
                    }
                    None => {
                        warn!(
                            run_id = run_id,
                            "run_metadata.json unavailable; skipping model_runs upsert"
                        );
                    }
                }
            }

            let mut guard = state.model_state().lock().await;
            *guard = Some(new_model_state);
            info!(artifact_key = latest_key, "Model hot-swapped");
        }
        Err(e) => {
            error!(error = %e, "Failed to load new model artifact");
        }
    }
}

pub async fn start_artifact_polling(state: AppState) {
    let poll_interval = std::time::Duration::from_secs(60);

    loop {
        tokio::time::sleep(poll_interval).await;
        poll_artifact_once(&state).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_test_state() -> AppState {
        let s3_client = {
            let config = aws_sdk_s3::Config::builder()
                .behavior_version(aws_sdk_s3::config::BehaviorVersion::latest())
                .region(aws_sdk_s3::config::Region::new("us-east-1"))
                .build();
            aws_sdk_s3::Client::from_conf(config)
        };
        AppState::for_tests(
            s3_client,
            "test-bucket".to_string(),
            "artifacts/tide/".to_string(),
            "latest".to_string(),
        )
    }

    #[tokio::test]
    async fn test_run_predictions_without_pool_reports_stage() {
        let state = make_test_state();

        let result = run_predictions(&state).await;
        let error = result.err().expect("run must fail without a pool");
        assert_eq!(error.stage(), "no_pool");
    }

    #[test]
    fn test_prediction_run_row_count() {
        let predictions = serde_json::json!([
            {"ticker": "AAPL", "timestamp": 1000, "quantile_10": 0.01, "quantile_50": 0.02, "quantile_90": 0.03},
            {"ticker": "GOOG", "timestamp": 1000, "quantile_10": 0.01, "quantile_50": 0.02, "quantile_90": 0.03},
        ]);
        let run = PredictionRun::new(predictions.clone(), 2);
        assert_eq!(run.row_count(), 2);
        assert_eq!(run.predictions(), &predictions);
    }

    #[test]
    fn test_prediction_run_into_predictions_consumes() {
        let predictions = serde_json::json!({"ticker": "AAPL"});
        let run = PredictionRun::new(predictions.clone(), 1);
        let extracted = run.into_predictions();
        assert_eq!(extracted, predictions);
    }

    #[test]
    fn test_prediction_run_zero_rows() {
        let run = PredictionRun::new(serde_json::json!([]), 0);
        assert_eq!(run.row_count(), 0);
    }

    #[test]
    fn test_pipeline_error_accessors() {
        let error = PipelineError::new("fetch_equity_bars", "connection refused".to_string());
        assert_eq!(error.stage(), "fetch_equity_bars");
        assert_eq!(error.message(), "connection refused");
    }

    #[test]
    fn test_pipeline_error_stage_strings_are_stable() {
        let stages = [
            "no_pool",
            "model_not_loaded",
            "fetch_equity_bars",
            "fetch_equity_details",
            "data_consolidation",
            "equity_bar_filtering",
            "ticker_filtering",
            "prediction",
            "validation",
            "insert_predictions",
        ];
        for stage in stages {
            let error = PipelineError::new(stage, String::new());
            assert_eq!(error.stage(), stage);
        }
    }

    #[test]
    fn test_prediction_run_new_stores_predictions_and_row_count() {
        let predictions = serde_json::json!([
            {"ticker": "TSLA", "timestamp": 2000, "quantile_10": 0.0, "quantile_50": 0.1, "quantile_90": 0.2},
        ]);
        let run = PredictionRun::new(predictions.clone(), 1);
        assert_eq!(run.row_count(), 1);
        assert_eq!(run.predictions(), &predictions);
        let extracted = run.into_predictions();
        assert_eq!(extracted, predictions);
    }

    #[test]
    fn test_prediction_run_row_count_zero_with_empty_array() {
        let run = PredictionRun::new(serde_json::json!([]), 0);
        assert_eq!(run.row_count(), 0);
        assert_eq!(run.predictions().as_array().unwrap().len(), 0);
    }

    #[test]
    fn test_pipeline_error_new_stores_stage_and_message() {
        let error = PipelineError::new("validation", "schema mismatch".to_string());
        assert_eq!(error.stage(), "validation");
        assert_eq!(error.message(), "schema mismatch");
    }

    #[test]
    fn test_pipeline_error_message_can_be_empty() {
        let error = PipelineError::new("prediction", String::new());
        assert_eq!(error.stage(), "prediction");
        assert_eq!(error.message(), "");
    }

    #[test]
    fn test_pipeline_error_with_multiline_message() {
        let message = "line one\nline two".to_string();
        let error = PipelineError::new("insert_predictions", message.clone());
        assert_eq!(error.message(), message);
    }

    #[tokio::test]
    async fn test_run_predictions_error_stage_is_no_pool() {
        let state = make_test_state();
        let error = run_predictions(&state)
            .await
            .err()
            .expect("expected an error when no pool is configured");
        assert_eq!(error.stage(), "no_pool");
        assert!(!error.message().is_empty());
    }
}
