use std::time::Instant;

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Json};
use axum::routing::{get, post};
use axum::Router;
use tower_http::trace::TraceLayer;
use tracing::{error, info, warn};
use uuid::Uuid;

use crate::ensemble_model::artifact;
use crate::ensemble_model::database;
use crate::ensemble_model::predict;
use crate::ensemble_model::state::AppState;

pub fn create_router(state: AppState) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/predictions", post(predictions))
        .route("/model/predictions", post(predictions))
        .route("/metrics", get(metrics))
        .with_state(state)
        .layer(TraceLayer::new_for_http())
}

async fn health(State(state): State<AppState>) -> impl IntoResponse {
    let model_loaded = state.model_state.lock().await.is_some();

    let status = if model_loaded {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };

    let body = serde_json::json!({
        "status": if model_loaded { "healthy" } else { "unhealthy" },
        "model_loaded": model_loaded,
    });

    (status, Json(body))
}

async fn metrics(State(state): State<AppState>) -> impl IntoResponse {
    let guard = state.model_state.lock().await;

    let load_timestamp = guard.as_ref().map(|ms| ms.load_timestamp).unwrap_or(0);
    let artifact_key = guard
        .as_ref()
        .map(|ms| ms.artifact_key.clone())
        .unwrap_or_default();

    drop(guard);

    let body = state
        .metrics
        .render_prometheus(load_timestamp, &artifact_key);

    (StatusCode::OK, body)
}

/// Successful outcome of a prediction run.
pub struct PredictionRun {
    pub predictions: serde_json::Value,
    pub row_count: usize,
}

/// A prediction-pipeline failure, tagged with the stage that failed so callers
/// can record metrics and emit `predictions_failed` with a reason.
pub struct PipelineError {
    pub stage: &'static str,
    pub message: String,
}

/// Run the full prediction pipeline once and persist the result.
///
/// Shared by the HTTP handler and the Postgres event consumer. When a pool is
/// configured, predictions are inserted into `equity_predictions` and a
/// `predictions_completed` event is emitted on success; any stage failure emits
/// `predictions_failed` (with the stage as the reason) so downstream consumers
/// aren't left waiting. Without a pool, predictions are POSTed to the data
/// manager as a fallback.
pub async fn run_predictions(state: &AppState) -> Result<PredictionRun, PipelineError> {
    let start = Instant::now();
    state.metrics.increment_requests();

    let correlation_id = Uuid::new_v4();
    let http_client = reqwest::Client::new();

    let result = run_pipeline_and_persist(state, &http_client, correlation_id).await;

    state
        .metrics
        .observe_duration(start.elapsed().as_secs_f64());

    if let Err(error) = &result {
        state.metrics.increment_error(error.stage);
        error!(stage = error.stage, error = %error.message, "Prediction pipeline failed");
        if let Some(pool) = &state.pool {
            if let Err(emit_error) = database::emit_event(
                pool,
                "predictions_failed",
                &serde_json::json!({
                    "correlation_id": correlation_id.to_string(),
                    "reason": error.stage,
                }),
            )
            .await
            {
                warn!(error = %emit_error, "Failed to emit predictions_failed event");
            }
        }
    }

    result
}

async fn run_pipeline_and_persist(
    state: &AppState,
    http_client: &reqwest::Client,
    correlation_id: Uuid,
) -> Result<PredictionRun, PipelineError> {
    // Inference holds the model lock across the data-fetch awaits, matching the
    // original handler. The block yields the predictions plus the run id so the
    // lock is released before persistence.
    let (predictions, model_run_id) = {
        let guard = state.model_state.lock().await;
        let model_state = guard.as_ref().ok_or_else(|| PipelineError {
            stage: "model_not_loaded",
            message: "Model not loaded".to_string(),
        })?;

        let equity_bars = predict::fetch_equity_bars_auto(
            state.pool.as_ref(),
            &state.data_manager_base_url,
            http_client,
        )
        .await
        .map_err(|e| PipelineError {
            stage: "fetch_equity_bars",
            message: e.to_string(),
        })?;

        let equity_details = predict::fetch_equity_details_auto(
            state.pool.as_ref(),
            &state.data_manager_base_url,
            http_client,
        )
        .await
        .map_err(|e| PipelineError {
            stage: "fetch_equity_details",
            message: e.to_string(),
        })?;

        let consolidated =
            predict::consolidate_data(equity_bars, equity_details).map_err(|e| PipelineError {
                stage: "data_consolidation",
                message: e.to_string(),
            })?;

        let equity_filtered = predict::filter_equity_bars(
            consolidated,
            crate::domain::market::MINIMUM_CLOSE_PRICE,
            crate::domain::market::MINIMUM_VOLUME,
        )
        .map_err(|e| PipelineError {
            stage: "equity_bar_filtering",
            message: e.to_string(),
        })?;

        let filtered =
            predict::filter_to_trained_tickers(equity_filtered, model_state).map_err(|e| {
                PipelineError {
                    stage: "ticker_filtering",
                    message: e.to_string(),
                }
            })?;

        let predictions =
            predict::generate_predictions(filtered, model_state).map_err(|e| PipelineError {
                stage: "prediction",
                message: e.to_string(),
            })?;

        (predictions, model_state.run_id.clone())
    };

    if let Some(prediction_array) = predictions.as_array() {
        predict::validate_predictions(prediction_array).map_err(|message| PipelineError {
            stage: "validation",
            message,
        })?;
        state.metrics.set_batch_count(1);
        state.metrics.set_row_count(prediction_array.len() as u64);
    }

    let row_count = predictions.as_array().map(|array| array.len()).unwrap_or(0);

    if let Some(pool) = &state.pool {
        if let Some(prediction_array) = predictions.as_array() {
            let rows =
                database::insert_predictions(pool, prediction_array, correlation_id, &model_run_id)
                    .await
                    .map_err(|e| PipelineError {
                        stage: "insert_predictions",
                        message: e.to_string(),
                    })?;
            info!(rows = rows, "Predictions inserted into PostgreSQL");
            if let Err(e) = database::emit_event(
                pool,
                "predictions_completed",
                &serde_json::json!({"correlation_id": correlation_id.to_string()}),
            )
            .await
            {
                warn!(error = %e, "Failed to emit predictions_completed event");
            }
        }
    } else {
        let save_result = http_client
            .post(format!("{}/predictions", state.data_manager_base_url))
            .json(&serde_json::json!({
                "timestamp": chrono::Utc::now().to_rfc3339(),
                "data": predictions,
            }))
            .send()
            .await;

        match save_result {
            Ok(resp) if resp.status().is_success() => {
                info!("Predictions saved to data manager");
            }
            Ok(resp) => {
                warn!(status = %resp.status(), "Failed to save predictions to data manager");
            }
            Err(e) => {
                warn!(error = %e, "Failed to save predictions to data manager");
            }
        }
    }

    Ok(PredictionRun {
        predictions,
        row_count,
    })
}

async fn predictions(State(state): State<AppState>) -> impl IntoResponse {
    let start = Instant::now();
    info!("Prediction request received");

    let response = match run_predictions(&state).await {
        Ok(run) => (StatusCode::OK, Json(run.predictions)),
        Err(error) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": error.message})),
        ),
    };

    info!(
        duration_ms = start.elapsed().as_millis(),
        "Prediction request complete"
    );

    response
}

/// Resolve the latest artifact and load it if it differs from the current
/// model, recording training lineage in `model_runs`. Called once at startup
/// (before the event consumer spawns, so a catch-up run has a model to use)
/// and then from the polling loop.
pub async fn poll_artifact_once(state: &AppState) {
    let latest_key = match artifact::resolve_artifact_key(
        &state.s3_client,
        &state.artifact_bucket,
        &state.artifact_prefix,
        &state.model_version,
        state.local_artifact_dir.as_deref(),
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
        let guard = state.model_state.lock().await;
        guard.as_ref().map(|ms| ms.artifact_key.clone())
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
        &state.s3_client,
        &state.artifact_bucket,
        &latest_key,
        state.local_artifact_dir.as_deref(),
    )
    .await
    {
        Ok(new_model_state) => {
            // Record training lineage in model_runs so predictions written
            // with this run_id join back to its metrics. Best-effort.
            if let Some(pool) = &state.pool {
                let run_id = new_model_state.run_id.clone();
                match artifact::fetch_run_metadata(
                    &state.s3_client,
                    &state.artifact_bucket,
                    &latest_key,
                    state.local_artifact_dir.as_deref(),
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

            let mut guard = state.model_state.lock().await;
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
    use axum::body::Body;
    use axum::http::Request;
    use tower::ServiceExt;

    fn make_test_state() -> AppState {
        AppState {
            model_state: std::sync::Arc::new(tokio::sync::Mutex::new(None)),
            s3_client: {
                let config = aws_sdk_s3::Config::builder()
                    .behavior_version(aws_sdk_s3::config::BehaviorVersion::latest())
                    .region(aws_sdk_s3::config::Region::new("us-east-1"))
                    .build();
                aws_sdk_s3::Client::from_conf(config)
            },
            artifact_bucket: "test-bucket".to_string(),
            artifact_prefix: "artifacts/tide/".to_string(),
            data_manager_base_url: "http://localhost:8080".to_string(),
            model_version: "latest".to_string(),
            metrics: std::sync::Arc::new(crate::ensemble_model::state::Metrics::new()),
            local_artifact_dir: None,
            pool: None,
        }
    }

    #[tokio::test]
    async fn test_health_no_model() {
        let state = make_test_state();
        let app = create_router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/health")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[tokio::test]
    async fn test_metrics_contains_all_prometheus_metrics() {
        let state = make_test_state();
        let app = create_router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/metrics")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let text = String::from_utf8(body.to_vec()).unwrap();

        assert!(text.contains("ensemble_prediction_requests_total"));
        assert!(text.contains("ensemble_prediction_errors_total"));
        assert!(text.contains("ensemble_prediction_duration_seconds_bucket"));
        assert!(text.contains("ensemble_prediction_batch_count"));
        assert!(text.contains("ensemble_prediction_row_count"));
        assert!(text.contains("ensemble_model_load_timestamp"));
        assert!(text.contains("ensemble_model_artifact_info"));
    }

    #[tokio::test]
    async fn test_run_predictions_records_request_metrics() {
        // The Python service counts every prediction run (event-triggered
        // included) and observes its duration; the shared run_predictions must
        // record both so the consumer path isn't blind.
        let state = make_test_state();

        let result = run_predictions(&state).await;
        assert!(result.is_err());

        let rendered = state.metrics.render_prometheus(0, "");
        assert!(
            rendered.contains("ensemble_prediction_requests_total 1"),
            "request counter not incremented by run_predictions"
        );
        assert!(
            rendered.contains("ensemble_prediction_duration_seconds_count 1"),
            "duration histogram not observed by run_predictions"
        );
    }

    #[tokio::test]
    async fn test_predictions_no_model() {
        let state = make_test_state();
        let app = create_router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/predictions")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    }
}
