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

async fn predictions(State(state): State<AppState>) -> impl IntoResponse {
    let start = Instant::now();
    state.metrics.increment_requests();
    info!("Prediction request received");

    let guard = state.model_state.lock().await;
    let model_state = match guard.as_ref() {
        Some(ms) => ms,
        None => {
            error!("Model not loaded");
            state.metrics.increment_error("model_not_loaded");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": "Model not loaded"})),
            );
        }
    };

    let http_client = reqwest::Client::new();

    let equity_bars = match predict::fetch_equity_bars_auto(
        state.pool.as_ref(),
        &state.data_manager_base_url,
        &http_client,
    )
    .await
    {
        Ok(data) => data,
        Err(e) => {
            error!(error = %e, stage = "fetch_equity_bars", "Prediction failed");
            state.metrics.increment_error("fetch_equity_bars");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": e.to_string()})),
            );
        }
    };

    let equity_details = match predict::fetch_equity_details_auto(
        state.pool.as_ref(),
        &state.data_manager_base_url,
        &http_client,
    )
    .await
    {
        Ok(data) => data,
        Err(e) => {
            error!(error = %e, stage = "fetch_equity_details", "Prediction failed");
            state.metrics.increment_error("fetch_equity_details");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": e.to_string()})),
            );
        }
    };

    let consolidated = match predict::consolidate_data(equity_bars, equity_details) {
        Ok(data) => data,
        Err(e) => {
            error!(error = %e, stage = "data_consolidation", "Prediction failed");
            state.metrics.increment_error("data_consolidation");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": e.to_string()})),
            );
        }
    };

    let equity_filtered = match predict::filter_equity_bars(consolidated, 10.0, 1_000_000.0) {
        Ok(data) => data,
        Err(e) => {
            error!(error = %e, stage = "equity_bar_filtering", "Prediction failed");
            state.metrics.increment_error("equity_bar_filtering");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": e.to_string()})),
            );
        }
    };

    let filtered = match predict::filter_to_trained_tickers(equity_filtered, model_state) {
        Ok(data) => data,
        Err(e) => {
            error!(error = %e, stage = "ticker_filtering", "Prediction failed");
            state.metrics.increment_error("ticker_filtering");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": e.to_string()})),
            );
        }
    };

    let prediction_result = predict::generate_predictions(filtered, model_state);

    drop(guard);

    let predictions = match prediction_result {
        Ok(data) => data,
        Err(e) => {
            error!(error = %e, stage = "prediction", "Prediction failed");
            state.metrics.increment_error("prediction");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": e.to_string()})),
            );
        }
    };

    if let Some(prediction_array) = predictions.as_array() {
        if let Err(e) = predict::validate_predictions(prediction_array) {
            error!(error = %e, stage = "validation", "Prediction validation failed");
            state.metrics.increment_error("validation");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": e})),
            );
        }

        state.metrics.set_batch_count(1);
        state.metrics.set_row_count(prediction_array.len() as u64);
    }

    // Save predictions via PG when available, HTTP fallback otherwise
    if let Some(pool) = &state.pool {
        let correlation_id = Uuid::new_v4();
        let model_run_id = {
            let guard = state.model_state.lock().await;
            guard
                .as_ref()
                .map(|ms| ms.artifact_key.clone())
                .unwrap_or_default()
        };

        if let Some(prediction_array) = predictions.as_array() {
            match database::insert_predictions(
                pool,
                prediction_array,
                correlation_id,
                &model_run_id,
            )
            .await
            {
                Ok(rows) => {
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
                Err(e) => {
                    warn!(error = %e, "Failed to insert predictions into PostgreSQL");
                    if let Err(event_error) = database::emit_event(
                        pool,
                        "predictions_failed",
                        &serde_json::json!({
                            "correlation_id": correlation_id.to_string(),
                            "reason": "insert_predictions",
                        }),
                    )
                    .await
                    {
                        warn!(error = %event_error, "Failed to emit predictions_failed event");
                    }
                }
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

    let duration = start.elapsed();
    state.metrics.observe_duration(duration.as_secs_f64());
    info!(
        duration_ms = duration.as_millis(),
        "Prediction request complete"
    );

    (StatusCode::OK, Json(predictions))
}

pub async fn start_artifact_polling(state: AppState) {
    let poll_interval = std::time::Duration::from_secs(60);

    loop {
        tokio::time::sleep(poll_interval).await;

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
                continue;
            }
        };

        let current_key = {
            let guard = state.model_state.lock().await;
            guard.as_ref().map(|ms| ms.artifact_key.clone())
        };

        if current_key.as_deref() == Some(&latest_key) {
            continue;
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
                let mut guard = state.model_state.lock().await;
                *guard = Some(new_model_state);
                info!(artifact_key = latest_key, "Model hot-swapped");
            }
            Err(e) => {
                error!(error = %e, "Failed to load new model artifact");
            }
        }
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
