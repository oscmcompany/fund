//! Axum HTTP server for the portfolio_manager service.
//!
//! Routes:
//! - `GET /health` — liveness probe; returns `200 OK` with `{"status": "ok"}`
//! - `POST /rebalance` — triggers a rebalance cycle immediately

use axum::extract::Extension;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Json, Response};
use axum::routing::{get, post};
use axum::Router;
use serde_json::json;
use tracing::{info, warn};

use crate::portfolio_manager::rebalance::{run_rebalance, RebalanceError};
use crate::portfolio_manager::state::AppState;

/// Liveness probe returning `200 OK`.
pub async fn health() -> impl IntoResponse {
    Json(json!({"status": "ok"}))
}

/// Triggers an immediate rebalance cycle.
///
/// Returns `200 OK` with outcome fields on success, or an appropriate HTTP
/// error status with a JSON body describing the failure reason.
pub async fn rebalance(Extension(state): Extension<AppState>) -> Response {
    info!("Rebalance requested");

    match run_rebalance(&state).await {
        Ok(outcome) => {
            info!(
                session_id = %outcome.session_id,
                pairs_opened = outcome.pairs_opened,
                pairs_closed = outcome.pairs_closed,
                pairs_kept = outcome.pairs_kept,
                "Rebalance handler completed successfully"
            );
            (
                StatusCode::OK,
                Json(json!({
                    "session_id": outcome.session_id.to_string(),
                    "pairs_opened": outcome.pairs_opened,
                    "pairs_closed": outcome.pairs_closed,
                    "pairs_kept": outcome.pairs_kept,
                    "net_asset_value": outcome.net_asset_value,
                })),
            )
                .into_response()
        }
        Err(RebalanceError::StalePredictions) => {
            warn!("Rebalance skipped: stale or absent predictions");
            (
                StatusCode::OK,
                Json(json!({"status": "skipped", "reason": "Predictions are absent or stale."})),
            )
                .into_response()
        }
        Err(RebalanceError::TrendingRegime) => {
            info!("Rebalance skipped: trending regime");
            (
                StatusCode::OK,
                Json(json!({"skipped": true, "reason": "trending_regime"})),
            )
                .into_response()
        }
        Err(RebalanceError::DrawdownBreached { current, threshold }) => {
            warn!(
                current = current,
                threshold = threshold,
                "Rebalance halted: drawdown threshold breached"
            );
            (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({
                    "error": "Drawdown threshold breached",
                    "current": current,
                    "threshold": threshold,
                })),
            )
                .into_response()
        }
        Err(RebalanceError::InsufficientPairs(error)) => {
            warn!(error = %error, "Rebalance failed: insufficient pairs");
            (
                StatusCode::UNPROCESSABLE_ENTITY,
                Json(json!({"error": format!("{error}")})),
            )
                .into_response()
        }
        Err(RebalanceError::PortfolioInvalid(error)) => {
            warn!(error = %error, "Rebalance failed: portfolio validation");
            (
                StatusCode::UNPROCESSABLE_ENTITY,
                Json(json!({"error": format!("{error}")})),
            )
                .into_response()
        }
        Err(RebalanceError::Database(error)) => {
            warn!(error = %error, "Rebalance failed: database error");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "Internal database error"})),
            )
                .into_response()
        }
        Err(RebalanceError::Execution(error)) => {
            warn!(error = %error, "Rebalance failed: execution error");
            (
                StatusCode::BAD_GATEWAY,
                Json(json!({"error": format!("{error}")})),
            )
                .into_response()
        }
        Err(RebalanceError::Conversion(message)) => {
            warn!(message = %message, "Rebalance failed: numeric conversion error");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "Internal numeric conversion error"})),
            )
                .into_response()
        }
    }
}

/// Builds the Axum router with all routes and the shared `AppState` extension.
pub fn create_router(state: AppState) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/rebalance", post(rebalance))
        .layer(axum::extract::Extension(state))
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::Request;
    use tower::ServiceExt;

    #[tokio::test]
    async fn test_health_returns_200() {
        let app = Router::new().route("/health", get(health));
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/health")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_health_response_body_contains_ok() {
        let app = Router::new().route("/health", get(health));
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/health")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["status"], "ok");
    }

    #[tokio::test]
    async fn test_health_endpoint_returns_json_content_type() {
        let app = Router::new().route("/health", get(health));
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/health")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        let content_type = response
            .headers()
            .get(axum::http::header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .unwrap_or("");
        assert!(content_type.contains("application/json"));
    }

    #[tokio::test]
    async fn test_unknown_route_returns_404() {
        let app = Router::new().route("/health", get(health));
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/unknown-route")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_health_returns_only_status_field() {
        // Verifies that the health response body contains exactly the "status"
        // key with value "ok" and no other top-level fields are expected absent.
        let app = Router::new().route("/health", get(health));
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/health")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert!(json.is_object());
        assert_eq!(json["status"], "ok");
        assert_eq!(json.as_object().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn test_health_allows_get_method() {
        let app = Router::new().route("/health", get(health));
        let response = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/health")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_health_post_returns_method_not_allowed() {
        let app = Router::new().route("/health", get(health));
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/health")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::METHOD_NOT_ALLOWED);
    }
}
