//! Ensemble model service: loads trained TiDE artifacts from S3 and consumes
//! `predictions_requested` events from the Postgres event bus.

pub mod artifact;
pub mod consumer;
pub mod database;
pub mod predict;
pub mod server;
pub mod state;

use tracing::info;

use crate::common::observability::init_tracing;
use crate::common::server::serve;
use state::AppState;

/// Initialize tracing and run the prediction HTTP server, blocking until it exits.
pub async fn run(bind_address: &str) {
    let _tracing_guard = init_tracing("ensemble-manager.log", None);

    info!("Starting ensemble model service");

    let state = AppState::from_env().await;

    let app = server::create_router(state.clone());

    // Load the current model before anything can ask for predictions: the
    // Python service loaded synchronously at startup, and the event consumer's
    // catch-up would otherwise consume a pending predictions_requested with no
    // model loaded. Failure here is non-fatal; the polling loop keeps retrying.
    server::poll_artifact_once(&state).await;

    tokio::spawn(server::start_artifact_polling(state.clone()));
    consumer::spawn_event_consumer(state);

    let listener = tokio::net::TcpListener::bind(bind_address)
        .await
        .unwrap_or_else(|error| panic!("Failed to bind to {bind_address}: {error}"));

    info!("Listening on {bind_address}");
    serve(listener, app).await.expect("Server failed");
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_s3_client() -> aws_sdk_s3::Client {
        let config = aws_sdk_s3::Config::builder()
            .behavior_version(aws_sdk_s3::config::BehaviorVersion::latest())
            .region(aws_sdk_s3::config::Region::new("us-east-1"))
            .build();
        aws_sdk_s3::Client::from_conf(config)
    }

    #[tokio::test]
    async fn test_create_router_health_route_exists() {
        use axum::body::Body;
        use axum::http::{Request, StatusCode};
        use tower::ServiceExt;

        let state = AppState::for_tests(
            make_s3_client(),
            "test-bucket".to_string(),
            "artifacts/tide/".to_string(),
            "latest".to_string(),
        );
        let app = server::create_router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/health")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        // Without a loaded model the health check returns SERVICE_UNAVAILABLE,
        // confirming the route is registered and reachable.
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    }
}
