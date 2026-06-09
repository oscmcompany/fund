//! Ensemble model service: loads trained TiDE artifacts from S3, serves
//! predictions over HTTP, and consumes `predictions_requested` events from the
//! Postgres event bus.

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
    let _tracing_guard = init_tracing("ensemble-model.log", None);

    info!("Starting ensemble model service");

    let state = AppState::from_env().await;

    let app = server::create_router(state.clone());

    tokio::spawn(server::start_artifact_polling(state.clone()));
    consumer::spawn_event_consumer(state);

    let listener = tokio::net::TcpListener::bind(bind_address)
        .await
        .unwrap_or_else(|error| panic!("Failed to bind to {bind_address}: {error}"));

    info!("Listening on {bind_address}");
    serve(listener, app).await.expect("Server failed");
}
