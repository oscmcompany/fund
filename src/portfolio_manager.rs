//! Portfolio manager service: selects statistical arbitrage pairs from ensemble
//! predictions, sizes positions with volatility parity, and executes rebalance
//! cycles against the Alpaca trading API.

pub mod alpaca;
pub mod beta;
pub mod consolidation;
pub mod consumer;
pub mod database;
pub mod execution;
pub mod math;
pub mod rebalance;
pub mod regime;
pub mod server;
pub mod sizing;
pub mod state;
pub mod statistical_arbitrage;

use tracing::info;

use crate::common::observability::init_tracing;
use crate::common::server::serve;
use state::AppState;

/// Initialize tracing and run the portfolio manager HTTP server.
///
/// The server listens on `bind_address` and blocks until it exits.
/// Exits with a panic if the bind address is unavailable or configuration
/// is missing from the environment.
pub async fn run(bind_address: &str) {
    let _tracing_guard = init_tracing("portfolio-manager.log", None);

    info!("Starting portfolio manager service");

    let state = AppState::from_env()
        .await
        .unwrap_or_else(|error| panic!("Failed to initialize app state: {error}"));

    consumer::spawn_event_consumer(state.clone());

    let app = server::create_router(state);

    let listener = tokio::net::TcpListener::bind(bind_address)
        .await
        .unwrap_or_else(|error| panic!("Failed to bind to {bind_address}: {error}"));

    info!("Listening on {bind_address}");
    serve(listener, app).await.expect("Server failed");
}
