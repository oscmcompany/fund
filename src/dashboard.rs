//! The dashboard: a read-only page describing what the fund is doing.
//!
//! Runs as its own process alongside the service, connecting as `dashboard_reader` — a role holding
//! SELECT on five tables and nothing else, so a bug here cannot write to the database the strategy
//! trades from.
//!
//! Two background tasks keep one shared state current: a poller that refreshes every view on a
//! fixed interval, and a listener that appends events from the `events` NOTIFY channel as they
//! happen. Request handlers read that state and never query, so the number of viewers has no
//! bearing on the connection pool.

pub mod cache;
pub mod database;
pub mod html;
pub mod server;

use std::sync::Arc;

use sqlx::postgres::PgPoolOptions;
use tokio::sync::RwLock;
use tracing::info;

use crate::common::observability::init_tracing_file_only;

/// Connections the dashboard pool may open: one for the poller, one for the listener, and headroom
/// for the overlap while a dropped listener reconnects.
const POOL_MAX_CONNECTIONS: u32 = 4;

/// Connects, spawns the background tasks, and serves until the process ends.
///
/// Panics on a missing `DATABASE_URL` or an unreachable database. Both are configuration errors
/// that no amount of retrying fixes, and a dashboard that starts without a database would serve an
/// empty page indistinguishable from a flat one.
pub async fn run() {
    let _tracing_guard = init_tracing_file_only("dashboard-service.log", "dashboard");
    info!("Starting dashboard service");

    let database_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| panic!("DATABASE_URL must be set for the dashboard"));

    let pool = PgPoolOptions::new()
        .max_connections(POOL_MAX_CONNECTIONS)
        .connect(&database_url)
        .await
        .unwrap_or_else(|error| panic!("Failed to connect to the database: {error}"));

    let state = Arc::new(RwLock::new(cache::DashboardState::default()));
    cache::spawn_polling_task(state.clone(), pool.clone());
    cache::spawn_event_listener_task(state.clone(), pool);

    server::run_server(state).await;
}
