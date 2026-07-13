//! Dashboard service: SSH-accessible read-only TUI displaying live fund data.
//!
//! Entry point is [`run`], which connects to the read-only production Postgres
//! instance, spawns the background polling and event-listener tasks, then
//! hands off to the ratatui terminal event loop.
//!
//! Data flow:
//! - A single [`cache::spawn_polling_task`] refreshes all static view data
//!   every 30 seconds behind an `Arc<RwLock<DashboardState>>`.
//! - A [`cache::spawn_event_listener_task`] subscribes to the Postgres `events`
//!   NOTIFY channel and appends real-time events to the ring buffer.
//! - All SSH sessions (ratatui render loops) read from the shared cache without
//!   touching the database directly, so viewer count does not affect Postgres
//!   connection load.

pub mod application;
pub mod cache;
pub mod database;
pub mod events;
pub mod performance;
pub mod positions;
pub mod predictions;
pub mod trades;

use sqlx::postgres::PgPoolOptions;
use tracing::info;

use crate::common::observability::init_tracing_file_only;

/// Maximum number of Postgres connections the dashboard pool may open.
///
/// One connection is used by the polling task and one by the LISTEN/NOTIFY
/// event listener. A small additional allowance covers startup overlap.
const POOL_MAX_CONNECTIONS: u32 = 4;

/// Initializes tracing, connects to the read-only database, spawns background
/// tasks, and runs the ratatui terminal event loop.
///
/// Panics on startup if `DATABASE_URL` is unset or the database is unreachable.
pub async fn run() {
    let _tracing_guard = init_tracing_file_only("dashboard-service.log", "dashboard");
    info!("Starting dashboard service");

    let database_url =
        std::env::var("DATABASE_URL").unwrap_or_else(|_| panic!("DATABASE_URL must be set"));

    let pool = PgPoolOptions::new()
        .max_connections(POOL_MAX_CONNECTIONS)
        .connect(&database_url)
        .await
        .unwrap_or_else(|error| panic!("Failed to connect to database: {error}"));

    let state = std::sync::Arc::new(tokio::sync::RwLock::new(cache::DashboardState::default()));

    cache::spawn_polling_task(state.clone(), pool.clone());
    cache::spawn_event_listener_task(state.clone(), pool);

    application::run_event_loop(state).await;
}
