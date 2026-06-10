//! Postgres event consumer for the ensemble service.
//!
//! Listens on the `events` channel and runs the prediction pipeline whenever a
//! `predictions_requested` event arrives, mirroring the data_manager LISTEN
//! loop (`src/data_manager/scheduler.rs`). This is what wires the Rust ensemble
//! service into the event system, replacing the former Python consumer.

use std::time::Duration;

use sqlx::postgres::PgListener;
use sqlx::PgPool;
use tokio::time::sleep;
use tracing::{error, info, warn};

use crate::ensemble_manager::database;
use crate::ensemble_manager::server::run_predictions;
use crate::ensemble_manager::state::AppState;

/// Consumer name for offset tracking. Reuses the prior Python consumer's name so
/// it continues from the same `event_consumer_offsets` position.
const CONSUMER_NAME: &str = "ensemble-manager";
const PREDICTIONS_REQUESTED: &str = "predictions_requested";

/// Spawn the event consumer if a database pool is configured.
pub fn spawn_event_consumer(state: AppState) {
    if state.pool.is_none() {
        info!("PostgreSQL not available, event consumer disabled");
        return;
    }
    tokio::spawn(consumer_loop(state));
}

/// Supervisor: restart the listener on error with a backoff (matches the
/// data_manager listen loop).
async fn consumer_loop(state: AppState) {
    let pool = match &state.pool {
        Some(pool) => pool.clone(),
        None => return,
    };

    loop {
        match run_consumer(&state, &pool).await {
            Ok(()) => info!("Event consumer exited, restarting"),
            Err(error) => {
                warn!("Event consumer error: {}, restarting in 30s", error);
                sleep(Duration::from_secs(30)).await;
            }
        }
    }
}

async fn run_consumer(state: &AppState, pool: &PgPool) -> Result<(), sqlx::Error> {
    let mut listener = PgListener::connect_with(pool).await?;
    listener.listen("events").await?;
    info!("Event consumer connected, listening on channel 'events'");

    // Catch up on a predictions_requested that arrived while we were down.
    let offset = database::get_consumer_offset(pool, CONSUMER_NAME).await?;
    if let Some(event_id) =
        database::latest_event_after(pool, PREDICTIONS_REQUESTED, offset).await?
    {
        info!(event_id, "Catching up on missed predictions_requested");
        handle_predictions_requested(state, pool, event_id).await;
    }

    loop {
        let notification = listener.recv().await?;
        let parsed: serde_json::Value = match serde_json::from_str(notification.payload()) {
            Ok(value) => value,
            Err(_) => continue,
        };

        if parsed.get("event_type").and_then(|value| value.as_str()) != Some(PREDICTIONS_REQUESTED)
        {
            continue;
        }

        let event_id = parsed
            .get("event_id")
            .and_then(|value| value.as_i64())
            .unwrap_or(0);
        info!(event_id, "Received predictions_requested");
        handle_predictions_requested(state, pool, event_id).await;
    }
}

/// Run a prediction pass and advance the consumer offset. `run_predictions`
/// already persists results and emits `predictions_completed`/`predictions_failed`,
/// so this only logs and records progress.
async fn handle_predictions_requested(state: &AppState, pool: &PgPool, event_id: i64) {
    match run_predictions(state).await {
        Ok(run) => info!(rows = run.row_count, "Predictions generated from event"),
        Err(error) => {
            error!(stage = error.stage, error = %error.message, "Prediction run failed")
        }
    }

    if let Err(error) = database::update_consumer_offset(pool, CONSUMER_NAME, event_id).await {
        warn!(error = %error, "Failed to update consumer offset");
    }
}
