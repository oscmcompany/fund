//! Postgres event consumer for the ensemble service.
//!
//! Listens on the `events` channel and runs the prediction pipeline whenever an
//! `equity_predictions_requested` event arrives. Mirrors the data_manager LISTEN
//! loop (`src/data_manager/scheduler.rs`). This wires the Rust ensemble service
//! into the event system, replacing the former Python consumer.

use std::time::Duration;

use sqlx::postgres::PgListener;
use sqlx::PgPool;
use tokio::time::sleep;
use tracing::{error, info, warn};

use crate::common::events::{
    emit_event, get_consumer_offset, latest_event_after, update_consumer_offset, EventType,
    CONSUMER_ENSEMBLE_MANAGER,
};
use crate::ensemble_manager::server::run_predictions;
use crate::ensemble_manager::state::AppState;

/// Spawn the event consumer if a database pool is configured.
pub fn spawn_event_consumer(state: AppState) {
    if state.pool().is_none() {
        info!("PostgreSQL not available, event consumer disabled");
        return;
    }
    tokio::spawn(consumer_loop(state));
}

/// Supervisor: restart the listener on error with a backoff.
async fn consumer_loop(state: AppState) {
    let pool = match state.pool() {
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

    // Catch up on an equity_predictions_requested that arrived while we were down.
    let offset = get_consumer_offset(pool, CONSUMER_ENSEMBLE_MANAGER).await?;
    if let Some(event_id) =
        latest_event_after(pool, EventType::EquityPredictionsRequested, offset).await?
    {
        info!(
            event_id,
            "Catching up on missed equity_predictions_requested"
        );
        handle_equity_predictions_requested(state, pool, event_id).await;
    }

    loop {
        let notification = listener.recv().await?;
        let parsed: serde_json::Value = match serde_json::from_str(notification.payload()) {
            Ok(value) => value,
            Err(_) => continue,
        };

        if parsed.get("event_type").and_then(|value| value.as_str())
            != Some(EventType::EquityPredictionsRequested.as_str())
        {
            continue;
        }

        let event_id = parsed
            .get("event_id")
            .and_then(|value| value.as_i64())
            .unwrap_or(0);
        info!(event_id, "Received equity_predictions_requested");
        handle_equity_predictions_requested(state, pool, event_id).await;
    }
}

/// Run a prediction pass and advance the consumer offset.
///
/// Emits `equity_predictions_started` before running, then
/// `equity_predictions_completed` on success or `equity_predictions_errored`
/// on failure. `run_predictions` persists results and emits those terminal
/// events, so this function only handles offset bookkeeping.
async fn handle_equity_predictions_requested(state: &AppState, pool: &PgPool, event_id: i64) {
    if let Err(error) = emit_event(
        pool,
        EventType::EquityPredictionsStarted,
        &serde_json::json!({}),
    )
    .await
    {
        warn!(error = %error, "Failed to emit equity_predictions_started");
    }

    match run_predictions(state).await {
        Ok(run) => info!(rows = run.row_count(), "Predictions generated from event"),
        Err(error) => {
            error!(stage = error.stage(), error = %error.message(), "Prediction run failed")
        }
    }

    if let Err(error) = update_consumer_offset(pool, CONSUMER_ENSEMBLE_MANAGER, event_id).await {
        warn!(error = %error, "Failed to update consumer offset");
    }
}
