//! PostgreSQL event consumer for the portfolio_manager service.
//!
//! Listens on the `events` channel and runs a rebalance cycle whenever a
//! `predictions_completed` event arrives, mirroring the ensemble_manager
//! consumer pattern.

use std::time::Duration;

use sqlx::postgres::PgListener;
use sqlx::PgPool;
use tokio::time::sleep;
use tracing::{error, info, warn};

use crate::portfolio_manager::database;
use crate::portfolio_manager::rebalance::{
    run_end_of_day_liquidation, run_rebalance, RebalanceError,
};
use crate::portfolio_manager::state::AppState;

const CONSUMER_NAME: &str = "portfolio-manager";
const PREDICTIONS_COMPLETED: &str = "predictions_completed";
const END_OF_DAY_LIQUIDATION_REQUESTED: &str = "end_of_day_liquidation_requested";

/// Spawns the event consumer as a background task.
pub fn spawn_event_consumer(state: AppState) {
    tokio::spawn(consumer_loop(state));
}

async fn consumer_loop(state: AppState) {
    let pool = state.pool().clone();
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

    // Catch up on a predictions_completed that arrived while we were down.
    let offset = database::get_consumer_offset(pool, CONSUMER_NAME).await?;
    if let Some(event_id) =
        database::latest_event_after(pool, PREDICTIONS_COMPLETED, offset).await?
    {
        info!(event_id, "Catching up on missed predictions_completed");
        handle_predictions_completed(state, pool, event_id).await;
    }

    loop {
        let notification = listener.recv().await?;
        let parsed: serde_json::Value = match serde_json::from_str(notification.payload()) {
            Ok(value) => value,
            Err(_) => continue,
        };

        let event_type = parsed
            .get("event_type")
            .and_then(|value| value.as_str())
            .unwrap_or("");
        let event_id = parsed
            .get("event_id")
            .and_then(|value| value.as_i64())
            .unwrap_or(0);

        match event_type {
            PREDICTIONS_COMPLETED => {
                info!(event_id, "Received predictions_completed");
                handle_predictions_completed(state, pool, event_id).await;
            }
            END_OF_DAY_LIQUIDATION_REQUESTED => {
                info!(event_id, "Received end_of_day_liquidation_requested");
                handle_end_of_day_liquidation(state).await;
            }
            _ => {}
        }
    }
}

async fn handle_predictions_completed(state: &AppState, pool: &PgPool, event_id: i64) {
    match run_rebalance(state).await {
        Ok(outcome) => info!(
            session_id = %outcome.session_id,
            pairs_filled = outcome.pairs_filled,
            "Rebalance completed from event"
        ),
        Err(RebalanceError::StalePredictions) => {
            warn!("Rebalance skipped: stale or absent predictions");
        }
        Err(RebalanceError::TrendingRegime) => {
            info!("Rebalance skipped: trending regime");
        }
        Err(RebalanceError::DrawdownBreached { current, threshold }) => {
            warn!(
                current = current,
                threshold = threshold,
                "Rebalance halted: drawdown threshold breached"
            );
        }
        Err(error) => {
            error!(error = %error, "Rebalance failed");
        }
    }

    if let Err(error) = database::update_consumer_offset(pool, CONSUMER_NAME, event_id).await {
        warn!(error = %error, "Failed to update consumer offset");
    }
}

async fn handle_end_of_day_liquidation(state: &AppState) {
    match run_end_of_day_liquidation(state).await {
        Ok(pairs_closed) => info!(pairs_closed, "End-of-day liquidation completed"),
        Err(RebalanceError::Execution(error)) => {
            error!(error = %error, "End-of-day liquidation failed: Alpaca execution error");
        }
        Err(error) => {
            error!(error = %error, "End-of-day liquidation failed");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{CONSUMER_NAME, END_OF_DAY_LIQUIDATION_REQUESTED, PREDICTIONS_COMPLETED};

    #[test]
    fn test_consumer_name_is_stable() {
        assert_eq!(CONSUMER_NAME, "portfolio-manager");
    }

    #[test]
    fn test_predictions_completed_event_name_is_stable() {
        assert_eq!(PREDICTIONS_COMPLETED, "predictions_completed");
    }

    #[test]
    fn test_end_of_day_liquidation_requested_event_name_is_stable() {
        assert_eq!(
            END_OF_DAY_LIQUIDATION_REQUESTED,
            "end_of_day_liquidation_requested"
        );
    }
}
