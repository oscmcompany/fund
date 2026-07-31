//! Postgres event consumer for the inference service.
//!
//! Listens on the `events` channel and runs the prediction pipeline whenever an
//! `equity_predictions_requested` event arrives. Mirrors the data LISTEN
//! loop (`src/data/scheduler.rs`). This wires the Rust inference service
//! into the event system, replacing the former Python consumer.

use std::time::Duration;

use sqlx::PgPool;
use tokio::task::JoinHandle;
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use crate::common::events::{
    emit_event, get_consumer_offset, latest_event_after, run_event_listener,
    update_consumer_offset, EventType, Outcome, CONSUMER_INFERENCE,
};
use crate::inference::pipeline::run_predictions;
use crate::inference::state::AppState;

/// Spawn the event consumer if a database pool is configured.
///
/// Returns join handles for the spawned tasks, or an empty vec if no pool
/// is available. Callers must cancel the `shutdown_token` and then await the
/// returned handles for graceful shutdown.
pub fn spawn_event_consumer(
    state: AppState,
    shutdown_token: CancellationToken,
) -> Vec<JoinHandle<()>> {
    if state.pool().is_none() {
        info!("PostgreSQL not available, event consumer disabled");
        return Vec::new();
    }
    vec![tokio::spawn(consumer_loop(state, shutdown_token))]
}

/// Supervisor: restart the listener on error with a backoff.
async fn consumer_loop(state: AppState, shutdown_token: CancellationToken) {
    let pool = match state.pool() {
        Some(pool) => pool.clone(),
        None => return,
    };

    loop {
        match run_consumer(&state, &pool, &shutdown_token).await {
            Ok(()) => {
                if shutdown_token.is_cancelled() {
                    info!("Event consumer stopped for shutdown");
                    break;
                }
                info!("Event consumer exited, restarting");
            }
            Err(error) => {
                if shutdown_token.is_cancelled() {
                    info!("Event consumer stopped for shutdown");
                    break;
                }
                warn!("Event consumer error: {}, restarting in 30s", error);
                tokio::select! {
                    _ = sleep(Duration::from_secs(30)) => {}
                    _ = shutdown_token.cancelled() => {
                        info!("Event consumer stopped for shutdown");
                        break;
                    }
                }
            }
        }
    }
}

async fn run_consumer(
    state: &AppState,
    pool: &PgPool,
    shutdown_token: &CancellationToken,
) -> Result<(), sqlx::Error> {
    run_event_listener(
        pool,
        shutdown_token,
        CONSUMER_INFERENCE,
        || async {
            // Catch up on an equity_predictions_requested that arrived while we
            // were down.
            let offset = get_consumer_offset(pool, CONSUMER_INFERENCE).await?;
            if let Some(event_id) = latest_event_after(
                pool,
                EventType::EquityPredictions(Outcome::Requested),
                offset,
            )
            .await?
            {
                info!(
                    event_id,
                    "Catching up on missed equity_predictions_requested"
                );
                handle_equity_predictions_requested(state, pool, event_id).await;
            }
            Ok(())
        },
        |notification| async move {
            let event_id = notification.event_id();
            match notification.event_type() {
                EventType::EquityPredictions(Outcome::Requested) => {
                    info!(event_id, "Received equity_predictions_requested");
                    handle_equity_predictions_requested(state, pool, event_id).await;
                }
                // Every consumer receives every event, so the catch-all is
                // mostly other services' traffic. Inference acts on nothing
                // else; the match exists so a new variant is a compile-time
                // decision rather than a silent omission.
                _ => {}
            }
        },
    )
    .await
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
        EventType::EquityPredictions(Outcome::Started),
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

    if let Err(error) = update_consumer_offset(pool, CONSUMER_INFERENCE, event_id).await {
        warn!(error = %error, "Failed to update consumer offset");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_spawn_event_consumer_no_pool_does_not_panic() {
        // When no pool is configured, spawn_event_consumer must return
        // immediately without spawning a task and without panicking.
        let s3_client = {
            let config = aws_sdk_s3::Config::builder()
                .behavior_version(aws_sdk_s3::config::BehaviorVersion::latest())
                .region(aws_sdk_s3::config::Region::new("us-east-1"))
                .build();
            aws_sdk_s3::Client::from_conf(config)
        };
        let state = AppState::for_tests(
            s3_client,
            "bucket".to_string(),
            "prefix/".to_string(),
            "latest".to_string(),
        );
        let token = CancellationToken::new();
        // No pool configured; the function must log and return without spawning.
        let handles = spawn_event_consumer(state, token);
        assert!(handles.is_empty());
    }
}
