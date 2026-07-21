//! Postgres event consumer for the inference service.
//!
//! Listens on the `events` channel and runs the prediction pipeline whenever an
//! `equity_predictions_requested` event arrives. Mirrors the data LISTEN
//! loop (`src/data/scheduler.rs`). This wires the Rust inference service
//! into the event system, replacing the former Python consumer.

use std::time::Duration;

use sqlx::postgres::PgListener;
use sqlx::PgPool;
use tokio::task::JoinHandle;
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use crate::common::events::{
    emit_event, get_consumer_offset, latest_event_after, update_consumer_offset, EventType,
    CONSUMER_INFERENCE,
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
    let mut listener = PgListener::connect_with(pool).await?;
    listener.listen("events").await?;
    info!("Event consumer connected, listening on channel 'events'");

    if shutdown_token.is_cancelled() {
        return Ok(());
    }

    // Catch up on an equity_predictions_requested that arrived while we were down.
    let offset = get_consumer_offset(pool, CONSUMER_INFERENCE).await?;
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
        let notification = tokio::select! {
            result = listener.recv() => result?,
            _ = shutdown_token.cancelled() => {
                info!("Shutdown signal received, draining");
                break;
            }
        };
        let payload = notification.payload();

        if parse_event_type(payload).as_deref()
            != Some(EventType::EquityPredictionsRequested.as_str())
        {
            continue;
        }

        let event_id = parse_event_id(payload);
        if event_id == 0 {
            warn!("Skipping equity_predictions_requested with invalid event_id");
            continue;
        }
        info!(event_id, "Received equity_predictions_requested");
        handle_equity_predictions_requested(state, pool, event_id).await;
    }

    Ok(())
}

/// Parse an event notification payload and return the event type string if
/// present. Returns `None` when the payload is not valid JSON or does not
/// carry an `event_type` field.
pub(crate) fn parse_event_type(payload: &str) -> Option<String> {
    let parsed: serde_json::Value = serde_json::from_str(payload).ok()?;
    parsed
        .get("event_type")
        .and_then(|value| value.as_str())
        .map(String::from)
}

/// Extract the `event_id` integer from a notification payload, returning 0
/// when the field is absent or not an integer.
pub(crate) fn parse_event_id(payload: &str) -> i64 {
    serde_json::from_str::<serde_json::Value>(payload)
        .ok()
        .and_then(|parsed| parsed.get("event_id").and_then(|value| value.as_i64()))
        .unwrap_or(0)
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

    if let Err(error) = update_consumer_offset(pool, CONSUMER_INFERENCE, event_id).await {
        warn!(error = %error, "Failed to update consumer offset");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::events::EventType;

    #[test]
    fn test_parse_event_type_equity_predictions_requested() {
        let payload = serde_json::json!({
            "event_type": EventType::EquityPredictionsRequested.as_str(),
            "event_id": 42,
        })
        .to_string();
        let result = parse_event_type(&payload);
        assert_eq!(result.as_deref(), Some("equity_predictions_requested"));
    }

    #[test]
    fn test_parse_event_type_other_event() {
        let payload = serde_json::json!({
            "event_type": "equity_bars_sync_completed",
            "event_id": 7,
        })
        .to_string();
        let result = parse_event_type(&payload);
        assert_eq!(result.as_deref(), Some("equity_bars_sync_completed"));
    }

    #[test]
    fn test_parse_event_type_missing_field() {
        let payload = serde_json::json!({"event_id": 1}).to_string();
        assert!(parse_event_type(&payload).is_none());
    }

    #[test]
    fn test_parse_event_type_invalid_json() {
        assert!(parse_event_type("not-json").is_none());
        assert!(parse_event_type("").is_none());
        assert!(parse_event_type("{unclosed").is_none());
    }

    #[test]
    fn test_parse_event_type_non_string_value() {
        // event_type with a numeric value must return None (not a string).
        let payload = serde_json::json!({"event_type": 99}).to_string();
        assert!(parse_event_type(&payload).is_none());
    }

    #[test]
    fn test_parse_event_id_present() {
        let payload = serde_json::json!({
            "event_type": "equity_predictions_requested",
            "event_id": 123,
        })
        .to_string();
        assert_eq!(parse_event_id(&payload), 123);
    }

    #[test]
    fn test_parse_event_id_missing_defaults_to_zero() {
        let payload = serde_json::json!({"event_type": "equity_predictions_requested"}).to_string();
        assert_eq!(parse_event_id(&payload), 0);
    }

    #[test]
    fn test_parse_event_id_invalid_json_defaults_to_zero() {
        assert_eq!(parse_event_id("bad json"), 0);
    }

    #[test]
    fn test_parse_event_id_non_integer_defaults_to_zero() {
        let payload = serde_json::json!({"event_id": "not-a-number"}).to_string();
        assert_eq!(parse_event_id(&payload), 0);
    }

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
