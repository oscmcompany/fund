//! Integration tests for the ensemble service's event-system SQL: consumer
//! offsets, event catch-up lookup, model_runs lineage upsert, and prediction
//! inserts.
//!
//! Run against the devenv-managed Postgres, which persists between runs. Tests
//! must therefore be idempotent rather than assuming empty tables.

mod common;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use fund::common::events::{
    emit_event, get_consumer_offset, latest_event_after, run_event_listener,
    update_consumer_offset, EventNotification, EventType, Outcome,
};
use fund::inference::database::{insert_predictions, upsert_model_run, ModelRunRecord};
use serial_test::serial;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

#[tokio::test]
#[serial]
async fn test_consumer_offset_round_trip() {
    let pool = common::get_pg_pool().await;
    // Unique per run. The test database persists between runs now that it is
    // the devenv Postgres rather than a fresh container, so a fixed consumer
    // name carried its offset forward and the "starts at zero" assertion only
    // held the first time.
    let consumer = format!("ensemble-offset-test-{}", Uuid::new_v4());
    let consumer = consumer.as_str();

    assert_eq!(get_consumer_offset(&pool, consumer).await.unwrap(), 0);

    update_consumer_offset(&pool, consumer, 5).await.unwrap();
    assert_eq!(get_consumer_offset(&pool, consumer).await.unwrap(), 5);

    // GREATEST guards against moving the offset backwards.
    update_consumer_offset(&pool, consumer, 3).await.unwrap();
    assert_eq!(get_consumer_offset(&pool, consumer).await.unwrap(), 5);

    update_consumer_offset(&pool, consumer, 9).await.unwrap();
    assert_eq!(get_consumer_offset(&pool, consumer).await.unwrap(), 9);
}

#[tokio::test]
#[serial]
async fn test_latest_event_after_matches_only_requested_type() {
    let pool = common::get_pg_pool().await;

    let before = latest_event_after(&pool, EventType::EquityPredictions(Outcome::Requested), 0)
        .await
        .unwrap()
        .unwrap_or(0);

    emit_event(
        &pool,
        EventType::EquityPredictions(Outcome::Requested),
        &serde_json::json!({}),
    )
    .await
    .unwrap();

    let found = latest_event_after(
        &pool,
        EventType::EquityPredictions(Outcome::Requested),
        before,
    )
    .await
    .unwrap();
    assert!(found.is_some());
    let requested_id = found.unwrap();
    assert!(requested_id > before);

    // A later event of a different type must not be returned.
    emit_event(
        &pool,
        EventType::PortfolioEvaluationRequested,
        &serde_json::json!({}),
    )
    .await
    .unwrap();
    assert_eq!(
        latest_event_after(
            &pool,
            EventType::EquityPredictions(Outcome::Requested),
            requested_id
        )
        .await
        .unwrap(),
        None
    );
}

#[tokio::test]
#[serial]
async fn test_upsert_model_run_inserts_then_updates() {
    let pool = common::get_pg_pool().await;

    let metadata = serde_json::json!({
        "lookback_days": 1200,
        "start_date": "2023-02-25",
        "end_date": "2026-06-09",
        "train_samples": 100,
        "validation_samples": 20,
        "metrics": {"crps": 0.0059, "directional_accuracy": 0.617, "quantile_coverage": 0.719},
        "drift": {"status": "no_drift", "message": "No drift detected", "baseline_crps": 0.0056, "prior_runs": 7}
    });
    let record = ModelRunRecord::from_metadata(
        "run-events-test",
        "models/tide/run-events-test/output/model.tar.gz",
        &metadata,
    );
    upsert_model_run(&pool, &record).await.unwrap();

    let (crps, lookback, status, drift_status): (Option<f64>, Option<i32>, String, Option<String>) =
        sqlx::query_as(
            "SELECT continuous_ranked_probability_score, lookback_days, status, drift_status \
             FROM model_runs WHERE run_id = $1",
        )
        .bind("run-events-test")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(status, "completed");
    assert_eq!(lookback, Some(1200));
    assert!((crps.unwrap() - 0.0059).abs() < 1e-9);
    assert_eq!(drift_status.as_deref(), Some("no_drift"));

    // Upsert again with the same run_id updates in place (no duplicate row).
    let updated = ModelRunRecord::from_metadata(
        "run-events-test",
        "models/tide/run-events-test/output/model.tar.gz",
        &serde_json::json!({"metrics": {"crps": 0.01}}),
    );
    upsert_model_run(&pool, &updated).await.unwrap();

    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM model_runs WHERE run_id = $1")
        .bind("run-events-test")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(count, 1);
}

#[tokio::test]
#[serial]
async fn test_insert_predictions_writes_rows() {
    let pool = common::get_pg_pool().await;

    // Five characters: `Ticker` allows a 1-5 letter base, so the former
    // eight-character "PREDTEST" fixture was rejected by the decoder. The file
    // was never executed, so the invalid fixture went unnoticed.
    let predictions = vec![serde_json::json!({
        "ticker": "PREDT",
        "timestamp": 1_735_689_600_000_i64,
        "quantile_10": -0.01,
        "quantile_50": 0.0,
        "quantile_90": 0.02,
    })];

    let rows = insert_predictions(&pool, &predictions, Uuid::new_v4(), "run-events-test")
        .await
        .unwrap();
    assert_eq!(rows, 1);

    let count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM equity_predictions WHERE ticker = 'PREDT'")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(count, 1);
}

// --- Shared LISTEN loop ---
//
// These exercise `run_event_listener` against the real NOTIFY channel, because
// the behaviour worth pinning down — that a notification reaches the handler,
// that catch-up runs per connection, and that cancellation is prompt without
// truncating in-flight work — only exists end to end.
//
// Every test tags its events with a unique marker in the payload and ignores
// anything else, since the channel is shared with whatever else is running.

/// How long a cancelled listener is given to return before the test fails.
///
/// Without this the four tests below would hang rather than fail if
/// `run_event_listener` stopped returning on cancellation, which turns a clear
/// regression into a silent CI timeout.
const LISTENER_EXIT_TIMEOUT: Duration = Duration::from_secs(5);

/// Emits a `stress_test` event carrying a unique marker.
async fn emit_marked_event(pool: &sqlx::PgPool, marker: &str) {
    emit_event(
        pool,
        EventType::StressTest,
        &serde_json::json!({"marker": marker}),
    )
    .await
    .unwrap();
}

/// Returns true when a notification carries the given marker.
fn has_marker(notification: &EventNotification, marker: &str) -> bool {
    notification
        .payload()
        .get("marker")
        .and_then(|value| value.as_str())
        == Some(marker)
}

#[tokio::test]
#[serial]
async fn test_run_event_listener_dispatches_notifications_to_the_handler() {
    let pool = common::get_pg_pool().await;
    let marker = Uuid::new_v4().to_string();
    let token = CancellationToken::new();
    let seen: Arc<Mutex<Vec<i64>>> = Arc::new(Mutex::new(Vec::new()));

    let listener = tokio::spawn({
        let (pool, token, seen, marker) =
            (pool.clone(), token.clone(), seen.clone(), marker.clone());
        async move {
            run_event_listener(
                &pool,
                &token,
                "test-dispatch",
                || async { Ok(()) },
                |notification| {
                    let seen = seen.clone();
                    let marker = marker.clone();
                    async move {
                        if has_marker(&notification, &marker) {
                            seen.lock().await.push(notification.event_id());
                        }
                    }
                },
            )
            .await
        }
    });

    // The listener needs to be subscribed before the event is emitted; NOTIFY
    // does not replay to a connection that was not listening at the time.
    tokio::time::sleep(Duration::from_millis(300)).await;
    emit_marked_event(&pool, &marker).await;

    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while seen.lock().await.is_empty() && tokio::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    token.cancel();
    tokio::time::timeout(LISTENER_EXIT_TIMEOUT, listener)
        .await
        .expect("listener did not exit after cancellation")
        .unwrap()
        .unwrap();

    let received = seen.lock().await;
    assert_eq!(
        received.len(),
        1,
        "handler should have received exactly the marked event"
    );
    assert!(received[0] > 0, "event_id should be a real row id");
}

#[tokio::test]
#[serial]
async fn test_run_event_listener_runs_catch_up_before_dispatching() {
    let pool = common::get_pg_pool().await;
    let token = CancellationToken::new();
    let catch_up_runs = Arc::new(AtomicUsize::new(0));

    let listener = tokio::spawn({
        let (pool, token, catch_up_runs) = (pool.clone(), token.clone(), catch_up_runs.clone());
        async move {
            run_event_listener(
                &pool,
                &token,
                "test-catch-up",
                || {
                    let catch_up_runs = catch_up_runs.clone();
                    async move {
                        catch_up_runs.fetch_add(1, Ordering::SeqCst);
                        Ok(())
                    }
                },
                |_| async {},
            )
            .await
        }
    });

    tokio::time::sleep(Duration::from_millis(300)).await;
    token.cancel();
    tokio::time::timeout(LISTENER_EXIT_TIMEOUT, listener)
        .await
        .expect("listener did not exit after cancellation")
        .unwrap()
        .unwrap();

    assert_eq!(
        catch_up_runs.load(Ordering::SeqCst),
        1,
        "catch-up should run exactly once per connection"
    );
}

#[tokio::test]
#[serial]
async fn test_run_event_listener_exits_promptly_when_cancelled_while_idle() {
    let pool = common::get_pg_pool().await;
    let token = CancellationToken::new();

    let listener = tokio::spawn({
        let (pool, token) = (pool.clone(), token.clone());
        async move {
            run_event_listener(
                &pool,
                &token,
                "test-idle-cancel",
                || async { Ok(()) },
                |_| async {},
            )
            .await
        }
    });

    tokio::time::sleep(Duration::from_millis(300)).await;
    let cancelled_at = tokio::time::Instant::now();
    token.cancel();
    tokio::time::timeout(LISTENER_EXIT_TIMEOUT, listener)
        .await
        .expect("listener did not exit after cancellation")
        .unwrap()
        .unwrap();

    assert!(
        cancelled_at.elapsed() < Duration::from_secs(2),
        "an idle listener should exit on cancellation without waiting for a notification"
    );
}

#[tokio::test]
#[serial]
async fn test_run_event_listener_finishes_an_in_flight_handler_before_exiting() {
    let pool = common::get_pg_pool().await;
    let marker = Uuid::new_v4().to_string();
    let token = CancellationToken::new();
    let handler_started = Arc::new(AtomicUsize::new(0));
    let handler_finished = Arc::new(AtomicUsize::new(0));

    let listener = tokio::spawn({
        let (pool, token, marker) = (pool.clone(), token.clone(), marker.clone());
        let (started, finished) = (handler_started.clone(), handler_finished.clone());
        async move {
            run_event_listener(
                &pool,
                &token,
                "test-in-flight",
                || async { Ok(()) },
                |notification| {
                    let (started, finished) = (started.clone(), finished.clone());
                    let marker = marker.clone();
                    async move {
                        if !has_marker(&notification, &marker) {
                            return;
                        }
                        started.fetch_add(1, Ordering::SeqCst);
                        tokio::time::sleep(Duration::from_millis(800)).await;
                        finished.fetch_add(1, Ordering::SeqCst);
                    }
                },
            )
            .await
        }
    });

    tokio::time::sleep(Duration::from_millis(300)).await;
    emit_marked_event(&pool, &marker).await;

    // Cancel while the handler is mid-sleep.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while handler_started.load(Ordering::SeqCst) == 0 && tokio::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(
        handler_started.load(Ordering::SeqCst),
        1,
        "handler should have started before cancellation"
    );
    token.cancel();

    tokio::time::timeout(LISTENER_EXIT_TIMEOUT, listener)
        .await
        .expect("listener did not exit after cancellation")
        .unwrap()
        .unwrap();
    assert_eq!(
        handler_finished.load(Ordering::SeqCst),
        1,
        "an in-flight handler must run to completion rather than being dropped"
    );
}
