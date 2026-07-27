//! PostgreSQL event consumer for the portfolio service.
//!
//! Listens on the `events` channel and:
//! - Runs an evaluation pass on each `portfolio_evaluation_requested` event,
//!   which also triggers liquidation once the session close is near.
//! - Runs a rebalance cycle on each `equity_predictions_completed` event so a
//!   fresh prediction set is acted on immediately.
//! - Runs end-of-day liquidation on each `portfolio_liquidation_requested` event.
//!
//! Predictions are requested pre-market by pg_cron rather than on a recurring
//! intraday tick. They derive from daily bars, so a single run produces every
//! distinct value the session will have; re-running mid-session recomputed an
//! identical answer. The evaluation pass re-requests them only when a session
//! begins with none recorded.

use std::time::Duration;

use chrono::{DateTime, Utc};
use sqlx::postgres::PgListener;
use sqlx::PgPool;
use tokio::task::JoinHandle;
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use crate::common::events::{
    emit_event, get_consumer_offset, latest_event_after, update_consumer_offset, EventType,
    CONSUMER_PORTFOLIO, CONSUMER_PORTFOLIO_LIQUIDATION,
};
use crate::common::market_hours::MarketSession;
use crate::portfolio::database::predictions_exist_for_today;
use crate::portfolio::rebalance::{run_end_of_day_liquidation, run_rebalance, RebalanceError};
use crate::portfolio::reconciliation;
use crate::portfolio::state::AppState;

/// How close to the session close the liquidation request is emitted.
///
/// Derived from Alpaca's reported close rather than a fixed wall-clock time, so
/// an early close pulls liquidation forward automatically. The pg_cron job at
/// 15:45 Eastern remains as a fail-safe for the case where the clock endpoint is
/// unreachable; liquidation is idempotent, so both firing is harmless.
const LIQUIDATION_LEAD_TIME_MINUTES: i64 = 15;

/// Spawns the event consumer as a background task.
pub fn spawn_event_consumer(state: AppState, shutdown_token: CancellationToken) -> JoinHandle<()> {
    tokio::spawn(consumer_loop(state, shutdown_token))
}

async fn consumer_loop(state: AppState, shutdown_token: CancellationToken) {
    let pool = state.pool().clone();
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

    // Run startup reconciliation to resolve any DB-Alpaca drift accumulated
    // while the service was down.
    match reconciliation::reconcile(pool, state.alpaca_client()).await {
        Ok(report) => {
            info!(
                orphans_closed = report.orphans_closed,
                pairs_marked_closed = report.pairs_marked_closed,
                stale_orders_resolved = report.stale_orders_resolved,
                compensation_retries = report.compensation_retries,
                "Startup reconciliation completed"
            );
        }
        Err(error) => {
            warn!(error = %error, "Startup reconciliation failed; continuing with event loop");
        }
    }

    // Catch up on equity_predictions_completed that arrived while we were down.
    // Periodic portfolio_evaluation_requested ticks are intentionally not caught
    // up because stale ticks carry no meaningful signal.
    let predictions_offset = get_consumer_offset(pool, CONSUMER_PORTFOLIO).await?;
    if let Some(event_id) = latest_event_after(
        pool,
        EventType::EquityPredictionsCompleted,
        predictions_offset,
    )
    .await?
    {
        info!(
            event_id,
            "Catching up on missed equity_predictions_completed"
        );
        handle_equity_predictions_completed(state, pool, event_id).await;
    }

    // Catch up on portfolio_liquidation_requested if we missed it while the
    // market was still open. Guarded by the real session so a restart after an
    // early close does not submit orders into a shut market.
    let liquidation_offset = get_consumer_offset(pool, CONSUMER_PORTFOLIO_LIQUIDATION).await?;
    if let Some(event_id) = latest_event_after(
        pool,
        EventType::PortfolioLiquidationRequested,
        liquidation_offset,
    )
    .await?
    {
        let session_is_open = match state.alpaca_client().fetch_market_session().await {
            Ok(session) => session.is_open(),
            Err(error) => {
                warn!(error = %error, "Market session fetch failed during liquidation catch-up");
                false
            }
        };
        if session_is_open {
            info!(
                event_id,
                "Catching up on missed portfolio_liquidation_requested"
            );
            handle_portfolio_liquidation(state, pool, event_id).await;
        } else {
            info!(
                event_id,
                "Skipping missed portfolio_liquidation_requested: market session has ended"
            );
            if let Err(error) =
                update_consumer_offset(pool, CONSUMER_PORTFOLIO_LIQUIDATION, event_id).await
            {
                warn!(error = %error, "Failed to update liquidation consumer offset");
            }
        }
    }

    loop {
        let notification = tokio::select! {
            result = listener.recv() => result?,
            _ = shutdown_token.cancelled() => {
                info!("Shutdown signal received, draining");
                break;
            }
        };
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

        if event_type == EventType::PortfolioEvaluationRequested.as_str() {
            handle_portfolio_evaluation(state, pool).await;
        } else if event_type == EventType::EquityPredictionsCompleted.as_str() {
            info!(event_id, "Received equity_predictions_completed");
            handle_equity_predictions_completed(state, pool, event_id).await;
        } else if event_type == EventType::EquityPredictionsErrored.as_str() {
            // Nothing to unwind: the request timestamp is a retry backoff, not a
            // lock, so the next due evaluation re-requests on its own.
            info!(event_id, "Received equity_predictions_errored");
        } else if event_type == EventType::PortfolioLiquidationRequested.as_str() {
            info!(event_id, "Received portfolio_liquidation_requested");
            handle_portfolio_liquidation(state, pool, event_id).await;
        }
    }

    Ok(())
}

/// Runs one evaluation pass in response to `portfolio_evaluation_requested`.
///
/// The pass is skipped entirely when the market is closed. Otherwise it:
/// 1. Emits `portfolio_liquidation_requested` and returns when the session close
///    is within [`LIQUIDATION_LEAD_TIME_MINUTES`], so an early close pulls
///    liquidation forward without a calendar.
/// 2. Re-requests predictions when the day has none recorded, then continues —
///    exit monitoring does not depend on predictions and must not be blocked by
///    their absence.
/// 3. Runs the rebalance pass.
///
/// Fails closed on a clock error: an unreachable clock endpoint skips the pass
/// rather than trading on degraded connectivity. The 15:45 Eastern pg_cron job
/// still guarantees liquidation in that case.
///
/// No consumer offset tracking, because a stale evaluation tick carries no
/// meaningful signal.
async fn handle_portfolio_evaluation(state: &AppState, pool: &PgPool) {
    let session = match state.alpaca_client().fetch_market_session().await {
        Ok(session) => session,
        Err(error) => {
            warn!(error = %error, "Skipping evaluation: market session fetch failed");
            return;
        }
    };

    if !session.is_open() {
        info!("Skipping evaluation: market is not open");
        return;
    }

    if request_liquidation_if_close_is_near(pool, &session, Utc::now()).await {
        return;
    }

    ensure_predictions_requested(state, pool).await;

    if !state.try_begin_rebalance() {
        info!("Skipping evaluation: a rebalance pass is already running");
        return;
    }

    run_rebalance_pass(state, pool).await;
    state.finish_rebalance();
}

/// Emits `portfolio_liquidation_requested` when the close is within the lead time.
///
/// Returns `true` when liquidation was requested, signalling the caller to skip
/// the rebalance pass: opening positions minutes before the close is exactly what
/// the exit-feasibility gate exists to prevent.
async fn request_liquidation_if_close_is_near(
    pool: &PgPool,
    session: &MarketSession,
    now: DateTime<Utc>,
) -> bool {
    let minutes_to_close = session.time_until_close(now).num_minutes();
    if minutes_to_close > LIQUIDATION_LEAD_TIME_MINUTES {
        return false;
    }

    info!(
        minutes_to_close,
        session_close = %session.close(),
        "Session close is near; requesting liquidation"
    );
    if let Err(error) = emit_event(
        pool,
        EventType::PortfolioLiquidationRequested,
        &serde_json::json!({"reason": "session_close_approaching"}),
    )
    .await
    {
        warn!(error = %error, "Failed to emit portfolio_liquidation_requested");
    }
    true
}

/// Re-requests predictions when today has none and the retry backoff has elapsed.
///
/// Covers a failed or missed pre-market run. A database error is logged and
/// swallowed so the caller still runs its exit monitoring.
async fn ensure_predictions_requested(state: &AppState, pool: &PgPool) {
    match predictions_exist_for_today(pool).await {
        Ok(true) => return,
        Ok(false) => {}
        Err(error) => {
            warn!(error = %error, "Could not determine whether predictions exist for today");
            return;
        }
    }

    if !state.prediction_request_is_due() {
        info!("Predictions absent for today but a request is already pending");
        return;
    }

    info!("No predictions recorded for today; requesting a run");
    match emit_event(
        pool,
        EventType::EquityPredictionsRequested,
        &serde_json::json!({"reason": "missing_for_session"}),
    )
    .await
    {
        Ok(_) => state.record_prediction_request(),
        Err(error) => {
            warn!(error = %error, "Failed to emit equity_predictions_requested");
        }
    }
}

/// Runs a rebalance pass, emitting the start event and translating the outcome
/// into log lines and, where warranted, a `portfolio_rebalance_errored` event.
///
/// Callers own the in-progress flag; this function neither claims nor releases it.
async fn run_rebalance_pass(state: &AppState, pool: &PgPool) {
    if let Err(error) = emit_event(
        pool,
        EventType::PortfolioRebalanceStarted,
        &serde_json::json!({}),
    )
    .await
    {
        warn!(error = %error, "Failed to emit portfolio_rebalance_started");
    }

    match run_rebalance(state).await {
        Ok(outcome) => {
            info!(
                session_id = %outcome.session_id,
                pairs_opened = outcome.pairs_opened,
                pairs_closed = outcome.pairs_closed,
                pairs_kept = outcome.pairs_kept,
                "Rebalance completed from event"
            );
        }
        Err(RebalanceError::StalePredictions) => {
            warn!("Rebalance skipped: stale or absent predictions");
            if let Err(error) = emit_event(
                pool,
                EventType::PortfolioRebalanceErrored,
                &serde_json::json!({"reason": "stale_predictions"}),
            )
            .await
            {
                warn!(error = %error, "Failed to emit portfolio_rebalance_errored");
            }
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
            if let Err(error) = emit_event(
                pool,
                EventType::PortfolioRebalanceErrored,
                &serde_json::json!({"reason": "drawdown_breached"}),
            )
            .await
            {
                warn!(error = %error, "Failed to emit portfolio_rebalance_errored");
            }
        }
        Err(error) => {
            error!(error = %error, "Rebalance errored");
            if let Err(emit_error) = emit_event(
                pool,
                EventType::PortfolioRebalanceErrored,
                &serde_json::json!({"reason": error.to_string()}),
            )
            .await
            {
                warn!(error = %emit_error, "Failed to emit portfolio_rebalance_errored");
            }
        }
    }
}

/// Runs a rebalance pass in response to a completed prediction run.
///
/// A fresh prediction set is the one intraday moment where entry candidates can
/// change, so it is acted on immediately rather than waiting for the next tick.
async fn handle_equity_predictions_completed(state: &AppState, pool: &PgPool, event_id: i64) {
    if state.try_begin_rebalance() {
        run_rebalance_pass(state, pool).await;
        state.finish_rebalance();
    } else {
        info!("Skipping prediction-driven rebalance: a pass is already running");
    }

    if let Err(error) = update_consumer_offset(pool, CONSUMER_PORTFOLIO, event_id).await {
        warn!(error = %error, "Failed to update consumer offset");
    }
}

async fn handle_portfolio_liquidation(state: &AppState, pool: &PgPool, event_id: i64) {
    if let Err(error) = emit_event(
        pool,
        EventType::PortfolioLiquidationStarted,
        &serde_json::json!({}),
    )
    .await
    {
        warn!(error = %error, "Failed to emit portfolio_liquidation_started");
    }

    match run_end_of_day_liquidation(state).await {
        Ok(pairs_closed) => info!(pairs_closed, "Portfolio liquidation completed"),
        Err(RebalanceError::Execution(error)) => {
            error!(error = %error, "Portfolio liquidation errored: Alpaca execution error");
            if let Err(emit_error) = emit_event(
                pool,
                EventType::PortfolioLiquidationErrored,
                &serde_json::json!({"reason": error.to_string()}),
            )
            .await
            {
                warn!(error = %emit_error, "Failed to emit portfolio_liquidation_errored");
            }
        }
        Err(error) => {
            error!(error = %error, "Portfolio liquidation errored");
            if let Err(emit_error) = emit_event(
                pool,
                EventType::PortfolioLiquidationErrored,
                &serde_json::json!({"reason": error.to_string()}),
            )
            .await
            {
                warn!(error = %emit_error, "Failed to emit portfolio_liquidation_errored");
            }
        }
    }

    if let Err(error) = update_consumer_offset(pool, CONSUMER_PORTFOLIO_LIQUIDATION, event_id).await
    {
        warn!(error = %error, "Failed to update liquidation consumer offset");
    }
}

#[cfg(test)]
mod tests {
    use super::{
        CONSUMER_PORTFOLIO, CONSUMER_PORTFOLIO_LIQUIDATION, LIQUIDATION_LEAD_TIME_MINUTES,
    };
    use crate::common::events::EventType;

    #[test]
    fn test_consumer_names_are_stable() {
        assert_eq!(CONSUMER_PORTFOLIO, "portfolio");
        assert_eq!(CONSUMER_PORTFOLIO_LIQUIDATION, "portfolio-liquidation");
    }

    #[test]
    fn test_event_type_strings_are_stable() {
        assert_eq!(
            EventType::EquityPredictionsCompleted.as_str(),
            "equity_predictions_completed"
        );
        assert_eq!(
            EventType::EquityPredictionsErrored.as_str(),
            "equity_predictions_errored"
        );
        assert_eq!(
            EventType::PortfolioLiquidationRequested.as_str(),
            "portfolio_liquidation_requested"
        );
        assert_eq!(
            EventType::PortfolioEvaluationRequested.as_str(),
            "portfolio_evaluation_requested"
        );
    }

    #[test]
    fn test_liquidation_lead_time_matches_cron_backstop() {
        // The 15:45 Eastern pg_cron fail-safe fires 15 minutes before a regular
        // close. Keeping the dynamic trigger at the same lead time means the
        // Alpaca-derived path fires first on a regular day and earlier on an
        // early close, with the cron only covering a clock outage.
        assert_eq!(LIQUIDATION_LEAD_TIME_MINUTES, 15);
    }

    // --- evaluation handler state machine tests ---

    use super::{handle_portfolio_evaluation, request_liquidation_if_close_is_near};
    use crate::common::market_hours::MarketSession;
    use crate::portfolio::alpaca::MockTrading;
    use crate::portfolio::state::AppState;
    use chrono::{DateTime, Utc};
    use std::sync::Arc;

    fn utc(rfc3339: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(rfc3339)
            .expect("valid RFC3339 timestamp")
            .with_timezone(&Utc)
    }

    /// Creates an `AppState` with a dummy pool and the given mock trading client.
    ///
    /// Uses port 1 (TCP reserved, unreachable) so that any query fails rather
    /// than reaching a real local Postgres. The acquire timeout is shortened
    /// from the 30-second default because these tests deliberately exercise
    /// paths that query, and the failure is the expected outcome.
    fn make_test_state(mock: MockTrading) -> AppState {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(1)
            .acquire_timeout(std::time::Duration::from_millis(100))
            .connect_lazy("postgres://localhost:1/nonexistent_consumer_test")
            .expect("lazy pool creation should not fail");
        AppState::with_mock(pool, Arc::new(mock))
    }

    #[tokio::test]
    async fn test_evaluation_skips_when_market_closed() {
        let mock = MockTrading {
            market_open: false,
            ..MockTrading::default()
        };
        let state = make_test_state(mock);

        handle_portfolio_evaluation(&state, state.pool()).await;

        // The pass returned before claiming the rebalance slot.
        assert!(!state.rebalance_in_progress());
    }

    #[tokio::test]
    async fn test_evaluation_releases_slot_after_pass() {
        let mock = MockTrading {
            market_open: true,
            ..MockTrading::default()
        };
        let state = make_test_state(mock);

        // The dummy pool makes every query fail, so the pass errors out. The
        // slot must still be released or the service would wedge after one
        // transient database failure.
        handle_portfolio_evaluation(&state, state.pool()).await;

        assert!(!state.rebalance_in_progress());
    }

    #[tokio::test]
    async fn test_evaluation_skips_when_pass_already_running() {
        let mock = MockTrading {
            market_open: true,
            ..MockTrading::default()
        };
        let state = make_test_state(mock);

        assert!(state.try_begin_rebalance());

        handle_portfolio_evaluation(&state, state.pool()).await;

        // Still held by the simulated in-flight pass, not cleared by the skip.
        assert!(state.rebalance_in_progress());
    }

    /// Regular session on 2024-07-15: 09:30–16:00 EDT.
    fn regular_session() -> MarketSession {
        MarketSession::new(true, utc("2024-07-15T20:00:00Z")).expect("regular session")
    }

    #[tokio::test]
    async fn test_liquidation_requested_when_close_is_near() {
        let mock = MockTrading::default();
        let state = make_test_state(mock);

        // 15:50 EDT: ten minutes to a 16:00 close, inside the lead time.
        let now = utc("2024-07-15T19:50:00Z");

        assert!(request_liquidation_if_close_is_near(state.pool(), &regular_session(), now).await);
    }

    #[tokio::test]
    async fn test_liquidation_not_requested_mid_session() {
        let mock = MockTrading::default();
        let state = make_test_state(mock);

        // 13:00 EDT: three hours to the close.
        let now = utc("2024-07-15T17:00:00Z");

        assert!(!request_liquidation_if_close_is_near(state.pool(), &regular_session(), now).await);
    }

    #[tokio::test]
    async fn test_liquidation_requested_after_close_passed() {
        let mock = MockTrading::default();
        let state = make_test_state(mock);

        // time_until_close saturates at zero, so a close already behind us is
        // inside the lead time and still requests liquidation.
        let now = utc("2024-07-15T20:30:00Z");

        assert!(request_liquidation_if_close_is_near(state.pool(), &regular_session(), now).await);
    }

    #[tokio::test]
    async fn test_early_close_triggers_liquidation_when_regular_close_would_not() {
        let mock = MockTrading::default();
        let state = make_test_state(mock);

        // 12:50 Eastern. Under a 16:00 close this is mid-session and the 15:45
        // pg_cron backstop is still hours away; under a 13:00 early close it is
        // ten minutes out and liquidation must be requested now. This is the
        // case the fixed cron schedule missed entirely.
        let now = utc("2024-07-03T16:50:00Z");
        let early_close =
            MarketSession::new(true, utc("2024-07-03T17:00:00Z")).expect("early close session");
        let regular_close =
            MarketSession::new(true, utc("2024-07-03T20:00:00Z")).expect("regular close session");

        assert!(request_liquidation_if_close_is_near(state.pool(), &early_close, now).await);
        assert!(!request_liquidation_if_close_is_near(state.pool(), &regular_close, now).await);
    }

    // --- rebalance slot claiming ---

    #[tokio::test]
    async fn test_rebalance_slot_is_exclusive() {
        let mock = MockTrading::default();
        let state = make_test_state(mock);

        assert!(state.try_begin_rebalance());
        assert!(!state.try_begin_rebalance());

        state.finish_rebalance();
        assert!(state.try_begin_rebalance());
    }

    // --- prediction request backoff ---

    #[tokio::test]
    async fn test_prediction_request_due_before_first_request() {
        let mock = MockTrading::default();
        let state = make_test_state(mock);

        assert!(state.prediction_request_is_due());
    }

    #[tokio::test]
    async fn test_prediction_request_not_due_immediately_after_request() {
        let mock = MockTrading::default();
        let state = make_test_state(mock);

        state.record_prediction_request();

        assert!(!state.prediction_request_is_due());
    }

    #[tokio::test]
    async fn test_prediction_request_due_again_after_backoff() {
        let mock = MockTrading::default();
        let state = make_test_state(mock);

        state.record_prediction_request();
        // Backdate past the retry window: a run that died without emitting a
        // terminal event must not wedge the day.
        state.last_prediction_request_at_atomic().store(
            Utc::now().timestamp() - 11 * 60,
            std::sync::atomic::Ordering::SeqCst,
        );

        assert!(state.prediction_request_is_due());
    }
}
