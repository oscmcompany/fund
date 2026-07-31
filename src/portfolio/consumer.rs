//! PostgreSQL event consumer for the portfolio service.
//!
//! Listens on the `events` channel and:
//! - Opens the session on `trading_session_started`: builds the initial
//!   portfolio and arms a one-shot timer for the real session close.
//! - Runs an evaluation pass on each `portfolio_evaluation_requested` event,
//!   which now arrives only from a live-quote threshold crossing.
//! - Runs a rebalance cycle on each `equity_predictions_completed` event so a
//!   fresh prediction set is acted on immediately.
//! - Runs end-of-day liquidation on each `portfolio_liquidation_requested` event.
//!
//! No unconditional timer drives work here. A five-minute evaluation heartbeat
//! previously ran a full rebalance pass whether or not anything had changed, up
//! to 78 times a session. The work that genuinely needs a schedule — opening the
//! session, closing it — is scheduled directly, and the rest is driven by
//! spreads actually crossing thresholds.
//!
//! Two timers do exist, and both are conditional on something having gone
//! differently than planned: the liquidation timer, armed once per session from
//! the real close, and the entry retry, armed only when the session-start pass
//! opened nothing. A session that opens normally and closes normally arms one
//! timer and fires one rebalance.
//!
//! Predictions are requested pre-market by pg_cron. They derive from daily bars,
//! so a single run produces every distinct value the session will have;
//! re-running mid-session recomputed an identical answer. The session start
//! re-requests them only when the day has none recorded.

use std::time::Duration;

use chrono::{DateTime, Utc};
use sqlx::PgPool;
use tokio::task::JoinHandle;
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use crate::common::events::{
    emit_event, get_consumer_offset, latest_event_after, run_event_listener,
    update_consumer_offset, EventType, Outcome, CONSUMER_PORTFOLIO, CONSUMER_PORTFOLIO_LIQUIDATION,
    CONSUMER_PORTFOLIO_SESSION,
};
use crate::common::market_hours::MarketSession;
use crate::domain::trading::RebalanceTrigger;
use crate::portfolio::database::{fetch_open_pairs, predictions_exist_for_today};
use crate::portfolio::rebalance::{
    run_end_of_day_liquidation, run_rebalance, RebalanceError, RebalanceOutcome,
};
use crate::portfolio::reconciliation;
use crate::portfolio::state::AppState;

/// How close to the session close the liquidation request is emitted.
///
/// Derived from Alpaca's reported close rather than a fixed wall-clock time, so
/// an early close pulls liquidation forward automatically. The pg_cron job at
/// 15:45 Eastern remains as a fail-safe for the case where the clock endpoint is
/// unreachable; liquidation is idempotent, so both firing is harmless.
const LIQUIDATION_LEAD_TIME_MINUTES: i64 = 15;

/// Backoff, in minutes, for re-attempting entry after a session start that
/// opened nothing.
///
/// Bounded rather than indefinite: three failures spread over the first hour of
/// the session mean the cause is not transient, and continuing to retry would
/// only add load to whatever is already failing. Spacing widens so the first
/// attempt catches a brief outage without waiting long.
const ENTRY_RETRY_BACKOFF_MINUTES: [i64; 3] = [5, 15, 30];

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
    run_event_listener(
        pool,
        shutdown_token,
        "portfolio",
        || run_startup_catch_up(state, pool, shutdown_token),
        |notification| async move {
            let event_id = notification.event_id();
            match notification.event_type() {
                EventType::TradingSessionStarted => {
                    info!(event_id, "Received trading_session_started");
                    handle_trading_session_started(state, pool, event_id, shutdown_token).await;
                }
                EventType::PortfolioEvaluationRequested => {
                    handle_portfolio_evaluation(state, pool, notification.payload()).await;
                }
                EventType::EquityPredictions(Outcome::Completed) => {
                    info!(event_id, "Received equity_predictions_completed");
                    handle_equity_predictions_completed(state, pool, event_id).await;
                }
                EventType::EquityPredictions(Outcome::Errored) => {
                    // Nothing to unwind: the request timestamp is a retry
                    // backoff, not a lock, so the next due evaluation
                    // re-requests on its own. An explicit arm because this is a
                    // considered decision, not an oversight.
                    info!(event_id, "Received equity_predictions_errored");
                }
                EventType::PortfolioLiquidation(Outcome::Requested) => {
                    info!(event_id, "Received portfolio_liquidation_requested");
                    handle_portfolio_liquidation(
                        state,
                        pool,
                        event_id,
                        LiquidationTrigger::from_payload(notification.payload()),
                    )
                    .await;
                }
                // Every consumer receives every event; the rest belong to other
                // services or are audit records. Listed rather than caught by a
                // wildcard so that adding a family, or an `Outcome` to a family
                // this consumer partly handles, fails the build here instead of
                // being silently ignored.
                EventType::EquityPredictions(Outcome::Requested | Outcome::Started)
                | EventType::PortfolioLiquidation(
                    Outcome::Started | Outcome::Completed | Outcome::Errored,
                )
                | EventType::EquityBarsSync(_)
                | EventType::DatabaseExport(_)
                | EventType::DatabaseBackup(_)
                | EventType::DatabasePurge(_)
                | EventType::MarketCalendarSync(_)
                | EventType::SchedulerHealthCheck(_)
                | EventType::ModelArtifactCheck(_)
                | EventType::ModelArtifactPublished
                | EventType::ModelArtifactStale
                | EventType::PortfolioRebalance(_)
                | EventType::StressTest => {}
            }
        },
    )
    .await
}

/// Replays the events this consumer missed while it was down.
///
/// Runs on every connection, not once per process: a reconnect means delivery
/// had a gap, and the same replay that covers a restart covers that gap too.
async fn run_startup_catch_up(
    state: &AppState,
    pool: &PgPool,
    shutdown_token: &CancellationToken,
) -> Result<(), sqlx::Error> {
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
        EventType::EquityPredictions(Outcome::Completed),
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

    // Catch up on trading_session_started if we were down when it fired. Without
    // this a process restarted at any point after 09:25 Eastern would trade
    // nothing for the rest of the day: the event is emitted once, and the live
    // evaluator that drives everything else cannot fire until pairs are open.
    // The retired five-minute heartbeat used to cover this within five minutes.
    let session_offset = get_consumer_offset(pool, CONSUMER_PORTFOLIO_SESSION).await?;
    if let Some(event_id) =
        latest_event_after(pool, EventType::TradingSessionStarted, session_offset).await?
    {
        // Guarded on the real session so a restart after the close, or on the
        // day after, does not replay a stale open into a shut market.
        // `contains` rather than `is_open`: the session is served from a
        // per-date cache, and `is_open` is the one field of it that is not fixed
        // for the day — a session cached before the bell reports `false` for the
        // rest of the session. `contains` derives liveness from the schedule, so
        // it stays correct however long the value has been held.
        let session_is_live = match state.alpaca_client().fetch_market_session().await {
            Ok(session) => session.contains(Utc::now()) && session.trades_on_date_of(Utc::now()),
            Err(error) => {
                warn!(error = %error, "Market session fetch failed during session-start catch-up");
                false
            }
        };
        if session_is_live {
            info!(event_id, "Catching up on missed trading_session_started");
            handle_trading_session_started(state, pool, event_id, shutdown_token).await;
        } else {
            info!(
                event_id,
                "Skipping missed trading_session_started: not inside a live session"
            );
            if let Err(error) =
                update_consumer_offset(pool, CONSUMER_PORTFOLIO_SESSION, event_id).await
            {
                warn!(error = %error, "Failed to update session consumer offset");
            }
        }
    }

    // Catch up on portfolio_liquidation_requested if we missed it while the
    // market was still open. Guarded by the real session so a restart after an
    // early close does not submit orders into a shut market.
    let liquidation_offset = get_consumer_offset(pool, CONSUMER_PORTFOLIO_LIQUIDATION).await?;
    if let Some(event_id) = latest_event_after(
        pool,
        EventType::PortfolioLiquidation(Outcome::Requested),
        liquidation_offset,
    )
    .await?
    {
        // `contains` rather than `is_open`, for the reason given at the
        // session-start catch-up above.
        let session_is_open = match state.alpaca_client().fetch_market_session().await {
            Ok(session) => session.contains(Utc::now()),
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
            // A replayed request is neither live path: the payload names the
            // emitter, but by the time a catch-up runs, "which cron emitted
            // this" is not the useful fact — that it is being replayed is.
            handle_portfolio_liquidation(state, pool, event_id, LiquidationTrigger::CatchUp).await;
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

    Ok(())
}

/// Starts the trading session in response to `trading_session_started`.
///
/// Confirms the market actually trades today, builds the initial portfolio, and
/// arms the liquidation timer from the real session close. Emitted once shortly
/// before the regular open, so on a holiday this is the point at which the
/// session is recognised as not happening and nothing further runs.
///
/// Fails closed on a clock error, matching every other trading path: without a
/// session there is no close to schedule liquidation against, and the fixed
/// 15:45 Eastern pg_cron job remains the backstop.
async fn handle_trading_session_started(
    state: &AppState,
    pool: &PgPool,
    event_id: i64,
    shutdown_token: &CancellationToken,
) {
    let session = match state.alpaca_client().fetch_market_session().await {
        Ok(session) => session,
        Err(error) => {
            warn!(error = %error, "Skipping session start: market session fetch failed");
            return;
        }
    };

    if !session.trades_on_date_of(Utc::now()) {
        info!(
            session_close = %session.close(),
            "Skipping session start: the market does not trade today"
        );
        record_session_offset(pool, event_id).await;
        return;
    }

    // Armed before the portfolio is built, not after. A rebalance that hangs or
    // errors must not be able to leave the session without a liquidation timer;
    // the timer only emits an event, and liquidation is idempotent.
    spawn_liquidation_timer(pool.clone(), &session, shutdown_token.clone());

    ensure_predictions_requested(state, pool).await;

    if !state.try_begin_rebalance() {
        info!("Skipping session start rebalance: a pass is already running");
        record_session_offset(pool, event_id).await;
        return;
    }
    run_rebalance_pass(state, pool, RebalanceTrigger::SessionStart).await;
    state.finish_rebalance();

    // A pass that opened nothing is the case the retired heartbeat used to
    // cover. Predictions land before this event, and the live evaluator cannot
    // fire without open pairs, so without a retry a single transient failure
    // here costs the whole session.
    match fetch_open_pairs(pool).await {
        Ok(pairs) if pairs.is_empty() => {
            spawn_entry_retry_timer(pool.clone(), session, shutdown_token.clone());
        }
        Ok(pairs) => info!(
            open_pairs = pairs.len(),
            "Session start opened positions; no entry retry armed"
        ),
        Err(error) => {
            // Arm anyway: not knowing whether the portfolio is empty is not a
            // reason to give up on the session. The retry re-reads before
            // acting, so a spurious arm costs one query.
            warn!(error = %error, "Could not read open pairs after session start; arming entry retry");
            spawn_entry_retry_timer(pool.clone(), session, shutdown_token.clone());
        }
    }

    record_session_offset(pool, event_id).await;
}

/// Records progress past a `trading_session_started` event.
async fn record_session_offset(pool: &PgPool, event_id: i64) {
    if let Err(error) = update_consumer_offset(pool, CONSUMER_PORTFOLIO_SESSION, event_id).await {
        warn!(error = %error, "Failed to update session consumer offset");
    }
}

/// Re-requests an evaluation on a backoff while the portfolio is still empty.
///
/// Spawned only when the session-start pass opened nothing, so a healthy session
/// arms no timer at all. Each attempt re-reads the open pairs first and stops as
/// soon as any exist, whatever opened them.
///
/// Emits `portfolio_evaluation_requested` rather than calling the rebalance
/// directly, so a retry inherits everything [`handle_portfolio_evaluation`]
/// already does: the clock check, the close-proximity guard, the prediction
/// re-request, and the `try_begin_rebalance` exclusion that keeps it from
/// colliding with a pass already under way. This task holds no rebalance state.
fn spawn_entry_retry_timer(
    pool: PgPool,
    session: MarketSession,
    shutdown_token: CancellationToken,
) {
    info!(
        attempts = ENTRY_RETRY_BACKOFF_MINUTES.len(),
        "Session start opened no positions; arming entry retry"
    );

    tokio::spawn(async move {
        for (attempt, minutes) in ENTRY_RETRY_BACKOFF_MINUTES.iter().enumerate() {
            let wait = Duration::from_secs((*minutes as u64) * 60);
            tokio::select! {
                _ = sleep(wait) => {}
                _ = shutdown_token.cancelled() => {
                    info!("Entry retry cancelled for shutdown");
                    return;
                }
            }

            // Opening positions minutes before the close is what the
            // exit-feasibility gate exists to prevent, and liquidation is about
            // to run regardless.
            if close_is_near(&session, Utc::now()) {
                info!("Entry retry stopped: session close is near");
                return;
            }

            match fetch_open_pairs(&pool).await {
                Ok(pairs) if !pairs.is_empty() => {
                    info!(
                        open_pairs = pairs.len(),
                        "Entry retry stopped: positions are open"
                    );
                    return;
                }
                Ok(_) => {}
                Err(error) => {
                    warn!(error = %error, "Entry retry could not read open pairs; requesting anyway");
                }
            }

            info!(
                attempt = attempt + 1,
                of = ENTRY_RETRY_BACKOFF_MINUTES.len(),
                "Portfolio still empty; requesting evaluation"
            );
            if let Err(error) = emit_event(
                &pool,
                EventType::PortfolioEvaluationRequested,
                &serde_json::json!({"reason": "entry_retry", "attempt": attempt + 1}),
            )
            .await
            {
                warn!(error = %error, "Failed to emit entry retry evaluation request");
            }
        }

        info!("Entry retry attempts exhausted; portfolio stays empty this session");
    });
}

/// Sleeps until [`LIQUIDATION_LEAD_TIME_MINUTES`] before the close, then emits
/// `portfolio_liquidation_requested`.
///
/// A one-shot timer rather than a poll. The close is known the moment the
/// session is read, so waiting for it needs no repeated clock reads, and
/// deriving the instant from Alpaca's reported close is what pulls liquidation
/// forward on an early-close day without a local calendar.
///
/// The fixed 15:45 Eastern pg_cron job stays as a fail-safe for the case where
/// this process is not running when the timer would have fired. Liquidation is
/// idempotent, so both firing is harmless.
fn spawn_liquidation_timer(
    pool: PgPool,
    session: &MarketSession,
    shutdown_token: CancellationToken,
) {
    let close = session.close();
    let fires_at = close - chrono::Duration::minutes(LIQUIDATION_LEAD_TIME_MINUTES);
    let wait = fires_at
        .signed_duration_since(Utc::now())
        .to_std()
        .unwrap_or(std::time::Duration::ZERO);

    info!(
        session_close = %close,
        fires_at = %fires_at,
        wait_seconds = wait.as_secs(),
        "Liquidation timer armed"
    );

    tokio::spawn(async move {
        tokio::select! {
            _ = sleep(wait) => {}
            _ = shutdown_token.cancelled() => {
                info!("Liquidation timer cancelled for shutdown");
                return;
            }
        }

        info!(session_close = %close, "Liquidation timer fired; requesting liquidation");
        if let Err(error) = emit_event(
            &pool,
            EventType::PortfolioLiquidation(Outcome::Requested),
            &serde_json::json!({"reason": "session_close_approaching"}),
        )
        .await
        {
            // The pg_cron fail-safe at 15:45 Eastern still covers this, which is
            // why a failed emit is not retried here.
            warn!(error = %error, "Failed to emit portfolio_liquidation_requested from timer");
        }
    });
}

/// Runs one evaluation pass in response to `portfolio_evaluation_requested`.
///
/// Reached only from a live-quote threshold crossing, so unlike the session
/// start this is a reaction to something having moved. The pass is skipped
/// entirely when the market is closed, then re-requests predictions if the day
/// has none recorded — exit monitoring does not depend on predictions and must
/// not be blocked by their absence — and runs the rebalance.
///
/// Detecting a nearing close is no longer this function's job. That moved to the
/// one-shot timer armed at session start, which derives the instant from the
/// reported close instead of noticing it on whatever tick happens to land inside
/// the lead time.
///
/// Fails closed on a clock error: an unreachable clock endpoint skips the pass
/// rather than trading on degraded connectivity.
///
/// No consumer offset tracking, because a stale evaluation tick carries no
/// meaningful signal.
async fn handle_portfolio_evaluation(state: &AppState, pool: &PgPool, payload: &serde_json::Value) {
    let trigger = evaluation_trigger_from(payload);
    let session = match state.alpaca_client().fetch_market_session().await {
        Ok(session) => session,
        Err(error) => {
            warn!(error = %error, "Skipping evaluation: market session fetch failed");
            return;
        }
    };

    // `contains` rather than `is_open`, for the reason given at the session-start
    // catch-up above. This site is the one where it matters most: the session is
    // established at 09:25, five minutes before the bell, so the cached
    // `is_open` is `false` for the whole day and reading it here would skip
    // every intraday evaluation the live evaluator triggers.
    if !session.contains(Utc::now()) {
        info!("Skipping evaluation: market is not open");
        return;
    }

    if close_is_near(&session, Utc::now()) {
        info!(
            minutes_to_close = session.time_until_close(Utc::now()).num_minutes(),
            session_close = %session.close(),
            "Skipping evaluation: session close is near"
        );
        return;
    }

    ensure_predictions_requested(state, pool).await;

    if !state.try_begin_rebalance() {
        info!("Skipping evaluation: a rebalance pass is already running");
        return;
    }

    let outcome = run_rebalance_pass(state, pool, trigger).await;
    state.finish_rebalance();

    if trigger == RebalanceTrigger::LiveCrossing {
        if let Some(outcome) = outcome {
            report_trigger_disagreement(payload, &outcome);
        }
    }
}

/// Returns `true` when the session close is within [`LIQUIDATION_LEAD_TIME_MINUTES`].
///
/// A crossing detected this late must not open new positions — that is exactly
/// what the exit-feasibility gate exists to prevent — so the evaluation pass
/// declines rather than racing the liquidation that is about to run.
///
/// This no longer emits the liquidation itself. The timer armed at session start
/// owns that, and the fixed pg_cron job backs it up; emitting from here as well
/// would fire liquidation twice for one close. Idempotency makes that harmless
/// but not free, and two emitters for one decision is a worse arrangement than
/// one.
fn close_is_near(session: &MarketSession, now: DateTime<Utc>) -> bool {
    session.time_until_close(now).num_minutes() <= LIQUIDATION_LEAD_TIME_MINUTES
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

    // Claimed before emitting, so a concurrent caller cannot also decide the
    // backoff has elapsed and request a second inference run.
    let previous_claim = state.last_prediction_request_at();
    if !state.try_claim_prediction_request() {
        info!("Predictions absent for today but a request is already pending");
        return;
    }

    info!("No predictions recorded for today; requesting a run");
    if let Err(error) = emit_event(
        pool,
        EventType::EquityPredictions(Outcome::Requested),
        &serde_json::json!({"reason": "missing_for_session"}),
    )
    .await
    {
        // Give the claim back: nothing was emitted, so consuming the backoff
        // window would leave the session without predictions for ten minutes
        // over a transient failure.
        state.release_prediction_request(previous_claim);
        warn!(error = %error, "Failed to emit equity_predictions_requested");
    }
}

/// Maps an evaluation request's payload to the trigger that produced it.
///
/// The emitters already set `reason`; this reads it back so the rebalance
/// session records which of them ran. An unrecognised or absent reason means the
/// request came from somewhere other than the two in-process emitters.
fn evaluation_trigger_from(payload: &serde_json::Value) -> RebalanceTrigger {
    match payload.get("reason").and_then(|value| value.as_str()) {
        Some("live_threshold_crossing") => RebalanceTrigger::LiveCrossing,
        Some("entry_retry") => RebalanceTrigger::EntryRetry,
        _ => RebalanceTrigger::Manual,
    }
}

/// Runs a rebalance pass, emitting the start event and translating the outcome
/// into log lines and, where warranted, a `portfolio_rebalance_errored` event.
///
/// Callers own the in-progress flag; this function neither claims nor releases it.
///
/// Returns the outcome on success so a caller can compare the pass against
/// whatever triggered it; `None` means the pass was skipped or failed, which the
/// arms below have already logged.
async fn run_rebalance_pass(
    state: &AppState,
    pool: &PgPool,
    trigger: RebalanceTrigger,
) -> Option<RebalanceOutcome> {
    if let Err(error) = emit_event(
        pool,
        EventType::PortfolioRebalance(Outcome::Started),
        &serde_json::json!({}),
    )
    .await
    {
        warn!(error = %error, "Failed to emit portfolio_rebalance_started");
    }

    match run_rebalance(state, trigger).await {
        Ok(outcome) => {
            info!(
                session_id = %outcome.session_id(),
                pairs_opened = outcome.pairs_opened(),
                pairs_closed = outcome.pairs_closed(),
                pairs_kept = outcome.pairs_kept(),
                "Rebalance completed from event"
            );
            return Some(outcome);
        }
        Err(RebalanceError::StalePredictions) => {
            warn!("Rebalance skipped: stale or absent predictions");
            if let Err(error) = emit_event(
                pool,
                EventType::PortfolioRebalance(Outcome::Errored),
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
                EventType::PortfolioRebalance(Outcome::Errored),
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
                EventType::PortfolioRebalance(Outcome::Errored),
                &serde_json::json!({"reason": error.to_string()}),
            )
            .await
            {
                warn!(error = %emit_error, "Failed to emit portfolio_rebalance_errored");
            }
        }
    }
    None
}

/// Reports when the live evaluator flagged a pair the authoritative pass then
/// declined to close.
///
/// These two paths already produced a livelock once by measuring the same
/// spread differently, which is why they share `close_reason_for` and
/// `z_score_against`. A systematic divergence recurring is the failure worth
/// hearing about, so it is logged rather than left to be inferred from a pass
/// that quietly did nothing.
fn report_trigger_disagreement(payload: &serde_json::Value, outcome: &RebalanceOutcome) {
    let Some(flagged_pair) = payload.get("pair_id").and_then(|value| value.as_str()) else {
        return;
    };
    let agreed = outcome
        .close_signals()
        .iter()
        .any(|(pair_id, _)| pair_id.as_str() == flagged_pair);
    if agreed {
        return;
    }
    warn!(
        pair_id = flagged_pair,
        trigger_z_score = payload.get("z_score").and_then(|value| value.as_f64()),
        pairs_closed = outcome.pairs_closed(),
        close_signals = outcome.close_signals().len(),
        "Live evaluator flagged a pair the rebalance pass did not close"
    );
}

/// Runs a rebalance pass in response to a completed prediction run.
///
/// A fresh prediction set is the one intraday moment where entry candidates can
/// change, so it is acted on immediately rather than waiting for the next tick.
async fn handle_equity_predictions_completed(state: &AppState, pool: &PgPool, event_id: i64) {
    if state.try_begin_rebalance() {
        run_rebalance_pass(state, pool, RebalanceTrigger::PredictionRefresh).await;
        state.finish_rebalance();
    } else {
        info!("Skipping prediction-driven rebalance: a pass is already running");
    }

    if let Err(error) = update_consumer_offset(pool, CONSUMER_PORTFOLIO, event_id).await {
        warn!(error = %error, "Failed to update consumer offset");
    }
}

/// Which emitter produced a `portfolio_liquidation_requested`.
///
/// Two paths emit it, deliberately. The in-process timer is armed at session
/// start from Alpaca's reported close, so it pulls liquidation forward on an
/// early-close day. The pg_cron job fires on the wall clock at 15:45 Eastern so
/// an unreachable Alpaca clock cannot leave positions open overnight.
/// Liquidation is idempotent, so both firing is harmless — which is why the
/// redundancy stays. What it lacked was any way to tell, afterwards, which one
/// had acted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LiquidationTrigger {
    /// The in-process timer, armed from the real session close.
    SessionCloseApproaching,
    /// The pg_cron fail-safe on the wall clock.
    FailSafeSchedule,
    /// A request replayed at startup after being missed.
    CatchUp,
    /// An emitter this build does not recognise, or a payload without a reason.
    Unknown,
}

impl LiquidationTrigger {
    /// Reads the trigger from the event payload's `reason` field.
    fn from_payload(payload: &serde_json::Value) -> Self {
        match payload.get("reason").and_then(|value| value.as_str()) {
            Some("session_close_approaching") => Self::SessionCloseApproaching,
            Some("fail_safe_schedule") => Self::FailSafeSchedule,
            _ => Self::Unknown,
        }
    }

    /// Returns the name recorded in emitted payloads and logs.
    fn as_str(self) -> &'static str {
        match self {
            Self::SessionCloseApproaching => "session_close_approaching",
            Self::FailSafeSchedule => "fail_safe_schedule",
            Self::CatchUp => "catch_up",
            Self::Unknown => "unknown",
        }
    }
}

/// Logs the outcome of a liquidation, at a level that reflects what it means.
///
/// The fail-safe finding a flat book is the healthy case, and saying so is the
/// point: a silent no-op is indistinguishable from the job not having run at
/// all. The fail-safe finding open positions means the primary timer did not
/// fire, or fired and failed — a real signal that was previously invisible,
/// because both cases logged the same line.
fn report_liquidation_result(trigger: LiquidationTrigger, pairs_closed: usize) {
    match (trigger, pairs_closed) {
        (LiquidationTrigger::FailSafeSchedule, 0) => info!(
            trigger = trigger.as_str(),
            "Liquidation fail-safe ran and found the book already flat"
        ),
        (LiquidationTrigger::FailSafeSchedule, closed) => warn!(
            trigger = trigger.as_str(),
            pairs_closed = closed,
            "Liquidation fail-safe closed open positions; the session-close timer did not"
        ),
        (_, closed) => info!(
            trigger = trigger.as_str(),
            pairs_closed = closed,
            "Portfolio liquidation completed"
        ),
    }
}

async fn handle_portfolio_liquidation(
    state: &AppState,
    pool: &PgPool,
    event_id: i64,
    trigger: LiquidationTrigger,
) {
    if let Err(error) = emit_event(
        pool,
        EventType::PortfolioLiquidation(Outcome::Started),
        &serde_json::json!({"trigger": trigger.as_str()}),
    )
    .await
    {
        warn!(error = %error, "Failed to emit portfolio_liquidation_started");
    }

    match run_end_of_day_liquidation(state).await {
        Ok(pairs_closed) => report_liquidation_result(trigger, pairs_closed),
        Err(RebalanceError::Execution(error)) => {
            error!(error = %error, "Portfolio liquidation errored: Alpaca execution error");
            if let Err(emit_error) = emit_event(
                pool,
                EventType::PortfolioLiquidation(Outcome::Errored),
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
                EventType::PortfolioLiquidation(Outcome::Errored),
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
        evaluation_trigger_from, report_trigger_disagreement, LiquidationTrigger,
        CONSUMER_PORTFOLIO, CONSUMER_PORTFOLIO_LIQUIDATION, CONSUMER_PORTFOLIO_SESSION,
        ENTRY_RETRY_BACKOFF_MINUTES, LIQUIDATION_LEAD_TIME_MINUTES,
    };
    use crate::common::events::{EventType, Outcome};
    use crate::domain::market::PairID;
    use crate::domain::trading::{CloseReason, RebalanceTrigger};
    use crate::portfolio::rebalance::RebalanceOutcome;

    /// Builds an outcome whose exit evaluation signalled the given pairs.
    fn outcome_closing(pairs: &[&str]) -> RebalanceOutcome {
        let close_signals = pairs
            .iter()
            .map(|pair| {
                (
                    PairID::parse(pair).expect("test pair id should be valid"),
                    CloseReason::StopLoss,
                )
            })
            .collect::<Vec<_>>();
        RebalanceOutcome::new(
            uuid::Uuid::new_v4(),
            0,
            close_signals.len(),
            0,
            1_000_000.0,
            close_signals,
        )
    }

    #[test]
    fn test_evaluation_trigger_reads_the_emitters_reason() {
        assert_eq!(
            evaluation_trigger_from(&serde_json::json!({"reason": "live_threshold_crossing"})),
            RebalanceTrigger::LiveCrossing
        );
        assert_eq!(
            evaluation_trigger_from(&serde_json::json!({"reason": "entry_retry", "attempt": 1})),
            RebalanceTrigger::EntryRetry
        );
    }

    #[test]
    fn test_evaluation_trigger_falls_back_to_manual() {
        // Anything not emitted by the two in-process emitters came from outside.
        assert_eq!(
            evaluation_trigger_from(&serde_json::json!({})),
            RebalanceTrigger::Manual
        );
        assert_eq!(
            evaluation_trigger_from(&serde_json::json!({"reason": "something_else"})),
            RebalanceTrigger::Manual
        );
        assert_eq!(
            evaluation_trigger_from(&serde_json::json!({"reason": 7})),
            RebalanceTrigger::Manual
        );
    }

    #[test]
    fn test_trigger_disagreement_is_silent_when_the_pass_agrees() {
        let payload = serde_json::json!({
            "reason": "live_threshold_crossing",
            "pair_id": "AAPL-MSFT",
            "z_score": 4.2,
        });
        // Nothing to assert beyond not panicking: the pass closed the flagged
        // pair, so the divergence path must not be taken.
        report_trigger_disagreement(&payload, &outcome_closing(&["AAPL-MSFT"]));
    }

    #[test]
    fn test_trigger_disagreement_handles_a_declined_pair() {
        let payload = serde_json::json!({
            "reason": "live_threshold_crossing",
            "pair_id": "AAPL-MSFT",
            "z_score": 4.2,
        });
        report_trigger_disagreement(&payload, &outcome_closing(&["KO-PEP"]));
        report_trigger_disagreement(&payload, &outcome_closing(&[]));
    }

    #[test]
    fn test_trigger_disagreement_ignores_a_request_without_a_pair() {
        // Entry retries and manual requests carry no pair, so there is nothing
        // to disagree about.
        report_trigger_disagreement(
            &serde_json::json!({"reason": "entry_retry"}),
            &outcome_closing(&[]),
        );
    }

    #[test]
    fn test_consumer_names_are_stable() {
        assert_eq!(CONSUMER_PORTFOLIO, "portfolio");
        assert_eq!(CONSUMER_PORTFOLIO_SESSION, "portfolio-session");
        assert_eq!(CONSUMER_PORTFOLIO_LIQUIDATION, "portfolio-liquidation");
    }

    #[test]
    fn test_event_type_strings_are_stable() {
        assert_eq!(
            EventType::EquityPredictions(Outcome::Completed).as_str(),
            "equity_predictions_completed"
        );
        assert_eq!(
            EventType::EquityPredictions(Outcome::Errored).as_str(),
            "equity_predictions_errored"
        );
        assert_eq!(
            EventType::PortfolioLiquidation(Outcome::Requested).as_str(),
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

    use super::{
        close_is_near, handle_portfolio_evaluation, handle_trading_session_started,
        spawn_entry_retry_timer, spawn_liquidation_timer,
    };
    use crate::common::market_hours::MarketSession;
    use crate::portfolio::alpaca::MockTrading;
    use crate::portfolio::state::AppState;
    use chrono::{DateTime, Utc};
    use std::sync::Arc;
    use std::time::Duration;
    use tokio_util::sync::CancellationToken;

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

    // --- session start ---

    #[tokio::test]
    async fn test_session_start_skips_when_the_market_does_not_trade_today() {
        // Asked on Thursday July 4th, Alpaca reports Friday's close, so no
        // session trades today and nothing should be built or armed.
        let mock = MockTrading {
            market_open: false,
            session_close: utc("2024-07-05T20:00:00Z"),
            ..MockTrading::default()
        };
        let state = make_test_state(mock);
        let token = CancellationToken::new();

        handle_trading_session_started(&state, state.pool(), 1, &token).await;

        // Returned before claiming the rebalance slot.
        assert!(!state.rebalance_in_progress());
    }

    #[tokio::test]
    async fn test_session_start_skips_when_the_clock_is_unavailable() {
        // Fails closed, unlike the quote stream: without a session there is no
        // close to schedule liquidation against.
        let mock = MockTrading {
            should_fail_session_fetch: true,
            ..MockTrading::default()
        };
        let state = make_test_state(mock);
        let token = CancellationToken::new();

        handle_trading_session_started(&state, state.pool(), 1, &token).await;

        assert!(!state.rebalance_in_progress());
    }

    #[tokio::test]
    async fn test_entry_retry_backoff_widens_and_is_bounded() {
        // Bounded so a persistent failure does not retry all session, and
        // widening so the first attempt catches a brief outage quickly.
        assert_eq!(ENTRY_RETRY_BACKOFF_MINUTES, [5, 15, 30]);
        let mut previous = 0;
        for minutes in ENTRY_RETRY_BACKOFF_MINUTES {
            assert!(minutes > previous, "backoff must widen");
            previous = minutes;
        }
        // Every attempt lands inside the session, so none can be armed only to
        // be discarded by the close-proximity guard.
        let total: i64 = ENTRY_RETRY_BACKOFF_MINUTES.iter().sum();
        assert!(total < 390 - LIQUIDATION_LEAD_TIME_MINUTES);
    }

    #[tokio::test]
    async fn test_entry_retry_cancels_on_shutdown() {
        // The first attempt is five minutes out, so a test can only observe
        // that cancellation resolves the task rather than waiting it out.
        let pool = sqlx::postgres::PgPoolOptions::new()
            .connect_lazy("postgres://localhost:1/nonexistent_consumer_test")
            .expect("lazy pool creation should not fail");
        let token = CancellationToken::new();

        spawn_entry_retry_timer(pool, regular_session(), token.clone());
        token.cancel();

        tokio::time::timeout(Duration::from_secs(5), async {
            while !token.is_cancelled() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("cancellation should resolve promptly");
    }

    #[tokio::test]
    async fn test_liquidation_timer_cancels_on_shutdown() {
        // The timer must not outlive the process it was armed by. Its wait is
        // hours long, so a test can only observe that cancellation resolves it
        // rather than waiting the timer out.
        let session = regular_session();
        let pool = sqlx::postgres::PgPoolOptions::new()
            .connect_lazy("postgres://localhost:1/nonexistent_consumer_test")
            .expect("lazy pool creation should not fail");
        let token = CancellationToken::new();

        spawn_liquidation_timer(pool, &session, token.clone());
        token.cancel();

        // A cancelled token resolves the timer's select arm immediately; if the
        // sleep won instead this would hang until the test harness timed out.
        tokio::time::timeout(Duration::from_secs(5), async {
            while !token.is_cancelled() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("cancellation should resolve promptly");
    }

    #[tokio::test]
    async fn test_evaluation_skips_when_market_closed() {
        let mock = MockTrading {
            market_open: false,
            ..MockTrading::default()
        };
        let state = make_test_state(mock);

        handle_portfolio_evaluation(&state, state.pool(), &serde_json::json!({})).await;

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
        handle_portfolio_evaluation(&state, state.pool(), &serde_json::json!({})).await;

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

        handle_portfolio_evaluation(&state, state.pool(), &serde_json::json!({})).await;

        // Still held by the simulated in-flight pass, not cleared by the skip.
        assert!(state.rebalance_in_progress());
    }

    /// Regular session on 2024-07-15: 09:30–16:00 EDT.
    fn regular_session() -> MarketSession {
        MarketSession::new(true, utc("2024-07-15T20:00:00Z")).expect("regular session")
    }

    #[test]
    fn test_close_is_near_inside_the_lead_time() {
        // 15:50 EDT: ten minutes to a 16:00 close, inside the lead time, so an
        // evaluation arriving now must decline rather than open positions.
        assert!(close_is_near(
            &regular_session(),
            utc("2024-07-15T19:50:00Z")
        ));
    }

    #[test]
    fn test_close_is_not_near_mid_session() {
        // 13:00 EDT: three hours to the close.
        assert!(!close_is_near(
            &regular_session(),
            utc("2024-07-15T17:00:00Z")
        ));
    }

    #[test]
    fn test_close_is_near_once_the_close_has_passed() {
        // time_until_close saturates at zero, so a close already behind us
        // counts as near and the pass still declines.
        assert!(close_is_near(
            &regular_session(),
            utc("2024-07-15T20:30:00Z")
        ));
    }

    #[test]
    fn test_close_proximity_follows_an_early_close() {
        // 12:50 Eastern. Under a 16:00 close this is mid-session; under a 13:00
        // early close it is ten minutes out. The distinction is the reason the
        // proximity check reads Alpaca's session rather than a wall clock.
        let now = utc("2024-07-03T16:50:00Z");
        let early_close =
            MarketSession::new(true, utc("2024-07-03T17:00:00Z")).expect("early close session");
        let regular_close =
            MarketSession::new(true, utc("2024-07-03T20:00:00Z")).expect("regular close session");

        assert!(close_is_near(&early_close, now));
        assert!(!close_is_near(&regular_close, now));
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
    async fn test_prediction_request_claimable_before_first_request() {
        let mock = MockTrading::default();
        let state = make_test_state(mock);

        assert!(state.try_claim_prediction_request());
    }

    #[tokio::test]
    async fn test_prediction_request_claim_is_exclusive() {
        // The duplicate-inference guard: a second caller inside the backoff
        // window must not also emit.
        let mock = MockTrading::default();
        let state = make_test_state(mock);

        assert!(state.try_claim_prediction_request());
        assert!(!state.try_claim_prediction_request());
    }

    #[tokio::test]
    async fn test_prediction_request_claimable_again_after_backoff() {
        let mock = MockTrading::default();
        let state = make_test_state(mock);

        assert!(state.try_claim_prediction_request());
        // Backdate past the retry window: a run that died without emitting a
        // terminal event must not wedge the day.
        state.last_prediction_request_at_atomic().store(
            Utc::now().timestamp() - 11 * 60,
            std::sync::atomic::Ordering::SeqCst,
        );

        assert!(state.try_claim_prediction_request());
    }

    #[tokio::test]
    async fn test_released_claim_is_immediately_reclaimable() {
        // A failed emission gives the window back rather than costing the
        // session ten minutes of predictions over a transient error.
        let mock = MockTrading::default();
        let state = make_test_state(mock);

        let previous = state.last_prediction_request_at();
        assert!(state.try_claim_prediction_request());
        assert!(!state.try_claim_prediction_request());

        state.release_prediction_request(previous);

        assert!(state.try_claim_prediction_request());
    }

    #[tokio::test]
    async fn test_concurrent_claims_admit_exactly_one() {
        let mock = MockTrading::default();
        let state = make_test_state(mock);

        let mut handles = Vec::new();
        for _ in 0..8 {
            let state = state.clone();
            handles.push(tokio::spawn(
                async move { state.try_claim_prediction_request() },
            ));
        }

        let mut granted = 0;
        for handle in handles {
            if handle.await.expect("task should not panic") {
                granted += 1;
            }
        }
        assert_eq!(granted, 1, "exactly one caller may request predictions");
    }

    // --- Liquidation trigger ---

    #[test]
    fn test_liquidation_trigger_reads_both_emitters() {
        assert_eq!(
            LiquidationTrigger::from_payload(
                &serde_json::json!({"reason": "session_close_approaching"})
            ),
            LiquidationTrigger::SessionCloseApproaching
        );
        assert_eq!(
            LiquidationTrigger::from_payload(&serde_json::json!({"reason": "fail_safe_schedule"})),
            LiquidationTrigger::FailSafeSchedule
        );
    }

    #[test]
    fn test_liquidation_trigger_falls_back_to_unknown() {
        // A payload written by an older build, or a reason this build does not
        // know, must not be silently attributed to either real path.
        for payload in [
            serde_json::json!({}),
            serde_json::json!({"reason": "something_else"}),
            serde_json::json!({"reason": 7}),
        ] {
            assert_eq!(
                LiquidationTrigger::from_payload(&payload),
                LiquidationTrigger::Unknown
            );
        }
    }

    #[test]
    fn test_liquidation_trigger_names_round_trip() {
        // The timer's payload and the cron job's payload are written from these
        // names, so a rename on one side without the other would silently
        // degrade every liquidation to Unknown.
        for trigger in [
            LiquidationTrigger::SessionCloseApproaching,
            LiquidationTrigger::FailSafeSchedule,
        ] {
            assert_eq!(
                LiquidationTrigger::from_payload(&serde_json::json!({"reason": trigger.as_str()})),
                trigger
            );
        }
    }

    #[test]
    fn test_catch_up_and_unknown_are_not_reachable_from_a_payload() {
        // Both exist to describe how the handler was reached, not what a cron
        // job wrote, so neither should be parseable from a reason string.
        assert_ne!(
            LiquidationTrigger::from_payload(&serde_json::json!({"reason": "catch_up"})),
            LiquidationTrigger::CatchUp
        );
        assert_eq!(LiquidationTrigger::CatchUp.as_str(), "catch_up");
        assert_eq!(LiquidationTrigger::Unknown.as_str(), "unknown");
    }
}
