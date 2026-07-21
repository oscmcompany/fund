//! PostgreSQL event consumer for the portfolio service.
//!
//! Listens on the `events` channel and:
//! - Emits `equity_predictions_requested` on each `market_session_check` tick.
//! - Runs a rebalance cycle on each `equity_predictions_completed` event.
//! - Runs end-of-day liquidation on each `portfolio_liquidation_requested` event.

use std::time::Duration;

use chrono::Utc;
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
use crate::common::market_hours::is_within_trading_session;
use crate::portfolio::rebalance::{run_end_of_day_liquidation, run_rebalance, RebalanceError};
use crate::portfolio::reconciliation;
use crate::portfolio::state::AppState;

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
    // Periodic market_session_check ticks are intentionally not caught up because
    // stale ticks carry no meaningful signal.
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
    // market was still open. Guarded by a trading-session window check.
    let liquidation_offset = get_consumer_offset(pool, CONSUMER_PORTFOLIO_LIQUIDATION).await?;
    if let Some(event_id) = latest_event_after(
        pool,
        EventType::PortfolioLiquidationRequested,
        liquidation_offset,
    )
    .await?
    {
        if is_within_trading_session() {
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

        if event_type == EventType::MarketSessionCheck.as_str() {
            handle_market_session_check(state, pool).await;
        } else if event_type == EventType::EquityPredictionsCompleted.as_str() {
            info!(event_id, "Received equity_predictions_completed");
            handle_equity_predictions_completed(state, pool, event_id).await;
        } else if event_type == EventType::EquityPredictionsErrored.as_str() {
            info!(
                event_id,
                "Received equity_predictions_errored; clearing rebalance cycle flag"
            );
            state.set_rebalance_cycle_in_progress(false);
        } else if event_type == EventType::PortfolioLiquidationRequested.as_str() {
            info!(event_id, "Received portfolio_liquidation_requested");
            handle_portfolio_liquidation(state, pool, event_id).await;
        }
    }

    Ok(())
}

/// Maximum duration (seconds) for a prediction-pipeline run before the in-progress
/// flag is considered stale. Set to one hour, well past any realistic pipeline run.
const STALE_CYCLE_SECS: i64 = 60 * 60;

/// Emits `equity_predictions_requested` in response to a periodic market session check.
///
/// Skips emission when:
/// - A rebalance cycle is already in progress (predictions still running or rebalance
///   executing), preventing duplicate concurrent cycles when the pipeline takes longer
///   than the 5-minute tick interval. If the flag has been set for longer than
///   [`STALE_CYCLE_SECS`], it is considered stale (e.g., upstream crash without an
///   `equity_predictions_errored` event) and is automatically reset so trading can resume.
/// - Alpaca reports the market is not open (handles holidays and early closes without
///   requiring a hardcoded calendar). Fails open: if the clock endpoint is unreachable,
///   the tick is skipped rather than risking a trade on degraded connectivity.
///
/// No consumer offset tracking because stale ticks carry no meaningful signal.
async fn handle_market_session_check(state: &AppState, pool: &PgPool) {
    if state.rebalance_cycle_in_progress() {
        let elapsed = Utc::now().timestamp() - state.rebalance_cycle_started_at();
        if elapsed < STALE_CYCLE_SECS {
            info!("Skipping market session check: rebalance cycle already in progress");
            return;
        }
        warn!(
            elapsed_minutes = elapsed / 60,
            "Rebalance cycle flag stale; resetting to allow new cycle"
        );
        state.set_rebalance_cycle_in_progress(false);
    }

    match state.alpaca_client().is_market_open().await {
        Ok(true) => {}
        Ok(false) => {
            info!("Skipping market session check: market is not open");
            return;
        }
        Err(error) => {
            warn!(error = %error, "Skipping market session check: market clock check failed");
            return;
        }
    }

    state.set_rebalance_cycle_in_progress(true);

    if let Err(error) = emit_event(
        pool,
        EventType::EquityPredictionsRequested,
        &serde_json::json!({}),
    )
    .await
    {
        // Emission failed; clear the flag so the next tick can retry.
        state.set_rebalance_cycle_in_progress(false);
        warn!(error = %error, "Failed to emit equity_predictions_requested");
    }
}

async fn handle_equity_predictions_completed(state: &AppState, pool: &PgPool, event_id: i64) {
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

    if let Err(error) = update_consumer_offset(pool, CONSUMER_PORTFOLIO, event_id).await {
        warn!(error = %error, "Failed to update consumer offset");
    }

    state.set_rebalance_cycle_in_progress(false);
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
    use super::{CONSUMER_PORTFOLIO, CONSUMER_PORTFOLIO_LIQUIDATION};
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
            EventType::MarketSessionCheck.as_str(),
            "market_session_check"
        );
    }
}
