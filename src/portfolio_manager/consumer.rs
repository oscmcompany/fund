//! PostgreSQL event consumer for the portfolio_manager service.
//!
//! Listens on the `events` channel and:
//! - Emits `equity_predictions_requested` on each `market_session_check` tick.
//! - Runs a rebalance cycle on each `equity_predictions_completed` event.
//! - Runs end-of-day liquidation on each `portfolio_liquidation_requested` event.

use std::time::Duration;

use chrono::{Datelike, Timelike, Weekday};
use sqlx::postgres::PgListener;
use sqlx::PgPool;
use tokio::time::sleep;
use tracing::{error, info, warn};

use crate::common::events::{
    emit_event, get_consumer_offset, latest_event_after, update_consumer_offset, EventType,
    CONSUMER_PORTFOLIO_MANAGER, CONSUMER_PORTFOLIO_MANAGER_LIQUIDATION,
};
use crate::portfolio_manager::rebalance::{
    run_end_of_day_liquidation, run_rebalance, RebalanceError,
};
use crate::portfolio_manager::state::AppState;

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

    // Catch up on equity_predictions_completed that arrived while we were down.
    // Periodic market_session_check ticks are intentionally not caught up because
    // stale ticks carry no meaningful signal.
    let predictions_offset = get_consumer_offset(pool, CONSUMER_PORTFOLIO_MANAGER).await?;
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
    let liquidation_offset =
        get_consumer_offset(pool, CONSUMER_PORTFOLIO_MANAGER_LIQUIDATION).await?;
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
                update_consumer_offset(pool, CONSUMER_PORTFOLIO_MANAGER_LIQUIDATION, event_id).await
            {
                warn!(error = %error, "Failed to update liquidation consumer offset");
            }
        }
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

        if event_type == EventType::MarketSessionCheck.as_str() {
            handle_market_session_check(pool).await;
        } else if event_type == EventType::EquityPredictionsCompleted.as_str() {
            info!(event_id, "Received equity_predictions_completed");
            handle_equity_predictions_completed(state, pool, event_id).await;
        } else if event_type == EventType::PortfolioLiquidationRequested.as_str() {
            info!(event_id, "Received portfolio_liquidation_requested");
            handle_portfolio_liquidation(state, pool, event_id).await;
        }
    }
}

/// Returns true when the current UTC time falls within approximate US equity
/// market hours (14:30–21:00 UTC, weekdays). Used to guard against replaying a
/// missed end-of-day liquidation event after the market has already closed.
fn is_within_trading_session() -> bool {
    let now = chrono::Utc::now();
    if matches!(now.weekday(), Weekday::Sat | Weekday::Sun) {
        return false;
    }
    let minutes_utc = now.hour() * 60 + now.minute();
    // US equity open: ~14:30 UTC (09:30 ET), close: ~21:00 UTC (16:00 ET)
    (14 * 60 + 30..21 * 60).contains(&minutes_utc)
}

/// Emits `equity_predictions_requested` in response to a periodic market session check.
/// The ensemble_manager consumer picks this up and runs the prediction pipeline.
/// No consumer offset tracking because stale ticks carry no meaningful signal.
async fn handle_market_session_check(pool: &PgPool) {
    if let Err(error) = emit_event(
        pool,
        EventType::EquityPredictionsRequested,
        &serde_json::json!({}),
    )
    .await
    {
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
                pairs_filled = outcome.pairs_filled,
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

    if let Err(error) = update_consumer_offset(pool, CONSUMER_PORTFOLIO_MANAGER, event_id).await {
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

    if let Err(error) =
        update_consumer_offset(pool, CONSUMER_PORTFOLIO_MANAGER_LIQUIDATION, event_id).await
    {
        warn!(error = %error, "Failed to update liquidation consumer offset");
    }
}

#[cfg(test)]
mod tests {
    use super::{
        is_within_trading_session, CONSUMER_PORTFOLIO_MANAGER,
        CONSUMER_PORTFOLIO_MANAGER_LIQUIDATION,
    };
    use crate::common::events::EventType;

    #[test]
    fn test_consumer_names_are_stable() {
        assert_eq!(CONSUMER_PORTFOLIO_MANAGER, "portfolio-manager");
        assert_eq!(
            CONSUMER_PORTFOLIO_MANAGER_LIQUIDATION,
            "portfolio-manager-liquidation"
        );
    }

    #[test]
    fn test_event_type_strings_are_stable() {
        assert_eq!(
            EventType::EquityPredictionsCompleted.as_str(),
            "equity_predictions_completed"
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

    #[test]
    fn test_is_within_trading_session_returns_bool() {
        // Just verify it compiles and returns a bool; actual value depends on when the test runs.
        let _ = is_within_trading_session();
    }
}
