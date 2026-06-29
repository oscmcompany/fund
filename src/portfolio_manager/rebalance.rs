//! Main rebalance orchestration pipeline.
//!
//! `run_rebalance` drives the full lifecycle across four private phases:
//! 1. `fetch_market_data` — load predictions and price history from the database
//! 2. `close_existing_positions` — close open Alpaca positions and mark them done
//! 3. `check_drawdown` — gate on account equity vs previous NAV
//! 4. `select_size_execute` — select pairs, size, filter shortable, trade
//! 5. `persist_filled_pairs` — write session, pairs, allocations, orders, and snapshot

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use chrono::{DateTime, Utc};
use num_traits::ToPrimitive;
use rust_decimal::Decimal;
use tokio::sync::RwLock;
use tracing::{info, warn};
use uuid::Uuid;

use crate::common::events::{emit_event, EventType};
use crate::domain::market::Ticker;
use crate::domain::orders::FilledPair;
use crate::domain::portfolio::{Portfolio, PortfolioError};
use crate::domain::predictions::EquityPrediction;
use crate::domain::trading::{
    AllocationAction, AllocationSide, EquityAllocation, EquityOrder, EquityPair, EquityPairStatus,
    EquityRebalanceSession, RebalanceSessionStatus,
};
use crate::portfolio_manager::alpaca::AlpacaTradingClient;
use crate::portfolio_manager::beta::compute_market_betas;
use crate::portfolio_manager::consolidation::{consolidate_predictions, ConsolidatedSignal};
use crate::portfolio_manager::database::{
    close_equity_pair, close_equity_pair_end_of_day, fetch_equity_details, fetch_historical_prices,
    fetch_latest_portfolio_net_asset_value, fetch_live_quote_mid_prices, fetch_open_pairs,
    fetch_predictions, fetch_spy_prices, insert_equity_allocation, insert_equity_order,
    insert_equity_pair, insert_portfolio_snapshot, insert_rebalance_session,
    update_rebalance_session_status,
};
use crate::portfolio_manager::execution::{
    close_positions, confirm_fills, execute_open_pairs, ExecutionError,
};
use crate::portfolio_manager::regime::classify_regime;
use crate::portfolio_manager::sizing::{size_pairs_with_volatility_parity, SizingError};
use crate::portfolio_manager::state::AppState;
use crate::portfolio_manager::statistical_arbitrage::select_pairs;

/// Outcome of a completed rebalance cycle.
#[derive(Debug)]
pub struct RebalanceOutcome {
    pub session_id: Uuid,
    pub pairs_filled: usize,
    pub net_asset_value: f64,
}

/// Error returned when `run_rebalance` cannot complete the cycle.
#[derive(Debug)]
pub enum RebalanceError {
    /// Database query or insert failed.
    Database(sqlx::Error),
    /// Predictions are absent or stale.
    StalePredictions,
    /// Regime is trending; stat-arb exposure is reduced to zero.
    TrendingRegime,
    /// Drawdown threshold was breached; trading is halted.
    DrawdownBreached { current: f64, threshold: f64 },
    /// Pair sizing produced fewer than the required minimum.
    InsufficientPairs(SizingError),
    /// The filled portfolio failed its invariant checks.
    PortfolioInvalid(PortfolioError),
    /// Alpaca returned an error during position close or order submission.
    Execution(ExecutionError),
    /// A numeric type conversion failed (e.g. f64 → Decimal or Decimal → f64).
    Conversion(String),
}

impl std::fmt::Display for RebalanceError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RebalanceError::Database(error) => write!(formatter, "Database error: {error}"),
            RebalanceError::StalePredictions => {
                write!(formatter, "Predictions are absent or stale.")
            }
            RebalanceError::TrendingRegime => {
                write!(formatter, "Trending regime detected; skipping rebalance.")
            }
            RebalanceError::DrawdownBreached { current, threshold } => write!(
                formatter,
                "Drawdown breach: current NAV {current:.2} below threshold {threshold:.2}."
            ),
            RebalanceError::InsufficientPairs(error) => {
                write!(formatter, "Pair sizing failed: {error}")
            }
            RebalanceError::PortfolioInvalid(error) => {
                write!(formatter, "Portfolio validation failed: {error}")
            }
            RebalanceError::Execution(error) => write!(formatter, "Execution error: {error}"),
            RebalanceError::Conversion(message) => {
                write!(formatter, "Numeric conversion failed: {message}")
            }
        }
    }
}

impl std::error::Error for RebalanceError {}

impl From<sqlx::Error> for RebalanceError {
    fn from(error: sqlx::Error) -> Self {
        RebalanceError::Database(error)
    }
}

impl From<SizingError> for RebalanceError {
    fn from(error: SizingError) -> Self {
        RebalanceError::InsufficientPairs(error)
    }
}

impl From<PortfolioError> for RebalanceError {
    fn from(error: PortfolioError) -> Self {
        RebalanceError::PortfolioInvalid(error)
    }
}

/// Runs one complete rebalance cycle.
///
/// Returns `RebalanceOutcome` on success or a `RebalanceError` describing
/// why the cycle was skipped or failed. All database writes are committed
/// individually (no wrapping transaction) so partial progress is visible
/// in structured logs.
pub async fn run_rebalance(state: &AppState) -> Result<RebalanceOutcome, RebalanceError> {
    let pool = state.pool();
    let alpaca = state.alpaca_client();

    // Phase 1: load predictions and market data.
    let (predictions, historical_prices, spy_prices, equity_details) =
        fetch_market_data(pool).await?;

    // Phase 2: classify regime; skip if trending.
    let regime_result = classify_regime(&spy_prices);
    let exposure_scale = regime_result.state.exposure_factor();
    if exposure_scale < 0.6 {
        info!(
            regime = ?regime_result.state,
            confidence = ?regime_result.confidence.value(),
            "Trending regime detected; skipping rebalance"
        );
        return Err(RebalanceError::TrendingRegime);
    }

    // Phase 3: consolidate signals.
    let signals = consolidate_predictions(&predictions, &historical_prices, &equity_details);
    info!(tickers = signals.len(), "Signals consolidated");

    // Phase 4: close old positions, check drawdown, then select/size/execute.
    close_existing_positions(alpaca, pool).await?;
    let (current_equity, buying_power) = check_drawdown(
        alpaca,
        pool,
        state.constraints().drawdown_threshold().0.value(),
    )
    .await?;

    let required_pairs = state.constraints().minimum_pairs().0.get() as usize;
    let filled = select_size_execute(
        alpaca,
        pool,
        state.tradable_assets(),
        &signals,
        &historical_prices,
        &spy_prices,
        buying_power,
        exposure_scale,
        state.candidate_pool_count(),
        required_pairs,
    )
    .await?;

    // Phase 5: validate portfolio invariants.
    let filled_pairs_only: Vec<FilledPair> = filled
        .iter()
        .map(|(filled_pair, _)| filled_pair.clone())
        .collect();
    let portfolio = Portfolio::new(filled_pairs_only, state.constraints())?;

    // Phase 6: persist session, pairs, allocations, orders, and snapshot.
    let session_id = Uuid::new_v4();
    let now = Utc::now();

    let session = EquityRebalanceSession::new(
        session_id,
        now,
        "market_session_check".to_string(),
        None,
        None,
        RebalanceSessionStatus::Completed,
    );
    insert_rebalance_session(pool, &session).await?;

    let total_slippage_cost = persist_filled_pairs(pool, session_id, now, &filled).await?;

    let net_asset_value_decimal = Decimal::try_from(current_equity).map_err(|_| {
        RebalanceError::Conversion("current equity cannot be represented as Decimal".to_string())
    })?;
    insert_portfolio_snapshot(pool, now, net_asset_value_decimal, total_slippage_cost).await?;
    update_rebalance_session_status(pool, session_id, &RebalanceSessionStatus::Completed, now)
        .await?;

    let pairs_filled = portfolio.pairs().len();
    let net_asset_value = net_asset_value_decimal.to_f64().ok_or_else(|| {
        RebalanceError::Conversion(
            "net_asset_value_decimal cannot be represented as f64".to_string(),
        )
    })?;

    emit_event(
        pool,
        EventType::PortfolioRebalanceCompleted,
        &serde_json::json!({
            "session_id": session_id.to_string(),
            "pairs_filled": pairs_filled,
            "net_asset_value": net_asset_value,
        }),
    )
    .await?;

    info!(
        session_id = %session_id,
        pairs_filled = pairs_filled,
        net_asset_value = net_asset_value,
        "Rebalance completed"
    );

    Ok(RebalanceOutcome {
        session_id,
        pairs_filled,
        net_asset_value,
    })
}

/// Closes all open positions at end of day and emits `portfolio_liquidation_completed`.
///
/// Fetches open pairs, submits close orders via Alpaca, marks each pair closed
/// with `close_reason = 'end_of_day'`, then emits the completion event so
/// data_manager can unsubscribe from the WebSocket quote stream.
///
/// Returns the number of pairs closed, or a `RebalanceError` if Alpaca or
/// the database returns an error.
pub async fn run_end_of_day_liquidation(state: &AppState) -> Result<usize, RebalanceError> {
    let pool = state.pool();
    let alpaca = state.alpaca_client();

    let open_pairs = fetch_open_pairs(pool).await?;
    if open_pairs.is_empty() {
        info!("No open pairs to close at end of day");
        emit_event(
            pool,
            EventType::PortfolioLiquidationCompleted,
            &serde_json::json!({"pairs_closed": 0}),
        )
        .await?;
        return Ok(0);
    }

    let close_tickers: Vec<String> = open_pairs
        .iter()
        .flat_map(|pair| {
            [
                pair.long_ticker().to_string(),
                pair.short_ticker().to_string(),
            ]
        })
        .collect();

    close_positions(alpaca, &close_tickers)
        .await
        .map_err(RebalanceError::Execution)?;

    let closed_at = Utc::now();
    for open_pair in &open_pairs {
        close_equity_pair_end_of_day(pool, open_pair.id(), closed_at).await?;
    }

    let pairs_closed = open_pairs.len();
    info!(count = pairs_closed, "Open pairs closed at end of day");

    emit_event(
        pool,
        EventType::PortfolioLiquidationCompleted,
        &serde_json::json!({ "pairs_closed": pairs_closed }),
    )
    .await?;

    Ok(pairs_closed)
}

// ---------------------------------------------------------------------------
// Private pipeline phases
// ---------------------------------------------------------------------------

/// Fetches and staleness-checks today's predictions, then loads market data.
///
/// Returns `(predictions, historical_prices, spy_prices, equity_details)`.
/// Errors with `StalePredictions` when no valid predictions exist for today.
async fn fetch_market_data(
    pool: &sqlx::PgPool,
) -> Result<
    (
        Vec<EquityPrediction>,
        HashMap<Ticker, Vec<f64>>,
        Vec<f64>,
        HashMap<Ticker, String>,
    ),
    RebalanceError,
> {
    let fresh_predictions = fetch_predictions(pool).await?;
    let predictions = fresh_predictions
        .get()
        .ok_or(RebalanceError::StalePredictions)?;

    if predictions.is_empty() {
        warn!("No predictions available for today; skipping rebalance");
        return Err(RebalanceError::StalePredictions);
    }

    let predictions = predictions.to_vec();

    let (historical_prices_result, spy_prices_result, equity_details_result) = tokio::join!(
        fetch_historical_prices(pool),
        fetch_spy_prices(pool),
        fetch_equity_details(pool),
    );

    Ok((
        predictions,
        historical_prices_result?,
        spy_prices_result?,
        equity_details_result?,
    ))
}

/// Closes all currently open pair positions in Alpaca and marks them closed in the DB.
async fn close_existing_positions(
    alpaca: &AlpacaTradingClient,
    pool: &sqlx::PgPool,
) -> Result<(), RebalanceError> {
    let open_pairs = fetch_open_pairs(pool).await?;
    if open_pairs.is_empty() {
        return Ok(());
    }

    let close_tickers: Vec<String> = open_pairs
        .iter()
        .flat_map(|pair| {
            [
                pair.long_ticker().to_string(),
                pair.short_ticker().to_string(),
            ]
        })
        .collect();

    close_positions(alpaca, &close_tickers)
        .await
        .map_err(RebalanceError::Execution)?;

    let closed_at = Utc::now();
    for open_pair in &open_pairs {
        close_equity_pair(pool, open_pair.id(), closed_at).await?;
    }

    info!(count = open_pairs.len(), "Open pairs closed");
    Ok(())
}

/// Fetches current account equity and checks the drawdown guard.
///
/// Returns `(current_equity, buying_power)` when within the allowed drawdown.
/// Errors with `DrawdownBreached` when the drop from the previous NAV exceeds
/// the configured threshold.
async fn check_drawdown(
    alpaca: &AlpacaTradingClient,
    pool: &sqlx::PgPool,
    threshold: f64,
) -> Result<(f64, f64), RebalanceError> {
    let account = alpaca.get_account().await.map_err(|error| {
        RebalanceError::Execution(ExecutionError::PositionClose {
            ticker: "account".to_string(),
            source: error,
        })
    })?;

    let current_equity = account.equity;
    let buying_power = account.buying_power;

    if let Some(previous_nav) = fetch_latest_portfolio_net_asset_value(pool).await? {
        let drop_fraction = if previous_nav > 0.0 {
            (previous_nav - current_equity) / previous_nav
        } else {
            0.0
        };
        if drop_fraction > threshold {
            warn!(
                current_equity = current_equity,
                previous_nav = previous_nav,
                drop_fraction = drop_fraction,
                drawdown_threshold = threshold,
                "Drawdown threshold breached; halting rebalance"
            );
            return Err(RebalanceError::DrawdownBreached {
                current: current_equity,
                threshold,
            });
        }
    }

    Ok((current_equity, buying_power))
}

/// Selects candidate pairs, sizes them, filters to shortable tickers, and executes orders.
///
/// Returns the filled pairs paired with their sizing metadata. Errors with
/// `InsufficientPairs` when no fills are confirmed.
#[allow(clippy::too_many_arguments)]
async fn select_size_execute(
    alpaca: &AlpacaTradingClient,
    pool: &sqlx::PgPool,
    tradable_assets_cache: &Arc<RwLock<Option<Arc<HashSet<String>>>>>,
    signals: &[ConsolidatedSignal],
    historical_prices: &HashMap<Ticker, Vec<f64>>,
    spy_prices: &[f64],
    buying_power: f64,
    exposure_scale: f64,
    candidate_pool: usize,
    required_pairs: usize,
) -> Result<Vec<(FilledPair, crate::portfolio_manager::sizing::SizedPair)>, RebalanceError> {
    let candidate_pairs = select_pairs(signals, historical_prices, candidate_pool);
    info!(
        candidates = candidate_pairs.len(),
        required = required_pairs,
        "Candidate pairs selected"
    );

    let market_betas = compute_market_betas(historical_prices, spy_prices);

    let all_tickers: Vec<Ticker> = candidate_pairs
        .iter()
        .flat_map(|pair| [pair.long_ticker().clone(), pair.short_ticker().clone()])
        .collect();

    let mut entry_prices = fetch_live_quote_mid_prices(pool, &all_tickers).await?;

    // Cold-start fallback: the live quote stream only subscribes to tickers held
    // in open positions, so on the first rebalance (or for any candidate without a
    // fresh quote) `equity_quotes` has no entry price. Fall back to the most recent
    // daily close, already loaded in `historical_prices` (ordered oldest to
    // newest), so the pair can still be sized.
    for ticker in &all_tickers {
        if !entry_prices.contains_key(ticker) {
            if let Some(latest_close) = historical_prices
                .get(ticker)
                .and_then(|closes| closes.last())
            {
                entry_prices.insert(ticker.clone(), *latest_close);
            }
        }
    }

    let sized_pairs = size_pairs_with_volatility_parity(
        &candidate_pairs,
        buying_power,
        &market_betas,
        &entry_prices,
        exposure_scale,
        required_pairs,
    )?;

    // Resolve the tradable asset universe from the session cache, populating it
    // on first use. Subsequent rebalances within the same service instance reuse
    // the cached Arc without cloning the underlying set.
    let tradable_assets: Arc<HashSet<String>> = {
        let read_guard = tradable_assets_cache.read().await;
        if let Some(assets) = read_guard.as_ref() {
            Arc::clone(assets)
        } else {
            drop(read_guard);
            let assets = Arc::new(
                alpaca
                    .fetch_tradable_assets()
                    .await
                    .map_err(|error| RebalanceError::Conversion(error.to_string()))?,
            );
            let mut write_guard = tradable_assets_cache.write().await;
            *write_guard = Some(Arc::clone(&assets));
            info!(count = assets.len(), "Tradable asset cache populated");
            assets
        }
    };

    let shortable_pairs: Vec<_> = sized_pairs
        .into_iter()
        .filter(|pair| tradable_assets.contains(pair.short_ticker().as_str()))
        .collect();

    let pending = execute_open_pairs(alpaca, &shortable_pairs).await;
    let filled = confirm_fills(alpaca, pending).await;

    if filled.is_empty() {
        warn!("No pairs filled; aborting rebalance session");
        return Err(RebalanceError::InsufficientPairs(
            SizingError::InsufficientPairs {
                found: 0,
                required: required_pairs,
            },
        ));
    }

    Ok(filled)
}

/// Persists pairs, allocations, and orders for a completed rebalance cycle.
///
/// Returns the total estimated slippage cost across all pairs (1 bp per leg).
async fn persist_filled_pairs(
    pool: &sqlx::PgPool,
    session_id: Uuid,
    now: DateTime<Utc>,
    filled: &[(FilledPair, crate::portfolio_manager::sizing::SizedPair)],
) -> Result<Decimal, RebalanceError> {
    let mut total_slippage_cost = Decimal::ZERO;

    for (filled_pair, sized_pair) in filled {
        let pair_uuid = Uuid::new_v4();

        let z_score_decimal = Decimal::try_from(sized_pair.z_score()).map_err(|_| {
            RebalanceError::Conversion("z_score cannot be represented as Decimal".to_string())
        })?;
        let hedge_ratio_decimal = Decimal::try_from(sized_pair.hedge_ratio()).map_err(|_| {
            RebalanceError::Conversion("hedge_ratio cannot be represented as Decimal".to_string())
        })?;
        let signal_strength_decimal =
            Decimal::try_from(sized_pair.signal_strength()).map_err(|_| {
                RebalanceError::Conversion(
                    "signal_strength cannot be represented as Decimal".to_string(),
                )
            })?;

        // Tickers in filled_pair come from orders built with validated SizedPair tickers;
        // the UNKNOWN fallback guards against any edge case where the string is malformed.
        let long_ticker = Ticker::new(&filled_pair.long.ticker)
            .unwrap_or_else(|| Ticker::new("UNKNOWN").expect("UNKNOWN is a valid ticker"));
        let short_ticker = Ticker::new(&filled_pair.short.ticker)
            .unwrap_or_else(|| Ticker::new("UNKNOWN").expect("UNKNOWN is a valid ticker"));

        let equity_pair = EquityPair::new(
            pair_uuid,
            session_id,
            sized_pair.pair_id().clone(),
            long_ticker.clone(),
            short_ticker.clone(),
            z_score_decimal,
            hedge_ratio_decimal,
            signal_strength_decimal,
            EquityPairStatus::Open,
            now,
            None,
            None,
            None,
        );
        insert_equity_pair(pool, &equity_pair).await?;

        let long_notional_decimal = filled_pair.long_notional.value();
        let long_entry_price_decimal = filled_pair.long.fill_price.unwrap_or(Decimal::ZERO);
        let long_alloc_id = Uuid::new_v4();
        let long_allocation = EquityAllocation::new(
            long_alloc_id,
            session_id,
            pair_uuid,
            now,
            None,
            long_ticker.clone(),
            AllocationSide::Long,
            AllocationAction::OpenPosition,
            long_notional_decimal,
            Some(long_entry_price_decimal),
            None,
            Some(long_notional_decimal),
        );
        insert_equity_allocation(pool, &long_allocation).await?;

        let short_notional_decimal = filled_pair.short_notional.value();
        let short_entry_price_decimal = filled_pair.short.fill_price.unwrap_or(Decimal::ZERO);
        let short_quantity_decimal = filled_pair.short.quantity;
        let short_alloc_id = Uuid::new_v4();
        let short_allocation = EquityAllocation::new(
            short_alloc_id,
            session_id,
            pair_uuid,
            now,
            None,
            short_ticker.clone(),
            AllocationSide::Short,
            AllocationAction::OpenPosition,
            short_notional_decimal,
            Some(short_entry_price_decimal),
            Some(short_quantity_decimal),
            None,
        );
        insert_equity_allocation(pool, &short_allocation).await?;

        let long_order_ticker = Ticker::new(&filled_pair.long.ticker)
            .unwrap_or_else(|| Ticker::new("UNKNOWN").expect("UNKNOWN is a valid ticker"));
        let long_order = EquityOrder::new(
            Uuid::new_v4(),
            long_alloc_id,
            filled_pair.long.submitted_at,
            long_order_ticker,
            AllocationSide::Long,
            filled_pair.long.quantity,
            filled_pair.long.order_type.clone(),
            filled_pair.long.limit_price,
            filled_pair.long.alpaca_order_id.clone(),
        );
        insert_equity_order(pool, &long_order).await?;

        let short_order_ticker = Ticker::new(&filled_pair.short.ticker)
            .unwrap_or_else(|| Ticker::new("UNKNOWN").expect("UNKNOWN is a valid ticker"));
        let short_order = EquityOrder::new(
            Uuid::new_v4(),
            short_alloc_id,
            filled_pair.short.submitted_at,
            short_order_ticker,
            AllocationSide::Short,
            filled_pair.short.quantity,
            filled_pair.short.order_type.clone(),
            filled_pair.short.limit_price,
            filled_pair.short.alpaca_order_id.clone(),
        );
        insert_equity_order(pool, &short_order).await?;

        // Slippage estimate: 1 bp per leg (0.01% of notional).
        let pair_notional = long_notional_decimal + short_notional_decimal;
        let slippage = pair_notional * Decimal::new(1, 4);
        total_slippage_cost += slippage;
    }

    Ok(total_slippage_cost)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rebalance_error_display_stale_predictions() {
        let error = RebalanceError::StalePredictions;
        assert!(format!("{error}").contains("stale"));
    }

    #[test]
    fn test_rebalance_error_display_trending_regime() {
        let error = RebalanceError::TrendingRegime;
        assert!(format!("{error}").contains("Trending"));
    }

    #[test]
    fn test_rebalance_error_display_drawdown_breached() {
        let error = RebalanceError::DrawdownBreached {
            current: 90_000.0,
            threshold: 0.10,
        };
        let message = format!("{error}");
        assert!(message.contains("Drawdown"));
        assert!(message.contains("90000"));
    }

    #[test]
    fn test_rebalance_error_display_insufficient_pairs() {
        let sizing_error = SizingError::InsufficientPairs {
            found: 3,
            required: 10,
        };
        let error = RebalanceError::InsufficientPairs(sizing_error);
        let message = format!("{error}");
        assert!(message.contains("sizing"));
    }

    #[test]
    fn test_rebalance_error_display_portfolio_invalid() {
        use crate::domain::portfolio::PortfolioError;
        let portfolio_error = PortfolioError::InsufficientPairs {
            required: 10,
            found: 5,
        };
        let error = RebalanceError::PortfolioInvalid(portfolio_error);
        let message = format!("{error}");
        assert!(message.contains("Portfolio"));
    }

    #[test]
    fn test_rebalance_error_display_database() {
        let db_error = sqlx::Error::RowNotFound;
        let error = RebalanceError::Database(db_error);
        let message = format!("{error}");
        assert!(message.contains("Database"));
    }

    #[test]
    fn test_rebalance_error_from_sqlx() {
        let db_error = sqlx::Error::RowNotFound;
        let error: RebalanceError = db_error.into();
        assert!(matches!(error, RebalanceError::Database(_)));
    }

    #[test]
    fn test_rebalance_error_from_sizing_error() {
        let sizing_error = SizingError::InsufficientPairs {
            found: 0,
            required: 10,
        };
        let error: RebalanceError = sizing_error.into();
        assert!(matches!(error, RebalanceError::InsufficientPairs(_)));
    }

    #[test]
    fn test_rebalance_error_from_portfolio_error() {
        use crate::domain::portfolio::PortfolioError;
        let portfolio_error = PortfolioError::InsufficientPairs {
            required: 10,
            found: 0,
        };
        let error: RebalanceError = portfolio_error.into();
        assert!(matches!(error, RebalanceError::PortfolioInvalid(_)));
    }

    #[test]
    fn test_rebalance_error_is_error_trait() {
        let error = RebalanceError::StalePredictions;
        let _boxed: Box<dyn std::error::Error> = Box::new(error);
    }

    #[test]
    fn test_rebalance_outcome_fields() {
        let outcome = RebalanceOutcome {
            session_id: Uuid::new_v4(),
            pairs_filled: 10,
            net_asset_value: 500_000.0,
        };
        assert_eq!(outcome.pairs_filled, 10);
        assert_eq!(outcome.net_asset_value, 500_000.0);
    }
}
