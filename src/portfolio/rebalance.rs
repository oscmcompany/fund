//! Main rebalance orchestration pipeline.
//!
//! `run_rebalance` follows a build-then-monitor model:
//! - **Build** (no open pairs): full portfolio construction with invariant validation
//! - **Monitor** (open pairs exist): evaluate each pair for close signals, close triggered
//!   ones, leave vacant slots unfilled
//!
//! Key functions:
//! 1. `fetch_market_data` — load predictions and price history from the database
//! 2. `evaluate_open_pairs` — check each open pair for close signals (convergence, stop-loss)
//! 3. `close_triggered_pairs` — close only pairs that hit a signal on Alpaca and in the DB
//! 4. `check_drawdown` — gate on account equity vs previous NAV
//! 5. `select_size_execute` — select, size, and execute new pairs (build path only)
//! 6. `persist_filled_pairs` — write session, pairs, allocations, orders, and snapshot

use std::collections::HashMap;
use std::sync::Arc;

/// Z-score magnitude that triggers a stop-loss close.
///
/// Entry is at |z| >= 2.0. A threshold of 4.0 means the spread has doubled
/// against the position relative to entry, indicating the mean-reversion
/// thesis has failed.
const STOP_LOSS_Z_SCORE_THRESHOLD: f64 = 4.0;

use chrono::{DateTime, Utc};
use num_traits::ToPrimitive;
use rust_decimal::Decimal;
use tokio::sync::RwLock;
use tracing::{error, info, warn};
use uuid::Uuid;

use crate::common::events::{emit_event, EventType};
use crate::domain::market::Ticker;
use crate::domain::orders::FilledPair;
use crate::domain::portfolio::{Portfolio, PortfolioError};
use crate::domain::predictions::EquityPrediction;
use crate::domain::trading::{
    AllocationAction, AllocationSide, CloseReason, EquityAllocation, EquityOrder, EquityPair,
    EquityPairStatus, EquityRebalanceSession, RebalanceSessionStatus,
};
use crate::portfolio::alpaca::{TradableAssets, TradingClient};
use crate::portfolio::beta::compute_market_betas;
use crate::portfolio::consolidation::{consolidate_predictions, ConsolidatedSignal};
use crate::portfolio::database::{
    close_equity_pair_with_reason, fetch_equity_details, fetch_equity_predictions,
    fetch_historical_equity_prices, fetch_latest_portfolio_net_asset_value, fetch_open_pairs,
    fetch_spy_equity_prices, insert_equity_allocation, insert_equity_order, insert_equity_pair,
    insert_portfolio_snapshot, insert_rebalance_session, insert_submitted_order,
    mark_order_cancelled, mark_order_filled, update_rebalance_session_status, OpenPair,
};
use crate::portfolio::execution::{
    close_positions, confirm_fills, execute_open_pairs, ExecutionError,
};
use crate::portfolio::math::z_score_last;
use crate::portfolio::regime::classify_regime;
use crate::portfolio::sizing::{size_pairs_with_volatility_parity, SizingError};
use crate::portfolio::state::AppState;
use crate::portfolio::statistical_arbitrage::select_pairs;

/// Outcome of a completed rebalance cycle.
#[derive(Debug)]
pub struct RebalanceOutcome {
    pub session_id: Uuid,
    pub pairs_opened: usize,
    pub pairs_closed: usize,
    pub pairs_kept: usize,
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

/// Runs one rebalance cycle using a cold-start / warm-start model.
///
/// Alpaca positions are the source of truth for deciding the path:
/// - **Cold start** (no Alpaca positions): build a fresh portfolio using account
///   equity as the capital base. Entry prices come from the Alpaca REST snapshot API
///   with daily close prices as fallback.
/// - **Warm start** (Alpaca has positions): evaluate existing pairs for close signals
///   (convergence, stop-loss). New pairs are never opened while any positions remain.
///
/// Returns `RebalanceOutcome` on success or a `RebalanceError` describing
/// why the cycle was skipped or failed.
pub async fn run_rebalance(state: &AppState) -> Result<RebalanceOutcome, RebalanceError> {
    let pool = state.pool();
    let alpaca = state.alpaca_client();

    // Phase 1: check Alpaca positions to decide build vs monitor path.
    let alpaca_positions = alpaca.fetch_positions().await.map_err(|error| {
        RebalanceError::Execution(ExecutionError::PositionFetch { source: error })
    })?;

    let open_pairs = fetch_open_pairs(pool).await?;

    if !alpaca_positions.is_empty() {
        // Alpaca has live positions — enter monitor mode.
        if open_pairs.is_empty() {
            warn!(
                alpaca_positions = alpaca_positions.len(),
                "Alpaca has positions but database has no open pairs; state mismatch"
            );
            return Err(RebalanceError::Execution(ExecutionError::StateMismatch {
                message: "Alpaca has positions but database has no open pairs".to_string(),
            }));
        }

        // Load historical prices for monitor evaluation.
        let historical_prices = fetch_historical_equity_prices(pool).await?;
        return run_monitor_cycle(state, pool, alpaca, &open_pairs, &historical_prices).await;
    }

    // Alpaca has no positions. If DB still has open pairs, they were closed
    // externally (manual close, Alpaca risk controls, corporate action). Mark
    // them closed with a reconciliation-specific reason and continue as cold start.
    if !open_pairs.is_empty() {
        error!(
            open_pairs = open_pairs.len(),
            "Database has open pairs but Alpaca has no positions; \
             marking stale pairs closed with reconciliation_alpaca_missing"
        );
        let closed_at = Utc::now();
        for pair in &open_pairs {
            close_equity_pair_with_reason(
                pool,
                pair.id(),
                closed_at,
                &CloseReason::ReconciliationAlpacaMissing,
            )
            .await?;
        }
    }

    // Cold start: no Alpaca positions. Build a fresh portfolio.
    info!("No Alpaca positions; building fresh portfolio");

    // Phase 2: load predictions and market data.
    let (predictions, historical_prices, spy_prices, equity_details) =
        fetch_market_data(pool).await?;

    // Phase 3: classify regime; skip if trending.
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

    // Phase 4: consolidate signals.
    let signals = consolidate_predictions(&predictions, &historical_prices, &equity_details);
    info!(tickers = signals.len(), "Signals consolidated");

    // Phase 5: check drawdown. Use current_equity as the capital base for sizing.
    let (current_equity, _buying_power) = check_drawdown(
        alpaca,
        pool,
        state.constraints().drawdown_threshold().0.value(),
    )
    .await?;

    // Phase 6: select, size, and execute new pairs using current_equity.
    let required_pairs = state.constraints().minimum_pairs().0.get() as usize;
    let filled = select_size_execute(
        pool,
        alpaca,
        state.tradable_assets(),
        &signals,
        &historical_prices,
        &spy_prices,
        current_equity,
        exposure_scale,
        state.candidate_pool_count(),
        required_pairs,
    )
    .await?;

    let pairs_opened = filled.len();

    // Phase 7: validate portfolio invariants on the fresh set.
    let filled_pairs_only: Vec<FilledPair> = filled
        .iter()
        .map(|(filled_pair, _)| filled_pair.clone())
        .collect();
    Portfolio::new(filled_pairs_only, state.constraints())?;

    // Phase 8: persist session, pairs, allocations, orders, and snapshot
    // inside a single transaction so a mid-cycle failure rolls back all writes.
    let session_id = Uuid::new_v4();
    let now = Utc::now();

    let net_asset_value_decimal = Decimal::try_from(current_equity).map_err(|_| {
        RebalanceError::Conversion("current equity cannot be represented as Decimal".to_string())
    })?;
    let net_asset_value = net_asset_value_decimal.to_f64().ok_or_else(|| {
        RebalanceError::Conversion(
            "net_asset_value_decimal cannot be represented as f64".to_string(),
        )
    })?;

    let mut transaction = pool.begin().await?;

    let session = EquityRebalanceSession::new(
        session_id,
        now,
        "market_session_check".to_string(),
        None,
        None,
        RebalanceSessionStatus::Completed,
    );
    insert_rebalance_session(&mut *transaction, &session).await?;

    let total_slippage_cost =
        persist_filled_pairs(&mut transaction, session_id, now, &filled).await?;

    insert_portfolio_snapshot(
        &mut *transaction,
        now,
        net_asset_value_decimal,
        total_slippage_cost,
    )
    .await?;
    update_rebalance_session_status(
        &mut *transaction,
        session_id,
        &RebalanceSessionStatus::Completed,
        now,
    )
    .await?;

    emit_event(
        &mut *transaction,
        EventType::PortfolioRebalanceCompleted,
        &serde_json::json!({
            "session_id": session_id.to_string(),
            "pairs_opened": pairs_opened,
            "pairs_closed": 0,
            "pairs_kept": 0,
            "net_asset_value": net_asset_value,
        }),
    )
    .await?;

    transaction.commit().await?;

    info!(
        session_id = %session_id,
        pairs_opened = pairs_opened,
        net_asset_value = net_asset_value,
        "Fresh portfolio built"
    );

    Ok(RebalanceOutcome {
        session_id,
        pairs_opened,
        pairs_closed: 0,
        pairs_kept: 0,
        net_asset_value,
    })
}

/// Monitor path: evaluate existing open pairs for close signals and persist a snapshot.
///
/// Closed pair slots are intentionally left vacant. If all pairs close during
/// evaluation, the next rebalance tick will trigger a fresh portfolio build.
async fn run_monitor_cycle(
    state: &AppState,
    pool: &sqlx::PgPool,
    alpaca: &TradingClient,
    open_pairs: &[OpenPair],
    historical_prices: &HashMap<Ticker, Vec<f64>>,
) -> Result<RebalanceOutcome, RebalanceError> {
    let close_signals = evaluate_open_pairs(open_pairs, historical_prices);
    let pairs_closed = close_triggered_pairs(alpaca, pool, &close_signals).await?;
    let pairs_kept = open_pairs.len() - pairs_closed;

    info!(
        pairs_kept = pairs_kept,
        pairs_closed = pairs_closed,
        "Monitor cycle: evaluated open pairs"
    );

    // Persist a snapshot even when no pairs were opened.
    let (current_equity, _buying_power) = check_drawdown(
        alpaca,
        pool,
        state.constraints().drawdown_threshold().0.value(),
    )
    .await?;

    let session_id = Uuid::new_v4();
    let now = Utc::now();

    let net_asset_value_decimal = Decimal::try_from(current_equity).map_err(|_| {
        RebalanceError::Conversion("current equity cannot be represented as Decimal".to_string())
    })?;
    let net_asset_value = net_asset_value_decimal.to_f64().ok_or_else(|| {
        RebalanceError::Conversion(
            "net_asset_value_decimal cannot be represented as f64".to_string(),
        )
    })?;

    let mut transaction = pool.begin().await?;

    let session = EquityRebalanceSession::new(
        session_id,
        now,
        "market_session_check".to_string(),
        None,
        None,
        RebalanceSessionStatus::Completed,
    );
    insert_rebalance_session(&mut *transaction, &session).await?;
    insert_portfolio_snapshot(
        &mut *transaction,
        now,
        net_asset_value_decimal,
        Decimal::ZERO,
    )
    .await?;
    update_rebalance_session_status(
        &mut *transaction,
        session_id,
        &RebalanceSessionStatus::Completed,
        now,
    )
    .await?;

    emit_event(
        &mut *transaction,
        EventType::PortfolioRebalanceCompleted,
        &serde_json::json!({
            "session_id": session_id.to_string(),
            "pairs_opened": 0,
            "pairs_closed": pairs_closed,
            "pairs_kept": pairs_kept,
            "net_asset_value": net_asset_value,
        }),
    )
    .await?;

    transaction.commit().await?;

    Ok(RebalanceOutcome {
        session_id,
        pairs_opened: 0,
        pairs_closed,
        pairs_kept,
        net_asset_value,
    })
}

/// Closes all open positions at end of day and emits `portfolio_liquidation_completed`.
///
/// Fetches open pairs, submits close orders via Alpaca, marks each pair closed
/// with `close_reason = 'end_of_day'`, then emits the completion event.
///
/// Returns the number of pairs closed, or a `RebalanceError` if Alpaca or
/// the database returns an error.
pub async fn run_end_of_day_liquidation(state: &AppState) -> Result<usize, RebalanceError> {
    let pool = state.pool();
    let alpaca = state.alpaca_client();

    let open_pairs = fetch_open_pairs(pool).await?;

    if open_pairs.is_empty() {
        // No DB pairs, but check Alpaca for orphaned positions.
        let alpaca_positions = alpaca.fetch_positions().await.map_err(|error| {
            RebalanceError::Execution(ExecutionError::PositionFetch { source: error })
        })?;

        if !alpaca_positions.is_empty() {
            warn!(
                alpaca_positions = alpaca_positions.len(),
                "No open pairs in database but Alpaca has positions; closing all Alpaca positions"
            );
            let orphan_tickers: Vec<String> = alpaca_positions
                .iter()
                .map(|position| position.symbol.clone())
                .collect();
            close_positions(alpaca, &orphan_tickers)
                .await
                .map_err(RebalanceError::Execution)?;
            info!(
                rows = orphan_tickers.len(),
                "Orphaned Alpaca positions closed at end of day"
            );
        } else {
            info!("No open pairs to close at end of day");
        }

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
    let pairs_closed = open_pairs.len();

    for open_pair in &open_pairs {
        close_equity_pair_with_reason(pool, open_pair.id(), closed_at, &CloseReason::EndOfDay)
            .await?;
    }

    emit_event(
        pool,
        EventType::PortfolioLiquidationCompleted,
        &serde_json::json!({ "pairs_closed": pairs_closed }),
    )
    .await?;

    info!(rows = pairs_closed, "Open pairs closed at end of day");

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
    let fresh_predictions = fetch_equity_predictions(pool).await?;
    let predictions = fresh_predictions
        .get()
        .ok_or(RebalanceError::StalePredictions)?;

    if predictions.is_empty() {
        warn!("No predictions available for today; skipping rebalance");
        return Err(RebalanceError::StalePredictions);
    }

    let predictions = predictions.to_vec();

    let (historical_prices_result, spy_prices_result, equity_details_result) = tokio::join!(
        fetch_historical_equity_prices(pool),
        fetch_spy_equity_prices(pool),
        fetch_equity_details(pool),
    );

    Ok((
        predictions,
        historical_prices_result?,
        spy_prices_result?,
        equity_details_result?,
    ))
}

/// A close signal produced by per-pair evaluation.
struct PairCloseSignal {
    open_pair: OpenPair,
    reason: CloseReason,
}

/// Evaluates each open pair for close signals using the current spread z-score.
///
/// The spread for each pair is `long_price - hedge_ratio * short_price`, computed
/// over the historical price window. The z-score of the latest spread value
/// determines the signal:
///
/// - **Profit taken**: the z-score has crossed back through zero relative to the
///   entry z-score sign, meaning the spread has converged (trade thesis played out).
/// - **Stop loss**: the z-score magnitude exceeds [`STOP_LOSS_Z_SCORE_THRESHOLD`]
///   and the sign matches the entry direction, meaning the spread has diverged
///   further against the position.
///
/// Pairs where either leg lacks historical price data are silently skipped (kept open).
fn evaluate_open_pairs(
    open_pairs: &[OpenPair],
    historical_prices: &HashMap<Ticker, Vec<f64>>,
) -> Vec<PairCloseSignal> {
    let mut signals = Vec::new();

    for pair in open_pairs {
        let long_prices = match historical_prices.get(pair.long_ticker()) {
            Some(prices) if prices.len() >= 2 => prices,
            _ => {
                warn!(
                    pair_id = pair.pair_id().as_str(),
                    "Insufficient long-leg price history for evaluation; keeping pair"
                );
                continue;
            }
        };
        let short_prices = match historical_prices.get(pair.short_ticker()) {
            Some(prices) if prices.len() >= 2 => prices,
            _ => {
                warn!(
                    pair_id = pair.pair_id().as_str(),
                    "Insufficient short-leg price history for evaluation; keeping pair"
                );
                continue;
            }
        };

        let common_length = long_prices.len().min(short_prices.len());
        let long_slice = &long_prices[long_prices.len() - common_length..];
        let short_slice = &short_prices[short_prices.len() - common_length..];

        let spread: Vec<f64> = long_slice
            .iter()
            .zip(short_slice.iter())
            .map(|(long, short)| long - pair.hedge_ratio() * short)
            .collect();

        let current_z = z_score_last(&spread);

        // z_score_last returns 0.0 when the spread has near-zero standard deviation
        // (degenerate/halted). Treating this as a real z-score would falsely trigger
        // convergence. Skip evaluation and keep the pair open.
        if current_z == 0.0 {
            info!(
                pair_id = pair.pair_id().as_str(),
                "Spread has zero variance; skipping evaluation and keeping pair"
            );
            continue;
        }

        // Convergence: z-score crossed back through zero relative to entry direction.
        let converged = (pair.entry_z_score() > 0.0 && current_z <= 0.0)
            || (pair.entry_z_score() < 0.0 && current_z >= 0.0);

        // Stop loss: z-score diverged further against the position past the threshold.
        let stopped_out = current_z.abs() >= STOP_LOSS_Z_SCORE_THRESHOLD
            && current_z.signum() == pair.entry_z_score().signum();

        if converged {
            info!(
                pair_id = pair.pair_id().as_str(),
                entry_z = pair.entry_z_score(),
                current_z = current_z,
                "Pair converged; closing with profit taken"
            );
            signals.push(PairCloseSignal {
                open_pair: pair.clone(),
                reason: CloseReason::ProfitTaken,
            });
        } else if stopped_out {
            info!(
                pair_id = pair.pair_id().as_str(),
                entry_z = pair.entry_z_score(),
                current_z = current_z,
                threshold = STOP_LOSS_Z_SCORE_THRESHOLD,
                "Pair diverged past stop-loss threshold; closing"
            );
            signals.push(PairCloseSignal {
                open_pair: pair.clone(),
                reason: CloseReason::StopLoss,
            });
        } else {
            info!(
                pair_id = pair.pair_id().as_str(),
                entry_z = pair.entry_z_score(),
                current_z = current_z,
                "Pair within range; keeping open"
            );
        }
    }

    signals
}

/// Closes the given pairs on Alpaca and marks them closed in the database.
///
/// Returns the number of pairs successfully closed.
async fn close_triggered_pairs(
    alpaca: &TradingClient,
    pool: &sqlx::PgPool,
    signals: &[PairCloseSignal],
) -> Result<usize, RebalanceError> {
    if signals.is_empty() {
        return Ok(0);
    }

    let close_tickers: Vec<String> = signals
        .iter()
        .flat_map(|signal| {
            [
                signal.open_pair.long_ticker().to_string(),
                signal.open_pair.short_ticker().to_string(),
            ]
        })
        .collect::<std::collections::HashSet<_>>()
        .into_iter()
        .collect();

    close_positions(alpaca, &close_tickers)
        .await
        .map_err(RebalanceError::Execution)?;

    let closed_at = Utc::now();
    for signal in signals {
        close_equity_pair_with_reason(pool, signal.open_pair.id(), closed_at, &signal.reason)
            .await?;
    }

    info!(rows = signals.len(), "Triggered pairs closed");
    Ok(signals.len())
}

/// Fetches current account equity and checks the drawdown guard.
///
/// Returns `(current_equity, buying_power)` when within the allowed drawdown.
/// Errors with `DrawdownBreached` when the drop from the previous NAV exceeds
/// the configured threshold.
async fn check_drawdown(
    alpaca: &TradingClient,
    pool: &sqlx::PgPool,
    threshold: f64,
) -> Result<(f64, f64), RebalanceError> {
    let account = alpaca.get_account().await.map_err(|error| {
        RebalanceError::Execution(ExecutionError::PositionFetch { source: error })
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
/// Submitted orders are persisted to the database before polling for fills,
/// ensuring that a crash between submission and fill confirmation leaves a
/// durable breadcrumb for reconciliation to resolve.
///
/// Returns the filled pairs paired with their sizing metadata. Errors with
/// `InsufficientPairs` when no fills are confirmed.
#[allow(clippy::too_many_arguments)]
async fn select_size_execute(
    pool: &sqlx::PgPool,
    alpaca: &TradingClient,
    tradable_assets_cache: &Arc<RwLock<Option<Arc<TradableAssets>>>>,
    signals: &[ConsolidatedSignal],
    historical_prices: &HashMap<Ticker, Vec<f64>>,
    spy_prices: &[f64],
    capital: f64,
    exposure_scale: f64,
    candidate_pool: usize,
    required_pairs: usize,
) -> Result<Vec<(FilledPair, crate::portfolio::sizing::SizedPair)>, RebalanceError> {
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

    // Fetch live quotes from Alpaca REST API for all candidate tickers.
    // Fall back to latest daily close price for any ticker without a quote.
    let ticker_strings: Vec<String> = all_tickers.iter().map(|t| t.to_string()).collect();
    let latest_quotes = alpaca
        .fetch_latest_quotes(&ticker_strings)
        .await
        .unwrap_or_else(|error| {
            warn!(error = %error, "Failed to fetch Alpaca quotes; falling back to close prices");
            Vec::new()
        });

    let mut entry_prices: HashMap<Ticker, f64> = latest_quotes
        .into_iter()
        .filter_map(|quote| Ticker::new(&quote.symbol).map(|ticker| (ticker, quote.mid_price)))
        .collect();

    // Fill in any missing tickers with the most recent daily close price.
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
        capital,
        &market_betas,
        &entry_prices,
        exposure_scale,
        required_pairs,
    )?;

    // Resolve the tradable asset universe from the session cache, populating it
    // on first use. Subsequent rebalances within the same service instance reuse
    // the cached Arc without cloning the underlying struct.
    let tradable_assets: Arc<TradableAssets> = {
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
            info!(
                tradable = assets.tradable_count(),
                shortable = assets.shortable_count(),
                "Tradable asset cache populated"
            );
            assets
        }
    };

    // Filter pairs to those where the long leg is tradable on Alpaca and the
    // short leg is both tradable and shortable (easy to borrow).
    let eligible_pairs: Vec<_> = sized_pairs
        .into_iter()
        .filter(|pair| {
            let long_ok = tradable_assets.is_tradable(pair.long_ticker().as_str());
            let short_ok = tradable_assets.is_shortable(pair.short_ticker().as_str());
            if !long_ok {
                info!(
                    ticker = pair.long_ticker().as_str(),
                    "Long leg not tradable on Alpaca; dropping pair"
                );
            }
            if !short_ok {
                info!(
                    ticker = pair.short_ticker().as_str(),
                    "Short leg not shortable on Alpaca; dropping pair"
                );
            }
            long_ok && short_ok
        })
        .collect();

    let pending = execute_open_pairs(alpaca, &eligible_pairs).await;

    // Persist submitted order records before polling for fills. Each order gets
    // a durable breadcrumb so that if the process crashes during fill polling,
    // the reconciliation process can find and resolve these orders.
    for (pending_pair, _sized_pair) in &pending {
        let long = pending_pair.long();
        if let Err(error) = insert_submitted_order(
            pool,
            long.id,
            &long.alpaca_order_id,
            &long.ticker,
            &long.side.to_string(),
            long.quantity,
            &long.order_type,
            long.submitted_at,
        )
        .await
        {
            warn!(
                alpaca_order_id = long.alpaca_order_id.as_str(),
                error = %error,
                "Failed to persist submitted order; continuing without durable tracking"
            );
        }
        let short = pending_pair.short();
        if let Err(error) = insert_submitted_order(
            pool,
            short.id,
            &short.alpaca_order_id,
            &short.ticker,
            &short.side.to_string(),
            short.quantity,
            &short.order_type,
            short.submitted_at,
        )
        .await
        {
            warn!(
                alpaca_order_id = short.alpaca_order_id.as_str(),
                error = %error,
                "Failed to persist submitted order; continuing without durable tracking"
            );
        }
    }

    // Collect all submitted order IDs before confirm_fills consumes the pending pairs.
    let all_submitted_order_ids: Vec<Uuid> = pending
        .iter()
        .flat_map(|(pending_pair, _)| [pending_pair.long().id, pending_pair.short().id])
        .collect();

    let filled = confirm_fills(alpaca, pending).await;

    // Cancel tracking records for orders that did not fill. Filled orders will
    // be updated with allocation_id in persist_filled_pairs.
    let filled_order_ids: std::collections::HashSet<Uuid> = filled
        .iter()
        .flat_map(|(filled_pair, _)| [filled_pair.long.id, filled_pair.short.id])
        .collect();
    for order_id in &all_submitted_order_ids {
        if !filled_order_ids.contains(order_id) {
            if let Err(error) = mark_order_cancelled(pool, *order_id).await {
                warn!(
                    order_id = %order_id,
                    error = %error,
                    "Failed to mark unfilled order as cancelled"
                );
            }
        }
    }

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
/// Accepts a mutable transaction reference so all writes participate in the
/// caller's transaction. Returns the total estimated slippage cost across all
/// pairs (1 bp per leg).
async fn persist_filled_pairs(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    session_id: Uuid,
    now: DateTime<Utc>,
    filled: &[(FilledPair, crate::portfolio::sizing::SizedPair)],
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
        insert_equity_pair(&mut **transaction, &equity_pair).await?;

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
        insert_equity_allocation(&mut **transaction, &long_allocation).await?;

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
        insert_equity_allocation(&mut **transaction, &short_allocation).await?;

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
        insert_equity_order(&mut **transaction, &long_order).await?;

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
        insert_equity_order(&mut **transaction, &short_order).await?;

        // Mark the original submitted order tracking records as filled.
        // Uses the Order ID from the filled pair (same UUID that was used in
        // insert_submitted_order). Silently handles the case where the submitted
        // record was never persisted (e.g., DB was unavailable at submission time).
        mark_order_filled(&mut **transaction, filled_pair.long.id, long_alloc_id, now).await?;
        mark_order_filled(
            &mut **transaction,
            filled_pair.short.id,
            short_alloc_id,
            now,
        )
        .await?;

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
            pairs_opened: 3,
            pairs_closed: 2,
            pairs_kept: 8,
            net_asset_value: 500_000.0,
        };
        assert_eq!(outcome.pairs_opened, 3);
        assert_eq!(outcome.pairs_closed, 2);
        assert_eq!(outcome.pairs_kept, 8);
        assert_eq!(outcome.net_asset_value, 500_000.0);
    }

    // --- evaluate_open_pairs tests ---

    use crate::domain::market::PairID;
    use crate::portfolio::database::OpenPair;

    fn make_open_pair(long: &str, short: &str, entry_z: f64, hedge_ratio: f64) -> OpenPair {
        OpenPair::new_for_test(
            Uuid::new_v4(),
            PairID::new(Ticker::new(long).unwrap(), Ticker::new(short).unwrap()),
            Ticker::new(long).unwrap(),
            Ticker::new(short).unwrap(),
            entry_z,
            hedge_ratio,
        )
    }

    /// Builds a synthetic price series with a linear trend.
    fn make_prices(length: usize, start: f64, step: f64) -> Vec<f64> {
        (0..length)
            .map(|index| start + step * index as f64)
            .collect()
    }

    #[test]
    fn test_evaluate_open_pairs_convergence_positive_entry() {
        // Entry z > 0 (spread was wide), and current spread has collapsed below mean → converged.
        let pair = make_open_pair("AAPL", "MSFT", 2.5, 1.0);
        // Long prices decrease, short prices increase → spread goes negative → z crosses zero.
        let mut prices = HashMap::new();
        prices.insert(Ticker::new("AAPL").unwrap(), make_prices(60, 150.0, -1.0));
        prices.insert(Ticker::new("MSFT").unwrap(), make_prices(60, 100.0, 1.0));

        let signals = evaluate_open_pairs(&[pair], &prices);
        assert_eq!(signals.len(), 1);
        assert_eq!(signals[0].reason, CloseReason::ProfitTaken);
    }

    #[test]
    fn test_evaluate_open_pairs_convergence_negative_entry() {
        // Entry z < 0 (spread was narrow), and current spread has widened above mean → converged.
        let pair = make_open_pair("AAPL", "MSFT", -2.5, 1.0);
        // Long prices increase, short prices decrease → spread goes positive → z crosses zero.
        let mut prices = HashMap::new();
        prices.insert(Ticker::new("AAPL").unwrap(), make_prices(60, 100.0, 1.0));
        prices.insert(Ticker::new("MSFT").unwrap(), make_prices(60, 150.0, -1.0));

        let signals = evaluate_open_pairs(&[pair], &prices);
        assert_eq!(signals.len(), 1);
        assert_eq!(signals[0].reason, CloseReason::ProfitTaken);
    }

    #[test]
    fn test_evaluate_open_pairs_stop_loss() {
        // Entry z > 0 and spread spikes at the end → z exceeds threshold → stop loss.
        let pair = make_open_pair("AAPL", "MSFT", 2.5, 1.0);
        // Build prices where the spread is stable at ~50 for most of the window,
        // then spikes dramatically at the end to produce z > 4.0.
        let mut long_prices = vec![150.0; 58];
        long_prices.push(400.0);
        long_prices.push(450.0);
        let short_prices = vec![100.0; 60];

        let mut prices = HashMap::new();
        prices.insert(Ticker::new("AAPL").unwrap(), long_prices);
        prices.insert(Ticker::new("MSFT").unwrap(), short_prices);

        let signals = evaluate_open_pairs(&[pair], &prices);
        assert_eq!(signals.len(), 1);
        assert_eq!(signals[0].reason, CloseReason::StopLoss);
    }

    #[test]
    fn test_evaluate_open_pairs_kept_within_range() {
        // Spread is gently increasing → z is positive but moderate → pair kept open.
        let pair = make_open_pair("AAPL", "MSFT", 2.5, 1.0);
        let mut prices = HashMap::new();
        // Long increases faster than short → spread grows linearly → z_score_last ≈ 1.73.
        prices.insert(Ticker::new("AAPL").unwrap(), make_prices(60, 150.0, 1.0));
        prices.insert(Ticker::new("MSFT").unwrap(), make_prices(60, 100.0, 0.5));

        let signals = evaluate_open_pairs(&[pair], &prices);
        assert!(signals.is_empty());
    }

    #[test]
    fn test_evaluate_open_pairs_missing_prices_skips_pair() {
        let pair = make_open_pair("AAPL", "MSFT", 2.5, 1.0);
        let prices = HashMap::new(); // No price data at all.

        let signals = evaluate_open_pairs(&[pair], &prices);
        assert!(signals.is_empty()); // Pair kept open due to missing data.
    }

    #[test]
    fn test_evaluate_open_pairs_multiple_mixed_signals() {
        let converging = make_open_pair("A", "B", 2.5, 1.0);
        let stable = make_open_pair("C", "D", 2.5, 1.0);
        let diverging = make_open_pair("E", "F", 2.5, 1.0);

        let mut prices = HashMap::new();
        // A-B: spread collapses → converged.
        prices.insert(Ticker::new("A").unwrap(), make_prices(60, 150.0, -1.0));
        prices.insert(Ticker::new("B").unwrap(), make_prices(60, 100.0, 1.0));
        // C-D: spread gently increasing → kept.
        prices.insert(Ticker::new("C").unwrap(), make_prices(60, 150.0, 1.0));
        prices.insert(Ticker::new("D").unwrap(), make_prices(60, 100.0, 0.5));
        // E-F: spread spikes at the end → stop loss.
        let mut long_e = vec![150.0; 58];
        long_e.push(400.0);
        long_e.push(450.0);
        prices.insert(Ticker::new("E").unwrap(), long_e);
        prices.insert(Ticker::new("F").unwrap(), vec![100.0; 60]);

        let signals = evaluate_open_pairs(&[converging, stable, diverging], &prices);
        assert_eq!(signals.len(), 2);

        let reasons: Vec<&CloseReason> = signals.iter().map(|signal| &signal.reason).collect();
        assert!(reasons.contains(&&CloseReason::ProfitTaken));
        assert!(reasons.contains(&&CloseReason::StopLoss));
    }

    #[test]
    fn test_evaluate_open_pairs_zero_variance_keeps_pair() {
        // When both legs have constant prices, the spread has zero variance and
        // z_score_last returns 0.0. The pair should be kept open, not falsely
        // closed as converged.
        let pair = make_open_pair("AAPL", "MSFT", 2.5, 1.0);
        let mut prices = HashMap::new();
        prices.insert(Ticker::new("AAPL").unwrap(), vec![150.0; 60]);
        prices.insert(Ticker::new("MSFT").unwrap(), vec![100.0; 60]);

        let signals = evaluate_open_pairs(&[pair], &prices);
        assert!(signals.is_empty());
    }

    #[test]
    fn test_evaluate_open_pairs_empty_input() {
        let signals = evaluate_open_pairs(&[], &HashMap::new());
        assert!(signals.is_empty());
    }

    #[test]
    fn test_stop_loss_threshold_is_documented() {
        // Verify the threshold constant is the expected value (guards against
        // accidental changes without updating documentation).
        assert!((STOP_LOSS_Z_SCORE_THRESHOLD - 4.0).abs() < f64::EPSILON);
    }
}
