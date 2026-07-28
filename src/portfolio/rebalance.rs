//! Main rebalance orchestration pipeline.
//!
//! `run_rebalance` runs a unified evaluation pass on every cycle:
//! 1. **Exit evaluation**: check each open pair for close signals (convergence, stop-loss),
//!    close triggered pairs on Alpaca and in the database.
//! 2. **Entry evaluation**: if vacant pair slots exist and market conditions allow (fresh
//!    predictions, non-trending regime), select, size, risk-gate, and execute new pairs
//!    using available capital proportional to the number of vacant slots.
//!
//! Key functions:
//! 1. `evaluate_open_pairs` — check each open pair for close signals
//! 2. `close_triggered_pairs` — close only pairs that hit a signal
//! 3. `check_drawdown` — gate account equity against the previous NAV
//! 4. `try_evaluate_entries` — load predictions, check regime, run entry pipeline
//! 5. `select_size_execute` — select, size, risk-gate, and execute new pairs
//! 6. `persist_filled_pairs` — write session, pairs, allocations, orders, and snapshot

use std::collections::{HashMap, HashSet};
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
use tracing::{debug, info, warn};
use uuid::Uuid;

use crate::common::events::{emit_event, EventType};
use crate::common::market_hours::MarketSession;
use crate::domain::freshness::StalenessWindow;
use crate::domain::market::{BookQualityLimits, PairID, Ticker, UsableQuote};
use crate::domain::orders::{FilledPair, Order, Pending};
use crate::domain::portfolio::{Portfolio, PortfolioError};
use crate::domain::trading::{
    AllocationAction, AllocationSide, CloseReason, EquityAllocation, EquityOrder, EquityPair,
    EquityPairStatus, EquityRebalanceSession, RebalanceSessionStatus,
};
use crate::portfolio::alpaca::{AccountInfo, TradableAssets, Trading};
use crate::portfolio::beta::compute_market_betas;
use crate::portfolio::consolidation::{consolidate_predictions, ConsolidatedSignal};
use crate::portfolio::database::{
    close_equity_pair_with_reason, fetch_equity_details, fetch_equity_predictions,
    fetch_historical_equity_prices, fetch_latest_portfolio_net_asset_value, fetch_open_pairs,
    fetch_spy_equity_prices, insert_equity_allocation, insert_equity_order, insert_equity_pair,
    insert_portfolio_snapshot, insert_rebalance_session, insert_submitted_order, mark_order_filled,
    update_rebalance_session_status, OpenPair,
};
use crate::portfolio::execution::{
    close_positions, confirm_fills, execute_open_pairs, ExecutionError,
};
use crate::portfolio::math::z_score_against;
use crate::portfolio::reconciliation;
use crate::portfolio::regime::classify_regime;
use crate::portfolio::risk_gate::{
    self, AssetType, LiquidityMetrics, PortfolioSnapshot, PositionRequest, PositionSnapshot,
    RiskGateDecision, StrategyId,
};
use crate::portfolio::sizing::{size_pairs_with_volatility_parity, SizingError};
use crate::portfolio::state::AppState;
use crate::portfolio::statistical_arbitrage::{
    score_candidate_pairs, select_disjoint_pairs, ScoredPair,
};

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

/// Runs one rebalance cycle with unified exit monitoring and incremental entry.
///
/// Every cycle performs both phases:
/// 1. **Exit evaluation**: if open pairs exist, evaluate each for close signals
///    (convergence, stop-loss) and close triggered pairs.
/// 2. **Entry evaluation**: if vacant pair slots exist and market conditions allow,
///    select, size, risk-gate, and execute new pairs using available capital.
///
/// Entry is gated on fresh predictions, a non-trending regime, and passing the
/// pre-trade risk gate. Exit evaluation runs unconditionally.
///
/// Returns `RebalanceOutcome` on success or a `RebalanceError` describing
/// why the cycle was skipped or failed.
pub async fn run_rebalance(state: &AppState) -> Result<RebalanceOutcome, RebalanceError> {
    let pool = state.pool();
    let alpaca = state.alpaca_client();

    // Phase 1: reconcile DB state against Alpaca positions.
    let reconciliation_report = match reconciliation::reconcile(pool, alpaca).await {
        Ok(report) => report,
        Err(reconciliation::ReconciliationError::AlpacaFetch(error)) => {
            return Err(RebalanceError::Execution(ExecutionError::PositionFetch {
                source: error,
            }));
        }
        Err(reconciliation::ReconciliationError::Database(error)) => {
            return Err(RebalanceError::Database(error));
        }
    };

    if reconciliation_report.orphans_closed > 0
        || reconciliation_report.pairs_marked_closed > 0
        || reconciliation_report.stale_orders_resolved > 0
    {
        info!(
            orphans_closed = reconciliation_report.orphans_closed,
            pairs_marked_closed = reconciliation_report.pairs_marked_closed,
            stale_orders_resolved = reconciliation_report.stale_orders_resolved,
            "Reconciliation resolved discrepancies before rebalance"
        );
    }

    // Phase 2: re-fetch state after reconciliation.
    let mut alpaca_positions = alpaca.fetch_positions().await.map_err(|error| {
        RebalanceError::Execution(ExecutionError::PositionFetch { source: error })
    })?;
    let open_pairs = fetch_open_pairs(pool).await?;

    // Sanity check: Alpaca has positions but DB has none after reconciliation.
    if !alpaca_positions.is_empty() && open_pairs.is_empty() {
        warn!(
            alpaca_positions = alpaca_positions.len(),
            "Alpaca has positions but database has no open pairs after reconciliation"
        );
        return Err(RebalanceError::Execution(ExecutionError::StateMismatch {
            message: "Alpaca has positions but database has no open pairs after reconciliation"
                .to_string(),
        }));
    }

    // Phase 3: load market data. Historical closes feed exit evaluation, so a
    // failure there genuinely aborts the pass.
    //
    // SPY closes feed entry sizing, the regime check, and the exposure
    // measurement — never exit evaluation. Its error is held rather than
    // propagated so that a failure isolated to this one query cannot abort the
    // pass that closes converged and stopped-out positions; it is re-raised in
    // the entry branch below, where it does matter.
    let (historical_prices, spy_prices) = tokio::join!(
        fetch_historical_equity_prices(pool),
        fetch_spy_equity_prices(pool)
    );
    let historical_prices = historical_prices?;
    let market_betas = match &spy_prices {
        Ok(prices) => compute_market_betas(&historical_prices, prices),
        Err(error) => {
            warn!(
                error = %error,
                "SPY price fetch failed; net beta unmeasured and entries blocked this pass"
            );
            HashMap::new()
        }
    };

    // Phase 4: exit evaluation — always runs when open pairs exist.
    let mut pairs_closed: usize = 0;
    if !open_pairs.is_empty() {
        // Tier 1: streamed mids, already filtered by the sixty-second guard.
        let mut exit_mid_prices = state.live_prices().fresh_mid_prices(Utc::now()).await;
        let streamed = exit_mid_prices.len();

        // Tier 2: one batched snapshot for legs the stream did not cover. Only
        // the gap is fetched, so a fully streamed book costs no request at all.
        // A leg stale in the cache is already absent from `fresh_mid_prices`,
        // so "not streamed" and "streamed but stale" are the same case here.
        let unpriced_legs: Vec<Ticker> = open_pairs
            .iter()
            .flat_map(|pair| [pair.long_ticker().clone(), pair.short_ticker().clone()])
            .filter(|ticker| !exit_mid_prices.contains_key(ticker))
            .collect::<HashSet<Ticker>>()
            .into_iter()
            .collect();

        let snapshot_filled = if unpriced_legs.is_empty() {
            0
        } else {
            let filled = fetch_validated_mid_prices(alpaca, &unpriced_legs, "exit").await;
            let count = filled.len();
            exit_mid_prices.extend(filled);
            count
        };

        info!(
            open_pairs = open_pairs.len(),
            legs = open_pairs.len() * 2,
            priced_from_stream = streamed,
            priced_from_snapshot = snapshot_filled,
            unpriced = unpriced_legs.len() - snapshot_filled,
            "Exit evaluation pricing"
        );
        let close_signals = evaluate_open_pairs(&open_pairs, &historical_prices, &exit_mid_prices);
        pairs_closed = close_triggered_pairs(alpaca, pool, &close_signals).await?;
        let pairs_kept_after_exits = open_pairs.len() - pairs_closed;
        info!(
            pairs_closed = pairs_closed,
            pairs_kept = pairs_kept_after_exits,
            "Exit evaluation completed"
        );
    }
    let pairs_remaining = open_pairs.len() - pairs_closed;

    // Re-fetch positions after exits so the risk gate sees post-exit exposure.
    if pairs_closed > 0 {
        alpaca_positions = alpaca.fetch_positions().await.map_err(|error| {
            RebalanceError::Execution(ExecutionError::PositionFetch { source: error })
        })?;
    }

    // Phase 5: drawdown check. Required for both snapshot persistence and entry gating.
    let account = alpaca.get_account().await.map_err(|error| {
        RebalanceError::Execution(ExecutionError::PositionFetch { source: error })
    })?;
    let previous_net_asset_value = fetch_latest_portfolio_net_asset_value(pool).await?;
    let account = check_drawdown(
        account,
        previous_net_asset_value,
        state.constraints().drawdown_threshold().0.value(),
    )?;
    let current_equity = account.equity;
    let buying_power = account.buying_power;

    // Measured after exits so the figures describe the capital actually
    // available to the entry phase that follows, not the pre-exit book.
    let utilization = measure_capital_utilization(&account, &alpaca_positions, &market_betas);
    info!(
        idle_cash = format!("{:.2}", utilization.idle_cash),
        idle_cash_percent = format!("{:.2}", utilization.idle_cash_fraction * 100.0),
        gross_exposure = format!("{:.2}", utilization.gross_exposure),
        net_exposure = format!("{:.2}", utilization.net_exposure),
        margin_utilization_percent = format!("{:.2}", utilization.margin_utilization * 100.0),
        net_beta = format!("{:.4}", utilization.net_beta),
        beta_coverage_percent = format!("{:.2}", utilization.beta_coverage_fraction * 100.0),
        open_positions = alpaca_positions.len(),
        "Capital utilization measured"
    );

    // Phase 6: entry evaluation — gated on predictions, regime, and vacant slots.
    let required_pairs = state.constraints().minimum_pairs().0.get() as usize;
    let vacant_slots = required_pairs.saturating_sub(pairs_remaining);

    let mut filled: Vec<(FilledPair, crate::portfolio::sizing::SizedPair)> = Vec::new();

    if vacant_slots > 0 {
        // Fetched here rather than before the exit phase so a transient clock
        // failure blocks only entries. Exits reduce risk and do not need the
        // session; aborting the pass on a clock error would leave converged and
        // stopped-out positions open for want of a timestamp.
        let market_session = alpaca.fetch_market_session().await.map_err(|error| {
            RebalanceError::Execution(ExecutionError::SessionFetch { source: error })
        })?;

        // Re-raise the held SPY error. Entries need it for both the regime check
        // and the beta-neutral solve; proceeding without it would size a basket
        // against no betas at all, which is worse than skipping the pass.
        let spy_prices = spy_prices?;

        // Load entry-specific data: predictions and equity details.
        let entry_result = try_evaluate_entries(
            state,
            pool,
            alpaca,
            &market_session,
            &historical_prices,
            &spy_prices,
            &market_betas,
            &alpaca_positions,
            current_equity,
            buying_power,
            vacant_slots,
            required_pairs,
        )
        .await;

        match entry_result {
            Ok(new_fills) => {
                filled = new_fills;
            }
            Err(EntrySkipReason::StalePredictions) => {
                if pairs_remaining == 0 && pairs_closed == 0 {
                    return Err(RebalanceError::StalePredictions);
                }
                warn!("Skipping new entries: stale or absent predictions");
            }
            Err(EntrySkipReason::TrendingRegime) => {
                if pairs_remaining == 0 && pairs_closed == 0 {
                    return Err(RebalanceError::TrendingRegime);
                }
                info!("Skipping new entries: trending regime detected");
            }
            Err(EntrySkipReason::InsufficientPairs(error)) => {
                if pairs_remaining == 0 && pairs_closed == 0 {
                    return Err(RebalanceError::InsufficientPairs(error));
                }
                info!("No new pairs found; continuing with exits only");
            }
            Err(EntrySkipReason::Other(error)) => {
                return Err(error);
            }
        }
    } else {
        info!(
            pairs_remaining = pairs_remaining,
            "No vacant pair slots; skipping entry evaluation"
        );
    }

    let pairs_opened = filled.len();
    let pairs_kept = pairs_remaining;

    // Phase 7: validate portfolio invariants when new pairs were opened on a
    // fresh (cold-start) portfolio. When adding to an existing portfolio, the
    // risk gate has already validated each individual entry.
    if pairs_opened > 0 && pairs_remaining == 0 {
        let filled_pairs_only: Vec<FilledPair> = filled
            .iter()
            .map(|(filled_pair, _)| filled_pair.clone())
            .collect();
        Portfolio::new(filled_pairs_only, state.constraints())?;
    }

    // Phase 8: persist session, pairs, allocations, orders, and snapshot.
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

    let rebalance_session = EquityRebalanceSession::new(
        session_id,
        now,
        "portfolio_evaluation".to_string(),
        None,
        None,
        RebalanceSessionStatus::Completed,
    );
    insert_rebalance_session(&mut *transaction, &rebalance_session).await?;

    let total_slippage_cost = if filled.is_empty() {
        Decimal::ZERO
    } else {
        persist_filled_pairs(&mut transaction, session_id, now, &filled).await?
    };

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
            "pairs_closed": pairs_closed,
            "pairs_kept": pairs_kept,
            "net_asset_value": net_asset_value,
        }),
    )
    .await?;

    transaction.commit().await?;

    info!(
        session_id = %session_id,
        pairs_opened = pairs_opened,
        pairs_closed = pairs_closed,
        pairs_kept = pairs_kept,
        net_asset_value = net_asset_value,
        "Rebalance cycle completed"
    );

    Ok(RebalanceOutcome {
        session_id,
        pairs_opened,
        pairs_closed,
        pairs_kept,
        net_asset_value,
    })
}

/// Reasons entry evaluation can be skipped without aborting the full cycle.
enum EntrySkipReason {
    StalePredictions,
    TrendingRegime,
    InsufficientPairs(SizingError),
    /// A non-recoverable error that should propagate.
    Other(RebalanceError),
}

/// Attempts to evaluate and execute new pair entries.
///
/// Loads predictions, checks regime, consolidates signals, sizes candidates,
/// applies the risk gate to each, and executes approved entries.
///
/// Returns filled pairs on success, or an `EntrySkipReason` explaining why
/// entry was skipped (which the caller may treat as non-fatal if exits
/// already occurred).
#[allow(clippy::too_many_arguments)]
async fn try_evaluate_entries(
    state: &AppState,
    pool: &sqlx::PgPool,
    alpaca: &dyn Trading,
    market_session: &MarketSession,
    historical_prices: &HashMap<Ticker, Vec<f64>>,
    spy_prices: &[f64],
    market_betas: &HashMap<Ticker, f64>,
    alpaca_positions: &[crate::portfolio::alpaca::Position],
    current_equity: f64,
    buying_power: f64,
    vacant_slots: usize,
    required_pairs: usize,
) -> Result<Vec<(FilledPair, crate::portfolio::sizing::SizedPair)>, EntrySkipReason> {
    // Load predictions.
    let fresh_predictions = fetch_equity_predictions(pool)
        .await
        .map_err(|error| EntrySkipReason::Other(RebalanceError::Database(error)))?;
    let predictions = fresh_predictions
        .get()
        .ok_or(EntrySkipReason::StalePredictions)?;
    if predictions.is_empty() {
        return Err(EntrySkipReason::StalePredictions);
    }
    let predictions = predictions.to_vec();

    // Load remaining market data for entry selection.
    let equity_details = fetch_equity_details(pool)
        .await
        .map_err(|error| EntrySkipReason::Other(error.into()))?;

    // Regime check: skip entries if trending.
    let regime_result = classify_regime(spy_prices);
    let exposure_scale = regime_result.state.exposure_factor();
    if exposure_scale < 0.6 {
        info!(
            regime = ?regime_result.state,
            confidence = ?regime_result.confidence.value(),
            "Trending regime detected; skipping new entries"
        );
        return Err(EntrySkipReason::TrendingRegime);
    }

    // Consolidate signals.
    let signals = consolidate_predictions(&predictions, historical_prices, &equity_details);
    info!(
        tickers = signals.len(),
        "Signals consolidated for entry evaluation"
    );

    // Compute available capital: capped at per-slot allocation to prevent
    // over-concentration when fewer candidates survive than vacant slots.
    let per_slot_capital = current_equity / required_pairs as f64;
    let slot_capped_capital = per_slot_capital * vacant_slots as f64;
    let available_capital = slot_capped_capital.min(current_equity);

    info!(
        vacant_slots = vacant_slots,
        available_capital = format!("{:.2}", available_capital),
        "Entry capital computed"
    );

    // Select, size, and execute.
    let result = select_size_execute(
        pool,
        alpaca,
        state.tradable_assets(),
        state.risk_gate_configuration(),
        market_session,
        alpaca_positions,
        current_equity,
        buying_power,
        &signals,
        historical_prices,
        market_betas,
        available_capital,
        exposure_scale,
        state.candidate_pool_count(),
        vacant_slots,
    )
    .await;

    match result {
        Ok(filled) => Ok(filled),
        Err(RebalanceError::InsufficientPairs(error)) => {
            Err(EntrySkipReason::InsufficientPairs(error))
        }
        Err(error) => Err(EntrySkipReason::Other(error)),
    }
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

/// Builds a `PortfolioSnapshot` from Alpaca account data and current positions
/// for use by the risk gate.
fn build_portfolio_snapshot(
    account_equity: f64,
    buying_power: f64,
    positions: &[crate::portfolio::alpaca::Position],
) -> PortfolioSnapshot {
    let position_snapshots = positions
        .iter()
        .map(|position| PositionSnapshot {
            ticker: position.symbol.clone(),
            market_value_absolute: position.market_value.abs(),
            strategy: StrategyId::StatisticalArbitrage,
        })
        .collect();

    PortfolioSnapshot {
        account_equity,
        buying_power,
        positions: position_snapshots,
    }
}

/// Capital deployment and market exposure measured at one point in a pass.
///
/// Every field is a ratio or a dollar amount derived from Alpaca account state
/// and current positions; nothing here influences a decision. It exists so the
/// two properties this project set out to control — that capital does not sit
/// idle, and that net beta does not drift as pairs exit at staggered times —
/// are observable per pass rather than inferred after the fact.
struct CapitalUtilization {
    /// Uninvested settled cash.
    idle_cash: f64,
    /// Idle cash as a fraction of account equity.
    idle_cash_fraction: f64,
    /// Sum of absolute position market values.
    gross_exposure: f64,
    /// Signed sum of position market values; long minus short.
    net_exposure: f64,
    /// Fraction of buying power consumed, per the risk gate's own definition.
    margin_utilization: f64,
    /// Beta-weighted net exposure divided by account equity.
    net_beta: f64,
    /// Share of gross exposure whose ticker had an estimable beta.
    ///
    /// `net_beta` is computed only over positions with a beta, so a low
    /// coverage figure means the reported beta describes a fraction of the
    /// book. Without it a near-zero `net_beta` reads as neutrality when it may
    /// only mean the exposed names were the ones that could not be estimated.
    beta_coverage_fraction: f64,
}

/// Measures capital deployment and net market exposure for the current pass.
///
/// `market_betas` maps ticker to estimated beta; positions whose ticker is
/// absent contribute to `gross_exposure` but not to `net_beta`, and lower
/// `beta_coverage_fraction` accordingly.
fn measure_capital_utilization(
    account: &AccountInfo,
    positions: &[crate::portfolio::alpaca::Position],
    market_betas: &HashMap<Ticker, f64>,
) -> CapitalUtilization {
    let gross_exposure: f64 = positions
        .iter()
        .map(|position| position.market_value.abs())
        .sum();
    let net_exposure: f64 = positions.iter().map(|position| position.market_value).sum();

    let mut beta_weighted_exposure = 0.0;
    let mut covered_exposure = 0.0;
    for position in positions {
        let Some(ticker) = Ticker::new(&position.symbol) else {
            continue;
        };
        let Some(beta) = market_betas.get(&ticker) else {
            continue;
        };
        beta_weighted_exposure += position.market_value * beta;
        covered_exposure += position.market_value.abs();
    }

    let equity = account.equity;
    let (idle_cash_fraction, net_beta) = if equity > 0.0 {
        (
            account.cash_amount / equity,
            beta_weighted_exposure / equity,
        )
    } else {
        (0.0, 0.0)
    };

    let beta_coverage_fraction = if gross_exposure > 0.0 {
        covered_exposure / gross_exposure
    } else {
        1.0
    };

    CapitalUtilization {
        idle_cash: account.cash_amount,
        idle_cash_fraction,
        gross_exposure,
        net_exposure,
        margin_utilization: risk_gate::margin_utilization(equity, account.buying_power),
        net_beta,
        beta_coverage_fraction,
    }
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
///
/// `current_mid_prices` carries validated same-session mid prices, streamed
/// where the feed covers a symbol and pulled from the REST snapshot where it
/// does not. Daily closes supply the distribution the z-score is measured
/// against; they never supply the current observation.
///
/// A pair whose legs are not both priced is kept and re-evaluated next pass. The
/// alternative, and the previous behaviour, was to fall back to `z_score_last`
/// over the daily series — which scored the position on the *prior session's
/// close*, up to 65 hours old on a Monday afternoon, and did so with a different
/// estimator than the live path. That produced closes triggered by a symbol
/// going quiet rather than by the spread moving.
fn evaluate_open_pairs(
    open_pairs: &[OpenPair],
    historical_prices: &HashMap<Ticker, Vec<f64>>,
    current_mid_prices: &HashMap<Ticker, f64>,
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

        // Both legs must be priced or neither is used: pricing one leg current
        // against the other's prior close would move the spread by a day of
        // drift in that leg alone and read as a signal.
        let (Some(long_mid), Some(short_mid)) = (
            current_mid_prices.get(pair.long_ticker()),
            current_mid_prices.get(pair.short_ticker()),
        ) else {
            info!(
                pair_id = pair.pair_id().as_str(),
                "No usable current price for both legs; keeping pair for re-evaluation"
            );
            continue;
        };

        // Measured against the historical distribution, not one that includes
        // it. Appending the current point first would let a large move pull the
        // mean toward itself and inflate the deviation, shrinking its own
        // z-score, and would put this pass on a different scale from
        // `PairBaseline::live_z_score`, which standardizes against history only.
        // The trigger would then fire on a larger magnitude than this pass
        // computes, keep the pair open, and fire again after the debounce.
        let current_spread = long_mid - pair.hedge_ratio() * short_mid;
        let current_z = z_score_against(&spread, current_spread);

        match close_reason_for(pair.entry_z_score(), current_z) {
            Some(CloseReason::ProfitTaken) => {
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
            }
            Some(reason) => {
                info!(
                    pair_id = pair.pair_id().as_str(),
                    entry_z = pair.entry_z_score(),
                    current_z = current_z,
                    threshold = STOP_LOSS_Z_SCORE_THRESHOLD,
                    "Pair diverged past stop-loss threshold; closing"
                );
                signals.push(PairCloseSignal {
                    open_pair: pair.clone(),
                    reason,
                });
            }
            None => {
                info!(
                    pair_id = pair.pair_id().as_str(),
                    entry_z = pair.entry_z_score(),
                    current_z = current_z,
                    "Pair within range; keeping open"
                );
            }
        }
    }

    signals
}

/// Returns the close reason implied by `current_z`, or `None` to keep the pair.
///
/// Shared by the authoritative exit evaluation and the live-quote trigger so a
/// single definition of "this pair should close" governs both. The two differ
/// only in how they arrive at `current_z`; they must never disagree on what it
/// means.
///
/// A `current_z` of exactly zero is treated as no signal rather than as
/// convergence: `z_score_last` returns zero when the spread has near-zero
/// variance, which happens on a halted or degenerate series, and reading that
/// as convergence would close healthy positions on missing data.
pub fn close_reason_for(entry_z_score: f64, current_z: f64) -> Option<CloseReason> {
    if current_z == 0.0 || !current_z.is_finite() {
        return None;
    }

    // Convergence: the z-score crossed back through zero relative to the
    // direction the pair was entered on.
    let converged =
        (entry_z_score > 0.0 && current_z <= 0.0) || (entry_z_score < 0.0 && current_z >= 0.0);
    if converged {
        return Some(CloseReason::ProfitTaken);
    }

    // Stop loss: the spread diverged further against the position past the
    // threshold, in the same direction it was entered on.
    let stopped_out = current_z.abs() >= STOP_LOSS_Z_SCORE_THRESHOLD
        && current_z.signum() == entry_z_score.signum();
    if stopped_out {
        return Some(CloseReason::StopLoss);
    }

    None
}

/// Closes the given pairs on Alpaca and marks them closed in the database.
///
/// Returns the number of pairs successfully closed.
async fn close_triggered_pairs(
    alpaca: &dyn Trading,
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

/// Checks account equity against the previous net asset value.
///
/// Returns `account` unchanged when within the allowed drawdown. Errors with
/// `DrawdownBreached` when the drop from `previous_net_asset_value` exceeds
/// `threshold`. A `None` previous value means there is no prior snapshot to
/// compare against, so the guard does not apply.
///
/// Pure by construction: the caller performs the account and snapshot reads and
/// hands the results in, matching how `risk_gate` keeps every check free of I/O.
/// Folding the fetches in here would make the passing path reachable only with a
/// live database, which is what left the guard untested before.
fn check_drawdown(
    account: AccountInfo,
    previous_net_asset_value: Option<f64>,
    threshold: f64,
) -> Result<AccountInfo, RebalanceError> {
    let current_equity = account.equity;

    if let Some(previous_nav) = previous_net_asset_value {
        // A non-positive previous value cannot express a meaningful fraction and
        // would divide by zero, so it is treated as no drawdown rather than as a
        // total loss.
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

    Ok(account)
}

/// Persists a single submitted order record for durable tracking.
///
/// Logs a warning and continues if the insert fails — the order has already
/// been submitted to Alpaca, so we must not abort the pipeline.
async fn persist_submitted_order(pool: &sqlx::PgPool, leg: &Order<Pending>) {
    if let Err(error) = insert_submitted_order(
        pool,
        leg.id,
        &leg.alpaca_order_id,
        &leg.ticker,
        &leg.side.to_string(),
        leg.quantity,
        &leg.order_type,
        leg.submitted_at,
    )
    .await
    {
        warn!(
            alpaca_order_id = leg.alpaca_order_id.as_str(),
            error = %error,
            "Failed to persist submitted order; continuing without durable tracking"
        );
    }
}

/// Selects candidate pairs, sizes them, filters to tradable and shortable
/// tickers, applies the pre-trade risk gate, and executes approved orders.
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
    alpaca: &dyn Trading,
    tradable_assets_cache: &Arc<RwLock<Option<Arc<TradableAssets>>>>,
    risk_gate_config: &risk_gate::RiskGateConfiguration,
    market_session: &MarketSession,
    alpaca_positions: &[crate::portfolio::alpaca::Position],
    current_equity: f64,
    buying_power: f64,
    signals: &[ConsolidatedSignal],
    historical_prices: &HashMap<Ticker, Vec<f64>>,
    market_betas: &HashMap<Ticker, f64>,
    capital: f64,
    exposure_scale: f64,
    candidate_pool: usize,
    minimum_pairs: usize,
) -> Result<Vec<(FilledPair, crate::portfolio::sizing::SizedPair)>, RebalanceError> {
    // Rank the full reservoir once. This is the quadratic half of selection, so
    // the convergence loop below re-selects from this list rather than rescoring.
    let scored_pairs = score_candidate_pairs(signals, historical_prices);
    info!(
        scored = scored_pairs.len(),
        working_set = candidate_pool,
        target = minimum_pairs,
        "Candidate pairs ranked"
    );

    let tradable_assets = resolve_tradable_assets(alpaca, tradable_assets_cache).await?;

    // Settle tradability and quote quality before the convergence loop. Both are
    // properties of the symbols rather than of how a set was sized, so neither
    // belongs inside a loop whose purpose is resolving the joint sizing problem.
    let (eligible_candidates, entry_prices) =
        screen_entry_candidates(alpaca, &scored_pairs, &tradable_assets).await;

    if eligible_candidates.is_empty() {
        info!("No candidates survived screening; no pairs will be opened");
        return Ok(Vec::new());
    }

    let eligible_pairs = converge_entry_set(
        risk_gate_config,
        market_session,
        &eligible_candidates,
        &entry_prices,
        market_betas,
        alpaca_positions,
        current_equity,
        buying_power,
        capital,
        exposure_scale,
        candidate_pool,
        minimum_pairs,
    )?;

    let pending = execute_open_pairs(alpaca, pool, &eligible_pairs).await;

    // Persist submitted order records before polling for fills. Each order gets
    // a durable breadcrumb so that if the process crashes during fill polling,
    // the reconciliation process can find and resolve these orders.
    for (pending_pair, _sized_pair) in &pending {
        persist_submitted_order(pool, pending_pair.long()).await;
        persist_submitted_order(pool, pending_pair.short()).await;
    }

    let filled = confirm_fills(alpaca, pending).await;

    if filled.is_empty() {
        warn!("No pairs filled; aborting rebalance session");
        return Err(RebalanceError::InsufficientPairs(
            SizingError::InsufficientPairs {
                found: 0,
                required: minimum_pairs,
            },
        ));
    }

    Ok(filled)
}

/// Iterations allowed beyond one per candidate in the working set.
///
/// Each iteration either excludes at least one pair or lowers the target, both
/// monotone, so the loop terminates on its own; the budget is a backstop against
/// an unforeseen cycle. Sizing every candidate out one at a time needs one
/// iteration each, and target reductions consume an iteration without excluding
/// anything, so the budget is the working set plus this headroom.
const CONVERGENCE_ITERATION_HEADROOM: usize = 4;

/// Resolves the tradable asset universe, populating the session cache on first use.
///
/// Subsequent rebalances within the same service instance reuse the cached `Arc`
/// without cloning the underlying struct.
async fn resolve_tradable_assets(
    alpaca: &dyn Trading,
    cache: &Arc<RwLock<Option<Arc<TradableAssets>>>>,
) -> Result<Arc<TradableAssets>, RebalanceError> {
    let read_guard = cache.read().await;
    if let Some(assets) = read_guard.as_ref() {
        return Ok(Arc::clone(assets));
    }
    drop(read_guard);

    let assets = Arc::new(
        alpaca
            .fetch_tradable_assets()
            .await
            .map_err(|error| RebalanceError::Conversion(error.to_string()))?,
    );
    let mut write_guard = cache.write().await;
    *write_guard = Some(Arc::clone(&assets));
    info!(
        tradable = assets.tradable_count(),
        shortable = assets.shortable_count(),
        "Tradable asset cache populated"
    );
    Ok(assets)
}

/// Fetches validated mid prices for `tickers` from the REST snapshot endpoint.
///
/// Quotes are gated through [`UsableQuote`] and the REST staleness window, so a
/// book too wide, too thin, or too old to price against is absent from the
/// result rather than averaged into a midpoint. A ticker missing from the
/// returned map has no usable price, and callers must drop it rather than
/// substitute one — see [`screen_quoted_candidates`] on the entry side and
/// [`evaluate_open_pairs`] on the exit side.
///
/// Rejections are counted and logged by cause. They are the measurement that
/// says whether the book-quality bound is set sensibly, and there is no
/// production history for this yet.
///
/// A failed request yields no prices. On the entry side that opens nothing this
/// pass; on the exit side it leaves pairs unpriced and therefore held. Both are
/// intended: acting on unavailable market data is worse than not acting.
///
/// `purpose` distinguishes the two call sites in the logs, which otherwise
/// produce identical lines from different phases of the same pass.
async fn fetch_validated_mid_prices(
    alpaca: &dyn Trading,
    tickers: &[Ticker],
    purpose: &'static str,
) -> HashMap<Ticker, f64> {
    if tickers.is_empty() {
        return HashMap::new();
    }

    let ticker_strings: Vec<String> = tickers.iter().map(Ticker::to_string).collect();
    let latest_quotes = alpaca
        .fetch_latest_quotes(&ticker_strings)
        .await
        .unwrap_or_else(|error| {
            warn!(error = %error, purpose, "Failed to fetch Alpaca quotes; symbols will be left unpriced");
            Vec::new()
        });

    let book_limits = BookQualityLimits::default();
    let staleness_window = StalenessWindow::rest_quotes();
    let now = Utc::now();
    let returned = latest_quotes.len();
    let mut rejected_book: usize = 0;
    let mut rejected_stale: usize = 0;

    let mut mid_prices: HashMap<Ticker, f64> = HashMap::new();
    for latest in latest_quotes {
        let Some(quote) = latest.to_equity_quote() else {
            continue;
        };
        let Some(usable) = UsableQuote::new(&quote, book_limits) else {
            rejected_book += 1;
            continue;
        };
        if now.signed_duration_since(usable.observed_at()) > staleness_window.0 {
            rejected_stale += 1;
            continue;
        }
        mid_prices.insert(quote.ticker().clone(), usable.mid_price());
    }

    info!(
        purpose,
        requested = tickers.len(),
        returned,
        accepted = mid_prices.len(),
        rejected_book,
        rejected_stale,
        "Snapshot quote pricing"
    );

    mid_prices
}
/// Keeps candidates whose long leg is tradable and short leg shortable.
///
/// Runs before sizing rather than after. Tradability is a property of the
/// symbol, fixed for the session and known from a cached asset list, so
/// discovering it after the joint sizing optimization wasted the whole
/// computation: a working set of ten could size successfully and then lose six
/// pairs here, leaving too few to meet the target and aborting the pass.
fn screen_tradable_candidates(
    scored_pairs: &[ScoredPair],
    tradable_assets: &TradableAssets,
) -> (Vec<ScoredPair>, usize) {
    let mut kept = Vec::new();
    let mut rejected: usize = 0;

    for scored in scored_pairs {
        let pair = scored.pair();
        let long_ok = tradable_assets.is_tradable(pair.long_ticker().as_str());
        // The short leg is checked for tradability as well as shortability.
        // `fetch_tradable_assets` only ever inserts into the shortable set from
        // inside the tradable branch, so shortable is a subset of tradable and
        // this rejects nothing today. It is here so that the subset property
        // has to hold for the screen to pass, rather than being an invariant of
        // a different function that this one silently depends on.
        let short_ok = tradable_assets.is_shortable(pair.short_ticker().as_str())
            && tradable_assets.is_tradable(pair.short_ticker().as_str());
        if long_ok && short_ok {
            kept.push(scored.clone());
        } else {
            // Debug rather than info: the untradable set is stable across a
            // session, so this repeats identically on every pass.
            debug!(
                pair_id = pair.pair_id().as_str(),
                long_tradable = long_ok,
                short_eligible = short_ok,
                "Candidate dropped: leg not tradable on Alpaca"
            );
            rejected += 1;
        }
    }

    (kept, rejected)
}

/// Keeps candidates whose legs both carry a usable, current quote.
///
/// A pair is dropped when either leg fails the book-quality gate or the REST
/// staleness window. It is dropped for this pass only: the snapshot is retaken
/// every pass, so a symbol quoting temporarily wide becomes eligible again once
/// it tightens, and no persistent state has to be reconciled when it does.
///
/// Dropping is the point. The previous behaviour substituted the prior daily
/// close whenever a quote was unusable, which meant a symbol rejected for a
/// three thousand basis point book was still sized and still entered, priced off
/// a number from the day before. Entering a position that cannot be priced is
/// entering one that cannot be exited.
fn screen_quoted_candidates(
    scored_pairs: Vec<ScoredPair>,
    entry_prices: &HashMap<Ticker, f64>,
) -> (Vec<ScoredPair>, usize) {
    let mut kept = Vec::new();
    let mut rejected: usize = 0;

    for scored in scored_pairs {
        let pair = scored.pair();
        let long_priced = entry_prices.contains_key(pair.long_ticker());
        let short_priced = entry_prices.contains_key(pair.short_ticker());
        if long_priced && short_priced {
            kept.push(scored);
        } else {
            debug!(
                pair_id = pair.pair_id().as_str(),
                long_priced, short_priced, "Candidate dropped: leg lacks a usable current quote"
            );
            rejected += 1;
        }
    }

    (kept, rejected)
}

/// Screens candidates down to those that can actually be opened, and prices them.
///
/// One batched snapshot call covers every distinct leg of every surviving
/// candidate; the client chunks internally, so the whole set goes in one call
/// from here regardless of size. The former arrangement fetched incrementally
/// inside the convergence loop, re-entering the network on each iteration, which
/// only made sense while the request was believed to be symbol-capped.
async fn screen_entry_candidates(
    alpaca: &dyn Trading,
    scored_pairs: &[ScoredPair],
    tradable_assets: &TradableAssets,
) -> (Vec<ScoredPair>, HashMap<Ticker, f64>) {
    let considered = scored_pairs.len();
    let (tradable_candidates, rejected_untradable) =
        screen_tradable_candidates(scored_pairs, tradable_assets);

    let legs: Vec<Ticker> = tradable_candidates
        .iter()
        .flat_map(|scored| {
            [
                scored.pair().long_ticker().clone(),
                scored.pair().short_ticker().clone(),
            ]
        })
        .collect::<HashSet<Ticker>>()
        .into_iter()
        .collect();

    let entry_prices = fetch_validated_mid_prices(alpaca, &legs, "entry").await;
    let (eligible, rejected_unquoted) =
        screen_quoted_candidates(tradable_candidates, &entry_prices);

    info!(
        considered,
        rejected_untradable,
        rejected_unquoted,
        eligible = eligible.len(),
        legs_priced = entry_prices.len(),
        legs_requested = legs.len(),
        "Entry candidates screened"
    );

    (eligible, entry_prices)
}

/// Applies the risk gate to each pair, returning approvals and rejected identifiers.
///
/// Legs are evaluated in rank order against a snapshot that accumulates each
/// approval, so a later pair sees the exposure earlier ones already claimed.
/// Order therefore matters, which is why selection hands pairs over ranked.
fn partition_risk_gated_pairs(
    risk_gate_config: &risk_gate::RiskGateConfiguration,
    market_session: &MarketSession,
    snapshot: &mut PortfolioSnapshot,
    pairs: Vec<crate::portfolio::sizing::SizedPair>,
    now_utc: DateTime<Utc>,
) -> (Vec<crate::portfolio::sizing::SizedPair>, Vec<PairID>) {
    let mut approved = Vec::new();
    let mut rejected = Vec::new();

    for pair in pairs {
        let long_request = PositionRequest {
            ticker: pair.long_ticker().to_string(),
            asset_type: AssetType::Equity,
            notional: pair.long_dollar_amount(),
            strategy: StrategyId::StatisticalArbitrage,
        };
        // ADV placeholder: entry price × 1M shares. A proper ADV data source
        // can be added later.
        let long_liquidity = LiquidityMetrics {
            average_daily_volume_dollars: pair.long_entry_price() * 1_000_000.0,
        };
        let long_decision = risk_gate::evaluate(
            risk_gate_config,
            snapshot,
            &long_request,
            &long_liquidity,
            market_session,
            now_utc,
        );
        if let RiskGateDecision::Rejected { reasons } = long_decision {
            for reason in &reasons {
                info!(
                    pair_id = pair.pair_id().as_str(),
                    leg = "long",
                    reason = %reason,
                    "Risk gate rejected pair"
                );
            }
            rejected.push(pair.pair_id().clone());
            continue;
        }

        // Temporarily add the approved long leg so the short leg evaluation
        // sees cumulative exposure.
        snapshot.positions.push(PositionSnapshot {
            ticker: pair.long_ticker().to_string(),
            market_value_absolute: pair.long_dollar_amount(),
            strategy: StrategyId::StatisticalArbitrage,
        });

        let short_request = PositionRequest {
            ticker: pair.short_ticker().to_string(),
            asset_type: AssetType::Equity,
            notional: pair.short_dollar_amount(),
            strategy: StrategyId::StatisticalArbitrage,
        };
        let short_liquidity = LiquidityMetrics {
            average_daily_volume_dollars: pair.short_entry_price() * 1_000_000.0,
        };
        let short_decision = risk_gate::evaluate(
            risk_gate_config,
            snapshot,
            &short_request,
            &short_liquidity,
            market_session,
            now_utc,
        );
        if let RiskGateDecision::Rejected { reasons } = short_decision {
            // Roll back the tentatively added long leg.
            snapshot.positions.pop();
            for reason in &reasons {
                info!(
                    pair_id = pair.pair_id().as_str(),
                    leg = "short",
                    reason = %reason,
                    "Risk gate rejected pair"
                );
            }
            rejected.push(pair.pair_id().clone());
            continue;
        }

        snapshot.positions.push(PositionSnapshot {
            ticker: pair.short_ticker().to_string(),
            market_value_absolute: pair.short_dollar_amount(),
            strategy: StrategyId::StatisticalArbitrage,
        });

        approved.push(pair);
    }

    (approved, rejected)
}

/// Converges on an entry set whose sizing matches the pairs actually executed.
///
/// Sizing is a joint optimization, not a per-pair calculation: volatility parity
/// weights are normalized across the set and beta neutrality is a whole-basket
/// constraint, so adding or removing one pair resizes every other. The risk gate
/// cannot run before sizing because every check needs a notional, and notionals
/// come from sizing. Running them in sequence therefore produced a basket sized
/// for N pairs but executed with fewer, whose parity weights no longer summed to
/// one and whose beta was no longer neutral.
///
/// The loop closes that gap. Each pass selects a working set, sizes it, and
/// gates it; any rejection excludes those pairs and re-runs the whole pass, so
/// whatever is finally executed was sized as exactly that set.
///
/// Only the risk gate can reject here. Tradability and quote quality are settled
/// by [`screen_entry_candidates`] before the loop starts, because neither
/// depends on how a pair was sized — feeding them through the joint optimization
/// only to discard the result was wasted work, and losing enough pairs that way
/// aborted the whole pass.
///
/// Two properties make re-selection worthwhile rather than merely correct:
///
/// - Excluding a pair frees both of its tickers, so pairs further down the
///   ranking that were skipped for a ticker collision become selectable. A
///   rejection can promote candidates from anywhere below it.
/// - The affordability threshold divides the per-pair capital allowance by the
///   target count, so lowering the target raises the threshold and can rescue
///   pairs that were previously unaffordable.
///
/// Degrades gracefully: a target that cannot be met is lowered rather than
/// aborting the pass, since filling four of ten slots beats filling none.
#[allow(clippy::too_many_arguments)]
fn converge_entry_set(
    risk_gate_config: &risk_gate::RiskGateConfiguration,
    market_session: &MarketSession,
    scored_pairs: &[ScoredPair],
    entry_prices: &HashMap<Ticker, f64>,
    market_betas: &HashMap<Ticker, f64>,
    alpaca_positions: &[crate::portfolio::alpaca::Position],
    current_equity: f64,
    buying_power: f64,
    capital: f64,
    exposure_scale: f64,
    working_set_limit: usize,
    target_pairs: usize,
) -> Result<Vec<crate::portfolio::sizing::SizedPair>, RebalanceError> {
    let mut excluded: HashSet<PairID> = HashSet::new();
    let mut target = target_pairs;

    // The iteration budget scales with the working set because each iteration
    // excludes only the pairs rejected in that pass. Rejections arriving a pair
    // at a time would otherwise exhaust a fixed budget routinely rather than
    // exceptionally, silently costing a whole entry pass. The extra headroom
    // covers target reductions, which consume an iteration without excluding
    // anything.
    let maximum_iterations = working_set_limit + CONVERGENCE_ITERATION_HEADROOM;

    for iteration in 1..=maximum_iterations {
        if target == 0 {
            info!("Entry target reduced to zero; no pairs will be opened");
            return Ok(Vec::new());
        }

        let working_set = select_disjoint_pairs(scored_pairs, working_set_limit, &excluded);
        if working_set.is_empty() {
            info!(
                iteration,
                excluded = excluded.len(),
                "No candidates remain; no pairs will be opened"
            );
            return Ok(Vec::new());
        }

        let sized_pairs = match size_pairs_with_volatility_parity(
            &working_set,
            capital,
            market_betas,
            entry_prices,
            exposure_scale,
            target,
        ) {
            Ok(sized) => sized,
            Err(SizingError::InsufficientPairs { found, required }) if found > 0 => {
                // Lowering the target also raises the per-pair affordability
                // allowance, so this can recover pairs the previous pass priced
                // out rather than simply shrinking the portfolio.
                warn!(
                    iteration,
                    found,
                    required,
                    requested_target = target_pairs,
                    "Entry target lowered: fewer feasible pairs than requested"
                );
                target = found;
                continue;
            }
            Err(error) => return Err(error.into()),
        };

        // Rebuilt every iteration: the snapshot accumulates approvals, so it
        // must start from real portfolio state rather than a previous attempt.
        let mut snapshot = build_portfolio_snapshot(current_equity, buying_power, alpaca_positions);
        let (approved, gate_rejected) = partition_risk_gated_pairs(
            risk_gate_config,
            market_session,
            &mut snapshot,
            sized_pairs,
            Utc::now(),
        );

        if gate_rejected.is_empty() {
            info!(
                iteration,
                approved = approved.len(),
                target,
                "Entry set converged"
            );
            return Ok(approved);
        }

        let newly_excluded = gate_rejected.len();
        excluded.extend(gate_rejected);
        info!(
            iteration,
            newly_excluded,
            excluded_total = excluded.len(),
            "Entry set rejected pairs; re-selecting and resizing"
        );
    }

    // Falling through means the set never stabilized. Returning nothing is the
    // safe outcome: the alternative is executing a basket whose sizing assumed
    // pairs that were rejected, which is the defect this loop exists to remove.
    // Logged at warn with the exclusion count so exhaustion is distinguishable
    // from a genuine absence of candidates, which otherwise look identical from
    // the caller's side — both return no pairs.
    warn!(
        iterations = maximum_iterations,
        excluded_total = excluded.len(),
        requested_target = target_pairs,
        final_target = target,
        "Entry set did not converge; opening no pairs this pass"
    );
    Ok(Vec::new())
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
        let long_allocation_id = Uuid::new_v4();
        let long_allocation = EquityAllocation::new(
            long_allocation_id,
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
        let short_allocation_id = Uuid::new_v4();
        let short_allocation = EquityAllocation::new(
            short_allocation_id,
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
            Some(long_allocation_id),
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
            Some(short_allocation_id),
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
        mark_order_filled(
            &mut **transaction,
            filled_pair.long.id,
            Some(long_allocation_id),
            now,
        )
        .await?;
        mark_order_filled(
            &mut **transaction,
            filled_pair.short.id,
            Some(short_allocation_id),
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

    // --- Entry candidate screening ---

    mod screening {
        use super::*;
        use crate::portfolio::alpaca::{LatestQuote, MockTrading};
        use crate::portfolio::statistical_arbitrage::CandidatePair;
        use chrono::Duration;

        fn ticker(symbol: &str) -> Ticker {
            Ticker::new(symbol).expect("valid ticker")
        }

        fn candidate(long: &str, short: &str) -> ScoredPair {
            let pair = CandidatePair::new(
                PairID::new(ticker(long), ticker(short)),
                ticker(long),
                ticker(short),
                2.0,
                1.0,
                0.05,
                0.02,
                0.02,
            )
            .expect("valid candidate pair");
            ScoredPair::new(pair, 1.0)
        }

        /// Builds an asset universe where every listed symbol is both tradable
        /// and shortable, and nothing else is.
        fn assets(symbols: &[&str]) -> TradableAssets {
            TradableAssets::from_sets(
                symbols.iter().map(|s| s.to_string()).collect(),
                symbols.iter().map(|s| s.to_string()).collect(),
            )
        }

        fn quote(symbol: &str, bid: f64, ask: f64, age: Duration) -> LatestQuote {
            LatestQuote {
                symbol: symbol.to_string(),
                bid_price: bid,
                ask_price: ask,
                bid_size: crate::domain::market::MINIMUM_QUOTE_SIZE,
                ask_size: crate::domain::market::MINIMUM_QUOTE_SIZE,
                observed_at: Utc::now() - age,
            }
        }

        #[test]
        fn test_screen_tradable_drops_pair_when_either_leg_ineligible() {
            let candidates = vec![
                candidate("AAPL", "MSFT"),
                candidate("GOOG", "META"),
                candidate("NVDA", "TSLA"),
            ];
            // META is absent, so GOOG-META loses its short leg.
            let universe = assets(&["AAPL", "MSFT", "GOOG", "NVDA", "TSLA"]);

            let (kept, rejected) = screen_tradable_candidates(&candidates, &universe);

            assert_eq!(rejected, 1);
            assert_eq!(kept.len(), 2);
            assert!(kept
                .iter()
                .all(|scored| scored.pair().pair_id().as_str() != "GOOG-META"));
        }

        #[test]
        fn test_screen_tradable_requires_short_leg_to_be_tradable_too() {
            // A symbol marked shortable but not tradable cannot arise from
            // `fetch_tradable_assets`, which only fills the shortable set from
            // inside the tradable branch. The screen must not depend on that
            // invariant holding elsewhere.
            let candidates = vec![candidate("AAPL", "MSFT")];
            let universe = TradableAssets::from_sets(
                ["AAPL".to_string()].into_iter().collect(),
                ["MSFT".to_string()].into_iter().collect(),
            );

            let (kept, rejected) = screen_tradable_candidates(&candidates, &universe);

            assert!(kept.is_empty());
            assert_eq!(rejected, 1);
        }

        #[test]
        fn test_screen_quoted_drops_pair_missing_either_price() {
            let candidates = vec![candidate("AAPL", "MSFT"), candidate("GOOG", "META")];
            let mut prices = HashMap::new();
            prices.insert(ticker("AAPL"), 180.0);
            prices.insert(ticker("MSFT"), 400.0);
            // GOOG priced, META not: the pair must go, not just the leg.
            prices.insert(ticker("GOOG"), 150.0);

            let (kept, rejected) = screen_quoted_candidates(candidates, &prices);

            assert_eq!(rejected, 1);
            assert_eq!(kept.len(), 1);
            assert_eq!(kept[0].pair().pair_id().as_str(), "AAPL-MSFT");
        }

        #[tokio::test]
        async fn test_fetch_validated_mid_prices_rejects_wide_and_stale_books() {
            let mock = MockTrading {
                latest_quotes: vec![
                    // Tight and current: accepted.
                    quote("AAPL", 180.00, 180.20, Duration::seconds(5)),
                    // 1,053 basis points wide: rejected on book quality.
                    quote("WIDE", 180.00, 200.00, Duration::seconds(5)),
                    // Tight but older than the five-minute REST window.
                    quote("OLD", 180.00, 180.20, Duration::seconds(400)),
                ],
                ..MockTrading::default()
            };

            let prices = fetch_validated_mid_prices(
                &mock,
                &[ticker("AAPL"), ticker("WIDE"), ticker("OLD")],
                "test",
            )
            .await;

            assert_eq!(prices.len(), 1);
            assert!((prices[&ticker("AAPL")] - 180.1).abs() < 1e-9);
            assert!(!prices.contains_key(&ticker("WIDE")));
            assert!(!prices.contains_key(&ticker("OLD")));
        }

        #[tokio::test]
        async fn test_fetch_validated_mid_prices_does_not_substitute_a_close() {
            // The behaviour this replaces: an unusable quote fell back to the
            // prior daily close, so a symbol rejected for its book was still
            // sized and still entered.
            let mock = MockTrading {
                latest_quotes: vec![quote("WIDE", 180.00, 200.00, Duration::seconds(5))],
                ..MockTrading::default()
            };

            let prices = fetch_validated_mid_prices(&mock, &[ticker("WIDE")], "test").await;

            assert!(prices.is_empty());
        }

        #[tokio::test]
        async fn test_fetch_validated_mid_prices_returns_empty_for_no_tickers() {
            let mock = MockTrading::default();
            assert!(fetch_validated_mid_prices(&mock, &[], "test")
                .await
                .is_empty());
        }

        #[tokio::test]
        async fn test_screen_entry_candidates_applies_both_screens() {
            let candidates = vec![
                candidate("AAPL", "MSFT"),
                // Tradable, but NOQT quotes too wide to price.
                candidate("GOOG", "NOQT"),
                // TSLA is not in the tradable universe at all.
                candidate("NVDA", "TSLA"),
            ];
            let universe = assets(&["AAPL", "MSFT", "GOOG", "NOQT", "NVDA"]);
            let mock = MockTrading {
                latest_quotes: vec![
                    quote("AAPL", 180.00, 180.20, Duration::seconds(5)),
                    quote("MSFT", 400.00, 400.30, Duration::seconds(5)),
                    quote("GOOG", 150.00, 150.10, Duration::seconds(5)),
                    quote("NOQT", 10.00, 12.00, Duration::seconds(5)),
                ],
                ..MockTrading::default()
            };

            let (eligible, prices) = screen_entry_candidates(&mock, &candidates, &universe).await;

            assert_eq!(eligible.len(), 1);
            assert_eq!(eligible[0].pair().pair_id().as_str(), "AAPL-MSFT");
            // NVDA never reached pricing: its pair failed the tradable screen,
            // so it must not appear in the request at all.
            assert!(!prices.contains_key(&ticker("NVDA")));
            assert!(!prices.contains_key(&ticker("NOQT")));
        }

        #[tokio::test]
        async fn test_screen_entry_candidates_yields_nothing_when_quotes_unavailable() {
            // A failed or empty quote response must open nothing rather than
            // fall through to some other price source.
            let candidates = vec![candidate("AAPL", "MSFT")];
            let universe = assets(&["AAPL", "MSFT"]);
            let mock = MockTrading::default();

            let (eligible, prices) = screen_entry_candidates(&mock, &candidates, &universe).await;

            assert!(eligible.is_empty());
            assert!(prices.is_empty());
        }
    }

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

    /// Prices every leg at its own final historical close.
    ///
    /// The resulting current spread equals the last point of the daily series,
    /// so `z_score_against(&spread, current)` returns exactly what the retired
    /// `z_score_last(&spread)` did — same mean, same deviation, same numerator.
    /// Fixtures written against the daily-close path therefore keep asserting
    /// the same signal boundaries, now expressed through the one estimator the
    /// exit path still uses.
    fn priced_at_last_close(prices: &HashMap<Ticker, Vec<f64>>) -> HashMap<Ticker, f64> {
        prices
            .iter()
            .filter_map(|(ticker, series)| series.last().map(|last| (ticker.clone(), *last)))
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

        let signals = evaluate_open_pairs(&[pair], &prices, &priced_at_last_close(&prices));
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

        let signals = evaluate_open_pairs(&[pair], &prices, &priced_at_last_close(&prices));
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

        let signals = evaluate_open_pairs(&[pair], &prices, &priced_at_last_close(&prices));
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

        let signals = evaluate_open_pairs(&[pair], &prices, &priced_at_last_close(&prices));
        assert!(signals.is_empty());
    }

    #[test]
    fn test_evaluate_open_pairs_missing_prices_skips_pair() {
        let pair = make_open_pair("AAPL", "MSFT", 2.5, 1.0);
        let prices = HashMap::new(); // No price data at all.

        let signals = evaluate_open_pairs(&[pair], &prices, &priced_at_last_close(&prices));
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

        let signals = evaluate_open_pairs(
            &[converging, stable, diverging],
            &prices,
            &priced_at_last_close(&prices),
        );
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

        let signals = evaluate_open_pairs(&[pair], &prices, &priced_at_last_close(&prices));
        assert!(signals.is_empty());
    }

    #[test]
    fn test_evaluate_open_pairs_empty_input() {
        let signals = evaluate_open_pairs(&[], &HashMap::new(), &HashMap::new());
        assert!(signals.is_empty());
    }

    // --- live price influence on exit decisions ---

    /// Daily closes where the spread sits steadily above its mean, so the pair
    /// has not converged on daily data alone.
    fn diverged_daily_prices() -> HashMap<Ticker, Vec<f64>> {
        let mut prices = HashMap::new();
        prices.insert(Ticker::new("AAPL").unwrap(), make_prices(60, 100.0, 1.0));
        prices.insert(Ticker::new("MSFT").unwrap(), make_prices(60, 100.0, 0.5));
        prices
    }

    #[test]
    fn test_live_quotes_can_trigger_convergence_daily_closes_miss() {
        // This is the whole point of streaming quotes. On daily closes the
        // spread is still wide and the pair stays open; a live collapse in the
        // long leg converges it now rather than at tomorrow's bar sync.
        let pair = make_open_pair("AAPL", "MSFT", 2.5, 1.0);
        let prices = diverged_daily_prices();

        assert!(
            evaluate_open_pairs(std::slice::from_ref(&pair), &prices, &HashMap::new()).is_empty()
        );

        let mut live = HashMap::new();
        live.insert(Ticker::new("AAPL").unwrap(), 100.0);
        live.insert(Ticker::new("MSFT").unwrap(), 200.0);

        let signals = evaluate_open_pairs(&[pair], &prices, &live);
        assert_eq!(signals.len(), 1);
        assert_eq!(signals[0].reason, CloseReason::ProfitTaken);
    }

    #[test]
    fn test_live_quote_for_one_leg_only_is_ignored() {
        // Pricing the long leg live against the short leg's prior close would
        // move the spread by a day of drift in one leg and read as a signal.
        let pair = make_open_pair("AAPL", "MSFT", 2.5, 1.0);
        let prices = diverged_daily_prices();

        let mut live = HashMap::new();
        live.insert(Ticker::new("AAPL").unwrap(), 100.0);

        assert_eq!(
            evaluate_open_pairs(std::slice::from_ref(&pair), &prices, &live).len(),
            evaluate_open_pairs(&[pair], &prices, &priced_at_last_close(&prices)).len()
        );
    }

    #[test]
    fn test_live_quote_for_unrelated_ticker_is_ignored() {
        let pair = make_open_pair("AAPL", "MSFT", 2.5, 1.0);
        let prices = diverged_daily_prices();

        let mut live = HashMap::new();
        live.insert(Ticker::new("TSLA").unwrap(), 250.0);

        assert!(evaluate_open_pairs(&[pair], &prices, &live).is_empty());
    }

    #[test]
    fn test_unpriced_pair_is_kept_rather_than_scored_on_daily_closes() {
        // The inverse of the previous behaviour, and the reason it changed.
        // These fixtures describe a converged pair: scored against its own last
        // daily close it yields ProfitTaken, which is what the retired fallback
        // returned whenever quotes stopped flowing. But that close can be two
        // and a half days old on a Monday afternoon, so the signal reported the
        // symbol going quiet, not the spread moving. With no current price the
        // pair is now held for the next pass.
        let pair = make_open_pair("AAPL", "MSFT", 2.5, 1.0);
        let mut prices = HashMap::new();
        prices.insert(Ticker::new("AAPL").unwrap(), make_prices(60, 150.0, -1.0));
        prices.insert(Ticker::new("MSFT").unwrap(), make_prices(60, 100.0, 1.0));

        // Priced, the same fixtures do signal a close.
        assert_eq!(
            evaluate_open_pairs(
                std::slice::from_ref(&pair),
                &prices,
                &priced_at_last_close(&prices)
            )
            .len(),
            1
        );

        // Unpriced, nothing is signalled and the pair stays open.
        assert!(evaluate_open_pairs(&[pair], &prices, &HashMap::new()).is_empty());
    }

    #[test]
    fn test_one_priced_leg_does_not_score_the_pair() {
        // Pricing one leg current against the other's stale value measures a
        // day of drift in one name and reads as a spread move.
        let pair = make_open_pair("AAPL", "MSFT", 2.5, 1.0);
        let mut prices = HashMap::new();
        prices.insert(Ticker::new("AAPL").unwrap(), make_prices(60, 150.0, -1.0));
        prices.insert(Ticker::new("MSFT").unwrap(), make_prices(60, 100.0, 1.0));

        let mut long_only = HashMap::new();
        long_only.insert(Ticker::new("AAPL").unwrap(), 91.0);
        assert!(evaluate_open_pairs(std::slice::from_ref(&pair), &prices, &long_only).is_empty());

        let mut short_only = HashMap::new();
        short_only.insert(Ticker::new("MSFT").unwrap(), 159.0);
        assert!(evaluate_open_pairs(&[pair], &prices, &short_only).is_empty());
    }

    #[test]
    fn test_larger_live_divergence_yields_larger_z_score() {
        // The live point is standardized against history, not against a series
        // containing itself. Appending it first let a big move pull the mean
        // toward itself and inflate the deviation, so a larger divergence
        // produced a *smaller* z-score — the move discounting itself.
        let prices = diverged_daily_prices();
        let pair = make_open_pair("AAPL", "MSFT", 2.5, 1.0);

        let long_prices = prices.get(&Ticker::new("AAPL").unwrap()).unwrap();
        let short_prices = prices.get(&Ticker::new("MSFT").unwrap()).unwrap();
        let common_length = long_prices.len().min(short_prices.len());
        let history: Vec<f64> = long_prices[long_prices.len() - common_length..]
            .iter()
            .zip(short_prices[short_prices.len() - common_length..].iter())
            .map(|(long, short)| long - pair.hedge_ratio() * short)
            .collect();

        let moderate = z_score_against(&history, 200.0);
        let extreme = z_score_against(&history, 400.0);

        assert!(
            extreme > moderate,
            "a larger divergence must not shrink its own z-score: {extreme} vs {moderate}"
        );
    }

    #[test]
    fn test_live_quotes_can_trigger_stop_loss() {
        // A live divergence past the threshold closes the position intraday
        // rather than letting it run until the next daily bar.
        let pair = make_open_pair("AAPL", "MSFT", 2.5, 1.0);
        let prices = diverged_daily_prices();

        assert!(
            evaluate_open_pairs(std::slice::from_ref(&pair), &prices, &HashMap::new()).is_empty()
        );

        let mut live = HashMap::new();
        live.insert(Ticker::new("AAPL").unwrap(), 400.0);
        live.insert(Ticker::new("MSFT").unwrap(), 100.0);

        let signals = evaluate_open_pairs(&[pair], &prices, &live);
        assert_eq!(signals.len(), 1);
        assert_eq!(signals[0].reason, CloseReason::StopLoss);
    }

    #[test]
    fn test_stop_loss_threshold_is_documented() {
        // Verify the threshold constant is the expected value (guards against
        // accidental changes without updating documentation).
        assert!((STOP_LOSS_Z_SCORE_THRESHOLD - 4.0).abs() < f64::EPSILON);
    }

    // --- close rule shared by the trigger and the authoritative evaluation ---

    #[test]
    fn test_close_reason_convergence_from_positive_entry() {
        assert_eq!(
            close_reason_for(2.5, -0.1),
            Some(CloseReason::ProfitTaken),
            "a positive-entry spread crossing below zero has converged"
        );
    }

    #[test]
    fn test_close_reason_convergence_from_negative_entry() {
        assert_eq!(close_reason_for(-2.5, 0.1), Some(CloseReason::ProfitTaken));
    }

    #[test]
    fn test_close_reason_stop_loss_requires_matching_direction() {
        // Diverging further in the entry direction is a stop-loss.
        assert_eq!(close_reason_for(2.5, 4.5), Some(CloseReason::StopLoss));
        assert_eq!(close_reason_for(-2.5, -4.5), Some(CloseReason::StopLoss));
    }

    #[test]
    fn test_close_reason_within_range_keeps_pair() {
        assert_eq!(close_reason_for(2.5, 2.0), None);
        assert_eq!(close_reason_for(-2.5, -2.0), None);
    }

    #[test]
    fn test_close_reason_treats_zero_as_no_signal() {
        // z_score_last returns zero for a near-zero-variance spread, which
        // happens on a halted or degenerate series. Reading that as convergence
        // would close healthy positions on missing data.
        assert_eq!(close_reason_for(2.5, 0.0), None);
        assert_eq!(close_reason_for(-2.5, 0.0), None);
    }

    #[test]
    fn test_close_reason_rejects_non_finite_z_score() {
        assert_eq!(close_reason_for(2.5, f64::NAN), None);
        assert_eq!(close_reason_for(2.5, f64::INFINITY), None);
    }

    #[test]
    fn test_evaluate_open_pairs_single_price_point_insufficient() {
        // A single price point per leg does not meet the >= 2 threshold for
        // z-score computation. The pair should be silently kept open.
        let pair = make_open_pair("AAPL", "MSFT", 2.5, 1.0);
        let mut prices = HashMap::new();
        prices.insert(Ticker::new("AAPL").unwrap(), vec![150.0]);
        prices.insert(Ticker::new("MSFT").unwrap(), vec![100.0]);

        let signals = evaluate_open_pairs(&[pair], &prices, &priced_at_last_close(&prices));
        assert!(signals.is_empty());
    }

    #[test]
    fn test_evaluate_open_pairs_different_length_histories() {
        // When price histories have different lengths, the function trims to
        // the common length. With a 60-point long history and a 30-point short
        // history, the spread is computed over the last 30 points.
        let pair = make_open_pair("AAPL", "MSFT", 2.5, 1.0);
        let mut prices = HashMap::new();
        // Long: 60 points decreasing (converging toward short).
        prices.insert(Ticker::new("AAPL").unwrap(), make_prices(60, 150.0, -1.0));
        // Short: only 30 points increasing.
        prices.insert(Ticker::new("MSFT").unwrap(), make_prices(30, 100.0, 1.0));

        let signals = evaluate_open_pairs(&[pair], &prices, &priced_at_last_close(&prices));
        // Should still evaluate correctly using the last 30 points.
        assert_eq!(signals.len(), 1);
        assert_eq!(signals[0].reason, CloseReason::ProfitTaken);
    }

    #[test]
    fn test_evaluate_open_pairs_long_prices_only_keeps_pair() {
        // When only the long leg has price data, the pair is kept open.
        let pair = make_open_pair("AAPL", "MSFT", 2.5, 1.0);
        let mut prices = HashMap::new();
        prices.insert(Ticker::new("AAPL").unwrap(), make_prices(60, 150.0, 1.0));
        // No MSFT prices.

        let signals = evaluate_open_pairs(&[pair], &prices, &priced_at_last_close(&prices));
        assert!(signals.is_empty());
    }

    #[test]
    fn test_evaluate_open_pairs_stop_loss_negative_entry_z() {
        // Entry z < 0, spread diverges further negative past threshold → stop loss.
        let pair = make_open_pair("AAPL", "MSFT", -2.5, 1.0);
        // Build prices where the spread is stable near zero for most of the window,
        // then collapses dramatically at the end (long drops, short spikes).
        let mut long_prices = vec![150.0; 58];
        long_prices.push(10.0);
        long_prices.push(5.0);
        let short_prices = vec![100.0; 60];

        let mut prices = HashMap::new();
        prices.insert(Ticker::new("AAPL").unwrap(), long_prices);
        prices.insert(Ticker::new("MSFT").unwrap(), short_prices);

        let signals = evaluate_open_pairs(&[pair], &prices, &priced_at_last_close(&prices));
        assert_eq!(signals.len(), 1);
        assert_eq!(signals[0].reason, CloseReason::StopLoss);
    }

    #[test]
    fn test_evaluate_open_pairs_hedge_ratio_affects_spread() {
        // With a hedge ratio of 2.0, the spread is long - 2*short.
        // Even if long rises, a doubling short ratio can cause the spread to
        // cross zero, triggering convergence.
        let pair = make_open_pair("AAPL", "MSFT", 2.5, 2.0);
        let mut prices = HashMap::new();
        // Long increases slowly, short increases faster.
        // spread = long - 2*short = (150 + i) - 2*(100 + 1.5*i) = -50 - 2*i
        // Spread goes very negative with entry_z > 0 → converged.
        prices.insert(Ticker::new("AAPL").unwrap(), make_prices(60, 150.0, 1.0));
        prices.insert(Ticker::new("MSFT").unwrap(), make_prices(60, 100.0, 1.5));

        let signals = evaluate_open_pairs(&[pair], &prices, &priced_at_last_close(&prices));
        assert_eq!(signals.len(), 1);
        assert_eq!(signals[0].reason, CloseReason::ProfitTaken);
    }

    #[test]
    fn test_rebalance_error_display_execution() {
        let error = RebalanceError::Execution(ExecutionError::PositionFetch {
            source: crate::portfolio::alpaca::ClientError::Parse("timeout".to_string()),
        });
        let message = format!("{error}");
        assert!(message.contains("Execution"));
        assert!(message.contains("timeout"));
    }

    #[test]
    fn test_rebalance_error_display_conversion() {
        let error =
            RebalanceError::Conversion("z_score cannot be represented as Decimal".to_string());
        let message = format!("{error}");
        assert!(message.contains("Numeric conversion"));
        assert!(message.contains("z_score"));
    }

    // --- build_portfolio_snapshot tests ---

    #[test]
    fn test_build_portfolio_snapshot_empty_positions() {
        let snapshot = build_portfolio_snapshot(100_000.0, 400_000.0, &[]);
        assert!((snapshot.account_equity - 100_000.0).abs() < f64::EPSILON);
        assert!((snapshot.buying_power - 400_000.0).abs() < f64::EPSILON);
        assert!(snapshot.positions.is_empty());
    }

    #[test]
    fn test_build_portfolio_snapshot_with_positions() {
        let positions = vec![
            crate::portfolio::alpaca::Position {
                symbol: "AAPL".to_string(),
                side: "long".to_string(),
                quantity: 100.0,
                market_value: 15_000.0,
                unrealized_profit_and_loss: 500.0,
            },
            crate::portfolio::alpaca::Position {
                symbol: "MSFT".to_string(),
                side: "short".to_string(),
                quantity: 50.0,
                market_value: -10_000.0,
                unrealized_profit_and_loss: -200.0,
            },
        ];
        let snapshot = build_portfolio_snapshot(100_000.0, 350_000.0, &positions);
        assert_eq!(snapshot.positions.len(), 2);
        assert_eq!(snapshot.positions[0].ticker, "AAPL");
        assert!((snapshot.positions[0].market_value_absolute - 15_000.0).abs() < f64::EPSILON);
        assert_eq!(snapshot.positions[1].ticker, "MSFT");
        // Short market_value is negative; absolute value should be positive.
        assert!((snapshot.positions[1].market_value_absolute - 10_000.0).abs() < f64::EPSILON);
        assert!(matches!(
            snapshot.positions[0].strategy,
            StrategyId::StatisticalArbitrage
        ));
    }

    #[test]
    fn test_build_portfolio_snapshot_preserves_equity_and_buying_power() {
        let snapshot = build_portfolio_snapshot(250_000.0, 800_000.0, &[]);
        assert!((snapshot.account_equity - 250_000.0).abs() < f64::EPSILON);
        assert!((snapshot.buying_power - 800_000.0).abs() < f64::EPSILON);
    }

    // --- measure_capital_utilization tests ---

    fn account(cash_amount: f64, buying_power: f64, equity: f64) -> AccountInfo {
        AccountInfo {
            cash_amount,
            buying_power,
            equity,
        }
    }

    fn position(symbol: &str, market_value: f64) -> crate::portfolio::alpaca::Position {
        crate::portfolio::alpaca::Position {
            symbol: symbol.to_string(),
            side: if market_value >= 0.0 { "long" } else { "short" }.to_string(),
            quantity: 100.0,
            market_value,
            unrealized_profit_and_loss: 0.0,
        }
    }

    fn betas(entries: &[(&str, f64)]) -> HashMap<Ticker, f64> {
        entries
            .iter()
            .map(|(symbol, beta)| (Ticker::new(symbol).unwrap(), *beta))
            .collect()
    }

    #[test]
    fn test_measure_capital_utilization_fully_idle_portfolio() {
        let utilization = measure_capital_utilization(
            &account(100_000.0, 400_000.0, 100_000.0),
            &[],
            &betas(&[]),
        );
        assert!((utilization.idle_cash - 100_000.0).abs() < f64::EPSILON);
        assert!((utilization.idle_cash_fraction - 1.0).abs() < f64::EPSILON);
        assert!(utilization.gross_exposure.abs() < f64::EPSILON);
        assert!(utilization.net_exposure.abs() < f64::EPSILON);
        assert!(utilization.margin_utilization.abs() < f64::EPSILON);
        assert!(utilization.net_beta.abs() < f64::EPSILON);
        // No exposure means nothing was left unmeasured.
        assert!((utilization.beta_coverage_fraction - 1.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_measure_capital_utilization_exposure_is_gross_and_net() {
        let positions = vec![position("AAPL", 15_000.0), position("MSFT", -10_000.0)];
        let utilization = measure_capital_utilization(
            &account(20_000.0, 350_000.0, 100_000.0),
            &positions,
            &betas(&[("AAPL", 1.0), ("MSFT", 1.0)]),
        );
        assert!((utilization.gross_exposure - 25_000.0).abs() < f64::EPSILON);
        assert!((utilization.net_exposure - 5_000.0).abs() < f64::EPSILON);
        assert!((utilization.idle_cash_fraction - 0.2).abs() < 1e-12);
    }

    #[test]
    fn test_measure_capital_utilization_offsetting_betas_net_to_zero() {
        // Equal notional legs with equal beta are market neutral by construction.
        let positions = vec![position("AAPL", 10_000.0), position("MSFT", -10_000.0)];
        let utilization = measure_capital_utilization(
            &account(0.0, 200_000.0, 100_000.0),
            &positions,
            &betas(&[("AAPL", 1.2), ("MSFT", 1.2)]),
        );
        assert!(utilization.net_beta.abs() < 1e-12);
        assert!((utilization.beta_coverage_fraction - 1.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_measure_capital_utilization_beta_mismatch_leaves_residual() {
        // A high-beta long against a low-beta short is dollar neutral but not
        // beta neutral: (10_000 × 1.5 - 10_000 × 0.5) / 100_000 = 0.10.
        let positions = vec![position("AAPL", 10_000.0), position("MSFT", -10_000.0)];
        let utilization = measure_capital_utilization(
            &account(0.0, 200_000.0, 100_000.0),
            &positions,
            &betas(&[("AAPL", 1.5), ("MSFT", 0.5)]),
        );
        assert!((utilization.net_beta - 0.10).abs() < 1e-12);
    }

    #[test]
    fn test_measure_capital_utilization_missing_beta_lowers_coverage() {
        // MSFT has no estimable beta, so it counts toward gross exposure but not
        // toward net beta. Reporting 0.15 without the coverage figure would imply
        // a measured neutrality that half the book never contributed to.
        let positions = vec![position("AAPL", 15_000.0), position("MSFT", -15_000.0)];
        let utilization = measure_capital_utilization(
            &account(0.0, 200_000.0, 100_000.0),
            &positions,
            &betas(&[("AAPL", 1.0)]),
        );
        assert!((utilization.net_beta - 0.15).abs() < 1e-12);
        assert!((utilization.beta_coverage_fraction - 0.5).abs() < 1e-12);
    }

    #[test]
    fn test_measure_capital_utilization_zero_equity_reports_zero_ratios() {
        // A wiped-out account must not divide by zero; margin utilization still
        // reports fully consumed, matching the risk gate's own treatment.
        let positions = vec![position("AAPL", 5_000.0)];
        let utilization = measure_capital_utilization(
            &account(0.0, 0.0, 0.0),
            &positions,
            &betas(&[("AAPL", 1.0)]),
        );
        assert!(utilization.idle_cash_fraction.abs() < f64::EPSILON);
        assert!(utilization.net_beta.abs() < f64::EPSILON);
        assert!((utilization.margin_utilization - 1.0).abs() < f64::EPSILON);
    }

    // --- check_drawdown tests ---

    #[test]
    fn test_check_drawdown_within_threshold_preserves_account() {
        // 5% below the previous NAV against a 10% threshold: the pass proceeds
        // and the account is handed back untouched for downstream sizing.
        let result = check_drawdown(
            account(20_000.0, 350_000.0, 95_000.0),
            Some(100_000.0),
            0.10,
        );
        let returned = result.expect("5% drop is within a 10% threshold");
        assert!((returned.equity - 95_000.0).abs() < f64::EPSILON);
        assert!((returned.buying_power - 350_000.0).abs() < f64::EPSILON);
        assert!((returned.cash_amount - 20_000.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_check_drawdown_no_previous_snapshot_passes() {
        // First pass ever: nothing to compare against, so the guard cannot apply.
        let result = check_drawdown(account(100_000.0, 400_000.0, 100_000.0), None, 0.10);
        assert!(result.is_ok());
    }

    #[test]
    fn test_check_drawdown_exactly_at_threshold_passes() {
        // The comparison is strictly greater-than, so a drop landing exactly on
        // the threshold is allowed.
        let result = check_drawdown(account(0.0, 200_000.0, 90_000.0), Some(100_000.0), 0.10);
        assert!(result.is_ok());
    }

    #[test]
    fn test_check_drawdown_beyond_threshold_breaches() {
        let result = check_drawdown(account(0.0, 200_000.0, 85_000.0), Some(100_000.0), 0.10);
        match result {
            Err(RebalanceError::DrawdownBreached { current, threshold }) => {
                assert!((current - 85_000.0).abs() < f64::EPSILON);
                assert!((threshold - 0.10).abs() < f64::EPSILON);
            }
            other => panic!("expected DrawdownBreached, got {:?}", other.is_ok()),
        }
    }

    #[test]
    fn test_check_drawdown_gain_passes() {
        // A gain yields a negative fraction, which must not read as a breach.
        let result = check_drawdown(account(0.0, 500_000.0, 120_000.0), Some(100_000.0), 0.10);
        assert!(result.is_ok());
    }

    #[test]
    fn test_check_drawdown_non_positive_previous_nav_passes() {
        // Guards the divide: a zero previous NAV would otherwise produce infinity
        // and halt every subsequent pass.
        let zero = check_drawdown(account(0.0, 200_000.0, 50_000.0), Some(0.0), 0.10);
        assert!(zero.is_ok());
        let negative = check_drawdown(account(0.0, 200_000.0, 50_000.0), Some(-1_000.0), 0.10);
        assert!(negative.is_ok());
    }

    #[test]
    fn test_measure_capital_utilization_margin_matches_risk_gate() {
        // The logged figure must be the gate's figure, not a parallel definition.
        let utilization =
            measure_capital_utilization(&account(0.0, 200_000.0, 100_000.0), &[], &betas(&[]));
        assert!(
            (utilization.margin_utilization - risk_gate::margin_utilization(100_000.0, 200_000.0))
                .abs()
                < f64::EPSILON
        );
    }
}
