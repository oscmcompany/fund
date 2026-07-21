//! Database access layer for the portfolio service.

use std::collections::HashMap;

use chrono::{DateTime, Duration, Utc};
use num_traits::ToPrimitive;
use rust_decimal::Decimal;
use sqlx::PgPool;
use tracing::{info, warn};
use uuid::Uuid;

use crate::domain::freshness::{Fresh, StalenessWindow};
use crate::domain::market::{PairID, Ticker};
use crate::domain::predictions::EquityPrediction;
use crate::domain::trading::{
    CloseReason, EquityAllocation, EquityOrder, EquityPair, EquityRebalanceSession, OrderStatus,
    RebalanceSessionStatus, ReconciliationAction, ReconciliationEventType,
};

/// Lookback window for historical close prices (calendar days).
///
/// 90 days aligns with the `equity_bars` TimescaleDB retention policy and
/// covers the 60-trading-day windows needed for correlation and beta estimation.
const HISTORICAL_PRICE_LOOKBACK_DAYS: i64 = 90;

/// Lookback window for SPY close prices (calendar days).
const SPY_PRICE_LOOKBACK_DAYS: i64 = 90;

/// An open equity pair position fetched from the database.
///
/// Returned by [`fetch_open_pairs`] and consumed by the per-pair evaluation
/// logic to decide whether to close (profit taken, stop loss) or keep each pair.
///
/// The ticker fields are validated `Ticker` values: a value in scope is proof
/// that the symbol passed format validation when it was read from the database.
/// The `entry_z_score` and `hedge_ratio` fields are carried from the original
/// pair opening and used to recompute the current z-score for close signal
/// evaluation.
#[derive(Debug, Clone)]
pub struct OpenPair {
    id: Uuid,
    pair_id: PairID,
    long_ticker: Ticker,
    short_ticker: Ticker,
    /// Z-score at the time the pair was opened; sign indicates trade direction.
    entry_z_score: f64,
    /// OLS hedge ratio at pair opening; used to compute the current spread.
    hedge_ratio: f64,
}

impl OpenPair {
    pub fn id(&self) -> Uuid {
        self.id
    }

    pub fn pair_id(&self) -> &PairID {
        &self.pair_id
    }

    pub fn long_ticker(&self) -> &Ticker {
        &self.long_ticker
    }

    pub fn short_ticker(&self) -> &Ticker {
        &self.short_ticker
    }

    pub fn entry_z_score(&self) -> f64 {
        self.entry_z_score
    }

    pub fn hedge_ratio(&self) -> f64 {
        self.hedge_ratio
    }

    /// Test-only constructor for building `OpenPair` values without a database.
    #[cfg(test)]
    pub fn new_for_test(
        id: Uuid,
        pair_id: PairID,
        long_ticker: Ticker,
        short_ticker: Ticker,
        entry_z_score: f64,
        hedge_ratio: f64,
    ) -> Self {
        Self {
            id,
            pair_id,
            long_ticker,
            short_ticker,
            entry_z_score,
            hedge_ratio,
        }
    }
}

/// Fetches today's latest equity predictions from PostgreSQL.
///
/// Selects all predictions sharing the most recently inserted `correlation_id`
/// where `created_at::date = CURRENT_DATE`. Returns an empty `Vec` wrapped in
/// `Fresh` when no predictions exist for today. The `Fresh` wrapper enforces
/// the 20-hour staleness window from `StalenessWindow::predictions()`.
pub async fn fetch_equity_predictions(
    pool: &PgPool,
) -> Result<Fresh<Vec<EquityPrediction>>, sqlx::Error> {
    let rows = sqlx::query!(
        "SELECT correlation_id, model_run_id, ticker, timestamp, \
                quantile_10, quantile_50, quantile_90, created_at \
         FROM equity_predictions \
         WHERE correlation_id = ( \
             SELECT correlation_id FROM equity_predictions \
             WHERE created_at::date = CURRENT_DATE \
             ORDER BY created_at DESC LIMIT 1 \
         ) \
         ORDER BY ticker, timestamp"
    )
    .fetch_all(pool)
    .await?;

    let predictions: Vec<EquityPrediction> = rows
        .into_iter()
        .map(|row| {
            let ticker = Ticker::new(&row.ticker).ok_or_else(|| {
                sqlx::Error::Decode(Box::new(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("invalid ticker from database: {}", row.ticker),
                )))
            })?;
            Ok(EquityPrediction::new(
                row.correlation_id,
                row.model_run_id,
                ticker,
                row.timestamp,
                row.quantile_10,
                row.quantile_50,
                row.quantile_90,
                row.created_at,
            ))
        })
        .collect::<Result<Vec<_>, sqlx::Error>>()?;

    info!(
        rows = predictions.len(),
        "Predictions fetched from PostgreSQL"
    );
    Ok(Fresh::new(predictions, StalenessWindow::predictions()))
}

/// Fetches historical close prices for all tickers over the trailing 90-day window.
///
/// Returns a map from ticker symbol to ordered close prices (oldest to newest).
/// Tickers with partial data are included as-is; callers are responsible for
/// filtering by minimum length.
pub async fn fetch_historical_equity_prices(
    pool: &PgPool,
) -> Result<HashMap<Ticker, Vec<f64>>, sqlx::Error> {
    let end_date = Utc::now();
    let start_date = end_date - Duration::days(HISTORICAL_PRICE_LOOKBACK_DAYS);

    let rows = sqlx::query!(
        "SELECT ticker, close_price \
         FROM equity_bars \
         WHERE timestamp >= $1 AND timestamp <= $2 \
         ORDER BY ticker, timestamp",
        start_date,
        end_date
    )
    .fetch_all(pool)
    .await?;

    let mut closes: HashMap<Ticker, Vec<f64>> = HashMap::new();
    for row in rows {
        let Some(ticker) = Ticker::new(&row.ticker) else {
            warn!(ticker = %row.ticker, "Skipping invalid ticker in equity_bars");
            continue;
        };
        closes.entry(ticker).or_default().push(row.close_price);
    }

    info!(
        tickers = closes.len(),
        "Historical prices fetched from PostgreSQL"
    );
    Ok(closes)
}

/// Fetches SPY close prices over the trailing 90-day window.
///
/// Returns prices ordered oldest to newest. Returns an empty `Vec` when no SPY
/// bars exist in the retention window.
pub async fn fetch_spy_equity_prices(pool: &PgPool) -> Result<Vec<f64>, sqlx::Error> {
    let end_date = Utc::now();
    let start_date = end_date - Duration::days(SPY_PRICE_LOOKBACK_DAYS);

    let rows = sqlx::query!(
        "SELECT close_price \
         FROM equity_bars \
         WHERE ticker = 'SPY' AND timestamp >= $1 AND timestamp <= $2 \
         ORDER BY timestamp",
        start_date,
        end_date
    )
    .fetch_all(pool)
    .await?;

    let prices: Vec<f64> = rows.into_iter().map(|row| row.close_price).collect();

    info!(rows = prices.len(), "SPY prices fetched from PostgreSQL");
    Ok(prices)
}

/// Fetches the GICS sector for each ticker from `equity_details`.
///
/// Returns a map from ticker to sector string. Tickers absent from
/// `equity_details` will not appear in the map; callers should default to
/// `"NOT AVAILABLE"`.
pub async fn fetch_equity_details(pool: &PgPool) -> Result<HashMap<Ticker, String>, sqlx::Error> {
    let rows = sqlx::query!("SELECT ticker, sector FROM equity_details")
        .fetch_all(pool)
        .await?;

    let mut details: HashMap<Ticker, String> = HashMap::new();
    for row in rows {
        let Some(ticker) = Ticker::new(&row.ticker) else {
            warn!(ticker = %row.ticker, "Skipping invalid ticker in equity_details");
            continue;
        };
        details.insert(ticker, row.sector);
    }

    info!(
        tickers = details.len(),
        "Equity details fetched from PostgreSQL"
    );
    Ok(details)
}

/// Fetches the latest mid-price for each requested ticker from `equity_quotes`.
///
/// Mid-price is computed as `(bid_price + ask_price) / 2`. Returns a map
/// containing only tickers for which a recent quote exists. Tickers without a
/// quote are absent from the result.
///
/// Returns an empty map immediately when `tickers` is empty.
pub async fn fetch_live_quote_mid_prices(
    pool: &PgPool,
    tickers: &[Ticker],
) -> Result<HashMap<Ticker, f64>, sqlx::Error> {
    if tickers.is_empty() {
        return Ok(HashMap::new());
    }

    let ticker_strings: Vec<String> = tickers.iter().map(Ticker::to_string).collect();

    let rows = sqlx::query!(
        r#"SELECT DISTINCT ON (ticker) ticker, (bid_price + ask_price) / 2.0 AS "mid_price!"
           FROM equity_quotes
           WHERE ticker = ANY($1::text[])
           ORDER BY ticker, timestamp DESC"#,
        &ticker_strings as &[String]
    )
    .fetch_all(pool)
    .await?;

    let mut prices: HashMap<Ticker, f64> = HashMap::new();
    for row in rows {
        let ticker = Ticker::new(&row.ticker).ok_or_else(|| {
            sqlx::Error::Decode(Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("invalid ticker from database: {}", row.ticker),
            )))
        })?;
        prices.insert(ticker, row.mid_price);
    }

    info!(
        tickers = prices.len(),
        "Live quote mid prices fetched from PostgreSQL"
    );
    Ok(prices)
}

/// Fetches all currently open equity pair positions, ordered by `opened_at` ascending.
///
/// Includes `z_score` and `hedge_ratio` from the pair opening so the per-pair
/// evaluation can recompute the current spread z-score and determine close signals.
pub async fn fetch_open_pairs(pool: &PgPool) -> Result<Vec<OpenPair>, sqlx::Error> {
    let rows = sqlx::query!(
        "SELECT id, pair_id, long_ticker, short_ticker, z_score, hedge_ratio \
         FROM equity_pairs \
         WHERE status = 'open' \
         ORDER BY opened_at ASC"
    )
    .fetch_all(pool)
    .await?;

    let pairs: Vec<OpenPair> = rows
        .into_iter()
        .map(|row| {
            let id = row.id;
            let pair_id_str = row.pair_id;
            let long_ticker_str = row.long_ticker;
            let short_ticker_str = row.short_ticker;
            let pair_id = PairID::parse(&pair_id_str).ok_or_else(|| {
                sqlx::Error::Decode(Box::new(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("invalid pair_id from database: {pair_id_str}"),
                )))
            })?;
            let long_ticker = Ticker::new(&long_ticker_str).ok_or_else(|| {
                sqlx::Error::Decode(Box::new(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("invalid long_ticker from database: {long_ticker_str}"),
                )))
            })?;
            let short_ticker = Ticker::new(&short_ticker_str).ok_or_else(|| {
                sqlx::Error::Decode(Box::new(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("invalid short_ticker from database: {short_ticker_str}"),
                )))
            })?;
            let entry_z_score = row.z_score.to_f64().ok_or_else(|| {
                sqlx::Error::Decode(Box::new(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "z_score cannot be represented as f64",
                )))
            })?;
            let hedge_ratio = row.hedge_ratio.to_f64().ok_or_else(|| {
                sqlx::Error::Decode(Box::new(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "hedge_ratio cannot be represented as f64",
                )))
            })?;
            Ok(OpenPair {
                id,
                pair_id,
                long_ticker,
                short_ticker,
                entry_z_score,
                hedge_ratio,
            })
        })
        .collect::<Result<_, sqlx::Error>>()?;

    info!(
        rows = pairs.len(),
        "Open equity pairs fetched from PostgreSQL"
    );
    Ok(pairs)
}

/// Fetches the most recent portfolio net asset value from `equity_portfolio_snapshots`.
///
/// Returns `None` when no snapshot exists yet, which is the expected state before
/// the first successful rebalance.
pub async fn fetch_latest_portfolio_net_asset_value(
    pool: &PgPool,
) -> Result<Option<f64>, sqlx::Error> {
    let row = sqlx::query!(
        "SELECT net_asset_value \
         FROM equity_portfolio_snapshots \
         ORDER BY snapshot_timestamp DESC \
         LIMIT 1"
    )
    .fetch_optional(pool)
    .await?;

    match row {
        None => Ok(None),
        Some(record) => {
            let net_asset_value = record.net_asset_value.to_f64().ok_or_else(|| {
                sqlx::Error::Decode(Box::new(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "net_asset_value cannot be represented as f64",
                )))
            })?;
            Ok(Some(net_asset_value))
        }
    }
}

/// Inserts a new equity rebalance session record.
pub async fn insert_rebalance_session<'e>(
    executor: impl sqlx::Executor<'e, Database = sqlx::Postgres>,
    session: &EquityRebalanceSession,
) -> Result<(), sqlx::Error> {
    sqlx::query!(
        "INSERT INTO equity_rebalance_sessions \
         (id, triggered_at, trigger_reason, model_run_id, completed_at, status) \
         VALUES ($1, $2, $3, $4, $5, $6)",
        session.id(),
        session.triggered_at(),
        session.trigger_reason(),
        session.model_run_id(),
        session.completed_at(),
        session.status().as_str()
    )
    .execute(executor)
    .await?;

    info!(session_id = %session.id(), "Rebalance session inserted into PostgreSQL");
    Ok(())
}

/// Updates the `status` and `completed_at` of a rebalance session.
pub async fn update_rebalance_session_status<'e>(
    executor: impl sqlx::Executor<'e, Database = sqlx::Postgres>,
    session_id: Uuid,
    status: &RebalanceSessionStatus,
    completed_at: DateTime<Utc>,
) -> Result<(), sqlx::Error> {
    let result = sqlx::query!(
        "UPDATE equity_rebalance_sessions \
         SET status = $1, completed_at = $2 \
         WHERE id = $3",
        status.as_str(),
        completed_at,
        session_id
    )
    .execute(executor)
    .await?;

    if result.rows_affected() != 1 {
        return Err(sqlx::Error::RowNotFound);
    }

    info!(session_id = %session_id, status = status.as_str(), "Rebalance session status updated");
    Ok(())
}

/// Inserts an equity pair record with status `open`.
pub async fn insert_equity_pair<'e>(
    executor: impl sqlx::Executor<'e, Database = sqlx::Postgres>,
    pair: &EquityPair,
) -> Result<(), sqlx::Error> {
    sqlx::query!(
        "INSERT INTO equity_pairs \
         (id, rebalance_id, pair_id, long_ticker, short_ticker, z_score, hedge_ratio, \
          signal_strength, status, opened_at) \
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)",
        pair.id(),
        pair.rebalance_id(),
        pair.pair_id().as_str(),
        pair.long_ticker().as_str(),
        pair.short_ticker().as_str(),
        *pair.z_score(),
        *pair.hedge_ratio(),
        *pair.signal_strength(),
        pair.status().as_str(),
        pair.opened_at()
    )
    .execute(executor)
    .await?;

    info!(
        pair_id = pair.pair_id().as_str(),
        "Equity pair inserted into PostgreSQL"
    );
    Ok(())
}

/// Marks an equity pair as closed with the given close reason.
///
/// Used by per-pair evaluation (`ProfitTaken`, `StopLoss`), and end-of-day
/// liquidation (`EndOfDay`).
pub async fn close_equity_pair_with_reason<'e>(
    executor: impl sqlx::Executor<'e, Database = sqlx::Postgres>,
    pair_id: Uuid,
    closed_at: DateTime<Utc>,
    reason: &CloseReason,
) -> Result<(), sqlx::Error> {
    let result = sqlx::query!(
        "UPDATE equity_pairs \
         SET status = 'closed', closed_at = $1, close_reason = $2 \
         WHERE id = $3",
        closed_at,
        reason.as_str(),
        pair_id
    )
    .execute(executor)
    .await?;

    if result.rows_affected() != 1 {
        return Err(sqlx::Error::RowNotFound);
    }

    info!(
        pair_id = %pair_id,
        close_reason = reason.as_str(),
        "Equity pair closed in PostgreSQL"
    );
    Ok(())
}

/// Inserts an equity allocation record for one leg of a pair.
pub async fn insert_equity_allocation<'e>(
    executor: impl sqlx::Executor<'e, Database = sqlx::Postgres>,
    allocation: &EquityAllocation,
) -> Result<(), sqlx::Error> {
    sqlx::query!(
        "INSERT INTO equity_allocations \
         (id, rebalance_id, equity_pair_id, generated_at, model_run_id, ticker, side, action, \
          dollar_amount, entry_price, quantity, notional) \
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)",
        allocation.id(),
        allocation.rebalance_id(),
        allocation.equity_pair_id(),
        allocation.generated_at(),
        allocation.model_run_id(),
        allocation.ticker().as_str(),
        allocation.side().as_str(),
        allocation.action().as_str(),
        *allocation.dollar_amount(),
        allocation.entry_price().copied(),
        allocation.quantity().copied(),
        allocation.notional().copied()
    )
    .execute(executor)
    .await?;

    Ok(())
}

/// Inserts an equity order record linking a filled order to its allocation.
pub async fn insert_equity_order<'e>(
    executor: impl sqlx::Executor<'e, Database = sqlx::Postgres>,
    order: &EquityOrder,
) -> Result<(), sqlx::Error> {
    sqlx::query!(
        "INSERT INTO equity_orders \
         (id, allocation_id, submitted_at, ticker, side, quantity, order_type, limit_price, \
          alpaca_order_id) \
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)",
        order.id(),
        order.allocation_id(),
        order.submitted_at(),
        order.ticker().as_str(),
        order.side().as_str(),
        *order.quantity(),
        order.order_type(),
        order.limit_price().copied(),
        order.alpaca_order_id()
    )
    .execute(executor)
    .await?;

    Ok(())
}

/// A submitted order awaiting fill confirmation or reconciliation resolution.
///
/// Returned by [`fetch_submitted_orders`] for reconciliation to check against Alpaca.
#[derive(Debug, Clone)]
pub struct SubmittedOrder {
    id: Uuid,
    alpaca_order_id: String,
    ticker: String,
    side: String,
    submitted_at: DateTime<Utc>,
}

impl SubmittedOrder {
    pub fn id(&self) -> Uuid {
        self.id
    }

    pub fn alpaca_order_id(&self) -> &str {
        &self.alpaca_order_id
    }

    pub fn ticker(&self) -> &str {
        &self.ticker
    }

    pub fn side(&self) -> &str {
        &self.side
    }

    pub fn submitted_at(&self) -> DateTime<Utc> {
        self.submitted_at
    }
}

/// Inserts a submitted order record before polling for fills.
///
/// This creates a durable breadcrumb so that if the process crashes between
/// order submission and fill confirmation, the reconciliation process can
/// find and resolve the order.
#[allow(clippy::too_many_arguments)]
pub async fn insert_submitted_order(
    pool: &PgPool,
    id: Uuid,
    alpaca_order_id: &str,
    ticker: &str,
    side: &str,
    quantity: Decimal,
    order_type: &str,
    submitted_at: DateTime<Utc>,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO equity_orders \
         (id, submitted_at, ticker, side, quantity, order_type, alpaca_order_id, status) \
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
    )
    .bind(id)
    .bind(submitted_at)
    .bind(ticker)
    .bind(side)
    .bind(quantity)
    .bind(order_type)
    .bind(alpaca_order_id)
    .bind(OrderStatus::Submitted.as_str())
    .execute(pool)
    .await?;

    info!(
        alpaca_order_id = alpaca_order_id,
        ticker = ticker,
        "Submitted order tracked in PostgreSQL"
    );
    Ok(())
}

/// Marks a submitted order as filled and sets the fill timestamp.
///
/// Accepts a generic executor so it can participate in an existing transaction.
pub async fn mark_order_filled<'e>(
    executor: impl sqlx::Executor<'e, Database = sqlx::Postgres>,
    id: Uuid,
    allocation_id: Uuid,
    filled_at: DateTime<Utc>,
) -> Result<(), sqlx::Error> {
    let result = sqlx::query(
        "UPDATE equity_orders \
         SET status = $1, filled_at = $2, allocation_id = $3 \
         WHERE id = $4 AND status = $5",
    )
    .bind(OrderStatus::Filled.as_str())
    .bind(filled_at)
    .bind(allocation_id)
    .bind(id)
    .bind(OrderStatus::Submitted.as_str())
    .execute(executor)
    .await?;

    if result.rows_affected() != 1 {
        warn!(
            order_id = %id,
            "Order not found or not in submitted status"
        );
    }

    Ok(())
}

/// Marks a submitted order as cancelled.
pub async fn mark_order_cancelled(pool: &PgPool, id: Uuid) -> Result<(), sqlx::Error> {
    sqlx::query(
        "UPDATE equity_orders \
         SET status = $1 \
         WHERE id = $2 AND status = $3",
    )
    .bind(OrderStatus::Cancelled.as_str())
    .bind(id)
    .bind(OrderStatus::Submitted.as_str())
    .execute(pool)
    .await?;

    Ok(())
}

/// Fetches all orders with `status = 'submitted'` older than the given threshold.
///
/// These represent orders that were submitted to Alpaca but never confirmed as
/// filled. The reconciliation process uses this to check Alpaca for the order
/// status and resolve them.
pub async fn fetch_submitted_orders(
    pool: &PgPool,
    older_than: DateTime<Utc>,
) -> Result<Vec<SubmittedOrder>, sqlx::Error> {
    let rows = sqlx::query(
        "SELECT id, alpaca_order_id, ticker, side, submitted_at \
         FROM equity_orders \
         WHERE status = 'submitted' AND submitted_at < $1 \
         ORDER BY submitted_at ASC",
    )
    .bind(older_than)
    .fetch_all(pool)
    .await?;

    let orders: Vec<SubmittedOrder> = rows
        .into_iter()
        .map(|row| {
            use sqlx::Row;
            SubmittedOrder {
                id: row.get("id"),
                alpaca_order_id: row.get("alpaca_order_id"),
                ticker: row.get("ticker"),
                side: row.get("side"),
                submitted_at: row.get("submitted_at"),
            }
        })
        .collect();

    info!(
        rows = orders.len(),
        "Submitted orders fetched from PostgreSQL"
    );
    Ok(orders)
}

/// An unresolved reconciliation event awaiting retry or human review.
///
/// Returned by [`fetch_unresolved_reconciliation_events`] for the reconciliation
/// process to retry corrective actions.
#[derive(Debug, Clone)]
pub struct UnresolvedReconciliationEvent {
    id: i64,
    event_type: String,
    ticker: String,
    alpaca_order_id: Option<String>,
}

impl UnresolvedReconciliationEvent {
    pub fn id(&self) -> i64 {
        self.id
    }

    pub fn event_type(&self) -> &str {
        &self.event_type
    }

    pub fn ticker(&self) -> &str {
        &self.ticker
    }

    pub fn alpaca_order_id(&self) -> Option<&str> {
        self.alpaca_order_id.as_deref()
    }
}

/// Inserts a reconciliation event recording a detected discrepancy and the action taken.
#[allow(clippy::too_many_arguments)]
pub async fn insert_reconciliation_event(
    pool: &PgPool,
    event_type: &ReconciliationEventType,
    ticker: &str,
    expected_quantity: Option<Decimal>,
    actual_quantity: Option<Decimal>,
    equity_pair_id: Option<Uuid>,
    alpaca_order_id: Option<&str>,
    action_taken: &ReconciliationAction,
    resolved_at: Option<DateTime<Utc>>,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO equity_reconciliation_events \
         (event_type, ticker, expected_quantity, actual_quantity, equity_pair_id, \
          alpaca_order_id, action_taken, resolved_at) \
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
    )
    .bind(event_type.as_str())
    .bind(ticker)
    .bind(expected_quantity)
    .bind(actual_quantity)
    .bind(equity_pair_id)
    .bind(alpaca_order_id)
    .bind(action_taken.as_str())
    .bind(resolved_at)
    .execute(pool)
    .await?;

    info!(
        event_type = event_type.as_str(),
        ticker = ticker,
        action = action_taken.as_str(),
        "Reconciliation event recorded"
    );
    Ok(())
}

/// Fetches all unresolved reconciliation events (where `resolved_at IS NULL`).
///
/// Used by the reconciliation process to retry compensation failures and
/// other events that need follow-up.
pub async fn fetch_unresolved_reconciliation_events(
    pool: &PgPool,
) -> Result<Vec<UnresolvedReconciliationEvent>, sqlx::Error> {
    let rows = sqlx::query(
        "SELECT id, event_type, ticker, alpaca_order_id \
         FROM equity_reconciliation_events \
         WHERE resolved_at IS NULL \
         ORDER BY detected_at ASC",
    )
    .fetch_all(pool)
    .await?;

    let events: Vec<UnresolvedReconciliationEvent> = rows
        .into_iter()
        .map(|row| {
            use sqlx::Row;
            UnresolvedReconciliationEvent {
                id: row.get("id"),
                event_type: row.get("event_type"),
                ticker: row.get("ticker"),
                alpaca_order_id: row.get("alpaca_order_id"),
            }
        })
        .collect();

    info!(
        rows = events.len(),
        "Unresolved reconciliation events fetched from PostgreSQL"
    );
    Ok(events)
}

/// Marks a reconciliation event as resolved by setting `resolved_at`.
pub async fn resolve_reconciliation_event(pool: &PgPool, event_id: i64) -> Result<(), sqlx::Error> {
    sqlx::query(
        "UPDATE equity_reconciliation_events \
         SET resolved_at = now() \
         WHERE id = $1",
    )
    .bind(event_id)
    .execute(pool)
    .await?;

    info!(event_id = event_id, "Reconciliation event resolved");
    Ok(())
}

/// Inserts an intraday portfolio snapshot recording the post-rebalance net asset value.
pub async fn insert_portfolio_snapshot<'e>(
    executor: impl sqlx::Executor<'e, Database = sqlx::Postgres>,
    snapshot_timestamp: DateTime<Utc>,
    net_asset_value: Decimal,
    total_slippage_cost: Decimal,
) -> Result<(), sqlx::Error> {
    sqlx::query!(
        "INSERT INTO equity_portfolio_snapshots \
         (snapshot_timestamp, snapshot_type, net_asset_value, total_slippage_cost) \
         VALUES ($1, 'intraday', $2, $3)",
        snapshot_timestamp,
        net_asset_value,
        total_slippage_cost
    )
    .execute(executor)
    .await?;

    info!(net_asset_value = %net_asset_value, "Portfolio snapshot inserted into PostgreSQL");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::runtime::Builder;

    fn lazy_pool() -> PgPool {
        PgPool::connect_lazy("postgresql://localhost:5432/fund_test_nonexistent")
            .expect("lazy pool creation should not fail")
    }

    fn make_runtime() -> tokio::runtime::Runtime {
        Builder::new_current_thread().enable_all().build().unwrap()
    }

    // --- OpenPair accessors ---

    #[test]
    fn test_open_pair_accessors() {
        let pair_id = Uuid::new_v4();
        let open_pair = OpenPair {
            id: pair_id,
            pair_id: PairID::new(Ticker::new("AAPL").unwrap(), Ticker::new("MSFT").unwrap()),
            long_ticker: Ticker::new("AAPL").unwrap(),
            short_ticker: Ticker::new("MSFT").unwrap(),
            entry_z_score: 2.5,
            hedge_ratio: 0.85,
        };
        assert_eq!(open_pair.id(), pair_id);
        assert_eq!(open_pair.pair_id().as_str(), "AAPL-MSFT");
        assert_eq!(open_pair.long_ticker().as_str(), "AAPL");
        assert_eq!(open_pair.short_ticker().as_str(), "MSFT");
        assert!((open_pair.entry_z_score() - 2.5).abs() < f64::EPSILON);
        assert!((open_pair.hedge_ratio() - 0.85).abs() < f64::EPSILON);
    }

    #[test]
    fn test_open_pair_clone() {
        let open_pair = OpenPair {
            id: Uuid::new_v4(),
            pair_id: PairID::new(Ticker::new("GOOG").unwrap(), Ticker::new("META").unwrap()),
            long_ticker: Ticker::new("GOOG").unwrap(),
            short_ticker: Ticker::new("META").unwrap(),
            entry_z_score: -3.0,
            hedge_ratio: 1.2,
        };
        let cloned = open_pair.clone();
        assert_eq!(cloned.pair_id().as_str(), open_pair.pair_id().as_str());
        assert_eq!(
            cloned.long_ticker().as_str(),
            open_pair.long_ticker().as_str()
        );
        assert_eq!(
            cloned.short_ticker().as_str(),
            open_pair.short_ticker().as_str()
        );
        assert!((cloned.entry_z_score() - open_pair.entry_z_score()).abs() < f64::EPSILON);
        assert!((cloned.hedge_ratio() - open_pair.hedge_ratio()).abs() < f64::EPSILON);
    }

    // --- fetch_live_quote_mid_prices empty-tickers fast path ---

    #[test]
    fn test_fetch_live_quote_mid_prices_empty_tickers_returns_empty() {
        make_runtime().block_on(async {
            let result = fetch_live_quote_mid_prices(&lazy_pool(), &[]).await;
            assert!(result.is_ok());
            assert!(result.unwrap().is_empty());
        });
    }

    #[test]
    fn test_fetch_live_quote_mid_prices_compiles_with_ticker_slice() {
        make_runtime().block_on(async {
            let tickers = vec![Ticker::new("AAPL").unwrap()];
            assert!(fetch_live_quote_mid_prices(&lazy_pool(), &tickers)
                .await
                .is_err());
        });
    }

    // --- compile / connection-error coverage for all DB functions ---

    #[test]
    fn test_fetch_equity_predictions_compiles() {
        make_runtime().block_on(async {
            assert!(fetch_equity_predictions(&lazy_pool()).await.is_err());
        });
    }

    #[test]
    fn test_fetch_historical_equity_prices_compiles() {
        make_runtime().block_on(async {
            assert!(fetch_historical_equity_prices(&lazy_pool()).await.is_err());
        });
    }

    #[test]
    fn test_fetch_spy_equity_prices_compiles() {
        make_runtime().block_on(async {
            assert!(fetch_spy_equity_prices(&lazy_pool()).await.is_err());
        });
    }

    #[test]
    fn test_fetch_equity_details_compiles() {
        make_runtime().block_on(async {
            assert!(fetch_equity_details(&lazy_pool()).await.is_err());
        });
    }

    #[test]
    fn test_fetch_open_pairs_compiles() {
        make_runtime().block_on(async {
            assert!(fetch_open_pairs(&lazy_pool()).await.is_err());
        });
    }

    #[test]
    fn test_fetch_latest_portfolio_net_asset_value_compiles() {
        make_runtime().block_on(async {
            assert!(fetch_latest_portfolio_net_asset_value(&lazy_pool())
                .await
                .is_err());
        });
    }

    #[test]
    fn test_insert_rebalance_session_compiles() {
        make_runtime().block_on(async {
            use crate::domain::trading::{EquityRebalanceSession, RebalanceSessionStatus};
            let session = EquityRebalanceSession::new(
                Uuid::new_v4(),
                Utc::now(),
                "market_session_check".to_string(),
                None,
                None,
                RebalanceSessionStatus::Completed,
            );
            assert!(insert_rebalance_session(&lazy_pool(), &session)
                .await
                .is_err());
        });
    }

    #[test]
    fn test_update_rebalance_session_status_compiles() {
        make_runtime().block_on(async {
            assert!(update_rebalance_session_status(
                &lazy_pool(),
                Uuid::new_v4(),
                &RebalanceSessionStatus::Failed,
                Utc::now(),
            )
            .await
            .is_err());
        });
    }

    #[test]
    fn test_insert_equity_pair_compiles() {
        make_runtime().block_on(async {
            use crate::domain::market::{PairID, Ticker};
            use crate::domain::trading::{EquityPair, EquityPairStatus};
            use rust_decimal::Decimal;

            let pair = EquityPair::new(
                Uuid::new_v4(),
                Uuid::new_v4(),
                PairID::new(Ticker::new("AAPL").unwrap(), Ticker::new("MSFT").unwrap()),
                Ticker::new("AAPL").unwrap(),
                Ticker::new("MSFT").unwrap(),
                Decimal::from(2),
                Decimal::from(1),
                Decimal::new(75, 2),
                EquityPairStatus::Open,
                Utc::now(),
                None,
                None,
                None,
            );
            assert!(insert_equity_pair(&lazy_pool(), &pair).await.is_err());
        });
    }

    #[test]
    fn test_close_equity_pair_with_reason_compiles() {
        make_runtime().block_on(async {
            for reason in [
                CloseReason::ProfitTaken,
                CloseReason::StopLoss,
                CloseReason::EndOfDay,
                CloseReason::ReconciliationAlpacaMissing,
            ] {
                assert!(close_equity_pair_with_reason(
                    &lazy_pool(),
                    Uuid::new_v4(),
                    Utc::now(),
                    &reason
                )
                .await
                .is_err());
            }
        });
    }

    #[test]
    fn test_insert_equity_allocation_compiles() {
        make_runtime().block_on(async {
            use crate::domain::market::Ticker;
            use crate::domain::trading::{AllocationAction, AllocationSide, EquityAllocation};
            use rust_decimal::Decimal;

            let allocation = EquityAllocation::new(
                Uuid::new_v4(),
                Uuid::new_v4(),
                Uuid::new_v4(),
                Utc::now(),
                None,
                Ticker::new("AAPL").unwrap(),
                AllocationSide::Long,
                AllocationAction::OpenPosition,
                Decimal::from(10_000),
                Some(Decimal::from(150)),
                None,
                Some(Decimal::from(10_000)),
            );
            assert!(insert_equity_allocation(&lazy_pool(), &allocation)
                .await
                .is_err());
        });
    }

    #[test]
    fn test_insert_equity_order_compiles() {
        make_runtime().block_on(async {
            use crate::domain::market::Ticker;
            use crate::domain::trading::{AllocationSide, EquityOrder};
            use rust_decimal::Decimal;

            let order = EquityOrder::new(
                Uuid::new_v4(),
                Uuid::new_v4(),
                Utc::now(),
                Ticker::new("MSFT").unwrap(),
                AllocationSide::Short,
                Decimal::from(25),
                "market".to_string(),
                None,
                "alpaca-order-xyz".to_string(),
            );
            assert!(insert_equity_order(&lazy_pool(), &order).await.is_err());
        });
    }

    // --- SubmittedOrder accessors ---

    #[test]
    fn test_submitted_order_accessors() {
        let id = Uuid::new_v4();
        let now = Utc::now();
        let order = SubmittedOrder {
            id,
            alpaca_order_id: "alpaca-123".to_string(),
            ticker: "AAPL".to_string(),
            side: "LONG".to_string(),
            submitted_at: now,
        };
        assert_eq!(order.id(), id);
        assert_eq!(order.alpaca_order_id(), "alpaca-123");
        assert_eq!(order.ticker(), "AAPL");
        assert_eq!(order.side(), "LONG");
        assert_eq!(order.submitted_at(), now);
    }

    #[test]
    fn test_submitted_order_clone() {
        let order = SubmittedOrder {
            id: Uuid::new_v4(),
            alpaca_order_id: "alpaca-456".to_string(),
            ticker: "MSFT".to_string(),
            side: "SHORT".to_string(),
            submitted_at: Utc::now(),
        };
        let cloned = order.clone();
        assert_eq!(cloned.alpaca_order_id(), order.alpaca_order_id());
        assert_eq!(cloned.ticker(), order.ticker());
    }

    // --- compile / connection-error coverage for durable order tracking ---

    #[test]
    fn test_insert_submitted_order_compiles() {
        make_runtime().block_on(async {
            assert!(insert_submitted_order(
                &lazy_pool(),
                Uuid::new_v4(),
                "alpaca-order-001",
                "AAPL",
                "LONG",
                Decimal::from(100),
                "market",
                Utc::now(),
            )
            .await
            .is_err());
        });
    }

    #[test]
    fn test_mark_order_filled_compiles() {
        make_runtime().block_on(async {
            assert!(
                mark_order_filled(&lazy_pool(), Uuid::new_v4(), Uuid::new_v4(), Utc::now(),)
                    .await
                    .is_err()
            );
        });
    }

    #[test]
    fn test_mark_order_cancelled_compiles() {
        make_runtime().block_on(async {
            assert!(mark_order_cancelled(&lazy_pool(), Uuid::new_v4())
                .await
                .is_err());
        });
    }

    #[test]
    fn test_fetch_submitted_orders_compiles() {
        make_runtime().block_on(async {
            assert!(fetch_submitted_orders(&lazy_pool(), Utc::now())
                .await
                .is_err());
        });
    }

    // --- UnresolvedReconciliationEvent accessors ---

    #[test]
    fn test_unresolved_reconciliation_event_accessors() {
        let event = UnresolvedReconciliationEvent {
            id: 42,
            event_type: "compensation_failure".to_string(),
            ticker: "AAPL".to_string(),
            alpaca_order_id: Some("alpaca-order-001".to_string()),
        };
        assert_eq!(event.id(), 42);
        assert_eq!(event.event_type(), "compensation_failure");
        assert_eq!(event.ticker(), "AAPL");
        assert_eq!(event.alpaca_order_id(), Some("alpaca-order-001"));
    }

    #[test]
    fn test_unresolved_reconciliation_event_without_order_id() {
        let event = UnresolvedReconciliationEvent {
            id: 1,
            event_type: "database_only".to_string(),
            ticker: "MSFT".to_string(),
            alpaca_order_id: None,
        };
        assert!(event.alpaca_order_id().is_none());
    }

    #[test]
    fn test_unresolved_reconciliation_event_clone() {
        let event = UnresolvedReconciliationEvent {
            id: 5,
            event_type: "alpaca_only".to_string(),
            ticker: "GOOG".to_string(),
            alpaca_order_id: None,
        };
        let cloned = event.clone();
        assert_eq!(cloned.id(), event.id());
        assert_eq!(cloned.ticker(), event.ticker());
    }

    // --- compile / connection-error coverage for reconciliation DB functions ---

    #[test]
    fn test_insert_reconciliation_event_compiles() {
        make_runtime().block_on(async {
            use crate::domain::trading::{ReconciliationAction, ReconciliationEventType};
            assert!(insert_reconciliation_event(
                &lazy_pool(),
                &ReconciliationEventType::AlpacaOnly,
                "AAPL",
                None,
                Some(Decimal::from(100)),
                None,
                None,
                &ReconciliationAction::ClosedOrphan,
                Some(Utc::now()),
            )
            .await
            .is_err());
        });
    }

    #[test]
    fn test_fetch_unresolved_reconciliation_events_compiles() {
        make_runtime().block_on(async {
            assert!(fetch_unresolved_reconciliation_events(&lazy_pool())
                .await
                .is_err());
        });
    }

    #[test]
    fn test_resolve_reconciliation_event_compiles() {
        make_runtime().block_on(async {
            assert!(resolve_reconciliation_event(&lazy_pool(), 1).await.is_err());
        });
    }

    #[test]
    fn test_insert_portfolio_snapshot_compiles() {
        make_runtime().block_on(async {
            assert!(insert_portfolio_snapshot(
                &lazy_pool(),
                Utc::now(),
                Decimal::from(100_000),
                Decimal::from(50),
            )
            .await
            .is_err());
        });
    }
}
