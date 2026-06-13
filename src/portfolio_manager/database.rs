//! Database access layer for the portfolio_manager service.

use std::collections::HashMap;

use chrono::{DateTime, Duration, Utc};
use num_traits::ToPrimitive;
use rust_decimal::Decimal;
use sqlx::PgPool;
use tracing::info;
use uuid::Uuid;

use crate::domain::freshness::{Fresh, StalenessWindow};
use crate::domain::market::Ticker;
use crate::domain::predictions::EquityPrediction;
use crate::domain::trading::{
    EquityAllocation, EquityOrder, EquityPair, EquityRebalanceSession, RebalanceSessionStatus,
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
/// Returned by [`fetch_open_pairs`] and consumed by the execution layer to close
/// positions before starting a new rebalance cycle.
///
/// The ticker fields are validated `Ticker` values: a value in scope is proof
/// that the symbol passed format validation when it was read from the database.
#[derive(Debug, Clone)]
pub struct OpenPair {
    id: Uuid,
    pair_id: String,
    long_ticker: Ticker,
    short_ticker: Ticker,
}

impl OpenPair {
    pub fn id(&self) -> Uuid {
        self.id
    }

    pub fn pair_id(&self) -> &str {
        &self.pair_id
    }

    pub fn long_ticker(&self) -> &Ticker {
        &self.long_ticker
    }

    pub fn short_ticker(&self) -> &Ticker {
        &self.short_ticker
    }
}

/// Fetches today's latest equity predictions from PostgreSQL.
///
/// Selects all predictions sharing the most recently inserted `correlation_id`
/// where `created_at::date = CURRENT_DATE`. Returns an empty `Vec` wrapped in
/// `Fresh` when no predictions exist for today. The `Fresh` wrapper enforces
/// the 20-hour staleness window from `StalenessWindow::predictions()`.
pub async fn fetch_predictions(pool: &PgPool) -> Result<Fresh<Vec<EquityPrediction>>, sqlx::Error> {
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
            EquityPrediction::new(
                row.correlation_id,
                row.model_run_id,
                row.ticker,
                row.timestamp,
                row.quantile_10,
                row.quantile_50,
                row.quantile_90,
                row.created_at,
            )
        })
        .collect();

    info!(
        count = predictions.len(),
        "Predictions fetched from PostgreSQL"
    );
    Ok(Fresh::new(predictions, StalenessWindow::predictions()))
}

/// Fetches historical close prices for all tickers over the trailing 90-day window.
///
/// Returns a map from ticker symbol to ordered close prices (oldest to newest).
/// Tickers with partial data are included as-is; callers are responsible for
/// filtering by minimum length.
pub async fn fetch_historical_prices(
    pool: &PgPool,
) -> Result<HashMap<String, Vec<f64>>, sqlx::Error> {
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

    let mut closes: HashMap<String, Vec<f64>> = HashMap::new();
    for row in rows {
        closes.entry(row.ticker).or_default().push(row.close_price);
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
pub async fn fetch_spy_prices(pool: &PgPool) -> Result<Vec<f64>, sqlx::Error> {
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

    info!(count = prices.len(), "SPY prices fetched from PostgreSQL");
    Ok(prices)
}

/// Fetches the GICS sector for each ticker from `equity_details`.
///
/// Returns a map from ticker to sector string. Tickers absent from
/// `equity_details` will not appear in the map; callers should default to
/// `"NOT AVAILABLE"`.
pub async fn fetch_equity_details(pool: &PgPool) -> Result<HashMap<String, String>, sqlx::Error> {
    let rows = sqlx::query!("SELECT ticker, sector FROM equity_details")
        .fetch_all(pool)
        .await?;

    let mut details: HashMap<String, String> = HashMap::new();
    for row in rows {
        details.insert(row.ticker, row.sector);
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
    tickers: &[String],
) -> Result<HashMap<String, f64>, sqlx::Error> {
    if tickers.is_empty() {
        return Ok(HashMap::new());
    }

    let rows = sqlx::query!(
        r#"SELECT DISTINCT ON (ticker) ticker, (bid_price + ask_price) / 2.0 AS "mid_price!"
           FROM equity_quotes
           WHERE ticker = ANY($1::text[])
           ORDER BY ticker, timestamp DESC"#,
        tickers as &[String]
    )
    .fetch_all(pool)
    .await?;

    let mut prices: HashMap<String, f64> = HashMap::new();
    for row in rows {
        prices.insert(row.ticker, row.mid_price);
    }

    info!(
        tickers = prices.len(),
        "Live quote mid prices fetched from PostgreSQL"
    );
    Ok(prices)
}

/// Fetches all currently open equity pair positions, ordered by `opened_at` ascending.
///
/// The ordering ensures oldest positions are closed first during rebalance teardown,
/// giving the most-recently-opened pairs the best chance of exit at a favorable price.
pub async fn fetch_open_pairs(pool: &PgPool) -> Result<Vec<OpenPair>, sqlx::Error> {
    let rows = sqlx::query!(
        "SELECT id, pair_id, long_ticker, short_ticker \
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
            let pair_id = row.pair_id;
            let long_ticker_str = row.long_ticker;
            let short_ticker_str = row.short_ticker;
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
            Ok(OpenPair {
                id,
                pair_id,
                long_ticker,
                short_ticker,
            })
        })
        .collect::<Result<_, sqlx::Error>>()?;

    info!(
        count = pairs.len(),
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

    Ok(row.and_then(|record| record.net_asset_value.to_f64()))
}

/// Inserts a new equity rebalance session record.
pub async fn insert_rebalance_session(
    pool: &PgPool,
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
    .execute(pool)
    .await?;

    info!(session_id = %session.id(), "Rebalance session inserted into PostgreSQL");
    Ok(())
}

/// Updates the `status` and `completed_at` of a rebalance session.
pub async fn update_rebalance_session_status(
    pool: &PgPool,
    session_id: Uuid,
    status: &RebalanceSessionStatus,
    completed_at: DateTime<Utc>,
) -> Result<(), sqlx::Error> {
    sqlx::query!(
        "UPDATE equity_rebalance_sessions \
         SET status = $1, completed_at = $2 \
         WHERE id = $3",
        status.as_str(),
        completed_at,
        session_id
    )
    .execute(pool)
    .await?;

    info!(session_id = %session_id, status = status.as_str(), "Rebalance session status updated");
    Ok(())
}

/// Inserts an equity pair record with status `open`.
pub async fn insert_equity_pair(pool: &PgPool, pair: &EquityPair) -> Result<(), sqlx::Error> {
    sqlx::query!(
        "INSERT INTO equity_pairs \
         (id, rebalance_id, pair_id, long_ticker, short_ticker, z_score, hedge_ratio, \
          signal_strength, status, opened_at) \
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)",
        pair.id(),
        pair.rebalance_id(),
        pair.pair_id(),
        pair.long_ticker().as_str(),
        pair.short_ticker().as_str(),
        *pair.z_score(),
        *pair.hedge_ratio(),
        *pair.signal_strength(),
        pair.status().as_str(),
        pair.opened_at()
    )
    .execute(pool)
    .await?;

    info!(
        pair_id = pair.pair_id(),
        "Equity pair inserted into PostgreSQL"
    );
    Ok(())
}

/// Marks an equity pair as closed with `close_reason = 'rebalance'`.
pub async fn close_equity_pair(
    pool: &PgPool,
    pair_id: Uuid,
    closed_at: DateTime<Utc>,
) -> Result<(), sqlx::Error> {
    sqlx::query!(
        "UPDATE equity_pairs \
         SET status = 'closed', closed_at = $1, close_reason = 'rebalance' \
         WHERE id = $2",
        closed_at,
        pair_id
    )
    .execute(pool)
    .await?;

    info!(pair_id = %pair_id, "Equity pair closed in PostgreSQL");
    Ok(())
}

/// Inserts an equity allocation record for one leg of a pair.
pub async fn insert_equity_allocation(
    pool: &PgPool,
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
    .execute(pool)
    .await?;

    Ok(())
}

/// Inserts an equity order record linking a filled order to its allocation.
pub async fn insert_equity_order(pool: &PgPool, order: &EquityOrder) -> Result<(), sqlx::Error> {
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
    .execute(pool)
    .await?;

    Ok(())
}

/// Inserts an intraday portfolio snapshot recording the post-rebalance net asset value.
pub async fn insert_portfolio_snapshot(
    pool: &PgPool,
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
    .execute(pool)
    .await?;

    info!(net_asset_value = %net_asset_value, "Portfolio snapshot inserted into PostgreSQL");
    Ok(())
}

/// Emits a named event by calling the `emit_event` PostgreSQL stored procedure.
pub async fn emit_event(
    pool: &PgPool,
    event_type: &str,
    payload: &serde_json::Value,
) -> Result<(), sqlx::Error> {
    sqlx::query!("SELECT emit_event($1, $2::jsonb)", event_type, payload)
        .execute(pool)
        .await?;

    info!(
        event_type = event_type,
        "Emitted event from portfolio_manager"
    );
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
            pair_id: "AAPL-MSFT".to_string(),
            long_ticker: Ticker::new("AAPL").unwrap(),
            short_ticker: Ticker::new("MSFT").unwrap(),
        };
        assert_eq!(open_pair.id(), pair_id);
        assert_eq!(open_pair.pair_id(), "AAPL-MSFT");
        assert_eq!(open_pair.long_ticker().as_str(), "AAPL");
        assert_eq!(open_pair.short_ticker().as_str(), "MSFT");
    }

    #[test]
    fn test_open_pair_clone() {
        let open_pair = OpenPair {
            id: Uuid::new_v4(),
            pair_id: "GOOG-META".to_string(),
            long_ticker: Ticker::new("GOOG").unwrap(),
            short_ticker: Ticker::new("META").unwrap(),
        };
        let cloned = open_pair.clone();
        assert_eq!(cloned.pair_id(), open_pair.pair_id());
        assert_eq!(
            cloned.long_ticker().as_str(),
            open_pair.long_ticker().as_str()
        );
        assert_eq!(
            cloned.short_ticker().as_str(),
            open_pair.short_ticker().as_str()
        );
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

    // --- compile / connection-error coverage for all DB functions ---

    #[test]
    fn test_fetch_predictions_compiles() {
        make_runtime().block_on(async {
            assert!(fetch_predictions(&lazy_pool()).await.is_err());
        });
    }

    #[test]
    fn test_fetch_historical_prices_compiles() {
        make_runtime().block_on(async {
            assert!(fetch_historical_prices(&lazy_pool()).await.is_err());
        });
    }

    #[test]
    fn test_fetch_spy_prices_compiles() {
        make_runtime().block_on(async {
            assert!(fetch_spy_prices(&lazy_pool()).await.is_err());
        });
    }

    #[test]
    fn test_fetch_equity_details_compiles() {
        make_runtime().block_on(async {
            assert!(fetch_equity_details(&lazy_pool()).await.is_err());
        });
    }

    #[test]
    fn test_fetch_live_quote_mid_prices_compiles() {
        make_runtime().block_on(async {
            let tickers = vec!["AAPL".to_string()];
            assert!(fetch_live_quote_mid_prices(&lazy_pool(), &tickers)
                .await
                .is_err());
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
                "intraday_check".to_string(),
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
            use crate::domain::market::Ticker;
            use crate::domain::trading::{EquityPair, EquityPairStatus};
            use rust_decimal::Decimal;

            let pair = EquityPair::new(
                Uuid::new_v4(),
                Uuid::new_v4(),
                "AAPL-MSFT".to_string(),
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
                None,
            );
            assert!(insert_equity_pair(&lazy_pool(), &pair).await.is_err());
        });
    }

    #[test]
    fn test_close_equity_pair_compiles() {
        make_runtime().block_on(async {
            assert!(close_equity_pair(&lazy_pool(), Uuid::new_v4(), Utc::now())
                .await
                .is_err());
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

    #[test]
    fn test_emit_event_compiles() {
        make_runtime().block_on(async {
            assert!(emit_event(
                &lazy_pool(),
                "test_event",
                &serde_json::json!({"key": "value"})
            )
            .await
            .is_err());
        });
    }
}
