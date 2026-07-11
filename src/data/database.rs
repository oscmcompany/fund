use crate::domain::market::{EquityBar, EquityDetail, EquityQuote, PairID, Ticker};
use crate::domain::predictions::{EquityPrediction, ModelRun, ModelRunStatus};
use crate::domain::trading::{
    AllocationAction, AllocationSide, EquityAllocation, EquityOrder, EquityPair, EquityPairStatus,
    EquityPortfolioSnapshot, EquityRebalanceSession, RebalanceSessionStatus, SnapshotType,
};
use chrono::{DateTime, Days, NaiveDate, Utc};
use sqlx::PgPool;
use std::collections::HashSet;
use tracing::{debug, info, warn};

/// Collapse bars sharing a `(ticker, timestamp)` key, keeping the last
/// occurrence.
///
/// Massive's grouped-daily endpoint occasionally returns a ticker more than once
/// for a date. A single `INSERT ... ON CONFLICT (ticker, timestamp) DO UPDATE`
/// rejects a repeated conflict target with "ON CONFLICT DO UPDATE command cannot
/// affect row a second time", which fails the whole 1000-row chunk. Keeping the
/// last occurrence mirrors the upsert's latest-write semantics.
fn deduplicate_equity_bars(bars: &[EquityBar]) -> Vec<EquityBar> {
    let mut seen: HashSet<(Ticker, DateTime<Utc>)> = HashSet::with_capacity(bars.len());
    let mut deduplicated: Vec<EquityBar> = Vec::with_capacity(bars.len());
    for bar in bars.iter().rev() {
        if seen.insert((bar.ticker().clone(), bar.timestamp())) {
            deduplicated.push(bar.clone());
        }
    }
    deduplicated.reverse();
    deduplicated
}

pub async fn insert_equity_bars(pool: &PgPool, bars: &[EquityBar]) -> Result<u64, sqlx::Error> {
    if bars.is_empty() {
        return Ok(0);
    }

    let bars = deduplicate_equity_bars(bars);

    let mut rows_affected: u64 = 0;
    let mut transaction = pool.begin().await?;

    for chunk in bars.chunks(1000) {
        let mut query_builder = sqlx::QueryBuilder::new(
            "INSERT INTO equity_bars (ticker, timestamp, open_price, high_price, low_price, close_price, volume, volume_weighted_average_price, transactions, inserted_at) ",
        );

        query_builder.push_values(chunk, |mut builder, bar| {
            builder
                .push_bind(bar.ticker())
                .push_bind(bar.timestamp())
                .push_bind(bar.open_price())
                .push_bind(bar.high_price())
                .push_bind(bar.low_price())
                .push_bind(bar.close_price())
                .push_bind(bar.volume())
                .push_bind(bar.volume_weighted_average_price())
                .push_bind(bar.transactions())
                .push_bind(bar.inserted_at());
        });

        query_builder.push(
            " ON CONFLICT (ticker, timestamp) DO UPDATE SET \
             open_price = EXCLUDED.open_price, \
             high_price = EXCLUDED.high_price, \
             low_price = EXCLUDED.low_price, \
             close_price = EXCLUDED.close_price, \
             volume = EXCLUDED.volume, \
             volume_weighted_average_price = EXCLUDED.volume_weighted_average_price, \
             transactions = EXCLUDED.transactions, \
             inserted_at = EXCLUDED.inserted_at",
        );

        let result = query_builder.build().execute(&mut *transaction).await?;
        rows_affected += result.rows_affected();
    }

    transaction.commit().await?;
    info!("Inserted {} equity bars into PostgreSQL", rows_affected);
    Ok(rows_affected)
}

pub async fn insert_equity_quotes(
    pool: &PgPool,
    quotes: &[EquityQuote],
) -> Result<u64, sqlx::Error> {
    if quotes.is_empty() {
        return Ok(0);
    }

    let mut rows_affected: u64 = 0;
    let mut transaction = pool.begin().await?;

    for chunk in quotes.chunks(1000) {
        let mut query_builder = sqlx::QueryBuilder::new(
            "INSERT INTO equity_quotes (timestamp, ticker, bid_price, ask_price, bid_size, ask_size) ",
        );

        query_builder.push_values(chunk, |mut builder, quote| {
            builder
                .push_bind(quote.timestamp())
                .push_bind(quote.ticker())
                .push_bind(quote.bid_price())
                .push_bind(quote.ask_price())
                .push_bind(quote.bid_size())
                .push_bind(quote.ask_size());
        });

        let result = query_builder.build().execute(&mut *transaction).await?;
        rows_affected += result.rows_affected();
    }

    transaction.commit().await?;
    debug!("Inserted {} equity quotes into PostgreSQL", rows_affected);
    Ok(rows_affected)
}

pub async fn get_active_tickers(pool: &PgPool) -> Result<Vec<Ticker>, sqlx::Error> {
    let rows = sqlx::query!(
        r#"SELECT DISTINCT ea.ticker
           FROM equity_allocations ea
           JOIN equity_pairs ep ON ea.equity_pair_id = ep.id
           WHERE ep.status = 'open'
           ORDER BY ea.ticker"#
    )
    .fetch_all(pool)
    .await?;

    let tickers: Vec<Ticker> = rows
        .into_iter()
        .filter_map(|row| Ticker::new(&row.ticker))
        .collect();
    debug!("Queried {} active tickers from PostgreSQL", tickers.len());
    Ok(tickers)
}

/// Seeds `equity_details` from the provided rows using an idempotent upsert.
///
/// Assumes a blank database on first startup. Subsequent runs are safe because
/// `ON CONFLICT (ticker) DO NOTHING` skips rows that already exist.
pub async fn seed_equity_details(pool: &PgPool, rows: &[EquityDetail]) -> Result<u64, sqlx::Error> {
    if rows.is_empty() {
        warn!("No equity details rows provided for seeding; skipping");
        return Ok(0);
    }

    let mut rows_affected: u64 = 0;

    for chunk in rows.chunks(1000) {
        let mut query_builder =
            sqlx::QueryBuilder::new("INSERT INTO equity_details (ticker, sector, industry) ");

        query_builder.push_values(chunk, |mut builder, detail| {
            builder
                .push_bind(detail.ticker())
                .push_bind(detail.sector())
                .push_bind(detail.industry());
        });

        query_builder.push(" ON CONFLICT (ticker) DO NOTHING");

        let result = query_builder.build().execute(pool).await?;
        rows_affected += result.rows_affected();
    }

    info!("Seeded equity_details with {} rows", rows_affected);
    Ok(rows_affected)
}

fn date_to_utc_range(date: NaiveDate) -> (DateTime<Utc>, DateTime<Utc>) {
    let start = date.and_hms_opt(0, 0, 0).unwrap().and_utc();
    let end = date
        .checked_add_days(Days::new(1))
        .expect("date out of representable range")
        .and_hms_opt(0, 0, 0)
        .unwrap()
        .and_utc();
    (start, end)
}

pub async fn query_equity_quotes_for_date(
    pool: &PgPool,
    date: NaiveDate,
) -> Result<Vec<EquityQuote>, sqlx::Error> {
    let (date_start, date_end) = date_to_utc_range(date);

    let rows = sqlx::query!(
        "SELECT timestamp, ticker, bid_price, ask_price, bid_size, ask_size
         FROM equity_quotes
         WHERE timestamp >= $1 AND timestamp < $2
         ORDER BY timestamp ASC",
        date_start,
        date_end
    )
    .fetch_all(pool)
    .await?;

    let quotes: Vec<EquityQuote> = rows
        .into_iter()
        .map(|row| {
            let ticker = Ticker::new(&row.ticker).ok_or_else(|| {
                sqlx::Error::Decode(format!("Invalid ticker: {}", row.ticker).into())
            })?;
            Ok(EquityQuote::new(
                row.timestamp,
                ticker,
                row.bid_price,
                row.ask_price,
                row.bid_size,
                row.ask_size,
            ))
        })
        .collect::<Result<Vec<_>, sqlx::Error>>()?;

    debug!("Queried {} equity quotes for {}", quotes.len(), date);
    Ok(quotes)
}

pub async fn delete_equity_quotes_for_date(
    pool: &PgPool,
    date: NaiveDate,
) -> Result<u64, sqlx::Error> {
    let (date_start, date_end) = date_to_utc_range(date);

    let result = sqlx::query!(
        "DELETE FROM equity_quotes WHERE timestamp >= $1 AND timestamp < $2",
        date_start,
        date_end
    )
    .execute(pool)
    .await?;

    let deleted = result.rows_affected();
    info!(
        "Deleted {} equity quotes from PostgreSQL for {}",
        deleted, date
    );
    Ok(deleted)
}

pub async fn query_equity_bars_for_date(
    pool: &PgPool,
    date: NaiveDate,
) -> Result<Vec<EquityBar>, sqlx::Error> {
    let (date_start, date_end) = date_to_utc_range(date);

    let rows = sqlx::query!(
        "SELECT ticker, timestamp, open_price, high_price, low_price, close_price, volume,
         volume_weighted_average_price, transactions, inserted_at
         FROM equity_bars
         WHERE timestamp >= $1 AND timestamp < $2
         ORDER BY ticker ASC, timestamp ASC",
        date_start,
        date_end
    )
    .fetch_all(pool)
    .await?;

    let bars: Vec<EquityBar> = rows
        .into_iter()
        .map(|row| {
            let ticker = Ticker::new(&row.ticker).ok_or_else(|| {
                sqlx::Error::Decode(format!("Invalid ticker: {}", row.ticker).into())
            })?;
            Ok(EquityBar::new(
                ticker,
                row.timestamp,
                row.open_price,
                row.high_price,
                row.low_price,
                row.close_price,
                row.volume,
                row.volume_weighted_average_price,
                row.transactions,
                row.inserted_at,
            ))
        })
        .collect::<Result<Vec<_>, sqlx::Error>>()?;

    debug!("Queried {} equity bars for {}", bars.len(), date);
    Ok(bars)
}

pub async fn query_equity_rebalance_sessions(
    pool: &PgPool,
) -> Result<Vec<EquityRebalanceSession>, sqlx::Error> {
    let rows = sqlx::query!(
        "SELECT id, triggered_at, trigger_reason, model_run_id, completed_at, status
         FROM equity_rebalance_sessions
         ORDER BY triggered_at ASC"
    )
    .fetch_all(pool)
    .await?;

    rows.into_iter()
        .map(|row| {
            let status = RebalanceSessionStatus::parse(&row.status).ok_or_else(|| {
                sqlx::Error::Decode(
                    format!("Invalid rebalance session status: {}", row.status).into(),
                )
            })?;
            Ok(EquityRebalanceSession::new(
                row.id,
                row.triggered_at,
                row.trigger_reason,
                row.model_run_id,
                row.completed_at,
                status,
            ))
        })
        .collect()
}

pub async fn query_equity_pairs(pool: &PgPool) -> Result<Vec<EquityPair>, sqlx::Error> {
    let rows = sqlx::query!(
        "SELECT id, rebalance_id, pair_id, long_ticker, short_ticker, z_score, hedge_ratio,
         signal_strength, status, opened_at, closed_at, realized_profit_and_loss,
         return_percent
         FROM equity_pairs
         ORDER BY opened_at ASC"
    )
    .fetch_all(pool)
    .await?;

    rows.into_iter()
        .map(|row| {
            let long_ticker = Ticker::new(&row.long_ticker).ok_or_else(|| {
                sqlx::Error::Decode(format!("Invalid long ticker: {}", row.long_ticker).into())
            })?;
            let short_ticker = Ticker::new(&row.short_ticker).ok_or_else(|| {
                sqlx::Error::Decode(format!("Invalid short ticker: {}", row.short_ticker).into())
            })?;
            let status = EquityPairStatus::parse(&row.status).ok_or_else(|| {
                sqlx::Error::Decode(format!("Invalid equity pair status: {}", row.status).into())
            })?;
            let pair_id = PairID::parse(&row.pair_id).ok_or_else(|| {
                sqlx::Error::Decode(format!("Invalid pair id: {}", row.pair_id).into())
            })?;
            let expected_pair_id = format!("{}-{}", long_ticker.as_str(), short_ticker.as_str());
            if pair_id.as_str() != expected_pair_id {
                return Err(sqlx::Error::Decode(
                    format!(
                        "Pair ID/ticker mismatch: pair_id={}, expected={}",
                        pair_id.as_str(),
                        expected_pair_id
                    )
                    .into(),
                ));
            }
            Ok(EquityPair::new(
                row.id,
                row.rebalance_id,
                pair_id,
                long_ticker,
                short_ticker,
                row.z_score,
                row.hedge_ratio,
                row.signal_strength,
                status,
                row.opened_at,
                row.closed_at,
                row.realized_profit_and_loss,
                row.return_percent,
            ))
        })
        .collect()
}

pub async fn query_equity_allocations(pool: &PgPool) -> Result<Vec<EquityAllocation>, sqlx::Error> {
    let rows = sqlx::query!(
        "SELECT id, rebalance_id, equity_pair_id, generated_at, model_run_id, ticker, side,
         action, dollar_amount, entry_price, quantity, notional
         FROM equity_allocations
         ORDER BY generated_at ASC"
    )
    .fetch_all(pool)
    .await?;

    rows.into_iter()
        .map(|row| {
            let ticker = Ticker::new(&row.ticker).ok_or_else(|| {
                sqlx::Error::Decode(format!("Invalid ticker: {}", row.ticker).into())
            })?;
            let side = AllocationSide::parse(&row.side).ok_or_else(|| {
                sqlx::Error::Decode(format!("Invalid allocation side: {}", row.side).into())
            })?;
            let action = AllocationAction::parse(&row.action).ok_or_else(|| {
                sqlx::Error::Decode(format!("Invalid allocation action: {}", row.action).into())
            })?;
            Ok(EquityAllocation::new(
                row.id,
                row.rebalance_id,
                row.equity_pair_id,
                row.generated_at,
                row.model_run_id,
                ticker,
                side,
                action,
                row.dollar_amount,
                row.entry_price,
                row.quantity,
                row.notional,
            ))
        })
        .collect()
}

pub async fn query_equity_orders(pool: &PgPool) -> Result<Vec<EquityOrder>, sqlx::Error> {
    let rows = sqlx::query!(
        "SELECT id, allocation_id, submitted_at, ticker, side, quantity, order_type,
         limit_price, alpaca_order_id
         FROM equity_orders
         ORDER BY submitted_at ASC"
    )
    .fetch_all(pool)
    .await?;

    rows.into_iter()
        .map(|row| {
            let ticker = Ticker::new(&row.ticker).ok_or_else(|| {
                sqlx::Error::Decode(format!("Invalid ticker: {}", row.ticker).into())
            })?;
            let side = AllocationSide::parse(&row.side).ok_or_else(|| {
                sqlx::Error::Decode(format!("Invalid order side: {}", row.side).into())
            })?;
            Ok(EquityOrder::new(
                row.id,
                row.allocation_id,
                row.submitted_at,
                ticker,
                side,
                row.quantity,
                row.order_type,
                row.limit_price,
                row.alpaca_order_id,
            ))
        })
        .collect()
}

pub async fn query_equity_portfolio_snapshots(
    pool: &PgPool,
) -> Result<Vec<EquityPortfolioSnapshot>, sqlx::Error> {
    let rows = sqlx::query!(
        "SELECT id, snapshot_timestamp, snapshot_type, net_asset_value, gross_return,
         net_return, total_slippage_cost, created_at
         FROM equity_portfolio_snapshots
         ORDER BY snapshot_timestamp ASC"
    )
    .fetch_all(pool)
    .await?;

    rows.into_iter()
        .map(|row| {
            let snapshot_type = SnapshotType::parse(&row.snapshot_type).ok_or_else(|| {
                sqlx::Error::Decode(format!("Invalid snapshot type: {}", row.snapshot_type).into())
            })?;
            Ok(EquityPortfolioSnapshot::new(
                row.id,
                row.snapshot_timestamp,
                snapshot_type,
                row.net_asset_value,
                row.gross_return,
                row.net_return,
                row.total_slippage_cost,
                row.created_at,
            ))
        })
        .collect()
}

pub async fn query_equity_predictions_for_date(
    pool: &PgPool,
    date: NaiveDate,
) -> Result<Vec<EquityPrediction>, sqlx::Error> {
    let (date_start, date_end) = date_to_utc_range(date);

    let rows = sqlx::query!(
        "SELECT correlation_id, model_run_id, ticker, timestamp, quantile_10, quantile_50,
         quantile_90, created_at
         FROM equity_predictions
         WHERE timestamp >= $1 AND timestamp < $2
         ORDER BY ticker ASC, timestamp ASC",
        date_start,
        date_end
    )
    .fetch_all(pool)
    .await?;

    let predictions: Vec<EquityPrediction> = rows
        .into_iter()
        .map(|row| {
            let ticker = Ticker::new(&row.ticker).ok_or_else(|| {
                sqlx::Error::Decode(format!("Invalid ticker from database: {}", row.ticker).into())
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

    debug!(
        "Queried {} equity predictions for {}",
        predictions.len(),
        date
    );
    Ok(predictions)
}

pub async fn query_model_runs(pool: &PgPool) -> Result<Vec<ModelRun>, sqlx::Error> {
    let rows = sqlx::query!(
        "SELECT id, run_id, model_name, artifact_key, training_data_key, start_date, end_date,
         lookback_days, status, continuous_ranked_probability_score, directional_accuracy,
         quantile_coverage, drift_status, stage_counts, started_at, completed_at
         FROM model_runs
         ORDER BY started_at ASC",
    )
    .fetch_all(pool)
    .await?;

    rows.into_iter()
        .map(|row| {
            let status = ModelRunStatus::parse(&row.status).ok_or_else(|| {
                sqlx::Error::Decode(format!("Invalid model run status: {}", row.status).into())
            })?;
            Ok(ModelRun::new(
                row.id,
                row.run_id,
                row.model_name,
                row.artifact_key,
                row.training_data_key,
                row.start_date,
                row.end_date,
                row.lookback_days,
                status,
                row.continuous_ranked_probability_score,
                row.directional_accuracy,
                row.quantile_coverage,
                row.drift_status,
                row.stage_counts,
                row.started_at,
                row.completed_at,
            ))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::market::{EquityBar, Ticker};
    use chrono::Utc;

    fn test_pool() -> PgPool {
        PgPool::connect_lazy("postgresql://localhost:5432/fund_test_nonexistent")
            .expect("lazy pool creation should not fail")
    }

    fn sample_bars() -> Vec<EquityBar> {
        let now = Utc::now();
        vec![
            EquityBar::new(
                Ticker::new("AAPL").unwrap(),
                now,
                150.0,
                155.0,
                149.0,
                153.0,
                1_000_000,
                Some(152.0),
                Some(50_000),
                now,
            ),
            EquityBar::new(
                Ticker::new("MSFT").unwrap(),
                now,
                350.0,
                355.0,
                349.0,
                353.0,
                500_000,
                Some(352.0),
                Some(25_000),
                now,
            ),
        ]
    }

    #[test]
    fn test_sample_bars_are_valid() {
        let bars = sample_bars();
        assert_eq!(bars.len(), 2);
        assert_eq!(bars[0].ticker(), "AAPL");
        assert_eq!(bars[1].ticker(), "MSFT");
    }

    #[test]
    fn test_deduplicate_equity_bars_keeps_last_occurrence_per_key() {
        let now = Utc::now();
        let make = |ticker: &str, close: f64| {
            EquityBar::new(
                Ticker::new(ticker).unwrap(),
                now,
                close,
                close,
                close,
                close,
                1,
                None,
                None,
                now,
            )
        };

        // AAPL appears twice for the same timestamp, as Massive sometimes
        // returns it. The duplicate would trip "ON CONFLICT DO UPDATE command
        // cannot affect row a second time" if passed to the upsert unchanged.
        let bars = vec![
            make("AAPL", 150.0),
            make("MSFT", 350.0),
            make("AAPL", 151.0),
        ];

        let deduplicated = deduplicate_equity_bars(&bars);

        assert_eq!(deduplicated.len(), 2, "the duplicate ticker must collapse");
        let aapl = deduplicated
            .iter()
            .find(|bar| bar.ticker() == "AAPL")
            .expect("AAPL must survive deduplication");
        assert_eq!(
            aapl.close_price(),
            151.0,
            "the last occurrence wins, matching the upsert's latest-write semantics",
        );
        assert!(deduplicated.iter().any(|bar| bar.ticker() == "MSFT"));
    }

    #[test]
    fn test_deduplicate_equity_bars_distinct_keys_are_untouched() {
        let bars = sample_bars();
        let deduplicated = deduplicate_equity_bars(&bars);
        assert_eq!(deduplicated.len(), bars.len());
    }

    #[test]
    fn test_insert_empty_bars_returns_zero() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            let pool = test_pool();
            let result = insert_equity_bars(&pool, &[]).await;
            assert!(result.is_ok());
            assert_eq!(result.unwrap(), 0);
        });
    }

    #[test]
    fn test_insert_empty_quotes_returns_zero() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            let pool = test_pool();
            let result = insert_equity_quotes(&pool, &[]).await;
            assert!(result.is_ok());
            assert_eq!(result.unwrap(), 0);
        });
    }

    #[test]
    fn test_get_active_tickers_compiles() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            let pool = test_pool();
            let result = get_active_tickers(&pool).await;
            assert!(result.is_err());
        });
    }

    #[test]
    fn test_query_equity_quotes_for_date_compiles() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            use chrono::NaiveDate;
            let pool = test_pool();
            let date = NaiveDate::from_ymd_opt(2026, 5, 1).unwrap();
            let result = query_equity_quotes_for_date(&pool, date).await;
            assert!(result.is_err());
        });
    }

    #[test]
    fn test_delete_equity_quotes_for_date_compiles() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            use chrono::NaiveDate;
            let pool = test_pool();
            let date = NaiveDate::from_ymd_opt(2026, 5, 1).unwrap();
            let result = delete_equity_quotes_for_date(&pool, date).await;
            assert!(result.is_err());
        });
    }

    #[test]
    fn test_query_equity_bars_for_date_compiles() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            use chrono::NaiveDate;
            let pool = test_pool();
            let date = NaiveDate::from_ymd_opt(2026, 5, 1).unwrap();
            let result = query_equity_bars_for_date(&pool, date).await;
            assert!(result.is_err());
        });
    }

    #[test]
    fn test_query_equity_rebalance_sessions_compiles() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            let pool = test_pool();
            let result = query_equity_rebalance_sessions(&pool).await;
            assert!(result.is_err());
        });
    }

    #[test]
    fn test_query_equity_pairs_compiles() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            let pool = test_pool();
            let result = query_equity_pairs(&pool).await;
            assert!(result.is_err());
        });
    }

    #[test]
    fn test_query_equity_allocations_compiles() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            let pool = test_pool();
            let result = query_equity_allocations(&pool).await;
            assert!(result.is_err());
        });
    }

    #[test]
    fn test_query_equity_orders_compiles() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            let pool = test_pool();
            let result = query_equity_orders(&pool).await;
            assert!(result.is_err());
        });
    }

    #[test]
    fn test_query_equity_portfolio_snapshots_compiles() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            let pool = test_pool();
            let result = query_equity_portfolio_snapshots(&pool).await;
            assert!(result.is_err());
        });
    }

    #[test]
    fn test_query_equity_predictions_for_date_compiles() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            use chrono::NaiveDate;
            let pool = PgPool::connect_lazy("postgresql://localhost:5432/fund_test_nonexistent")
                .expect("lazy pool creation should not fail");
            let date = NaiveDate::from_ymd_opt(2026, 5, 1).unwrap();
            let result = query_equity_predictions_for_date(&pool, date).await;
            assert!(result.is_err());
        });
    }

    #[test]
    fn test_query_model_runs_compiles() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            let pool = PgPool::connect_lazy("postgresql://localhost:5432/fund_test_nonexistent")
                .expect("lazy pool creation should not fail");
            let result = query_model_runs(&pool).await;
            assert!(result.is_err());
        });
    }
}
