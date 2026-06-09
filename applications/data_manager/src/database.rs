use chrono::{DateTime, Days, NaiveDate, Utc};
use internal::market::{EquityBar, EquityDetails, EquityQuote, Ticker};
use internal::predictions::{EquityPrediction, ModelRun};
use internal::trading::{
    EquityAllocation, EquityOrder, EquityPair, EquityPortfolioSnapshot, EquityRebalanceSession,
};
use sqlx::PgPool;
use tracing::{debug, info, warn};

pub async fn insert_equity_bars(pool: &PgPool, bars: &[EquityBar]) -> Result<u64, sqlx::Error> {
    if bars.is_empty() {
        return Ok(0);
    }

    let mut rows_affected: u64 = 0;

    for chunk in bars.chunks(1000) {
        let mut query_builder = sqlx::QueryBuilder::new(
            "INSERT INTO equity_bars (ticker, timestamp, open_price, high_price, low_price, close_price, volume, volume_weighted_average_price, transactions, inserted_at) ",
        );

        query_builder.push_values(chunk, |mut builder, bar| {
            builder
                .push_bind(&bar.ticker)
                .push_bind(bar.timestamp)
                .push_bind(bar.open_price)
                .push_bind(bar.high_price)
                .push_bind(bar.low_price)
                .push_bind(bar.close_price)
                .push_bind(bar.volume)
                .push_bind(bar.volume_weighted_average_price)
                .push_bind(bar.transactions)
                .push_bind(bar.inserted_at);
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

        let result = query_builder.build().execute(pool).await?;
        rows_affected += result.rows_affected();
    }

    info!("Inserted {} equity bars into PostgreSQL", rows_affected);
    Ok(rows_affected)
}

pub async fn claim_pending_job(
    pool: &PgPool,
    job_name: &str,
) -> Result<Option<(i64, DateTime<Utc>)>, sqlx::Error> {
    let row = sqlx::query!(
        r#"UPDATE scheduled_jobs
           SET claimed_at = now(), status = 'claimed'
           WHERE id = (
               SELECT id FROM scheduled_jobs
               WHERE job_name = $1 AND status = 'pending' AND scheduled_at <= now()
               ORDER BY scheduled_at
               LIMIT 1
               FOR UPDATE SKIP LOCKED
           )
           RETURNING id, scheduled_at"#,
        job_name
    )
    .fetch_optional(pool)
    .await?;

    if let Some(ref r) = row {
        debug!("Claimed job {} with id {}", job_name, r.id);
    }
    Ok(row.map(|r| (r.id, r.scheduled_at)))
}

pub async fn complete_job(pool: &PgPool, job_id: i64, result: &str) -> Result<(), sqlx::Error> {
    sqlx::query!(
        r#"UPDATE scheduled_jobs
           SET completed_at = now(), status = 'completed', result = $2
           WHERE id = $1"#,
        job_id,
        result
    )
    .execute(pool)
    .await?;

    debug!("Completed job {}", job_id);
    Ok(())
}

pub async fn fail_job(pool: &PgPool, job_id: i64, error_message: &str) -> Result<(), sqlx::Error> {
    sqlx::query!(
        r#"UPDATE scheduled_jobs
           SET completed_at = now(), status = 'failed', result = $2
           WHERE id = $1"#,
        job_id,
        error_message
    )
    .execute(pool)
    .await?;

    warn!("Failed job {}: {}", job_id, error_message);
    Ok(())
}

pub async fn insert_equity_quotes(
    pool: &PgPool,
    quotes: &[EquityQuote],
) -> Result<u64, sqlx::Error> {
    if quotes.is_empty() {
        return Ok(0);
    }

    let mut rows_affected: u64 = 0;

    for chunk in quotes.chunks(1000) {
        let mut query_builder = sqlx::QueryBuilder::new(
            "INSERT INTO equity_quotes (timestamp, ticker, bid_price, ask_price, bid_size, ask_size) ",
        );

        query_builder.push_values(chunk, |mut builder, quote| {
            builder
                .push_bind(quote.timestamp)
                .push_bind(&quote.ticker)
                .push_bind(quote.bid_price)
                .push_bind(quote.ask_price)
                .push_bind(quote.bid_size)
                .push_bind(quote.ask_size);
        });

        let result = query_builder.build().execute(pool).await?;
        rows_affected += result.rows_affected();
    }

    debug!("Inserted {} equity quotes into PostgreSQL", rows_affected);
    Ok(rows_affected)
}

pub async fn get_active_tickers(pool: &PgPool) -> Result<Vec<String>, sqlx::Error> {
    let rows = sqlx::query!(
        r#"SELECT DISTINCT ea.ticker
           FROM equity_allocations ea
           JOIN equity_pairs ep ON ea.equity_pair_id = ep.id
           WHERE ep.status = 'open'
           ORDER BY ea.ticker"#
    )
    .fetch_all(pool)
    .await?;

    let tickers: Vec<String> = rows.into_iter().map(|row| row.ticker).collect();
    debug!("Queried {} active tickers from PostgreSQL", tickers.len());
    Ok(tickers)
}

pub async fn emit_equity_bars_synced(pool: &PgPool) -> Result<(), sqlx::Error> {
    sqlx::query!(
        "SELECT emit_event($1, $2::jsonb)",
        "equity_bars_synced",
        serde_json::Value::Object(Default::default()),
    )
    .execute(pool)
    .await?;
    info!("Emitted equity_bars_synced event");
    Ok(())
}

pub async fn populate_equity_details_if_empty(
    pool: &PgPool,
    rows: &[EquityDetails],
) -> Result<u64, sqlx::Error> {
    let mut transaction = pool.begin().await?;

    let row = sqlx::query!(r#"SELECT COUNT(*) as "count!" FROM equity_details"#)
        .fetch_one(&mut *transaction)
        .await?;

    if row.count > 0 {
        info!(
            "equity_details already populated with {} rows, skipping migration",
            row.count
        );
        return Ok(0);
    }

    if rows.is_empty() {
        warn!("equity_details is empty and no rows provided; skipping migration");
        return Ok(0);
    }

    let mut rows_affected: u64 = 0;

    for chunk in rows.chunks(1000) {
        let mut query_builder =
            sqlx::QueryBuilder::new("INSERT INTO equity_details (ticker, sector, industry) ");

        query_builder.push_values(chunk, |mut builder, details| {
            builder
                .push_bind(&details.ticker)
                .push_bind(&details.sector)
                .push_bind(&details.industry);
        });

        query_builder.push(" ON CONFLICT (ticker) DO NOTHING");

        let result = query_builder.build().execute(&mut *transaction).await?;
        rows_affected += result.rows_affected();
    }

    transaction.commit().await?;

    info!(
        "Populated equity_details with {} rows from S3 migration",
        rows_affected
    );
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
        .filter_map(|row| {
            Some(EquityQuote {
                timestamp: row.timestamp,
                ticker: Ticker::new(&row.ticker)?,
                bid_price: row.bid_price,
                ask_price: row.ask_price,
                bid_size: row.bid_size,
                ask_size: row.ask_size,
            })
        })
        .collect();

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
        .filter_map(|row| {
            Some(EquityBar {
                ticker: Ticker::new(&row.ticker)?,
                timestamp: row.timestamp,
                open_price: row.open_price,
                high_price: row.high_price,
                low_price: row.low_price,
                close_price: row.close_price,
                volume: row.volume,
                volume_weighted_average_price: row.volume_weighted_average_price,
                transactions: row.transactions,
                inserted_at: row.inserted_at,
            })
        })
        .collect();

    debug!("Queried {} equity bars for {}", bars.len(), date);
    Ok(bars)
}

pub async fn query_equity_rebalance_sessions(
    pool: &PgPool,
) -> Result<Vec<EquityRebalanceSession>, sqlx::Error> {
    sqlx::query_as!(
        EquityRebalanceSession,
        "SELECT id, triggered_at, trigger_reason, model_run_id, completed_at, status
         FROM equity_rebalance_sessions
         ORDER BY triggered_at ASC"
    )
    .fetch_all(pool)
    .await
}

pub async fn query_equity_pairs(pool: &PgPool) -> Result<Vec<EquityPair>, sqlx::Error> {
    sqlx::query_as!(
        EquityPair,
        "SELECT id, rebalance_id, pair_id, long_ticker, short_ticker, z_score, hedge_ratio,
         signal_strength, status, opened_at, closed_at, realized_profit_and_loss,
         return_percent, holding_days
         FROM equity_pairs
         ORDER BY opened_at ASC"
    )
    .fetch_all(pool)
    .await
}

pub async fn query_equity_allocations(pool: &PgPool) -> Result<Vec<EquityAllocation>, sqlx::Error> {
    sqlx::query_as!(
        EquityAllocation,
        "SELECT id, rebalance_id, equity_pair_id, generated_at, model_run_id, ticker, side,
         action, dollar_amount, entry_price, quantity, notional
         FROM equity_allocations
         ORDER BY generated_at ASC"
    )
    .fetch_all(pool)
    .await
}

pub async fn query_equity_orders(pool: &PgPool) -> Result<Vec<EquityOrder>, sqlx::Error> {
    sqlx::query_as!(
        EquityOrder,
        "SELECT id, allocation_id, submitted_at, ticker, side, quantity, order_type,
         limit_price, alpaca_order_id
         FROM equity_orders
         ORDER BY submitted_at ASC"
    )
    .fetch_all(pool)
    .await
}

pub async fn query_equity_portfolio_snapshots(
    pool: &PgPool,
) -> Result<Vec<EquityPortfolioSnapshot>, sqlx::Error> {
    sqlx::query_as!(
        EquityPortfolioSnapshot,
        "SELECT id, snapshot_timestamp, snapshot_type, net_asset_value, gross_return,
         net_return, total_slippage_cost, created_at
         FROM equity_portfolio_snapshots
         ORDER BY snapshot_timestamp ASC"
    )
    .fetch_all(pool)
    .await
}

pub async fn query_equity_predictions_for_date(
    pool: &PgPool,
    date: NaiveDate,
) -> Result<Vec<EquityPrediction>, sqlx::Error> {
    let (date_start, date_end) = date_to_utc_range(date);

    let predictions = sqlx::query_as!(
        EquityPrediction,
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

    debug!(
        "Queried {} equity predictions for {}",
        predictions.len(),
        date
    );
    Ok(predictions)
}

pub async fn query_model_runs(pool: &PgPool) -> Result<Vec<ModelRun>, sqlx::Error> {
    sqlx::query_as!(
        ModelRun,
        "SELECT id, run_id, model_name, artifact_key, training_data_key, start_date, end_date,
         lookback_days, status, continuous_ranked_probability_score, directional_accuracy,
         quantile_coverage, drift_status, stage_counts, started_at, completed_at
         FROM model_runs
         ORDER BY started_at ASC"
    )
    .fetch_all(pool)
    .await
}

pub async fn requeue_stale_claimed_jobs(
    pool: &PgPool,
    job_name: &str,
    stale_after: std::time::Duration,
) -> Result<u64, sqlx::Error> {
    let interval_secs = stale_after.as_secs() as f64;
    let result = sqlx::query!(
        r#"UPDATE scheduled_jobs
           SET status = 'pending', claimed_at = NULL
           WHERE job_name = $1
             AND status = 'claimed'
             AND claimed_at < now() - make_interval(secs => $2)"#,
        job_name,
        interval_secs
    )
    .execute(pool)
    .await?;
    Ok(result.rows_affected())
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use internal::market::{EquityBar, Ticker};

    fn sample_bars() -> Vec<EquityBar> {
        let now = Utc::now();
        vec![
            EquityBar {
                ticker: Ticker::new("AAPL").unwrap(),
                timestamp: now,
                open_price: 150.0,
                high_price: 155.0,
                low_price: 149.0,
                close_price: 153.0,
                volume: 1_000_000,
                volume_weighted_average_price: Some(152.0),
                transactions: Some(50_000),
                inserted_at: now,
            },
            EquityBar {
                ticker: Ticker::new("MSFT").unwrap(),
                timestamp: now,
                open_price: 350.0,
                high_price: 355.0,
                low_price: 349.0,
                close_price: 353.0,
                volume: 500_000,
                volume_weighted_average_price: Some(352.0),
                transactions: Some(25_000),
                inserted_at: now,
            },
        ]
    }

    #[test]
    fn test_sample_bars_are_valid() {
        let bars = sample_bars();
        assert_eq!(bars.len(), 2);
        assert_eq!(bars[0].ticker, "AAPL");
        assert_eq!(bars[1].ticker, "MSFT");
    }

    #[test]
    fn test_insert_empty_bars_returns_zero() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            let pool = PgPool::connect_lazy("postgresql://localhost:5432/fund_test_nonexistent")
                .expect("lazy pool creation should not fail");
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
            let pool = PgPool::connect_lazy("postgresql://localhost:5432/fund_test_nonexistent")
                .expect("lazy pool creation should not fail");
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
            let pool = PgPool::connect_lazy("postgresql://localhost:5432/fund_test_nonexistent")
                .expect("lazy pool creation should not fail");
            let result = get_active_tickers(&pool).await;
            assert!(result.is_err());
        });
    }

    #[test]
    fn test_requeue_stale_claimed_jobs_compiles() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            let pool = PgPool::connect_lazy("postgresql://localhost:5432/fund_test_nonexistent")
                .expect("lazy pool creation should not fail");
            let result = requeue_stale_claimed_jobs(
                &pool,
                "equity-bar-sync",
                std::time::Duration::from_secs(2 * 3600),
            )
            .await;
            assert!(result.is_err());
        });
    }

    #[test]
    fn test_emit_equity_bars_synced_compiles() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            let pool = PgPool::connect_lazy("postgresql://localhost:5432/fund_test_nonexistent")
                .expect("lazy pool creation should not fail");
            let result = emit_equity_bars_synced(&pool).await;
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
            let pool = PgPool::connect_lazy("postgresql://localhost:5432/fund_test_nonexistent")
                .expect("lazy pool creation should not fail");
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
            let pool = PgPool::connect_lazy("postgresql://localhost:5432/fund_test_nonexistent")
                .expect("lazy pool creation should not fail");
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
            let pool = PgPool::connect_lazy("postgresql://localhost:5432/fund_test_nonexistent")
                .expect("lazy pool creation should not fail");
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
            let pool = PgPool::connect_lazy("postgresql://localhost:5432/fund_test_nonexistent")
                .expect("lazy pool creation should not fail");
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
            let pool = PgPool::connect_lazy("postgresql://localhost:5432/fund_test_nonexistent")
                .expect("lazy pool creation should not fail");
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
            let pool = PgPool::connect_lazy("postgresql://localhost:5432/fund_test_nonexistent")
                .expect("lazy pool creation should not fail");
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
            let pool = PgPool::connect_lazy("postgresql://localhost:5432/fund_test_nonexistent")
                .expect("lazy pool creation should not fail");
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
            let pool = PgPool::connect_lazy("postgresql://localhost:5432/fund_test_nonexistent")
                .expect("lazy pool creation should not fail");
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
