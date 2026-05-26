use crate::data::{EquityBar, EquityQuote};
use chrono::DateTime;
use chrono::Utc;
use sqlx::postgres::PgRow;
use sqlx::{PgPool, Row};
use tracing::{debug, info, warn};

fn is_valid_equity_bar(bar: &EquityBar) -> bool {
    bar.open_price.is_some()
        && bar.high_price.is_some()
        && bar.low_price.is_some()
        && bar.close_price.is_some()
        && bar.volume.is_some()
        && DateTime::<Utc>::from_timestamp_millis(bar.timestamp).is_some()
}

pub async fn insert_equity_bars(pool: &PgPool, bars: &[EquityBar]) -> Result<u64, sqlx::Error> {
    if bars.is_empty() {
        return Ok(0);
    }

    let valid_bars: Vec<&EquityBar> = bars.iter().filter(|bar| is_valid_equity_bar(bar)).collect();

    let skipped = bars.len() - valid_bars.len();
    if skipped > 0 {
        warn!(
            "Skipping {} bars with null OHLCV or invalid timestamp",
            skipped
        );
    }

    if valid_bars.is_empty() {
        return Ok(0);
    }

    let mut rows_affected: u64 = 0;

    for chunk in valid_bars.chunks(1000) {
        let transaction_values: Vec<Option<i64>> = chunk
            .iter()
            .map(|bar| match bar.transactions {
                Some(transactions) => i64::try_from(transactions).map(Some).map_err(|_| {
                    sqlx::Error::Protocol(
                        format!(
                            "transactions value {} for {} at {} exceeds BIGINT",
                            transactions, bar.ticker, bar.timestamp
                        )
                        .into(),
                    )
                }),
                None => Ok(None),
            })
            .collect::<Result<Vec<Option<i64>>, sqlx::Error>>()?;

        let mut query_builder = sqlx::QueryBuilder::new(
            "INSERT INTO equity_bars (ticker, timestamp, open_price, high_price, low_price, close_price, volume, volume_weighted_average_price, transactions) ",
        );

        query_builder.push_values(
            chunk.iter().zip(transaction_values.iter()),
            |mut builder, (bar, transactions)| {
                let time = DateTime::<Utc>::from_timestamp_millis(bar.timestamp).unwrap();
                builder
                    .push_bind(&bar.ticker)
                    .push_bind(time)
                    .push_bind(bar.open_price.unwrap())
                    .push_bind(bar.high_price.unwrap())
                    .push_bind(bar.low_price.unwrap())
                    .push_bind(bar.close_price.unwrap())
                    .push_bind(bar.volume.unwrap())
                    .push_bind(bar.volume_weighted_average_price)
                    .push_bind(*transactions);
            },
        );

        query_builder.push(
            " ON CONFLICT (ticker, timestamp) DO UPDATE SET \
             open_price = EXCLUDED.open_price, \
             high_price = EXCLUDED.high_price, \
             low_price = EXCLUDED.low_price, \
             close_price = EXCLUDED.close_price, \
             volume = EXCLUDED.volume, \
             volume_weighted_average_price = EXCLUDED.volume_weighted_average_price, \
             transactions = EXCLUDED.transactions, \
             inserted_at = now()",
        );

        let result = query_builder.build().execute(pool).await?;
        rows_affected += result.rows_affected();
    }

    info!("Inserted {} equity bars into PostgreSQL", rows_affected);
    Ok(rows_affected)
}

pub async fn query_recent_equity_bars(
    pool: &PgPool,
    tickers: Option<&[String]>,
    days_back: i32,
) -> Result<Vec<EquityBar>, sqlx::Error> {
    debug!(
        "Querying recent equity bars, days_back: {}, tickers: {:?}",
        days_back, tickers
    );

    let rows: Vec<PgRow> = match tickers {
        Some(ticker_list) if !ticker_list.is_empty() => {
            sqlx::query(
                r#"SELECT ticker, timestamp, open_price, high_price, low_price, close_price, volume, volume_weighted_average_price, transactions
                   FROM equity_bars
                   WHERE timestamp >= now() - make_interval(days => $1)
                     AND ticker = ANY($2)
                   ORDER BY ticker, timestamp"#,
            )
            .bind(days_back)
            .bind(ticker_list)
            .fetch_all(pool)
            .await?
        }
        Some(_) => return Ok(Vec::new()),
        _ => {
            sqlx::query(
                r#"SELECT ticker, timestamp, open_price, high_price, low_price, close_price, volume, volume_weighted_average_price, transactions
                   FROM equity_bars
                   WHERE timestamp >= now() - make_interval(days => $1)
                   ORDER BY ticker, timestamp"#,
            )
            .bind(days_back)
            .fetch_all(pool)
            .await?
        }
    };

    let bars: Vec<EquityBar> = rows.iter().map(equity_bar_from_row).collect();
    info!("Queried {} equity bars from PostgreSQL", bars.len());
    Ok(bars)
}

fn equity_bar_from_row(row: &PgRow) -> EquityBar {
    let timestamp: DateTime<Utc> = row.get("timestamp");
    let transactions: Option<i64> = row.get("transactions");
    EquityBar {
        ticker: row.get("ticker"),
        timestamp: timestamp.timestamp_millis(),
        open_price: row.get::<Option<f64>, _>("open_price"),
        high_price: row.get::<Option<f64>, _>("high_price"),
        low_price: row.get::<Option<f64>, _>("low_price"),
        close_price: row.get::<Option<f64>, _>("close_price"),
        volume: row.get::<Option<i64>, _>("volume"),
        volume_weighted_average_price: row.get("volume_weighted_average_price"),
        transactions: transactions.map(|t| t as u64),
    }
}

pub async fn claim_pending_job(pool: &PgPool, job_name: &str) -> Result<Option<i64>, sqlx::Error> {
    let row: Option<(i64,)> = sqlx::query_as(
        r#"UPDATE scheduled_jobs
           SET claimed_at = now(), status = 'claimed'
           WHERE id = (
               SELECT id FROM scheduled_jobs
               WHERE job_name = $1 AND status = 'pending' AND scheduled_at <= now()
               ORDER BY scheduled_at
               LIMIT 1
               FOR UPDATE SKIP LOCKED
           )
           RETURNING id"#,
    )
    .bind(job_name)
    .fetch_optional(pool)
    .await?;

    let job_id = row.map(|(id,)| id);
    if let Some(id) = job_id {
        debug!("Claimed job {} with id {}", job_name, id);
    }
    Ok(job_id)
}

pub async fn complete_job(pool: &PgPool, job_id: i64, result: &str) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"UPDATE scheduled_jobs
           SET completed_at = now(), status = 'completed', result = $2
           WHERE id = $1"#,
    )
    .bind(job_id)
    .bind(result)
    .execute(pool)
    .await?;

    debug!("Completed job {}", job_id);
    Ok(())
}

pub async fn fail_job(pool: &PgPool, job_id: i64, error_message: &str) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"UPDATE scheduled_jobs
           SET completed_at = now(), status = 'failed', result = $2
           WHERE id = $1"#,
    )
    .bind(job_id)
    .bind(error_message)
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
    let rows: Vec<(String,)> = sqlx::query_as(
        r#"SELECT DISTINCT ea.ticker
           FROM equity_allocations ea
           JOIN equity_pairs ep ON ea.equity_pair_id = ep.id
           WHERE ep.status = 'open'
           ORDER BY ea.ticker"#,
    )
    .fetch_all(pool)
    .await?;

    let tickers: Vec<String> = rows.into_iter().map(|(ticker,)| ticker).collect();
    debug!("Queried {} active tickers from PostgreSQL", tickers.len());
    Ok(tickers)
}

pub async fn emit_equity_bars_synced(pool: &PgPool) -> Result<(), sqlx::Error> {
    sqlx::query("SELECT emit_event($1, $2::jsonb)")
        .bind("equity_bars_synced")
        .bind("{}")
        .execute(pool)
        .await?;
    info!("Emitted equity_bars_synced event");
    Ok(())
}

pub async fn set_data_bucket_guc(pool: &PgPool, bucket_name: &str) -> Result<(), sqlx::Error> {
    let alter_statement: String = sqlx::query_scalar(
        r#"SELECT format('ALTER DATABASE %I SET "app.data_bucket_name" = %L', current_database(), $1)"#,
    )
    .bind(bucket_name)
    .fetch_one(pool)
    .await?;
    sqlx::query(&alter_statement).execute(pool).await?;
    info!("Set app.data_bucket_name database GUC to {}", bucket_name);
    Ok(())
}

pub async fn requeue_stale_claimed_jobs(
    pool: &PgPool,
    job_name: &str,
    stale_after: std::time::Duration,
) -> Result<u64, sqlx::Error> {
    let interval_secs = stale_after.as_secs() as f64;
    let result = sqlx::query(
        r#"UPDATE scheduled_jobs
           SET status = 'pending', claimed_at = NULL
           WHERE job_name = $1
             AND status = 'claimed'
             AND claimed_at < now() - make_interval(secs => $2)"#,
    )
    .bind(job_name)
    .bind(interval_secs)
    .execute(pool)
    .await?;
    Ok(result.rows_affected())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::EquityBar;

    fn sample_bars() -> Vec<EquityBar> {
        vec![
            EquityBar {
                ticker: "AAPL".to_string(),
                timestamp: 1700000000000,
                open_price: Some(150.0),
                high_price: Some(155.0),
                low_price: Some(149.0),
                close_price: Some(153.0),
                volume: Some(1000000),
                volume_weighted_average_price: Some(152.0),
                transactions: Some(50000),
            },
            EquityBar {
                ticker: "MSFT".to_string(),
                timestamp: 1700000000000,
                open_price: Some(350.0),
                high_price: Some(355.0),
                low_price: Some(349.0),
                close_price: Some(353.0),
                volume: Some(500000),
                volume_weighted_average_price: Some(352.0),
                transactions: Some(25000),
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
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let pool = PgPool::connect_lazy("postgresql://localhost:5432/fund_test_nonexistent")
                .expect("lazy pool creation should not fail");
            let result = insert_equity_bars(&pool, &[]).await;
            assert!(result.is_ok());
            assert_eq!(result.unwrap(), 0);
        });
    }

    #[test]
    fn test_bars_with_null_ohlcv_are_filtered() {
        let bars_with_nulls = vec![EquityBar {
            ticker: "AAPL".to_string(),
            timestamp: 1700000000000,
            open_price: None,
            high_price: None,
            low_price: None,
            close_price: None,
            volume: None,
            volume_weighted_average_price: None,
            transactions: None,
        }];
        assert!(!is_valid_equity_bar(&bars_with_nulls[0]));
    }

    #[test]
    fn test_insert_empty_quotes_returns_zero() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let pool = PgPool::connect_lazy("postgresql://localhost:5432/fund_test_nonexistent")
                .expect("lazy pool creation should not fail");
            let result = insert_equity_quotes(&pool, &[]).await;
            assert!(result.is_ok());
            assert_eq!(result.unwrap(), 0);
        });
    }

    #[test]
    fn test_get_active_tickers_compiles() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let pool = PgPool::connect_lazy("postgresql://localhost:5432/fund_test_nonexistent")
                .expect("lazy pool creation should not fail");
            let result = get_active_tickers(&pool).await;
            assert!(result.is_err());
        });
    }

    #[test]
    fn test_requeue_stale_claimed_jobs_compiles() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
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
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let pool = PgPool::connect_lazy("postgresql://localhost:5432/fund_test_nonexistent")
                .expect("lazy pool creation should not fail");
            let result = emit_equity_bars_synced(&pool).await;
            assert!(result.is_err());
        });
    }

    #[test]
    fn test_set_data_bucket_guc_compiles() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let pool = PgPool::connect_lazy("postgresql://localhost:5432/fund_test_nonexistent")
                .expect("lazy pool creation should not fail");
            let result = set_data_bucket_guc(&pool, "test-bucket").await;
            assert!(result.is_err());
        });
    }
}
