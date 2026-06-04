use internal::market::{EquityBar, EquityDetails, EquityQuote};
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

pub async fn populate_equity_details_if_empty(
    pool: &PgPool,
    rows: &[EquityDetails],
) -> Result<u64, sqlx::Error> {
    let mut transaction = pool.begin().await?;

    let count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM equity_details")
        .fetch_one(&mut *transaction)
        .await?;

    if count.0 > 0 {
        info!(
            "equity_details already populated with {} rows, skipping migration",
            count.0
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

pub async fn set_bucket_guc(pool: &PgPool, bucket_name: &str) -> Result<(), sqlx::Error> {
    let alter_statement: String = sqlx::query_scalar(
        r#"SELECT format('ALTER DATABASE %I SET "app.bucket_name" = %L', current_database(), $1)"#,
    )
    .bind(bucket_name)
    .fetch_one(pool)
    .await?;
    sqlx::query(&alter_statement).execute(pool).await?;
    info!("Set app.bucket_name database GUC to {}", bucket_name);
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
    fn test_set_bucket_guc_compiles() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            let pool = PgPool::connect_lazy("postgresql://localhost:5432/fund_test_nonexistent")
                .expect("lazy pool creation should not fail");
            let result = set_bucket_guc(&pool, "test-bucket").await;
            assert!(result.is_err());
        });
    }
}
