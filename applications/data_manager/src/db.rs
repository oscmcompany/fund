use crate::data::EquityBar;
use chrono::DateTime;
use chrono::Utc;
use sqlx::postgres::PgRow;
use sqlx::{PgPool, Row};
use tracing::{debug, info, warn};

pub async fn insert_equity_prices(pool: &PgPool, bars: &[EquityBar]) -> Result<u64, sqlx::Error> {
    if bars.is_empty() {
        return Ok(0);
    }

    // Filter to bars that have all required non-null fields and a valid timestamp.
    let valid_bars: Vec<&EquityBar> = bars
        .iter()
        .filter(|bar| {
            bar.open_price.is_some()
                && bar.high_price.is_some()
                && bar.low_price.is_some()
                && bar.close_price.is_some()
                && bar.volume.is_some()
                && DateTime::<Utc>::from_timestamp_millis(bar.timestamp).is_some()
        })
        .collect();

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
        let mut query_builder = sqlx::QueryBuilder::new(
            "INSERT INTO equity_prices (time, symbol, open, high, low, close, volume) ",
        );

        query_builder.push_values(chunk, |mut builder, bar| {
            let time = DateTime::<Utc>::from_timestamp_millis(bar.timestamp).unwrap();
            builder
                .push_bind(time)
                .push_bind(&bar.ticker)
                .push_bind(bar.open_price.unwrap())
                .push_bind(bar.high_price.unwrap())
                .push_bind(bar.low_price.unwrap())
                .push_bind(bar.close_price.unwrap())
                .push_bind(bar.volume.unwrap());
        });

        query_builder.push(
            " ON CONFLICT (symbol, time) DO UPDATE SET \
             open = EXCLUDED.open, \
             high = EXCLUDED.high, \
             low = EXCLUDED.low, \
             close = EXCLUDED.close, \
             volume = EXCLUDED.volume",
        );

        let result = query_builder.build().execute(pool).await?;
        rows_affected += result.rows_affected();
    }

    info!("Inserted {} equity prices into PostgreSQL", rows_affected);
    Ok(rows_affected)
}

pub async fn query_recent_equity_prices(
    pool: &PgPool,
    tickers: Option<&[String]>,
    days_back: i32,
) -> Result<Vec<EquityBar>, sqlx::Error> {
    debug!(
        "Querying recent equity prices, days_back: {}, tickers: {:?}",
        days_back, tickers
    );

    let rows: Vec<PgRow> = match tickers {
        Some(ticker_list) if !ticker_list.is_empty() => {
            sqlx::query(
                r#"SELECT symbol, time, open, high, low, close, volume
                   FROM equity_prices
                   WHERE time >= now() - make_interval(days => $1)
                     AND symbol = ANY($2)
                   ORDER BY symbol, time"#,
            )
            .bind(days_back)
            .bind(ticker_list)
            .fetch_all(pool)
            .await?
        }
        _ => {
            sqlx::query(
                r#"SELECT symbol, time, open, high, low, close, volume
                   FROM equity_prices
                   WHERE time >= now() - make_interval(days => $1)
                   ORDER BY symbol, time"#,
            )
            .bind(days_back)
            .fetch_all(pool)
            .await?
        }
    };

    let bars: Vec<EquityBar> = rows.iter().map(equity_bar_from_row).collect();
    info!("Queried {} equity prices from PostgreSQL", bars.len());
    Ok(bars)
}

fn equity_bar_from_row(row: &PgRow) -> EquityBar {
    let time: DateTime<Utc> = row.get("time");
    EquityBar {
        ticker: row.get("symbol"),
        timestamp: time.timestamp_millis(),
        open_price: Some(row.get::<f64, _>("open")),
        high_price: Some(row.get::<f64, _>("high")),
        low_price: Some(row.get::<f64, _>("low")),
        close_price: Some(row.get::<f64, _>("close")),
        volume: Some(row.get("volume")),
        volume_weighted_average_price: None,
        transactions: None,
    }
}

pub async fn claim_pending_job(pool: &PgPool, job_name: &str) -> Result<Option<i64>, sqlx::Error> {
    let row: Option<(i64,)> = sqlx::query_as(
        r#"UPDATE scheduled_jobs
           SET claimed_at = now(), status = 'claimed'
           WHERE id = (
               SELECT id FROM scheduled_jobs
               WHERE job_name = $1 AND status = 'pending'
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
            // Without a pool we can only test the empty-slice shortcut
            // (which returns before touching the database)
            let pool = PgPool::connect_lazy("postgresql://localhost:5432/fund_test_nonexistent")
                .expect("lazy pool creation should not fail");
            let result = insert_equity_prices(&pool, &[]).await;
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
        let valid_bars: Vec<&EquityBar> = bars_with_nulls
            .iter()
            .filter(|bar| {
                bar.open_price.is_some()
                    && bar.high_price.is_some()
                    && bar.low_price.is_some()
                    && bar.close_price.is_some()
                    && bar.volume.is_some()
                    && DateTime::<Utc>::from_timestamp_millis(bar.timestamp).is_some()
            })
            .collect();
        assert_eq!(valid_bars.len(), 0);
    }
}
