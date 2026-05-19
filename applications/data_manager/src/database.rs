use crate::data::EquityBar;
use sqlx::postgres::PgRow;
use sqlx::{PgPool, Row};
use tracing::{debug, info, warn};

pub async fn insert_equity_bars(pool: &PgPool, bars: &[EquityBar]) -> Result<u64, sqlx::Error> {
    if bars.is_empty() {
        return Ok(0);
    }

    let mut rows_affected: u64 = 0;

    for chunk in bars.chunks(1000) {
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
                builder
                    .push_bind(&bar.ticker)
                    .push_bind(bar.timestamp)
                    .push_bind(bar.open_price)
                    .push_bind(bar.high_price)
                    .push_bind(bar.low_price)
                    .push_bind(bar.close_price)
                    .push_bind(bar.volume)
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
                r#"SELECT ticker, timestamp, open_price, high_price, low_price,
                          close_price, volume, volume_weighted_average_price, transactions
                   FROM equity_bars
                   WHERE timestamp >= (EXTRACT(EPOCH FROM now() - make_interval(days => $1)) * 1000)::BIGINT
                     AND ticker = ANY($2)
                   ORDER BY ticker, timestamp"#,
            )
            .bind(days_back)
            .bind(ticker_list)
            .fetch_all(pool)
            .await?
        }
        _ => {
            sqlx::query(
                r#"SELECT ticker, timestamp, open_price, high_price, low_price,
                          close_price, volume, volume_weighted_average_price, transactions
                   FROM equity_bars
                   WHERE timestamp >= (EXTRACT(EPOCH FROM now() - make_interval(days => $1)) * 1000)::BIGINT
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
    let transactions: Option<i64> = row.get("transactions");
    EquityBar {
        ticker: row.get("ticker"),
        timestamp: row.get("timestamp"),
        open_price: row.get("open_price"),
        high_price: row.get("high_price"),
        low_price: row.get("low_price"),
        close_price: row.get("close_price"),
        volume: row.get("volume"),
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
            let result = insert_equity_bars(&pool, &[]).await;
            assert!(result.is_ok());
            assert_eq!(result.unwrap(), 0);
        });
    }
}
