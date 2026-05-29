use chrono::{DateTime, Duration, Utc};
use polars::prelude::*;
use sqlx::postgres::PgRow;
use sqlx::{PgPool, Row};
use tracing::info;
use uuid::Uuid;

pub async fn query_equity_bars(pool: &PgPool) -> Result<DataFrame, sqlx::Error> {
    let end_date = Utc::now();
    let start_date = end_date - Duration::days(70);

    let rows: Vec<PgRow> = sqlx::query(
        r#"SELECT ticker,
                  EXTRACT(EPOCH FROM timestamp)::bigint * 1000 AS timestamp_ms,
                  open_price, high_price, low_price, close_price,
                  volume, volume_weighted_average_price
           FROM equity_bars
           WHERE timestamp >= $1 AND timestamp <= $2
           ORDER BY ticker, timestamp"#,
    )
    .bind(start_date)
    .bind(end_date)
    .fetch_all(pool)
    .await?;

    let mut tickers: Vec<String> = Vec::with_capacity(rows.len());
    let mut timestamps: Vec<i64> = Vec::with_capacity(rows.len());
    let mut opens: Vec<f64> = Vec::with_capacity(rows.len());
    let mut highs: Vec<f64> = Vec::with_capacity(rows.len());
    let mut lows: Vec<f64> = Vec::with_capacity(rows.len());
    let mut closes: Vec<f64> = Vec::with_capacity(rows.len());
    let mut volumes: Vec<i64> = Vec::with_capacity(rows.len());
    let mut vwaps: Vec<Option<f64>> = Vec::with_capacity(rows.len());

    for row in &rows {
        tickers.push(row.get("ticker"));
        timestamps.push(row.get("timestamp_ms"));
        opens.push(row.get("open_price"));
        highs.push(row.get("high_price"));
        lows.push(row.get("low_price"));
        closes.push(row.get("close_price"));
        volumes.push(row.get("volume"));
        vwaps.push(row.get("volume_weighted_average_price"));
    }

    let dataframe = DataFrame::new(vec![
        Column::new("ticker".into(), tickers),
        Column::new("timestamp".into(), timestamps),
        Column::new("open_price".into(), opens),
        Column::new("high_price".into(), highs),
        Column::new("low_price".into(), lows),
        Column::new("close_price".into(), closes),
        Column::new("volume".into(), volumes),
        Column::new("volume_weighted_average_price".into(), vwaps),
    ])
    .map_err(|e| sqlx::Error::Protocol(e.to_string()))?;

    info!(rows = dataframe.height(), "Equity bars queried from PostgreSQL");
    Ok(dataframe)
}

pub async fn query_equity_details(pool: &PgPool) -> Result<DataFrame, sqlx::Error> {
    let rows: Vec<PgRow> = sqlx::query(
        r#"SELECT ticker, sector, industry FROM equity_details"#,
    )
    .fetch_all(pool)
    .await?;

    let mut tickers: Vec<String> = Vec::with_capacity(rows.len());
    let mut sectors: Vec<String> = Vec::with_capacity(rows.len());
    let mut industries: Vec<String> = Vec::with_capacity(rows.len());

    for row in &rows {
        tickers.push(row.get("ticker"));
        sectors.push(row.get::<Option<String>, _>("sector").unwrap_or_default());
        industries.push(row.get::<Option<String>, _>("industry").unwrap_or_default());
    }

    let dataframe = DataFrame::new(vec![
        Column::new("ticker".into(), tickers),
        Column::new("sector".into(), sectors),
        Column::new("industry".into(), industries),
    ])
    .map_err(|e| sqlx::Error::Protocol(e.to_string()))?;

    info!(rows = dataframe.height(), "Equity details queried from PostgreSQL");
    Ok(dataframe)
}

pub async fn insert_predictions(
    pool: &PgPool,
    predictions: &[serde_json::Value],
    correlation_id: Uuid,
    model_run_id: &str,
) -> Result<u64, sqlx::Error> {
    if predictions.is_empty() {
        return Ok(0);
    }

    let mut rows_affected: u64 = 0;

    for chunk in predictions.chunks(1000) {
        let mut query_builder = sqlx::QueryBuilder::new(
            "INSERT INTO equity_predictions (correlation_id, model_run_id, ticker, timestamp, quantile_10, quantile_50, quantile_90) ",
        );

        query_builder.push_values(chunk, |mut builder, prediction| {
            let ticker = prediction["ticker"].as_str().unwrap_or("UNKNOWN");
            let timestamp_ms = prediction["timestamp"].as_i64().unwrap_or(0);
            let timestamp = DateTime::<Utc>::from_timestamp_millis(timestamp_ms)
                .unwrap_or_else(Utc::now);
            let quantile_10 = prediction["quantile_10"].as_f64().unwrap_or(0.0);
            let quantile_50 = prediction["quantile_50"].as_f64().unwrap_or(0.0);
            let quantile_90 = prediction["quantile_90"].as_f64().unwrap_or(0.0);

            builder
                .push_bind(correlation_id)
                .push_bind(model_run_id.to_string())
                .push_bind(ticker.to_string())
                .push_bind(timestamp)
                .push_bind(quantile_10)
                .push_bind(quantile_50)
                .push_bind(quantile_90);
        });

        query_builder.push(
            " ON CONFLICT (ticker, timestamp) DO UPDATE SET \
             correlation_id = EXCLUDED.correlation_id, \
             model_run_id = EXCLUDED.model_run_id, \
             quantile_10 = EXCLUDED.quantile_10, \
             quantile_50 = EXCLUDED.quantile_50, \
             quantile_90 = EXCLUDED.quantile_90",
        );

        let result = query_builder.build().execute(pool).await?;
        rows_affected += result.rows_affected();
    }

    info!(rows = rows_affected, "Predictions inserted into PostgreSQL");
    Ok(rows_affected)
}

pub async fn emit_event(
    pool: &PgPool,
    event_type: &str,
    payload: &serde_json::Value,
) -> Result<(), sqlx::Error> {
    let payload_string = payload.to_string();
    sqlx::query("SELECT emit_event($1, $2::jsonb)")
        .bind(event_type)
        .bind(payload_string)
        .execute(pool)
        .await?;
    info!(event_type = event_type, "Emitted event");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_insert_empty_predictions_returns_zero() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let pool = PgPool::connect_lazy("postgresql://localhost:5432/fund_test_nonexistent")
                .expect("lazy pool creation should not fail");
            let result =
                insert_predictions(&pool, &[], Uuid::new_v4(), "test-model").await;
            assert!(result.is_ok());
            assert_eq!(result.unwrap(), 0);
        });
    }

    #[test]
    fn test_query_equity_bars_compiles() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let pool = PgPool::connect_lazy("postgresql://localhost:5432/fund_test_nonexistent")
                .expect("lazy pool creation should not fail");
            let result = query_equity_bars(&pool).await;
            assert!(result.is_err());
        });
    }

    #[test]
    fn test_query_equity_details_compiles() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let pool = PgPool::connect_lazy("postgresql://localhost:5432/fund_test_nonexistent")
                .expect("lazy pool creation should not fail");
            let result = query_equity_details(&pool).await;
            assert!(result.is_err());
        });
    }

    #[test]
    fn test_emit_event_compiles() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let pool = PgPool::connect_lazy("postgresql://localhost:5432/fund_test_nonexistent")
                .expect("lazy pool creation should not fail");
            let result =
                emit_event(&pool, "test_event", &serde_json::json!({"key": "value"})).await;
            assert!(result.is_err());
        });
    }
}
