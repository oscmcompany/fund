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

    info!(
        rows = dataframe.height(),
        "Equity bars queried from PostgreSQL"
    );
    Ok(dataframe)
}

pub async fn query_equity_details(pool: &PgPool) -> Result<DataFrame, sqlx::Error> {
    let rows: Vec<PgRow> = sqlx::query(r#"SELECT ticker, sector, industry FROM equity_details"#)
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

    info!(
        rows = dataframe.height(),
        "Equity details queried from PostgreSQL"
    );
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
            let timestamp =
                DateTime::<Utc>::from_timestamp_millis(timestamp_ms).unwrap_or_else(Utc::now);
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

/// Return the last processed event id for a consumer, or 0 if not yet recorded.
pub async fn get_consumer_offset(pool: &PgPool, consumer_name: &str) -> Result<i64, sqlx::Error> {
    let row: Option<(i64,)> =
        sqlx::query_as("SELECT last_event_id FROM event_consumer_offsets WHERE consumer_name = $1")
            .bind(consumer_name)
            .fetch_optional(pool)
            .await?;
    Ok(row.map(|(id,)| id).unwrap_or(0))
}

/// Upsert the last processed event id for a consumer. `GREATEST` guards against
/// moving the offset backwards under concurrent updates.
pub async fn update_consumer_offset(
    pool: &PgPool,
    consumer_name: &str,
    last_event_id: i64,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO event_consumer_offsets (consumer_name, last_event_id, updated_at) \
         VALUES ($1, $2, now()) \
         ON CONFLICT (consumer_name) DO UPDATE SET \
           last_event_id = GREATEST(event_consumer_offsets.last_event_id, EXCLUDED.last_event_id), \
           updated_at = EXCLUDED.updated_at",
    )
    .bind(consumer_name)
    .bind(last_event_id)
    .execute(pool)
    .await?;
    Ok(())
}

/// Id of the most recent event of `event_type` with id greater than `after_id`,
/// used to catch up on a request that arrived while the consumer was down.
pub async fn latest_event_after(
    pool: &PgPool,
    event_type: &str,
    after_id: i64,
) -> Result<Option<i64>, sqlx::Error> {
    let row: Option<(i64,)> = sqlx::query_as(
        "SELECT id FROM events WHERE event_type = $1 AND id > $2 ORDER BY id DESC LIMIT 1",
    )
    .bind(event_type)
    .bind(after_id)
    .fetch_optional(pool)
    .await?;
    Ok(row.map(|(id,)| id))
}

/// Lineage row for the `model_runs` table, extracted from a trained model's
/// `run_metadata.json`.
#[derive(Debug, Clone)]
pub struct ModelRunRecord {
    pub run_id: String,
    pub artifact_key: String,
    pub crps: Option<f64>,
    pub directional_accuracy: Option<f64>,
    pub quantile_coverage: Option<f64>,
    pub lookback_days: Option<i32>,
    pub start_date: Option<String>,
    pub end_date: Option<String>,
    pub training_data_key: Option<String>,
    pub stage_counts: serde_json::Value,
}

impl ModelRunRecord {
    /// Build a record from the trainer's `run_metadata.json`. Missing fields stay
    /// `None`/null so the row can still be written.
    pub fn from_metadata(run_id: &str, artifact_key: &str, metadata: &serde_json::Value) -> Self {
        let metrics = &metadata["metrics"];
        let stage_counts = metadata.get("stage_counts").cloned().unwrap_or_else(|| {
            serde_json::json!({
                "train_samples": metadata.get("train_samples"),
                "validation_samples": metadata.get("validation_samples"),
            })
        });
        Self {
            run_id: run_id.to_string(),
            artifact_key: artifact_key.to_string(),
            crps: metrics.get("crps").and_then(|v| v.as_f64()),
            directional_accuracy: metrics.get("directional_accuracy").and_then(|v| v.as_f64()),
            quantile_coverage: metrics.get("quantile_coverage").and_then(|v| v.as_f64()),
            lookback_days: metadata
                .get("lookback_days")
                .and_then(|v| v.as_i64())
                .map(|v| v as i32),
            start_date: metadata
                .get("start_date")
                .and_then(|v| v.as_str())
                .map(String::from),
            end_date: metadata
                .get("end_date")
                .and_then(|v| v.as_str())
                .map(String::from),
            training_data_key: metadata
                .get("training_data_key")
                .and_then(|v| v.as_str())
                .map(String::from),
            stage_counts,
        }
    }
}

/// Upsert a `model_runs` lineage row so `equity_predictions.model_run_id` joins
/// back to training metadata. Mirrors the prior Python `ensemble_manager` sync.
pub async fn upsert_model_run(pool: &PgPool, record: &ModelRunRecord) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO model_runs ( \
             run_id, artifact_key, status, completed_at, \
             continuous_ranked_probability_score, directional_accuracy, quantile_coverage, \
             lookback_days, start_date, end_date, training_data_key, stage_counts \
         ) VALUES ($1, $2, 'completed', now(), $3, $4, $5, $6, $7::date, $8::date, $9, $10::jsonb) \
         ON CONFLICT (run_id) DO UPDATE SET \
             artifact_key = EXCLUDED.artifact_key, \
             status = EXCLUDED.status, \
             completed_at = EXCLUDED.completed_at, \
             continuous_ranked_probability_score = EXCLUDED.continuous_ranked_probability_score, \
             directional_accuracy = EXCLUDED.directional_accuracy, \
             quantile_coverage = EXCLUDED.quantile_coverage, \
             lookback_days = EXCLUDED.lookback_days, \
             start_date = EXCLUDED.start_date, \
             end_date = EXCLUDED.end_date, \
             training_data_key = EXCLUDED.training_data_key, \
             stage_counts = EXCLUDED.stage_counts",
    )
    .bind(&record.run_id)
    .bind(&record.artifact_key)
    .bind(record.crps)
    .bind(record.directional_accuracy)
    .bind(record.quantile_coverage)
    .bind(record.lookback_days)
    .bind(&record.start_date)
    .bind(&record.end_date)
    .bind(&record.training_data_key)
    .bind(record.stage_counts.to_string())
    .execute(pool)
    .await?;
    info!(run_id = record.run_id, "Upserted model_runs lineage row");
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
            let result = insert_predictions(&pool, &[], Uuid::new_v4(), "test-model").await;
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
    fn test_model_run_record_from_metadata() {
        let metadata = serde_json::json!({
            "artifact_timestamp": "2026-06-09-16-21-25-195",
            "lookback_days": 1200,
            "start_date": "2023-02-25",
            "end_date": "2026-06-09",
            "training_data_key": "models/tide/2026-06-09-16-21-25-195/filtered_data.parquet",
            "train_samples": 2529261,
            "validation_samples": 531330,
            "metrics": {
                "crps": 0.0059,
                "directional_accuracy": 0.617,
                "quantile_coverage": 0.719
            }
        });
        let record = ModelRunRecord::from_metadata(
            "2026-06-09-16-21-25-195",
            "models/tide/2026-06-09-16-21-25-195/output/model.tar.gz",
            &metadata,
        );
        assert_eq!(record.run_id, "2026-06-09-16-21-25-195");
        assert_eq!(record.crps, Some(0.0059));
        assert_eq!(record.directional_accuracy, Some(0.617));
        assert_eq!(record.quantile_coverage, Some(0.719));
        assert_eq!(record.lookback_days, Some(1200));
        assert_eq!(record.start_date.as_deref(), Some("2023-02-25"));
        assert_eq!(record.stage_counts["train_samples"], 2529261);
    }

    #[test]
    fn test_model_run_record_missing_fields_default_to_none() {
        let metadata = serde_json::json!({});
        let record = ModelRunRecord::from_metadata("run", "key", &metadata);
        assert_eq!(record.crps, None);
        assert_eq!(record.lookback_days, None);
        assert_eq!(record.start_date, None);
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
