use chrono::{DateTime, Duration, NaiveDate, Utc};
use polars::prelude::*;
use sqlx::PgPool;
use tracing::info;
use uuid::Uuid;

use crate::domain::predictions::EquityPrediction;

pub async fn query_equity_bars(pool: &PgPool) -> Result<DataFrame, sqlx::Error> {
    let end_date = Utc::now();
    let start_date = end_date - Duration::days(70);

    let rows = sqlx::query!(
        r#"SELECT ticker,
                  EXTRACT(EPOCH FROM timestamp)::bigint * 1000 AS "timestamp_ms!",
                  open_price, high_price, low_price, close_price,
                  volume, volume_weighted_average_price
           FROM equity_bars
           WHERE timestamp >= $1 AND timestamp <= $2
           ORDER BY ticker, timestamp"#,
        start_date,
        end_date
    )
    .fetch_all(pool)
    .await?;

    let mut tickers: Vec<String> = Vec::with_capacity(rows.len());
    let mut timestamps: Vec<i64> = Vec::with_capacity(rows.len());
    let mut opens: Vec<f64> = Vec::with_capacity(rows.len());
    let mut highs: Vec<f64> = Vec::with_capacity(rows.len());
    let mut lows: Vec<f64> = Vec::with_capacity(rows.len());
    let mut closes: Vec<f64> = Vec::with_capacity(rows.len());
    let mut volumes: Vec<i64> = Vec::with_capacity(rows.len());
    let mut volume_weighted_average_prices: Vec<Option<f64>> = Vec::with_capacity(rows.len());

    for row in rows {
        tickers.push(row.ticker);
        timestamps.push(row.timestamp_ms);
        opens.push(row.open_price);
        highs.push(row.high_price);
        lows.push(row.low_price);
        closes.push(row.close_price);
        volumes.push(row.volume);
        volume_weighted_average_prices.push(row.volume_weighted_average_price);
    }

    let dataframe = DataFrame::new(vec![
        Column::new("ticker".into(), tickers),
        Column::new("timestamp".into(), timestamps),
        Column::new("open_price".into(), opens),
        Column::new("high_price".into(), highs),
        Column::new("low_price".into(), lows),
        Column::new("close_price".into(), closes),
        Column::new("volume".into(), volumes),
        Column::new(
            "volume_weighted_average_price".into(),
            volume_weighted_average_prices,
        ),
    ])
    .map_err(|e| sqlx::Error::Protocol(e.to_string()))?;

    info!(
        rows = dataframe.height(),
        "Equity bars queried from PostgreSQL"
    );
    Ok(dataframe)
}

pub async fn query_equity_details(pool: &PgPool) -> Result<DataFrame, sqlx::Error> {
    let rows = sqlx::query!(r#"SELECT ticker, sector, industry FROM equity_details"#)
        .fetch_all(pool)
        .await?;

    let mut tickers: Vec<String> = Vec::with_capacity(rows.len());
    let mut sectors: Vec<String> = Vec::with_capacity(rows.len());
    let mut industries: Vec<String> = Vec::with_capacity(rows.len());

    for row in rows {
        tickers.push(row.ticker);
        sectors.push(row.sector);
        industries.push(row.industry);
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

/// Boundary morphism: converts an untrusted pipeline prediction JSON object
/// into a validated [`EquityPrediction`].
///
/// Predictions come from our own pipeline, so a missing or mistyped field is a
/// real data bug upstream; this fails loudly with the offending field and
/// ticker instead of persisting placeholder values.
fn prediction_from_json(
    prediction: &serde_json::Value,
    correlation_id: Uuid,
    model_run_id: &str,
) -> Result<EquityPrediction, sqlx::Error> {
    let ticker = prediction
        .get("ticker")
        .and_then(|value| value.as_str())
        .ok_or_else(|| sqlx::Error::Decode("Prediction is missing a string ticker field".into()))?;

    let timestamp_milliseconds = prediction
        .get("timestamp")
        .and_then(|value| value.as_i64())
        .ok_or_else(|| {
            sqlx::Error::Decode(
                format!("Prediction for ticker {ticker} is missing an integer timestamp field")
                    .into(),
            )
        })?;
    let timestamp = DateTime::<Utc>::from_timestamp_millis(timestamp_milliseconds)
        .filter(|_| timestamp_milliseconds > 0)
        .ok_or_else(|| {
            sqlx::Error::Decode(
                format!(
                    "Prediction for ticker {ticker} has an invalid timestamp: {timestamp_milliseconds}"
                )
                .into(),
            )
        })?;

    let quantile = |field: &str| -> Result<f64, sqlx::Error> {
        prediction
            .get(field)
            .and_then(|value| value.as_f64())
            .ok_or_else(|| {
                sqlx::Error::Decode(
                    format!("Prediction for ticker {ticker} is missing a numeric {field} field")
                        .into(),
                )
            })
    };

    Ok(EquityPrediction::new(
        correlation_id,
        model_run_id.to_string(),
        ticker.to_string(),
        timestamp,
        quantile("quantile_10")?,
        quantile("quantile_50")?,
        quantile("quantile_90")?,
        Utc::now(),
    ))
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

    let validated: Vec<EquityPrediction> = predictions
        .iter()
        .map(|prediction| prediction_from_json(prediction, correlation_id, model_run_id))
        .collect::<Result<_, _>>()?;

    let mut rows_affected: u64 = 0;

    for chunk in validated.chunks(1000) {
        let mut query_builder = sqlx::QueryBuilder::new(
            "INSERT INTO equity_predictions (correlation_id, model_run_id, ticker, timestamp, quantile_10, quantile_50, quantile_90) ",
        );

        query_builder.push_values(chunk, |mut builder, prediction| {
            builder
                .push_bind(prediction.correlation_id())
                .push_bind(prediction.model_run_id().to_string())
                .push_bind(prediction.ticker().to_string())
                .push_bind(prediction.timestamp())
                .push_bind(prediction.quantile_10())
                .push_bind(prediction.quantile_50())
                .push_bind(prediction.quantile_90());
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

/// Lineage row for the `model_runs` table, extracted from a trained model's
/// `run_metadata.json`.
#[derive(Debug, Clone)]
pub struct ModelRunRecord {
    run_id: String,
    artifact_key: String,
    continuous_ranked_probability_score: Option<f64>,
    directional_accuracy: Option<f64>,
    quantile_coverage: Option<f64>,
    lookback_days: Option<i32>,
    start_date: Option<NaiveDate>,
    end_date: Option<NaiveDate>,
    training_data_key: Option<String>,
    stage_counts: serde_json::Value,
    drift_status: Option<String>,
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
            continuous_ranked_probability_score: metrics.get("crps").and_then(|v| v.as_f64()),
            directional_accuracy: metrics.get("directional_accuracy").and_then(|v| v.as_f64()),
            quantile_coverage: metrics.get("quantile_coverage").and_then(|v| v.as_f64()),
            lookback_days: metadata
                .get("lookback_days")
                .and_then(|v| v.as_i64())
                .map(|v| v as i32),
            start_date: metadata
                .get("start_date")
                .and_then(|v| v.as_str())
                .and_then(|value| NaiveDate::parse_from_str(value, "%Y-%m-%d").ok()),
            end_date: metadata
                .get("end_date")
                .and_then(|v| v.as_str())
                .and_then(|value| NaiveDate::parse_from_str(value, "%Y-%m-%d").ok()),
            training_data_key: metadata
                .get("training_data_key")
                .and_then(|v| v.as_str())
                .map(String::from),
            stage_counts,
            drift_status: metadata
                .get("drift")
                .and_then(|drift| drift.get("status"))
                .and_then(|v| v.as_str())
                .map(String::from),
        }
    }

    pub fn run_id(&self) -> &str {
        &self.run_id
    }

    pub fn artifact_key(&self) -> &str {
        &self.artifact_key
    }

    pub fn continuous_ranked_probability_score(&self) -> Option<f64> {
        self.continuous_ranked_probability_score
    }

    pub fn directional_accuracy(&self) -> Option<f64> {
        self.directional_accuracy
    }

    pub fn quantile_coverage(&self) -> Option<f64> {
        self.quantile_coverage
    }

    pub fn lookback_days(&self) -> Option<i32> {
        self.lookback_days
    }

    pub fn start_date(&self) -> Option<NaiveDate> {
        self.start_date
    }

    pub fn end_date(&self) -> Option<NaiveDate> {
        self.end_date
    }

    pub fn training_data_key(&self) -> Option<&str> {
        self.training_data_key.as_deref()
    }

    pub fn stage_counts(&self) -> &serde_json::Value {
        &self.stage_counts
    }

    pub fn drift_status(&self) -> Option<&str> {
        self.drift_status.as_deref()
    }
}

/// Upsert a `model_runs` lineage row so `equity_predictions.model_run_id` joins
/// back to training metadata. Mirrors the prior Python `ensemble_manager` sync.
pub async fn upsert_model_run(pool: &PgPool, record: &ModelRunRecord) -> Result<(), sqlx::Error> {
    sqlx::query!(
        "INSERT INTO model_runs ( \
             run_id, artifact_key, status, completed_at, \
             continuous_ranked_probability_score, directional_accuracy, quantile_coverage, \
             lookback_days, start_date, end_date, training_data_key, stage_counts, \
             drift_status \
         ) VALUES ($1, $2, 'completed', now(), $3, $4, $5, $6, $7, $8, $9, $10, $11) \
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
             stage_counts = EXCLUDED.stage_counts, \
             drift_status = EXCLUDED.drift_status",
        record.run_id(),
        record.artifact_key(),
        record.continuous_ranked_probability_score(),
        record.directional_accuracy(),
        record.quantile_coverage(),
        record.lookback_days(),
        record.start_date(),
        record.end_date(),
        record.training_data_key(),
        record.stage_counts(),
        record.drift_status(),
    )
    .execute(pool)
    .await?;
    info!(run_id = record.run_id(), "Upserted model_runs lineage row");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lazy_pool() -> PgPool {
        PgPool::connect_lazy("postgresql://localhost:5432/fund_test_nonexistent")
            .expect("lazy pool creation should not fail")
    }

    #[test]
    fn test_insert_empty_predictions_returns_zero() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let result = insert_predictions(&lazy_pool(), &[], Uuid::new_v4(), "test-model").await;
            assert!(result.is_ok());
            assert_eq!(result.unwrap(), 0);
        });
    }

    #[test]
    fn test_insert_predictions_rejects_missing_ticker() {
        // Validation happens before any database round trip, so a lazy pool to
        // a nonexistent server still surfaces the decode error.
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let predictions = vec![serde_json::json!({
                "timestamp": 1_735_689_600_000_i64,
                "quantile_10": -0.01,
                "quantile_50": 0.0,
                "quantile_90": 0.02,
            })];
            let error = insert_predictions(&lazy_pool(), &predictions, Uuid::new_v4(), "run-x")
                .await
                .unwrap_err();
            assert!(matches!(error, sqlx::Error::Decode(_)));
            assert!(error.to_string().contains("ticker"));
        });
    }

    #[test]
    fn test_insert_predictions_rejects_mistyped_ticker() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let predictions = vec![serde_json::json!({
                "ticker": 42,
                "timestamp": 1_735_689_600_000_i64,
                "quantile_10": -0.01,
                "quantile_50": 0.0,
                "quantile_90": 0.02,
            })];
            let error = insert_predictions(&lazy_pool(), &predictions, Uuid::new_v4(), "run-x")
                .await
                .unwrap_err();
            assert!(matches!(error, sqlx::Error::Decode(_)));
            assert!(error.to_string().contains("ticker"));
        });
    }

    #[test]
    fn test_insert_predictions_rejects_missing_timestamp() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let predictions = vec![serde_json::json!({
                "ticker": "AAPL",
                "quantile_10": -0.01,
                "quantile_50": 0.0,
                "quantile_90": 0.02,
            })];
            let error = insert_predictions(&lazy_pool(), &predictions, Uuid::new_v4(), "run-x")
                .await
                .unwrap_err();
            assert!(matches!(error, sqlx::Error::Decode(_)));
            let message = error.to_string();
            assert!(message.contains("timestamp"));
            assert!(message.contains("AAPL"));
        });
    }

    #[test]
    fn test_insert_predictions_rejects_non_positive_timestamp() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let predictions = vec![serde_json::json!({
                "ticker": "AAPL",
                "timestamp": 0_i64,
                "quantile_10": -0.01,
                "quantile_50": 0.0,
                "quantile_90": 0.02,
            })];
            let error = insert_predictions(&lazy_pool(), &predictions, Uuid::new_v4(), "run-x")
                .await
                .unwrap_err();
            assert!(matches!(error, sqlx::Error::Decode(_)));
            let message = error.to_string();
            assert!(message.contains("invalid timestamp"));
            assert!(message.contains("AAPL"));
        });
    }

    #[test]
    fn test_insert_predictions_rejects_mistyped_quantile() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let predictions = vec![serde_json::json!({
                "ticker": "AAPL",
                "timestamp": 1_735_689_600_000_i64,
                "quantile_10": -0.01,
                "quantile_50": "not-a-number",
                "quantile_90": 0.02,
            })];
            let error = insert_predictions(&lazy_pool(), &predictions, Uuid::new_v4(), "run-x")
                .await
                .unwrap_err();
            assert!(matches!(error, sqlx::Error::Decode(_)));
            let message = error.to_string();
            assert!(message.contains("quantile_50"));
            assert!(message.contains("AAPL"));
        });
    }

    #[test]
    fn test_insert_predictions_rejects_missing_quantile() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let predictions = vec![serde_json::json!({
                "ticker": "AAPL",
                "timestamp": 1_735_689_600_000_i64,
                "quantile_10": -0.01,
                "quantile_50": 0.0,
            })];
            let error = insert_predictions(&lazy_pool(), &predictions, Uuid::new_v4(), "run-x")
                .await
                .unwrap_err();
            assert!(matches!(error, sqlx::Error::Decode(_)));
            let message = error.to_string();
            assert!(message.contains("quantile_90"));
            assert!(message.contains("AAPL"));
        });
    }

    #[test]
    fn test_prediction_from_json_accepts_valid_input() {
        let prediction = serde_json::json!({
            "ticker": "AAPL",
            "timestamp": 1_735_689_600_000_i64,
            "quantile_10": -0.01,
            "quantile_50": 0.0,
            "quantile_90": 0.02,
        });
        let correlation_id = Uuid::new_v4();
        let validated = prediction_from_json(&prediction, correlation_id, "run-x").unwrap();
        assert_eq!(validated.ticker(), "AAPL");
        assert_eq!(validated.model_run_id(), "run-x");
        assert_eq!(validated.correlation_id(), correlation_id);
        assert_eq!(validated.timestamp().timestamp_millis(), 1_735_689_600_000);
        assert_eq!(validated.quantile_10(), -0.01);
        assert_eq!(validated.quantile_50(), 0.0);
        assert_eq!(validated.quantile_90(), 0.02);
    }

    #[test]
    fn test_query_equity_bars_compiles() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let pool = lazy_pool();
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
            let pool = lazy_pool();
            let result = query_equity_details(&pool).await;
            assert!(result.is_err());
        });
    }

    #[test]
    fn test_model_run_record_parses_drift_status() {
        let metadata = serde_json::json!({
            "metrics": {"crps": 0.31},
            "drift": {
                "status": "drift_detected",
                "message": "Drift detected: ...",
                "baseline_crps": 0.25,
                "prior_runs": 7,
            },
        });
        let record = ModelRunRecord::from_metadata("run-x", "models/tide/run-x", &metadata);
        assert_eq!(record.drift_status(), Some("drift_detected"));

        // Metadata without a drift section (older runs) yields no status.
        let without = serde_json::json!({"metrics": {"crps": 0.31}});
        let record = ModelRunRecord::from_metadata("run-y", "models/tide/run-y", &without);
        assert_eq!(record.drift_status(), None);
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
        assert_eq!(record.run_id(), "2026-06-09-16-21-25-195");
        assert_eq!(record.continuous_ranked_probability_score(), Some(0.0059));
        assert_eq!(record.directional_accuracy(), Some(0.617));
        assert_eq!(record.quantile_coverage(), Some(0.719));
        assert_eq!(record.lookback_days(), Some(1200));
        assert_eq!(record.start_date(), NaiveDate::from_ymd_opt(2023, 2, 25));
        assert_eq!(record.stage_counts()["train_samples"], 2529261);
    }

    #[test]
    fn test_model_run_record_missing_fields_default_to_none() {
        let metadata = serde_json::json!({});
        let record = ModelRunRecord::from_metadata("run", "key", &metadata);
        assert_eq!(record.continuous_ranked_probability_score(), None);
        assert_eq!(record.lookback_days(), None);
        assert_eq!(record.start_date(), None);
    }
}
