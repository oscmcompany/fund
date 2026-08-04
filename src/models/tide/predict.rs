//! Running the TiDE model: from stored market history to rows in `equity_predictions`.
//!
//! Training and inference must apply the same liquidity thresholds — training per row, inference
//! per ticker average. Both read them from [`crate::common::types`] so they cannot drift; a
//! mismatch trains the scaler on dynamics the service never predicts.

use burn::backend::NdArray;
use chrono::{DateTime, Duration, Utc};
use polars::prelude::*;
use sqlx::PgPool;
use tracing::{info, warn};
use uuid::Uuid;

use crate::common::types::{EquityPrediction, Ticker};
use crate::models::tide::artifact::ModelState;
use crate::models::tide::data::Data;

#[derive(Debug, thiserror::Error)]
pub enum PredictionError {
    #[error("Model not loaded")]
    ModelNotLoaded,
    #[error("Failed to fetch equity bars: {0}")]
    FetchEquityBars(String),
    #[error("Failed to fetch equity details: {0}")]
    FetchEquityDetails(String),
    #[error("Data consolidation failed: {0}")]
    DataConsolidation(String),
    #[error("No matching tickers")]
    NoMatchingTickers,
    #[error("Preprocessing failed: {0}")]
    Preprocessing(String),
    #[error("Dataset creation failed: {0}")]
    DatasetCreation(String),
    #[error("Inference failed: {0}")]
    Inference(String),
    #[error("Postprocessing failed: {0}")]
    Postprocessing(String),
}

pub fn consolidate_data(
    equity_bars: DataFrame,
    equity_details: DataFrame,
) -> Result<DataFrame, PredictionError> {
    let bars = equity_bars
        .lazy()
        .unique_stable(
            Some(polars::prelude::Selector::ByName {
                names: vec![PlSmallStr::from("ticker"), PlSmallStr::from("timestamp")].into(),
                strict: false,
            }),
            UniqueKeepStrategy::Last,
        )
        .filter(
            col("open_price")
                .gt(lit(0.0))
                .and(col("high_price").gt(lit(0.0)))
                .and(col("low_price").gt(lit(0.0)))
                .and(col("close_price").gt(lit(0.0))),
        )
        .collect()
        .map_err(|e| PredictionError::DataConsolidation(e.to_string()))?;

    let details = equity_details
        .lazy()
        .select([
            col("ticker"),
            col("sector")
                .cast(DataType::String)
                .str()
                .strip_chars(lit(" ")),
            col("industry")
                .cast(DataType::String)
                .str()
                .strip_chars(lit(" ")),
        ])
        // Rows without a sector or industry cannot be categorically encoded.
        .filter(
            col("sector")
                .is_not_null()
                .and(col("industry").is_not_null()),
        )
        .collect()
        .map_err(|e| PredictionError::DataConsolidation(e.to_string()))?;

    let consolidated = bars
        .join(
            &details,
            ["ticker"],
            ["ticker"],
            JoinArgs::new(JoinType::Inner),
            None,
        )
        .map_err(|e| PredictionError::DataConsolidation(e.to_string()))?;

    let columns = [
        "ticker",
        "timestamp",
        "open_price",
        "high_price",
        "low_price",
        "close_price",
        "volume",
        "volume_weighted_average_price",
        "sector",
        "industry",
    ];

    let selected = consolidated
        .select(columns)
        .map_err(|e| PredictionError::DataConsolidation(e.to_string()))?;

    info!(rows = selected.height(), "Data consolidated");
    Ok(selected)
}

pub fn filter_equity_bars(
    data: DataFrame,
    minimum_average_close_price: f64,
    minimum_average_volume: f64,
) -> Result<DataFrame, PredictionError> {
    let before_count = data.height();

    let valid_tickers = data
        .clone()
        .lazy()
        .group_by([col("ticker")])
        .agg([
            col("close_price").mean().alias("average_close_price"),
            col("volume")
                .cast(DataType::Float64)
                .mean()
                .alias("average_volume"),
        ])
        .filter(
            col("average_close_price")
                .gt(lit(minimum_average_close_price))
                .and(col("average_volume").gt(lit(minimum_average_volume))),
        )
        .select([col("ticker")])
        .collect()
        .map_err(|e| PredictionError::DataConsolidation(e.to_string()))?;

    let filtered = data
        .lazy()
        .join(
            valid_tickers.lazy(),
            [col("ticker")],
            [col("ticker")],
            JoinArgs::new(JoinType::Semi),
        )
        .collect()
        .map_err(|e| PredictionError::DataConsolidation(e.to_string()))?;

    info!(
        before = before_count,
        after = filtered.height(),
        "Filtered equity bars by price and volume thresholds"
    );

    Ok(filtered)
}

pub fn filter_to_trained_tickers(
    data: DataFrame,
    model_state: &ModelState,
) -> Result<DataFrame, PredictionError> {
    let trained_tickers: Vec<String> = model_state
        .mappings()
        .get("ticker")
        .map(|m| m.keys().cloned().collect())
        .unwrap_or_default();

    if trained_tickers.is_empty() {
        return Err(PredictionError::NoMatchingTickers);
    }

    let ticker_series = Series::new("valid_ticker".into(), &trained_tickers);

    let original_count = data.height();
    let filtered = data
        .lazy()
        .with_column(col("ticker").cast(DataType::String).str().to_uppercase())
        .filter(col("ticker").is_in(lit(ticker_series), false))
        .collect()
        .map_err(|e| PredictionError::DataConsolidation(e.to_string()))?;

    if filtered.height() == 0 {
        return Err(PredictionError::NoMatchingTickers);
    }

    let original_tickers = original_count;
    let filtered_tickers = filtered.height();
    if original_tickers != filtered_tickers {
        info!(
            original = original_tickers,
            filtered = filtered_tickers,
            dropped = original_tickers - filtered_tickers,
            "Filtered to trained tickers"
        );
    }

    Ok(filtered)
}

/// Inverse-scale the predicted `daily_return` quantiles and sort them monotonic.
///
/// Quantile crossing is routine in quantile regression; sorting is the standard remedy.
pub(crate) fn unscale_and_sort_quantiles(
    scaled_quantiles: &[f64],
    scaler: &crate::models::tide::data::Scaler,
) -> Vec<f64> {
    let mut unscaled: Vec<f64> = scaled_quantiles
        .iter()
        .map(|value| scaler.inverse_transform_value("daily_return", *value))
        .collect();
    unscaled.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    unscaled
}

/// Timestamp for horizon step `step`, where step 0 is the Eastern session `now` falls in.
///
/// Midnight *Eastern*, not UTC. The evaluation pass selects forecasts with
/// [`crate::data::calendar::eastern_day_bounds`], and a UTC midnight sits at 20:00 the previous
/// Eastern day under daylight time -- so a UTC-stamped forecast lands in the wrong session's
/// window and the morning's inference run is never read by that day's passes.
pub(crate) fn step_timestamp_milliseconds(now: chrono::DateTime<Utc>, step: usize) -> i64 {
    let session = crate::data::calendar::eastern_date(now) + Duration::days(step as i64);
    crate::data::calendar::eastern_midnight(session).timestamp_millis()
}

pub fn generate_predictions(
    data: DataFrame,
    model_state: &ModelState,
) -> Result<serde_json::Value, PredictionError> {
    // One `now` for the whole run. The session the synthesized future row carries and the session
    // the output is stamped with are both derived from it, so the features and the label cannot
    // describe different days -- which is the shape of the bug this replaced.
    let now = Utc::now();
    let session = crate::data::calendar::eastern_date(now);

    let tide_data =
        Data::apply_existing_scaler(data, model_state.scaler(), model_state.mappings(), session)
            .map_err(|e| PredictionError::Preprocessing(e.to_string()))?;

    let output_length = model_state.parameters().output_length();
    let dataset_input_length = model_state.parameters().input_length();
    let dataset = tide_data
        .get_dataset("predict", 0.8, dataset_input_length, output_length)
        .map_err(|e| PredictionError::DatasetCreation(e.to_string()))?;

    if dataset.is_empty() {
        return Err(PredictionError::DatasetCreation(
            "No prediction samples created".to_string(),
        ));
    }

    info!(samples = dataset.len(), "Prediction dataset created");

    let device = Default::default();
    let num_samples = dataset.len();

    let indices: Vec<usize> = (0..num_samples).collect();
    let inputs = crate::models::tide::batch::build_input_tensor::<NdArray>(
        &dataset,
        &indices,
        dataset_input_length,
        output_length,
        &device,
    );

    let predictions = model_state.model().forward(inputs);
    let predictions_data: Vec<f32> = predictions
        .to_data()
        .to_vec()
        .map_err(|e| PredictionError::Inference(format!("{e:?}")))?;

    let quantile_count = model_state.parameters().quantiles().len();
    // The output schema is fixed at quantile_10/quantile_50/quantile_90, so a
    // model with any other quantile count cannot be served correctly; fail
    // loudly instead of indexing out of bounds or mislabeling values.
    if quantile_count != 3 {
        return Err(PredictionError::Postprocessing(format!(
            "Expected exactly 3 quantiles (10/50/90); the loaded model has {quantile_count}"
        )));
    }
    let mut results = Vec::new();

    // Indexing a `HashMap` panics on a missing key. An artifact without a `ticker` mapping is a
    // malformed artifact, which is a condition to report rather than one to abort the process on --
    // the pre-open handler can then fall back and record the failure in its errored payload.
    let ticker_mapping = model_state.mappings().get("ticker").ok_or_else(|| {
        PredictionError::Inference(
            "the loaded artifact has no 'ticker' categorical mapping".to_string(),
        )
    })?;
    let reverse_ticker_map: std::collections::HashMap<i32, &String> =
        ticker_mapping.iter().map(|(k, v)| (*v, k)).collect();

    for sample_idx in 0..num_samples {
        let ticker_id = dataset.static_categorical[[sample_idx, 0, 0]];
        let ticker = reverse_ticker_map
            .get(&ticker_id)
            .map(|s| s.as_str())
            .unwrap_or("UNKNOWN");

        for t in 0..output_length {
            let base_idx = (sample_idx * output_length + t) * quantile_count;

            let scaled: Vec<f64> = (0..quantile_count)
                .map(|q| predictions_data[base_idx + q] as f64)
                .collect();
            let quantiles = unscale_and_sort_quantiles(&scaled, model_state.scaler());

            results.push(serde_json::json!({
                "ticker": ticker,
                "timestamp": step_timestamp_milliseconds(now, t),
                "quantile_10": quantiles[0],
                "quantile_50": quantiles[1],
                "quantile_90": quantiles[2],
            }));
        }
    }

    // Step 0 -- the coming close -- is the only horizon the book can act on: the pre-close
    // liquidation flattens every position the same session, so a forecast further out describes a
    // holding period this strategy never has. Selected explicitly rather than as the last step, so
    // widening `output_length` for research does not silently move the traded signal.
    let target_date = step_timestamp_milliseconds(now, 0);

    let final_predictions: Vec<serde_json::Value> = results
        .into_iter()
        .filter(|r| r["timestamp"] == target_date)
        .collect();

    info!(count = final_predictions.len(), "Predictions generated");

    Ok(serde_json::json!(final_predictions))
}

pub fn validate_predictions(predictions: &[serde_json::Value]) -> Result<(), String> {
    if predictions.is_empty() {
        return Ok(());
    }

    let mut seen_pairs: std::collections::HashSet<(String, i64)> = std::collections::HashSet::new();
    let mut timestamps_by_ticker: std::collections::HashMap<String, Vec<i64>> =
        std::collections::HashMap::new();

    for prediction in predictions {
        let ticker = prediction["ticker"]
            .as_str()
            .ok_or("Missing ticker field")?;

        if ticker != ticker.to_uppercase() {
            let message = format!("Ticker not uppercase: {ticker}");
            return Err(message);
        }

        let timestamp = prediction["timestamp"]
            .as_i64()
            .ok_or("Missing timestamp field")?;

        let q10 = prediction["quantile_10"]
            .as_f64()
            .ok_or("Missing quantile_10 field")?;
        let q50 = prediction["quantile_50"]
            .as_f64()
            .ok_or("Missing quantile_50 field")?;
        let q90 = prediction["quantile_90"]
            .as_f64()
            .ok_or("Missing quantile_90 field")?;

        if q10 > q50 || q50 > q90 {
            let message =
                format!("Non-monotonic quantiles for {ticker}: q10={q10}, q50={q50}, q90={q90}");
            return Err(message);
        }

        let pair = (ticker.to_string(), timestamp);
        if !seen_pairs.insert(pair) {
            let message = format!("Duplicate ticker/timestamp pair: {ticker}/{timestamp}");
            return Err(message);
        }

        timestamps_by_ticker
            .entry(ticker.to_string())
            .or_default()
            .push(timestamp);
    }

    let all_timestamp_sets: Vec<Vec<i64>> = timestamps_by_ticker
        .values()
        .map(|ts| {
            let mut sorted = ts.clone();
            sorted.sort();
            sorted
        })
        .collect();

    if let Some(reference) = all_timestamp_sets.first() {
        for ts_set in &all_timestamp_sets[1..] {
            if ts_set != reference {
                let message = "Timestamps are not consistent across all tickers".to_string();
                return Err(message);
            }
        }
    }

    Ok(())
}

// --------------------------------------------------------------------------
// Persistence
// --------------------------------------------------------------------------

/// Converts a pipeline prediction JSON object into a validated [`EquityPrediction`].
///
/// These come from our own pipeline, so a missing or mistyped field is a bug upstream. It fails
/// loudly with the offending field and ticker rather than persisting placeholders.
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

    let validated_ticker = Ticker::new(ticker).ok_or_else(|| {
        sqlx::Error::Decode(format!("Invalid ticker in prediction payload: {ticker}").into())
    })?;
    // `EquityPrediction::new` enforces the quantile ordering invariant, so a crossed set from the
    // model is rejected here rather than stored. Every downstream use -- the confidence measure,
    // the directional signal -- produces plausible nonsense from crossed quantiles rather than
    // failing, which makes this the last place the error is cheap to catch.
    EquityPrediction::new(
        correlation_id,
        model_run_id.to_string(),
        validated_ticker,
        timestamp,
        quantile("quantile_10")?,
        quantile("quantile_50")?,
        quantile("quantile_90")?,
    )
    .map_err(|error| {
        sqlx::Error::Decode(format!("Prediction for ticker {ticker} is invalid: {error}").into())
    })
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
    let mut transaction = pool.begin().await?;

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

        let result = query_builder.build().execute(&mut *transaction).await?;
        rows_affected += result.rows_affected();
    }

    transaction.commit().await?;
    info!(rows = rows_affected, "Predictions inserted into PostgreSQL");
    Ok(rows_affected)
}

/// Loads the most recent prediction per ticker within a half-open instant range.
///
/// The range is the current session's Eastern day, so a pass on a morning when the pre-open
/// inference failed reads nothing rather than reading yesterday's forecast as though it were
/// today's. A stale artifact is acceptable and logged; a stale *prediction* silently presented as
/// current is not, because nothing downstream carries the timestamp far enough to notice.
///
/// `DISTINCT ON` rather than a group-by join: the table's primary key is `(ticker, timestamp)`, so
/// the newest row per ticker is a single ordered scan of the day's chunk.
pub async fn load_predictions_between(
    pool: &PgPool,
    start: chrono::DateTime<chrono::Utc>,
    end: chrono::DateTime<chrono::Utc>,
) -> Result<Vec<EquityPrediction>, sqlx::Error> {
    let rows = sqlx::query!(
        r#"
        SELECT DISTINCT ON (ticker)
               ticker AS "ticker!",
               correlation_id AS "correlation_id!",
               model_run_id AS "model_run_id!",
               timestamp AS "timestamp!",
               quantile_10 AS "quantile_10!",
               quantile_50 AS "quantile_50!",
               quantile_90 AS "quantile_90!"
        FROM equity_predictions
        WHERE timestamp >= $1 AND timestamp < $2
        ORDER BY ticker, timestamp DESC
        "#,
        start,
        end,
    )
    .fetch_all(pool)
    .await?;

    let mut predictions = Vec::with_capacity(rows.len());
    let mut rejected: usize = 0;
    for row in rows {
        // Named individually, not just counted. A rejected row means the stored data already
        // violates the invariant `insert_predictions` enforces at write time, so the operator needs
        // to find that row -- and a count alone cannot locate it.
        let Some(ticker) = Ticker::new(&row.ticker) else {
            warn!(ticker = %row.ticker, "Dropped a prediction row whose ticker is unusable");
            rejected += 1;
            continue;
        };
        let stored_ticker = ticker.clone();
        match EquityPrediction::new(
            row.correlation_id,
            row.model_run_id,
            ticker,
            row.timestamp,
            row.quantile_10,
            row.quantile_50,
            row.quantile_90,
        ) {
            Ok(prediction) => predictions.push(prediction),
            Err(error) => {
                warn!(
                    ticker = %stored_ticker,
                    timestamp = %row.timestamp,
                    %error,
                    "Dropped a stored prediction that violates the quantile ordering invariant"
                );
                rejected += 1;
            }
        }
    }

    if rejected > 0 {
        warn!(rejected, "Dropped unreadable prediction rows");
    }
    info!(predictions = predictions.len(), "Predictions loaded");
    Ok(predictions)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_filter_equity_bars_above_thresholds() {
        let data = DataFrame::new(vec![
            Column::new("ticker".into(), vec!["AAPL", "AAPL", "GOOG", "GOOG"]),
            Column::new("timestamp".into(), vec![1000i64, 2000, 1000, 2000]),
            Column::new("close_price".into(), vec![150.0, 160.0, 200.0, 210.0]),
            Column::new(
                "volume".into(),
                vec![2_000_000i64, 3_000_000, 5_000_000, 4_000_000],
            ),
        ])
        .unwrap();

        let result = filter_equity_bars(data, 10.0, 1_000_000.0).unwrap();
        assert_eq!(result.height(), 4);
    }

    #[test]
    fn test_filter_equity_bars_below_close_threshold() {
        let data = DataFrame::new(vec![
            Column::new("ticker".into(), vec!["PENNY", "PENNY", "GOOG", "GOOG"]),
            Column::new("timestamp".into(), vec![1000i64, 2000, 1000, 2000]),
            Column::new("close_price".into(), vec![5.0, 6.0, 200.0, 210.0]),
            Column::new(
                "volume".into(),
                vec![2_000_000i64, 3_000_000, 5_000_000, 4_000_000],
            ),
        ])
        .unwrap();

        let result = filter_equity_bars(data, 10.0, 1_000_000.0).unwrap();
        assert_eq!(result.height(), 2);
        let tickers: Vec<&str> = result
            .column("ticker")
            .unwrap()
            .str()
            .unwrap()
            .into_no_null_iter()
            .collect();
        assert!(tickers.iter().all(|t| *t == "GOOG"));
    }

    #[test]
    fn test_filter_equity_bars_below_volume_threshold() {
        let data = DataFrame::new(vec![
            Column::new("ticker".into(), vec!["LOW", "LOW", "GOOG", "GOOG"]),
            Column::new("timestamp".into(), vec![1000i64, 2000, 1000, 2000]),
            Column::new("close_price".into(), vec![50.0, 60.0, 200.0, 210.0]),
            Column::new("volume".into(), vec![100i64, 200, 5_000_000, 4_000_000]),
        ])
        .unwrap();

        let result = filter_equity_bars(data, 10.0, 1_000_000.0).unwrap();
        assert_eq!(result.height(), 2);
        let tickers: Vec<&str> = result
            .column("ticker")
            .unwrap()
            .str()
            .unwrap()
            .into_no_null_iter()
            .collect();
        assert!(tickers.iter().all(|t| *t == "GOOG"));
    }

    #[test]
    fn test_filter_equity_bars_empty_input() {
        let data = DataFrame::new(vec![
            Column::new("ticker".into(), Vec::<&str>::new()),
            Column::new("timestamp".into(), Vec::<i64>::new()),
            Column::new("close_price".into(), Vec::<f64>::new()),
            Column::new("volume".into(), Vec::<i64>::new()),
        ])
        .unwrap();

        let result = filter_equity_bars(data, 10.0, 1_000_000.0).unwrap();
        assert_eq!(result.height(), 0);
    }

    #[test]
    fn test_validate_predictions_valid() {
        let predictions = vec![
            serde_json::json!({"ticker": "AAPL", "timestamp": 1000, "quantile_10": 0.01, "quantile_50": 0.02, "quantile_90": 0.03}),
            serde_json::json!({"ticker": "GOOG", "timestamp": 1000, "quantile_10": 0.05, "quantile_50": 0.06, "quantile_90": 0.07}),
        ];
        assert!(validate_predictions(&predictions).is_ok());
    }

    #[test]
    fn test_validate_predictions_non_monotonic() {
        let predictions = vec![
            serde_json::json!({"ticker": "AAPL", "timestamp": 1000, "quantile_10": 0.05, "quantile_50": 0.02, "quantile_90": 0.03}),
        ];
        let result = validate_predictions(&predictions);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Non-monotonic"));
    }

    #[test]
    fn test_validate_predictions_mixed_timestamps() {
        let predictions = vec![
            serde_json::json!({"ticker": "AAPL", "timestamp": 1000, "quantile_10": 0.01, "quantile_50": 0.02, "quantile_90": 0.03}),
            serde_json::json!({"ticker": "GOOG", "timestamp": 2000, "quantile_10": 0.01, "quantile_50": 0.02, "quantile_90": 0.03}),
        ];
        let result = validate_predictions(&predictions);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Timestamps"));
    }

    #[test]
    fn test_validate_predictions_duplicate_pair() {
        let predictions = vec![
            serde_json::json!({"ticker": "AAPL", "timestamp": 1000, "quantile_10": 0.01, "quantile_50": 0.02, "quantile_90": 0.03}),
            serde_json::json!({"ticker": "AAPL", "timestamp": 1000, "quantile_10": 0.04, "quantile_50": 0.05, "quantile_90": 0.06}),
        ];
        let result = validate_predictions(&predictions);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Duplicate"));
    }

    #[test]
    fn test_unscale_and_sort_quantiles_repairs_crossing() {
        let mut means = std::collections::HashMap::new();
        means.insert("daily_return".to_string(), 0.0);
        let mut standard_deviations = std::collections::HashMap::new();
        standard_deviations.insert("daily_return".to_string(), 1.0);
        let scaler = crate::models::tide::data::Scaler {
            means,
            standard_deviations,
        };

        // Crossed raw quantiles (q10 > q50) must come back monotonic.
        let sorted = unscale_and_sort_quantiles(&[0.05, 0.02, 0.03], &scaler);
        assert_eq!(sorted, vec![0.02, 0.03, 0.05]);
    }

    #[test]
    fn test_predictions_are_visible_to_the_same_session_evaluation() {
        // The 09:00 Eastern run exists to feed the same session's evaluation passes, which select
        // rows with `eastern_day_bounds`. A stamp built at UTC midnight falls in the *previous*
        // Eastern day's window -- 20:00 the day before, under EDT -- so the forecast written this
        // morning is never the one read this afternoon.
        use crate::data::calendar::{eastern_date, eastern_day_bounds};

        for day in [
            "2026-08-03",
            "2026-08-04",
            "2026-08-05",
            "2026-08-06",
            "2026-08-07",
        ] {
            // 13:00Z is 09:00 Eastern while daylight time is in effect.
            let now = chrono::DateTime::parse_from_rfc3339(&format!("{day}T13:00:00Z"))
                .unwrap()
                .with_timezone(&Utc);
            let stamp = DateTime::<Utc>::from_timestamp_millis(step_timestamp_milliseconds(now, 0))
                .unwrap();
            let (start, end) = eastern_day_bounds(eastern_date(now));
            assert!(
                stamp >= start && stamp < end,
                "{day}: forecast stamped {stamp} is outside the session window [{start}, {end})"
            );
        }
    }

    #[test]
    fn test_step_timestamp_step_zero_is_the_current_eastern_session() {
        // Step t is the Eastern session t days out, stamped at Eastern midnight. 15:30Z is 11:30
        // Eastern, so step 0 is that same session rather than the one UTC is already into.
        use crate::data::calendar::eastern_midnight;

        let now = chrono::DateTime::parse_from_rfc3339("2026-06-09T15:30:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let session = chrono::NaiveDate::from_ymd_opt(2026, 6, 9).unwrap();
        assert_eq!(
            step_timestamp_milliseconds(now, 0),
            eastern_midnight(session).timestamp_millis()
        );
        assert_eq!(
            step_timestamp_milliseconds(now, 4),
            eastern_midnight(session + Duration::days(4)).timestamp_millis()
        );
    }

    #[test]
    fn test_late_evening_eastern_still_stamps_the_current_session() {
        // 01:00Z is 21:00 the previous Eastern day. Stamping off the UTC date would jump the
        // forecast a session forward, which is the failure this function exists to avoid.
        use crate::data::calendar::eastern_midnight;

        let now = chrono::DateTime::parse_from_rfc3339("2026-06-10T01:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        assert_eq!(
            step_timestamp_milliseconds(now, 0),
            eastern_midnight(chrono::NaiveDate::from_ymd_opt(2026, 6, 9).unwrap())
                .timestamp_millis()
        );
    }

    #[test]
    fn test_consolidate_data_drops_null_sector_or_industry() {
        let bars = DataFrame::new(vec![
            Column::new("ticker".into(), vec!["AAPL", "GOOG"]),
            Column::new("timestamp".into(), vec![1000i64, 1000]),
            Column::new("open_price".into(), vec![100.0, 200.0]),
            Column::new("high_price".into(), vec![105.0, 205.0]),
            Column::new("low_price".into(), vec![95.0, 195.0]),
            Column::new("close_price".into(), vec![102.0, 202.0]),
            Column::new("volume".into(), vec![1_000_000i64, 2_000_000]),
            Column::new("volume_weighted_average_price".into(), vec![101.0, 201.0]),
        ])
        .unwrap();

        let details = DataFrame::new(vec![
            Column::new("ticker".into(), vec!["AAPL", "GOOG"]),
            Column::new("sector".into(), vec![Some("Technology"), None::<&str>]),
            Column::new(
                "industry".into(),
                vec![Some("Consumer Electronics"), Some("Internet")],
            ),
        ])
        .unwrap();

        let result = consolidate_data(bars, details).unwrap();
        assert_eq!(result.height(), 1);
        let tickers: Vec<&str> = result
            .column("ticker")
            .unwrap()
            .str()
            .unwrap()
            .into_no_null_iter()
            .collect();
        assert_eq!(tickers, vec!["AAPL"]);
    }

    #[test]
    fn test_consolidate_data() {
        let bars = DataFrame::new(vec![
            Column::new("ticker".into(), vec!["AAPL", "GOOG", "AAPL"]),
            Column::new("timestamp".into(), vec![1000i64, 1000, 2000]),
            Column::new("open_price".into(), vec![100.0, 200.0, 101.0]),
            Column::new("high_price".into(), vec![105.0, 205.0, 106.0]),
            Column::new("low_price".into(), vec![95.0, 195.0, 96.0]),
            Column::new("close_price".into(), vec![102.0, 202.0, 103.0]),
            Column::new("volume".into(), vec![1000000i64, 2000000, 1100000]),
            Column::new(
                "volume_weighted_average_price".into(),
                vec![101.0, 201.0, 102.0],
            ),
        ])
        .unwrap();

        let details = DataFrame::new(vec![
            Column::new("ticker".into(), vec!["AAPL", "GOOG"]),
            Column::new("sector".into(), vec!["Technology", "Technology"]),
            Column::new("industry".into(), vec!["Consumer Electronics", "Internet"]),
        ])
        .unwrap();

        let result = consolidate_data(bars, details).unwrap();
        assert!(result.height() > 0);
        assert!(result.column("sector").is_ok());
    }

    #[test]
    fn test_consolidate_data_filters_zero_price_bars() {
        // Bars with a zero close_price must be dropped.
        let bars = DataFrame::new(vec![
            Column::new("ticker".into(), vec!["AAPL", "AAPL"]),
            Column::new("timestamp".into(), vec![1000i64, 2000]),
            Column::new("open_price".into(), vec![0.0, 100.0]),
            Column::new("high_price".into(), vec![0.0, 105.0]),
            Column::new("low_price".into(), vec![0.0, 95.0]),
            Column::new("close_price".into(), vec![0.0, 102.0]),
            Column::new("volume".into(), vec![1_000_000i64, 1_100_000]),
            Column::new("volume_weighted_average_price".into(), vec![0.0f64, 101.0]),
        ])
        .unwrap();

        let details = DataFrame::new(vec![
            Column::new("ticker".into(), vec!["AAPL"]),
            Column::new("sector".into(), vec!["Technology"]),
            Column::new("industry".into(), vec!["Consumer Electronics"]),
        ])
        .unwrap();

        let result = consolidate_data(bars, details).unwrap();
        // Only the valid bar (timestamp 2000) should survive.
        assert_eq!(result.height(), 1);
    }

    #[test]
    fn test_consolidate_data_deduplicates_same_ticker_timestamp() {
        // Duplicate (ticker, timestamp) pairs should be deduplicated, keeping the last.
        let bars = DataFrame::new(vec![
            Column::new("ticker".into(), vec!["AAPL", "AAPL"]),
            Column::new("timestamp".into(), vec![1000i64, 1000]),
            Column::new("open_price".into(), vec![100.0, 101.0]),
            Column::new("high_price".into(), vec![105.0, 106.0]),
            Column::new("low_price".into(), vec![95.0, 96.0]),
            Column::new("close_price".into(), vec![102.0, 103.0]),
            Column::new("volume".into(), vec![1_000_000i64, 1_100_000]),
            Column::new(
                "volume_weighted_average_price".into(),
                vec![101.0f64, 102.0],
            ),
        ])
        .unwrap();

        let details = DataFrame::new(vec![
            Column::new("ticker".into(), vec!["AAPL"]),
            Column::new("sector".into(), vec!["Technology"]),
            Column::new("industry".into(), vec!["Consumer Electronics"]),
        ])
        .unwrap();

        let result = consolidate_data(bars, details).unwrap();
        assert_eq!(result.height(), 1);
    }

    #[test]
    fn test_consolidate_data_inner_join_drops_unmatched_tickers() {
        // Bars for a ticker that has no entry in equity_details must be dropped.
        let bars = DataFrame::new(vec![
            Column::new("ticker".into(), vec!["AAPL", "UNKWN"]),
            Column::new("timestamp".into(), vec![1000i64, 1000]),
            Column::new("open_price".into(), vec![100.0, 50.0]),
            Column::new("high_price".into(), vec![105.0, 55.0]),
            Column::new("low_price".into(), vec![95.0, 45.0]),
            Column::new("close_price".into(), vec![102.0, 52.0]),
            Column::new("volume".into(), vec![1_000_000i64, 500_000]),
            Column::new("volume_weighted_average_price".into(), vec![101.0f64, 51.0]),
        ])
        .unwrap();

        let details = DataFrame::new(vec![
            Column::new("ticker".into(), vec!["AAPL"]),
            Column::new("sector".into(), vec!["Technology"]),
            Column::new("industry".into(), vec!["Consumer Electronics"]),
        ])
        .unwrap();

        let result = consolidate_data(bars, details).unwrap();
        assert_eq!(result.height(), 1);
        let tickers: Vec<&str> = result
            .column("ticker")
            .unwrap()
            .str()
            .unwrap()
            .into_no_null_iter()
            .collect();
        assert_eq!(tickers, vec!["AAPL"]);
    }

    #[test]
    fn test_validate_predictions_empty_is_ok() {
        assert!(validate_predictions(&[]).is_ok());
    }

    #[test]
    fn test_validate_predictions_lowercase_ticker_errors() {
        let predictions = vec![
            serde_json::json!({"ticker": "aapl", "timestamp": 1000, "quantile_10": 0.01, "quantile_50": 0.02, "quantile_90": 0.03}),
        ];
        let result = validate_predictions(&predictions);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("not uppercase"));
    }

    #[test]
    fn test_validate_predictions_missing_ticker_field_errors() {
        let predictions = vec![
            serde_json::json!({"timestamp": 1000, "quantile_10": 0.01, "quantile_50": 0.02, "quantile_90": 0.03}),
        ];
        let result = validate_predictions(&predictions);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Missing ticker"));
    }

    #[test]
    fn test_validate_predictions_missing_timestamp_field_errors() {
        let predictions = vec![
            serde_json::json!({"ticker": "AAPL", "quantile_10": 0.01, "quantile_50": 0.02, "quantile_90": 0.03}),
        ];
        let result = validate_predictions(&predictions);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Missing timestamp"));
    }

    #[test]
    fn test_validate_predictions_missing_quantile_10_field_errors() {
        let predictions = vec![
            serde_json::json!({"ticker": "AAPL", "timestamp": 1000, "quantile_50": 0.02, "quantile_90": 0.03}),
        ];
        let result = validate_predictions(&predictions);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Missing quantile_10"));
    }

    #[test]
    fn test_validate_predictions_missing_quantile_50_field_errors() {
        let predictions = vec![
            serde_json::json!({"ticker": "AAPL", "timestamp": 1000, "quantile_10": 0.01, "quantile_90": 0.03}),
        ];
        let result = validate_predictions(&predictions);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Missing quantile_50"));
    }

    #[test]
    fn test_validate_predictions_missing_quantile_90_field_errors() {
        let predictions = vec![
            serde_json::json!({"ticker": "AAPL", "timestamp": 1000, "quantile_10": 0.01, "quantile_50": 0.02}),
        ];
        let result = validate_predictions(&predictions);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Missing quantile_90"));
    }

    #[test]
    fn test_validate_predictions_equal_quantiles_passes() {
        // q10 == q50 == q90 is technically monotonic; must not error.
        let predictions = vec![
            serde_json::json!({"ticker": "AAPL", "timestamp": 1000, "quantile_10": 0.02, "quantile_50": 0.02, "quantile_90": 0.02}),
        ];
        assert!(validate_predictions(&predictions).is_ok());
    }

    #[test]
    fn test_validate_predictions_q50_exceeds_q90_errors() {
        let predictions = vec![
            serde_json::json!({"ticker": "AAPL", "timestamp": 1000, "quantile_10": 0.01, "quantile_50": 0.05, "quantile_90": 0.03}),
        ];
        let result = validate_predictions(&predictions);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Non-monotonic"));
    }

    #[test]
    fn test_validate_predictions_consistent_timestamps_multiple_tickers() {
        // Both tickers must have the same set of timestamps.
        let predictions = vec![
            serde_json::json!({"ticker": "AAPL", "timestamp": 1000, "quantile_10": 0.01, "quantile_50": 0.02, "quantile_90": 0.03}),
            serde_json::json!({"ticker": "AAPL", "timestamp": 2000, "quantile_10": 0.01, "quantile_50": 0.02, "quantile_90": 0.03}),
            serde_json::json!({"ticker": "GOOG", "timestamp": 1000, "quantile_10": 0.01, "quantile_50": 0.02, "quantile_90": 0.03}),
            serde_json::json!({"ticker": "GOOG", "timestamp": 2000, "quantile_10": 0.01, "quantile_50": 0.02, "quantile_90": 0.03}),
        ];
        assert!(validate_predictions(&predictions).is_ok());
    }

    #[test]
    fn test_unscale_and_sort_quantiles_already_sorted() {
        let mut means = std::collections::HashMap::new();
        means.insert("daily_return".to_string(), 0.0);
        let mut standard_deviations = std::collections::HashMap::new();
        standard_deviations.insert("daily_return".to_string(), 1.0);
        let scaler = crate::models::tide::data::Scaler {
            means,
            standard_deviations,
        };

        // Already-sorted quantiles must come back unchanged.
        let result = unscale_and_sort_quantiles(&[0.01, 0.02, 0.03], &scaler);
        assert_eq!(result, vec![0.01, 0.02, 0.03]);
    }

    #[test]
    fn test_unscale_and_sort_quantiles_with_nonzero_mean_and_std() {
        // inverse_transform = value * std + mean
        let mut means = std::collections::HashMap::new();
        means.insert("daily_return".to_string(), 0.005);
        let mut standard_deviations = std::collections::HashMap::new();
        standard_deviations.insert("daily_return".to_string(), 0.01);
        let scaler = crate::models::tide::data::Scaler {
            means,
            standard_deviations,
        };

        let result = unscale_and_sort_quantiles(&[-1.0, 0.0, 1.0], &scaler);
        // -1.0 * 0.01 + 0.005 = -0.005, 0.0 * 0.01 + 0.005 = 0.005, 1.0 * 0.01 + 0.005 = 0.015
        assert!((result[0] - (-0.005)).abs() < 1e-12);
        assert!((result[1] - 0.005).abs() < 1e-12);
        assert!((result[2] - 0.015).abs() < 1e-12);
    }

    #[test]
    fn test_step_timestamp_advances_one_session_per_step() {
        // Compared against `eastern_midnight` rather than a fixed 86,400,000 ms stride: successive
        // Eastern midnights are 23 or 25 hours apart across a daylight-saving transition, so a
        // fixed stride asserts something that is only true away from March and November.
        use crate::data::calendar::eastern_midnight;

        let now = chrono::DateTime::parse_from_rfc3339("2026-01-01T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let session = chrono::NaiveDate::from_ymd_opt(2025, 12, 31).unwrap();
        for step in [0usize, 1, 7] {
            assert_eq!(
                step_timestamp_milliseconds(now, step),
                eastern_midnight(session + Duration::days(step as i64)).timestamp_millis(),
                "step {step} did not land on its Eastern session midnight"
            );
        }
    }

    #[test]
    fn test_step_timestamp_crosses_daylight_saving_transitions() {
        // Eastern time springs forward 2026-03-08 and falls back 2026-11-01. Stepping across either
        // boundary must still land on the session's own midnight, which a fixed day-length stride
        // would miss by an hour in each direction.
        use crate::data::calendar::eastern_midnight;

        for (start, label) in [
            ("2026-03-06T17:00:00Z", "spring forward"),
            ("2026-10-30T17:00:00Z", "fall back"),
        ] {
            let now = chrono::DateTime::parse_from_rfc3339(start)
                .unwrap()
                .with_timezone(&Utc);
            let session = crate::data::calendar::eastern_date(now);
            for step in 0..5usize {
                let expected = eastern_midnight(session + Duration::days(step as i64));
                assert_eq!(
                    step_timestamp_milliseconds(now, step),
                    expected.timestamp_millis(),
                    "{label}: step {step} did not land on {expected}"
                );
            }
        }
    }

    #[test]
    fn test_prediction_error_display_model_not_loaded() {
        let error = PredictionError::ModelNotLoaded;
        assert_eq!(error.to_string(), "Model not loaded");
    }

    #[test]
    fn test_prediction_error_display_fetch_equity_bars() {
        let error = PredictionError::FetchEquityBars("connection refused".to_string());
        assert!(error.to_string().contains("connection refused"));
    }

    #[test]
    fn test_prediction_error_display_fetch_equity_details() {
        let error = PredictionError::FetchEquityDetails("timeout".to_string());
        assert!(error.to_string().contains("timeout"));
    }

    #[test]
    fn test_prediction_error_display_data_consolidation() {
        let error = PredictionError::DataConsolidation("schema mismatch".to_string());
        assert!(error.to_string().contains("schema mismatch"));
    }

    #[test]
    fn test_prediction_error_display_no_matching_tickers() {
        let error = PredictionError::NoMatchingTickers;
        assert_eq!(error.to_string(), "No matching tickers");
    }

    #[test]
    fn test_prediction_error_display_preprocessing() {
        let error = PredictionError::Preprocessing("scaler failed".to_string());
        assert!(error.to_string().contains("scaler failed"));
    }

    #[test]
    fn test_prediction_error_display_dataset_creation() {
        let error = PredictionError::DatasetCreation("empty dataset".to_string());
        assert!(error.to_string().contains("empty dataset"));
    }

    #[test]
    fn test_prediction_error_display_inference() {
        let error = PredictionError::Inference("tensor shape".to_string());
        assert!(error.to_string().contains("tensor shape"));
    }

    #[test]
    fn test_prediction_error_display_postprocessing() {
        let error = PredictionError::Postprocessing("3 quantiles".to_string());
        assert!(error.to_string().contains("3 quantiles"));
    }

    #[test]
    fn test_filter_equity_bars_all_below_thresholds() {
        // When all tickers fail both thresholds, result is empty.
        let data = DataFrame::new(vec![
            Column::new("ticker".into(), vec!["PENNY"]),
            Column::new("timestamp".into(), vec![1000i64]),
            Column::new("close_price".into(), vec![1.0]),
            Column::new("volume".into(), vec![100i64]),
        ])
        .unwrap();

        let result = filter_equity_bars(data, 10.0, 1_000_000.0).unwrap();
        assert_eq!(result.height(), 0);
    }

    #[test]
    fn test_filter_equity_bars_single_ticker_passes_both() {
        let data = DataFrame::new(vec![
            Column::new("ticker".into(), vec!["AAPL"]),
            Column::new("timestamp".into(), vec![1000i64]),
            Column::new("close_price".into(), vec![200.0]),
            Column::new("volume".into(), vec![5_000_000i64]),
        ])
        .unwrap();

        let result = filter_equity_bars(data, 10.0, 1_000_000.0).unwrap();
        assert_eq!(result.height(), 1);
    }

    #[test]
    fn test_consolidate_data_empty_bars_returns_empty() {
        let bars = DataFrame::new(vec![
            Column::new("ticker".into(), Vec::<&str>::new()),
            Column::new("timestamp".into(), Vec::<i64>::new()),
            Column::new("open_price".into(), Vec::<f64>::new()),
            Column::new("high_price".into(), Vec::<f64>::new()),
            Column::new("low_price".into(), Vec::<f64>::new()),
            Column::new("close_price".into(), Vec::<f64>::new()),
            Column::new("volume".into(), Vec::<i64>::new()),
            Column::new("volume_weighted_average_price".into(), Vec::<f64>::new()),
        ])
        .unwrap();

        let details = DataFrame::new(vec![
            Column::new("ticker".into(), vec!["AAPL"]),
            Column::new("sector".into(), vec!["Technology"]),
            Column::new("industry".into(), vec!["Consumer Electronics"]),
        ])
        .unwrap();

        let result = consolidate_data(bars, details).unwrap();
        assert_eq!(result.height(), 0);
    }

    #[test]
    fn test_consolidate_data_both_null_sector_and_industry_dropped() {
        let bars = DataFrame::new(vec![
            Column::new("ticker".into(), vec!["AAPL"]),
            Column::new("timestamp".into(), vec![1000i64]),
            Column::new("open_price".into(), vec![100.0]),
            Column::new("high_price".into(), vec![105.0]),
            Column::new("low_price".into(), vec![95.0]),
            Column::new("close_price".into(), vec![102.0]),
            Column::new("volume".into(), vec![1_000_000i64]),
            Column::new("volume_weighted_average_price".into(), vec![101.0]),
        ])
        .unwrap();

        let details = DataFrame::new(vec![
            Column::new("ticker".into(), vec!["AAPL"]),
            Column::new("sector".into(), vec![None::<&str>]),
            Column::new("industry".into(), vec![None::<&str>]),
        ])
        .unwrap();

        let result = consolidate_data(bars, details).unwrap();
        assert_eq!(result.height(), 0);
    }

    #[test]
    fn test_validate_predictions_single_ticker_single_timestamp_ok() {
        let predictions = vec![serde_json::json!({
            "ticker": "AAPL",
            "timestamp": 1_750_000_000_000i64,
            "quantile_10": -0.01,
            "quantile_50": 0.0,
            "quantile_90": 0.01,
        })];
        assert!(validate_predictions(&predictions).is_ok());
    }

    #[test]
    fn test_validate_predictions_three_tickers_same_timestamps_ok() {
        let ts = 1_750_000_000_000i64;
        let predictions = vec![
            serde_json::json!({"ticker": "AAPL", "timestamp": ts, "quantile_10": 0.0, "quantile_50": 0.01, "quantile_90": 0.02}),
            serde_json::json!({"ticker": "MSFT", "timestamp": ts, "quantile_10": 0.0, "quantile_50": 0.01, "quantile_90": 0.02}),
            serde_json::json!({"ticker": "GOOG", "timestamp": ts, "quantile_10": 0.0, "quantile_50": 0.01, "quantile_90": 0.02}),
        ];
        assert!(validate_predictions(&predictions).is_ok());
    }

    #[test]
    fn test_unscale_and_sort_quantiles_single_element() {
        let mut means = std::collections::HashMap::new();
        means.insert("daily_return".to_string(), 0.0);
        let mut standard_deviations = std::collections::HashMap::new();
        standard_deviations.insert("daily_return".to_string(), 1.0);
        let scaler = crate::models::tide::data::Scaler {
            means,
            standard_deviations,
        };

        let result = unscale_and_sort_quantiles(&[0.05], &scaler);
        assert_eq!(result.len(), 1);
        assert!((result[0] - 0.05).abs() < 1e-12);
    }

    #[test]
    fn test_unscale_and_sort_quantiles_empty_input() {
        let mut means = std::collections::HashMap::new();
        means.insert("daily_return".to_string(), 0.0);
        let mut standard_deviations = std::collections::HashMap::new();
        standard_deviations.insert("daily_return".to_string(), 1.0);
        let scaler = crate::models::tide::data::Scaler {
            means,
            standard_deviations,
        };

        let result = unscale_and_sort_quantiles(&[], &scaler);
        assert!(result.is_empty());
    }
}
