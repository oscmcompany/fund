use std::io::Cursor;

use burn::backend::NdArray;
use chrono::{Duration, Utc};
use polars::prelude::*;
use sqlx::PgPool;
use tracing::info;

use crate::models::tide::data::Data;

use crate::ensemble_manager::database;
use crate::ensemble_manager::state::ModelState;

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

pub async fn fetch_equity_bars(
    base_url: &str,
    http_client: &reqwest::Client,
) -> Result<DataFrame, PredictionError> {
    let end_date = Utc::now();
    let start_date = end_date - Duration::days(70);

    let start_timestamp = start_date.timestamp_millis();
    let end_timestamp = end_date.timestamp_millis();

    let url = format!(
        "{base_url}/equity-bars?start_timestamp={start_timestamp}&end_timestamp={end_timestamp}"
    );

    info!(url = url, "Fetching equity bars");

    let response = http_client
        .get(&url)
        .send()
        .await
        .map_err(|e| PredictionError::FetchEquityBars(e.to_string()))?;

    let bytes = response
        .bytes()
        .await
        .map_err(|e| PredictionError::FetchEquityBars(e.to_string()))?;

    let cursor = Cursor::new(bytes);
    let data = ParquetReader::new(cursor)
        .finish()
        .map_err(|e| PredictionError::FetchEquityBars(e.to_string()))?;

    info!(rows = data.height(), "Equity bars fetched");
    Ok(data)
}

pub async fn fetch_equity_details(
    base_url: &str,
    http_client: &reqwest::Client,
) -> Result<DataFrame, PredictionError> {
    let url = format!("{base_url}/equity-details");

    info!(url = url, "Fetching equity details");

    let response = http_client
        .get(&url)
        .send()
        .await
        .map_err(|e| PredictionError::FetchEquityDetails(e.to_string()))?;

    let bytes = response
        .bytes()
        .await
        .map_err(|e| PredictionError::FetchEquityDetails(e.to_string()))?;

    let cursor = Cursor::new(bytes);
    let data = CsvReader::new(cursor)
        .finish()
        .map_err(|e| PredictionError::FetchEquityDetails(e.to_string()))?;

    info!(rows = data.height(), "Equity details fetched");
    Ok(data)
}

pub async fn fetch_equity_bars_from_pool_or_service(
    pool: Option<&PgPool>,
    base_url: &str,
    http_client: &reqwest::Client,
) -> Result<DataFrame, PredictionError> {
    if let Some(pool) = pool {
        info!("Fetching equity bars from PostgreSQL");
        database::query_equity_bars(pool)
            .await
            .map_err(|e| PredictionError::FetchEquityBars(e.to_string()))
    } else {
        fetch_equity_bars(base_url, http_client).await
    }
}

pub async fn fetch_equity_details_from_pool_or_service(
    pool: Option<&PgPool>,
    base_url: &str,
    http_client: &reqwest::Client,
) -> Result<DataFrame, PredictionError> {
    if let Some(pool) = pool {
        info!("Fetching equity details from PostgreSQL");
        database::query_equity_details(pool)
            .await
            .map_err(|e| PredictionError::FetchEquityDetails(e.to_string()))
    } else {
        fetch_equity_details(base_url, http_client).await
    }
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
        // Rows without a sector or industry cannot be categorically encoded;
        // the Python pipeline drops them after the join.
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

/// Inverse-scale the predicted `daily_return` quantiles and sort them so they
/// are monotonic, exactly as the Python postprocessing does (`np.sort` per
/// row). Quantile crossing is routine in quantile regression; sorting is the
/// standard rearrangement remedy.
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

/// Timestamp (UTC midnight, milliseconds) for horizon step `step`, where step 0
/// is `now`'s date — matching the Python labeling `now + timedelta(days=step)`.
pub(crate) fn step_timestamp_milliseconds(now: chrono::DateTime<Utc>, step: usize) -> i64 {
    (now + Duration::days(step as i64))
        .date_naive()
        .and_hms_opt(0, 0, 0)
        .expect("midnight is always a valid time")
        .and_utc()
        .timestamp_millis()
}

pub fn generate_predictions(
    data: DataFrame,
    model_state: &ModelState,
) -> Result<serde_json::Value, PredictionError> {
    let tide_data = Data::apply_existing_scaler(data, model_state.scaler(), model_state.mappings())
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

    let num_quantiles = model_state.parameters().quantiles().len();
    // The output schema is fixed at quantile_10/quantile_50/quantile_90, so a
    // model with any other quantile count cannot be served correctly; fail
    // loudly instead of indexing out of bounds or mislabeling values.
    if num_quantiles != 3 {
        return Err(PredictionError::Postprocessing(format!(
            "Expected exactly 3 quantiles (10/50/90); the loaded model has {num_quantiles}"
        )));
    }
    let mut results = Vec::new();

    let ticker_mapping = &model_state.mappings()["ticker"];
    let reverse_ticker_map: std::collections::HashMap<i32, &String> =
        ticker_mapping.iter().map(|(k, v)| (*v, k)).collect();

    let now = Utc::now();

    for sample_idx in 0..num_samples {
        let ticker_id = dataset.static_categorical[[sample_idx, 0, 0]];
        let ticker = reverse_ticker_map
            .get(&ticker_id)
            .map(|s| s.as_str())
            .unwrap_or("UNKNOWN");

        for t in 0..output_length {
            let base_idx = (sample_idx * output_length + t) * num_quantiles;

            let scaled: Vec<f64> = (0..num_quantiles)
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

    // Persist the final horizon step, now + (output_length - 1) days, exactly
    // like the Python service.
    let target_date = step_timestamp_milliseconds(now, output_length - 1);

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
    fn test_step_timestamp_step_zero_is_today_midnight() {
        // Python labels horizon step t as now + t days at midnight: step 0 is
        // today, and the persisted target is step output_length - 1.
        let now = chrono::DateTime::parse_from_rfc3339("2026-06-09T15:30:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let midnight = chrono::NaiveDate::from_ymd_opt(2026, 6, 9)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap()
            .and_utc()
            .timestamp_millis();
        assert_eq!(step_timestamp_milliseconds(now, 0), midnight);
        assert_eq!(
            step_timestamp_milliseconds(now, 4),
            midnight + 4 * 86_400_000
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
    fn test_step_timestamp_advances_one_day_per_step() {
        let now = chrono::DateTime::parse_from_rfc3339("2026-01-01T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let step0 = step_timestamp_milliseconds(now, 0);
        let step1 = step_timestamp_milliseconds(now, 1);
        let step7 = step_timestamp_milliseconds(now, 7);
        assert_eq!(step1 - step0, 86_400_000);
        assert_eq!(step7 - step0, 7 * 86_400_000);
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

    /// Serializes a Polars DataFrame to Parquet bytes in memory.
    fn dataframe_to_parquet_bytes(mut dataframe: DataFrame) -> Vec<u8> {
        let mut buffer = Vec::new();
        polars::prelude::ParquetWriter::new(&mut buffer)
            .finish(&mut dataframe)
            .expect("parquet serialization must succeed");
        buffer
    }

    #[tokio::test]
    async fn test_fetch_equity_bars_returns_dataframe_on_valid_parquet_response() {
        // Build a Parquet payload from a minimal equity bars DataFrame and serve
        // it from a mock HTTP server. The function must parse it and return a
        // non-error result.
        let bars_dataframe = DataFrame::new(vec![
            Column::new("ticker".into(), vec!["AAPL"]),
            Column::new("timestamp".into(), vec![1_748_908_800_000i64]),
            Column::new("open_price".into(), vec![150.0f64]),
            Column::new("high_price".into(), vec![155.0f64]),
            Column::new("low_price".into(), vec![149.0f64]),
            Column::new("close_price".into(), vec![153.0f64]),
            Column::new("volume".into(), vec![1_000_000i64]),
            Column::new("volume_weighted_average_price".into(), vec![Some(152.0f64)]),
            Column::new("sector".into(), vec!["Technology"]),
            Column::new("industry".into(), vec!["Consumer Electronics"]),
        ])
        .unwrap();
        let parquet_bytes = dataframe_to_parquet_bytes(bars_dataframe);

        let mut server = mockito::Server::new_async().await;
        let mock = server
            .mock("GET", mockito::Matcher::Any)
            .with_status(200)
            .with_header("content-type", "application/octet-stream")
            .with_body(parquet_bytes)
            .create_async()
            .await;

        let http_client = reqwest::Client::new();
        let result = fetch_equity_bars(&server.url(), &http_client).await;

        mock.assert_async().await;
        assert!(result.is_ok(), "expected Ok but got: {:?}", result.err());
        let dataframe = result.unwrap();
        assert_eq!(dataframe.height(), 1);
    }

    #[tokio::test]
    async fn test_fetch_equity_bars_returns_error_on_invalid_parquet() {
        // When the server returns bytes that are not valid Parquet, the function
        // must return a FetchEquityBars error rather than panicking.
        let mut server = mockito::Server::new_async().await;
        let mock = server
            .mock("GET", mockito::Matcher::Any)
            .with_status(200)
            .with_body(b"not parquet data".as_ref())
            .create_async()
            .await;

        let http_client = reqwest::Client::new();
        let result = fetch_equity_bars(&server.url(), &http_client).await;

        mock.assert_async().await;
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            PredictionError::FetchEquityBars(_)
        ));
    }

    #[tokio::test]
    async fn test_fetch_equity_details_returns_dataframe_on_valid_csv() {
        // A minimal CSV payload must be parsed into a DataFrame successfully.
        let csv_body = "ticker,sector,industry\nAAPL,Technology,Consumer Electronics\nGOOG,Technology,Internet\n";

        let mut server = mockito::Server::new_async().await;
        let mock = server
            .mock("GET", "/equity-details")
            .with_status(200)
            .with_header("content-type", "text/csv")
            .with_body(csv_body)
            .create_async()
            .await;

        let http_client = reqwest::Client::new();
        let result = fetch_equity_details(&server.url(), &http_client).await;

        mock.assert_async().await;
        assert!(result.is_ok(), "expected Ok but got: {:?}", result.err());
        let dataframe = result.unwrap();
        assert_eq!(dataframe.height(), 2);
        assert!(dataframe.column("ticker").is_ok());
    }

    #[tokio::test]
    async fn test_fetch_equity_details_returns_ok_on_valid_csv() {
        // A minimal valid CSV response must parse successfully and return one row.
        let csv_body = "ticker,sector,industry\nAAPL,Technology,Consumer Electronics\n";

        let mut server = mockito::Server::new_async().await;
        let mock = server
            .mock("GET", "/equity-details")
            .with_status(200)
            .with_body(csv_body)
            .create_async()
            .await;

        let http_client = reqwest::Client::new();
        let result = fetch_equity_details(&server.url(), &http_client).await;

        mock.assert_async().await;
        // A valid CSV response must return Ok.
        assert!(result.is_ok());
        assert_eq!(result.unwrap().height(), 1);
    }
}
