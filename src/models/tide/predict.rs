//! Running the TiDE model: from stored market history to rows in `equity_predictions`.
//!
//! Applies the same liquidity thresholds training does, read from [`crate::common::types`] so a
//! drift cannot train the scaler on dynamics the service never predicts.

use burn::backend::NdArray;
use chrono::{DateTime, Utc};
use polars::prelude::*;
use sqlx::PgPool;
use tracing::{info, warn};
use uuid::Uuid;

use crate::common::types::{EquityPrediction, SessionDate, Ticker};

use crate::models::tide::artifact::ModelState;
use crate::models::tide::data::{Data, DatasetKind};

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
    let bars = resolve_duplicate_bars(equity_bars)?
        .lazy()
        .filter(
            col("open_price")
                .gt(lit(0.0))
                .and(col("high_price").gt(lit(0.0)))
                .and(col("low_price").gt(lit(0.0)))
                .and(col("close_price").gt(lit(0.0))),
        )
        .collect()
        .map_err(|error| PredictionError::DataConsolidation(error.to_string()))?;

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
        .map_err(|error| PredictionError::DataConsolidation(error.to_string()))?;

    let consolidated = bars
        .join(
            &details,
            ["ticker"],
            ["ticker"],
            JoinArgs::new(JoinType::Inner),
            None,
        )
        .map_err(|error| PredictionError::DataConsolidation(error.to_string()))?;

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
        .map_err(|error| PredictionError::DataConsolidation(error.to_string()))?;

    info!(rows = selected.height(), "Data consolidated");
    Ok(selected)
}

/// Collapses repeated `(ticker, timestamp)` bars, keeping the highest-volume row of each pair.
///
/// A provider that serves two instruments under one symbol yields two bars for one session, and
/// keeping whichever arrived last picked the wrong series for BCPC and OP on every day of a
/// two-year archive. Volume separates them where price cannot: the two TPC series opened fifty
/// cents apart, while their volumes differed five-fold.
///
/// Every collapse is logged, because a silent tie-break is how the wrong instrument trained for two
/// years without anything recording that a choice had been made.
fn resolve_duplicate_bars(bars: DataFrame) -> Result<DataFrame, PredictionError> {
    let before = bars.height();

    // Ascending volume puts the row worth keeping last, which is the one `Last` then takes. Sorting
    // the frame is free downstream: `engineer_features` re-sorts by (ticker, timestamp) anyway.
    let deduplicated = bars
        .clone()
        .lazy()
        .sort(
            ["volume"],
            SortMultipleOptions::default().with_maintain_order(true),
        )
        .unique_stable(
            Some(polars::prelude::Selector::ByName {
                names: vec![PlSmallStr::from("ticker"), PlSmallStr::from("timestamp")].into(),
                strict: false,
            }),
            UniqueKeepStrategy::Last,
        )
        .collect()
        .map_err(|error| PredictionError::DataConsolidation(error.to_string()))?;

    let collapsed = before.saturating_sub(deduplicated.height());
    if collapsed > 0 {
        // Only walked when a collision actually happened, which on clean data is never.
        let affected = duplicated_tickers(&bars)?;
        warn!(
            collapsed,
            tickers = ?affected,
            "Duplicate bars for one session; kept the highest-volume row of each"
        );
    }

    Ok(deduplicated)
}

/// The tickers carrying more than one bar for the same session, sorted, for the log line above.
///
/// Sorted because `UniqueKeepStrategy::Any` promises no ordering, and a log line that names the same
/// two symbols in a different order each session cannot be diffed or alerted on.
fn duplicated_tickers(bars: &DataFrame) -> Result<Vec<String>, PredictionError> {
    let frame = bars
        .clone()
        .lazy()
        .group_by([col("ticker"), col("timestamp")])
        .agg([len().alias("bar_count")])
        .filter(col("bar_count").gt(lit(1u32)))
        .select([col("ticker")])
        .unique(None, UniqueKeepStrategy::Any)
        .collect()
        .map_err(|error| PredictionError::DataConsolidation(error.to_string()))?;

    // Propagated rather than defaulted to empty: the caller logs this list as the record that a
    // tie-break happened, so swallowing the error here would report a collapse naming no ticker,
    // which is the silence this function exists to break.
    let tickers = frame
        .column("ticker")
        .map_err(|error| PredictionError::DataConsolidation(error.to_string()))?
        .str()
        .map_err(|error| PredictionError::DataConsolidation(error.to_string()))?;
    let mut names: Vec<String> = tickers.into_iter().flatten().map(str::to_string).collect();
    names.sort();
    Ok(names)
}

/// Drops tickers whose trailing averages fall below the liquidity thresholds.
///
/// **Both bounds are inclusive**, matching [`crate::models::tide::fit::filter_training_bars`] and
/// [`crate::data::universe::LiquidityRow`]'s liquidity test. The three read the same two constants, so a
/// difference in the comparison alone is enough to reopen the train/serve gap those constants were
/// introduced to close: an exclusive test here would admit a ticker to the universe, train on it,
/// and then refuse to predict for it at exactly the threshold.
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
                .gt_eq(lit(minimum_average_close_price))
                .and(col("average_volume").gt_eq(lit(minimum_average_volume))),
        )
        .select([col("ticker")])
        .collect()
        .map_err(|error| PredictionError::DataConsolidation(error.to_string()))?;

    let filtered = data
        .lazy()
        .join(
            valid_tickers.lazy(),
            [col("ticker")],
            [col("ticker")],
            JoinArgs::new(JoinType::Semi),
        )
        .collect()
        .map_err(|error| PredictionError::DataConsolidation(error.to_string()))?;

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
        .map(|mapping| mapping.keys().cloned().collect())
        .unwrap_or_default();

    if trained_tickers.is_empty() {
        return Err(PredictionError::NoMatchingTickers);
    }

    let ticker_series = Series::new("valid_ticker".into(), &trained_tickers);

    let original_rows = data.height();
    let filtered = data
        .lazy()
        .with_column(col("ticker").cast(DataType::String).str().to_uppercase())
        .filter(col("ticker").is_in(lit(ticker_series), false))
        .collect()
        .map_err(|error| PredictionError::DataConsolidation(error.to_string()))?;

    if filtered.height() == 0 {
        return Err(PredictionError::NoMatchingTickers);
    }

    // Rows, not tickers. The frame carries one row per ticker per bar, so these counts are roughly
    // the ticker count times the lookback — reported under a `tickers` name they read as a universe
    // two orders of magnitude larger than it is.
    let filtered_rows = filtered.height();
    if original_rows != filtered_rows {
        info!(
            original_rows,
            filtered_rows,
            dropped_rows = original_rows - filtered_rows,
            trained_tickers = trained_tickers.len(),
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
    unscaled.sort_by(|left, right| left.partial_cmp(right).unwrap_or(std::cmp::Ordering::Equal));
    unscaled
}

/// Timestamp for horizon step `step`, where step 0 is the Eastern session `now` falls in.
///
/// Midnight *Eastern*, not UTC. The evaluation pass selects predictions with
/// [`crate::data::calendar::eastern_day_bounds`], and a UTC midnight sits at 20:00 the previous
/// Eastern day under daylight time -- so a UTC-stamped prediction lands in the wrong session's
/// window and the morning's inference run is never read by that day's passes.
pub(crate) fn step_timestamp(now: chrono::DateTime<Utc>, step: usize) -> DateTime<Utc> {
    SessionDate::at(now)
        .plus_calendar_days(step as i64)
        .midnight()
}

/// Runs the forward pass and returns one validated prediction per ticker for the coming session.
///
/// Returns [`EquityPrediction`] rather than JSON. The values are typed the moment they leave the
/// tensor, so the quantile ordering, the ticker format, and the instant are checked once, here,
/// instead of being flattened into a map and re-parsed by the writer.
pub fn generate_predictions(
    data: DataFrame,
    model_state: &ModelState,
    correlation_id: Uuid,
) -> Result<Vec<EquityPrediction>, PredictionError> {
    // One `now` for the whole run. The session the synthesized future row carries and the session
    // the output is stamped with are both derived from it, so the features and the label cannot
    // describe different days -- which is the shape of the bug this replaced.
    let now = Utc::now();
    let session = SessionDate::at(now);

    let tide_data =
        Data::apply_existing_scaler(data, model_state.scaler(), model_state.mappings(), session)
            .map_err(|error| PredictionError::Preprocessing(error.to_string()))?;

    let output_length = model_state.parameters().output_length();
    let dataset_input_length = model_state.parameters().input_length();
    let dataset = tide_data
        .get_dataset(DatasetKind::Predict, dataset_input_length, output_length)
        .map_err(|error| PredictionError::DatasetCreation(error.to_string()))?;

    if dataset.is_empty() {
        return Err(PredictionError::DatasetCreation(
            "No prediction samples created".to_string(),
        ));
    }

    info!(samples = dataset.len(), "Prediction dataset created");

    // Before the forward pass, not inside it: a mismatch between this data and the loaded weights
    // is an artifact problem, and it should read as one rather than as a tensor panic.
    crate::models::tide::batch::validate_input_shape(&dataset, model_state.parameters())
        .map_err(PredictionError::DatasetCreation)?;

    let device = Default::default();
    let sample_count = dataset.len();

    let indices: Vec<usize> = (0..sample_count).collect();
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
        .map_err(|error| PredictionError::Inference(format!("{error:?}")))?;

    let quantile_count = model_state.parameters().quantiles().len();
    // The output schema is fixed at quantile_10/quantile_50/quantile_90, so a
    // model with any other quantile count cannot be served correctly; fail
    // loudly instead of indexing out of bounds or mislabeling values.
    if quantile_count != 3 {
        return Err(PredictionError::Postprocessing(format!(
            "Expected exactly 3 quantiles (10/50/90); the loaded model has {quantile_count}"
        )));
    }

    // Indexing a `HashMap` panics on a missing key. An artifact without a `ticker` mapping is a
    // malformed artifact, which is a condition to report rather than one to abort the process on --
    // the pre-open handler can then fall back and record the failure in its errored payload.
    let ticker_mapping = model_state.mappings().get("ticker").ok_or_else(|| {
        PredictionError::Inference(
            "the loaded artifact has no 'ticker' categorical mapping".to_string(),
        )
    })?;
    let reverse_ticker_map: std::collections::HashMap<i32, &String> = ticker_mapping
        .iter()
        .map(|(ticker, id)| (*id, ticker))
        .collect();

    // Step 0 -- the coming close -- is the only horizon the book can act on: the pre-close
    // liquidation flattens every position the same session, so a prediction further out describes a
    // holding period this strategy never has. Selected explicitly rather than as the last step, so
    // widening `output_length` for research does not silently move the traded signal.
    let traded_session = step_timestamp(now, 0);

    let mut results = Vec::with_capacity(sample_count);
    for sample_index in 0..sample_count {
        let ticker_id = dataset.static_categorical[[sample_index, 0, 0]];
        // Reported rather than defaulted. This used to fall back to the literal string "UNKNOWN",
        // which is a valid ticker format -- so an unmappable id was stored as a prediction for a
        // symbol called UNKNOWN rather than failing.
        let raw_ticker = reverse_ticker_map.get(&ticker_id).ok_or_else(|| {
            PredictionError::Postprocessing(format!(
                "sample {sample_index} carries encoded ticker {ticker_id}, which the artifact's \
                 mapping does not name"
            ))
        })?;
        let ticker = Ticker::new(raw_ticker).ok_or_else(|| {
            PredictionError::Postprocessing(format!(
                "the artifact's ticker mapping contains {raw_ticker}, which is not a usable ticker"
            ))
        })?;

        for step in 0..output_length {
            if step != 0 {
                continue;
            }
            let timestamp = traded_session;

            let base_index = (sample_index * output_length + step) * quantile_count;
            let scaled: Vec<f64> = (0..quantile_count)
                .map(|quantile| predictions_data[base_index + quantile] as f64)
                .collect();
            let quantiles = unscale_and_sort_quantiles(&scaled, model_state.scaler());

            results.push(
                EquityPrediction::new(
                    correlation_id,
                    model_state.run_id().to_string(),
                    ticker.clone(),
                    timestamp,
                    quantiles[0],
                    quantiles[1],
                    quantiles[2],
                )
                .map_err(|error| {
                    PredictionError::Postprocessing(format!(
                        "prediction for {ticker} is invalid: {error}"
                    ))
                })?,
            );
        }
    }

    info!(count = results.len(), "Predictions generated");
    Ok(results)
}

/// Checks the two invariants that are properties of the batch rather than of a row.
///
/// Everything about a single prediction — the quantile ordering, the finiteness, the ticker format
/// — is already guaranteed by [`EquityPrediction`] and [`Ticker`] having been constructed. What a
/// value cannot know is what its neighbours look like: two rows for one `(ticker, timestamp)` would
/// be silently collapsed by the upsert's `ON CONFLICT`, and tickers stamped with different sessions
/// would leave the book comparing forecasts for different days.
pub fn validate_predictions(predictions: &[EquityPrediction]) -> Result<(), String> {
    let mut seen_pairs: std::collections::HashSet<(&Ticker, DateTime<Utc>)> =
        std::collections::HashSet::new();
    let mut reference: Option<DateTime<Utc>> = None;

    for prediction in predictions {
        if !seen_pairs.insert((prediction.ticker(), prediction.timestamp())) {
            return Err(format!(
                "Duplicate ticker/timestamp pair: {}/{}",
                prediction.ticker(),
                prediction.timestamp()
            ));
        }

        match reference {
            None => reference = Some(prediction.timestamp()),
            Some(expected) if expected != prediction.timestamp() => {
                return Err(format!(
                    "Timestamps are not consistent across all tickers: {} is stamped {} where {} was expected",
                    prediction.ticker(),
                    prediction.timestamp(),
                    expected
                ));
            }
            Some(_) => {}
        }
    }

    Ok(())
}

pub async fn insert_predictions(
    pool: &PgPool,
    predictions: &[EquityPrediction],
) -> Result<u64, sqlx::Error> {
    if predictions.is_empty() {
        return Ok(0);
    }

    let mut rows_affected: u64 = 0;
    let mut transaction = pool.begin().await?;

    for chunk in predictions.chunks(1000) {
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
/// inference failed reads nothing rather than reading yesterday's prediction as though it were
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

    /// A scaler over `daily_return` alone, which is all `unscale_and_sort_quantiles` reads.
    ///
    /// Built through the validated constructor, so a future tightening of the `Scaler` contract
    /// fails here once rather than in five separate fixtures.
    fn daily_return_scaler(
        mean: f64,
        standard_deviation: f64,
    ) -> crate::models::tide::data::Scaler {
        crate::models::tide::data::Scaler::new(
            std::collections::HashMap::from([("daily_return".to_string(), mean)]),
            std::collections::HashMap::from([("daily_return".to_string(), standard_deviation)]),
        )
        .expect("test scaler statistics must be usable")
    }

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
        assert!(tickers.iter().all(|ticker| *ticker == "GOOG"));
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
        assert!(tickers.iter().all(|ticker| *ticker == "GOOG"));
    }

    /// The threshold itself must pass, because the other two sites that read these constants let it
    /// pass. `filter_training_bars` and `LiquidityRow::is_liquid` both compare inclusively and both have
    /// a boundary test; this one had neither, and was the site that diverged. A ticker averaging
    /// exactly $10.00 was admitted to the universe, trained on, and dropped at inference.
    #[test]
    fn test_filter_equity_bars_keeps_a_ticker_exactly_at_both_thresholds() {
        use crate::common::types::{MINIMUM_CLOSE_PRICE, MINIMUM_VOLUME};

        // Two bars either side of each threshold, so the averages land on it exactly rather than
        // near it: (9 + 11)/2 = 10.00 and (500k + 1.5M)/2 = 1,000,000.
        let data = DataFrame::new(vec![
            Column::new("ticker".into(), vec!["EDGE", "EDGE"]),
            Column::new("timestamp".into(), vec![1000i64, 2000]),
            Column::new(
                "close_price".into(),
                vec![MINIMUM_CLOSE_PRICE - 1.0, MINIMUM_CLOSE_PRICE + 1.0],
            ),
            Column::new(
                "volume".into(),
                vec![(MINIMUM_VOLUME / 2.0) as i64, (MINIMUM_VOLUME * 1.5) as i64],
            ),
        ])
        .unwrap();

        let result = filter_equity_bars(data, MINIMUM_CLOSE_PRICE, MINIMUM_VOLUME).unwrap();

        assert_eq!(
            result.height(),
            2,
            "a ticker sitting exactly on both thresholds must survive inference filtering"
        );
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

    fn prediction(ticker: &str, timestamp: DateTime<Utc>) -> EquityPrediction {
        EquityPrediction::new(
            Uuid::nil(),
            "2026-08-05-00-00-00-000".to_string(),
            Ticker::new(ticker).expect("a valid test ticker"),
            timestamp,
            -0.01,
            0.0,
            0.01,
        )
        .expect("ordered quantiles must construct")
    }

    fn instant(raw: &str) -> DateTime<Utc> {
        chrono::DateTime::parse_from_rfc3339(raw)
            .expect("a valid test instant")
            .with_timezone(&Utc)
    }

    /// The row-level invariants moved into `EquityPrediction` and `Ticker`, so what is left to
    /// check is what a single value cannot know about its neighbours.
    #[test]
    fn test_a_consistent_batch_passes() {
        let session = instant("2026-08-05T04:00:00Z");
        assert!(validate_predictions(&[]).is_ok());
        assert!(validate_predictions(&[prediction("AAPL", session)]).is_ok());
        assert!(validate_predictions(&[
            prediction("AAPL", session),
            prediction("MSFT", session),
            prediction("GOOG", session),
        ])
        .is_ok());
    }

    /// The upsert's `ON CONFLICT (ticker, timestamp)` would collapse a repeated pair silently, so
    /// the second row would overwrite the first and the batch would report a row count that does
    /// not match what it was handed.
    #[test]
    fn test_a_repeated_ticker_and_session_is_refused() {
        let session = instant("2026-08-05T04:00:00Z");
        let error = validate_predictions(&[
            prediction("AAPL", session),
            prediction("MSFT", session),
            prediction("AAPL", session),
        ])
        .expect_err("a repeated pair must be refused");
        assert!(error.contains("Duplicate"), "got: {error}");
    }

    /// Every prediction in a batch describes the same session. One stamped differently would have
    /// the book comparing forecasts for two different days against one universe.
    #[test]
    fn test_a_batch_spanning_two_sessions_is_refused() {
        let error = validate_predictions(&[
            prediction("AAPL", instant("2026-08-05T04:00:00Z")),
            prediction("MSFT", instant("2026-08-06T04:00:00Z")),
        ])
        .expect_err("a split batch must be refused");
        assert!(error.contains("not consistent"), "got: {error}");
    }

    #[test]
    fn test_unscale_and_sort_quantiles_repairs_crossing() {
        let scaler = daily_return_scaler(0.0, 1.0);

        // Crossed raw quantiles (q10 > q50) must come back monotonic.
        let sorted = unscale_and_sort_quantiles(&[0.05, 0.02, 0.03], &scaler);
        assert_eq!(sorted, vec![0.02, 0.03, 0.05]);
    }

    #[test]
    fn test_predictions_are_visible_to_the_same_session_evaluation() {
        // The 09:00 Eastern run exists to feed the same session's evaluation passes, which select
        // rows with `eastern_day_bounds`. A stamp built at UTC midnight falls in the *previous*
        // Eastern day's window -- 20:00 the day before, under EDT -- so the prediction written this
        // morning is never the one read this afternoon.
        use crate::common::types::SessionDate;

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
            let stamp = step_timestamp(now, 0);
            let (start, end) = SessionDate::at(now).bounds();
            assert!(
                stamp >= start && stamp < end,
                "{day}: prediction stamped {stamp} is outside the session window [{start}, {end})"
            );
        }
    }

    #[test]
    fn test_step_timestamp_step_zero_is_the_current_eastern_session() {
        // Step t is the Eastern session t days out, stamped at Eastern midnight. 15:30Z is 11:30
        // Eastern, so step 0 is that same session rather than the one UTC is already into.
        use crate::common::types::SessionDate;

        let now = chrono::DateTime::parse_from_rfc3339("2026-06-09T15:30:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let session = SessionDate::from_date(chrono::NaiveDate::from_ymd_opt(2026, 6, 9).unwrap());
        assert_eq!(step_timestamp(now, 0), session.midnight());
        assert_eq!(
            step_timestamp(now, 4),
            session.plus_calendar_days(4).midnight()
        );
    }

    #[test]
    fn test_late_evening_eastern_still_stamps_the_current_session() {
        // 01:00Z is 21:00 the previous Eastern day. Stamping off the UTC date would jump the
        // prediction a session forward, which is the failure this function exists to avoid.
        use crate::common::types::SessionDate;

        let now = chrono::DateTime::parse_from_rfc3339("2026-06-10T01:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        assert_eq!(
            step_timestamp(now, 0),
            SessionDate::from_date(chrono::NaiveDate::from_ymd_opt(2026, 6, 9).unwrap()).midnight()
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
        // Duplicate (ticker, timestamp) pairs collapse to one row.
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

    /// The real BCPC collision from 2024-07-05, in the order the archive stored it: the thin
    /// impostor sits *after* the genuine bar, so keeping the last row selected $24.75 over
    /// $160.51 on every session of a two-year archive.
    #[test]
    fn test_consolidate_data_keeps_the_highest_volume_of_a_duplicate_pair() {
        let bars = DataFrame::new(vec![
            Column::new("ticker".into(), vec!["BCPC", "BCPC"]),
            Column::new("timestamp".into(), vec![1_720_137_600_000i64; 2]),
            Column::new("open_price".into(), vec![159.0, 24.0]),
            Column::new("high_price".into(), vec![161.0, 25.0]),
            Column::new("low_price".into(), vec![158.0, 24.0]),
            Column::new("close_price".into(), vec![160.51, 24.75]),
            Column::new("volume".into(), vec![91_611i64, 3_955]),
            Column::new("volume_weighted_average_price".into(), vec![160.0f64, 24.5]),
        ])
        .unwrap();

        let details = DataFrame::new(vec![
            Column::new("ticker".into(), vec!["BCPC"]),
            Column::new("sector".into(), vec!["Industrials"]),
            Column::new("industry".into(), vec!["Specialty Chemicals"]),
        ])
        .unwrap();

        let result = consolidate_data(bars, details).unwrap();
        assert_eq!(result.height(), 1);
        assert_eq!(
            result
                .column("close_price")
                .unwrap()
                .f64()
                .unwrap()
                .get(0)
                .unwrap(),
            160.51,
            "the genuine high-volume bar must survive, not the row that happens to be last"
        );
    }

    /// The tie-break has to be reported, or the wrong-series failure recurs with nothing recording
    /// that a choice was made.
    #[test]
    fn test_duplicated_tickers_names_only_the_colliding_symbols() {
        let bars = DataFrame::new(vec![
            Column::new("ticker".into(), vec!["TPC", "TPC", "AAPL"]),
            Column::new("timestamp".into(), vec![1000i64, 1000, 1000]),
            Column::new("volume".into(), vec![59_618i64, 303_757, 1_000]),
        ])
        .unwrap();

        assert_eq!(duplicated_tickers(&bars).unwrap(), vec!["TPC".to_string()]);
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
    fn test_unscale_and_sort_quantiles_already_sorted() {
        let scaler = daily_return_scaler(0.0, 1.0);

        // Already-sorted quantiles must come back unchanged.
        let result = unscale_and_sort_quantiles(&[0.01, 0.02, 0.03], &scaler);
        assert_eq!(result, vec![0.01, 0.02, 0.03]);
    }

    #[test]
    fn test_unscale_and_sort_quantiles_with_nonzero_mean_and_std() {
        // inverse_transform = value * std + mean
        let scaler = daily_return_scaler(0.005, 0.01);

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
        use crate::common::types::SessionDate;

        let now = chrono::DateTime::parse_from_rfc3339("2026-01-01T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let session =
            SessionDate::from_date(chrono::NaiveDate::from_ymd_opt(2025, 12, 31).unwrap());
        for step in [0usize, 1, 7] {
            assert_eq!(
                step_timestamp(now, step),
                session.plus_calendar_days(step as i64).midnight(),
                "step {step} did not land on its Eastern session midnight"
            );
        }
    }

    #[test]
    fn test_step_timestamp_crosses_daylight_saving_transitions() {
        // Eastern time springs forward 2026-03-08 and falls back 2026-11-01. Stepping across either
        // boundary must still land on the session's own midnight, which a fixed day-length stride
        // would miss by an hour in each direction.
        use crate::common::types::SessionDate;

        for (start, label) in [
            ("2026-03-06T17:00:00Z", "spring forward"),
            ("2026-10-30T17:00:00Z", "fall back"),
        ] {
            let now = chrono::DateTime::parse_from_rfc3339(start)
                .unwrap()
                .with_timezone(&Utc);
            let session = SessionDate::at(now);
            for step in 0..5usize {
                let expected = session.plus_calendar_days(step as i64).midnight();
                assert_eq!(
                    step_timestamp(now, step),
                    expected,
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
    fn test_unscale_and_sort_quantiles_single_element() {
        let scaler = daily_return_scaler(0.0, 1.0);

        let result = unscale_and_sort_quantiles(&[0.05], &scaler);
        assert_eq!(result.len(), 1);
        assert!((result[0] - 0.05).abs() < 1e-12);
    }

    #[test]
    fn test_unscale_and_sort_quantiles_empty_input() {
        let scaler = daily_return_scaler(0.0, 1.0);

        let result = unscale_and_sort_quantiles(&[], &scaler);
        assert!(result.is_empty());
    }
}
