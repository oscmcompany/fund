//! Feature engineering, scaling, categorical encoding, and windowing.
//!
//! Training fits the scaler and mappings here; inference reuses the ones stored in the artifact.
//! Both paths run the same functions, so a change to any of them changes both.

use std::collections::HashMap;

use chrono::Datelike;
use polars::prelude::*;

use crate::data::calendar::SessionDate;
use crate::data::details::UNKNOWN as UNKNOWN_SECTOR_OR_INDUSTRY;

pub type CategoryMapping = HashMap<String, i32>;
pub type FeatureMappings = HashMap<String, CategoryMapping>;

pub fn selector_by_names(names: &[&str]) -> Vec<PlSmallStr> {
    names.iter().map(|name| PlSmallStr::from(*name)).collect()
}

/// Per-column standardization statistics, fitted during training and reloaded at inference.
///
/// **Holding one is proof that every column it carries can be scaled.** [`Scaler::new`] checks that
/// each mean is finite, each standard deviation is finite and strictly positive, and that the two
/// maps name the same columns — so no column is half-described and no call site re-checks.
///
/// It is deliberately *not* proof that a particular column is present, because a scaler over one
/// column is a legitimate value: the unscale path asks only for `daily_return`. Covering every
/// entry in `CONTINUOUS_COLUMNS` is a property of an *artifact*, not of the type, and
/// [`Scaler::load`] is where it is enforced.
///
/// That guarantee is the point of the type rather than decoration. This value crosses a machine
/// boundary as a JSON file, and the failure it prevents is silent: a scaler that degrades to the
/// identity produces predictions on the wrong scale, the service reports a successful load, and
/// nothing downstream can distinguish them from good ones — the only remaining validation is
/// `EquityPrediction::new`'s quantile ordering, which wrong-scale values satisfy perfectly well.
///
/// Deliberately not `Deserialize`: deriving it would reintroduce a path into the type that skips
/// the constructor, which is the hole this closes.
#[derive(Debug, Clone)]
pub struct Scaler {
    means: HashMap<String, f64>,
    standard_deviations: HashMap<String, f64>,
}

impl Scaler {
    /// Constructs a scaler, rejecting statistics that cannot standardize a column.
    ///
    /// A non-finite mean poisons every scaled value; a zero or negative standard deviation makes
    /// the transform degenerate or sign-flipping, and its inverse meaningless.
    ///
    /// The two maps must also name the same columns. A column with a mean but no deviation would
    /// otherwise fall back to `1.0` and a column with a deviation but no mean to `0.0` — half of
    /// the identity scaling this type exists to prevent, reached through the front door.
    pub fn new(
        means: HashMap<String, f64>,
        standard_deviations: HashMap<String, f64>,
    ) -> Result<Self, String> {
        for (column, mean) in &means {
            if !mean.is_finite() {
                return Err(format!(
                    "Mean for column `{column}` is {mean}, which is not finite"
                ));
            }
            if !standard_deviations.contains_key(column) {
                return Err(format!(
                    "Column `{column}` has a mean but no standard deviation, so scaling it would \
                     silently divide by one"
                ));
            }
        }
        for (column, standard_deviation) in &standard_deviations {
            if !standard_deviation.is_finite() || *standard_deviation <= 0.0 {
                return Err(format!(
                    "Standard deviation for column `{column}` is {standard_deviation}, which cannot scale"
                ));
            }
            if !means.contains_key(column) {
                return Err(format!(
                    "Column `{column}` has a standard deviation but no mean, so scaling it would \
                     silently centre on zero"
                ));
            }
        }
        Ok(Self {
            means,
            standard_deviations,
        })
    }

    pub fn means(&self) -> &HashMap<String, f64> {
        &self.means
    }

    pub fn standard_deviations(&self) -> &HashMap<String, f64> {
        &self.standard_deviations
    }

    /// Reads a scaler from a training artifact, rejecting anything it cannot verify.
    ///
    /// **Every branch here refuses rather than defaults.** The previous version replaced an
    /// unparsable `means` entry with `0.0`, an unparsable `standard_deviations` entry with `1.0`,
    /// and a missing object with an empty map — which is exactly identity scaling, reported as a
    /// successful load.
    ///
    /// The column lists the artifact was fitted with are checked against this build's constants and
    /// then dropped. Checking them is the whole of their value: an artifact fitted on a different
    /// column set would otherwise load without complaint and scale a different set than the weights
    /// were trained on. Carrying them further served nothing — no caller read them.
    pub fn load(path: &std::path::Path) -> Result<Self, Box<dyn std::error::Error>> {
        let display = path.display();
        let content = std::fs::read_to_string(path)?;
        let raw: serde_json::Value = serde_json::from_str(&content)?;

        let statistics = |field: &str| -> Result<HashMap<String, f64>, Box<dyn std::error::Error>> {
            let object = raw[field]
                .as_object()
                .ok_or_else(|| format!("Scaler artifact at {display} has no `{field}` object"))?;
            object
                .iter()
                .map(|(column, value)| {
                    value.as_f64().map(|number| (column.clone(), number)).ok_or_else(|| {
                        format!(
                            "Scaler artifact at {display} has a non-numeric `{field}` entry for column `{column}`"
                        )
                        .into()
                    })
                })
                .collect()
        };

        let means = statistics("means")?;
        let standard_deviations = statistics("standard_deviations")?;

        for (field, expected) in [
            ("continuous_columns", CONTINUOUS_COLUMNS),
            ("categorical_columns", CATEGORICAL_COLUMNS),
            ("static_categorical_columns", STATIC_CATEGORICAL_COLUMNS),
        ] {
            let array = raw[field]
                .as_array()
                .ok_or_else(|| format!("Scaler artifact at {display} has no `{field}` list"))?;
            // Every element must be a string. Skipping the ones that are not would let
            // `["close_price", 42]` collapse to a list that compares equal to a shorter expected
            // one — a malformed artifact passing the check written to catch malformed artifacts.
            let found: Vec<&str> = array
                .iter()
                .map(|value| {
                    value.as_str().ok_or_else(|| {
                        format!(
                            "Scaler artifact at {display} has a non-string entry {value} in `{field}`"
                        )
                    })
                })
                .collect::<Result<_, String>>()?;
            if found != expected {
                return Err(format!(
                    "Scaler artifact at {display} was fitted on {field} {found:?}, but this build \
                     expects {expected:?}"
                )
                .into());
            }
        }

        // The scaler is fitted over every continuous column, so a missing one means the artifact
        // and this build disagree about the feature set even though the column list matched.
        for column in CONTINUOUS_COLUMNS {
            if !means.contains_key(*column) || !standard_deviations.contains_key(*column) {
                return Err(format!(
                    "Scaler artifact at {display} has no statistics for continuous column `{column}`"
                )
                .into());
            }
        }

        Self::new(means, standard_deviations)
            .map_err(|reason| format!("Scaler artifact at {display} is unusable: {reason}").into())
    }

    /// Maps a scaled value back to its original units.
    ///
    /// Falls back to the identity for a column the scaler does not carry, which is reachable only
    /// for a column outside `CONTINUOUS_COLUMNS` — [`Scaler::load`] rejects an artifact missing any
    /// of those.
    pub fn inverse_transform_value(&self, column: &str, value: f64) -> f64 {
        let mean = self.means.get(column).copied().unwrap_or(0.0);
        let standard_deviation = self.standard_deviations.get(column).copied().unwrap_or(1.0);
        value * standard_deviation + mean
    }
}

pub struct TrainingDataset {
    pub past_continuous: ndarray::Array3<f32>,
    pub past_categorical: ndarray::Array3<i32>,
    pub future_categorical: ndarray::Array3<i32>,
    pub static_categorical: ndarray::Array3<i32>,
    pub targets: Option<ndarray::Array3<f32>>,
}

impl TrainingDataset {
    pub fn len(&self) -> usize {
        self.past_continuous.shape()[0]
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

pub(crate) const CONTINUOUS_COLUMNS: &[&str] = &[
    "open_price",
    "high_price",
    "low_price",
    "close_price",
    "volume",
    "volume_weighted_average_price",
    "daily_return",
];

pub(crate) const CATEGORICAL_COLUMNS: &[&str] = &[
    "day_of_week",
    "day_of_month",
    "day_of_year",
    "month",
    "year",
];

pub(crate) const STATIC_CATEGORICAL_COLUMNS: &[&str] = &["ticker", "sector", "industry"];

/// The model target is the future window of `daily_return`, which is the last
/// continuous column. Fitting and windowing index into this position.
pub(crate) const TARGET_COLUMN: &str = "daily_return";

/// Flattened model input width for the given window lengths: past continuous +
/// past categorical + future categorical + static features.
pub fn input_feature_size(input_length: usize, output_length: usize) -> usize {
    input_length * CONTINUOUS_COLUMNS.len()
        + input_length * CATEGORICAL_COLUMNS.len()
        + output_length * CATEGORICAL_COLUMNS.len()
        + STATIC_CATEGORICAL_COLUMNS.len()
}

/// The fraction of the observed time range that goes to **training**; the remainder validates.
///
/// Named for the side it measures rather than for the split it defines. "Validation split" is
/// ambiguous across the field — some libraries mean the held-out fraction by it — and a type whose
/// name reads backwards is worse than a bare `f64`, because `TrainingFraction::new(0.2)` looks
/// deliberate. At 0.2 this trains on a fifth of the window and every guard still passes.
///
/// Strictly between 0 and 1. At either endpoint one side of the split is empty, and an empty side
/// is not an error anywhere downstream — [`window_frame`] simply yields no windows, training runs
/// on nothing or validates against nothing, and the run reports a loss computed over zero rows.
/// Rejecting the endpoint here is the only place that failure is still legible.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct TrainingFraction(f64);

impl TrainingFraction {
    pub fn new(fraction: f64) -> Result<Self, String> {
        if !fraction.is_finite() || fraction <= 0.0 || fraction >= 1.0 {
            return Err(format!(
                "the training fraction must be strictly between 0 and 1, got {fraction}"
            ));
        }
        Ok(Self(fraction))
    }

    fn fraction(self) -> f64 {
        self.0
    }
}

/// Which dataset [`Data::get_dataset`] should window out of the engineered frame.
///
/// The split rides on the two variants that consume it rather than on the call, because inference
/// has no meaningful value to pass: it reads the whole frame. The previous `&str` selector took a
/// split regardless, so the predict call site carried a `0.8` that did nothing — and, worse, its
/// `_` arm turned any unrecognized string into training data rather than an error.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum DatasetKind {
    /// One window per ticker at the end of its series, no targets.
    Predict,
    /// All sliding windows at or before the cutoff, with the future `daily_return` as targets.
    Train(TrainingFraction),
    /// All sliding windows after the cutoff, with the future `daily_return` as targets.
    Validate(TrainingFraction),
}

pub struct Data {
    pub data: DataFrame,
    pub scaler: Scaler,
    pub mappings: FeatureMappings,
    pub continuous_columns: Vec<String>,
    pub categorical_columns: Vec<String>,
    pub static_categorical_columns: Vec<String>,
}

impl Data {
    /// Wrap an already scaled-and-encoded DataFrame with the scaler/mappings that
    /// produced it. Used by [`crate::models::tide::fit`] after fitting.
    pub fn from_parts(data: DataFrame, scaler: Scaler, mappings: FeatureMappings) -> Self {
        Self {
            data,
            scaler,
            mappings,
            continuous_columns: CONTINUOUS_COLUMNS
                .iter()
                .map(|name| name.to_string())
                .collect(),
            categorical_columns: CATEGORICAL_COLUMNS
                .iter()
                .map(|name| name.to_string())
                .collect(),
            static_categorical_columns: STATIC_CATEGORICAL_COLUMNS
                .iter()
                .map(|name| name.to_string())
                .collect(),
        }
    }

    /// Inference-only preparation: the training path fits its own scaler and mappings instead.
    ///
    /// `target_session` is the session being forecast. It exists because inference has no bar for
    /// that session yet — see [`append_forecast_session_rows`] — and it must be the same session the
    /// caller stamps the output with, or the features and the label describe different days.
    pub fn apply_existing_scaler(
        data: DataFrame,
        scaler: &Scaler,
        mappings: &FeatureMappings,
        target_session: SessionDate,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let data = append_forecast_session_rows(data, target_session)?;
        let data = engineer_features(data)?;
        let data = clean_data(data)?;
        let data = apply_scaling(data, scaler)?;
        let data = encode_categoricals(data, mappings)?;

        Ok(Self::from_parts(data, scaler.clone(), mappings.clone()))
    }

    /// Split the engineered frame into (train, validate) by a global date cutoff at
    /// `min + (max - min) * training_fraction`. Rows at or before the cutoff are training.
    pub fn split_by_timestamp(
        &self,
        training_fraction: TrainingFraction,
    ) -> Result<(DataFrame, DataFrame), Box<dyn std::error::Error>> {
        let cutoff = training_cutoff(&self.data, training_fraction)?;
        split_at_cutoff(&self.data, cutoff)
    }

    /// Build a windowed dataset.
    pub fn get_dataset(
        &self,
        kind: DatasetKind,
        input_length: usize,
        output_length: usize,
    ) -> Result<TrainingDataset, Box<dyn std::error::Error>> {
        match kind {
            DatasetKind::Predict => {
                window_frame(&self.data, input_length, output_length, true, false)
            }
            DatasetKind::Validate(training_fraction) => {
                let (_, validation) = self.split_by_timestamp(training_fraction)?;
                window_frame(&validation, input_length, output_length, false, true)
            }
            DatasetKind::Train(training_fraction) => {
                let (train, _) = self.split_by_timestamp(training_fraction)?;
                window_frame(&train, input_length, output_length, false, true)
            }
        }
    }
}

/// The timestamp at or before which a row belongs to the training side of the split:
/// `min + (max - min) * training_fraction`.
///
/// Shared with [`crate::models::tide::fit::fit`], which fits the scaler on the rows this selects
/// while [`Data::split_by_timestamp`] later selects the rows the model trains on. The two must name
/// the same instant — statistics fitted over a window other than the one they scale are the
/// look-ahead this function exists to prevent — so the arithmetic lives here once rather than at
/// both call sites.
///
/// A `TrainingFraction` is strictly between 0 and 1, so the cutoff never falls below the minimum
/// timestamp: the training side is non-empty for any non-empty frame.
pub(crate) fn training_cutoff(
    data: &DataFrame,
    training_fraction: TrainingFraction,
) -> Result<i64, Box<dyn std::error::Error>> {
    let timestamps = data
        .column("timestamp")
        .map_err(|error| error.to_string())?;
    let timestamps = timestamps.i64().map_err(|error| error.to_string())?;
    let minimum_timestamp = timestamps.min().unwrap_or(0);
    let maximum_timestamp = timestamps.max().unwrap_or(0);
    Ok(minimum_timestamp
        + (((maximum_timestamp - minimum_timestamp) as f64) * training_fraction.fraction()) as i64)
}

/// Partition a frame at `cutoff` into (at or before, after).
pub(crate) fn split_at_cutoff(
    data: &DataFrame,
    cutoff: i64,
) -> Result<(DataFrame, DataFrame), Box<dyn std::error::Error>> {
    let timestamps = data
        .column("timestamp")
        .map_err(|error| error.to_string())?;
    let timestamps = timestamps.i64().map_err(|error| error.to_string())?;
    let train = data
        .filter(&timestamps.lt_eq(cutoff))
        .map_err(|error| error.to_string())?;
    let validation = data
        .filter(&timestamps.gt(cutoff))
        .map_err(|error| error.to_string())?;
    Ok((train, validation))
}

/// Core windowing over a single (already preprocessed) frame.
///
/// `predict_mode` keeps only the final window per ticker; `with_targets`
/// additionally extracts the future `daily_return` window as the target.
fn window_frame(
    frame: &DataFrame,
    input_length: usize,
    output_length: usize,
    predict_mode: bool,
    with_targets: bool,
) -> Result<TrainingDataset, Box<dyn std::error::Error>> {
    let window_size = input_length + output_length;
    let continuous_feature_count = CONTINUOUS_COLUMNS.len();
    let categorical_feature_count = CATEGORICAL_COLUMNS.len();
    let static_feature_count = STATIC_CATEGORICAL_COLUMNS.len();
    let target_index = CONTINUOUS_COLUMNS
        .iter()
        .position(|column| *column == TARGET_COLUMN)
        .expect("daily_return must be a continuous column");

    // `ticker` is integer-encoded by this point (encode_categoricals). Group by
    // the encoded id; sorted for deterministic sample ordering.
    let mut tickers: Vec<i32> = frame
        .column("ticker")
        .map_err(|error| error.to_string())?
        .i32()
        .map_err(|error| error.to_string())?
        .into_no_null_iter()
        .collect::<std::collections::HashSet<_>>()
        .into_iter()
        .collect();
    tickers.sort_unstable();

    let mut all_past_continuous: Vec<Vec<f32>> = Vec::new();
    let mut all_past_categorical: Vec<Vec<i32>> = Vec::new();
    let mut all_future_categorical: Vec<Vec<i32>> = Vec::new();
    let mut all_static_categorical: Vec<Vec<i32>> = Vec::new();
    let mut all_targets: Vec<Vec<f32>> = Vec::new();

    for ticker in &tickers {
        let mask = frame
            .column("ticker")
            .map_err(|error| error.to_string())?
            .i32()
            .map_err(|error| error.to_string())?
            .equal(*ticker);
        let ticker_data = frame.filter(&mask).map_err(|error| error.to_string())?;

        if ticker_data.height() < window_size {
            continue;
        }

        let continuous_column_values = get_float_columns(&ticker_data, CONTINUOUS_COLUMNS)?;
        let categorical_column_values = get_int_columns(&ticker_data, CATEGORICAL_COLUMNS)?;
        let static_column_values = get_int_columns(&ticker_data, STATIC_CATEGORICAL_COLUMNS)?;

        let windows: Vec<usize> = if predict_mode {
            vec![ticker_data.height() - window_size]
        } else {
            (0..=ticker_data.height() - window_size).collect()
        };

        for start in windows {
            let mut past_continuous_window =
                Vec::with_capacity(input_length * continuous_feature_count);
            for row in start..start + input_length {
                for column in &continuous_column_values {
                    past_continuous_window.push(column[row]);
                }
            }

            let mut past_categorical_window =
                Vec::with_capacity(input_length * categorical_feature_count);
            for row in start..start + input_length {
                for column in &categorical_column_values {
                    past_categorical_window.push(column[row]);
                }
            }

            let mut future_categorical_window =
                Vec::with_capacity(output_length * categorical_feature_count);
            for row in (start + input_length)..(start + input_length + output_length) {
                for column in &categorical_column_values {
                    future_categorical_window.push(column[row]);
                }
            }

            let mut static_categorical_window = Vec::with_capacity(static_feature_count);
            for column in &static_column_values {
                static_categorical_window.push(column[start]);
            }

            all_past_continuous.push(past_continuous_window);
            all_past_categorical.push(past_categorical_window);
            all_future_categorical.push(future_categorical_window);
            all_static_categorical.push(static_categorical_window);

            if with_targets {
                let returns = &continuous_column_values[target_index];
                let future = (start + input_length)..(start + input_length + output_length);
                all_targets.push(returns[future].to_vec());
            }
        }
    }

    let sample_count = all_past_continuous.len();

    let past_continuous = if sample_count > 0 {
        let flat: Vec<f32> = all_past_continuous.into_iter().flatten().collect();
        ndarray::Array3::from_shape_vec(
            (sample_count, input_length, continuous_feature_count),
            flat,
        )?
    } else {
        ndarray::Array3::zeros((0, input_length, continuous_feature_count))
    };

    let past_categorical = if sample_count > 0 {
        let flat: Vec<i32> = all_past_categorical.into_iter().flatten().collect();
        ndarray::Array3::from_shape_vec(
            (sample_count, input_length, categorical_feature_count),
            flat,
        )?
    } else {
        ndarray::Array3::zeros((0, input_length, categorical_feature_count))
    };

    let future_categorical = if sample_count > 0 {
        let flat: Vec<i32> = all_future_categorical.into_iter().flatten().collect();
        ndarray::Array3::from_shape_vec(
            (sample_count, output_length, categorical_feature_count),
            flat,
        )?
    } else {
        ndarray::Array3::zeros((0, output_length, categorical_feature_count))
    };

    let static_categorical = if sample_count > 0 {
        let flat: Vec<i32> = all_static_categorical.into_iter().flatten().collect();
        ndarray::Array3::from_shape_vec((sample_count, 1, static_feature_count), flat)?
    } else {
        ndarray::Array3::zeros((0, 1, static_feature_count))
    };

    let targets = if with_targets && sample_count > 0 {
        let flat: Vec<f32> = all_targets.into_iter().flatten().collect();
        Some(ndarray::Array3::from_shape_vec(
            (sample_count, output_length, 1),
            flat,
        )?)
    } else if with_targets {
        Some(ndarray::Array3::zeros((0, output_length, 1)))
    } else {
        None
    };

    Ok(TrainingDataset {
        past_continuous,
        past_categorical,
        future_categorical,
        static_categorical,
        targets,
    })
}

/// Append one row per ticker carrying `target_session`, so the windowing has a future step to spend
/// that is not a real bar.
///
/// [`window_frame`] splits one contiguous block of `input_length + output_length` rows into a past
/// and a future half. Without this the newest bar is spent as the future calendar step, which both
/// feeds the model the wrong session's calendar and pushes the most recent close out of the past
/// window — so a pre-open run forecasts the session that already closed.
///
/// The appended row is the ticker's own last bar with `timestamp` moved to the target session.
/// Copying it rather than inventing values means `close_price` equals the previous close, so the
/// engineered `daily_return` is `0.0` and survives [`clean_data`]'s non-finite filter, and sector
/// and industry carry over. None of those prices reach the model: the future half consumes only
/// [`CATEGORICAL_COLUMNS`], and the extra row shifts the window so the past half lands on real bars.
/// Calendar fields are left to [`engineer_features`] so only one place derives them.
///
/// Tickers already holding a bar at or after the target session are skipped, so a late run cannot
/// duplicate a real row.
pub(crate) fn append_forecast_session_rows(
    data: DataFrame,
    target_session: SessionDate,
) -> Result<DataFrame, Box<dyn std::error::Error>> {
    let target_milliseconds = target_session.midnight().timestamp_millis();

    let sorted = data
        .sort(
            ["ticker", "timestamp"],
            SortMultipleOptions::default().with_maintain_order(true),
        )
        .map_err(|error| error.to_string())?;

    let tickers: Vec<&str> = sorted
        .column("ticker")
        .map_err(|error| error.to_string())?
        .str()
        .map_err(|error| error.to_string())?
        .into_no_null_iter()
        .collect();
    let timestamps: Vec<i64> = sorted
        .column("timestamp")
        .map_err(|error| error.to_string())?
        .i64()
        .map_err(|error| error.to_string())?
        .into_no_null_iter()
        .collect();

    if tickers.len() != sorted.height() || timestamps.len() != sorted.height() {
        return Err("Equity bars contain null ticker or timestamp values"
            .to_string()
            .into());
    }

    // Rows are sorted, so a ticker's last row is also its newest. Comparing that against the target
    // both finds the row to copy and skips any ticker already carrying the target session.
    let mut source_rows: Vec<polars::prelude::IdxSize> = Vec::new();
    for index in 0..sorted.height() {
        let is_last_for_ticker =
            index + 1 == sorted.height() || tickers[index + 1] != tickers[index];
        if is_last_for_ticker && timestamps[index] < target_milliseconds {
            source_rows.push(index as polars::prelude::IdxSize);
        }
    }

    if source_rows.is_empty() {
        return Ok(sorted);
    }

    let source_index = IdxCa::from_vec("index".into(), source_rows);
    let mut forecast_rows = sorted
        .take(&source_index)
        .map_err(|error| error.to_string())?;
    forecast_rows
        .with_column(Column::new(
            "timestamp".into(),
            vec![target_milliseconds; forecast_rows.height()],
        ))
        .map_err(|error| error.to_string())?;

    let mut combined = sorted;
    combined
        .vstack_mut(&forecast_rows)
        .map_err(|error| error.to_string())?;
    Ok(combined)
}

pub(crate) fn engineer_features(data: DataFrame) -> Result<DataFrame, Box<dyn std::error::Error>> {
    // Sort by [ticker, timestamp] so daily returns and the downstream windowing
    // are chronological and contiguous within each ticker, independent of the
    // order rows arrived in. Each ticker's first row gets a null return; clean_data drops it.
    let data = data
        .sort(
            ["ticker", "timestamp"],
            SortMultipleOptions::default().with_maintain_order(true),
        )
        .map_err(|error| error.to_string())?;

    let timestamps = data
        .column("timestamp")
        .map_err(|error| error.to_string())?;
    let height = data.height();

    let mut day_of_week = Vec::with_capacity(height);
    let mut day_of_month = Vec::with_capacity(height);
    let mut day_of_year = Vec::with_capacity(height);
    let mut month = Vec::with_capacity(height);
    let mut year = Vec::with_capacity(height);
    let mut daily_return: Vec<Option<f32>> = Vec::with_capacity(height);

    let close_prices: Vec<f64> = data
        .column("close_price")
        .map_err(|error| error.to_string())?
        .f64()
        .map_err(|error| error.to_string())?
        .into_no_null_iter()
        .collect();

    let tickers: Vec<String> = data
        .column("ticker")
        .map_err(|error| error.to_string())?
        .str()
        .map_err(|error| error.to_string())?
        .into_no_null_iter()
        .map(|name| name.to_string())
        .collect();

    let timestamp_values: Vec<i64> = timestamps
        .i64()
        .map_err(|error| error.to_string())?
        .into_no_null_iter()
        .collect();

    // The no-null iterators above silently skip nulls, which would misalign
    // the per-row zip below; fail fast with a clear message instead.
    if close_prices.len() != height || tickers.len() != height || timestamp_values.len() != height {
        let message = format!(
            "Equity bars contain null ticker, timestamp, or close_price values \
             ({} rows; {} tickers, {} timestamps, {} close prices)",
            height,
            tickers.len(),
            timestamp_values.len(),
            close_prices.len()
        );
        return Err(message.into());
    }

    for (index, &timestamp_milliseconds) in timestamp_values.iter().enumerate() {
        let instant = chrono::DateTime::from_timestamp_millis(timestamp_milliseconds)
            .unwrap_or_else(|| chrono::DateTime::from_timestamp(0, 0).unwrap());
        // Through `SessionDate`, not `date_naive()`. These are the covariates describing the
        // trading day a bar belongs to, so they must come from the Eastern date. Reading the UTC
        // date agreed only because a daily bar is stamped at Eastern midnight, which lands in the
        // morning of the same UTC day -- a coincidence of the offset's sign that would reverse if
        // the stamp ever moved later in the session.
        let date = SessionDate::at(instant).date();

        // Monday = 1 .. Sunday = 7, per polars `dt.weekday()`.
        day_of_week.push(date.weekday().number_from_monday() as i32);
        day_of_month.push(date.day() as i32);
        day_of_year.push(date.ordinal() as i32);
        month.push(date.month() as i32);
        year.push(date.year());

        let same_ticker = index > 0 && tickers[index] == tickers[index - 1];
        if same_ticker && close_prices[index - 1] != 0.0 {
            daily_return.push(Some(
                ((close_prices[index] / close_prices[index - 1]) - 1.0) as f32,
            ));
        } else {
            daily_return.push(None);
        }
    }

    let mut new_data = data.clone();
    new_data
        .with_column(Column::new("day_of_week".into(), day_of_week))
        .map_err(|error| error.to_string())?;
    new_data
        .with_column(Column::new("day_of_month".into(), day_of_month))
        .map_err(|error| error.to_string())?;
    new_data
        .with_column(Column::new("day_of_year".into(), day_of_year))
        .map_err(|error| error.to_string())?;
    new_data
        .with_column(Column::new("month".into(), month))
        .map_err(|error| error.to_string())?;
    new_data
        .with_column(Column::new("year".into(), year))
        .map_err(|error| error.to_string())?;
    new_data
        .with_column(Column::new("daily_return".into(), daily_return))
        .map_err(|error| error.to_string())?;

    Ok(new_data)
}

pub(crate) fn clean_data(mut data: DataFrame) -> Result<DataFrame, Box<dyn std::error::Error>> {
    // A row with no ticker cannot be attributed to an instrument, and substituting a placeholder
    // would make it indistinguishable from a real symbol of the same spelling. Reject it the way
    // `engineer_features` does rather than encoding one and filtering it back out.
    let tickers = data
        .column("ticker")
        .map_err(|error| error.to_string())?
        .str()
        .map_err(|error| error.to_string())?;
    if tickers.null_count() > 0 {
        return Err(format!(
            "Equity bars contain {} null ticker values out of {} rows",
            tickers.null_count(),
            data.height()
        )
        .into());
    }

    // Uppercase ticker, sector, industry columns in-place
    let ticker_upper: Vec<String> = tickers
        .into_no_null_iter()
        .map(|value| value.to_uppercase())
        .collect();

    let sector_upper: Vec<String> = data
        .column("sector")
        .map_err(|error| error.to_string())?
        .str()
        .map_err(|error| error.to_string())?
        .into_iter()
        .map(|value| value.unwrap_or(UNKNOWN_SECTOR_OR_INDUSTRY).to_uppercase())
        .collect();

    let industry_upper: Vec<String> = data
        .column("industry")
        .map_err(|error| error.to_string())?
        .str()
        .map_err(|error| error.to_string())?
        .into_iter()
        .map(|value| value.unwrap_or(UNKNOWN_SECTOR_OR_INDUSTRY).to_uppercase())
        .collect();

    data.with_column(Column::new("ticker".into(), ticker_upper))
        .map_err(|error| error.to_string())?;
    data.with_column(Column::new("sector".into(), sector_upper))
        .map_err(|error| error.to_string())?;
    data.with_column(Column::new("industry".into(), industry_upper))
        .map_err(|error| error.to_string())?;

    let cleaned = data;

    // Drop rows with a null or non-finite value in any continuous column —
    // each ticker's first observation (null return), missing vendor fields
    // such as volume_weighted_average_price, and any division artifacts. Downstream
    // scaling and windowing iterate these columns assuming they are dense and finite.
    let mut keep_row = vec![true; cleaned.height()];
    for column in CONTINUOUS_COLUMNS {
        let values = cleaned
            .column(column)
            .map_err(|error| error.to_string())?
            .cast(&DataType::Float64)
            .map_err(|error| error.to_string())?;
        let values = values.f64().map_err(|error| error.to_string())?;
        for (index, value) in values.into_iter().enumerate() {
            if !value.is_some_and(f64::is_finite) {
                keep_row[index] = false;
            }
        }
    }
    let finite_mask: BooleanChunked = keep_row.into_iter().collect();
    let cleaned = cleaned
        .filter(&finite_mask)
        .map_err(|error| error.to_string())?;
    Ok(cleaned)
}

pub(crate) fn apply_scaling(
    data: DataFrame,
    scaler: &Scaler,
) -> Result<DataFrame, Box<dyn std::error::Error>> {
    let mut result = data;
    for column_name in CONTINUOUS_COLUMNS {
        let mean = scaler.means().get(*column_name).copied().unwrap_or(0.0);
        // `Scaler::new` rejects a non-positive standard deviation, so no zero guard is needed here.
        let standard_deviation = scaler
            .standard_deviations()
            .get(*column_name)
            .copied()
            .unwrap_or(1.0);

        let values: Vec<f32> = result
            .column(column_name)
            .map_err(|error| error.to_string())?
            .cast(&DataType::Float64)
            .map_err(|error| error.to_string())?
            .f64()
            .map_err(|error| error.to_string())?
            .into_no_null_iter()
            .map(|value| ((value - mean) / standard_deviation) as f32)
            .collect();

        result
            .with_column(Column::new((*column_name).into(), values))
            .map_err(|error| error.to_string())?;
    }
    Ok(result)
}

pub(crate) fn encode_categoricals(
    data: DataFrame,
    mappings: &FeatureMappings,
) -> Result<DataFrame, Box<dyn std::error::Error>> {
    let mut result = data;

    let all_categorical: Vec<&str> = CATEGORICAL_COLUMNS
        .iter()
        .chain(STATIC_CATEGORICAL_COLUMNS.iter())
        .copied()
        .collect();

    for column_name in all_categorical {
        if let Some(mapping) = mappings.get(column_name) {
            let values: Vec<Option<i32>> = result
                .column(column_name)
                .map_err(|error| error.to_string())?
                .str()
                .map(|chunked| {
                    chunked
                        .into_iter()
                        .map(|value| value.and_then(|text| mapping.get(text).copied()))
                        .collect()
                })
                .or_else(|_| {
                    result
                        .column(column_name)
                        .map_err(|error| error.to_string())?
                        .i32()
                        .map(|chunked| chunked.into_iter().collect())
                        .map_err(|error| error.to_string())
                })?;

            // A sentinel merges every unmapped value into one id. For the static columns that
            // includes `ticker`, and `window_frame` groups rows by the encoded ticker id -- so
            // every unseen ticker would form a single group whose windows span unrelated
            // instruments, with static features taken from whichever row began the window. The
            // model would receive fabricated price histories and the predictions would be
            // attributed to the wrong symbol, silently.
            //
            // `filter_to_trained_tickers` removes unmapped tickers before this on the prediction
            // path, but that guard lives in another module and nothing enforces the ordering, so
            // the invariant is also enforced here where it is relied upon.
            let is_static = STATIC_CATEGORICAL_COLUMNS.contains(&column_name);
            let unmapped = values.iter().filter(|value| value.is_none()).count();
            let keep: BooleanChunked = values.iter().map(|value| value.is_some()).collect();
            let encoded: Vec<i32> = values
                .into_iter()
                .map(|value| value.unwrap_or(-1))
                .collect();
            result
                .with_column(Column::new(column_name.into(), encoded))
                .map_err(|error| error.to_string())?;

            if is_static && unmapped > 0 {
                result = result.filter(&keep).map_err(|error| error.to_string())?;
                tracing::warn!(
                    column = column_name,
                    dropped_rows = unmapped,
                    "Dropped rows whose static categorical value is absent from the training mapping"
                );
            }
        }
    }

    Ok(result)
}

/// Rejects a column carrying nulls before its values are read positionally.
///
/// `into_no_null_iter` does not skip nulls and does not check for them: it reads the raw value
/// sitting in the null slot, which is whatever the buffer happens to hold — `0.0` in practice, but
/// that is an implementation detail rather than a guarantee. The count therefore matches the frame
/// and nothing fails, so a null `close_price` is scaled and fed to the model as though someone had
/// observed it.
///
/// **A silent wrong value, not a crash.** Checking the extracted length instead would be inert,
/// because the length is never wrong. The null count is the only thing that carries the
/// information.
///
/// The same family of trap produced a real bug fixed in #1035, where a null `sector` put later rows
/// on the wrong ticker.
fn reject_null_column(
    column: &Column,
    column_name: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let nulls = column.null_count();
    if nulls > 0 {
        return Err(format!(
            "Column `{column_name}` has {nulls} null value(s) in {} rows; reading it positionally \
             would substitute the raw buffer value and treat it as a real observation",
            column.len()
        )
        .into());
    }
    Ok(())
}

fn get_float_columns(
    data: &DataFrame,
    columns: &[&str],
) -> Result<Vec<Vec<f32>>, Box<dyn std::error::Error>> {
    let mut result = Vec::new();
    for column_name in columns {
        let cast = data
            .column(column_name)
            .map_err(|error| error.to_string())?
            .cast(&DataType::Float32)
            .map_err(|error| error.to_string())?;
        reject_null_column(&cast, column_name)?;
        let values: Vec<f32> = cast
            .f32()
            .map_err(|error| error.to_string())?
            .into_no_null_iter()
            .collect();
        result.push(values);
    }
    Ok(result)
}

fn get_int_columns(
    data: &DataFrame,
    columns: &[&str],
) -> Result<Vec<Vec<i32>>, Box<dyn std::error::Error>> {
    let mut result = Vec::new();
    for column_name in columns {
        let cast = data
            .column(column_name)
            .map_err(|error| error.to_string())?
            .cast(&DataType::Int32)
            .map_err(|error| error.to_string())?;
        reject_null_column(&cast, column_name)?;
        let values: Vec<i32> = cast
            .i32()
            .map_err(|error| error.to_string())?
            .into_no_null_iter()
            .collect();
        result.push(values);
    }
    Ok(result)
}

#[cfg(test)]
mod scaler_boundary_tests {
    use super::*;

    fn statistics(value: f64) -> HashMap<String, f64> {
        HashMap::from([("close_price".to_string(), value)])
    }

    fn well_formed_artifact() -> serde_json::Value {
        let entries: HashMap<&str, f64> = CONTINUOUS_COLUMNS
            .iter()
            .map(|column| (*column, 1.0))
            .collect();
        serde_json::json!({
            "means": entries,
            "standard_deviations": entries,
            "continuous_columns": CONTINUOUS_COLUMNS,
            "categorical_columns": CATEGORICAL_COLUMNS,
            "static_categorical_columns": STATIC_CATEGORICAL_COLUMNS,
        })
    }

    fn load_artifact(artifact: &serde_json::Value) -> Result<Scaler, String> {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("tide_data_scaler.json");
        std::fs::write(&path, serde_json::to_string(artifact).unwrap()).unwrap();
        Scaler::load(&path).map_err(|error| error.to_string())
    }

    #[test]
    fn test_a_well_formed_artifact_loads() {
        assert!(load_artifact(&well_formed_artifact()).is_ok());
    }

    /// A standard deviation of zero divides, and a negative one flips the sign of every scaled
    /// value while still looking like a number.
    #[test]
    fn test_new_rejects_a_standard_deviation_that_cannot_scale() {
        for unusable in [0.0, -1.0, f64::NAN, f64::INFINITY] {
            assert!(
                Scaler::new(statistics(0.0), statistics(unusable)).is_err(),
                "a standard deviation of {unusable} must be refused"
            );
        }
        assert!(Scaler::new(statistics(0.0), statistics(1e-8)).is_ok());
    }

    #[test]
    fn test_new_rejects_a_non_finite_mean() {
        assert!(Scaler::new(statistics(f64::NAN), statistics(1.0)).is_err());
        assert!(Scaler::new(statistics(f64::NEG_INFINITY), statistics(1.0)).is_err());
    }

    /// Half a description is the identity scaling this type exists to prevent, reached through the
    /// constructor rather than the loader: a missing deviation divides by one, a missing mean
    /// centres on zero.
    #[test]
    fn test_new_rejects_a_column_described_by_only_one_of_the_two_maps() {
        assert!(
            Scaler::new(statistics(0.0), HashMap::new()).is_err(),
            "a mean with no standard deviation"
        );
        assert!(
            Scaler::new(HashMap::new(), statistics(1.0)).is_err(),
            "a standard deviation with no mean"
        );
        assert!(Scaler::new(statistics(0.0), statistics(1.0)).is_ok());
    }

    /// A `Scaler` over one column is a legitimate value — the unscale path asks only for
    /// `daily_return` — so full coverage is an artifact property enforced in `load`, not a type
    /// property enforced in `new`. Pinned so the two checks are not conflated later.
    #[test]
    fn test_new_accepts_a_scaler_over_a_single_column() {
        assert!(Scaler::new(statistics(0.0), statistics(1.0)).is_ok());
    }

    /// `filter_map` would have dropped the non-string and compared the remainder, so an artifact
    /// declaring one more column than this build knows about could match a shorter expected list.
    #[test]
    fn test_load_refuses_a_non_string_entry_in_a_column_list() {
        let mut artifact = well_formed_artifact();
        let mut columns: Vec<serde_json::Value> = CONTINUOUS_COLUMNS
            .iter()
            .map(|column| serde_json::json!(column))
            .collect();
        columns.push(serde_json::json!(42));
        artifact["continuous_columns"] = serde_json::Value::Array(columns);

        let error = load_artifact(&artifact).expect_err("a non-string entry must be refused");
        assert!(error.contains("non-string"), "got: {error}");
    }

    /// The headline case. A missing object used to yield an empty map, every lookup then fell back
    /// to mean 0.0 and deviation 1.0, and scaling became the identity — reported as a successful
    /// load, producing predictions on the wrong scale that nothing downstream can detect.
    #[test]
    fn test_load_refuses_a_missing_statistics_object_rather_than_scaling_by_identity() {
        for field in ["means", "standard_deviations"] {
            let mut artifact = well_formed_artifact();
            artifact.as_object_mut().unwrap().remove(field);
            let error = load_artifact(&artifact).expect_err("a missing object must be refused");
            assert!(
                error.contains(field),
                "the error should name the missing field, got: {error}"
            );
        }
    }

    /// Same failure by a different route: an entry that is present but not a number used to be
    /// silently replaced with the identity value for its field.
    #[test]
    fn test_load_refuses_a_non_numeric_entry() {
        let mut artifact = well_formed_artifact();
        artifact["means"]["close_price"] = serde_json::json!("not a number");
        let error = load_artifact(&artifact).expect_err("a non-numeric entry must be refused");
        assert!(
            error.contains("close_price"),
            "the error should name the column, got: {error}"
        );
    }

    /// An artifact fitted on a different column set loaded without complaint and scaled a
    /// different set than the weights were trained on.
    #[test]
    fn test_load_refuses_an_artifact_fitted_on_a_different_column_set() {
        let mut artifact = well_formed_artifact();
        artifact["continuous_columns"] = serde_json::json!(["close_price", "an_extra_feature"]);
        let error = load_artifact(&artifact).expect_err("a column mismatch must be refused");
        assert!(
            error.contains("continuous_columns"),
            "the error should name the list that disagreed, got: {error}"
        );
    }

    #[test]
    fn test_load_refuses_an_artifact_missing_a_continuous_column_statistic() {
        let mut artifact = well_formed_artifact();
        artifact["means"]
            .as_object_mut()
            .unwrap()
            .remove("close_price");
        let error = load_artifact(&artifact).expect_err("a missing statistic must be refused");
        assert!(error.contains("close_price"), "got: {error}");
    }

    /// A null must be refused before its column is read positionally.
    ///
    /// `into_no_null_iter` neither skips nulls nor fails on them — it returns the raw value in the
    /// null slot, so the count is always right and a null `close_price` becomes a fabricated
    /// observation. The assertion below pins that, because it is the reason the guard is on the
    /// null count rather than on the extracted length.
    #[test]
    fn test_a_null_in_a_continuous_column_is_refused_rather_than_read_as_a_value() {
        let data = DataFrame::new(vec![
            Column::new("close_price".into(), &[Some(1.0_f64), None, Some(3.0)]),
            Column::new("open_price".into(), &[1.0_f64, 2.0, 3.0]),
        ])
        .unwrap();

        // The behaviour being guarded against, pinned because it is not what the review that
        // raised this described: `into_no_null_iter` neither skips the null nor fails on it. The
        // count comes back correct and the null slot reads as a fabricated observation, which is
        // exactly why checking the extracted length instead would have been an inert guard.
        let raw: Vec<f32> = data
            .column("close_price")
            .unwrap()
            .cast(&DataType::Float32)
            .unwrap()
            .f32()
            .unwrap()
            .into_no_null_iter()
            .collect();
        assert_eq!(
            raw.len(),
            3,
            "nulls are not skipped, so no length is ever wrong"
        );

        let error = get_float_columns(&data, &["close_price", "open_price"])
            .expect_err("a null must be refused, not silently read");
        assert!(
            error.to_string().contains("close_price"),
            "the error must name the column: {error}"
        );
    }

    #[test]
    fn test_a_null_in_a_categorical_column_is_refused() {
        let data = DataFrame::new(vec![Column::new(
            "sector".into(),
            &[Some(1_i32), None, Some(3)],
        )])
        .unwrap();

        assert!(get_int_columns(&data, &["sector"]).is_err());
    }

    #[test]
    fn test_complete_columns_are_accepted() {
        let data = DataFrame::new(vec![Column::new(
            "close_price".into(),
            &[1.0_f64, 2.0, 3.0],
        )])
        .unwrap();
        let columns = get_float_columns(&data, &["close_price"]).unwrap();
        assert_eq!(columns[0].len(), 3);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_scaler_inverse_transform() {
        let mut means = HashMap::new();
        means.insert("daily_return".to_string(), 0.001);
        let mut stds = HashMap::new();
        stds.insert("daily_return".to_string(), 0.02);
        let scaler = Scaler {
            means,
            standard_deviations: stds,
        };
        let result = scaler.inverse_transform_value("daily_return", 1.0);
        assert!((result - 0.021).abs() < 1e-10);
    }

    #[test]
    fn test_selector_by_names() {
        let names = selector_by_names(&["ticker", "timestamp"]);
        assert_eq!(names.len(), 2);
    }

    #[test]
    fn test_training_dataset_empty() {
        let dataset = TrainingDataset {
            past_continuous: ndarray::Array3::zeros((0, 35, 7)),
            past_categorical: ndarray::Array3::zeros((0, 35, 5)),
            future_categorical: ndarray::Array3::zeros((0, 5, 5)),
            static_categorical: ndarray::Array3::zeros((0, 1, 3)),
            targets: None,
        };
        assert!(dataset.is_empty());
        assert_eq!(dataset.len(), 0);
    }

    /// Build a minimal, already engineered/scaled/encoded frame (ticker and the
    /// categorical columns are integer-encoded) for windowing tests.
    fn make_encoded_frame(ticker_count: i32, rows_per_ticker: usize) -> DataFrame {
        let total = ticker_count as usize * rows_per_ticker;
        let mut ticker = Vec::with_capacity(total);
        let mut timestamp = Vec::with_capacity(total);
        let mut close = Vec::with_capacity(total);
        let mut daily_return = Vec::with_capacity(total);
        for ticker_id in 0..ticker_count {
            for row in 0..rows_per_ticker {
                ticker.push(ticker_id);
                timestamp.push((row as i64) * 86_400_000);
                close.push(100.0_f64 + row as f64);
                daily_return.push(0.01_f32 * (row as f32 + ticker_id as f32));
            }
        }
        let ones_float = vec![1.0_f64; total];
        let ones_int = vec![1_i32; total];
        DataFrame::new(vec![
            Column::new("ticker".into(), ticker),
            Column::new("timestamp".into(), timestamp),
            Column::new("open_price".into(), ones_float.clone()),
            Column::new("high_price".into(), ones_float.clone()),
            Column::new("low_price".into(), ones_float.clone()),
            Column::new("close_price".into(), close),
            Column::new("volume".into(), ones_float.clone()),
            Column::new("volume_weighted_average_price".into(), ones_float),
            Column::new("daily_return".into(), daily_return),
            Column::new("day_of_week".into(), ones_int.clone()),
            Column::new("day_of_month".into(), ones_int.clone()),
            Column::new("day_of_year".into(), ones_int.clone()),
            Column::new("month".into(), ones_int.clone()),
            Column::new("year".into(), ones_int.clone()),
            Column::new("sector".into(), ones_int.clone()),
            Column::new("industry".into(), ones_int),
        ])
        .unwrap()
    }

    fn empty_data(frame: DataFrame) -> Data {
        Data::from_parts(
            frame,
            Scaler {
                means: HashMap::new(),
                standard_deviations: HashMap::new(),
            },
            FeatureMappings::new(),
        )
    }

    fn fraction_of(value: f64) -> TrainingFraction {
        TrainingFraction::new(value).expect("the fixture fraction must be in range")
    }

    #[test]
    fn test_get_dataset_predict_one_window_per_ticker() {
        let data = empty_data(make_encoded_frame(3, 6));
        let dataset = data.get_dataset(DatasetKind::Predict, 2, 1).unwrap();
        // One prediction window per ticker, no targets.
        assert_eq!(dataset.len(), 3);
        assert!(dataset.targets.is_none());
        assert_eq!(dataset.past_continuous.shape(), [3, 2, 7]);
        assert_eq!(dataset.future_categorical.shape(), [3, 1, 5]);
        assert_eq!(dataset.static_categorical.shape(), [3, 1, 3]);
    }

    #[test]
    fn test_get_dataset_train_has_targets() {
        let data = empty_data(make_encoded_frame(2, 10));
        let dataset = data
            .get_dataset(DatasetKind::Train(fraction_of(0.8)), 3, 2)
            .unwrap();
        let sample_count = dataset.len();
        assert!(sample_count > 0);
        let targets = dataset.targets.expect("train dataset must have targets");
        assert_eq!(targets.shape()[1], 2); // output_length
        assert_eq!(targets.shape()[2], 1);
        assert_eq!(targets.shape()[0], sample_count);
    }

    /// Both endpoints leave one side of the split empty, and nothing downstream reports that as a
    /// failure — the run trains on no windows and records a loss over zero rows. The constructor is
    /// the only place it is still visible.
    #[test]
    fn test_training_fraction_rejects_values_outside_the_open_unit_interval() {
        for rejected in [0.0, 1.0, -0.1, 1.5, f64::NAN, f64::INFINITY] {
            assert!(
                TrainingFraction::new(rejected).is_err(),
                "expected {rejected} to be rejected"
            );
        }
        assert!(TrainingFraction::new(0.8).is_ok());
    }

    /// A validation split can yield no windows at all while the training split yields plenty, and
    /// nothing in this module treats that as an error — `window_frame` just returns an empty
    /// dataset.
    ///
    /// This is the condition `bin/tide_model_trainer.rs` refuses before training, and it pins why
    /// that guard has to exist. Downstream, an empty validation set is silent in three places at
    /// once: early stopping falls back to the training loss, [`crate::models::tide::evaluate`]
    /// short-circuits to zeroed metrics, and the trainer publishes `crps: 0.0` — the best score
    /// achievable — into the drift baseline that every later run is measured against.
    ///
    /// The guard itself is not exercised here; it lives in `run()`, which fetches from S3.
    #[test]
    fn test_a_validation_split_can_produce_no_windows_while_training_produces_many() {
        // 11 rows split at 0.8 leaves 2 validation rows, and a window needs input + output = 4.
        let data = empty_data(make_encoded_frame(1, 11));

        let training = data
            .get_dataset(DatasetKind::Train(fraction_of(0.8)), 3, 1)
            .unwrap();
        let validation = data
            .get_dataset(DatasetKind::Validate(fraction_of(0.8)), 3, 1)
            .unwrap();

        assert!(
            !training.is_empty(),
            "the training split must produce windows, or this proves nothing"
        );
        assert!(
            validation.is_empty(),
            "expected no validation windows from 2 rows, got {}",
            validation.len()
        );
    }

    /// The predict variant carries no split, so inference cannot pass one that does nothing — which
    /// is what the `0.8` at the old predict call site was. This is a compile-time property; the
    /// assertion here records that predicting reads the whole frame rather than a split of it.
    #[test]
    fn test_predict_windows_the_whole_frame_rather_than_a_split() {
        let frame = make_encoded_frame(1, 10);
        let data = empty_data(frame);

        let predicted = data.get_dataset(DatasetKind::Predict, 3, 1).unwrap();
        // One window at the end of the ticker's full series.
        assert_eq!(predicted.len(), 1);

        // A split that keeps only the first 20% of the range cannot reach the predict path, so the
        // predict window count is unchanged by any split value.
        let trained = data
            .get_dataset(DatasetKind::Train(fraction_of(0.2)), 3, 1)
            .unwrap();
        assert!(
            trained.len() < 10,
            "the training split must cover fewer windows than the full frame"
        );
    }

    #[test]
    fn test_split_by_timestamp_partitions_rows() {
        let data = empty_data(make_encoded_frame(1, 10));
        let (train, valid) = data.split_by_timestamp(fraction_of(0.8)).unwrap();
        assert_eq!(train.height() + valid.height(), 10);
        assert!(train.height() > 0);
        assert!(valid.height() > 0);
    }

    /// Two tickers, two days each, unsorted on input; close prices chosen so
    /// each ticker's second-day return is 0.1.
    fn raw_two_ticker_frame() -> DataFrame {
        DataFrame::new(vec![
            Column::new("ticker".into(), vec!["BBB", "AAA", "BBB", "AAA"]),
            Column::new("timestamp".into(), vec![0_i64, 0, 86_400_000, 86_400_000]),
            Column::new("open_price".into(), vec![1.0_f64; 4]),
            Column::new("high_price".into(), vec![1.0_f64; 4]),
            Column::new("low_price".into(), vec![1.0_f64; 4]),
            Column::new("close_price".into(), vec![10.0_f64, 20.0, 11.0, 22.0]),
            Column::new("volume".into(), vec![1.0_f64; 4]),
            Column::new("volume_weighted_average_price".into(), vec![1.0_f64; 4]),
            Column::new("sector".into(), vec!["S", "S", "S", "S"]),
            Column::new("industry".into(), vec!["I", "I", "I", "I"]),
        ])
        .unwrap()
    }

    #[test]
    fn test_engineer_features_nulls_first_row_per_ticker() {
        // Each ticker's first row has a null daily_return (dropped later by clean_data), never
        // a synthetic zero and never a value carried across the ticker boundary.
        let engineered = engineer_features(raw_two_ticker_frame()).unwrap();
        // Sorted by [ticker, timestamp]: AAA@0, AAA@1, BBB@0, BBB@1.
        let returns: Vec<Option<f32>> = engineered
            .column("daily_return")
            .unwrap()
            .f32()
            .unwrap()
            .into_iter()
            .collect();
        assert_eq!(returns[0], None); // AAA first row
        assert!((returns[1].unwrap() - 0.1).abs() < 1e-6); // 22/20 - 1
        assert_eq!(returns[2], None); // BBB first row
        assert!((returns[3].unwrap() - 0.1).abs() < 1e-6); // 11/10 - 1
    }

    #[test]
    fn test_clean_data_drops_null_return_rows() {
        // Null, NaN, and non-finite daily_return rows are filtered, so each ticker's first
        // observation never reaches the scaler or windows.
        let engineered = engineer_features(raw_two_ticker_frame()).unwrap();
        let cleaned = clean_data(engineered).unwrap();
        assert_eq!(cleaned.height(), 2);
        let returns: Vec<f32> = cleaned
            .column("daily_return")
            .unwrap()
            .f32()
            .unwrap()
            .into_no_null_iter()
            .collect();
        assert!(returns
            .iter()
            .all(|daily_return| (daily_return - 0.1).abs() < 1e-6));
    }

    #[test]
    fn test_clean_data_fills_null_sector_with_the_stored_default() {
        // A null sector or industry must land on the same value the database writes by default,
        // or the concept splits into two categories: rows loaded from `equity_details` encode as
        // "NOT AVAILABLE" while any null-bearing frame encodes as something else. Because these
        // are static columns, `encode_categoricals` drops rows whose value is absent from the
        // training mapping rather than folding them into a fallback, so the split is silent.
        let mut engineered = engineer_features(raw_two_ticker_frame()).unwrap();
        engineered
            .with_column(Column::new(
                "sector".into(),
                vec![None::<&str>, None, None, None],
            ))
            .unwrap();
        engineered
            .with_column(Column::new(
                "industry".into(),
                vec![None::<&str>, None, None, None],
            ))
            .unwrap();

        let cleaned = clean_data(engineered).unwrap();
        for column in ["sector", "industry"] {
            let values: Vec<&str> = cleaned
                .column(column)
                .unwrap()
                .str()
                .unwrap()
                .into_no_null_iter()
                .collect();
            assert!(
                values
                    .iter()
                    .all(|value| *value == crate::data::details::UNKNOWN),
                "{column} fell back to {values:?} instead of the schema default"
            );
        }
    }

    /// Raw (pre-engineering) frame with string tickers and one bar per consecutive day.
    ///
    /// `close_price` is `100 + row`, so a window's contents identify which rows produced them.
    fn raw_frame(ticker_count: usize, rows_per_ticker: usize) -> DataFrame {
        let names = ["AAA", "BBB", "CCC"];
        let total = ticker_count * rows_per_ticker;
        let mut ticker = Vec::with_capacity(total);
        let mut timestamp = Vec::with_capacity(total);
        let mut close = Vec::with_capacity(total);
        for name in names.iter().take(ticker_count) {
            for row in 0..rows_per_ticker {
                ticker.push(*name);
                let date =
                    SessionDate::from_date(chrono::NaiveDate::from_ymd_opt(2026, 6, 1).unwrap())
                        .plus_calendar_days(row as i64);
                timestamp.push(date.midnight().timestamp_millis());
                close.push(100.0 + row as f64);
            }
        }
        DataFrame::new(vec![
            Column::new("ticker".into(), ticker),
            Column::new("timestamp".into(), timestamp),
            Column::new("open_price".into(), vec![1.0_f64; total]),
            Column::new("high_price".into(), vec![1.0_f64; total]),
            Column::new("low_price".into(), vec![1.0_f64; total]),
            Column::new("close_price".into(), close),
            Column::new("volume".into(), vec![1.0_f64; total]),
            Column::new("volume_weighted_average_price".into(), vec![1.0_f64; total]),
            Column::new("sector".into(), vec!["S"; total]),
            Column::new("industry".into(), vec!["I"; total]),
        ])
        .unwrap()
    }

    /// The session a pre-open run forecasts: the day after the newest bar in the frame.
    fn forecast_session(frame: &DataFrame) -> SessionDate {
        let newest = frame
            .column("timestamp")
            .unwrap()
            .i64()
            .unwrap()
            .max()
            .unwrap();
        SessionDate::at(chrono::DateTime::from_timestamp_millis(newest).unwrap())
            .plus_calendar_days(1)
    }

    /// Everything `apply_existing_scaler` does to a frame, minus the scaler and mappings.
    fn prepared(frame: DataFrame) -> DataFrame {
        clean_data(engineer_features(frame).unwrap()).unwrap()
    }

    /// `prepared`, then integer-encoded the way `window_frame` expects. Mappings are fitted from
    /// the frame itself, mirroring `fit_mappings`, which covers only the static columns.
    fn prepared_and_encoded(frame: DataFrame) -> DataFrame {
        let cleaned = prepared(frame);
        let mut mappings = FeatureMappings::new();
        for column in STATIC_CATEGORICAL_COLUMNS {
            let mut values: Vec<String> = cleaned
                .column(column)
                .unwrap()
                .str()
                .unwrap()
                .into_no_null_iter()
                .map(str::to_string)
                .collect::<std::collections::HashSet<_>>()
                .into_iter()
                .collect();
            values.sort();
            mappings.insert(
                (*column).to_string(),
                values
                    .into_iter()
                    .enumerate()
                    .map(|(index, value)| (value, index as i32))
                    .collect(),
            );
        }
        encode_categoricals(cleaned, &mappings).unwrap()
    }

    #[test]
    fn test_forecast_row_keeps_the_newest_bar_in_the_past_window() {
        // Without the appended row the newest bar is spent as the future calendar step, so the
        // model's price context stops a session short and it forecasts a close that already
        // happened. close_price is 100 + row, so the window's contents identify their rows.
        let input_length = 35usize;
        let output_length = 1usize;
        let frame = raw_frame(1, 40);
        let session = forecast_session(&frame);

        let without = window_frame(
            &prepared_and_encoded(frame.clone()),
            input_length,
            output_length,
            true,
            false,
        )
        .unwrap();
        let with = window_frame(
            &prepared_and_encoded(append_forecast_session_rows(frame, session).unwrap()),
            input_length,
            output_length,
            true,
            false,
        )
        .unwrap();

        let close_index = CONTINUOUS_COLUMNS
            .iter()
            .position(|column| *column == "close_price")
            .unwrap();
        let newest_in_past = |dataset: &TrainingDataset| {
            dataset.past_continuous[[0, input_length - 1, close_index]] as usize - 100
        };

        // The newest real bar is row 39. Cleaning drops each ticker's first row, so the frame the
        // window sees ends there either way -- only the appended row changes which side it lands on.
        assert_eq!(
            newest_in_past(&without),
            38,
            "precondition: without the appended row the newest bar is not a past feature"
        );
        assert_eq!(
            newest_in_past(&with),
            39,
            "the newest real bar must be the last step of the past window"
        );
    }

    #[test]
    fn test_forecast_row_carries_the_target_session_calendar() {
        // The future step must describe the session being forecast, not the last one observed.
        let frame = raw_frame(1, 40);
        let session = forecast_session(&frame);
        let engineered =
            engineer_features(append_forecast_session_rows(frame, session).unwrap()).unwrap();

        let target_milliseconds = session.midnight().timestamp_millis();
        let timestamps: Vec<i64> = engineered
            .column("timestamp")
            .unwrap()
            .i64()
            .unwrap()
            .into_no_null_iter()
            .collect();
        let row = timestamps
            .iter()
            .position(|value| *value == target_milliseconds)
            .expect("the appended session must survive feature engineering");

        let day_of_week: Vec<i32> = engineered
            .column("day_of_week")
            .unwrap()
            .i32()
            .unwrap()
            .into_no_null_iter()
            .collect();
        assert_eq!(
            day_of_week[row],
            session.date().weekday().number_from_monday() as i32,
            "the appended row's calendar must match the forecast session"
        );
    }

    #[test]
    fn test_forecast_row_survives_cleaning_and_matches_the_stamped_session() {
        // Two invariants together. The row must survive clean_data -- if it were dropped the window
        // would silently revert to the misaligned behaviour with no error anywhere. And its session
        // must equal the one `step_timestamp_milliseconds` stamps, or the features and the label
        // describe different days, which is the shape of the bug this replaced.
        let frame = raw_frame(2, 40);
        let session = forecast_session(&frame);
        let cleaned = prepared(append_forecast_session_rows(frame, session).unwrap());

        let target_milliseconds = session.midnight().timestamp_millis();
        let surviving = cleaned
            .column("timestamp")
            .unwrap()
            .i64()
            .unwrap()
            .into_no_null_iter()
            .filter(|value| *value == target_milliseconds)
            .count();
        assert_eq!(
            surviving, 2,
            "one appended row per ticker must survive cleaning"
        );

        // Any instant inside the forecast session must stamp that same session.
        let noon = session.midnight() + chrono::Duration::hours(12);
        assert_eq!(
            crate::models::tide::predict::step_timestamp_milliseconds(noon, 0),
            target_milliseconds,
            "the stamped session must be the session the appended row carries"
        );
    }

    #[test]
    fn test_forecast_row_is_not_appended_when_the_session_already_has_a_bar() {
        // A run after the session's own bar has landed must not duplicate it.
        let frame = raw_frame(1, 40);
        let newest = frame
            .column("timestamp")
            .unwrap()
            .i64()
            .unwrap()
            .max()
            .unwrap();
        let already_present =
            SessionDate::at(chrono::DateTime::from_timestamp_millis(newest).unwrap());
        let appended = append_forecast_session_rows(frame.clone(), already_present).unwrap();
        assert_eq!(
            appended.height(),
            frame.height(),
            "no row should be appended for a session already present"
        );
    }

    #[test]
    fn test_clean_data_rejects_a_null_ticker_and_keeps_every_real_one() {
        // A null ticker is refused rather than encoded as a placeholder and filtered back out.
        // `engineer_features` already rejects nulls, so this is the guard for a direct caller --
        // and it means no real symbol can collide with a sentinel spelling and be dropped.
        let mut engineered = engineer_features(raw_two_ticker_frame()).unwrap();
        let kept = clean_data(engineered.clone()).unwrap();
        let survivors: Vec<&str> = kept
            .column("ticker")
            .unwrap()
            .str()
            .unwrap()
            .into_no_null_iter()
            .collect();
        assert!(!survivors.is_empty(), "no rows survived cleaning");

        engineered
            .with_column(Column::new(
                "ticker".into(),
                vec![Some("AAA"), None::<&str>, Some("BBB"), Some("BBB")],
            ))
            .unwrap();
        let error = clean_data(engineered).unwrap_err().to_string();
        assert!(
            error.contains("null ticker"),
            "expected a null-ticker rejection, got: {error}"
        );
    }

    #[test]
    fn test_clean_data_drops_rows_with_null_continuous_values() {
        // A null volume_weighted_average_price (nullable in the database and in
        // vendor data) must drop the row; it must never silently shorten a column during
        // scaling or windowing.
        let mut engineered = engineer_features(raw_two_ticker_frame()).unwrap();
        engineered
            .with_column(Column::new(
                "volume_weighted_average_price".into(),
                vec![Some(1.0_f64), None, Some(1.0), Some(1.0)],
            ))
            .unwrap();

        let cleaned = clean_data(engineered).unwrap();
        // Two first-rows drop for null returns, one more for the null price.
        assert_eq!(cleaned.height(), 1);
    }

    #[test]
    fn test_engineer_features_day_of_week_is_monday_based_one_to_seven() {
        // Monday = 1 .. Sunday = 7.
        //
        // Stamped at Eastern midnight, which is how a daily bar actually arrives. Built at UTC
        // midnight instead — as this fixture was — every date reads as the previous Eastern day,
        // and the assertion below became [7, 6]. The fixture, not the feature, was wrong.
        let session = |year, month, day| {
            SessionDate::from_date(chrono::NaiveDate::from_ymd_opt(year, month, day).unwrap())
                .midnight()
                .timestamp_millis()
        };
        let monday = session(2026, 6, 8);
        let sunday = session(2026, 6, 14);

        let frame = DataFrame::new(vec![
            Column::new("ticker".into(), vec!["AAA", "AAA"]),
            Column::new("timestamp".into(), vec![monday, sunday]),
            Column::new("open_price".into(), vec![1.0_f64; 2]),
            Column::new("high_price".into(), vec![1.0_f64; 2]),
            Column::new("low_price".into(), vec![1.0_f64; 2]),
            Column::new("close_price".into(), vec![10.0_f64, 11.0]),
            Column::new("volume".into(), vec![1.0_f64; 2]),
            Column::new("volume_weighted_average_price".into(), vec![1.0_f64; 2]),
            Column::new("sector".into(), vec!["S", "S"]),
            Column::new("industry".into(), vec!["I", "I"]),
        ])
        .unwrap();

        let engineered = engineer_features(frame).unwrap();
        let day_of_week: Vec<i32> = engineered
            .column("day_of_week")
            .unwrap()
            .i32()
            .unwrap()
            .into_no_null_iter()
            .collect();
        assert_eq!(day_of_week, vec![1, 7]);
    }

    /// The covariates must come from the Eastern date, on an instant where Eastern and UTC disagree.
    ///
    /// The fixture above cannot prove this. A daily bar is stamped at Eastern midnight, which is
    /// 04:00 or 05:00 the *same* UTC day, so `date_naive()` and `SessionDate::at` return the same
    /// date for it and the assertion holds under either implementation.
    ///
    /// 01:00Z on 10 June is 21:00 on 9 June in New York — Wednesday in UTC, Tuesday in Eastern.
    /// Reading the UTC date here yields 3; the session's own weekday is 2.
    #[test]
    fn test_engineer_features_reads_the_eastern_date_where_the_two_disagree() {
        let late_evening = chrono::DateTime::parse_from_rfc3339("2026-06-10T01:00:00Z")
            .unwrap()
            .with_timezone(&chrono::Utc);

        // The precondition, so this cannot pass by the two agreeing: the UTC weekday is Wednesday.
        assert_eq!(
            late_evening.date_naive().weekday().number_from_monday(),
            3,
            "the fixture must sit on an instant where UTC and Eastern name different days"
        );

        let frame = DataFrame::new(vec![
            Column::new("ticker".into(), vec!["AAA"]),
            Column::new("timestamp".into(), vec![late_evening.timestamp_millis()]),
            Column::new("open_price".into(), vec![1.0_f64]),
            Column::new("high_price".into(), vec![1.0_f64]),
            Column::new("low_price".into(), vec![1.0_f64]),
            Column::new("close_price".into(), vec![10.0_f64]),
            Column::new("volume".into(), vec![1.0_f64]),
            Column::new("volume_weighted_average_price".into(), vec![1.0_f64]),
            Column::new("sector".into(), vec!["S"]),
            Column::new("industry".into(), vec!["I"]),
        ])
        .unwrap();

        let engineered = engineer_features(frame).unwrap();
        let read = |name: &str| {
            engineered
                .column(name)
                .unwrap()
                .i32()
                .unwrap()
                .get(0)
                .unwrap()
        };
        assert_eq!(read("day_of_week"), 2, "Tuesday in Eastern, not Wednesday");
        assert_eq!(read("day_of_month"), 9);
        assert_eq!(read("month"), 6);
    }

    #[test]
    fn test_split_by_timestamp_boundary_row_goes_to_train() {
        // Train is date <= split, validation is date > split. With 11 daily rows (0..=10 days)
        // and split 0.8 the cutoff lands exactly on day 8, which must belong to train.
        let data = empty_data(make_encoded_frame(1, 11));
        let (train, valid) = data.split_by_timestamp(fraction_of(0.8)).unwrap();
        assert_eq!(train.height(), 9);
        assert_eq!(valid.height(), 2);
    }
}
