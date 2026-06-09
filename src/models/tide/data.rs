use std::collections::HashMap;

use chrono::Datelike;
use polars::prelude::*;
use serde::{Deserialize, Serialize};

pub type CategoryMapping = HashMap<String, i32>;
pub type FeatureMappings = HashMap<String, CategoryMapping>;

pub fn selector_by_names(names: &[&str]) -> Vec<PlSmallStr> {
    names.iter().map(|name| PlSmallStr::from(*name)).collect()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Scaler {
    pub means: HashMap<String, f64>,
    pub standard_deviations: HashMap<String, f64>,
}

impl Scaler {
    #[allow(clippy::type_complexity)]
    pub fn load(
        path: &std::path::Path,
    ) -> Result<(Self, Vec<String>, Vec<String>, Vec<String>), Box<dyn std::error::Error>> {
        let content = std::fs::read_to_string(path)?;
        let raw: serde_json::Value = serde_json::from_str(&content)?;

        let means: HashMap<String, f64> = raw["means"]
            .as_object()
            .map(|obj| {
                obj.iter()
                    .map(|(k, v)| (k.clone(), v.as_f64().unwrap_or(0.0)))
                    .collect()
            })
            .unwrap_or_default();

        let standard_deviations: HashMap<String, f64> = raw["standard_deviations"]
            .as_object()
            .map(|obj| {
                obj.iter()
                    .map(|(k, v)| (k.clone(), v.as_f64().unwrap_or(1.0)))
                    .collect()
            })
            .unwrap_or_default();

        let continuous_columns: Vec<String> = raw["continuous_columns"]
            .as_array()
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();

        let categorical_columns: Vec<String> = raw["categorical_columns"]
            .as_array()
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();

        let static_categorical_columns: Vec<String> = raw["static_categorical_columns"]
            .as_array()
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();

        Ok((
            Self {
                means,
                standard_deviations,
            },
            continuous_columns,
            categorical_columns,
            static_categorical_columns,
        ))
    }

    pub fn inverse_transform_value(&self, column: &str, value: f64) -> f64 {
        let mean = self.means.get(column).copied().unwrap_or(0.0);
        let std = self.standard_deviations.get(column).copied().unwrap_or(1.0);
        value * std + mean
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
            continuous_columns: CONTINUOUS_COLUMNS.iter().map(|s| s.to_string()).collect(),
            categorical_columns: CATEGORICAL_COLUMNS.iter().map(|s| s.to_string()).collect(),
            static_categorical_columns: STATIC_CATEGORICAL_COLUMNS
                .iter()
                .map(|s| s.to_string())
                .collect(),
        }
    }

    pub fn apply_existing_scaler(
        data: DataFrame,
        scaler: &Scaler,
        mappings: &FeatureMappings,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let data = engineer_features(data)?;
        let data = clean_data(data)?;
        let data = apply_scaling(data, scaler)?;
        let data = encode_categoricals(data, mappings)?;

        Ok(Self::from_parts(data, scaler.clone(), mappings.clone()))
    }

    /// Split the engineered frame into (train, validate) by a global date cutoff
    /// at `min + (max - min) * validation_split`: rows at or before the cutoff
    /// are training, later rows are validation (the Python trainer uses
    /// `date <= split` for train).
    pub fn split_by_timestamp(
        &self,
        validation_split: f64,
    ) -> Result<(DataFrame, DataFrame), Box<dyn std::error::Error>> {
        let timestamps = self.data.column("timestamp").map_err(|e| e.to_string())?;
        let timestamps = timestamps.i64().map_err(|e| e.to_string())?;
        let min_ts = timestamps.min().unwrap_or(0);
        let max_ts = timestamps.max().unwrap_or(0);
        let cutoff = min_ts + (((max_ts - min_ts) as f64) * validation_split) as i64;

        let train_mask = timestamps.lt_eq(cutoff);
        let valid_mask = timestamps.gt(cutoff);
        let train = self.data.filter(&train_mask).map_err(|e| e.to_string())?;
        let valid = self.data.filter(&valid_mask).map_err(|e| e.to_string())?;
        Ok((train, valid))
    }

    /// Build a windowed dataset.
    ///
    /// - `predict` → one window per ticker at the end of its series, no targets.
    /// - `train` / `validate` → all sliding windows over the corresponding date
    ///   split, with the future `daily_return` window as targets.
    pub fn get_dataset(
        &self,
        data_type: &str,
        validation_split: f64,
        input_length: usize,
        output_length: usize,
    ) -> Result<TrainingDataset, Box<dyn std::error::Error>> {
        match data_type {
            "predict" => window_frame(&self.data, input_length, output_length, true, false),
            "validate" => {
                let (_, valid) = self.split_by_timestamp(validation_split)?;
                window_frame(&valid, input_length, output_length, false, true)
            }
            _ => {
                let (train, _) = self.split_by_timestamp(validation_split)?;
                window_frame(&train, input_length, output_length, false, true)
            }
        }
    }
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
    let n_cont = CONTINUOUS_COLUMNS.len();
    let n_cat = CATEGORICAL_COLUMNS.len();
    let n_static = STATIC_CATEGORICAL_COLUMNS.len();
    let target_index = CONTINUOUS_COLUMNS
        .iter()
        .position(|c| *c == TARGET_COLUMN)
        .expect("daily_return must be a continuous column");

    // `ticker` is integer-encoded by this point (encode_categoricals). Group by
    // the encoded id; sorted for deterministic sample ordering.
    let mut tickers: Vec<i32> = frame
        .column("ticker")
        .map_err(|e| e.to_string())?
        .i32()
        .map_err(|e| e.to_string())?
        .into_no_null_iter()
        .collect::<std::collections::HashSet<_>>()
        .into_iter()
        .collect();
    tickers.sort_unstable();

    let mut all_past_cont: Vec<Vec<f32>> = Vec::new();
    let mut all_past_cat: Vec<Vec<i32>> = Vec::new();
    let mut all_future_cat: Vec<Vec<i32>> = Vec::new();
    let mut all_static_cat: Vec<Vec<i32>> = Vec::new();
    let mut all_targets: Vec<Vec<f32>> = Vec::new();

    for ticker in &tickers {
        let mask = frame
            .column("ticker")
            .map_err(|e| e.to_string())?
            .i32()
            .map_err(|e| e.to_string())?
            .equal(*ticker);
        let ticker_data = frame.filter(&mask).map_err(|e| e.to_string())?;

        if ticker_data.height() < window_size {
            continue;
        }

        let cont_arrays = get_float_columns(&ticker_data, CONTINUOUS_COLUMNS)?;
        let cat_arrays = get_int_columns(&ticker_data, CATEGORICAL_COLUMNS)?;
        let static_arrays = get_int_columns(&ticker_data, STATIC_CATEGORICAL_COLUMNS)?;

        let windows: Vec<usize> = if predict_mode {
            vec![ticker_data.height() - window_size]
        } else {
            (0..=ticker_data.height() - window_size).collect()
        };

        for start in windows {
            let mut past_cont = Vec::with_capacity(input_length * n_cont);
            for t in start..start + input_length {
                for col in &cont_arrays {
                    past_cont.push(col[t]);
                }
            }

            let mut past_cat = Vec::with_capacity(input_length * n_cat);
            for t in start..start + input_length {
                for col in &cat_arrays {
                    past_cat.push(col[t]);
                }
            }

            let mut future_cat = Vec::with_capacity(output_length * n_cat);
            for t in (start + input_length)..(start + input_length + output_length) {
                for col in &cat_arrays {
                    future_cat.push(col[t]);
                }
            }

            let mut static_cat = Vec::with_capacity(n_static);
            for col in &static_arrays {
                static_cat.push(col[start]);
            }

            all_past_cont.push(past_cont);
            all_past_cat.push(past_cat);
            all_future_cat.push(future_cat);
            all_static_cat.push(static_cat);

            if with_targets {
                let returns = &cont_arrays[target_index];
                let future = (start + input_length)..(start + input_length + output_length);
                all_targets.push(returns[future].to_vec());
            }
        }
    }

    let num_samples = all_past_cont.len();

    let past_continuous = if num_samples > 0 {
        let flat: Vec<f32> = all_past_cont.into_iter().flatten().collect();
        ndarray::Array3::from_shape_vec((num_samples, input_length, n_cont), flat)?
    } else {
        ndarray::Array3::zeros((0, input_length, n_cont))
    };

    let past_categorical = if num_samples > 0 {
        let flat: Vec<i32> = all_past_cat.into_iter().flatten().collect();
        ndarray::Array3::from_shape_vec((num_samples, input_length, n_cat), flat)?
    } else {
        ndarray::Array3::zeros((0, input_length, n_cat))
    };

    let future_categorical = if num_samples > 0 {
        let flat: Vec<i32> = all_future_cat.into_iter().flatten().collect();
        ndarray::Array3::from_shape_vec((num_samples, output_length, n_cat), flat)?
    } else {
        ndarray::Array3::zeros((0, output_length, n_cat))
    };

    let static_categorical = if num_samples > 0 {
        let flat: Vec<i32> = all_static_cat.into_iter().flatten().collect();
        ndarray::Array3::from_shape_vec((num_samples, 1, n_static), flat)?
    } else {
        ndarray::Array3::zeros((0, 1, n_static))
    };

    let targets = if with_targets && num_samples > 0 {
        let flat: Vec<f32> = all_targets.into_iter().flatten().collect();
        Some(ndarray::Array3::from_shape_vec(
            (num_samples, output_length, 1),
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

pub(crate) fn engineer_features(data: DataFrame) -> Result<DataFrame, Box<dyn std::error::Error>> {
    // Sort by [ticker, timestamp] so daily returns and the downstream windowing
    // are chronological and contiguous within each ticker, independent of the
    // order rows arrived in. Each ticker's first row gets a null return (like
    // pct_change over ticker in the Python pipeline); clean_data drops it.
    let data = data
        .sort(
            ["ticker", "timestamp"],
            SortMultipleOptions::default().with_maintain_order(true),
        )
        .map_err(|e| e.to_string())?;

    let timestamps = data.column("timestamp").map_err(|e| e.to_string())?;
    let height = data.height();

    let mut day_of_week = Vec::with_capacity(height);
    let mut day_of_month = Vec::with_capacity(height);
    let mut day_of_year = Vec::with_capacity(height);
    let mut month = Vec::with_capacity(height);
    let mut year = Vec::with_capacity(height);
    let mut daily_return: Vec<Option<f32>> = Vec::with_capacity(height);

    let close_prices: Vec<f64> = data
        .column("close_price")
        .map_err(|e| e.to_string())?
        .f64()
        .map_err(|e| e.to_string())?
        .into_no_null_iter()
        .collect();

    let tickers: Vec<String> = data
        .column("ticker")
        .map_err(|e| e.to_string())?
        .str()
        .map_err(|e| e.to_string())?
        .into_no_null_iter()
        .map(|s| s.to_string())
        .collect();

    let timestamp_values: Vec<i64> = timestamps
        .i64()
        .map_err(|e| e.to_string())?
        .into_no_null_iter()
        .collect();

    for (i, &ts) in timestamp_values.iter().enumerate() {
        let datetime = chrono::DateTime::from_timestamp_millis(ts)
            .unwrap_or_else(|| chrono::DateTime::from_timestamp(0, 0).unwrap());
        let date = datetime.date_naive();

        // Monday = 1 .. Sunday = 7, matching polars dt.weekday() in Python.
        day_of_week.push(date.weekday().number_from_monday() as i32);
        day_of_month.push(date.day() as i32);
        day_of_year.push(date.ordinal() as i32);
        month.push(date.month() as i32);
        year.push(date.year());

        let same_ticker = i > 0 && tickers[i] == tickers[i - 1];
        if same_ticker && close_prices[i - 1] != 0.0 {
            daily_return.push(Some(((close_prices[i] / close_prices[i - 1]) - 1.0) as f32));
        } else {
            daily_return.push(None);
        }
    }

    let mut new_data = data.clone();
    new_data
        .with_column(Column::new("day_of_week".into(), day_of_week))
        .map_err(|e| e.to_string())?;
    new_data
        .with_column(Column::new("day_of_month".into(), day_of_month))
        .map_err(|e| e.to_string())?;
    new_data
        .with_column(Column::new("day_of_year".into(), day_of_year))
        .map_err(|e| e.to_string())?;
    new_data
        .with_column(Column::new("month".into(), month))
        .map_err(|e| e.to_string())?;
    new_data
        .with_column(Column::new("year".into(), year))
        .map_err(|e| e.to_string())?;
    new_data
        .with_column(Column::new("daily_return".into(), daily_return))
        .map_err(|e| e.to_string())?;

    Ok(new_data)
}

pub(crate) fn clean_data(mut data: DataFrame) -> Result<DataFrame, Box<dyn std::error::Error>> {
    // Uppercase ticker, sector, industry columns in-place
    let ticker_upper: Vec<String> = data
        .column("ticker")
        .map_err(|e| e.to_string())?
        .str()
        .map_err(|e| e.to_string())?
        .into_no_null_iter()
        .map(|s| s.to_uppercase())
        .collect();

    let sector_upper: Vec<String> = data
        .column("sector")
        .map_err(|e| e.to_string())?
        .str()
        .map_err(|e| e.to_string())?
        .into_no_null_iter()
        .map(|s| s.to_uppercase())
        .collect();

    let industry_upper: Vec<String> = data
        .column("industry")
        .map_err(|e| e.to_string())?
        .str()
        .map_err(|e| e.to_string())?
        .into_no_null_iter()
        .map(|s| s.to_uppercase())
        .collect();

    data.with_column(Column::new("ticker".into(), ticker_upper))
        .map_err(|e| e.to_string())?;
    data.with_column(Column::new("sector".into(), sector_upper))
        .map_err(|e| e.to_string())?;
    data.with_column(Column::new("industry".into(), industry_upper))
        .map_err(|e| e.to_string())?;

    // Filter out UNKNOWN tickers
    let mask = data
        .column("ticker")
        .map_err(|e| e.to_string())?
        .str()
        .map_err(|e| e.to_string())?
        .not_equal("UNKNOWN");

    let cleaned = data.filter(&mask).map_err(|e| e.to_string())?;

    // Drop rows without a finite daily return — each ticker's first observation
    // and any division artifacts — matching the Python CleanData stage.
    let returns = cleaned
        .column("daily_return")
        .map_err(|e| e.to_string())?
        .f32()
        .map_err(|e| e.to_string())?;
    let finite_mask: BooleanChunked = returns
        .into_iter()
        .map(|value| value.is_some_and(f32::is_finite))
        .collect();
    let cleaned = cleaned.filter(&finite_mask).map_err(|e| e.to_string())?;
    Ok(cleaned)
}

pub(crate) fn apply_scaling(
    data: DataFrame,
    scaler: &Scaler,
) -> Result<DataFrame, Box<dyn std::error::Error>> {
    let mut result = data;
    for col_name in CONTINUOUS_COLUMNS {
        let mean = scaler.means.get(*col_name).copied().unwrap_or(0.0);
        let std = scaler
            .standard_deviations
            .get(*col_name)
            .copied()
            .unwrap_or(1.0);
        let std = if std == 0.0 { 1e-8 } else { std };

        let values: Vec<f32> = result
            .column(col_name)
            .map_err(|e| e.to_string())?
            .cast(&DataType::Float64)
            .map_err(|e| e.to_string())?
            .f64()
            .map_err(|e| e.to_string())?
            .into_no_null_iter()
            .map(|v| ((v - mean) / std) as f32)
            .collect();

        result
            .with_column(Column::new((*col_name).into(), values))
            .map_err(|e| e.to_string())?;
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

    for col_name in all_categorical {
        if let Some(mapping) = mappings.get(col_name) {
            let values: Vec<Option<i32>> = result
                .column(col_name)
                .map_err(|e| e.to_string())?
                .str()
                .map(|ca| {
                    ca.into_iter()
                        .map(|opt_val| opt_val.and_then(|val| mapping.get(val).copied()))
                        .collect()
                })
                .or_else(|_| {
                    result
                        .column(col_name)
                        .map_err(|e| e.to_string())?
                        .i32()
                        .map(|ca| ca.into_iter().collect())
                        .map_err(|e| e.to_string())
                })?;

            let encoded: Vec<i32> = values.into_iter().map(|v| v.unwrap_or(-1)).collect();
            result
                .with_column(Column::new(col_name.into(), encoded))
                .map_err(|e| e.to_string())?;
        }
    }

    Ok(result)
}

fn get_float_columns(
    data: &DataFrame,
    columns: &[&str],
) -> Result<Vec<Vec<f32>>, Box<dyn std::error::Error>> {
    let mut result = Vec::new();
    for col_name in columns {
        let values: Vec<f32> = data
            .column(col_name)
            .map_err(|e| e.to_string())?
            .cast(&DataType::Float32)
            .map_err(|e| e.to_string())?
            .f32()
            .map_err(|e| e.to_string())?
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
    for col_name in columns {
        let values: Vec<i32> = data
            .column(col_name)
            .map_err(|e| e.to_string())?
            .cast(&DataType::Int32)
            .map_err(|e| e.to_string())?
            .i32()
            .map_err(|e| e.to_string())?
            .into_no_null_iter()
            .collect();
        result.push(values);
    }
    Ok(result)
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
    fn make_encoded_frame(num_tickers: i32, rows_per_ticker: usize) -> DataFrame {
        let total = num_tickers as usize * rows_per_ticker;
        let mut ticker = Vec::with_capacity(total);
        let mut timestamp = Vec::with_capacity(total);
        let mut close = Vec::with_capacity(total);
        let mut daily_return = Vec::with_capacity(total);
        for t in 0..num_tickers {
            for r in 0..rows_per_ticker {
                ticker.push(t);
                timestamp.push((r as i64) * 86_400_000);
                close.push(100.0_f64 + r as f64);
                daily_return.push(0.01_f32 * (r as f32 + t as f32));
            }
        }
        let ones_f = vec![1.0_f64; total];
        let ones_i = vec![1_i32; total];
        DataFrame::new(vec![
            Column::new("ticker".into(), ticker),
            Column::new("timestamp".into(), timestamp),
            Column::new("open_price".into(), ones_f.clone()),
            Column::new("high_price".into(), ones_f.clone()),
            Column::new("low_price".into(), ones_f.clone()),
            Column::new("close_price".into(), close),
            Column::new("volume".into(), ones_f.clone()),
            Column::new("volume_weighted_average_price".into(), ones_f),
            Column::new("daily_return".into(), daily_return),
            Column::new("day_of_week".into(), ones_i.clone()),
            Column::new("day_of_month".into(), ones_i.clone()),
            Column::new("day_of_year".into(), ones_i.clone()),
            Column::new("month".into(), ones_i.clone()),
            Column::new("year".into(), ones_i.clone()),
            Column::new("sector".into(), ones_i.clone()),
            Column::new("industry".into(), ones_i),
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

    #[test]
    fn test_get_dataset_predict_one_window_per_ticker() {
        let data = empty_data(make_encoded_frame(3, 6));
        let dataset = data.get_dataset("predict", 0.8, 2, 1).unwrap();
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
        let dataset = data.get_dataset("train", 0.8, 3, 2).unwrap();
        let sample_count = dataset.len();
        assert!(sample_count > 0);
        let targets = dataset.targets.expect("train dataset must have targets");
        assert_eq!(targets.shape()[1], 2); // output_length
        assert_eq!(targets.shape()[2], 1);
        assert_eq!(targets.shape()[0], sample_count);
    }

    #[test]
    fn test_split_by_timestamp_partitions_rows() {
        let data = empty_data(make_encoded_frame(1, 10));
        let (train, valid) = data.split_by_timestamp(0.8).unwrap();
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
        // Mirrors the Python pct_change().over("ticker"): each ticker's first
        // row has a null daily_return (dropped later by clean_data), never a
        // synthetic zero and never a value carried across the ticker boundary.
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
        // Python's CleanData filters null/NaN/non-finite daily_return rows, so
        // each ticker's first observation never reaches the scaler or windows.
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
        assert!(returns.iter().all(|r| (r - 0.1).abs() < 1e-6));
    }

    #[test]
    fn test_engineer_features_day_of_week_is_monday_based_one_to_seven() {
        // Python uses polars dt.weekday(): Monday = 1 .. Sunday = 7.
        let monday = chrono::NaiveDate::from_ymd_opt(2026, 6, 8)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap()
            .and_utc()
            .timestamp_millis();
        let sunday = chrono::NaiveDate::from_ymd_opt(2026, 6, 14)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap()
            .and_utc()
            .timestamp_millis();

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

    #[test]
    fn test_split_by_timestamp_boundary_row_goes_to_train() {
        // Python: train is date <= split, validation is date > split. With 11
        // daily rows (0..=10 days) and split 0.8 the cutoff lands exactly on
        // day 8, which must belong to train.
        let data = empty_data(make_encoded_frame(1, 11));
        let (train, valid) = data.split_by_timestamp(0.8).unwrap();
        assert_eq!(train.height(), 9);
        assert_eq!(valid.height(), 2);
    }
}
