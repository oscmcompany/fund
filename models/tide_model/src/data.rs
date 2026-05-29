use std::collections::HashMap;

use chrono::Datelike;
use polars::prelude::*;
use serde::{Deserialize, Serialize};

pub type CategoryMapping = HashMap<String, i32>;
pub type FeatureMappings = HashMap<String, CategoryMapping>;

pub fn selector_by_names(names: &[&str]) -> Vec<PlSmallStr> {
    names
        .iter()
        .map(|name| PlSmallStr::from(*name))
        .collect()
}



#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Scaler {
    pub means: HashMap<String, f64>,
    pub standard_deviations: HashMap<String, f64>,
}

impl Scaler {
    #[allow(clippy::type_complexity)]
    pub fn load(path: &std::path::Path) -> Result<(Self, Vec<String>, Vec<String>, Vec<String>), Box<dyn std::error::Error>> {
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

const CONTINUOUS_COLUMNS: &[&str] = &[
    "open_price",
    "high_price",
    "low_price",
    "close_price",
    "volume",
    "volume_weighted_average_price",
    "daily_return",
];

const CATEGORICAL_COLUMNS: &[&str] = &[
    "day_of_week",
    "day_of_month",
    "day_of_year",
    "month",
    "year",
];

const STATIC_CATEGORICAL_COLUMNS: &[&str] = &[
    "ticker",
    "sector",
    "industry",
];

pub struct Data {
    pub data: DataFrame,
    pub scaler: Scaler,
    pub mappings: FeatureMappings,
    pub continuous_columns: Vec<String>,
    pub categorical_columns: Vec<String>,
    pub static_categorical_columns: Vec<String>,
}

impl Data {
    pub fn apply_existing_scaler(
        data: DataFrame,
        scaler: &Scaler,
        mappings: &FeatureMappings,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let data = engineer_features(data)?;
        let data = clean_data(data)?;
        let data = apply_scaling(data, scaler)?;
        let data = encode_categoricals(data, mappings)?;

        Ok(Self {
            data,
            scaler: scaler.clone(),
            mappings: mappings.clone(),
            continuous_columns: CONTINUOUS_COLUMNS.iter().map(|s| s.to_string()).collect(),
            categorical_columns: CATEGORICAL_COLUMNS.iter().map(|s| s.to_string()).collect(),
            static_categorical_columns: STATIC_CATEGORICAL_COLUMNS.iter().map(|s| s.to_string()).collect(),
        })
    }

    pub fn get_dataset(
        &self,
        data_type: &str,
        _validation_split: f64,
        input_length: usize,
        output_length: usize,
    ) -> Result<TrainingDataset, Box<dyn std::error::Error>> {
        let window_size = input_length + output_length;
        let n_cont = CONTINUOUS_COLUMNS.len();
        let n_cat = CATEGORICAL_COLUMNS.len();
        let n_static = STATIC_CATEGORICAL_COLUMNS.len();

        let tickers: Vec<String> = self
            .data
            .column("ticker")
            .map_err(|e| e.to_string())?
            .str()
            .map_err(|e| e.to_string())?
            .into_no_null_iter()
            .map(|s| s.to_string())
            .collect::<std::collections::HashSet<_>>()
            .into_iter()
            .collect();

        let mut all_past_cont: Vec<Vec<f32>> = Vec::new();
        let mut all_past_cat: Vec<Vec<i32>> = Vec::new();
        let mut all_future_cat: Vec<Vec<i32>> = Vec::new();
        let mut all_static_cat: Vec<Vec<i32>> = Vec::new();

        for ticker in &tickers {
            let mask = self
                .data
                .column("ticker")
                .map_err(|e| e.to_string())?
                .str()
                .map_err(|e| e.to_string())?
                .equal(ticker.as_str());
            let ticker_data = self.data.filter(&mask).map_err(|e| e.to_string())?;

            if ticker_data.height() < window_size {
                continue;
            }

            let cont_arrays = get_float_columns(&ticker_data, CONTINUOUS_COLUMNS)?;
            let cat_arrays = get_int_columns(&ticker_data, CATEGORICAL_COLUMNS)?;
            let static_arrays = get_int_columns(&ticker_data, STATIC_CATEGORICAL_COLUMNS)?;

            let windows: Vec<usize> = if data_type == "predict" {
                if ticker_data.height() >= window_size {
                    vec![ticker_data.height() - window_size]
                } else {
                    vec![]
                }
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

        Ok(TrainingDataset {
            past_continuous,
            past_categorical,
            future_categorical,
            static_categorical,
            targets: None,
        })
    }
}

fn engineer_features(data: DataFrame) -> Result<DataFrame, Box<dyn std::error::Error>> {
    let timestamps = data.column("timestamp").map_err(|e| e.to_string())?;
    let height = data.height();

    let mut day_of_week = Vec::with_capacity(height);
    let mut day_of_month = Vec::with_capacity(height);
    let mut day_of_year = Vec::with_capacity(height);
    let mut month = Vec::with_capacity(height);
    let mut year = Vec::with_capacity(height);
    let mut daily_return = Vec::with_capacity(height);

    let close_prices: Vec<f64> = data
        .column("close_price")
        .map_err(|e| e.to_string())?
        .f64()
        .map_err(|e| e.to_string())?
        .into_no_null_iter()
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

        day_of_week.push(date.weekday().num_days_from_monday() as i32);
        day_of_month.push(date.day() as i32);
        day_of_year.push(date.ordinal() as i32);
        month.push(date.month() as i32);
        year.push(date.year());

        if i > 0 && close_prices[i - 1] != 0.0 {
            daily_return.push(((close_prices[i] / close_prices[i - 1]) - 1.0) as f32);
        } else {
            daily_return.push(0.0f32);
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

fn clean_data(mut data: DataFrame) -> Result<DataFrame, Box<dyn std::error::Error>> {
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
    Ok(cleaned)
}

fn apply_scaling(data: DataFrame, scaler: &Scaler) -> Result<DataFrame, Box<dyn std::error::Error>> {
    let mut result = data;
    for col_name in CONTINUOUS_COLUMNS {
        let mean = scaler.means.get(*col_name).copied().unwrap_or(0.0);
        let std = scaler.standard_deviations.get(*col_name).copied().unwrap_or(1.0);
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

fn encode_categoricals(
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
                        .map(|opt_val| {
                            opt_val.and_then(|val| mapping.get(val).copied())
                        })
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
}
