//! Fit the scaler and categorical mappings from training data, and serialize
//! the artifact JSON files the inference path loads.
//!
//! Mirrors the Python trainer's preprocessing: z-score the continuous columns,
//! integer-encode `ticker`/`sector`/`industry` over sorted-unique values (so the
//! mapping is deterministic and interchangeable), and leave the calendar
//! categoricals as their raw integers.

use std::collections::HashSet;
use std::path::Path;

use polars::prelude::*;

use crate::models::tide::config::ModelParameters;
use crate::models::tide::data::{
    apply_scaling, clean_data, encode_categoricals, engineer_features, CategoryMapping, Data,
    FeatureMappings, Scaler, CATEGORICAL_COLUMNS, CONTINUOUS_COLUMNS, STATIC_CATEGORICAL_COLUMNS,
};

/// Result of fitting: the preprocessed (scaled + encoded) data ready for
/// windowing, alongside the fitted scaler and mappings.
pub struct FitResult {
    pub data: Data,
    pub scaler: Scaler,
    pub mappings: FeatureMappings,
}

/// Fit preprocessing on a raw consolidated frame (bars joined with categories).
pub fn fit(raw: DataFrame) -> Result<FitResult, Box<dyn std::error::Error>> {
    let engineered = engineer_features(raw)?;
    let cleaned = clean_data(engineered)?;

    let scaler = fit_scaler(&cleaned)?;
    let scaled = apply_scaling(cleaned, &scaler)?;

    let mappings = fit_mappings(&scaled)?;
    let encoded = encode_categoricals(scaled, &mappings)?;

    let data = Data::from_parts(encoded, scaler.clone(), mappings.clone());
    Ok(FitResult {
        data,
        scaler,
        mappings,
    })
}

/// Compute per-column mean and (sample) standard deviation for the continuous
/// columns. A zero std is replaced with a tiny value so scaling and inverse
/// scaling stay finite.
fn fit_scaler(data: &DataFrame) -> Result<Scaler, Box<dyn std::error::Error>> {
    let mut means = std::collections::HashMap::new();
    let mut standard_deviations = std::collections::HashMap::new();

    for column in CONTINUOUS_COLUMNS {
        let series = data
            .column(column)
            .map_err(|e| e.to_string())?
            .cast(&DataType::Float64)
            .map_err(|e| e.to_string())?;
        let values = series.f64().map_err(|e| e.to_string())?;
        let mean = values.mean().unwrap_or(0.0);
        let std = values.std(1).unwrap_or(0.0);
        let std = if std == 0.0 { 1e-8 } else { std };
        means.insert((*column).to_string(), mean);
        standard_deviations.insert((*column).to_string(), std);
    }

    Ok(Scaler {
        means,
        standard_deviations,
    })
}

/// Build deterministic value->index maps for the static categorical columns.
fn fit_mappings(data: &DataFrame) -> Result<FeatureMappings, Box<dyn std::error::Error>> {
    let mut mappings = FeatureMappings::new();
    for column in STATIC_CATEGORICAL_COLUMNS {
        mappings.insert((*column).to_string(), build_mapping(data, column)?);
    }
    Ok(mappings)
}

fn build_mapping(
    data: &DataFrame,
    column: &str,
) -> Result<CategoryMapping, Box<dyn std::error::Error>> {
    let mut values: Vec<String> = data
        .column(column)
        .map_err(|e| e.to_string())?
        .str()
        .map_err(|e| e.to_string())?
        .into_no_null_iter()
        .map(|s| s.to_string())
        .collect::<HashSet<_>>()
        .into_iter()
        .collect();
    values.sort();
    Ok(values
        .into_iter()
        .enumerate()
        .map(|(index, value)| (value, index as i32))
        .collect())
}

/// Write the three artifact JSON files (scaler, mappings, parameters) the
/// inference loader reads, into `directory`.
pub fn write_artifact_json(
    directory: &Path,
    scaler: &Scaler,
    mappings: &FeatureMappings,
    parameters: &ModelParameters,
) -> Result<(), Box<dyn std::error::Error>> {
    std::fs::create_dir_all(directory)?;

    let scaler_json = serde_json::json!({
        "means": scaler.means,
        "standard_deviations": scaler.standard_deviations,
        "continuous_columns": CONTINUOUS_COLUMNS,
        "categorical_columns": CATEGORICAL_COLUMNS,
        "static_categorical_columns": STATIC_CATEGORICAL_COLUMNS,
    });
    std::fs::write(
        directory.join("tide_data_scaler.json"),
        serde_json::to_string_pretty(&scaler_json)?,
    )?;

    std::fs::write(
        directory.join("tide_data_mappings.json"),
        serde_json::to_string_pretty(mappings)?,
    )?;

    std::fs::write(
        directory.join("tide_parameters.json"),
        serde_json::to_string_pretty(parameters)?,
    )?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn raw_frame() -> DataFrame {
        // Two tickers, two days each; unsorted on input.
        DataFrame::new(vec![
            Column::new("ticker".into(), vec!["goog", "aapl", "goog", "aapl"]),
            Column::new("timestamp".into(), vec![0_i64, 0, 86_400_000, 86_400_000]),
            Column::new("open_price".into(), vec![10.0_f64, 20.0, 11.0, 21.0]),
            Column::new("high_price".into(), vec![10.0_f64, 20.0, 11.0, 21.0]),
            Column::new("low_price".into(), vec![10.0_f64, 20.0, 11.0, 21.0]),
            Column::new("close_price".into(), vec![10.0_f64, 20.0, 11.0, 21.0]),
            Column::new("volume".into(), vec![100.0_f64, 200.0, 110.0, 210.0]),
            Column::new(
                "volume_weighted_average_price".into(),
                vec![10.0_f64, 20.0, 11.0, 21.0],
            ),
            Column::new("sector".into(), vec!["tech", "tech", "tech", "tech"]),
            Column::new("industry".into(), vec!["web", "phones", "web", "phones"]),
        ])
        .unwrap()
    }

    #[test]
    fn test_fit_mappings_are_sorted_and_deterministic() {
        let result = fit(raw_frame()).unwrap();
        let tickers = &result.mappings["ticker"];
        // Uppercased and sorted: AAPL -> 0, GOOG -> 1.
        assert_eq!(tickers["AAPL"], 0);
        assert_eq!(tickers["GOOG"], 1);

        let industries = &result.mappings["industry"];
        // PHONES -> 0, WEB -> 1.
        assert_eq!(industries["PHONES"], 0);
        assert_eq!(industries["WEB"], 1);
    }

    #[test]
    fn test_fit_scaler_has_all_continuous_columns() {
        let result = fit(raw_frame()).unwrap();
        for column in CONTINUOUS_COLUMNS {
            assert!(result.scaler.means.contains_key(*column));
            assert!(result.scaler.standard_deviations.contains_key(*column));
            assert!(*result.scaler.standard_deviations.get(*column).unwrap() != 0.0);
        }
    }

    #[test]
    fn test_write_artifact_json_round_trips_via_loader() {
        let result = fit(raw_frame()).unwrap();
        let parameters = ModelParameters {
            input_size: 448,
            ..Default::default()
        };
        let dir = tempfile::tempdir().unwrap();
        write_artifact_json(dir.path(), &result.scaler, &result.mappings, &parameters).unwrap();

        // The inference-side loaders must read what we wrote.
        let (scaler, continuous_columns, _, static_columns) =
            Scaler::load(&dir.path().join("tide_data_scaler.json")).unwrap();
        assert_eq!(continuous_columns.len(), CONTINUOUS_COLUMNS.len());
        assert_eq!(static_columns.len(), STATIC_CATEGORICAL_COLUMNS.len());
        assert!(scaler.means.contains_key("daily_return"));

        let loaded_parameters =
            ModelParameters::load(&dir.path().join("tide_parameters.json")).unwrap();
        assert_eq!(loaded_parameters.input_size, 448);
    }
}
