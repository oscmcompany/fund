//! Fit the scaler and categorical mappings from training data, and serialize the artifact JSON
//! files the inference path loads.
//!
//! Categoricals are encoded over sorted-unique values so the mapping is deterministic across runs.
//!
//! The scaler and the mappings are deliberately fitted over different row sets; see [`fit`].

use std::collections::HashSet;
use std::path::Path;

use polars::prelude::*;

use crate::models::tide::configuration::ModelParameters;
use crate::models::tide::data::{
    apply_scaling, clean_data, encode_categoricals, engineer_features, split_at_cutoff,
    training_cutoff, CategoryMapping, Data, FeatureMappings, Scaler, TrainingFraction,
    CATEGORICAL_COLUMNS, CONTINUOUS_COLUMNS, STATIC_CATEGORICAL_COLUMNS,
};
use crate::models::tide::TideError;

/// Result of fitting: the preprocessed (scaled + encoded) data ready for
/// windowing, alongside the fitted scaler and mappings.
pub struct FitResult {
    pub data: Data,
    pub scaler: Scaler,
    pub mappings: FeatureMappings,
}

/// Fit preprocessing on a raw consolidated frame (bars joined with categories).
///
/// The scaler is fitted on the rows at or before `training_fraction`'s cutoff and then applied to
/// the whole frame, so the validation rows are standardized by statistics that never saw them.
/// Fitting over every row instead put the validation period's own volatility in the divisor of the
/// `daily_return` target — and [`crate::models::tide::evaluate`] scores in scaled units, so the
/// reported CRPS was denominated in a scale the held-out data had helped set.
///
/// The **mappings are fitted over the whole frame**, and that asymmetry is deliberate. A mapping is
/// a vocabulary, not a statistic: it carries which symbols, sectors, and industries exist, and
/// nothing about their returns. Restricting it to the training rows would instead be actively
/// harmful, because [`encode_categoricals`] drops rows whose static value it cannot map — every
/// instrument first listed (or first clearing the liquidity floors) after the cutoff would vanish
/// from the validation split, and, since these mappings ship in the artifact as the inference
/// vocabulary, could not be predicted at all until it had traded the full pre-cutoff window.
pub fn fit(raw: DataFrame, training_fraction: TrainingFraction) -> Result<FitResult, TideError> {
    let engineered = engineer_features(raw)?;
    let cleaned = clean_data(engineered)?;

    let cutoff = training_cutoff(&cleaned, training_fraction)?;
    let (training_rows, _) = split_at_cutoff(&cleaned, cutoff)?;
    let scaler = fit_scaler(&training_rows)?;
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
///
/// `data` is the training side of the split, not the whole frame. An empty one is refused rather
/// than fitted: `mean()` and `std()` both return `None` over no rows, every mean would fall back to
/// `0.0` and every deviation to the `1e-8` floor, and the result passes [`Scaler::new`] while
/// scaling by a hundred million.
fn fit_scaler(data: &DataFrame) -> Result<Scaler, TideError> {
    if data.height() == 0 {
        return Err(TideError::Data(
            "No rows at or before the training cutoff to fit the scaler on".to_string(),
        ));
    }

    let mut means = std::collections::HashMap::new();
    let mut standard_deviations = std::collections::HashMap::new();

    for column in CONTINUOUS_COLUMNS {
        let series = data.column(column)?.cast(&DataType::Float64)?;
        let values = series.f64()?;
        let mean = values.mean().unwrap_or(0.0);
        let standard_deviation = values.std(1).unwrap_or(0.0);
        let standard_deviation = if standard_deviation == 0.0 {
            1e-8
        } else {
            standard_deviation
        };
        means.insert((*column).to_string(), mean);
        standard_deviations.insert((*column).to_string(), standard_deviation);
    }

    Scaler::new(means, standard_deviations).map_err(|reason| {
        TideError::Data(format!(
            "Fitted scaler is unusable, so training would produce a bad artifact: {reason}"
        ))
    })
}

/// Build deterministic value->index maps for the static categorical columns.
fn fit_mappings(data: &DataFrame) -> Result<FeatureMappings, TideError> {
    let mut mappings = FeatureMappings::new();
    for column in STATIC_CATEGORICAL_COLUMNS {
        mappings.insert((*column).to_string(), build_mapping(data, column)?);
    }
    Ok(mappings)
}

fn build_mapping(data: &DataFrame, column: &str) -> Result<CategoryMapping, TideError> {
    let mut values: Vec<String> = data
        .column(column)?
        .str()?
        .into_no_null_iter()
        .map(|name| name.to_string())
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

/// Row-level training filter: thresholds are inclusive, and tickers containing lowercase letters
/// are dropped — they are distinct instruments that would collide with the uppercase ticker after
/// cleaning.
pub fn filter_training_bars(
    data: DataFrame,
    minimum_close_price: f64,
    minimum_volume: f64,
) -> Result<DataFrame, TideError> {
    let close_prices = data.column("close_price")?.cast(&DataType::Float64)?;
    let close_prices = close_prices.f64()?;
    let volumes = data.column("volume")?.cast(&DataType::Float64)?;
    let volumes = volumes.f64()?;
    let tickers = data.column("ticker")?.str()?;

    let mask: BooleanChunked = close_prices
        .into_iter()
        .zip(volumes)
        .zip(tickers)
        .map(|((close_price, volume), ticker)| {
            close_price.is_some_and(|value| value >= minimum_close_price)
                && volume.is_some_and(|value| value >= minimum_volume)
                && ticker
                    .is_some_and(|value| !value.chars().any(|character| character.is_lowercase()))
        })
        .collect();

    let filtered = data.filter(&mask)?;
    Ok(filtered)
}

/// Write the three artifact JSON files (scaler, mappings, parameters) the
/// inference loader reads, into `directory`.
///
/// Each file is serialized to a temporary name and renamed into place, and every rename happens
/// after every serialization. A reader therefore never sees a half-written file, and the window in
/// which the three could disagree shrinks from "between two writes" — which spans serializing a
/// scaler over every continuous column — to three consecutive renames.
///
/// This is a narrowed window rather than an atomic set, and the distinction is worth stating: POSIX
/// gives atomicity per rename, not across three. Closing it completely would mean staging a
/// directory and renaming that. The mixed-artifact failure this guards against is a crash *during*
/// the writes, which is where essentially all of the wall clock is.
pub fn write_artifact_json(
    directory: &Path,
    scaler: &Scaler,
    mappings: &FeatureMappings,
    parameters: &ModelParameters,
) -> Result<(), TideError> {
    std::fs::create_dir_all(directory)?;

    let scaler_json = serde_json::json!({
        "means": scaler.means(),
        "standard_deviations": scaler.standard_deviations(),
        "continuous_columns": CONTINUOUS_COLUMNS,
        "categorical_columns": CATEGORICAL_COLUMNS,
        "static_categorical_columns": STATIC_CATEGORICAL_COLUMNS,
    });

    let staged = [
        (
            "tide_data_scaler.json",
            serde_json::to_string_pretty(&scaler_json)?,
        ),
        (
            "tide_data_mappings.json",
            serde_json::to_string_pretty(mappings)?,
        ),
        (
            "tide_parameters.json",
            serde_json::to_string_pretty(parameters)?,
        ),
    ];

    let mut renames = Vec::with_capacity(staged.len());
    for (name, contents) in &staged {
        // Same directory as the destination, so the rename stays within one filesystem.
        let temporary = directory.join(format!("{name}.partial"));
        if let Err(error) = std::fs::write(&temporary, contents) {
            // Leave nothing behind on the way out. A surviving `.partial` would be packaged into
            // the tarball and shipped, and on the next run it is the stale half of exactly the
            // mixed-artifact state this staging exists to prevent.
            for (orphan, _) in &renames {
                let _ = std::fs::remove_file(orphan);
            }
            let _ = std::fs::remove_file(&temporary);
            return Err(error.into());
        }
        renames.push((temporary, directory.join(name)));
    }

    for (temporary, destination) in renames {
        std::fs::rename(&temporary, &destination)?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The fraction the trainer runs with, so the tests exercise the split production uses.
    fn training_fraction() -> TrainingFraction {
        TrainingFraction::new(0.8).expect("0.8 is a valid training fraction")
    }

    /// Build a raw frame from `(ticker, sector, industry, day, close_price)` rows. The remaining
    /// price columns track the close, and `day` is a session index turned into a midnight stamp.
    fn frame_from_rows(rows: &[(&str, &str, &str, i64, f64)]) -> DataFrame {
        let tickers: Vec<&str> = rows.iter().map(|row| row.0).collect();
        let sectors: Vec<&str> = rows.iter().map(|row| row.1).collect();
        let industries: Vec<&str> = rows.iter().map(|row| row.2).collect();
        let timestamps: Vec<i64> = rows.iter().map(|row| row.3 * 86_400_000).collect();
        let close_prices: Vec<f64> = rows.iter().map(|row| row.4).collect();
        let volumes: Vec<f64> = close_prices.iter().map(|price| price * 1_000.0).collect();

        DataFrame::new(vec![
            Column::new("ticker".into(), tickers),
            Column::new("timestamp".into(), timestamps),
            Column::new("open_price".into(), close_prices.clone()),
            Column::new("high_price".into(), close_prices.clone()),
            Column::new("low_price".into(), close_prices.clone()),
            Column::new("close_price".into(), close_prices.clone()),
            Column::new("volume".into(), volumes),
            Column::new("volume_weighted_average_price".into(), close_prices),
            Column::new("sector".into(), sectors),
            Column::new("industry".into(), industries),
        ])
        .unwrap()
    }

    /// One ticker over eleven sessions whose last two sit on a wholly different scale.
    ///
    /// `clean_data` drops each ticker's first row for its null return, so the fitted frame spans
    /// days 1 through 10 and the 0.8 cutoff lands at day 8.2: days 1 through 8 train, days 9 and 10
    /// validate. Close prices run 101 through 108 over the training days and jump to 5,000 over the
    /// validation days, which is what makes a leaked statistic impossible to mistake for noise.
    fn scale_shifted_frame() -> DataFrame {
        let close_prices = [
            100.0, 101.0, 102.0, 103.0, 104.0, 105.0, 106.0, 107.0, 108.0, 5_000.0, 5_100.0,
        ];
        let rows: Vec<(&str, &str, &str, i64, f64)> = close_prices
            .iter()
            .enumerate()
            .map(|(day, close_price)| ("AAAA", "TECH", "SOFTWARE", day as i64, *close_price))
            .collect();
        frame_from_rows(&rows)
    }

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
        let result = fit(raw_frame(), training_fraction()).unwrap();
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
        let result = fit(raw_frame(), training_fraction()).unwrap();
        for column in CONTINUOUS_COLUMNS {
            assert!(result.scaler.means().contains_key(*column));
            assert!(result.scaler.standard_deviations().contains_key(*column));
            assert!(*result.scaler.standard_deviations().get(*column).unwrap() != 0.0);
        }
    }

    /// The leak this module was changed to close: the scaler used to be fitted over every cleaned
    /// row, so the held-out period set the scale its own predictions were later measured on.
    #[test]
    fn test_the_scaler_is_fitted_only_on_rows_at_or_before_the_training_cutoff() {
        let result = fit(scale_shifted_frame(), training_fraction()).unwrap();

        // Close prices 101 through 108 over the eight training days: mean 104.5, sample standard
        // deviation sqrt(6). Fitted over every row the mean would be 1093.6 instead, dragged there
        // by the two validation sessions.
        let mean = result.scaler.means()["close_price"];
        let standard_deviation = result.scaler.standard_deviations()["close_price"];
        assert!(
            (mean - 104.5).abs() < 1e-9,
            "close_price mean is {mean}, not the training window's 104.5"
        );
        assert!(
            (standard_deviation - 6.0_f64.sqrt()).abs() < 1e-9,
            "close_price standard deviation is {standard_deviation}, not the training window's \
             sqrt(6)"
        );

        // The one that matters most. `daily_return` is the model target and `evaluate` scores in
        // scaled units, so this divisor sets what a good CRPS even means. The training days return
        // about a percent each; day nine returns 4,529 percent. Over every row the deviation
        // exceeds 10 — the leak is three orders of magnitude, not a rounding difference.
        let return_deviation = result.scaler.standard_deviations()["daily_return"];
        assert!(
            return_deviation < 0.01,
            "daily_return standard deviation is {return_deviation}, so the validation window's \
             volatility reached the target scale"
        );
    }

    /// `fit` and `Data::split_by_timestamp` derive the cutoff separately, and the scaler is only
    /// honest if they land on the same instant. Standardizing a window by its own statistics leaves
    /// mean 0 and sample deviation 1 exactly, so a disagreement of even one session shows up here.
    #[test]
    fn test_the_scaler_window_matches_the_rows_the_training_split_yields() {
        let result = fit(scale_shifted_frame(), training_fraction()).unwrap();
        let (train, validation) = result.data.split_by_timestamp(training_fraction()).unwrap();

        assert_eq!(train.height(), 8, "days 1 through 8 train");
        assert_eq!(validation.height(), 2, "days 9 and 10 validate");

        let scaled = train.column("close_price").unwrap().f32().unwrap();
        let mean = scaled.mean().unwrap();
        let standard_deviation = scaled.std(1).unwrap();
        assert!(
            mean.abs() < 1e-5,
            "the training split does not centre on zero under its own scaler: {mean}"
        );
        assert!(
            (standard_deviation - 1.0).abs() < 1e-5,
            "the training split is not unit-scaled under its own scaler: {standard_deviation}"
        );

        // The validation rows are scaled by the same statistics rather than their own, which is the
        // whole point: 5,000 against a mean of 104.5 is enormously far from zero.
        let held_out = validation.column("close_price").unwrap().f32().unwrap();
        assert!(
            held_out.into_no_null_iter().all(|value| value > 100.0),
            "the validation rows were not scaled by the training statistics"
        );
    }

    /// The deliberate asymmetry: the mappings stay fitted over the whole frame. A vocabulary
    /// carries which instruments exist, not how they returned, and `encode_categoricals` deletes
    /// rows whose static value it cannot map — so a training-only mapping would silently drop every
    /// late listing out of the validation split and out of the shipped inference vocabulary.
    #[test]
    fn test_mappings_cover_categories_that_appear_only_after_the_cutoff() {
        let mut rows: Vec<(&str, &str, &str, i64, f64)> = (0..=10)
            .map(|day| ("AAAA", "TECH", "SOFTWARE", day, 100.0 + day as f64))
            .collect();
        // Listed on day nine, which the 0.8 cutoff at day 8.2 puts wholly in the validation window.
        rows.push(("ZZZZ", "ENERGY", "SOLAR", 9, 50.0));
        rows.push(("ZZZZ", "ENERGY", "SOLAR", 10, 51.0));

        let result = fit(frame_from_rows(&rows), training_fraction()).unwrap();

        assert!(
            result.mappings["ticker"].contains_key("ZZZZ"),
            "a ticker listed after the cutoff must still be in the vocabulary"
        );
        assert!(result.mappings["sector"].contains_key("ENERGY"));
        assert!(result.mappings["industry"].contains_key("SOLAR"));

        // `clean_data` spends ZZZZ's first session on its null return, leaving one encoded row.
        let late_code = result.mappings["ticker"]["ZZZZ"];
        let encoded = result.data.data.column("ticker").unwrap().i32().unwrap();
        assert_eq!(
            encoded
                .into_no_null_iter()
                .filter(|code| *code == late_code)
                .count(),
            1,
            "the late listing's rows were dropped from the encoded frame"
        );
    }

    #[test]
    fn test_filter_training_bars_is_row_level_and_inclusive() {
        // The filter is per row, so a ticker with one qualifying row and one penny row keeps
        // only the qualifying row. Rows exactly at the threshold are kept.
        let data = DataFrame::new(vec![
            Column::new("ticker".into(), vec!["AAA", "AAA", "BBB"]),
            Column::new("timestamp".into(), vec![0_i64, 86_400_000, 0]),
            Column::new("close_price".into(), vec![0.5_f64, 1.0, 250.0]),
            Column::new("volume".into(), vec![100_000.0_f64, 100_000.0, 50_000.0]),
        ])
        .unwrap();

        let filtered = filter_training_bars(data, 1.0, 100_000.0).unwrap();
        assert_eq!(filtered.height(), 1);
        let tickers: Vec<&str> = filtered
            .column("ticker")
            .unwrap()
            .str()
            .unwrap()
            .into_no_null_iter()
            .collect();
        assert_eq!(tickers, vec!["AAA"]);
        let close: Vec<f64> = filtered
            .column("close_price")
            .unwrap()
            .f64()
            .unwrap()
            .into_no_null_iter()
            .collect();
        assert_eq!(close, vec![1.0]);
    }

    #[test]
    fn test_filter_training_bars_drops_lowercase_tickers() {
        // Tickers containing lowercase letters are distinct instruments that would collide
        // once uppercased.
        let data = DataFrame::new(vec![
            Column::new("ticker".into(), vec!["AAPL", "AAPLw", "brk.a"]),
            Column::new("timestamp".into(), vec![0_i64, 0, 0]),
            Column::new("close_price".into(), vec![150.0_f64, 150.0, 150.0]),
            Column::new(
                "volume".into(),
                vec![1_000_000.0_f64, 1_000_000.0, 1_000_000.0],
            ),
        ])
        .unwrap();

        let filtered = filter_training_bars(data, 1.0, 100_000.0).unwrap();
        let tickers: Vec<&str> = filtered
            .column("ticker")
            .unwrap()
            .str()
            .unwrap()
            .into_no_null_iter()
            .collect();
        assert_eq!(tickers, vec!["AAPL"]);
    }

    /// The staging files must not survive a successful write. A leftover `.partial` would be
    /// packaged into the tarball and shipped, and on the next run it would be the stale half of
    /// exactly the mixed-artifact state the staging exists to prevent.
    #[test]
    fn test_write_artifact_json_leaves_no_staging_files_behind() {
        let result = fit(raw_frame(), training_fraction()).unwrap();
        let parameters = ModelParameters::new(448, 35, 5);
        let directory = tempfile::tempdir().unwrap();

        write_artifact_json(
            directory.path(),
            &result.scaler,
            &result.mappings,
            &parameters,
        )
        .unwrap();

        let written: Vec<String> = std::fs::read_dir(directory.path())
            .unwrap()
            .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
            .collect();

        assert!(
            !written.iter().any(|name| name.ends_with(".partial")),
            "staging files survived the write: {written:?}"
        );
        assert_eq!(
            written.len(),
            3,
            "expected exactly the three artifact files: {written:?}"
        );
    }

    #[test]
    fn test_write_artifact_json_round_trips_via_loader() {
        let result = fit(raw_frame(), training_fraction()).unwrap();
        let parameters = ModelParameters::new(448, 35, 5);
        let directory = tempfile::tempdir().unwrap();
        write_artifact_json(
            directory.path(),
            &result.scaler,
            &result.mappings,
            &parameters,
        )
        .unwrap();

        // The inference-side loaders must read what we wrote. `Scaler::load` now checks the column
        // lists against this build's constants itself, so reaching this line at all is the
        // assertion that the three lists round-tripped intact.
        let scaler = Scaler::load(&directory.path().join("tide_data_scaler.json")).unwrap();
        assert!(scaler.means().contains_key("daily_return"));

        let loaded_parameters =
            ModelParameters::load(&directory.path().join("tide_parameters.json")).unwrap();
        assert_eq!(loaded_parameters.input_size(), 448);
    }
}
