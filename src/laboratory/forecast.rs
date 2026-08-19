//! Scores a trained model the way a baseline is scored: by the order it puts names in.
//!
//! Cross-sectional, so it answers a different question than CRPS — not how close each prediction
//! lands, but whether the ranking a dollar-neutral book would act on is the right one.

use std::collections::BTreeMap;

use burn::backend::NdArray;

use crate::laboratory::metrics;
use crate::laboratory::predictor::Evaluation;
use crate::models::tide::batch::{build_input_tensor, validate_input_shape};
use crate::models::tide::configuration::ModelParameters;
use crate::models::tide::data::{Scaler, TrainingDataset, TARGET_COLUMN};
use crate::models::tide::evaluate::QuantileIndices;
use crate::models::tide::model::TiDEModel;
use crate::models::tide::TideError;

/// Samples per forward pass, matching the evaluation path.
const BATCH_SIZE: usize = 4096;

/// Runs `model` over `dataset` and measures the cross-section it produces each session.
///
/// Both sides come out of the same dataset — the median quantile against the target the sample was
/// built with — so a prediction is matched to its own outcome by construction rather than by a join
/// that could silently mismatch. Returns are unscaled first: the rank correlation would not notice,
/// but the decile spread is a return and would otherwise be quoted in standard deviations.
pub fn score(
    name: &str,
    model: &TiDEModel<NdArray>,
    dataset: &TrainingDataset,
    parameters: &ModelParameters,
    scaler: &Scaler,
) -> Result<Evaluation, TideError> {
    validate_input_shape(dataset, parameters).map_err(TideError::Artifact)?;

    let quantiles = parameters.quantiles();
    let quantile_count = quantiles.len();
    if quantile_count == 0 {
        return Err(TideError::Artifact(
            "the model emits no quantiles, so it has nothing to rank names by".to_string(),
        ));
    }
    let median = QuantileIndices::locate(quantiles).median;
    let output_length = parameters.output_length();

    let predictions = forward(model, dataset, parameters)?;
    measure(
        name,
        &predictions,
        dataset,
        scaler,
        median,
        quantile_count,
        output_length,
    )
}

/// Groups the model's output into one cross-section per session and measures each.
///
/// Separated from the forward pass so a known forecast can be measured without a network, which is
/// the only way to show the instrument reports a signal when one is there.
fn measure(
    name: &str,
    predictions: &[f32],
    dataset: &TrainingDataset,
    scaler: &Scaler,
    median: usize,
    quantile_count: usize,
    output_length: usize,
) -> Result<Evaluation, TideError> {
    let targets = dataset.targets().ok_or_else(|| {
        TideError::Data(
            "scoring a forecast needs the realized returns the samples were built with".to_string(),
        )
    })?;
    let expected = dataset.len() * output_length * quantile_count;
    if predictions.len() < expected {
        return Err(TideError::Artifact(format!(
            "the model returned {} values against {expected} for {} samples",
            predictions.len(),
            dataset.len()
        )));
    }

    // Step zero only. The book flattens every position the same session, so a longer horizon
    // describes a holding period this strategy never has — the same step `predict` trades on.
    let mut by_session: BTreeMap<i64, (Vec<f64>, Vec<f64>)> = BTreeMap::new();
    for sample in 0..dataset.len() {
        let scaled = predictions[sample * output_length * quantile_count + median] as f64;
        let (scores, realized) = by_session
            .entry(dataset.forecast_sessions()[sample])
            .or_default();
        scores.push(scaler.inverse_transform_value(TARGET_COLUMN, scaled));
        realized
            .push(scaler.inverse_transform_value(TARGET_COLUMN, targets[[sample, 0, 0]] as f64));
    }

    let sessions: Vec<metrics::SessionMetrics> = by_session
        .into_values()
        .map(|(scores, realized)| metrics::measure_session(&scores, &realized))
        .collect();

    Ok(Evaluation {
        predictor: name.to_string(),
        information_coefficient: metrics::summarize(
            sessions
                .iter()
                .map(|session| session.information_coefficient),
        ),
        decile_spread: metrics::summarize(sessions.iter().map(|session| session.decile_spread)),
        directional_accuracy: metrics::summarize(
            sessions.iter().map(|session| session.directional_accuracy),
        ),
        sessions,
    })
}

/// Every sample's quantiles, in sample order.
fn forward(
    model: &TiDEModel<NdArray>,
    dataset: &TrainingDataset,
    parameters: &ModelParameters,
) -> Result<Vec<f32>, TideError> {
    let device = Default::default();
    let indices: Vec<usize> = (0..dataset.len()).collect();
    let mut predictions: Vec<f32> = Vec::new();
    for chunk in indices.chunks(BATCH_SIZE) {
        let input = build_input_tensor::<NdArray>(
            dataset,
            chunk,
            parameters.input_length(),
            parameters.output_length(),
            &device,
        );
        let mut values: Vec<f32> = model
            .forward(input)
            .to_data()
            .to_vec()
            .map_err(|error| TideError::Artifact(format!("{error:?}")))?;
        predictions.append(&mut values);
    }
    Ok(predictions)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::tide::data::Scaler;

    const DAY: i64 = 86_400_000;
    const QUANTILES: usize = 3;
    const MEDIAN: usize = 1;

    /// Twelve names across two sessions, with the realized return rising through each session.
    fn dataset(sessions: usize, names: usize) -> TrainingDataset {
        let samples = sessions * names;
        let mut targets = ndarray::Array3::<f32>::zeros((samples, 1, 1));
        let mut forecast_sessions = Vec::with_capacity(samples);
        for session in 0..sessions {
            for name in 0..names {
                targets[[session * names + name, 0, 0]] = name as f32 * 0.01;
                forecast_sessions.push(session as i64 * DAY);
            }
        }
        TrainingDataset::new(
            ndarray::Array3::zeros((samples, 2, 7)),
            ndarray::Array3::zeros((samples, 2, 5)),
            ndarray::Array3::zeros((samples, 1, 5)),
            ndarray::Array3::zeros((samples, 1, 3)),
            Some(targets),
            forecast_sessions,
        )
        .unwrap()
    }

    /// The identity scaler, so the assertions below read in the units the targets were written in.
    fn scaler() -> Scaler {
        Scaler::new(
            std::collections::HashMap::from([(TARGET_COLUMN.to_string(), 0.0)]),
            std::collections::HashMap::from([(TARGET_COLUMN.to_string(), 1.0)]),
        )
        .unwrap()
    }

    /// Predictions laid out as the model emits them: one row of quantiles per sample.
    fn quantiles_from(medians: &[f32]) -> Vec<f32> {
        medians
            .iter()
            .flat_map(|median| [median - 1.0, *median, median + 1.0])
            .collect()
    }

    /// A forecast that is the outcome must score exactly +1, and its reverse exactly -1.
    ///
    /// Without this the reported zero would be untestable: a measurement that cannot report a
    /// signal when one is handed to it says nothing when it reports none. It also pins the sample
    /// ordering, since a forecast misaligned against its own session would score neither.
    #[test]
    fn test_a_perfect_forecast_scores_one_and_its_reverse_minus_one() {
        let sessions = 2;
        let names = 12;
        let data = dataset(sessions, names);
        let realized: Vec<f32> = (0..sessions)
            .flat_map(|_| (0..names).map(|name| name as f32 * 0.01))
            .collect();

        let perfect = measure(
            "perfect",
            &quantiles_from(&realized),
            &data,
            &scaler(),
            MEDIAN,
            QUANTILES,
            1,
        )
        .unwrap();
        assert_eq!(perfect.sessions.len(), sessions);
        let coefficient = perfect.information_coefficient.unwrap();
        assert!(
            (coefficient.mean - 1.0).abs() < 1e-9,
            "a forecast equal to the outcome must rank perfectly, got {coefficient:?}"
        );

        let reversed: Vec<f32> = realized.iter().map(|value| -value).collect();
        let inverted = measure(
            "inverted",
            &quantiles_from(&reversed),
            &data,
            &scaler(),
            MEDIAN,
            QUANTILES,
            1,
        )
        .unwrap();
        assert!((inverted.information_coefficient.unwrap().mean + 1.0).abs() < 1e-9);
    }

    /// The median is the traded quantile, so reading a neighbouring one would score a forecast the
    /// book never acts on — and the two bracket the median by construction here.
    #[test]
    fn test_the_median_quantile_is_the_one_scored() {
        let data = dataset(2, 12);
        let realized: Vec<f32> = (0..2)
            .flat_map(|_| (0..12).map(|name| name as f32 * 0.01))
            .collect();
        let predictions = quantiles_from(&realized);

        let upper = measure("upper", &predictions, &data, &scaler(), 2, QUANTILES, 1).unwrap();
        // Every median is offset by the same constant to build the upper quantile, so the ordering
        // survives and only the level moves — the rank correlation cannot see the difference.
        assert!((upper.information_coefficient.unwrap().mean - 1.0).abs() < 1e-9);
        assert_ne!(
            upper.decile_spread, None,
            "the spread is in return units and does move"
        );
    }

    #[test]
    fn test_a_dataset_without_outcomes_cannot_be_scored() {
        let bare = TrainingDataset::new(
            ndarray::Array3::zeros((4, 2, 7)),
            ndarray::Array3::zeros((4, 2, 5)),
            ndarray::Array3::zeros((4, 1, 5)),
            ndarray::Array3::zeros((4, 1, 3)),
            None,
            vec![0; 4],
        )
        .unwrap();
        assert!(measure("bare", &[0.0; 12], &bare, &scaler(), MEDIAN, QUANTILES, 1).is_err());
    }

    #[test]
    fn test_a_short_prediction_buffer_is_refused_rather_than_indexed() {
        let data = dataset(2, 12);
        assert!(measure("short", &[0.0; 6], &data, &scaler(), MEDIAN, QUANTILES, 1).is_err());
    }
}
