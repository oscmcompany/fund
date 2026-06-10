//! Evaluation metrics for a trained TiDE model, computed on the validation set
//! in scaled space. Ports the Python `evaluate.py` definitions exactly:
//!
//! - CRPS: per row, the **sum** over quantiles of pinball loss (non-strict
//!   `error >= 0` split), then the mean over rows.
//! - directional accuracy: fraction of rows where `(q50 >= 0) == (target >= 0)`.
//! - quantile coverage: fraction of rows where `q10 <= target <= q90`.

use burn::backend::NdArray;

use crate::models::tide::batch::build_input_tensor;
use crate::models::tide::config::ModelParameters;
use crate::models::tide::data::TrainingDataset;
use crate::models::tide::model::TideModel;

const EVAL_BATCH: usize = 4096;

#[derive(Debug, Clone, Copy, serde::Serialize)]
pub struct EvalMetrics {
    pub crps: f64,
    pub directional_accuracy: f64,
    pub quantile_coverage: f64,
}

impl EvalMetrics {
    fn zero() -> Self {
        Self {
            crps: 0.0,
            directional_accuracy: 0.0,
            quantile_coverage: 0.0,
        }
    }
}

/// Run the (inner, non-autodiff) model over the validation dataset and compute
/// the metrics. Returns zeros for an empty or target-less dataset.
pub fn evaluate(
    model: &TideModel<NdArray>,
    dataset: &TrainingDataset,
    parameters: &ModelParameters,
) -> Result<EvalMetrics, Box<dyn std::error::Error>> {
    let sample_count = dataset.len();
    let targets = match dataset.targets.as_ref() {
        Some(targets) if sample_count > 0 => targets,
        _ => return Ok(EvalMetrics::zero()),
    };

    let output_length = parameters.output_length();
    let quantiles = parameters.quantiles();
    let num_quantiles = quantiles.len();
    if num_quantiles == 0 {
        return Ok(EvalMetrics::zero());
    }

    let lower_index = argmin(quantiles);
    let upper_index = argmax(quantiles);
    let median_index = closest_to(quantiles, 0.5);

    let device = Default::default();
    let mut predictions: Vec<f32> =
        Vec::with_capacity(sample_count * output_length * num_quantiles);
    let indices: Vec<usize> = (0..sample_count).collect();
    for chunk in indices.chunks(EVAL_BATCH) {
        let input = build_input_tensor::<NdArray>(
            dataset,
            chunk,
            parameters.input_length(),
            output_length,
            &device,
        );
        let output = model.forward(input);
        let mut values: Vec<f32> = output.to_data().to_vec().map_err(|e| format!("{e:?}"))?;
        predictions.append(&mut values);
    }

    let mut crps_sum = 0.0_f64;
    let mut directional_matches = 0_usize;
    let mut covered = 0_usize;
    let mut row_count = 0_usize;

    for sample in 0..sample_count {
        for t in 0..output_length {
            let target = targets[[sample, t, 0]] as f64;
            let base = (sample * output_length + t) * num_quantiles;

            let mut row_loss = 0.0_f64;
            for (q_index, &quantile) in quantiles.iter().enumerate() {
                let prediction = predictions[base + q_index] as f64;
                let error = target - prediction;
                let pinball = if error >= 0.0 {
                    quantile * error
                } else {
                    (quantile - 1.0) * error
                };
                row_loss += pinball;
            }
            crps_sum += row_loss;

            let q_median = predictions[base + median_index] as f64;
            if (q_median >= 0.0) == (target >= 0.0) {
                directional_matches += 1;
            }

            let q_lower = predictions[base + lower_index] as f64;
            let q_upper = predictions[base + upper_index] as f64;
            if target >= q_lower && target <= q_upper {
                covered += 1;
            }

            row_count += 1;
        }
    }

    if row_count == 0 {
        return Ok(EvalMetrics::zero());
    }

    let rows = row_count as f64;
    Ok(EvalMetrics {
        crps: crps_sum / rows,
        directional_accuracy: directional_matches as f64 / rows,
        quantile_coverage: covered as f64 / rows,
    })
}

fn argmin(values: &[f64]) -> usize {
    values
        .iter()
        .enumerate()
        .min_by(|a, b| a.1.partial_cmp(b.1).unwrap())
        .map(|(index, _)| index)
        .unwrap_or(0)
}

fn argmax(values: &[f64]) -> usize {
    values
        .iter()
        .enumerate()
        .max_by(|a, b| a.1.partial_cmp(b.1).unwrap())
        .map(|(index, _)| index)
        .unwrap_or(0)
}

fn closest_to(values: &[f64], target: f64) -> usize {
    values
        .iter()
        .enumerate()
        .min_by(|a, b| {
            (a.1 - target)
                .abs()
                .partial_cmp(&(b.1 - target).abs())
                .unwrap()
        })
        .map(|(index, _)| index)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::tide::data::TrainingDataset;

    /// Build a model that ignores its input and emits fixed quantile predictions
    /// is hard; instead we test the metric arithmetic directly via a tiny helper
    /// that mirrors `evaluate`'s row math.
    fn metrics_from(predictions: &[[f64; 3]], targets: &[f64], quantiles: &[f64]) -> EvalMetrics {
        let mut crps_sum = 0.0;
        let mut directional = 0;
        let mut covered = 0;
        for (row, &target) in targets.iter().enumerate() {
            let mut row_loss = 0.0;
            for (qi, &q) in quantiles.iter().enumerate() {
                let error = target - predictions[row][qi];
                row_loss += if error >= 0.0 {
                    q * error
                } else {
                    (q - 1.0) * error
                };
            }
            crps_sum += row_loss;
            if (predictions[row][1] >= 0.0) == (target >= 0.0) {
                directional += 1;
            }
            if target >= predictions[row][0] && target <= predictions[row][2] {
                covered += 1;
            }
        }
        let n = targets.len() as f64;
        EvalMetrics {
            crps: crps_sum / n,
            directional_accuracy: directional as f64 / n,
            quantile_coverage: covered as f64 / n,
        }
    }

    #[test]
    fn test_crps_sum_over_quantiles() {
        // target=1.0, preds all 0 -> errors 1.0 >=0 -> sum(0.1+0.5+0.9)*1 = 1.5
        let metrics = metrics_from(&[[0.0, 0.0, 0.0]], &[1.0], &[0.1, 0.5, 0.9]);
        assert!((metrics.crps - 1.5).abs() < 1e-9, "got {}", metrics.crps);
    }

    #[test]
    fn test_directional_and_coverage() {
        // q50=0.2>=0 and target=0.3>=0 -> match; target within [q10,q90].
        let metrics = metrics_from(&[[-0.1, 0.2, 0.5]], &[0.3], &[0.1, 0.5, 0.9]);
        assert_eq!(metrics.directional_accuracy, 1.0);
        assert_eq!(metrics.quantile_coverage, 1.0);

        // target negative but q50 positive -> no directional match; outside interval.
        let metrics = metrics_from(&[[0.1, 0.2, 0.5]], &[-0.3], &[0.1, 0.5, 0.9]);
        assert_eq!(metrics.directional_accuracy, 0.0);
        assert_eq!(metrics.quantile_coverage, 0.0);
    }

    #[test]
    fn test_evaluate_empty_dataset_is_zero() {
        let device = Default::default();
        let model = TideModel::<NdArray>::new(&device, 24, 16, 1, 1, 5, 3, 0.0);
        let dataset = TrainingDataset {
            past_continuous: ndarray::Array3::zeros((0, 2, 7)),
            past_categorical: ndarray::Array3::zeros((0, 2, 5)),
            future_categorical: ndarray::Array3::zeros((0, 1, 5)),
            static_categorical: ndarray::Array3::zeros((0, 1, 3)),
            targets: Some(ndarray::Array3::zeros((0, 1, 1))),
        };
        let parameters = ModelParameters::default();
        let metrics = evaluate(&model, &dataset, &parameters).unwrap();
        assert_eq!(metrics.crps, 0.0);
    }
}
