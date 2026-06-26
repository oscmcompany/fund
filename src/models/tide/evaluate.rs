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
    use crate::models::tide::config::ModelParameters;
    use crate::models::tide::data::TrainingDataset;
    use crate::models::tide::model::TideModel;

    // ---------------------------------------------------------------------------
    // argmin / argmax / closest_to
    // ---------------------------------------------------------------------------

    #[test]
    fn test_argmin_returns_index_of_smallest_value() {
        assert_eq!(argmin(&[0.9, 0.1, 0.5]), 1);
    }

    #[test]
    fn test_argmin_single_element_returns_zero() {
        assert_eq!(argmin(&[1.23]), 0);
    }

    #[test]
    fn test_argmin_empty_returns_zero_fallback() {
        assert_eq!(argmin(&[]), 0);
    }

    #[test]
    fn test_argmax_returns_index_of_largest_value() {
        assert_eq!(argmax(&[0.1, 0.9, 0.5]), 1);
    }

    #[test]
    fn test_argmax_single_element_returns_zero() {
        assert_eq!(argmax(&[2.0]), 0);
    }

    #[test]
    fn test_argmax_empty_returns_zero_fallback() {
        assert_eq!(argmax(&[]), 0);
    }

    #[test]
    fn test_closest_to_returns_nearest_index() {
        // 0.5 is closer to index 1 (0.5) than to 0 (0.1) or 2 (0.9).
        assert_eq!(closest_to(&[0.1, 0.5, 0.9], 0.5), 1);
    }

    #[test]
    fn test_closest_to_breaks_tie_toward_first() {
        // Two values equidistant from target: min_by picks the first encountered.
        assert_eq!(closest_to(&[0.4, 0.6], 0.5), 0);
    }

    #[test]
    fn test_closest_to_single_element_returns_zero() {
        assert_eq!(closest_to(&[0.9], 0.5), 0);
    }

    #[test]
    fn test_closest_to_empty_returns_zero_fallback() {
        assert_eq!(closest_to(&[], 0.5), 0);
    }

    // ---------------------------------------------------------------------------
    // EvalMetrics::zero
    // ---------------------------------------------------------------------------

    #[test]
    fn test_eval_metrics_zero_fields_are_zero() {
        let metrics = EvalMetrics::zero();
        assert_eq!(metrics.crps, 0.0);
        assert_eq!(metrics.directional_accuracy, 0.0);
        assert_eq!(metrics.quantile_coverage, 0.0);
    }

    // ---------------------------------------------------------------------------
    // metrics_from: helper that mirrors evaluate()'s row-level arithmetic so we
    // can verify the math without running the neural network.
    // ---------------------------------------------------------------------------

    /// Replicates the per-row accumulation in `evaluate` for a fixed prediction
    /// table where each row has exactly `quantiles.len()` predictions ordered
    /// lowest to highest quantile.
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
    fn test_continuous_ranked_probability_score_positive_error_branch() {
        // target=1.0, all predictions=0.0; error=1.0 >= 0 for every quantile.
        // row_loss = 0.1*1 + 0.5*1 + 0.9*1 = 1.5; single row so crps=1.5.
        let metrics = metrics_from(&[[0.0, 0.0, 0.0]], &[1.0], &[0.1, 0.5, 0.9]);
        assert!((metrics.crps - 1.5).abs() < 1e-9, "crps={}", metrics.crps);
    }

    #[test]
    fn test_continuous_ranked_probability_score_negative_error_branch() {
        // target=-1.0, all predictions=0.0; error=-1.0 < 0 for every quantile.
        // pinball = (q-1)*error: (0.1-1)*(-1)=0.9, (0.5-1)*(-1)=0.5, (0.9-1)*(-1)=0.1
        // row_loss = 1.5; single row so crps=1.5.
        let metrics = metrics_from(&[[0.0, 0.0, 0.0]], &[-1.0], &[0.1, 0.5, 0.9]);
        assert!((metrics.crps - 1.5).abs() < 1e-9, "crps={}", metrics.crps);
    }

    #[test]
    fn test_continuous_ranked_probability_score_exact_prediction_is_zero() {
        // When every prediction equals the target, error=0 so crps=0.
        let metrics = metrics_from(&[[0.3, 0.3, 0.3]], &[0.3], &[0.1, 0.5, 0.9]);
        assert!((metrics.crps).abs() < 1e-9, "crps={}", metrics.crps);
    }

    #[test]
    fn test_directional_accuracy_both_positive() {
        // q50=0.2>=0 and target=0.3>=0 -> directional match; target within [-0.1,0.5].
        let metrics = metrics_from(&[[-0.1, 0.2, 0.5]], &[0.3], &[0.1, 0.5, 0.9]);
        assert_eq!(metrics.directional_accuracy, 1.0);
        assert_eq!(metrics.quantile_coverage, 1.0);
    }

    #[test]
    fn test_directional_accuracy_mismatch_positive_median_negative_target() {
        // q50 positive but target negative -> no directional match; target outside [0.1,0.5].
        let metrics = metrics_from(&[[0.1, 0.2, 0.5]], &[-0.3], &[0.1, 0.5, 0.9]);
        assert_eq!(metrics.directional_accuracy, 0.0);
        assert_eq!(metrics.quantile_coverage, 0.0);
    }

    #[test]
    fn test_directional_accuracy_both_negative() {
        // q50 < 0 and target < 0 -> directional match.
        let metrics = metrics_from(&[[-0.5, -0.2, -0.1]], &[-0.3], &[0.1, 0.5, 0.9]);
        assert_eq!(metrics.directional_accuracy, 1.0);
    }

    #[test]
    fn test_coverage_target_exactly_at_lower_bound() {
        // target == q_lower (lower bound is inclusive).
        let metrics = metrics_from(&[[-0.3, 0.0, 0.3]], &[-0.3], &[0.1, 0.5, 0.9]);
        assert_eq!(metrics.quantile_coverage, 1.0);
    }

    #[test]
    fn test_coverage_target_exactly_at_upper_bound() {
        // target == q_upper (upper bound is inclusive).
        let metrics = metrics_from(&[[-0.3, 0.0, 0.3]], &[0.3], &[0.1, 0.5, 0.9]);
        assert_eq!(metrics.quantile_coverage, 1.0);
    }

    #[test]
    fn test_multiple_rows_partial_coverage() {
        // Three rows: first two covered, last not.
        let predictions = [[-0.5, 0.0, 0.5], [-0.5, 0.0, 0.5], [-0.5, 0.0, 0.5]];
        let targets = [0.0_f64, 0.3, 1.0];
        let metrics = metrics_from(&predictions, &targets, &[0.1, 0.5, 0.9]);
        let expected_coverage = 2.0 / 3.0;
        assert!(
            (metrics.quantile_coverage - expected_coverage).abs() < 1e-9,
            "coverage={}",
            metrics.quantile_coverage
        );
    }

    // ---------------------------------------------------------------------------
    // evaluate() — early-return paths that do not require model inference
    // ---------------------------------------------------------------------------

    /// Construct a minimal dataset with the array shapes expected for
    /// `input_length` and `output_length` so `build_input_tensor` does not
    /// panic, using the tiny 32-input-feature model defined below.
    ///
    /// input_size = input_length*n_cont + input_length*n_cat + output_length*n_cat + n_static
    ///            = 2*7 + 2*5 + 1*5 + 3 = 32
    fn make_tiny_dataset(num_samples: usize, with_targets: bool) -> TrainingDataset {
        let input_length = 2_usize;
        let output_length = 1_usize;
        TrainingDataset {
            past_continuous: ndarray::Array3::zeros((num_samples, input_length, 7)),
            past_categorical: ndarray::Array3::zeros((num_samples, input_length, 5)),
            future_categorical: ndarray::Array3::zeros((num_samples, output_length, 5)),
            static_categorical: ndarray::Array3::zeros((num_samples, 1, 3)),
            targets: if with_targets {
                Some(ndarray::Array3::zeros((num_samples, output_length, 1)))
            } else {
                None
            },
        }
    }

    /// Build a TideModel that matches `make_tiny_dataset`: input_size=32,
    /// output_length=1, num_quantiles=3.
    fn make_tiny_model() -> TideModel<NdArray> {
        let device = Default::default();
        TideModel::<NdArray>::new(&device, 32, 8, 1, 1, 1, 3, 0.0)
    }

    /// Build ModelParameters aligned with `make_tiny_dataset` and
    /// `make_tiny_model`.
    fn make_tiny_parameters() -> ModelParameters {
        ModelParameters::for_tests(32, 8, 1, 1, 1, 2, 0.0, vec![0.1, 0.5, 0.9], 0.5)
    }

    #[test]
    fn test_evaluate_empty_dataset_returns_zero() {
        let model = make_tiny_model();
        let dataset = make_tiny_dataset(0, true);
        let parameters = make_tiny_parameters();
        let metrics = evaluate(&model, &dataset, &parameters).unwrap();
        assert_eq!(metrics.crps, 0.0);
        assert_eq!(metrics.directional_accuracy, 0.0);
        assert_eq!(metrics.quantile_coverage, 0.0);
    }

    #[test]
    fn test_evaluate_no_targets_returns_zero() {
        // sample_count > 0 but targets is None -> early return zeros.
        let model = make_tiny_model();
        let dataset = make_tiny_dataset(4, false);
        let parameters = make_tiny_parameters();
        let metrics = evaluate(&model, &dataset, &parameters).unwrap();
        assert_eq!(metrics.crps, 0.0);
        assert_eq!(metrics.directional_accuracy, 0.0);
        assert_eq!(metrics.quantile_coverage, 0.0);
    }

    #[test]
    fn test_evaluate_empty_quantiles_returns_zero() {
        // num_quantiles == 0 triggers the early return after argmin/argmax/closest_to.
        let model = make_tiny_model();
        let dataset = make_tiny_dataset(4, true);
        // Use a model whose quantile list is empty.
        let parameters = ModelParameters::for_tests(32, 8, 1, 1, 1, 2, 0.0, vec![], 0.5);
        let metrics = evaluate(&model, &dataset, &parameters).unwrap();
        assert_eq!(metrics.crps, 0.0);
        assert_eq!(metrics.directional_accuracy, 0.0);
        assert_eq!(metrics.quantile_coverage, 0.0);
    }

    #[test]
    fn test_evaluate_with_samples_returns_valid_metrics() {
        // Run the full inference path: sample_count > 0, targets present, quantiles non-empty.
        // The model is randomly initialised so we only assert the metric ranges, not values.
        let model = make_tiny_model();
        let dataset = make_tiny_dataset(3, true);
        let parameters = make_tiny_parameters();
        let metrics = evaluate(&model, &dataset, &parameters).unwrap();
        // crps is a sum of non-negative pinball losses so it must be >= 0.
        assert!(metrics.crps >= 0.0, "crps={}", metrics.crps);
        // directional_accuracy is a fraction in [0,1].
        assert!(
            (0.0..=1.0).contains(&metrics.directional_accuracy),
            "directional_accuracy={}",
            metrics.directional_accuracy
        );
        // quantile_coverage is a fraction in [0,1].
        assert!(
            (0.0..=1.0).contains(&metrics.quantile_coverage),
            "quantile_coverage={}",
            metrics.quantile_coverage
        );
    }

    #[test]
    fn test_evaluate_larger_batch_than_eval_batch_size() {
        // Exercises multi-chunk iteration by exceeding EVAL_BATCH (4096).
        let model = make_tiny_model();
        let dataset = make_tiny_dataset(4097, true);
        let parameters = make_tiny_parameters();
        let result = evaluate(&model, &dataset, &parameters);
        assert!(
            result.is_ok(),
            "evaluate returned an error: {:?}",
            result.err()
        );
    }
}
