//! Windowed ndarray datasets into Burn tensors.
//!
//! Training and inference both flatten through here, so the feature ordering cannot drift between
//! them.

use burn::prelude::*;

use crate::models::tide::configuration::ModelParameters;
use crate::models::tide::data::TrainingDataset;

/// Checks that a dataset can produce the input the loaded weights were trained to consume.
///
/// [`build_input_tensor`] derives its width from the *dataset's* shapes and never consults the
/// parameters, so a dataset built from a different feature set reshapes cleanly and then fails
/// inside the first linear layer — a burn panic about tensor dimensions, on the pre-open inference
/// path, naming neither the artifact nor the column set.
///
/// Called once per dataset rather than inside the batch loop: the invariant is a property of
/// `(dataset, parameters)`, not of a chunk, and the inference path would otherwise re-check it for
/// every batch of 4096.
pub fn validate_input_shape(
    dataset: &TrainingDataset,
    parameters: &ModelParameters,
) -> Result<(), String> {
    let input_length = parameters.input_length();
    let output_length = parameters.output_length();

    let continuous_feature_count = dataset.past_continuous.shape()[2];
    let categorical_feature_count = dataset.past_categorical.shape()[2];
    let static_feature_count = dataset.static_categorical.shape()[2];

    let derived = input_length * continuous_feature_count
        + input_length * categorical_feature_count
        + output_length * categorical_feature_count
        + static_feature_count;

    if derived != parameters.input_size() {
        return Err(format!(
            "Dataset produces {derived} input features ({continuous_feature_count} continuous and \
             {categorical_feature_count} categorical over {input_length} steps, \
             {categorical_feature_count} known-future over {output_length}, {static_feature_count} \
             static), but the artifact was trained on {}",
            parameters.input_size()
        ));
    }

    // Widths are indexed positionally up to these lengths, so a short window is an out-of-bounds
    // panic rather than a short batch.
    for (name, available, required) in [
        (
            "past continuous",
            dataset.past_continuous.shape()[1],
            input_length,
        ),
        (
            "past categorical",
            dataset.past_categorical.shape()[1],
            input_length,
        ),
        (
            "known-future categorical",
            dataset.future_categorical.shape()[1],
            output_length,
        ),
        // Always one by construction, and read as `[sample, 0, feature]` regardless — so a dataset
        // built some other way with a zero-length axis would panic on a index this loop otherwise
        // never looks at.
        (
            "static categorical",
            dataset.static_categorical.shape()[1],
            1,
        ),
    ] {
        if available < required {
            return Err(format!(
                "Dataset carries {available} {name} step(s) but the artifact needs {required}"
            ));
        }
    }

    Ok(())
}

/// Build the `[batch, input_size]` forward input for the given sample indices.
pub fn build_input_tensor<B: Backend>(
    dataset: &TrainingDataset,
    indices: &[usize],
    input_length: usize,
    output_length: usize,
    device: &B::Device,
) -> Tensor<B, 2> {
    let continuous_feature_count = dataset.past_continuous.shape()[2];
    let categorical_feature_count = dataset.past_categorical.shape()[2];
    let static_feature_count = dataset.static_categorical.shape()[2];
    let input_size = input_length * continuous_feature_count
        + input_length * categorical_feature_count
        + output_length * categorical_feature_count
        + static_feature_count;

    let mut buffer = Vec::with_capacity(indices.len() * input_size);
    for &sample in indices {
        for step in 0..input_length {
            for feature in 0..continuous_feature_count {
                buffer.push(dataset.past_continuous[[sample, step, feature]]);
            }
        }
        for step in 0..input_length {
            for feature in 0..categorical_feature_count {
                buffer.push(dataset.past_categorical[[sample, step, feature]] as f32);
            }
        }
        for step in 0..output_length {
            for feature in 0..categorical_feature_count {
                buffer.push(dataset.future_categorical[[sample, step, feature]] as f32);
            }
        }
        for feature in 0..static_feature_count {
            buffer.push(dataset.static_categorical[[sample, 0, feature]] as f32);
        }
    }

    Tensor::<B, 1>::from_floats(buffer.as_slice(), device).reshape([indices.len(), input_size])
}

/// Build the `[batch, output_length]` target tensor for the given indices.
///
/// Panics if the dataset has no targets (e.g. a predict-only dataset).
pub fn build_target_tensor<B: Backend>(
    dataset: &TrainingDataset,
    indices: &[usize],
    output_length: usize,
    device: &B::Device,
) -> Tensor<B, 2> {
    let targets = dataset
        .targets
        .as_ref()
        .expect("targets are required to build a target tensor");
    let mut buffer = Vec::with_capacity(indices.len() * output_length);
    for &sample in indices {
        for step in 0..output_length {
            buffer.push(targets[[sample, step, 0]]);
        }
    }
    Tensor::<B, 1>::from_floats(buffer.as_slice(), device).reshape([indices.len(), output_length])
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A dataset with the given per-step feature widths. With `input_length` 2 and `output_length`
    /// 1, the derived input size is `2*continuous + 2*categorical + 1*categorical + static`.
    fn dataset(continuous: usize, categorical: usize, static_features: usize) -> TrainingDataset {
        TrainingDataset {
            past_continuous: ndarray::Array3::zeros((4, 2, continuous)),
            past_categorical: ndarray::Array3::zeros((4, 2, categorical)),
            future_categorical: ndarray::Array3::zeros((4, 1, categorical)),
            static_categorical: ndarray::Array3::zeros((4, 1, static_features)),
            targets: None,
        }
    }

    fn parameters(input_size: usize) -> ModelParameters {
        ModelParameters::new(input_size, 2, 1)
    }

    #[test]
    fn test_a_matching_dataset_passes() {
        // 2*7 + 2*5 + 1*5 + 3 = 32
        assert!(validate_input_shape(&dataset(7, 5, 3), &parameters(32)).is_ok());
    }

    /// The failure this replaces is a burn panic about tensor dimensions raised inside the first
    /// linear layer, on the pre-open inference path, naming neither the artifact nor the columns.
    #[test]
    fn test_a_feature_count_mismatch_is_reported_against_the_artifact() {
        let error = validate_input_shape(&dataset(6, 5, 3), &parameters(32))
            .expect_err("a dataset narrower than the trained input must be refused");

        assert!(
            error.contains("30"),
            "should state what the data produces: {error}"
        );
        assert!(
            error.contains("32"),
            "should state what the artifact wants: {error}"
        );
    }

    #[test]
    fn test_a_window_shorter_than_the_artifact_needs_is_refused() {
        let mut short = dataset(7, 5, 3);
        // One past step where the parameters ask for two: indexing step 1 would be out of bounds.
        short.past_continuous = ndarray::Array3::zeros((4, 1, 7));
        let error = validate_input_shape(&short, &parameters(32))
            .expect_err("a short window must be refused");
        assert!(error.contains("past continuous"), "got: {error}");
    }

    /// `build_input_tensor` reads `[sample, 0, feature]` from the static block unconditionally, so
    /// a zero-length time axis panics on an index the other three checks never look at.
    #[test]
    fn test_a_zero_length_static_axis_is_refused() {
        let mut degenerate = dataset(7, 5, 3);
        degenerate.static_categorical = ndarray::Array3::zeros((4, 0, 3));
        let error = validate_input_shape(&degenerate, &parameters(32))
            .expect_err("a static block with no time axis must be refused");
        assert!(error.contains("static categorical"), "got: {error}");
    }

    #[test]
    fn test_a_short_known_future_window_is_refused() {
        let mut short = dataset(7, 5, 3);
        short.future_categorical = ndarray::Array3::zeros((4, 0, 5));
        assert!(validate_input_shape(&short, &parameters(32)).is_err());
    }
}
