//! Windowed ndarray datasets into Burn tensors.
//!
//! Training and inference both flatten through here, so the feature ordering cannot drift between
//! them.

use burn::prelude::*;

use crate::models::tide::data::TrainingDataset;

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
