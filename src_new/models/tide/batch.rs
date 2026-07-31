//! Shared conversion of windowed ndarray datasets into Burn tensors.
//!
//! Both training and inference flatten a [`TrainingDataset`] into the model's
//! `[batch, input_size]` input here so the feature ordering can never drift
//! between the two paths.

use burn::prelude::*;

use crate::models::tide::data::TrainingDataset;

/// Build the `[batch, input_size]` forward input for the given sample indices.
///
/// Features are flattened in the canonical order the model expects: past
/// continuous, past categorical, future categorical, then static — each cast to
/// f32.
pub fn build_input_tensor<B: Backend>(
    dataset: &TrainingDataset,
    indices: &[usize],
    input_length: usize,
    output_length: usize,
    device: &B::Device,
) -> Tensor<B, 2> {
    let n_cont = dataset.past_continuous.shape()[2];
    let n_cat = dataset.past_categorical.shape()[2];
    let n_static = dataset.static_categorical.shape()[2];
    let input_size =
        input_length * n_cont + input_length * n_cat + output_length * n_cat + n_static;

    let mut buffer = Vec::with_capacity(indices.len() * input_size);
    for &sample in indices {
        for t in 0..input_length {
            for f in 0..n_cont {
                buffer.push(dataset.past_continuous[[sample, t, f]]);
            }
        }
        for t in 0..input_length {
            for f in 0..n_cat {
                buffer.push(dataset.past_categorical[[sample, t, f]] as f32);
            }
        }
        for t in 0..output_length {
            for f in 0..n_cat {
                buffer.push(dataset.future_categorical[[sample, t, f]] as f32);
            }
        }
        for f in 0..n_static {
            buffer.push(dataset.static_categorical[[sample, 0, f]] as f32);
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
        for t in 0..output_length {
            buffer.push(targets[[sample, t, 0]]);
        }
    }
    Tensor::<B, 1>::from_floats(buffer.as_slice(), device).reshape([indices.len(), output_length])
}
