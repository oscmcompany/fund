//! Rust-native TiDE training loop on the `Autodiff<NdArray>` backend.
//!
//! A hand-rolled loop (no `burn-train` `Learner`) mirroring the Python trainer:
//! Adam, mini-batch shuffling, validation-loss early stopping with patience, and
//! best-checkpoint restore (kept in memory rather than round-tripped to disk).

use burn::backend::{Autodiff, NdArray};
use burn::module::AutodiffModule;
use burn::optim::{AdamConfig, GradientsParams, Optimizer};
use burn::tensor::backend::Backend;
use rand::seq::SliceRandom;
use rand::SeedableRng;
use tracing::info;

use crate::models::tide::batch::{build_input_tensor, build_target_tensor};
use crate::models::tide::config::ModelParameters;
use crate::models::tide::data::TrainingDataset;
use crate::models::tide::loss::quantile_loss;
use crate::models::tide::model::TideModel;

/// The training backend: reverse-mode autodiff over the CPU NdArray backend.
pub type TrainBackend = Autodiff<NdArray>;

pub struct TrainConfig {
    pub learning_rate: f64,
    pub epoch_count: usize,
    pub batch_size: usize,
    pub early_stopping_patience: usize,
    pub min_delta: f64,
}

impl Default for TrainConfig {
    fn default() -> Self {
        Self {
            learning_rate: 0.001,
            epoch_count: 20,
            batch_size: 512,
            early_stopping_patience: 3,
            // The scaled-return quantile loss sits around 1e-4, so the Python
            // default min_delta of 1e-3 never registered an improvement and
            // training always stopped after `patience`. Use a delta below the
            // loss scale so genuine epoch-over-epoch gains keep training going.
            min_delta: 1e-5,
        }
    }
}

/// Epoch-end checkpoint and early-stopping policy, mirroring the Python
/// trainer: the best model is snapshotted on any improvement, while the
/// early-stopping counter only resets on improvements larger than `min_delta`.
pub(crate) struct EarlyStopping {
    best_checkpoint_metric: f64,
    best_stopping_metric: f64,
    epochs_without_improvement: usize,
}

impl EarlyStopping {
    pub(crate) fn new() -> Self {
        Self {
            best_checkpoint_metric: f64::INFINITY,
            best_stopping_metric: f64::INFINITY,
            epochs_without_improvement: 0,
        }
    }

    /// Observe an epoch's stopping metric. Returns `(snapshot, stop)`: whether
    /// to checkpoint the current model and whether to stop training.
    pub(crate) fn observe(&mut self, metric: f64, min_delta: f64, patience: usize) -> (bool, bool) {
        let snapshot = metric < self.best_checkpoint_metric;
        if snapshot {
            self.best_checkpoint_metric = metric;
        }

        let stop = if metric < self.best_stopping_metric - min_delta {
            self.best_stopping_metric = metric;
            self.epochs_without_improvement = 0;
            false
        } else {
            self.epochs_without_improvement += 1;
            self.epochs_without_improvement >= patience
        };

        (snapshot, stop)
    }
}

/// Train the model, returning the best-checkpoint model and the per-epoch
/// training loss history.
pub fn train(
    mut model: TideModel<TrainBackend>,
    train_dataset: &TrainingDataset,
    valid_dataset: Option<&TrainingDataset>,
    parameters: &ModelParameters,
    config: &TrainConfig,
    device: &<TrainBackend as Backend>::Device,
) -> (TideModel<TrainBackend>, Vec<f64>) {
    let num_samples = train_dataset.len();
    let mut losses = Vec::new();
    if num_samples == 0 {
        return (model, losses);
    }

    let mut optimizer = AdamConfig::new()
        .with_epsilon(1e-8)
        .init::<TrainBackend, TideModel<TrainBackend>>();

    // Deterministic shuffle so runs are reproducible.
    let mut rng = rand::rngs::StdRng::seed_from_u64(0);

    let mut best_model = model.clone();
    let mut early_stopping = EarlyStopping::new();

    for epoch in 0..config.epoch_count {
        let mut order: Vec<usize> = (0..num_samples).collect();
        order.shuffle(&mut rng);

        let mut loss_sum = 0.0_f64;
        let mut batch_count = 0usize;

        for batch_indices in order.chunks(config.batch_size) {
            let input = build_input_tensor::<TrainBackend>(
                train_dataset,
                batch_indices,
                parameters.input_length,
                parameters.output_length,
                device,
            );
            let target = build_target_tensor::<TrainBackend>(
                train_dataset,
                batch_indices,
                parameters.output_length,
                device,
            );

            let prediction = model.forward(input);
            let loss = quantile_loss(
                prediction,
                target,
                &parameters.quantiles,
                parameters.huber_delta,
                parameters.output_length,
            );

            loss_sum += loss.clone().into_scalar() as f64;
            batch_count += 1;

            let gradients = loss.backward();
            let gradient_params = GradientsParams::from_grads(gradients, &model);
            model = optimizer.step(config.learning_rate, model, gradient_params);
        }

        let train_loss = if batch_count > 0 {
            loss_sum / batch_count as f64
        } else {
            0.0
        };
        losses.push(train_loss);

        let stopping_loss = match valid_dataset {
            Some(valid) if !valid.is_empty() => {
                validation_loss(&model, valid, parameters, config.batch_size)
            }
            _ => train_loss,
        };

        info!(
            epoch = epoch + 1,
            train_loss = train_loss,
            validation_loss = stopping_loss,
            "Epoch complete"
        );

        let (snapshot, stop) = early_stopping.observe(
            stopping_loss,
            config.min_delta,
            config.early_stopping_patience,
        );
        if snapshot {
            best_model = model.clone();
        }
        if stop {
            info!(epoch = epoch + 1, "Early stopping triggered");
            break;
        }
    }

    (best_model, losses)
}

/// Mean validation loss using the dropout-disabled inner (`NdArray`) model.
fn validation_loss(
    model: &TideModel<TrainBackend>,
    valid: &TrainingDataset,
    parameters: &ModelParameters,
    batch_size: usize,
) -> f64 {
    let inner = model.valid();
    let device = <NdArray as Backend>::Device::default();
    let sample_count = valid.len();
    let indices: Vec<usize> = (0..sample_count).collect();

    let mut loss_sum = 0.0_f64;
    let mut batch_count = 0usize;
    for chunk in indices.chunks(batch_size) {
        let input = build_input_tensor::<NdArray>(
            valid,
            chunk,
            parameters.input_length,
            parameters.output_length,
            &device,
        );
        let target =
            build_target_tensor::<NdArray>(valid, chunk, parameters.output_length, &device);
        let prediction = inner.forward(input);
        let loss = quantile_loss(
            prediction,
            target,
            &parameters.quantiles,
            parameters.huber_delta,
            parameters.output_length,
        );
        loss_sum += loss.into_scalar() as f64;
        batch_count += 1;
    }

    if batch_count > 0 {
        loss_sum / batch_count as f64
    } else {
        0.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::tide::data::input_feature_size;

    /// A tiny dataset whose target is a constant the model can fit; used to prove
    /// the autodiff + optimizer + loss wiring actually reduces loss.
    fn overfit_dataset(
        num_samples: usize,
        input_length: usize,
        output_length: usize,
    ) -> TrainingDataset {
        let mut past_continuous = ndarray::Array3::<f32>::zeros((num_samples, input_length, 7));
        for s in 0..num_samples {
            for t in 0..input_length {
                for f in 0..7 {
                    past_continuous[[s, t, f]] = ((s + t + f) as f32) * 0.01;
                }
            }
        }
        let past_categorical = ndarray::Array3::<i32>::ones((num_samples, input_length, 5));
        let future_categorical = ndarray::Array3::<i32>::ones((num_samples, output_length, 5));
        let static_categorical = ndarray::Array3::<i32>::ones((num_samples, 1, 3));
        let mut targets = ndarray::Array3::<f32>::zeros((num_samples, output_length, 1));
        for s in 0..num_samples {
            for t in 0..output_length {
                targets[[s, t, 0]] = 0.5;
            }
        }
        TrainingDataset {
            past_continuous,
            past_categorical,
            future_categorical,
            static_categorical,
            targets: Some(targets),
        }
    }

    #[test]
    fn test_early_stopping_snapshots_below_min_delta_improvements() {
        // Truth table from the Python trainer: any improvement checkpoints the
        // model, but only improvements beyond min_delta reset the patience
        // counter — small gains keep the best weights while still counting
        // toward early stopping.
        let mut policy = EarlyStopping::new();
        assert_eq!(policy.observe(0.5, 1e-3, 2), (true, false));
        // Better, but by less than min_delta: snapshot, counter advances.
        assert_eq!(policy.observe(0.4999, 1e-3, 2), (true, false));
        // Again better by a hair: snapshot, and patience 2 is now exhausted.
        assert_eq!(policy.observe(0.4998, 1e-3, 2), (true, true));
    }

    #[test]
    fn test_early_stopping_counter_resets_on_real_improvement() {
        let mut policy = EarlyStopping::new();
        assert_eq!(policy.observe(0.5, 1e-3, 2), (true, false));
        assert_eq!(policy.observe(0.4999, 1e-3, 2), (true, false));
        // A genuine improvement resets the counter, so no stop at patience 2.
        assert_eq!(policy.observe(0.4, 1e-3, 2), (true, false));
        assert_eq!(policy.observe(0.4, 1e-3, 2), (false, false));
        assert_eq!(policy.observe(0.41, 1e-3, 2), (false, true));
    }

    #[test]
    fn test_best_model_checkpointed_even_below_min_delta() {
        // The Python trainer snapshots the best model whenever the stopping
        // metric improves at all; min_delta only gates the early-stopping
        // counter. With an enormous min_delta the counter never resets, but the
        // returned model must still be the best epoch, not the initial weights.
        let input_length = 3;
        let output_length = 1;
        let dataset = overfit_dataset(32, input_length, output_length);

        let parameters = ModelParameters {
            input_size: input_feature_size(input_length, output_length),
            hidden_size: 16,
            num_encoder_layers: 1,
            num_decoder_layers: 1,
            output_length,
            input_length,
            dropout_rate: 0.0,
            quantiles: vec![0.1, 0.5, 0.9],
            huber_delta: 0.0,
        };

        let device = Default::default();
        let model = TideModel::<TrainBackend>::new(
            &device,
            parameters.input_size,
            parameters.hidden_size,
            parameters.num_encoder_layers,
            parameters.num_decoder_layers,
            parameters.output_length,
            parameters.quantiles.len(),
            parameters.dropout_rate,
        );
        let initial = model.clone();

        let config = TrainConfig {
            learning_rate: 0.01,
            epoch_count: 10,
            batch_size: 16,
            early_stopping_patience: 1000,
            min_delta: 1e9,
        };

        let (best, losses) = train(model, &dataset, None, &parameters, &config, &device);
        assert_eq!(losses.len(), 10);

        let initial_loss = validation_loss(&initial, &dataset, &parameters, config.batch_size);
        let best_loss = validation_loss(&best, &dataset, &parameters, config.batch_size);
        assert!(
            best_loss < initial_loss,
            "best model was not checkpointed: {best_loss} vs initial {initial_loss}"
        );
    }

    #[test]
    fn test_training_reduces_loss() {
        let input_length = 3;
        let output_length = 1;
        let dataset = overfit_dataset(32, input_length, output_length);

        let parameters = ModelParameters {
            input_size: input_feature_size(input_length, output_length),
            hidden_size: 16,
            num_encoder_layers: 1,
            num_decoder_layers: 1,
            output_length,
            input_length,
            dropout_rate: 0.0,
            quantiles: vec![0.1, 0.5, 0.9],
            huber_delta: 0.0,
        };

        let device = Default::default();
        let model = TideModel::<TrainBackend>::new(
            &device,
            parameters.input_size,
            parameters.hidden_size,
            parameters.num_encoder_layers,
            parameters.num_decoder_layers,
            parameters.output_length,
            parameters.quantiles.len(),
            parameters.dropout_rate,
        );

        let config = TrainConfig {
            learning_rate: 0.01,
            epoch_count: 80,
            batch_size: 16,
            early_stopping_patience: 1000,
            min_delta: 0.0,
        };

        let (_model, losses) = train(model, &dataset, None, &parameters, &config, &device);
        assert!(losses.len() >= 2);
        let first = losses.first().unwrap();
        let last = losses.last().unwrap();
        assert!(last < first, "loss did not decrease: {first} -> {last}");
    }
}
