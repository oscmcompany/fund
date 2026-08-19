//! TiDE training loop on the `Autodiff<NdArray>` backend.
//!
//! Hand-rolled rather than `burn-train`'s `Learner`. The best checkpoint is restored from memory
//! rather than round-tripped to disk.

use burn::backend::{Autodiff, NdArray};
use burn::module::AutodiffModule;
use burn::optim::{AdamConfig, GradientsParams, Optimizer};
use burn::tensor::backend::Backend;
use rand::seq::SliceRandom;
use rand::SeedableRng;
use tracing::info;

use crate::models::tide::batch::{build_input_tensor, build_target_tensor};
use crate::models::tide::configuration::ModelParameters;
use crate::models::tide::data::TrainingDataset;
use crate::models::tide::loss::quantile_loss;
use crate::models::tide::model::TiDEModel;

/// The training backend: reverse-mode autodiff over the CPU NdArray backend.
pub type TrainBackend = Autodiff<NdArray>;

pub struct TrainConfiguration {
    learning_rate: f64,
    epoch_count: usize,
    batch_size: usize,
    early_stopping_patience: usize,
    min_delta: f64,
}

/// Why a training configuration was rejected.
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
#[error("invalid training configuration: {reason}")]
pub struct TrainConfigurationError {
    pub reason: String,
}

impl TrainConfiguration {
    /// Constructs a validated `TrainConfiguration`.
    ///
    /// `batch_size` is the one that bites hardest: it reaches `slice::chunks`, which panics on
    /// zero, so a zero batch size aborts the training task rather than failing it. A non-positive
    /// learning rate is quieter and worse — training runs to completion and learns nothing.
    pub fn new(
        learning_rate: f64,
        epoch_count: usize,
        batch_size: usize,
        early_stopping_patience: usize,
        min_delta: f64,
    ) -> Result<Self, TrainConfigurationError> {
        let reject = |reason: &str| {
            Err(TrainConfigurationError {
                reason: reason.to_string(),
            })
        };
        if batch_size == 0 {
            return reject("batch_size must be greater than zero");
        }
        if epoch_count == 0 {
            return reject("epoch_count must be greater than zero");
        }
        if !learning_rate.is_finite() || learning_rate <= 0.0 {
            return reject("learning_rate must be a positive, finite number");
        }
        if !min_delta.is_finite() || min_delta < 0.0 {
            return reject("min_delta must be a non-negative, finite number");
        }
        Ok(Self {
            learning_rate,
            epoch_count,
            batch_size,
            early_stopping_patience,
            min_delta,
        })
    }

    pub fn learning_rate(&self) -> f64 {
        self.learning_rate
    }

    pub fn epoch_count(&self) -> usize {
        self.epoch_count
    }

    pub fn batch_size(&self) -> usize {
        self.batch_size
    }

    pub fn early_stopping_patience(&self) -> usize {
        self.early_stopping_patience
    }

    pub fn min_delta(&self) -> f64 {
        self.min_delta
    }
}

impl Default for TrainConfiguration {
    fn default() -> Self {
        Self {
            learning_rate: 0.001,
            epoch_count: 20,
            batch_size: 512,
            early_stopping_patience: 3,
            // The scaled-return quantile loss sits around 1e-4, so a min_delta of 1e-3 never
            // registers an improvement and training always stops after `patience`. This must stay
            // below the loss scale for genuine epoch-over-epoch gains to keep training going.
            min_delta: 1e-5,
        }
    }
}

/// Epoch-end checkpoint and early-stopping policy. The best model is snapshotted on any
/// improvement; the early-stopping counter only resets on improvements larger than `min_delta`.
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

    /// Observe an epoch's stopping metric.
    pub(crate) fn observe(
        &mut self,
        metric: f64,
        min_delta: f64,
        patience: usize,
    ) -> EpochDecision {
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

        EpochDecision { snapshot, stop }
    }
}

/// What an epoch's stopping metric decided.
///
/// Two independent answers, and they disagree often enough to be worth naming: any improvement
/// checkpoints, but only one larger than `min_delta` resets the patience counter, so `snapshot`
/// without `stop` and `snapshot` with `stop` are both ordinary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct EpochDecision {
    /// Whether this epoch's weights are the best seen and should be checkpointed.
    pub(crate) snapshot: bool,
    /// Whether patience is exhausted and training should end.
    pub(crate) stop: bool,
}

/// Train the model, returning the best-checkpoint model and the per-epoch
/// training loss history.
///
/// `validation_dataset` drives early stopping. When it is `None` or empty, the **training** loss is
/// used as the stopping metric instead — which makes early stopping measure fit rather than
/// generalization, so it will happily run to the epoch limit on a model that has memorized the
/// window. The caller is expected to reject an empty validation split rather than rely on this;
/// `bin/tide_model_trainer.rs` does. The fallback exists so a deliberately validation-free run
/// (the overfitting tests below) still terminates, not as a supported production path.
pub fn train(
    mut model: TiDEModel<TrainBackend>,
    train_dataset: &TrainingDataset,
    validation_dataset: Option<&TrainingDataset>,
    parameters: &ModelParameters,
    configuration: &TrainConfiguration,
    device: &<TrainBackend as Backend>::Device,
) -> (TiDEModel<TrainBackend>, Vec<f64>) {
    let sample_count = train_dataset.len();
    let mut losses = Vec::new();
    if sample_count == 0 {
        return (model, losses);
    }

    let mut optimizer = AdamConfig::new()
        .with_epsilon(1e-8)
        .init::<TrainBackend, TiDEModel<TrainBackend>>();

    // Deterministic shuffle so runs are reproducible.
    let mut rng = rand::rngs::StdRng::seed_from_u64(0);

    let mut best_model = model.clone();
    let mut early_stopping = EarlyStopping::new();

    for epoch in 0..configuration.epoch_count {
        let mut order: Vec<usize> = (0..sample_count).collect();
        order.shuffle(&mut rng);

        let mut loss_sum = 0.0_f64;
        let mut batch_count = 0usize;

        for batch_indices in order.chunks(configuration.batch_size) {
            let input = build_input_tensor::<TrainBackend>(
                train_dataset,
                batch_indices,
                parameters.input_length(),
                parameters.output_length(),
                device,
            );
            let target = build_target_tensor::<TrainBackend>(
                train_dataset,
                batch_indices,
                parameters.output_length(),
                device,
            );

            let prediction = model.forward(input);
            let loss = quantile_loss(
                prediction,
                target,
                parameters.quantiles(),
                parameters.huber_delta(),
                parameters.output_length(),
            );

            loss_sum += loss.clone().into_scalar() as f64;
            batch_count += 1;

            let gradients = loss.backward();
            let gradient_params = GradientsParams::from_grads(gradients, &model);
            model = optimizer.step(configuration.learning_rate, model, gradient_params);
        }

        let train_loss = if batch_count > 0 {
            loss_sum / batch_count as f64
        } else {
            0.0
        };
        losses.push(train_loss);

        // Kept separate from `stopping_loss` so the log can say which one it is. When there is no
        // validation set, early stopping falls back to the training loss — and logging that
        // fallback under `validation_loss` produced a line where the two losses matched exactly,
        // which reads as a suspiciously well-generalizing model rather than as an absent split.
        let measured_validation_loss = match validation_dataset {
            Some(validation) if !validation.is_empty() => Some(validation_loss(
                &model,
                validation,
                parameters,
                configuration.batch_size,
            )),
            _ => None,
        };
        let stopping_loss = measured_validation_loss.unwrap_or(train_loss);

        info!(
            epoch = epoch + 1,
            train_loss,
            validation_loss = ?measured_validation_loss,
            stopping_loss,
            "Epoch complete"
        );

        let decision = early_stopping.observe(
            stopping_loss,
            configuration.min_delta,
            configuration.early_stopping_patience,
        );
        if decision.snapshot {
            best_model = model.clone();
        }
        if decision.stop {
            info!(epoch = epoch + 1, "Early stopping triggered");
            break;
        }
    }

    (best_model, losses)
}

/// Mean validation loss using the dropout-disabled inner (`NdArray`) model.
fn validation_loss(
    model: &TiDEModel<TrainBackend>,
    validation: &TrainingDataset,
    parameters: &ModelParameters,
    batch_size: usize,
) -> f64 {
    let inner = model.valid();
    let device = <NdArray as Backend>::Device::default();
    let sample_count = validation.len();
    let indices: Vec<usize> = (0..sample_count).collect();

    let mut loss_sum = 0.0_f64;
    let mut batch_count = 0usize;
    for chunk in indices.chunks(batch_size) {
        let input = build_input_tensor::<NdArray>(
            validation,
            chunk,
            parameters.input_length(),
            parameters.output_length(),
            &device,
        );
        let target =
            build_target_tensor::<NdArray>(validation, chunk, parameters.output_length(), &device);
        let prediction = inner.forward(input);
        let loss = quantile_loss(
            prediction,
            target,
            parameters.quantiles(),
            parameters.huber_delta(),
            parameters.output_length(),
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

    /// `batch_size` reaches `slice::chunks`, which panics on zero — so an invalid configuration
    /// aborts the training task rather than failing it. A non-positive learning rate is quieter and
    /// worse: training completes and learns nothing.
    #[test]
    fn test_train_configuration_rejects_values_that_break_the_loop() {
        assert!(
            TrainConfiguration::new(0.001, 20, 0, 3, 1e-5).is_err(),
            "zero batch"
        );
        assert!(
            TrainConfiguration::new(0.001, 0, 512, 3, 1e-5).is_err(),
            "zero epochs"
        );
        assert!(
            TrainConfiguration::new(0.0, 20, 512, 3, 1e-5).is_err(),
            "zero learning rate"
        );
        assert!(
            TrainConfiguration::new(-0.1, 20, 512, 3, 1e-5).is_err(),
            "negative learning rate"
        );
        assert!(
            TrainConfiguration::new(f64::NAN, 20, 512, 3, 1e-5).is_err(),
            "non-finite"
        );
        assert!(
            TrainConfiguration::new(0.001, 20, 512, 3, -1.0).is_err(),
            "negative min_delta"
        );
    }

    #[test]
    fn test_train_configuration_accepts_the_defaults() {
        let defaults = TrainConfiguration::default();
        assert!(TrainConfiguration::new(
            defaults.learning_rate(),
            defaults.epoch_count(),
            defaults.batch_size(),
            defaults.early_stopping_patience(),
            defaults.min_delta(),
        )
        .is_ok());
    }

    /// A tiny dataset whose target is a constant the model can fit; used to prove
    /// the autodiff + optimizer + loss wiring actually reduces loss.
    fn overfit_dataset(
        sample_count: usize,
        input_length: usize,
        output_length: usize,
    ) -> TrainingDataset {
        let mut past_continuous = ndarray::Array3::<f32>::zeros((sample_count, input_length, 7));
        for sample_index in 0..sample_count {
            for time_index in 0..input_length {
                for feature_index in 0..7 {
                    past_continuous[[sample_index, time_index, feature_index]] =
                        ((sample_index + time_index + feature_index) as f32) * 0.01;
                }
            }
        }
        let past_categorical = ndarray::Array3::<i32>::ones((sample_count, input_length, 5));
        let future_categorical = ndarray::Array3::<i32>::ones((sample_count, output_length, 5));
        let static_categorical = ndarray::Array3::<i32>::ones((sample_count, 1, 3));
        let mut targets = ndarray::Array3::<f32>::zeros((sample_count, output_length, 1));
        for sample_index in 0..sample_count {
            for step_index in 0..output_length {
                targets[[sample_index, step_index, 0]] = 0.5;
            }
        }
        TrainingDataset::new(
            past_continuous,
            past_categorical,
            future_categorical,
            static_categorical,
            Some(targets),
            vec![0; sample_count],
        )
        .unwrap()
    }

    #[test]
    fn test_early_stopping_snapshots_below_min_delta_improvements() {
        // Any improvement checkpoints the model, but only improvements beyond min_delta reset
        // the patience counter — small gains keep the best weights while still counting toward
        // early stopping.
        let mut policy = EarlyStopping::new();
        assert_eq!(
            policy.observe(0.5, 1e-3, 2),
            EpochDecision {
                snapshot: true,
                stop: false
            }
        );
        // Better, but by less than min_delta: snapshot, counter advances.
        assert_eq!(
            policy.observe(0.4999, 1e-3, 2),
            EpochDecision {
                snapshot: true,
                stop: false
            }
        );
        // Again better by a hair: snapshot, and patience 2 is now exhausted.
        assert_eq!(
            policy.observe(0.4998, 1e-3, 2),
            EpochDecision {
                snapshot: true,
                stop: true
            }
        );
    }

    #[test]
    fn test_early_stopping_counter_resets_on_real_improvement() {
        let mut policy = EarlyStopping::new();
        assert_eq!(
            policy.observe(0.5, 1e-3, 2),
            EpochDecision {
                snapshot: true,
                stop: false
            }
        );
        assert_eq!(
            policy.observe(0.4999, 1e-3, 2),
            EpochDecision {
                snapshot: true,
                stop: false
            }
        );
        // A genuine improvement resets the counter, so no stop at patience 2.
        assert_eq!(
            policy.observe(0.4, 1e-3, 2),
            EpochDecision {
                snapshot: true,
                stop: false
            }
        );
        assert_eq!(
            policy.observe(0.4, 1e-3, 2),
            EpochDecision {
                snapshot: false,
                stop: false
            }
        );
        assert_eq!(
            policy.observe(0.41, 1e-3, 2),
            EpochDecision {
                snapshot: false,
                stop: true
            }
        );
    }

    #[test]
    fn test_best_model_checkpointed_even_below_min_delta() {
        // The best model is snapshotted whenever the stopping metric improves at all; min_delta
        // only gates the early-stopping counter. With an enormous min_delta the counter never
        // resets, but the returned model must still be the best epoch, not the initial weights.
        let input_length = 3;
        let output_length = 1;
        let dataset = overfit_dataset(32, input_length, output_length);

        let parameters = ModelParameters::for_tests(
            input_feature_size(input_length, output_length),
            16,
            1,
            1,
            output_length,
            input_length,
            0.0,
            vec![0.1, 0.5, 0.9],
            0.0,
        );

        let device = Default::default();
        let model = TiDEModel::<TrainBackend>::new(
            &device,
            parameters.input_size(),
            parameters.hidden_size(),
            parameters.encoder_layer_count(),
            parameters.decoder_layer_count(),
            parameters.output_length(),
            parameters.quantiles().len(),
            parameters.dropout_rate(),
        );
        let initial = model.clone();

        // Through the constructor, so the tests exercise the validated path the trainer takes.
        let configuration = TrainConfiguration::new(0.01, 10, 16, 1000, 1e9)
            .expect("the fixture configuration must be valid");

        let (best, losses) = train(model, &dataset, None, &parameters, &configuration, &device);
        assert_eq!(losses.len(), 10);

        let initial_loss =
            validation_loss(&initial, &dataset, &parameters, configuration.batch_size);
        let best_loss = validation_loss(&best, &dataset, &parameters, configuration.batch_size);
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

        let parameters = ModelParameters::for_tests(
            input_feature_size(input_length, output_length),
            16,
            1,
            1,
            output_length,
            input_length,
            0.0,
            vec![0.1, 0.5, 0.9],
            0.0,
        );

        let device = Default::default();
        let model = TiDEModel::<TrainBackend>::new(
            &device,
            parameters.input_size(),
            parameters.hidden_size(),
            parameters.encoder_layer_count(),
            parameters.decoder_layer_count(),
            parameters.output_length(),
            parameters.quantiles().len(),
            parameters.dropout_rate(),
        );

        let configuration = TrainConfiguration::new(0.01, 80, 16, 1000, 0.0)
            .expect("the fixture configuration must be valid");

        let (_model, losses) = train(model, &dataset, None, &parameters, &configuration, &device);
        assert!(losses.len() >= 2);
        let first = losses.first().unwrap();
        let last = losses.last().unwrap();
        assert!(last < first, "loss did not decrease: {first} -> {last}");
    }
}
