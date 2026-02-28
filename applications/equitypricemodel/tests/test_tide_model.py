import tempfile
from dataclasses import dataclass

import numpy as np
import pytest
from equitypricemodel.tide_model import Model, quantile_loss
from tinygrad.tensor import Tensor

BATCH_SIZE = 4
INPUT_LENGTH = 35
OUTPUT_LENGTH = 7
CONTINUOUS_FEATURES = 7
CATEGORICAL_FEATURES = 6
STATIC_FEATURES = 3
HIDDEN_SIZE = 32
NUM_QUANTILES = 3
CATEGORICAL_UPPER_BOUND = 10
EPOCHS_SHORT = 2
EPOCHS_LONG = 20
LEARNING_RATE = 0.001
EARLY_STOPPING_PATIENCE = 2
EPOCH_SINGLE = 1

rng = np.random.default_rng(42)


@dataclass
class BatchConfig:
    batch_size: int = BATCH_SIZE
    input_length: int = INPUT_LENGTH
    output_length: int = OUTPUT_LENGTH
    continuous_features: int = CONTINUOUS_FEATURES
    categorical_features: int = CATEGORICAL_FEATURES
    static_features: int = STATIC_FEATURES


def _make_batch(
    config: BatchConfig | None = None,
    *,
    include_targets: bool = True,
) -> dict[str, Tensor]:
    if config is None:
        config = BatchConfig()
    batch: dict[str, Tensor] = {
        "encoder_continuous_features": Tensor(
            rng.standard_normal(
                (config.batch_size, config.input_length, config.continuous_features)
            ).astype(np.float32)
        ),
        "encoder_categorical_features": Tensor(
            rng.integers(
                0,
                CATEGORICAL_UPPER_BOUND,
                (config.batch_size, config.input_length, config.categorical_features),
            ).astype(np.int32)
        ),
        "decoder_categorical_features": Tensor(
            rng.integers(
                0,
                CATEGORICAL_UPPER_BOUND,
                (config.batch_size, config.output_length, config.categorical_features),
            ).astype(np.int32)
        ),
        "static_categorical_features": Tensor(
            rng.integers(
                0,
                CATEGORICAL_UPPER_BOUND,
                (config.batch_size, 1, config.static_features),
            ).astype(np.int32)
        ),
    }
    if include_targets:
        batch["targets"] = Tensor(
            rng.standard_normal((config.batch_size, config.output_length, 1)).astype(
                np.float32
            )
        )
    return batch


def _compute_input_size(
    input_length: int = INPUT_LENGTH,
    output_length: int = OUTPUT_LENGTH,
    continuous_features: int = CONTINUOUS_FEATURES,
    categorical_features: int = CATEGORICAL_FEATURES,
    static_features: int = STATIC_FEATURES,
) -> int:
    return (
        input_length * continuous_features
        + input_length * categorical_features
        + output_length * categorical_features
        + 1 * static_features
    )


def test_quantile_loss_valid_inputs() -> None:
    predictions = Tensor(
        rng.standard_normal((BATCH_SIZE, OUTPUT_LENGTH, NUM_QUANTILES)).astype(
            np.float32
        )
    )
    targets = Tensor(
        rng.standard_normal((BATCH_SIZE, OUTPUT_LENGTH)).astype(np.float32)
    )
    loss = quantile_loss(predictions, targets)
    assert loss.numpy().item() >= 0


def test_quantile_loss_rejects_invalid_quantiles() -> None:
    predictions = Tensor(
        rng.standard_normal((BATCH_SIZE, OUTPUT_LENGTH, NUM_QUANTILES)).astype(
            np.float32
        )
    )
    targets = Tensor(
        rng.standard_normal((BATCH_SIZE, OUTPUT_LENGTH)).astype(np.float32)
    )
    with pytest.raises(ValueError, match="between 0 and 1"):
        quantile_loss(predictions, targets, quantiles=[0.1, 1.5, 0.9])


def test_model_forward_output_shape() -> None:
    input_size = _compute_input_size()
    model = Model(
        input_size=input_size, hidden_size=HIDDEN_SIZE, output_length=OUTPUT_LENGTH
    )
    batch = _make_batch()
    combined, _, _ = model._combine_input_features(batch)  # noqa: SLF001
    output = model.forward(combined)
    assert output.shape == (BATCH_SIZE, OUTPUT_LENGTH, NUM_QUANTILES)


def test_model_train_returns_losses() -> None:
    input_size = _compute_input_size()
    model = Model(
        input_size=input_size, hidden_size=HIDDEN_SIZE, output_length=OUTPUT_LENGTH
    )
    batches = [_make_batch()]
    losses = model.train(
        train_batches=batches,
        epochs=EPOCHS_SHORT,
        learning_rate=LEARNING_RATE,
        validate_data=False,
    )
    assert len(losses) == EPOCHS_SHORT
    assert all(isinstance(loss, float) for loss in losses)


def test_model_train_empty_batch_list() -> None:
    input_size = _compute_input_size()
    model = Model(
        input_size=input_size, hidden_size=HIDDEN_SIZE, output_length=OUTPUT_LENGTH
    )
    losses = model.train(
        train_batches=[],
        epochs=EPOCHS_SHORT,
        learning_rate=LEARNING_RATE,
        validate_data=False,
    )
    assert losses == []


def test_model_train_skips_zero_size_batch() -> None:
    input_size = _compute_input_size()
    model = Model(
        input_size=input_size, hidden_size=HIDDEN_SIZE, output_length=OUTPUT_LENGTH
    )
    empty_batch = {
        "encoder_continuous_features": Tensor(
            np.zeros((0, INPUT_LENGTH, CONTINUOUS_FEATURES), dtype=np.float32)
        ),
        "encoder_categorical_features": Tensor(
            np.zeros((0, INPUT_LENGTH, CATEGORICAL_FEATURES), dtype=np.int32)
        ),
        "decoder_categorical_features": Tensor(
            np.zeros((0, OUTPUT_LENGTH, CATEGORICAL_FEATURES), dtype=np.int32)
        ),
        "static_categorical_features": Tensor(
            np.zeros((0, 1, STATIC_FEATURES), dtype=np.int32)
        ),
        "targets": Tensor(np.zeros((0, OUTPUT_LENGTH, 1), dtype=np.float32)),
    }
    normal_batch = _make_batch(BatchConfig(batch_size=BATCH_SIZE))
    losses = model.train(
        train_batches=[empty_batch, normal_batch],
        epochs=EPOCH_SINGLE,
        learning_rate=LEARNING_RATE,
        validate_data=False,
    )
    assert len(losses) == EPOCH_SINGLE


def test_model_train_missing_targets_raises() -> None:
    input_size = _compute_input_size()
    model = Model(
        input_size=input_size, hidden_size=HIDDEN_SIZE, output_length=OUTPUT_LENGTH
    )
    batch = _make_batch(include_targets=False)
    with pytest.raises(ValueError, match="Targets are required"):
        model.train(
            train_batches=[batch],
            epochs=EPOCH_SINGLE,
            learning_rate=LEARNING_RATE,
            validate_data=False,
        )


def test_model_validate_returns_loss() -> None:
    input_size = _compute_input_size()
    model = Model(
        input_size=input_size, hidden_size=HIDDEN_SIZE, output_length=OUTPUT_LENGTH
    )
    batches = [_make_batch()]
    loss = model.validate(batches)
    assert isinstance(loss, float)
    assert loss >= 0


def test_model_validate_empty_batches_returns_nan() -> None:
    input_size = _compute_input_size()
    model = Model(
        input_size=input_size, hidden_size=HIDDEN_SIZE, output_length=OUTPUT_LENGTH
    )
    loss = model.validate([])
    assert np.isnan(loss)


def test_model_validate_missing_targets_raises() -> None:
    input_size = _compute_input_size()
    model = Model(
        input_size=input_size, hidden_size=HIDDEN_SIZE, output_length=OUTPUT_LENGTH
    )
    batch = _make_batch(include_targets=False)
    with pytest.raises(ValueError, match="Targets are required"):
        model.validate([batch])


def test_model_predict_output_shape() -> None:
    input_size = _compute_input_size()
    model = Model(
        input_size=input_size, hidden_size=HIDDEN_SIZE, output_length=OUTPUT_LENGTH
    )
    batch = _make_batch(include_targets=False)
    predictions = model.predict(batch)
    assert predictions.shape == (BATCH_SIZE, OUTPUT_LENGTH, NUM_QUANTILES)


def test_model_save_and_load_roundtrip() -> None:
    input_size = _compute_input_size()
    model = Model(
        input_size=input_size, hidden_size=HIDDEN_SIZE, output_length=OUTPUT_LENGTH
    )
    with tempfile.TemporaryDirectory() as tmpdir:
        model.save(tmpdir)
        loaded = Model.load(tmpdir)
        assert loaded.input_size == model.input_size
        assert loaded.hidden_size == model.hidden_size
        assert loaded.output_length == model.output_length
        assert loaded.quantiles == model.quantiles


def test_model_early_stopping() -> None:
    input_size = _compute_input_size()
    model = Model(
        input_size=input_size, hidden_size=HIDDEN_SIZE, output_length=OUTPUT_LENGTH
    )
    batches = [_make_batch()]
    losses = model.train(
        train_batches=batches,
        epochs=EPOCHS_LONG,
        learning_rate=LEARNING_RATE,
        validate_data=False,
        early_stopping_patience=EARLY_STOPPING_PATIENCE,
    )
    assert len(losses) <= EPOCHS_LONG


def test_model_validation_sample_size_must_be_positive() -> None:
    input_size = _compute_input_size()
    model = Model(
        input_size=input_size, hidden_size=HIDDEN_SIZE, output_length=OUTPUT_LENGTH
    )
    with pytest.raises(ValueError, match="positive"):
        model.train(
            train_batches=[_make_batch()],
            epochs=EPOCH_SINGLE,
            validate_data=True,
            validation_sample_size=0,
        )


def test_model_validate_restores_training_state() -> None:
    input_size = _compute_input_size()
    model = Model(
        input_size=input_size, hidden_size=HIDDEN_SIZE, output_length=OUTPUT_LENGTH
    )
    batches = [_make_batch()]
    Tensor.training = True
    model.validate(batches)
    assert Tensor.training is True

    Tensor.training = False
    model.validate(batches)
    assert Tensor.training is False
