import tempfile

import numpy as np
import pytest
from tide.data import TrainingDataset
from tide.model import Model, quantile_loss
from tinygrad.tensor import Tensor

BATCH_SIZE = 4
INPUT_LENGTH = 35
OUTPUT_LENGTH = 5
CONTINUOUS_FEATURES = 7
CATEGORICAL_FEATURES = 5
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


def _make_dataset(
    num_samples: int = BATCH_SIZE,
    *,
    include_targets: bool = True,
) -> TrainingDataset:
    return TrainingDataset(
        past_continuous=rng.standard_normal(
            (num_samples, INPUT_LENGTH, CONTINUOUS_FEATURES)
        ).astype(np.float32),
        past_categorical=rng.integers(
            0,
            CATEGORICAL_UPPER_BOUND,
            (num_samples, INPUT_LENGTH, CATEGORICAL_FEATURES),
        ).astype(np.int32),
        future_categorical=rng.integers(
            0,
            CATEGORICAL_UPPER_BOUND,
            (num_samples, OUTPUT_LENGTH, CATEGORICAL_FEATURES),
        ).astype(np.int32),
        static_categorical=rng.integers(
            0,
            CATEGORICAL_UPPER_BOUND,
            (num_samples, 1, STATIC_FEATURES),
        ).astype(np.int32),
        targets=(
            rng.standard_normal((num_samples, OUTPUT_LENGTH, 1)).astype(np.float32)
            if include_targets
            else None
        ),
    )


def _make_batch(*, include_targets: bool = True) -> dict[str, Tensor]:
    """Create a single dict[str, Tensor] batch for forward/predict tests."""
    batch: dict[str, Tensor] = {
        "past_continuous_features": Tensor(
            rng.standard_normal((BATCH_SIZE, INPUT_LENGTH, CONTINUOUS_FEATURES)).astype(
                np.float32
            )
        ),
        "past_categorical_features": Tensor(
            rng.integers(
                0,
                CATEGORICAL_UPPER_BOUND,
                (BATCH_SIZE, INPUT_LENGTH, CATEGORICAL_FEATURES),
            ).astype(np.int32)
        ),
        "future_categorical_features": Tensor(
            rng.integers(
                0,
                CATEGORICAL_UPPER_BOUND,
                (BATCH_SIZE, OUTPUT_LENGTH, CATEGORICAL_FEATURES),
            ).astype(np.int32)
        ),
        "static_categorical_features": Tensor(
            rng.integers(
                0,
                CATEGORICAL_UPPER_BOUND,
                (BATCH_SIZE, 1, STATIC_FEATURES),
            ).astype(np.int32)
        ),
    }
    if include_targets:
        batch["targets"] = Tensor(
            rng.standard_normal((BATCH_SIZE, OUTPUT_LENGTH, 1)).astype(np.float32)
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


def test_quantile_loss_huber_path_returns_nonnegative() -> None:
    predictions = Tensor(
        rng.standard_normal((BATCH_SIZE, OUTPUT_LENGTH, NUM_QUANTILES)).astype(
            np.float32
        )
    )
    targets = Tensor(
        rng.standard_normal((BATCH_SIZE, OUTPUT_LENGTH)).astype(np.float32)
    )
    loss = quantile_loss(predictions, targets, huber_delta=0.5)
    assert loss.numpy().item() >= 0


def test_quantile_loss_huber_differs_from_pinball() -> None:
    predictions = Tensor(
        rng.standard_normal((BATCH_SIZE, OUTPUT_LENGTH, NUM_QUANTILES)).astype(
            np.float32
        )
    )
    targets = Tensor(
        rng.standard_normal((BATCH_SIZE, OUTPUT_LENGTH)).astype(np.float32)
    )
    pinball_loss = quantile_loss(predictions, targets, huber_delta=0.0)
    huber_loss = quantile_loss(predictions, targets, huber_delta=0.5)
    assert pinball_loss.numpy().item() != huber_loss.numpy().item()


def test_quantile_loss_rejects_negative_huber_delta() -> None:
    predictions = Tensor(
        rng.standard_normal((BATCH_SIZE, OUTPUT_LENGTH, NUM_QUANTILES)).astype(
            np.float32
        )
    )
    targets = Tensor(
        rng.standard_normal((BATCH_SIZE, OUTPUT_LENGTH)).astype(np.float32)
    )
    with pytest.raises(ValueError, match="huber_delta must be non-negative"):
        quantile_loss(predictions, targets, huber_delta=-0.5)


def test_model_train_with_huber_delta() -> None:
    input_size = _compute_input_size()
    model = Model(
        input_size=input_size,
        hidden_size=HIDDEN_SIZE,
        output_length=OUTPUT_LENGTH,
        huber_delta=0.5,
    )
    dataset = _make_dataset()
    losses = model.train(
        dataset=dataset,
        epochs=EPOCHS_SHORT,
        learning_rate=LEARNING_RATE,
        validate_data=False,
    )
    assert len(losses) == EPOCHS_SHORT
    assert all(isinstance(loss, float) for loss in losses)


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
    dataset = _make_dataset()
    losses = model.train(
        dataset=dataset,
        epochs=EPOCHS_SHORT,
        learning_rate=LEARNING_RATE,
        validate_data=False,
    )
    assert len(losses) == EPOCHS_SHORT
    assert all(isinstance(loss, float) for loss in losses)


def test_model_train_empty_dataset() -> None:
    input_size = _compute_input_size()
    model = Model(
        input_size=input_size, hidden_size=HIDDEN_SIZE, output_length=OUTPUT_LENGTH
    )
    empty_dataset = TrainingDataset(
        past_continuous=np.zeros(
            (0, INPUT_LENGTH, CONTINUOUS_FEATURES), dtype=np.float32
        ),
        past_categorical=np.zeros(
            (0, INPUT_LENGTH, CATEGORICAL_FEATURES), dtype=np.int32
        ),
        future_categorical=np.zeros(
            (0, OUTPUT_LENGTH, CATEGORICAL_FEATURES), dtype=np.int32
        ),
        static_categorical=np.zeros((0, 1, STATIC_FEATURES), dtype=np.int32),
        targets=np.zeros((0, OUTPUT_LENGTH, 1), dtype=np.float32),
    )
    losses = model.train(
        dataset=empty_dataset,
        epochs=EPOCHS_SHORT,
        learning_rate=LEARNING_RATE,
        validate_data=False,
    )
    assert losses == []


def test_model_train_missing_targets_raises() -> None:
    input_size = _compute_input_size()
    model = Model(
        input_size=input_size, hidden_size=HIDDEN_SIZE, output_length=OUTPUT_LENGTH
    )
    dataset = _make_dataset(include_targets=False)
    with pytest.raises(ValueError, match="Targets are required"):
        model.train(
            dataset=dataset,
            epochs=EPOCH_SINGLE,
            learning_rate=LEARNING_RATE,
            validate_data=False,
        )


def test_model_validate_model_returns_loss() -> None:
    input_size = _compute_input_size()
    model = Model(
        input_size=input_size, hidden_size=HIDDEN_SIZE, output_length=OUTPUT_LENGTH
    )
    dataset = _make_dataset()
    loss = model.validate_model(dataset)
    assert isinstance(loss, float)
    assert loss >= 0


def test_model_validate_model_empty_dataset_returns_nan() -> None:
    input_size = _compute_input_size()
    model = Model(
        input_size=input_size, hidden_size=HIDDEN_SIZE, output_length=OUTPUT_LENGTH
    )
    empty_dataset = TrainingDataset(
        past_continuous=np.zeros(
            (0, INPUT_LENGTH, CONTINUOUS_FEATURES), dtype=np.float32
        ),
        past_categorical=np.zeros(
            (0, INPUT_LENGTH, CATEGORICAL_FEATURES), dtype=np.int32
        ),
        future_categorical=np.zeros(
            (0, OUTPUT_LENGTH, CATEGORICAL_FEATURES), dtype=np.int32
        ),
        static_categorical=np.zeros((0, 1, STATIC_FEATURES), dtype=np.int32),
        targets=np.zeros((0, OUTPUT_LENGTH, 1), dtype=np.float32),
    )
    loss = model.validate_model(empty_dataset)
    assert np.isnan(loss)


def test_model_validate_model_missing_targets_raises() -> None:
    input_size = _compute_input_size()
    model = Model(
        input_size=input_size, hidden_size=HIDDEN_SIZE, output_length=OUTPUT_LENGTH
    )
    dataset = _make_dataset(include_targets=False)
    with pytest.raises(ValueError, match="Targets are required"):
        model.validate_model(dataset)


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
    dataset = _make_dataset()
    losses = model.train(
        dataset=dataset,
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
            dataset=_make_dataset(),
            epochs=EPOCH_SINGLE,
            validate_data=True,
            validation_sample_size=0,
        )


def test_model_validate_model_restores_training_state() -> None:
    input_size = _compute_input_size()
    model = Model(
        input_size=input_size, hidden_size=HIDDEN_SIZE, output_length=OUTPUT_LENGTH
    )
    dataset = _make_dataset()
    Tensor.training = True
    model.validate_model(dataset)
    assert Tensor.training is True

    Tensor.training = False
    model.validate_model(dataset)
    assert Tensor.training is False
