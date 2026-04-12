import json
import os
from typing import cast

import numpy as np
import structlog
from tinygrad import Device
from tinygrad.nn import Linear
from tinygrad.nn.optim import Adam
from tinygrad.nn.state import (
    get_parameters,
    get_state_dict,
    load_state_dict,
    safe_load,
    safe_save,
)
from tinygrad.tensor import Tensor

from tide.data import TrainingDataset

logger = structlog.get_logger()

_rng = np.random.default_rng()


def quantile_loss(
    predictions: Tensor,
    targets: Tensor,
    quantiles: list[float] | None = None,
    huber_delta: float = 0.0,
) -> Tensor:
    if quantiles is None:
        quantiles = [0.1, 0.5, 0.9]

    if not all(0 <= q <= 1 for q in quantiles):
        message = "All quantiles must be between 0 and 1"
        raise ValueError(message)

    if huber_delta < 0:
        message = f"huber_delta must be non-negative, got {huber_delta}"
        raise ValueError(message)

    errors_total = Tensor(0.0)
    for index, quantile in enumerate(quantiles):
        error = targets.sub(predictions[:, :, index])

        if huber_delta > 0:
            abs_error = error.abs()
            delta_t = Tensor(huber_delta)
            huber_error = Tensor.where(
                abs_error <= delta_t,
                cast("Tensor", (error**2) / (Tensor(2.0) * delta_t)),
                cast("Tensor", abs_error - delta_t / Tensor(2.0)),
            )
            quantile_tensor = Tensor(quantile)
            sign_weight = Tensor.where(
                error > 0,
                quantile_tensor,
                cast("Tensor", Tensor(1.0) - quantile_tensor),
            )
            errors_total = errors_total.add(
                cast("Tensor", sign_weight * huber_error).mean()
            )
        else:
            quantile_tensor = Tensor(quantile)
            errors_total = errors_total.add(
                Tensor.where(
                    error > 0,
                    cast("Tensor", quantile_tensor.mul(error)),
                    cast("Tensor", (quantile_tensor.sub(1)).mul(error)),
                ).mean()
            )

    return cast("Tensor", errors_total.div(len(quantiles)))


class _ResidualBlock:
    """Residual block with layer normalization and dropout"""

    def __init__(
        self,
        input_size: int,
        hidden_size: int,
        dropout_rate: float = 0.1,
    ) -> None:
        self.input_size = input_size
        self.hidden_size = hidden_size
        self.dropout_rate = dropout_rate

        self.dense = Linear(in_features=input_size, out_features=hidden_size)

        self.skip_connection = None
        if input_size != hidden_size:
            self.skip_connection = Linear(
                in_features=input_size,
                out_features=hidden_size,
            )

    def forward(self, x: Tensor) -> Tensor:
        x = x.cast("float32")  # ensure float32 precision

        out = self.dense(x).relu()  # relu activation

        if Tensor.training and self.dropout_rate > 0:
            out = out.dropout(p=self.dropout_rate)

        skip = x
        if self.skip_connection is not None:
            skip = self.skip_connection(x)

        out = cast("Tensor", out.add(skip))  # add residual connection

        mean = out.mean(axis=-1, keepdim=True)
        variance = ((out.sub(mean)) ** 2).mean(axis=-1, keepdim=True)
        return cast(
            "Tensor",
            (out - mean) / (variance + Tensor(1e-5).cast("float32")).sqrt(),
        )


class Model:
    """
    TiDE architecture for time series forecasting

    Model paper reference: https://arxiv.org/pdf/2304.08424"""

    def __init__(  # noqa: PLR0913
        self,
        input_size: int,
        hidden_size: int = 128,
        num_encoder_layers: int = 2,
        num_decoder_layers: int = 2,
        output_length: int = 5,  # number of trading days to forecast
        dropout_rate: float = 0.1,
        quantiles: list[float] | None = None,
        huber_delta: float = 0.0,
    ) -> None:
        self.input_size = input_size
        self.hidden_size = hidden_size
        self.num_encoder_layers = num_encoder_layers
        self.num_decoder_layers = num_decoder_layers
        self.output_length = output_length
        self.dropout_rate = dropout_rate
        self.quantiles = quantiles or [0.1, 0.5, 0.9]
        self.huber_delta = huber_delta

        self.feature_projection_1 = Linear(
            in_features=self.input_size,
            out_features=self.hidden_size * 2,
        )
        self.feature_projection_2 = Linear(
            in_features=self.hidden_size * 2,
            out_features=self.hidden_size,
        )

        self.encoder_blocks: list[_ResidualBlock] = []
        for _ in range(self.num_encoder_layers):
            block_input_size = self.hidden_size
            self.encoder_blocks.append(
                _ResidualBlock(
                    input_size=block_input_size,
                    hidden_size=self.hidden_size,
                    dropout_rate=self.dropout_rate,
                )
            )

        self.decoder_blocks: list[_ResidualBlock] = []
        for _ in range(self.num_decoder_layers):
            block_input_size = self.hidden_size
            self.decoder_blocks.append(
                _ResidualBlock(
                    input_size=block_input_size,
                    hidden_size=self.hidden_size,
                    dropout_rate=self.dropout_rate,
                )
            )

        # combined projection: hidden -> output_length * num_quantiles
        # avoids applying Linear to 3D tensors which triggers a tinygrad
        # CPU codegen bug in the backward pass (linearizer.py assertion)
        self.output_projection = Linear(
            in_features=self.hidden_size,
            out_features=self.output_length * len(self.quantiles),
        )

    def forward(self, x: Tensor) -> Tensor:
        """
        Forward pass through TiDE model
        Args:
            x: Input tensor of shape (batch_size, flattened_features)
        Returns:
            Tensor of shape (batch_size, output_length, num_quantiles)
        """
        x = x.cast("float32")  # ensure float32 precision
        batch_size = x.shape[0]

        x = self.feature_projection_1(x).relu()
        x = self.feature_projection_2(x).relu()

        for encoder_block in self.encoder_blocks:
            x = encoder_block.forward(x)

        encoder_output = cast("Tensor", x)

        for decoder_block in self.decoder_blocks:
            x = decoder_block.forward(x)

        # add skip connection from encoder to decoder output
        x = cast("Tensor", x.add(encoder_output))

        # layer normalization after skip connection
        mean = x.mean(axis=-1, keepdim=True)
        variance = ((x.sub(mean)) ** 2).mean(axis=-1, keepdim=True)
        x = cast(
            "Tensor",
            (x - mean) / (variance + Tensor(1e-5).cast("float32")).sqrt(),
        )

        # combined projection to (batch_size, output_length * num_quantiles)
        x = self.output_projection(x.relu())

        # reshape to (batch_size, output_length, num_quantiles)
        return x.reshape(batch_size, self.output_length, len(self.quantiles))

    def validate_training_data(
        self,
        dataset: TrainingDataset,
        sample_size: int = 10,
    ) -> bool:
        """Validate training data for NaN/Inf values."""
        total_samples = len(dataset)
        actual_sample_size = min(sample_size, total_samples)

        logger.info(
            "Validating training data",
            total_samples=total_samples,
            sample_size=actual_sample_size,
        )

        all_issues: dict[str, dict] = {}

        sampled_indices = _rng.choice(total_samples, actual_sample_size, replace=False)

        arrays: dict[str, np.ndarray] = {
            "past_continuous": dataset.past_continuous[sampled_indices],
            "past_categorical": dataset.past_categorical[sampled_indices],
            "future_categorical": dataset.future_categorical[sampled_indices],
            "static_categorical": dataset.static_categorical[sampled_indices],
        }
        if dataset.targets is not None:
            arrays["targets"] = dataset.targets[sampled_indices]

        for name, array in arrays.items():
            if array.dtype.kind != "f":
                continue
            nan_count = int(np.isnan(array).sum())
            inf_count = int(np.isinf(array).sum())
            if nan_count > 0 or inf_count > 0:
                all_issues[name] = {
                    "nan_count": nan_count,
                    "inf_count": inf_count,
                    "total_elements": array.size,
                    "nan_pct": f"{(nan_count / array.size) * 100:.2f}%",
                }

        if all_issues:
            for feature_key, stats in all_issues.items():
                logger.error(
                    "Invalid values in training data",
                    feature=feature_key,
                    **stats,
                )
            return False

        logger.info("Training data validation passed")
        return True

    def train(  # noqa: PLR0913, PLR0912, PLR0915, C901
        self,
        dataset: TrainingDataset,
        batch_size: int = 256,
        epochs: int = 10,
        learning_rate: float = 0.001,
        log_interval: int = 100,
        validate_data: bool = True,  # noqa: FBT001, FBT002
        validation_sample_size: int = 10,
        validation_dataset: TrainingDataset | None = None,
        early_stopping_patience: int | None = 3,
        early_stopping_min_delta: float = 0.001,
        checkpoint_directory: str | None = None,
    ) -> list[float]:
        """Train the TiDE model using quantile loss."""
        if len(dataset) == 0:
            return []

        if dataset.targets is None:
            message = "Targets are required for training"
            raise ValueError(message)

        if validation_sample_size <= 0:
            message = "validation_sample_size must be positive"
            raise ValueError(message)

        if validate_data:
            is_valid = self.validate_training_data(
                dataset,
                sample_size=validation_sample_size,
            )
            if not is_valid:
                message = "Training data contains NaN or Inf values"
                raise ValueError(message)

        prev_training = Tensor.training
        Tensor.training = True

        parameters = get_parameters(self)
        optimizer = Adam(params=parameters, lr=learning_rate)
        losses = []
        num_samples = len(dataset)
        total_batches = (num_samples + batch_size - 1) // batch_size

        best_loss = float("inf")
        best_saved_loss = float("inf")
        epochs_without_improvement = 0
        checkpoint_path = None
        checkpoint_saved = False

        if checkpoint_directory:
            os.makedirs(checkpoint_directory, exist_ok=True)  # noqa: PTH103
            checkpoint_path = os.path.join(  # noqa: PTH118
                checkpoint_directory, "best_checkpoint.safetensor"
            )

        logger.info("Training device", device=Device.DEFAULT)

        try:
            for epoch in range(epochs):
                logger.info(
                    "Starting training epoch",
                    epoch=epoch + 1,
                    total_epochs=epochs,
                    total_batches=total_batches,
                )
                epoch_losses = []
                indices = _rng.permutation(num_samples)

                for step in range(total_batches):
                    batch_idx = indices[step * batch_size : (step + 1) * batch_size]
                    batch = {
                        "past_continuous_features": Tensor(
                            dataset.past_continuous[batch_idx]
                        ),
                        "past_categorical_features": Tensor(
                            dataset.past_categorical[batch_idx]
                        ),
                        "future_categorical_features": Tensor(
                            dataset.future_categorical[batch_idx]
                        ),
                        "static_categorical_features": Tensor(
                            dataset.static_categorical[batch_idx]
                        ),
                        "targets": Tensor(dataset.targets[batch_idx]),
                    }

                    combined_input_features, targets, batch_size_actual = (
                        self._combine_input_features(batch)
                    )

                    if targets is None:
                        message = "Targets are required for training batches"
                        raise ValueError(message)

                    # predictions shape: (batch_size, output_length, num_quantiles)
                    predictions = self.forward(combined_input_features)

                    # reshape targets to (batch_size, output_length)
                    targets_reshaped = targets.reshape(
                        batch_size_actual, self.output_length
                    )

                    loss = quantile_loss(
                        predictions,
                        targets_reshaped,
                        self.quantiles,
                        huber_delta=self.huber_delta,
                    )

                    optimizer.zero_grad()
                    loss.backward()
                    optimizer.step()
                    Tensor.realize(*get_parameters(self))

                    step_loss = loss.numpy().item()
                    epoch_losses.append(step_loss)

                    if (step + 1) % log_interval == 0 or (step + 1) == total_batches:
                        running_avg_loss = sum(epoch_losses) / len(epoch_losses)
                        progress_pct = ((step + 1) / total_batches) * 100
                        logger.info(
                            "Training step",
                            epoch=epoch + 1,
                            step=step + 1,
                            total_steps=total_batches,
                            progress=f"{progress_pct:.1f}%",
                            step_loss=f"{step_loss:.4f}",
                            running_avg_loss=f"{running_avg_loss:.4f}",
                        )

                if not epoch_losses:
                    logger.warning("No training batches processed", epoch=epoch + 1)
                    continue

                epoch_loss = sum(epoch_losses) / len(epoch_losses)

                # Use validation loss for early stopping if validation_dataset provided
                if validation_dataset is not None:
                    stopping_loss = self.validate_model(validation_dataset, batch_size)
                else:
                    stopping_loss = epoch_loss

                logger.info(
                    "Completed training epoch",
                    epoch=epoch + 1,
                    total_epochs=epochs,
                    epoch_loss=f"{epoch_loss:.4f}",
                    best_loss=f"{best_loss:.4f}",
                )

                losses.append(epoch_loss)

                if checkpoint_path and epoch_loss < best_saved_loss:
                    best_saved_loss = epoch_loss
                    safe_save(get_state_dict(self), checkpoint_path)
                    checkpoint_saved = True
                    logger.info(
                        "Saved best checkpoint",
                        checkpoint_path=checkpoint_path,
                        loss=f"{epoch_loss:.4f}",
                    )

                if early_stopping_patience is not None:
                    if stopping_loss < best_loss - early_stopping_min_delta:
                        best_loss = stopping_loss
                        epochs_without_improvement = 0
                        logger.info(
                            "New best loss",
                            best_loss=f"{best_loss:.4f}",
                        )
                    else:
                        epochs_without_improvement += 1
                        logger.info(
                            "No improvement",
                            epochs_without_improvement=epochs_without_improvement,
                            patience=early_stopping_patience,
                        )

                    if epochs_without_improvement >= early_stopping_patience:
                        logger.info(
                            "Early stopping triggered",
                            epoch=epoch + 1,
                            best_loss=f"{best_loss:.4f}",
                            epochs_without_improvement=epochs_without_improvement,
                        )
                        break
        finally:
            Tensor.training = prev_training

        if (
            checkpoint_saved and checkpoint_path and os.path.exists(checkpoint_path)  # noqa: PTH110
        ):
            logger.info(
                "Restoring best checkpoint weights",
                checkpoint_path=checkpoint_path,
            )
            best_state = safe_load(checkpoint_path)
            load_state_dict(self, best_state)

        return losses

    def validate_model(self, dataset: TrainingDataset, batch_size: int = 256) -> float:
        """Validate the model using quantile loss."""
        prev_training = Tensor.training
        Tensor.training = False
        try:
            if len(dataset) == 0:
                logger.warning("No validation samples provided; returning NaN loss")
                return float("nan")

            if dataset.targets is None:
                message = "Targets are required for validation"
                raise ValueError(message)

            validation_losses = []
            num_samples = len(dataset)
            total_batches = (num_samples + batch_size - 1) // batch_size

            for step in range(total_batches):
                batch_idx = np.arange(
                    step * batch_size, min((step + 1) * batch_size, num_samples)
                )
                batch = {
                    "past_continuous_features": Tensor(
                        dataset.past_continuous[batch_idx]
                    ),
                    "past_categorical_features": Tensor(
                        dataset.past_categorical[batch_idx]
                    ),
                    "future_categorical_features": Tensor(
                        dataset.future_categorical[batch_idx]
                    ),
                    "static_categorical_features": Tensor(
                        dataset.static_categorical[batch_idx]
                    ),
                    "targets": Tensor(dataset.targets[batch_idx]),
                }

                combined_input, targets, batch_size_actual = (
                    self._combine_input_features(batch)
                )

                if targets is None:
                    message = "Targets are required for validation batches"
                    raise ValueError(message)

                predictions = self.forward(combined_input)
                targets_reshaped = targets.reshape(
                    batch_size_actual, self.output_length
                )
                loss = quantile_loss(predictions, targets_reshaped, self.quantiles)
                validation_losses.append(loss.numpy().item())

            if not validation_losses:
                logger.warning("No validation batches processed; returning NaN loss")
                return float("nan")

            return sum(validation_losses) / len(validation_losses)
        finally:
            Tensor.training = prev_training

    def save(
        self,
        directory_path: str,
    ) -> None:
        os.makedirs(directory_path, exist_ok=True)  # noqa: PTH103

        states = get_state_dict(self)

        safe_save(states, os.path.join(directory_path, "tide_states.safetensor"))  # noqa: PTH118

        parameters = {
            "input_size": self.input_size,
            "hidden_size": self.hidden_size,
            "num_encoder_layers": self.num_encoder_layers,
            "num_decoder_layers": self.num_decoder_layers,
            "output_length": self.output_length,
            "dropout_rate": self.dropout_rate,
            "quantiles": self.quantiles,
            "huber_delta": self.huber_delta,
        }

        parameters_file_path = os.path.join(directory_path, "tide_parameters.json")  # noqa: PTH118
        with open(parameters_file_path, "w") as parameters_file:  # noqa: PTH123
            json.dump(parameters, parameters_file)

    @classmethod
    def load(
        cls,
        directory_path: str,
    ) -> "Model":
        states_file_path = os.path.join(directory_path, "tide_states.safetensor")  # noqa: PTH118
        states = safe_load(states_file_path)
        with open(  # noqa: PTH123
            os.path.join(directory_path, "tide_parameters.json")  # noqa: PTH118
        ) as parameters_file:
            parameters = json.load(parameters_file)

        model = cls(**parameters)

        load_state_dict(model, states)

        return model

    def predict(
        self,
        inputs: dict[str, Tensor],
    ) -> Tensor:
        combined_input_features, _, _ = self._combine_input_features(inputs)

        return self.forward(combined_input_features)

    def _combine_input_features(
        self,
        inputs: dict[str, Tensor],
    ) -> tuple[Tensor, Tensor | None, int]:
        batch_size = inputs["past_continuous_features"].shape[0]

        past_cont_flat = inputs["past_continuous_features"].reshape(batch_size, -1)
        past_cat_flat = (
            inputs["past_categorical_features"].reshape(batch_size, -1).cast("float32")
        )
        future_cat_flat = (
            inputs["future_categorical_features"]
            .reshape(batch_size, -1)
            .cast("float32")
        )
        static_cat_flat = (
            inputs["static_categorical_features"]
            .reshape(batch_size, -1)
            .cast("float32")
        )

        return (
            Tensor.cat(
                past_cont_flat,
                past_cat_flat,
                future_cat_flat,
                static_cat_flat,
                dim=1,
            ),
            inputs.get("targets"),
            int(batch_size),
        )
