import json
import os
from typing import cast

import numpy as np
import structlog
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

logger = structlog.get_logger()


def quantile_loss(
    predictions: Tensor,
    targets: Tensor,
    quantiles: list[float] | None = None,
) -> Tensor:
    if quantiles is None:
        quantiles = [0.1, 0.5, 0.9]

    if not all(0 <= q <= 1 for q in quantiles):
        message = "All quantiles must be between 0 and 1"
        raise ValueError(message)

    errors_total = Tensor(0.0)
    for index, quantile in enumerate(quantiles):
        error = targets.sub(predictions[:, :, index])
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
        output_length: int = 7,  # number of days to forecast
        dropout_rate: float = 0.1,
        quantiles: list[float] | None = None,
    ) -> None:
        self.input_size = input_size
        self.hidden_size = hidden_size
        self.num_encoder_layers = num_encoder_layers
        self.num_decoder_layers = num_decoder_layers
        self.output_length = output_length
        self.dropout_rate = dropout_rate
        self.quantiles = quantiles or [0.1, 0.5, 0.9]

        self.feature_projection = Linear(
            in_features=self.input_size,
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

        self.temporal_projection = Linear(  # projects to output sequence length
            in_features=self.hidden_size,
            out_features=self.hidden_size * self.output_length,
        )

        # final output layer for quantiles
        self.output_layer = Linear(
            in_features=self.hidden_size,
            out_features=len(self.quantiles),
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

        x = self.feature_projection(x).relu()

        for encoder_block in self.encoder_blocks:
            x = encoder_block.forward(x)

        encoder_output = cast("Tensor", x)

        for decoder_block in self.decoder_blocks:
            x = decoder_block.forward(x)

        # add skip connection from encoder to decoder output
        x = cast("Tensor", x.add(encoder_output))

        # temporal projection to output sequence length
        x = self.temporal_projection(x).relu()

        # reshape to (batch_size, output_length, hidden_size)
        x = x.reshape(batch_size, self.output_length, self.hidden_size)

        # apply output layer across the sequence dimension
        predictions: list[Tensor] = []
        for t in range(self.output_length):
            prediction_batch = self.output_layer(
                x[:, t, :]
            )  # (batch_size, num_quantiles)
            predictions.append(prediction_batch)

        predictions_first = predictions[0]
        predictions_rest = predictions[1:]

        # stack predictions: (batch_size, output_length, num_quantiles e.g. (32, 7, 3))
        return predictions_first.stack(*predictions_rest, dim=1)

    def _validate_batch(self, batch: dict[str, Tensor], _batch_idx: int) -> dict:
        """Check a batch for NaN/Inf values and return statistics."""
        issues = {}
        for key, tensor in batch.items():
            data = tensor.numpy()
            nan_count = int(np.isnan(data).sum())
            inf_count = int(np.isinf(data).sum())
            if nan_count > 0 or inf_count > 0:
                issues[key] = {
                    "nan_count": nan_count,
                    "inf_count": inf_count,
                    "total_elements": data.size,
                    "nan_pct": f"{(nan_count / data.size) * 100:.2f}%",
                }
        return issues

    def validate_training_data(
        self,
        train_batches: list,
        sample_size: int = 10,
    ) -> bool:
        """Validate training data for NaN/Inf values."""
        logger.info(
            "Validating training data",
            total_batches=len(train_batches),
            sample_size=min(sample_size, len(train_batches)),
        )

        all_issues: dict[str, dict] = {}
        indices_to_check = [0, len(train_batches) - 1]
        step = max(1, len(train_batches) // sample_size)
        indices_to_check.extend(range(0, len(train_batches), step))
        indices_to_check = sorted(set(indices_to_check))[:sample_size]

        for idx in indices_to_check:
            batch_issues = self._validate_batch(train_batches[idx], idx)
            if batch_issues:
                all_issues[f"batch_{idx}"] = batch_issues

        if all_issues:
            for batch_key, features in all_issues.items():
                for feature_key, stats in features.items():
                    logger.error(
                        "Invalid values in training data",
                        batch=batch_key,
                        feature=feature_key,
                        **stats,
                    )
            return False

        logger.info("Training data validation passed")
        return True

    def train(  # noqa: PLR0913
        self,
        train_batches: list,
        epochs: int = 10,
        learning_rate: float = 0.001,
        log_interval: int = 100,
        validate_data: bool = True,  # noqa: FBT001, FBT002
        early_stopping_patience: int | None = 3,
        early_stopping_min_delta: float = 0.001,
    ) -> list:
        """Train the TiDE model using quantile loss.

        Args:
            train_batches: List of training batch dictionaries
            epochs: Maximum number of epochs to train
            learning_rate: Learning rate for optimizer
            log_interval: Log progress every N steps
            validate_data: Whether to validate data before training
            early_stopping_patience: Stop if no improvement for N epochs
                (None to disable)
            early_stopping_min_delta: Minimum improvement to reset patience counter
        """
        if validate_data:
            is_valid = self.validate_training_data(train_batches)
            if not is_valid:
                message = "Training data contains NaN or Inf values"
                raise ValueError(message)

        prev_training = Tensor.training
        Tensor.training = True

        parameters = get_parameters(self)
        optimizer = Adam(params=parameters, lr=learning_rate)
        losses = []
        total_batches = len(train_batches)

        best_loss = float("inf")
        epochs_without_improvement = 0

        try:
            for epoch in range(epochs):
                logger.info(
                    "Starting training epoch",
                    epoch=epoch + 1,
                    total_epochs=epochs,
                    total_batches=total_batches,
                )
                epoch_losses = []

                for step, batch in enumerate(train_batches):
                    combined_input_features, targets, batch_size = (
                        self._combine_input_features(batch)
                    )

                    # predictions shape: (batch_size, output_length, num_quantiles)
                    predictions = self.forward(combined_input_features)

                    # reshape targets to (batch_size, output_length)
                    targets_reshaped = targets.reshape(batch_size, self.output_length)

                    loss = quantile_loss(predictions, targets_reshaped, self.quantiles)

                    optimizer.zero_grad()
                    loss.backward()
                    optimizer.step()

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
                    logger.warning(
                        "No training batches processed", epoch=epoch + 1
                    )
                    continue

                epoch_loss = sum(epoch_losses) / len(epoch_losses)

                logger.info(
                    "Completed training epoch",
                    epoch=epoch + 1,
                    total_epochs=epochs,
                    epoch_loss=f"{epoch_loss:.4f}",
                    best_loss=f"{best_loss:.4f}",
                )

                losses.append(epoch_loss)

                if early_stopping_patience is not None:
                    if epoch_loss < best_loss - early_stopping_min_delta:
                        best_loss = epoch_loss
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

        return losses

    def validate(self, validation_batches: list) -> float:
        """Validate the model using quantile loss"""
        Tensor.training = False
        validation_losses = []

        for batch in validation_batches:
            combined_input, targets, batch_size = self._combine_input_features(batch)

            predictions = self.forward(combined_input)

            targets_reshaped = targets.reshape(batch_size, self.output_length)

            loss = quantile_loss(predictions, targets_reshaped, self.quantiles)
            validation_losses.append(loss.numpy().item())

        if not validation_losses:
            logger.warning("No validation batches provided; returning NaN loss")
            return float("nan")

        return sum(validation_losses) / len(validation_losses)

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
        batch_size = inputs.get("encoder_continuous_features").shape[0]

        encoder_cont_flat = inputs.get("encoder_continuous_features").reshape(
            batch_size, -1
        )
        encoder_cat_flat = (
            inputs.get("encoder_categorical_features")
            .reshape(batch_size, -1)
            .cast("float32")
        )
        decoder_cat_flat = (
            inputs.get("decoder_categorical_features")
            .reshape(batch_size, -1)
            .cast("float32")
        )
        static_cat_flat = (
            inputs.get("static_categorical_features")
            .reshape(batch_size, -1)
            .cast("float32")
        )

        return (
            Tensor.cat(
                encoder_cont_flat,
                encoder_cat_flat,
                decoder_cat_flat,
                static_cat_flat,
                dim=1,
            ),
            inputs.get("targets"),
            int(batch_size),
        )
