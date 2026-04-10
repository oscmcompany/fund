from typing import Any, cast

import polars as pl
import structlog

from tide.tide_data import Data
from tide.tide_model import Model
from tide.tracking import log_epoch_loss, log_training_result

logger = structlog.get_logger()

DEFAULT_CONFIGURATION: dict[str, Any] = {
    "architecture": "TiDE",
    "learning_rate": 0.0005,
    "epoch_count": 50,
    "early_stopping_patience": 10,
    "validation_split": 0.8,
    "input_length": 35,
    "output_length": 7,
    "hidden_size": 64,
    "num_encoder_layers": 3,
    "num_decoder_layers": 2,
    "dropout_rate": 0.1,
    "batch_size": 128,
    "huber_delta": 0.5,
    "quantiles": [0.1, 0.5, 0.9],
}


def train_model(
    training_data: pl.DataFrame,
    configuration: dict | None = None,
    checkpoint_directory: str | None = None,
) -> tuple[Model, Data, list[float]]:
    """Train TiDE model and return model + data processor."""
    merged_configuration = dict(DEFAULT_CONFIGURATION)
    if configuration is not None:
        merged_configuration.update(configuration)
    configuration = merged_configuration

    logger.info("Configuration loaded", **configuration)

    logger.info("Initializing data processor")
    tide_data = Data()

    logger.info("Preprocessing training data")
    tide_data.preprocess_and_set_data(data=training_data)

    logger.info("Getting data dimensions")
    dimensions = tide_data.get_dimensions()
    logger.info("Data dimensions", **dimensions)

    logger.info("Creating training batches")
    train_batches = tide_data.get_batches(
        data_type="train",
        validation_split=float(configuration["validation_split"]),
        input_length=int(configuration["input_length"]),
        output_length=int(configuration["output_length"]),
        batch_size=int(configuration["batch_size"]),
    )

    logger.info("Training batches created", batch_count=len(train_batches))

    if not train_batches:
        logger.error(
            "No training batches created",
            validation_split=configuration["validation_split"],
            input_length=configuration["input_length"],
            output_length=configuration["output_length"],
            batch_size=configuration["batch_size"],
            training_data_rows=training_data.height,
        )
        message = (
            "No training batches created - check input data and configuration. "
            f"Training data has {training_data.height} rows, "
            f"input_length={configuration['input_length']}, "
            f"output_length={configuration['output_length']}, "
            f"batch_size={configuration['batch_size']}"
        )
        raise ValueError(message)

    sample_batch = train_batches[0]

    batch_size = sample_batch["past_continuous_features"].shape[0]
    logger.info("Batch size determined", batch_size=batch_size)

    past_continuous_size = (
        sample_batch["past_continuous_features"].reshape(batch_size, -1).shape[1]
    )
    past_categorical_size = (
        sample_batch["past_categorical_features"].reshape(batch_size, -1).shape[1]
    )
    future_categorical_size = (
        sample_batch["future_categorical_features"].reshape(batch_size, -1).shape[1]
    )
    static_categorical_size = (
        sample_batch["static_categorical_features"].reshape(batch_size, -1).shape[1]
    )

    input_size = cast(
        "int",
        past_continuous_size
        + past_categorical_size
        + future_categorical_size
        + static_categorical_size,
    )

    logger.info("Input size calculated", input_size=input_size)

    logger.info("Creating model")
    tide_model = Model(
        input_size=input_size,
        hidden_size=int(configuration["hidden_size"]),
        num_encoder_layers=int(configuration["num_encoder_layers"]),
        num_decoder_layers=int(configuration["num_decoder_layers"]),
        output_length=int(configuration["output_length"]),
        dropout_rate=float(configuration["dropout_rate"]),
        quantiles=configuration["quantiles"],
        huber_delta=float(configuration["huber_delta"]),
    )

    logger.info("Training started", epochs=configuration["epoch_count"])

    early_stopping_patience = configuration.get("early_stopping_patience", 25)
    losses = tide_model.train(
        train_batches=train_batches,
        epochs=int(configuration["epoch_count"]),
        learning_rate=float(configuration["learning_rate"]),
        checkpoint_directory=checkpoint_directory,
        early_stopping_patience=(
            int(early_stopping_patience)
            if early_stopping_patience is not None
            else None
        ),
    )

    for epoch_index, loss in enumerate(losses):
        log_epoch_loss(epoch=epoch_index, loss=loss)

    best_loss = min(losses) if losses else 0.0
    log_training_result(
        best_loss=best_loss,
        all_losses=losses,
        total_epochs=len(losses),
    )

    logger.info(
        "Training complete",
        final_loss=losses[-1] if losses else None,
        best_loss=best_loss if losses else None,
        all_losses=losses,
    )

    return tide_model, tide_data, losses
