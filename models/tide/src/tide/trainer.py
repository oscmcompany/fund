from typing import Any, cast

import polars as pl
import structlog

from tide.data import Data
from tide.model import Model
from tide.tracking import log_epoch_loss, log_training_result

logger = structlog.get_logger()

DEFAULT_CONFIGURATION: dict[str, Any] = {
    "architecture": "TiDE",
    "learning_rate": 0.0005,
    "epoch_count": 20,
    "early_stopping_patience": 10,
    "validation_split": 0.8,
    "input_length": 35,
    "output_length": 5,
    "hidden_size": 64,
    "num_encoder_layers": 3,
    "num_decoder_layers": 2,
    "dropout_rate": 0.1,
    "batch_size": 256,
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

    logger.info("Creating training dataset")
    train_dataset = tide_data.get_dataset(
        data_type="train",
        validation_split=float(configuration["validation_split"]),
        input_length=int(configuration["input_length"]),
        output_length=int(configuration["output_length"]),
    )

    logger.info("Creating validation dataset")
    try:
        validation_dataset = tide_data.get_dataset(
            data_type="validate",
            validation_split=float(configuration["validation_split"]),
            input_length=int(configuration["input_length"]),
            output_length=int(configuration["output_length"]),
        )
    except ValueError:
        logger.warning(
            "Validation set too small for windowing; disabling validation early stopping"  # noqa: E501
        )
        validation_dataset = None

    logger.info("Training dataset created", sample_count=len(train_dataset))

    if len(train_dataset) == 0:
        logger.error(
            "No training samples created",
            validation_split=configuration["validation_split"],
            input_length=configuration["input_length"],
            output_length=configuration["output_length"],
            training_data_rows=training_data.height,
        )
        message = (
            "No training samples created - check input data and configuration. "
            f"Training data has {training_data.height} rows, "
            f"input_length={configuration['input_length']}, "
            f"output_length={configuration['output_length']}"
        )
        raise ValueError(message)

    # Compute input_size from numpy array shapes: [N, time_steps, features]
    past_continuous_size = (
        train_dataset.past_continuous.shape[1] * train_dataset.past_continuous.shape[2]
    )
    past_categorical_size = (
        train_dataset.past_categorical.shape[1]
        * train_dataset.past_categorical.shape[2]
    )
    future_categorical_size = (
        train_dataset.future_categorical.shape[1]
        * train_dataset.future_categorical.shape[2]
    )
    static_categorical_size = (
        train_dataset.static_categorical.shape[1]
        * train_dataset.static_categorical.shape[2]
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
        dataset=train_dataset,
        batch_size=int(configuration["batch_size"]),
        epochs=int(configuration["epoch_count"]),
        learning_rate=float(configuration["learning_rate"]),
        validation_dataset=validation_dataset,
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
