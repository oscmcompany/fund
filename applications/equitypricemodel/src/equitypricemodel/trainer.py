import os
from typing import cast

import polars as pl
import structlog
from equitypricemodel.tide_data import Data
from equitypricemodel.tide_model import Model
from tinygrad import Device

logger = structlog.get_logger()

DEFAULT_CONFIGURATION = {
    "architecture": "TiDE",
    "learning_rate": 0.003,
    "epoch_count": 20,
    "validation_split": 0.8,
    "input_length": 35,
    "output_length": 7,
    "hidden_size": 64,
    "num_encoder_layers": 2,
    "num_decoder_layers": 2,
    "dropout_rate": 0.1,
    "batch_size": 256,
}


def train_model(
    training_data: pl.DataFrame,
    configuration: dict | None = None,
) -> tuple[Model, Data]:
    """Train TiDE model and return model + data processor."""
    configuration = configuration or dict(DEFAULT_CONFIGURATION)

    logger.info("configuration_loaded", **configuration)

    logger.info("initializing_data_processor")
    tide_data = Data()

    logger.info("preprocessing_training_data")
    tide_data.preprocess_and_set_data(data=training_data)

    logger.info("getting_data_dimensions")
    dimensions = tide_data.get_dimensions()
    logger.info("data_dimensions", **dimensions)

    logger.info("creating_training_batches")
    train_batches = tide_data.get_batches(
        data_type="train",
        validation_split=float(configuration["validation_split"]),
        input_length=int(configuration["input_length"]),
        output_length=int(configuration["output_length"]),
        batch_size=int(configuration["batch_size"]),
    )

    logger.info("training_batches_created", batch_count=len(train_batches))

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

    batch_size = sample_batch["encoder_continuous_features"].shape[0]
    logger.info("batch_size_determined", batch_size=batch_size)

    encoder_continuous_size = (
        sample_batch["encoder_continuous_features"].reshape(batch_size, -1).shape[1]
    )
    encoder_categorical_size = (
        sample_batch["encoder_categorical_features"].reshape(batch_size, -1).shape[1]
    )
    decoder_categorical_size = (
        sample_batch["decoder_categorical_features"].reshape(batch_size, -1).shape[1]
    )
    static_categorical_size = (
        sample_batch["static_categorical_features"].reshape(batch_size, -1).shape[1]
    )

    input_size = cast(
        "int",
        encoder_continuous_size
        + encoder_categorical_size
        + decoder_categorical_size
        + static_categorical_size,
    )

    logger.info("input_size_calculated", input_size=input_size)

    logger.info("creating_model")
    tide_model = Model(
        input_size=input_size,
        hidden_size=int(configuration["hidden_size"]),
        num_encoder_layers=int(configuration["num_encoder_layers"]),
        num_decoder_layers=int(configuration["num_decoder_layers"]),
        output_length=int(configuration["output_length"]),
        dropout_rate=float(configuration["dropout_rate"]),
        quantiles=[0.1, 0.5, 0.9],
    )

    logger.info("training_started", epochs=configuration["epoch_count"])

    losses = tide_model.train(
        train_batches=train_batches,
        epochs=int(configuration["epoch_count"]),
        learning_rate=float(configuration["learning_rate"]),
    )

    logger.info(
        "training_complete",
        final_loss=losses[-1] if losses else None,
        all_losses=losses,
    )

    return tide_model, tide_data


if __name__ == "__main__":
    # Configure structlog for CloudWatch-friendly output
    structlog.configure(
        processors=[
            structlog.stdlib.add_log_level,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.BoundLogger,
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=True,
    )

    logger.info("trainer_started", device=Device.DEFAULT)

    training_data_input_path = os.environ.get(
        "TRAINING_DATA_PATH",
        os.path.join(  # noqa: PTH118
            "/opt/ml/input/data/train",
            "filtered_tide_training_data.parquet",
        ),
    )

    model_output_path = os.environ.get("MODEL_OUTPUT_PATH", "/opt/ml/model")

    logger.info(
        "paths_configured",
        training_data_path=training_data_input_path,
        model_output_path=model_output_path,
    )

    logger.info("loading_training_data")
    training_data = pl.read_parquet(training_data_input_path)
    logger.info(
        "training_data_loaded",
        rows=training_data.height,
        columns=training_data.width,
    )

    tide_model, tide_data = train_model(training_data)

    logger.info("saving_model")
    tide_model.save(directory_path=model_output_path)

    logger.info("saving_data_processor")
    tide_data.save(directory_path=model_output_path)

    logger.info("trainer_complete")
