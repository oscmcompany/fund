import io
import os
import sys
import tarfile
import tempfile
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, cast

import boto3
import polars as pl
import structlog
from prefect import flow, task
from tools.sync_equity_bars_data import sync_equity_bars_data
from tools.sync_equity_details_data import sync_equity_details_data

from tide.notifications import send_training_notification
from tide.tasks import prepare_training_data

logger = structlog.get_logger()

if TYPE_CHECKING:
    from tide.tide_data import Data
    from tide.tide_model import Model

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
    checkpoint_directory: str | None = None,
) -> "tuple[Model, Data]":
    """Train TiDE model and return model + data processor."""
    # Defer imports to avoid loading tinygrad at module level (heavy GPU dependency)
    from tide.tide_data import Data  # noqa: PLC0415
    from tide.tide_model import Model  # noqa: PLC0415

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
        quantiles=[0.1, 0.5, 0.9],
    )

    logger.info("Training started", epochs=configuration["epoch_count"])

    losses = tide_model.train(
        train_batches=train_batches,
        epochs=int(configuration["epoch_count"]),
        learning_rate=float(configuration["learning_rate"]),
        checkpoint_directory=checkpoint_directory,
    )

    logger.info(
        "Training complete",
        final_loss=losses[-1] if losses else None,
        all_losses=losses,
    )

    return tide_model, tide_data


def get_training_date_range(lookback_days: int) -> tuple[datetime, datetime]:
    """Build a UTC date range used by sync + prepare steps."""
    end_date = datetime.now(tz=UTC).replace(hour=0, minute=0, second=0, microsecond=0)
    start_date = end_date - timedelta(days=lookback_days)
    return start_date, end_date


@task(name="sync-equity-bars", retries=2, retry_delay_seconds=30)
def sync_equity_bars(base_url: str, start_date: datetime, end_date: datetime) -> None:
    """Trigger datamanager to sync equity bars."""

    logger.info(
        "Syncing equity bars",
        base_url=base_url,
        start_date=start_date.isoformat(),
        end_date=end_date.isoformat(),
    )

    sync_equity_bars_data(
        base_url=base_url,
        date_range=(start_date, end_date),
    )


@task(name="sync-equity-details", retries=2, retry_delay_seconds=30)
def sync_equity_details(base_url: str) -> None:
    """Trigger datamanager to sync equity details."""
    logger.info("Syncing equity details", base_url=base_url)
    try:
        sync_equity_details_data(base_url=base_url)
    except RuntimeError as error:
        if "status 501" in str(error):
            logger.warning(
                "Equity details sync is not implemented, skipping",
                base_url=base_url,
            )
            return
        raise


@task(name="prepare-training-data")
def prepare_data(
    data_bucket: str,
    artifacts_bucket: str,
    start_date: datetime,
    end_date: datetime,
    output_key: str = "training/filtered_tide_training_data.parquet",
) -> str:
    """Read equity bars + categories from S3, filter, write consolidated parquet."""
    logger.info(
        "Preparing training data",
        data_bucket=data_bucket,
        artifacts_bucket=artifacts_bucket,
        start_date=start_date.isoformat(),
        end_date=end_date.isoformat(),
    )

    training_data_uri = prepare_training_data(
        data_bucket_name=data_bucket,
        model_artifacts_bucket_name=artifacts_bucket,
        start_date=start_date,
        end_date=end_date,
        output_key=output_key,
    )

    bucket_prefix = f"s3://{artifacts_bucket}/"
    if training_data_uri.startswith(bucket_prefix):
        return training_data_uri.removeprefix(bucket_prefix)

    logger.warning(
        "Prepared training data URI did not match expected bucket",
        expected_bucket=artifacts_bucket,
        training_data_uri=training_data_uri,
    )
    return output_key


@task(name="train-tide-model", timeout_seconds=3600)
def train_tide_model(
    artifacts_bucket: str,
    training_data_key: str = "training/filtered_tide_training_data.parquet",
) -> str:
    """Download training data from S3, train model, upload artifact to S3."""
    resolved_training_data_key = training_data_key
    bucket_prefix = f"s3://{artifacts_bucket}/"
    if training_data_key.startswith(bucket_prefix):
        resolved_training_data_key = training_data_key.removeprefix(bucket_prefix)

    logger.info(
        "Starting model training",
        artifacts_bucket=artifacts_bucket,
        training_data_key=resolved_training_data_key,
    )

    s3_client = boto3.client("s3")

    response = s3_client.get_object(
        Bucket=artifacts_bucket,
        Key=resolved_training_data_key,
    )
    training_data = pl.read_parquet(response["Body"].read())
    logger.info("Training data loaded", rows=training_data.height)

    with tempfile.TemporaryDirectory(prefix="checkpoints_") as checkpoint_directory:
        tide_model, tide_data = train_model(
            training_data,
            checkpoint_directory=checkpoint_directory,
        )

    timestamp = datetime.now(tz=UTC).strftime("%Y-%m-%d-%H-%M-%S-%f")[:-3]
    artifact_folder = f"artifacts/tide/{timestamp}"
    artifact_key = f"{artifact_folder}/output/model.tar.gz"

    with tempfile.TemporaryDirectory() as tmpdir:
        tide_model.save(directory_path=tmpdir)
        tide_data.save(directory_path=tmpdir)

        tar_buffer = io.BytesIO()
        with tarfile.open(fileobj=tar_buffer, mode="w:gz") as tar:
            for entry in Path(tmpdir).iterdir():
                tar.add(entry, arcname=entry.name)
        tar_bytes = tar_buffer.getvalue()

    logger.info(
        "Uploading model artifact",
        bucket=artifacts_bucket,
        key=artifact_key,
        size_bytes=len(tar_bytes),
    )

    s3_client.put_object(
        Bucket=artifacts_bucket,
        Key=artifact_key,
        Body=tar_bytes,
        ContentType="application/gzip",
    )

    logger.info(
        "Model artifact uploaded",
        artifact_path=f"s3://{artifacts_bucket}/{artifact_key}",
    )

    return f"s3://{artifacts_bucket}/{artifact_key}"


@flow(  # type: ignore[no-matching-overload]
    name="tide-training-pipeline",
    log_prints=True,
    on_completion=[send_training_notification],
    on_failure=[send_training_notification],
)
def training_pipeline(
    base_url: str,
    data_bucket: str,
    artifacts_bucket: str,
    lookback_days: int = 365,
) -> str:
    """End-to-end training pipeline."""
    if lookback_days <= 0:
        message = "lookback_days must be positive"
        raise ValueError(message)

    training_data_key = "training/filtered_tide_training_data.parquet"
    start_date, end_date = get_training_date_range(lookback_days)

    sync_equity_bars(base_url, start_date, end_date)
    sync_equity_details(base_url)
    prepared_key = prepare_data(
        data_bucket,
        artifacts_bucket,
        start_date,
        end_date,
        training_data_key,
    )
    return train_tide_model(artifacts_bucket, prepared_key)


if __name__ == "__main__":
    base_url = os.getenv("FUND_DATAMANAGER_BASE_URL", "")
    data_bucket = os.getenv("AWS_S3_DATA_BUCKET_NAME", "")
    artifacts_bucket = os.getenv("AWS_S3_MODEL_ARTIFACTS_BUCKET_NAME", "")

    try:
        lookback_days = int(os.getenv("FUND_LOOKBACK_DAYS", "365"))
    except ValueError:
        logger.exception("FUND_LOOKBACK_DAYS must be a valid integer")
        sys.exit(1)

    if lookback_days <= 0:
        logger.error("FUND_LOOKBACK_DAYS must be positive", lookback_days=lookback_days)
        sys.exit(1)

    required_vars = {
        "FUND_DATAMANAGER_BASE_URL": base_url,
        "AWS_S3_DATA_BUCKET_NAME": data_bucket,
        "AWS_S3_MODEL_ARTIFACTS_BUCKET_NAME": artifacts_bucket,
    }

    missing = [key for key, value in required_vars.items() if not value]
    if missing:
        logger.error("Missing required environment variables", missing=missing)
        sys.exit(1)

    training_pipeline(
        base_url=base_url,
        data_bucket=data_bucket,
        artifacts_bucket=artifacts_bucket,
        lookback_days=lookback_days,
    )
