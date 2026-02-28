import io
import os
import sys
import tarfile
import tempfile
from datetime import UTC, datetime, timedelta
from pathlib import Path

import boto3
import polars as pl
import structlog
from prefect import flow, task

from tools.flows.notifications import send_training_notification
from tools.prepare_training_data import prepare_training_data
from tools.sync_equity_bars_data import sync_equity_bars_data
from tools.sync_equity_details_data import sync_equity_details_data

logger = structlog.get_logger()


@task(name="sync-equity-bars", retries=2, retry_delay_seconds=30)
def sync_equity_bars(base_url: str, lookback_days: int = 365) -> None:
    """Trigger datamanager to sync equity bars."""
    end_date = datetime.now(tz=UTC).replace(hour=0, minute=0, second=0, microsecond=0)
    start_date = end_date - timedelta(days=lookback_days)

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
    sync_equity_details_data(base_url=base_url)


@task(name="prepare-training-data")
def prepare_data(
    data_bucket: str,
    artifacts_bucket: str,
    lookback_days: int = 365,
    output_key: str = "training/filtered_tide_training_data.parquet",
) -> str:
    """Read equity bars + categories from S3, filter, write consolidated parquet."""
    logger.info(
        "Preparing training data",
        data_bucket=data_bucket,
        artifacts_bucket=artifacts_bucket,
        lookback_days=lookback_days,
    )

    end_date = datetime.now(tz=UTC).replace(hour=0, minute=0, second=0, microsecond=0)
    start_date = end_date - timedelta(days=lookback_days)

    return prepare_training_data(
        data_bucket_name=data_bucket,
        model_artifacts_bucket_name=artifacts_bucket,
        start_date=start_date,
        end_date=end_date,
        output_key=output_key,
    )


@task(name="train-tide-model", timeout_seconds=3600)
def train_tide_model(
    artifacts_bucket: str,
    training_data_key: str = "training/filtered_tide_training_data.parquet",
) -> str:
    """Download training data from S3, train model, upload artifact to S3."""
    # Defer import to avoid importing tinygrad at module level (heavy GPU dependency)
    from equitypricemodel.trainer import train_model  # noqa: PLC0415

    logger.info(
        "Starting model training",
        artifacts_bucket=artifacts_bucket,
        training_data_key=training_data_key,
    )

    s3_client = boto3.client("s3")

    response = s3_client.get_object(Bucket=artifacts_bucket, Key=training_data_key)
    training_data = pl.read_parquet(response["Body"].read())
    logger.info("Training data loaded", rows=training_data.height)

    tide_model, tide_data = train_model(training_data)

    timestamp = datetime.now(tz=UTC).strftime("%Y-%m-%d-%H-%M-%S-%f")[:-3]
    artifact_folder = f"artifacts/equitypricemodel-trainer-{timestamp}"
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
    training_data_key = "training/filtered_tide_training_data.parquet"
    sync_equity_bars(base_url, lookback_days)
    sync_equity_details(base_url)
    prepare_data(data_bucket, artifacts_bucket, lookback_days, training_data_key)
    return train_tide_model(artifacts_bucket, training_data_key)


if __name__ == "__main__":
    base_url = os.getenv("FUND_DATAMANAGER_BASE_URL", "")
    data_bucket = os.getenv("AWS_S3_DATA_BUCKET_NAME", "")
    artifacts_bucket = os.getenv("AWS_S3_MODEL_ARTIFACTS_BUCKET_NAME", "")
    lookback_days = int(os.getenv("LOOKBACK_DAYS", "365"))

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
