import io
import json
import os
import sys
import tarfile
import tempfile
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, cast

# Required for Prefect's ECS managed runner: after git-cloning the repo,
# models/tide/src/ is not on sys.path, so tide.* imports would fail.
_tide_src = os.path.join(os.path.dirname(__file__), "..")  # noqa: PTH118, PTH120
if _tide_src not in sys.path:
    sys.path.insert(0, _tide_src)

import polars as pl  # noqa: E402
import structlog  # noqa: E402
from prefect import flow, task  # noqa: E402
from prefect_aws.s3 import S3Bucket  # noqa: E402

from tide.notifications import send_training_notification  # noqa: E402
from tide.tasks import (  # noqa: E402
    MINIMUM_CLOSE_PRICE,
    MINIMUM_VOLUME,
    prepare_training_data,
)
from tide.tracking import end_run, log_model_artifact, start_run  # noqa: E402

logger = structlog.get_logger()

DATA_BLOCK_NAME = "data-bucket"
ARTIFACT_BLOCK_NAME = "artifact-bucket"


def get_training_date_range(lookback_days: int) -> tuple[datetime, datetime]:
    """Build a UTC date range used by training data preparation."""
    end_date = datetime.now(tz=UTC).replace(hour=0, minute=0, second=0, microsecond=0)
    start_date = end_date - timedelta(days=lookback_days)
    return start_date, end_date


@task(name="prepare-training-data")
def prepare_data(
    start_date: datetime,
    end_date: datetime,
    artifact_timestamp: str,
) -> tuple[str, dict[str, int]]:
    """Read equity bars + categories from S3, filter, write consolidated parquet."""
    try:
        data_block = cast("S3Bucket", S3Bucket.load(DATA_BLOCK_NAME))
        artifact_block = cast("S3Bucket", S3Bucket.load(ARTIFACT_BLOCK_NAME))
    except ValueError as err:
        message = (
            f"Prefect S3Bucket blocks '{DATA_BLOCK_NAME}' and '{ARTIFACT_BLOCK_NAME}' "
            "not found. Create them in Prefect Cloud or run 'prefect block register'. "
            "Check that credentials are configured on each block."
        )
        raise ValueError(message) from err
    s3_client = data_block.credentials.get_boto3_session().client("s3")

    output_key = f"data/tide/{artifact_timestamp}/filtered_data.parquet"

    logger.info(
        "Preparing training data",
        data_bucket=data_block.bucket_name,
        artifacts_bucket=artifact_block.bucket_name,
        start_date=start_date.isoformat(),
        end_date=end_date.isoformat(),
        output_key=output_key,
    )

    training_data_uri, stage_counts = prepare_training_data(
        s3_client=s3_client,
        data_bucket_name=data_block.bucket_name,
        model_artifacts_bucket_name=artifact_block.bucket_name,
        start_date=start_date,
        end_date=end_date,
        output_key=output_key,
    )

    bucket_prefix = f"s3://{artifact_block.bucket_name}/"
    if training_data_uri.startswith(bucket_prefix):
        return training_data_uri.removeprefix(bucket_prefix), stage_counts

    logger.warning(
        "Prepared training data URI did not match expected bucket",
        expected_bucket=artifact_block.bucket_name,
        training_data_uri=training_data_uri,
    )
    return output_key, stage_counts


@task(name="train-tide-model", timeout_seconds=14400)
def train_tide_model(
    training_data_key: str,
    training_summary: dict[str, Any],
    artifact_timestamp: str,
) -> str:
    """Download training data from S3, train model, upload artifact to S3."""
    from tide.trainer import DEFAULT_CONFIGURATION, train_model  # noqa: PLC0415

    try:
        artifact_block = cast("S3Bucket", S3Bucket.load(ARTIFACT_BLOCK_NAME))
    except ValueError as err:
        message = (
            f"Prefect S3Bucket block '{ARTIFACT_BLOCK_NAME}' not found. "
            "Create it in Prefect Cloud or run 'prefect block register'. "
            "Check that credentials are configured on the block."
        )
        raise ValueError(message) from err
    s3_client = artifact_block.credentials.get_boto3_session().client("s3")
    artifacts_bucket = artifact_block.bucket_name

    resolved_training_data_key = training_data_key
    bucket_prefix = f"s3://{artifacts_bucket}/"
    if training_data_key.startswith(bucket_prefix):
        resolved_training_data_key = training_data_key.removeprefix(bucket_prefix)

    logger.info(
        "Starting model training",
        artifacts_bucket=artifacts_bucket,
        training_data_key=resolved_training_data_key,
    )

    response = s3_client.get_object(
        Bucket=artifacts_bucket,
        Key=resolved_training_data_key,
    )
    training_data = pl.read_parquet(response["Body"].read())
    logger.info("Training data loaded", rows=training_data.height)

    start_run(
        configuration=DEFAULT_CONFIGURATION,
        tags={"source": "prefect", "task": "train-tide-model"},
    )

    try:
        with tempfile.TemporaryDirectory(prefix="checkpoints_") as checkpoint_directory:
            tide_model, tide_data, _losses = train_model(
                training_data,
                checkpoint_directory=checkpoint_directory,
            )

        artifact_folder = f"artifacts/tide/{artifact_timestamp}"
        artifact_key = f"{artifact_folder}/output/model.tar.gz"
        metadata_key = f"{artifact_folder}/run_metadata.json"

        with tempfile.TemporaryDirectory() as tmpdir:
            tide_model.save(directory_path=tmpdir)
            tide_data.save(directory_path=tmpdir)

            log_model_artifact(tmpdir)

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

        metadata_bytes = json.dumps(training_summary, indent=2).encode()
        s3_client.put_object(
            Bucket=artifacts_bucket,
            Key=metadata_key,
            Body=metadata_bytes,
            ContentType="application/json",
        )

        logger.info(
            "Run metadata uploaded",
            artifact_path=f"s3://{artifacts_bucket}/{metadata_key}",
        )

        end_run()
    except Exception:
        end_run(status="FAILED")
        raise

    return f"s3://{artifacts_bucket}/{artifact_key}"


@flow(  # type: ignore
    name="tide-training-pipeline",
    log_prints=True,
    on_completion=[send_training_notification],
    on_failure=[send_training_notification],
)
def training_pipeline(
    lookback_days: int = 365,
) -> str:
    """Train from whatever data is already available in S3."""
    if lookback_days <= 0:
        message = "lookback_days must be positive"
        raise ValueError(message)

    artifact_timestamp = datetime.now(tz=UTC).strftime("%Y-%m-%d-%H-%M-%S-%f")[:-3]
    start_date, end_date = get_training_date_range(lookback_days)

    training_key, stage_counts = prepare_data(start_date, end_date, artifact_timestamp)

    training_summary = {
        "artifact_timestamp": artifact_timestamp,
        "training_data_key": training_key,
        "start_date": start_date.date().isoformat(),
        "end_date": end_date.date().isoformat(),
        "lookback_days": lookback_days,
        "filter_thresholds": {
            "minimum_close_price": MINIMUM_CLOSE_PRICE,
            "minimum_volume": MINIMUM_VOLUME,
        },
        "stage_counts": stage_counts,
    }

    return train_tide_model(training_key, training_summary, artifact_timestamp)


if __name__ == "__main__":
    try:
        lookback_days = int(os.getenv("FUND_LOOKBACK_DAYS", "365"))
    except ValueError:
        logger.exception("FUND_LOOKBACK_DAYS must be a valid integer")
        sys.exit(1)

    if lookback_days <= 0:
        logger.error("FUND_LOOKBACK_DAYS must be positive", lookback_days=lookback_days)
        sys.exit(1)

    training_pipeline(
        lookback_days=lookback_days,
    )
