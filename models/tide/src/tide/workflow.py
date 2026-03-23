import io
import os
import sys
import tarfile
import tempfile
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import cast

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
from tide.tasks import prepare_training_data  # noqa: E402
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
    output_key: str = "training/filtered_tide_training_data.parquet",
) -> str:
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

    logger.info(
        "Preparing training data",
        data_bucket=data_block.bucket_name,
        artifacts_bucket=artifact_block.bucket_name,
        start_date=start_date.isoformat(),
        end_date=end_date.isoformat(),
    )

    training_data_uri = prepare_training_data(
        s3_client=s3_client,
        data_bucket_name=data_block.bucket_name,
        model_artifacts_bucket_name=artifact_block.bucket_name,
        start_date=start_date,
        end_date=end_date,
        output_key=output_key,
    )

    bucket_prefix = f"s3://{artifact_block.bucket_name}/"
    if training_data_uri.startswith(bucket_prefix):
        return training_data_uri.removeprefix(bucket_prefix)

    logger.warning(
        "Prepared training data URI did not match expected bucket",
        expected_bucket=artifact_block.bucket_name,
        training_data_uri=training_data_uri,
    )
    return output_key


@task(name="train-tide-model", timeout_seconds=3600)
def train_tide_model(
    training_data_key: str = "training/filtered_tide_training_data.parquet",
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

        timestamp = datetime.now(tz=UTC).strftime("%Y-%m-%d-%H-%M-%S-%f")[:-3]
        artifact_folder = f"artifacts/tide/{timestamp}"
        artifact_key = f"{artifact_folder}/output/model.tar.gz"

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

        end_run()
    except Exception:
        end_run(status="FAILED")
        raise

    return f"s3://{artifacts_bucket}/{artifact_key}"


@flow(  # type: ignore[no-matching-overload]
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

    training_data_key = "training/filtered_tide_training_data.parquet"
    start_date, end_date = get_training_date_range(lookback_days)

    prepared_key = prepare_data(
        start_date,
        end_date,
        training_data_key,
    )
    return train_tide_model(prepared_key)


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
