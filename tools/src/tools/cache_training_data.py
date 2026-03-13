"""Download training data from MinIO to local disk for fast local training.

Usage:
    uv run --package tools python -m tools.cache_training_data

Requires MinIO to be running (docker compose up -d minio minio-init).
Saves to results/equitypricemodel/training_data.parquet.
"""

import os
import sys

import boto3
import structlog

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

logger = structlog.get_logger()

OUTPUT_PATH = "results/equitypricemodel/training_data.parquet"
BUCKET = "fund-model-artifacts"
KEY = "training/filtered_tide_training_data.parquet"


def main() -> None:
    endpoint_url = os.getenv("AWS_ENDPOINT_URL", "http://localhost:9000")
    s3_client = boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "minioadmin"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin"),
    )

    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)  # noqa: PTH103, PTH120

    # First ensure training data exists in MinIO by running prepare step
    try:
        s3_client.head_object(Bucket=BUCKET, Key=KEY)
        logger.info("Training data found in MinIO", bucket=BUCKET, key=KEY)
    except s3_client.exceptions.ClientError:
        logger.info("Training data not in MinIO, running prepare step")
        from tools.prepare_training_data import prepare_training_data
        from datetime import UTC, datetime, timedelta

        end_date = datetime.now(tz=UTC)
        start_date = end_date - timedelta(days=365)
        prepare_training_data(
            data_bucket_name="fund-data",
            model_artifacts_bucket_name=BUCKET,
            start_date=start_date,
            end_date=end_date,
        )

    logger.info("Downloading training data", output=OUTPUT_PATH)
    s3_client.download_file(BUCKET, KEY, OUTPUT_PATH)
    file_size = os.path.getsize(OUTPUT_PATH)  # noqa: PTH202
    logger.info("Training data cached", path=OUTPUT_PATH, size_bytes=file_size)


if __name__ == "__main__":
    main()
