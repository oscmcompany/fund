"""Register Prefect S3Bucket blocks from environment variables.

Reads AWS_S3_BUCKET_NAME and AWS_REGION from the environment and creates
(or overwrites) the two blocks expected by the training workflow. Both blocks
point to the same bucket; path prefixes distinguish data from model artifacts.
"""

import os
import sys

import structlog
from prefect_aws.credentials import AwsCredentials
from prefect_aws.s3 import S3Bucket

logger = structlog.get_logger()


def register_blocks() -> None:
    bucket = os.environ.get("AWS_S3_BUCKET_NAME", "")
    region = os.environ.get("AWS_REGION", "us-east-1")

    if not bucket:
        message = "AWS_S3_BUCKET_NAME must be set"
        logger.error(message)
        sys.exit(1)

    credentials = AwsCredentials(region_name=region)

    S3Bucket(
        bucket_name=bucket,
        credentials=credentials,
    ).save("data-bucket", overwrite=True)

    S3Bucket(
        bucket_name=bucket,
        credentials=credentials,
    ).save("artifact-bucket", overwrite=True)

    logger.info(
        "Registered Prefect S3 blocks",
        bucket=bucket,
    )


if __name__ == "__main__":
    register_blocks()
