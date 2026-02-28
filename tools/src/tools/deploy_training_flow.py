import os
import sys

import structlog
from prefect.flows import EntrypointType

from tools.flows.training_flow import training_pipeline

logger = structlog.get_logger()


def deploy_training_flow(
    base_url: str,
    data_bucket: str,
    artifacts_bucket: str,
    lookback_days: int = 365,
) -> None:
    """Register the training pipeline deployment with the Prefect server."""
    logger.info(
        "Deploying training pipeline",
        base_url=base_url,
        data_bucket=data_bucket,
        artifacts_bucket=artifacts_bucket,
        lookback_days=lookback_days,
    )

    training_pipeline.deploy(
        name="daily-training",
        work_pool_name="training-pool",
        cron="0 22 * * *",
        parameters={
            "base_url": base_url,
            "data_bucket": data_bucket,
            "artifacts_bucket": artifacts_bucket,
            "lookback_days": lookback_days,
        },
        tags=["training", "daily"],
        entrypoint_type=EntrypointType.MODULE_PATH,
        build=False,
        push=False,
    )

    logger.info("Training pipeline deployed")


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

    deploy_training_flow(
        base_url=base_url,
        data_bucket=data_bucket,
        artifacts_bucket=artifacts_bucket,
        lookback_days=lookback_days,
    )
