import os
import sys

import structlog

from tools.flows.training_flow import training_pipeline

logger = structlog.get_logger()


def run_training_job(
    base_url: str,
    data_bucket: str,
    artifacts_bucket: str,
    lookback_days: int = 365,
) -> str:
    """Run the TiDE training pipeline via Prefect."""
    if lookback_days <= 0:
        message = "lookback_days must be positive"
        raise ValueError(message)

    logger.info(
        "Starting training pipeline",
        base_url=base_url,
        data_bucket=data_bucket,
        artifacts_bucket=artifacts_bucket,
        lookback_days=lookback_days,
    )

    artifact_path = training_pipeline(
        base_url=base_url,
        data_bucket=data_bucket,
        artifacts_bucket=artifacts_bucket,
        lookback_days=lookback_days,
    )

    logger.info("Training pipeline complete", artifact_path=artifact_path)

    return artifact_path


if __name__ == "__main__":
    base_url = os.getenv("FUND_DATAMANAGER_BASE_URL", "")
    data_bucket = os.getenv("AWS_S3_DATA_BUCKET_NAME", "")
    artifacts_bucket = os.getenv("AWS_S3_MODEL_ARTIFACTS_BUCKET_NAME", "")

    required_vars = {
        "FUND_DATAMANAGER_BASE_URL": base_url,
        "AWS_S3_DATA_BUCKET_NAME": data_bucket,
        "AWS_S3_MODEL_ARTIFACTS_BUCKET_NAME": artifacts_bucket,
    }

    missing = [key for key, value in required_vars.items() if not value]
    if missing:
        logger.error("Missing required environment variables", missing=missing)
        sys.exit(1)

    try:
        lookback_days = int(os.getenv("LOOKBACK_DAYS", "365"))
    except ValueError:
        logger.exception("LOOKBACK_DAYS must be a valid integer")
        sys.exit(1)

    if lookback_days <= 0:
        logger.error("LOOKBACK_DAYS must be positive", lookback_days=lookback_days)
        sys.exit(1)

    try:
        run_training_job(
            base_url=base_url,
            data_bucket=data_bucket,
            artifacts_bucket=artifacts_bucket,
            lookback_days=lookback_days,
        )
    except Exception as e:
        logger.exception("Training pipeline failed", error=str(e))
        sys.exit(1)
