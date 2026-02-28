import os
import sys
from datetime import UTC, datetime, timedelta

import structlog

from tools.flows.training_flow import training_pipeline

logger = structlog.get_logger()


def run_training_job(  # noqa: PLR0913
    base_url: str,
    data_bucket: str,
    artifacts_bucket: str,
    start_date: str,
    end_date: str,
    lookback_days: int = 365,
) -> str:
    """Run the TiDE training pipeline via Prefect."""
    logger.info(
        "Starting training pipeline",
        base_url=base_url,
        data_bucket=data_bucket,
        artifacts_bucket=artifacts_bucket,
        start_date=start_date,
        end_date=end_date,
        lookback_days=lookback_days,
    )

    artifact_path = training_pipeline(
        base_url=base_url,
        data_bucket=data_bucket,
        artifacts_bucket=artifacts_bucket,
        start_date=start_date,
        end_date=end_date,
        lookback_days=lookback_days,
    )

    logger.info("Training pipeline complete", artifact_path=artifact_path)

    return artifact_path


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

    end_date_dt = datetime.now(tz=UTC).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    start_date_dt = end_date_dt - timedelta(days=lookback_days)

    try:
        run_training_job(
            base_url=base_url,
            data_bucket=data_bucket,
            artifacts_bucket=artifacts_bucket,
            start_date=start_date_dt.strftime("%Y-%m-%d"),
            end_date=end_date_dt.strftime("%Y-%m-%d"),
            lookback_days=lookback_days,
        )
    except Exception as e:
        logger.exception("Training pipeline failed", error=str(e))
        sys.exit(1)
