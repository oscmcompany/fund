import os
import sys

import structlog
from prefect.schedules import Schedule

from tide.workflow import training_pipeline

logger = structlog.get_logger()


def deploy_training_flow(
    lookback_days: int = 365,
) -> None:
    """Register the training pipeline deployment with the Prefect server."""
    logger.info(
        "Deploying training pipeline",
        lookback_days=lookback_days,
    )

    training_pipeline.deploy(
        name="tide-trainer-remote",
        work_pool_name="fund-models-remote",
        schedule=Schedule(cron="0 22 * * 1-5", timezone="America/New_York"),
        parameters={
            "lookback_days": lookback_days,
        },
        tags=["training", "daily"],
        build=False,
        push=False,
    )

    logger.info("Training pipeline deployed")


if __name__ == "__main__":
    try:
        lookback_days = int(os.getenv("FUND_LOOKBACK_DAYS", "365"))
    except ValueError:
        logger.exception("FUND_LOOKBACK_DAYS must be a valid integer")
        sys.exit(1)

    if lookback_days <= 0:
        logger.error("FUND_LOOKBACK_DAYS must be positive", lookback_days=lookback_days)
        sys.exit(1)

    deploy_training_flow(
        lookback_days=lookback_days,
    )
