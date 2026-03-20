"""MLflow tracking integration for TiDE model training."""

from __future__ import annotations

import os
import socket
from typing import TYPE_CHECKING

import structlog

if TYPE_CHECKING:
    from mlflow import ActiveRun

logger = structlog.get_logger()

DEFAULT_EXPERIMENT_NAME = "tide"


def get_environment_tag() -> str:
    """Return the current environment tag for MLflow runs."""
    return os.getenv("ENVIRONMENT", "development")


def get_host_tag() -> str:
    """Return hostname for identifying where training ran."""
    return socket.gethostname()


def is_tracking_enabled() -> bool:
    """Check if MLflow tracking is configured."""
    return bool(os.getenv("MLFLOW_TRACKING_URI"))


def start_run(
    configuration: dict,
    run_name: str | None = None,
    tags: dict[str, str] | None = None,
    experiment_name: str = DEFAULT_EXPERIMENT_NAME,
) -> ActiveRun | None:
    """Start an MLflow run and log parameters.

    Returns the active run, or None if tracking is not configured.
    The caller is responsible for calling end_run() when done.
    """
    if not is_tracking_enabled():
        logger.info("MLflow tracking not configured, skipping")
        return None

    import mlflow  # noqa: PLC0415

    mlflow.set_experiment(experiment_name)

    run_tags = {
        "environment": get_environment_tag(),
        "host": get_host_tag(),
    }
    if tags:
        run_tags.update(tags)

    run = mlflow.start_run(run_name=run_name, tags=run_tags)

    mlflow.log_params(configuration)

    logger.info(
        "MLflow run started",
        run_id=run.info.run_id,
        experiment=experiment_name,
    )

    return run


def log_epoch_loss(epoch: int, loss: float) -> None:
    """Log a single epoch's loss metric."""
    if not is_tracking_enabled():
        return

    import mlflow  # noqa: PLC0415

    mlflow.log_metric("quantile_loss", loss, step=epoch)


def log_training_result(
    best_loss: float,
    all_losses: list[float],
    total_epochs: int,
) -> None:
    """Log final training metrics."""
    if not is_tracking_enabled():
        return

    import mlflow  # noqa: PLC0415

    mlflow.log_metric("best_quantile_loss", best_loss)
    mlflow.log_metric("final_quantile_loss", all_losses[-1] if all_losses else 0.0)
    mlflow.log_metric("total_epochs", total_epochs)


def log_model_artifact(directory_path: str) -> None:
    """Log model files as MLflow artifacts."""
    if not is_tracking_enabled():
        return

    import mlflow  # noqa: PLC0415

    mlflow.log_artifacts(directory_path, artifact_path="model")


def end_run(status: str = "FINISHED") -> None:
    """End the current MLflow run."""
    if not is_tracking_enabled():
        return

    import mlflow  # noqa: PLC0415

    mlflow.end_run(status=status)
