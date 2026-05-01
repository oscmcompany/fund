from dataclasses import dataclass
from typing import cast

import numpy as np
import polars as pl
import structlog
from tinygrad.tensor import Tensor

from tide.data import TrainingDataset
from tide.model import Model

logger = structlog.get_logger()


@dataclass
class DriftResult:
    status: str  # "insufficient_history" | "no_drift" | "drift_detected"
    message: str
    current_crps: float
    baseline_crps: float | None


def compute_crps(predictions: pl.DataFrame, actuals: pl.DataFrame) -> float:
    """Compute CRPS via pinball loss across quantile_10, quantile_50, quantile_90.

    predictions schema: quantile_10, quantile_50, quantile_90
    actuals schema: daily_return
    Both DataFrames must be aligned by row index.
    """
    actual_values = actuals["daily_return"]

    quantile_pairs = [
        (0.1, predictions["quantile_10"]),
        (0.5, predictions["quantile_50"]),
        (0.9, predictions["quantile_90"]),
    ]

    combined = predictions.with_columns(actual_values.alias("daily_return"))

    total_loss = pl.lit(0.0)
    for quantile, predicted_column in quantile_pairs:
        column_name = predicted_column.name
        error = pl.col("daily_return") - pl.col(column_name)
        pinball = (
            pl.when(error >= 0)
            .then(quantile * error)
            .otherwise((quantile - 1.0) * error)
        )
        total_loss = total_loss + pinball

    result = combined.select(total_loss.alias("row_loss"))
    return cast("float", result["row_loss"].mean())


def compute_directional_accuracy(
    predictions: pl.DataFrame, actuals: pl.DataFrame
) -> float:
    """Compute fraction of rows where sign(quantile_50) == sign(daily_return).

    predictions schema: quantile_50 (at minimum)
    actuals schema: daily_return
    Both DataFrames must be aligned by row index.
    """
    combined = predictions.with_columns(actuals["daily_return"].alias("daily_return"))

    matches = combined.select(
        ((pl.col("quantile_50") >= 0) == (pl.col("daily_return") >= 0)).alias(
            "direction_match"
        )
    )

    return cast("float", matches["direction_match"].mean())


def compute_quantile_coverage(
    predictions: pl.DataFrame, actuals: pl.DataFrame
) -> float:
    """Compute fraction where quantile_10 <= daily_return <= quantile_90.

    Well-calibrated model should produce ~0.80 coverage.

    predictions schema: quantile_10, quantile_90 (at minimum)
    actuals schema: daily_return
    Both DataFrames must be aligned by row index.
    """
    combined = predictions.with_columns(actuals["daily_return"].alias("daily_return"))

    coverage = combined.select(
        (
            (pl.col("daily_return") >= pl.col("quantile_10"))
            & (pl.col("daily_return") <= pl.col("quantile_90"))
        ).alias("within_interval")
    )

    return cast("float", coverage["within_interval"].mean())


def check_drift(
    current_metrics: dict[str, float],
    prior_evaluations: list[dict[str, float]],
    minimum_runs: int = 3,
    degradation_threshold: float = 0.20,
) -> DriftResult:
    """Check whether the current CRPS has degraded relative to the historical baseline.

    Returns a DriftResult with status "insufficient_history", "no_drift", or
    "drift_detected".
    """
    current_crps = current_metrics["crps"]

    if len(prior_evaluations) < minimum_runs:
        message = (
            f"Insufficient evaluation history: {len(prior_evaluations)} run(s) "
            f"recorded, {minimum_runs} required for baseline."
        )
        logger.info(
            "Insufficient evaluation history for drift check",
            prior_runs=len(prior_evaluations),
            minimum_runs=minimum_runs,
        )
        return DriftResult(
            status="insufficient_history",
            message=message,
            current_crps=current_crps,
            baseline_crps=None,
        )

    baseline_crps = float(
        np.mean([evaluation["crps"] for evaluation in prior_evaluations])
    )
    degradation_limit = max(baseline_crps, 1e-8) * (1.0 + degradation_threshold)

    if current_crps > degradation_limit:
        message = (
            f"Drift detected: current CRPS {current_crps:.6f} exceeds baseline "
            f"{baseline_crps:.6f} by more than {degradation_threshold * 100:.0f}%."
        )
        logger.warning(
            "Model drift detected",
            current_crps=current_crps,
            baseline_crps=baseline_crps,
            degradation_threshold=degradation_threshold,
        )
        return DriftResult(
            status="drift_detected",
            message=message,
            current_crps=current_crps,
            baseline_crps=baseline_crps,
        )

    message = (
        f"No drift detected: current CRPS {current_crps:.6f} is within "
        f"{degradation_threshold * 100:.0f}% of baseline {baseline_crps:.6f}."
    )
    return DriftResult(
        status="no_drift",
        message=message,
        current_crps=current_crps,
        baseline_crps=baseline_crps,
    )


def evaluate(
    model: Model,
    validation_dataset: TrainingDataset,
) -> dict[str, float]:
    """Run the model on the validation dataset and return evaluation metrics.

    Metrics returned: crps, directional_accuracy, quantile_coverage.
    All computations are performed in scaled space so that CRPS values are
    comparable across runs trained on the same scaler.
    """
    if len(validation_dataset) == 0:
        logger.warning("Empty validation dataset; returning zero metrics")
        return {"crps": 0.0, "directional_accuracy": 0.0, "quantile_coverage": 0.0}

    if validation_dataset.targets is None:
        message = "Validation dataset must include targets for evaluation"
        raise ValueError(message)

    previous_training = Tensor.training
    Tensor.training = False

    try:
        inputs = {
            "past_continuous_features": Tensor(validation_dataset.past_continuous),
            "past_categorical_features": Tensor(validation_dataset.past_categorical),
            "future_categorical_features": Tensor(
                validation_dataset.future_categorical
            ),
            "static_categorical_features": Tensor(
                validation_dataset.static_categorical
            ),
        }

        raw_predictions = model.predict(inputs)
        predictions_array = cast("np.ndarray", raw_predictions.numpy())
    finally:
        Tensor.training = previous_training

    # predictions_array shape: [N, output_length, 3]
    # targets shape: [N, output_length, 1]
    samples_count, output_length, _ = predictions_array.shape
    total_steps = samples_count * output_length

    predictions_flat = predictions_array.reshape(total_steps, 3)
    targets_flat = validation_dataset.targets.reshape(total_steps)

    predictions_dataframe = pl.DataFrame(
        {
            "quantile_10": predictions_flat[:, 0].tolist(),
            "quantile_50": predictions_flat[:, 1].tolist(),
            "quantile_90": predictions_flat[:, 2].tolist(),
        }
    )

    actuals_dataframe = pl.DataFrame({"daily_return": targets_flat.tolist()})

    crps = compute_crps(predictions_dataframe, actuals_dataframe)
    directional_accuracy = compute_directional_accuracy(
        predictions_dataframe, actuals_dataframe
    )
    quantile_coverage = compute_quantile_coverage(
        predictions_dataframe, actuals_dataframe
    )

    logger.info(
        "Evaluation complete",
        crps=crps,
        directional_accuracy=directional_accuracy,
        quantile_coverage=quantile_coverage,
        total_steps=total_steps,
    )

    return {
        "crps": crps,
        "directional_accuracy": directional_accuracy,
        "quantile_coverage": quantile_coverage,
    }
