from datetime import datetime

import numpy as np
import polars as pl
import scipy.optimize
import structlog

from .enums import PositionAction, PositionSide
from .exceptions import InsufficientPairsError

logger = structlog.get_logger()

REQUIRED_PAIRS = 10
Z_SCORE_HOLD_THRESHOLD = 0.5
Z_SCORE_STOP_LOSS = 4.0
BETA_WEIGHT_LOWER_BOUND = 0.5
BETA_WEIGHT_UPPER_BOUND = 2.0


def _apply_beta_neutral_weights(
    pairs: pl.DataFrame,
    market_betas: pl.DataFrame,
    volatility_parity_weights: np.ndarray,
) -> np.ndarray:
    beta_lookup = {
        row["ticker"]: row["market_beta"] for row in market_betas.iter_rows(named=True)
    }

    pair_net_betas = np.array(
        [
            beta_lookup.get(row["long_ticker"], 0.0)
            - beta_lookup.get(row["short_ticker"], 0.0)
            for row in pairs.iter_rows(named=True)
        ]
    )

    def objective(weights: np.ndarray) -> float:
        total = float(np.sum(weights))
        net_beta = float(np.dot(weights, pair_net_betas) / total)
        return net_beta**2

    bounds = [
        (BETA_WEIGHT_LOWER_BOUND * weight, BETA_WEIGHT_UPPER_BOUND * weight)
        for weight in volatility_parity_weights
    ]
    target_total = float(np.sum(volatility_parity_weights))
    constraints = [{"type": "eq", "fun": lambda w: float(np.sum(w)) - target_total}]

    result = scipy.optimize.minimize(
        objective,
        x0=volatility_parity_weights.copy(),
        method="SLSQP",
        bounds=bounds,
        constraints=constraints,
    )

    if result.success:
        return np.array(result.x)

    logger.warning("Beta-neutral optimizer did not converge, using vol-parity weights")
    return volatility_parity_weights


def size_pairs_with_volatility_parity(
    candidate_pairs: pl.DataFrame,
    maximum_capital: float,
    current_timestamp: datetime,
    market_betas: pl.DataFrame,
    exposure_scale: float = 1.0,
) -> pl.DataFrame:
    if candidate_pairs.height < REQUIRED_PAIRS:
        message = (
            f"Only {candidate_pairs.height} pairs available, "
            f"need at least {REQUIRED_PAIRS}."
        )
        raise InsufficientPairsError(message)

    pairs = (
        candidate_pairs.head(REQUIRED_PAIRS)
        .with_columns(
            (
                (
                    pl.col("long_realized_volatility")
                    + pl.col("short_realized_volatility")
                )
                / 2.0
            ).alias("pair_volatility")
        )
        .with_columns(
            (1.0 / pl.col("pair_volatility").clip(lower_bound=1e-8)).alias(
                "inverse_volatility_weight"
            )
        )
    )

    total_weight = pairs["inverse_volatility_weight"].sum()
    volatility_parity_weights = (
        pairs["inverse_volatility_weight"] / total_weight
    ).to_numpy()

    adjusted_weights = _apply_beta_neutral_weights(
        pairs, market_betas, volatility_parity_weights
    )
    if np.isclose(adjusted_weights.sum(), 0.0):
        adjusted_weights = volatility_parity_weights / volatility_parity_weights.sum()
    else:
        adjusted_weights = adjusted_weights / adjusted_weights.sum()

    dollar_amounts = adjusted_weights * (maximum_capital / 2.0) * exposure_scale
    pairs = pairs.with_columns(pl.Series("dollar_amount", dollar_amounts))

    logger.info(
        "Sized pairs with volatility parity",
        pair_count=pairs.height,
        total_capital=maximum_capital,
        exposure_scale=exposure_scale,
    )

    timestamp_val = float(current_timestamp.timestamp())
    long_positions = pairs.select(
        pl.col("long_ticker").alias("ticker"),
        pl.lit(timestamp_val).cast(pl.Float64).alias("timestamp"),
        pl.lit(PositionSide.LONG.value).alias("side"),
        pl.col("dollar_amount"),
        pl.lit(PositionAction.OPEN_POSITION.value).alias("action"),
        pl.col("pair_id"),
    )

    short_positions = pairs.select(
        pl.col("short_ticker").alias("ticker"),
        pl.lit(timestamp_val).cast(pl.Float64).alias("timestamp"),
        pl.lit(PositionSide.SHORT.value).alias("side"),
        pl.col("dollar_amount"),
        pl.lit(PositionAction.OPEN_POSITION.value).alias("action"),
        pl.col("pair_id"),
    )

    return pl.concat([long_positions, short_positions]).sort(["ticker", "side"])
