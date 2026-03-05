from datetime import datetime

import polars as pl
import structlog

from .enums import PositionAction, PositionSide
from .exceptions import InsufficientPairsError

logger = structlog.get_logger()

REQUIRED_PAIRS = 10
MINIMUM_PAIRS_REQUIRED = REQUIRED_PAIRS


def size_pairs_with_volatility_parity(
    candidate_pairs: pl.DataFrame,
    maximum_capital: float,
    current_timestamp: datetime,
) -> pl.DataFrame:
    if candidate_pairs.height < MINIMUM_PAIRS_REQUIRED:
        message = (
            f"Only {candidate_pairs.height} pairs available, "
            f"need at least {MINIMUM_PAIRS_REQUIRED}."
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
    pairs = pairs.with_columns(
        (
            (pl.col("inverse_volatility_weight") / total_weight)
            * (maximum_capital / 2.0)
        ).alias("dollar_amount")
    )

    logger.info(
        "Sized pairs with volatility parity",
        pair_count=pairs.height,
        total_capital=maximum_capital,
    )

    timestamp_val = float(current_timestamp.timestamp())
    long_positions = pairs.select(
        pl.col("long_ticker").alias("ticker"),
        pl.lit(timestamp_val).cast(pl.Float64).alias("timestamp"),
        pl.lit(PositionSide.LONG.value).alias("side"),
        pl.col("dollar_amount"),
        pl.lit(PositionAction.OPEN_POSITION.value).alias("action"),
    )

    short_positions = pairs.select(
        pl.col("short_ticker").alias("ticker"),
        pl.lit(timestamp_val).cast(pl.Float64).alias("timestamp"),
        pl.lit(PositionSide.SHORT.value).alias("side"),
        pl.col("dollar_amount"),
        pl.lit(PositionAction.OPEN_POSITION.value).alias("action"),
    )

    return pl.concat([long_positions, short_positions]).sort(["ticker", "side"])
