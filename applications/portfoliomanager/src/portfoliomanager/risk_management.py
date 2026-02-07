import os
from datetime import datetime

import polars as pl
import structlog

from .enums import PositionAction, PositionSide
from .exceptions import InsufficientPredictionsError

logger = structlog.get_logger()

UNCERTAINTY_THRESHOLD = float(os.getenv("OSCM_UNCERTAINTY_THRESHOLD", "0.1"))
REQUIRED_PORTFOLIO_SIZE = 20  # 10 long + 10 short


def add_predictions_zscore_ranked_columns(
    current_predictions: pl.DataFrame,
) -> pl.DataFrame:
    current_predictions = current_predictions.clone()

    quantile_50_mean = current_predictions.select(pl.col("quantile_50").mean()).item()
    quantile_50_standard_deviation = (
        current_predictions.select(pl.col("quantile_50").std()).item() or 1e-8
    )

    z_score_return = (
        pl.col("quantile_50") - quantile_50_mean
    ) / quantile_50_standard_deviation

    inter_quartile_range = pl.col("quantile_90") - pl.col("quantile_10")

    composite_score = z_score_return / (1 + inter_quartile_range)

    return current_predictions.with_columns(
        z_score_return.alias("z_score_return"),
        inter_quartile_range.alias("inter_quartile_range"),
        composite_score.alias("composite_score"),
        pl.lit(PositionAction.UNSPECIFIED.value).alias("action"),
    ).sort(["composite_score", "inter_quartile_range"], descending=[True, False])


def create_optimal_portfolio(
    current_predictions: pl.DataFrame,
    prior_portfolio_tickers: list[str],
    maximum_capital: float,
    current_timestamp: datetime,
) -> pl.DataFrame:
    current_predictions = current_predictions.clone()

    high_uncertainty_tickers = (
        current_predictions.filter(
            pl.col("inter_quartile_range") > UNCERTAINTY_THRESHOLD
        )
        .select("ticker")
        .to_series()
        .to_list()
    )

    # Excluding prior portfolio tickers to avoid pattern day trader restrictions.
    excluded_tickers = high_uncertainty_tickers + prior_portfolio_tickers

    logger.info(
        "Portfolio filtering breakdown",
        total_predictions=current_predictions.height,
        high_uncertainty_excluded=len(high_uncertainty_tickers),
        high_uncertainty_threshold=UNCERTAINTY_THRESHOLD,
        prior_portfolio_excluded=len(prior_portfolio_tickers),
        total_excluded=len(excluded_tickers),
    )

    available_predictions = current_predictions.filter(
        ~pl.col("ticker").is_in(excluded_tickers)
    )

    logger.info(
        "Available predictions after filtering",
        available_count=available_predictions.height,
        required_for_full_portfolio=20,
    )

    if available_predictions.height < REQUIRED_PORTFOLIO_SIZE:
        message = (
            f"Only {available_predictions.height} predictions available "
            f"after filtering, need {REQUIRED_PORTFOLIO_SIZE} (10 long + 10 short). "
            f"Excluded: {len(high_uncertainty_tickers)} high uncertainty, "
            f"{len(prior_portfolio_tickers)} prior portfolio tickers."
        )
        raise InsufficientPredictionsError(message)

    long_candidates = available_predictions.head(10)
    short_candidates = available_predictions.tail(10)

    target_side_capital = maximum_capital / 2
    dollar_amount_per_position = target_side_capital / 10

    logger.info(
        "Portfolio allocation",
        total_capital=maximum_capital,
        long_capital=target_side_capital,
        short_capital=target_side_capital,
        dollar_per_position=dollar_amount_per_position,
        long_count=10,
        short_count=10,
    )

    long_positions = long_candidates.select(
        pl.col("ticker"),
        pl.lit(current_timestamp.timestamp()).cast(pl.Float64).alias("timestamp"),
        pl.lit(PositionSide.LONG.value).alias("side"),
        pl.lit(dollar_amount_per_position).alias("dollar_amount"),
        pl.lit(PositionAction.OPEN_POSITION.value).alias("action"),
    )

    short_positions = short_candidates.select(
        pl.col("ticker"),
        pl.lit(current_timestamp.timestamp()).cast(pl.Float64).alias("timestamp"),
        pl.lit(PositionSide.SHORT.value).alias("side"),
        pl.lit(dollar_amount_per_position).alias("dollar_amount"),
        pl.lit(PositionAction.OPEN_POSITION.value).alias("action"),
    )

    return pl.concat([long_positions, short_positions]).sort(["ticker", "side"])
