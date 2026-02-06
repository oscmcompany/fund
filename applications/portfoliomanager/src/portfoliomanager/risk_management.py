import math
import os
from datetime import UTC, datetime

import polars as pl
import structlog

from .enums import PositionAction, PositionSide
from .exceptions import InsufficientPredictionsError

logger = structlog.get_logger()

UNCERTAINTY_THRESHOLD = float(os.getenv("OSCM_UNCERTAINTY_THRESHOLD", "0.1"))


def add_portfolio_action_column(
    prior_portfolio: pl.DataFrame,
    current_timestamp: datetime,
) -> pl.DataFrame:
    prior_portfolio = prior_portfolio.clone()

    return prior_portfolio.with_columns(
        pl.when(
            pl.col("timestamp")
            .cast(pl.Float64)
            .map_elements(
                lambda ts: datetime.fromtimestamp(ts, tz=UTC).date(),
                return_dtype=pl.Date,
            )
            == current_timestamp.date()
        )
        .then(pl.lit(PositionAction.PDT_LOCKED.value))
        .otherwise(pl.lit(PositionAction.UNSPECIFIED.value))
        .alias("action")
    )


def add_equity_bars_returns_and_realized_volatility_columns(
    prior_equity_bars: pl.DataFrame,
) -> pl.DataFrame:
    prior_equity_bars = prior_equity_bars.clone()

    minimum_bars_per_ticker_required = 30

    ticker_counts = prior_equity_bars.group_by("ticker").agg(pl.len().alias("count"))
    insufficient_tickers = ticker_counts.filter(
        pl.col("count") < minimum_bars_per_ticker_required
    )

    if insufficient_tickers.height > 0:
        insufficient_list = insufficient_tickers.select("ticker").to_series().to_list()
        message = f"Tickers with insufficient data (< {minimum_bars_per_ticker_required} rows): {insufficient_list}"  # noqa: E501
        raise ValueError(message)

    prior_equity_bars = prior_equity_bars.sort(["ticker", "timestamp"])
    daily_returns = pl.col("close_price").pct_change().over("ticker")
    return prior_equity_bars.with_columns(
        pl.when(pl.col("close_price").is_not_null())
        .then(daily_returns)
        .otherwise(None)
        .alias("daily_returns"),
        pl.when(pl.col("close_price").is_not_null())
        .then(
            pl.when((daily_returns + 1) > 0)
            .then((daily_returns + 1).log())
            .otherwise(None)
        )
        .otherwise(None)
        .alias("log_daily_returns"),
        daily_returns.rolling_std(window_size=minimum_bars_per_ticker_required).alias(
            "realized_volatility"
        ),
    )


def add_portfolio_performance_columns(
    prior_portfolio: pl.DataFrame,
    prior_predictions: pl.DataFrame,  # per original ticker and timestamp
    prior_equity_bars: pl.DataFrame,  # per original ticker and timestamp
    current_timestamp: datetime,
) -> pl.DataFrame:
    prior_portfolio = prior_portfolio.clone()
    prior_predictions = prior_predictions.clone()
    prior_equity_bars = prior_equity_bars.clone()

    # Ensure timestamp columns have matching types for joins and comparisons.
    # Timestamps may arrive as i64 (from JSON integer serialization) or f64 (from
    # Python float conversion). Unconditional casting to Float64 is simpler and
    # more robust than checking dtypes, and the performance cost is negligible.
    prior_portfolio = prior_portfolio.with_columns(pl.col("timestamp").cast(pl.Float64))
    prior_predictions = prior_predictions.with_columns(
        pl.col("timestamp").cast(pl.Float64)
    )
    prior_equity_bars = prior_equity_bars.with_columns(
        pl.col("timestamp").cast(pl.Float64)
    )

    prior_portfolio_predictions = prior_portfolio.join(
        other=prior_predictions,
        on=["ticker", "timestamp"],
        how="left",
    ).select(
        pl.col("ticker"),
        pl.col("timestamp"),
        pl.col("side"),
        pl.col("dollar_amount"),
        pl.col("action"),
        pl.col("quantile_10").alias("original_lower_threshold"),
        pl.col("quantile_90").alias("original_upper_threshold"),
    )

    prior_equity_bars_with_returns = prior_equity_bars.sort(["ticker", "timestamp"])

    position_returns = []

    for row in prior_portfolio_predictions.iter_rows(named=True):
        ticker = row["ticker"]
        position_timestamp = row["timestamp"]

        ticker_bars = prior_equity_bars_with_returns.filter(
            (pl.col("ticker") == ticker)
            & (pl.col("timestamp") >= position_timestamp)
            & (pl.col("timestamp") <= current_timestamp.timestamp())
        )

        cumulative_log_return = (
            ticker_bars.select(pl.col("log_daily_returns").sum()).item() or 0
        )

        cumulative_simple_return = math.exp(cumulative_log_return) - 1

        position_returns.append(
            {
                "ticker": ticker,
                "timestamp": position_timestamp,
                "cumulative_simple_return": cumulative_simple_return,
            }
        )

    returns = pl.DataFrame(position_returns)

    prior_portfolio_with_data = prior_portfolio_predictions.join(
        other=returns,
        on=["ticker", "timestamp"],
        how="left",
    )

    portfolio_with_actions = prior_portfolio_with_data.with_columns(
        pl.when(pl.col("action") == PositionAction.PDT_LOCKED.value)
        .then(pl.lit(PositionAction.PDT_LOCKED.value))
        .when(
            (pl.col("action") != PositionAction.PDT_LOCKED.value)
            & (
                (
                    (pl.col("side") == PositionSide.LONG.value)
                    & (
                        pl.col("cumulative_simple_return")
                        <= pl.col("original_lower_threshold")
                    )
                )
                | (
                    (pl.col("side") == PositionSide.SHORT.value)
                    & (
                        pl.col("cumulative_simple_return")
                        >= pl.col("original_upper_threshold")
                    )
                )
            )
        )
        .then(pl.lit(PositionAction.CLOSE_POSITION.value))
        .when(
            (
                (pl.col("side") == PositionSide.LONG.value)
                & (
                    pl.col("cumulative_simple_return")
                    >= pl.col("original_upper_threshold")
                )
            )
            | (
                (pl.col("side") == PositionSide.SHORT.value)
                & (
                    pl.col("cumulative_simple_return")
                    <= pl.col("original_lower_threshold")
                )
            )
        )
        .then(pl.lit(PositionAction.MAINTAIN_POSITION.value))
        .otherwise(pl.lit(PositionAction.UNSPECIFIED.value))
        .alias("action")
    )

    # Rebalancing logic: if one side has more closures than the other,
    # close equal number of best performers from the opposite side
    closed_long_count = portfolio_with_actions.filter(
        (pl.col("side") == PositionSide.LONG.value)
        & (pl.col("action") == PositionAction.CLOSE_POSITION.value)
    ).height

    closed_short_count = portfolio_with_actions.filter(
        (pl.col("side") == PositionSide.SHORT.value)
        & (pl.col("action") == PositionAction.CLOSE_POSITION.value)
    ).height

    # If more longs are being closed, close equal number of best-performing shorts
    if closed_long_count > closed_short_count:
        shorts_to_rebalance = closed_long_count - closed_short_count

        # Select best-performing shorts (most negative cumulative return = best gain)
        # Consider positions that are not already being closed and not PDT locked
        best_shorts = (
            portfolio_with_actions.filter(
                (pl.col("side") == PositionSide.SHORT.value)
                & (pl.col("action") != PositionAction.CLOSE_POSITION.value)
                & (pl.col("action") != PositionAction.PDT_LOCKED.value)
            )
            .sort("cumulative_simple_return", descending=False)
            .head(shorts_to_rebalance)
            .select("ticker")
        )

        if best_shorts.height > 0:
            logger.info(
                "Rebalancing portfolio by closing shorts",
                closed_longs=closed_long_count,
                closed_shorts=closed_short_count,
                additional_shorts_to_close=shorts_to_rebalance,
                shorts_being_closed=best_shorts.to_series().to_list(),
            )

            portfolio_with_actions = portfolio_with_actions.with_columns(
                pl.when(pl.col("ticker").is_in(best_shorts["ticker"]))
                .then(pl.lit(PositionAction.CLOSE_POSITION.value))
                .otherwise(pl.col("action"))
                .alias("action")
            )

    # If more shorts are being closed, close equal number of best-performing longs
    elif closed_short_count > closed_long_count:
        longs_to_rebalance = closed_short_count - closed_long_count

        # Select best-performing longs (most positive cumulative return = best gain)
        # Consider positions that are not already being closed and not PDT locked
        best_longs = (
            portfolio_with_actions.filter(
                (pl.col("side") == PositionSide.LONG.value)
                & (pl.col("action") != PositionAction.CLOSE_POSITION.value)
                & (pl.col("action") != PositionAction.PDT_LOCKED.value)
            )
            .sort("cumulative_simple_return", descending=True)
            .head(longs_to_rebalance)
            .select("ticker")
        )

        if best_longs.height > 0:
            logger.info(
                "Rebalancing portfolio by closing longs",
                closed_longs=closed_long_count,
                closed_shorts=closed_short_count,
                additional_longs_to_close=longs_to_rebalance,
                longs_being_closed=best_longs.to_series().to_list(),
            )

            portfolio_with_actions = portfolio_with_actions.with_columns(
                pl.when(pl.col("ticker").is_in(best_longs["ticker"]))
                .then(pl.lit(PositionAction.CLOSE_POSITION.value))
                .otherwise(pl.col("action"))
                .alias("action")
            )

    return portfolio_with_actions.drop(
        [
            "original_lower_threshold",
            "original_upper_threshold",
            "cumulative_simple_return",
        ]
    )


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
    prior_portfolio: pl.DataFrame,
    maximum_capital: float,
    current_timestamp: datetime,
) -> pl.DataFrame:
    current_predictions = current_predictions.clone()
    prior_portfolio = prior_portfolio.clone()

    high_uncertainty_tickers = (
        current_predictions.filter(
            pl.col("inter_quartile_range") > UNCERTAINTY_THRESHOLD
        )
        .select("ticker")
        .to_series()
        .to_list()
    )

    closed_positions, maintained_positions = _filter_positions(prior_portfolio)

    closed_position_tickers = closed_positions.select("ticker").to_series().to_list()
    maintained_position_tickers = (
        maintained_positions.select("ticker").to_series().to_list()
    )

    excluded_tickers = (
        high_uncertainty_tickers + closed_position_tickers + maintained_position_tickers
    )

    prediction_summary = current_predictions.select(
        "ticker",
        "quantile_10",
        "quantile_50",
        "quantile_90",
        "inter_quartile_range",
        "composite_score",
    ).to_dicts()

    logger.info(
        "Current predictions received",
        predictions=prediction_summary,
    )

    logger.info(
        "Portfolio filtering breakdown",
        total_predictions=current_predictions.height,
        high_uncertainty_excluded=len(high_uncertainty_tickers),
        high_uncertainty_threshold=UNCERTAINTY_THRESHOLD,
        closed_positions_excluded=len(closed_position_tickers),
        maintained_positions_excluded=len(maintained_position_tickers),
        total_excluded=len(excluded_tickers),
        high_uncertainty_tickers=high_uncertainty_tickers[:10]
        if high_uncertainty_tickers
        else [],
    )

    available_predictions = current_predictions.filter(
        ~pl.col("ticker").is_in(excluded_tickers)
    )

    logger.info(
        "Available predictions after filtering",
        available_count=available_predictions.height,
        required_for_full_portfolio=20,
    )

    maintained_long_capital = _filter_side_capital_amount(
        maintained_positions, PositionSide.LONG.value
    )
    maintained_short_capital = _filter_side_capital_amount(
        maintained_positions, PositionSide.SHORT.value
    )
    closed_long_capital = _filter_side_capital_amount(
        closed_positions, PositionSide.LONG.value
    )
    closed_short_capital = _filter_side_capital_amount(
        closed_positions, PositionSide.SHORT.value
    )

    target_side_capital = maximum_capital / 2
    available_long_capital = max(
        0.0,
        target_side_capital - maintained_long_capital + closed_long_capital,
    )
    available_short_capital = max(
        0.0,
        target_side_capital - maintained_short_capital + closed_short_capital,
    )

    maintained_long_count = maintained_positions.filter(
        pl.col("side") == PositionSide.LONG.value
    ).height
    maintained_short_count = maintained_positions.filter(
        pl.col("side") == PositionSide.SHORT.value
    ).height

    new_long_positions_needed = max(0, 10 - maintained_long_count)
    new_short_positions_needed = max(0, 10 - maintained_short_count)

    total_available = available_predictions.height
    maximum_long_candidates = min(new_long_positions_needed, total_available // 2)
    maximum_short_candidates = min(
        new_short_positions_needed, total_available - maximum_long_candidates
    )

    logger.info(
        "Position allocation calculation",
        total_available_predictions=total_available,
        new_long_positions_needed=new_long_positions_needed,
        new_short_positions_needed=new_short_positions_needed,
        maximum_long_candidates=maximum_long_candidates,
        maximum_short_candidates=maximum_short_candidates,
        maintained_long_count=maintained_long_count,
        maintained_short_count=maintained_short_count,
    )

    long_candidates = available_predictions.head(maximum_long_candidates)
    short_candidates = available_predictions.tail(maximum_short_candidates)

    dollar_amount_per_long = (
        available_long_capital / maximum_long_candidates
        if maximum_long_candidates > 0
        else 0
    )
    dollar_amount_per_short = (
        available_short_capital / maximum_short_candidates
        if maximum_short_candidates > 0
        else 0
    )

    long_positions = long_candidates.select(
        pl.col("ticker"),
        pl.lit(current_timestamp.timestamp()).cast(pl.Float64).alias("timestamp"),
        pl.lit(PositionSide.LONG.value).alias("side"),
        pl.lit(dollar_amount_per_long).alias("dollar_amount"),
        pl.lit(PositionAction.OPEN_POSITION.value).alias("action"),
    )

    short_positions = short_candidates.select(
        pl.col("ticker"),
        pl.lit(current_timestamp.timestamp()).cast(pl.Float64).alias("timestamp"),
        pl.lit(PositionSide.SHORT.value).alias("side"),
        pl.lit(dollar_amount_per_short).alias("dollar_amount"),
        pl.lit(PositionAction.OPEN_POSITION.value).alias("action"),
    )

    return _collect_portfolio_positions(
        long_positions,
        short_positions,
        maintained_positions,
    )


def _filter_positions(positions: pl.DataFrame) -> tuple[pl.DataFrame, pl.DataFrame]:
    positions = positions.clone()

    if positions.height == 0:
        return (
            pl.DataFrame(
                {
                    "ticker": [],
                    "timestamp": [],
                    "side": [],
                    "dollar_amount": [],
                    "action": [],
                }
            ),
            pl.DataFrame(
                {
                    "ticker": [],
                    "timestamp": [],
                    "side": [],
                    "dollar_amount": [],
                    "action": [],
                }
            ),
        )

    closed_positions = positions.filter(
        pl.col("action") == PositionAction.CLOSE_POSITION.value
    )
    maintained_positions = positions.filter(
        pl.col("action") == PositionAction.MAINTAIN_POSITION.value
    )

    return closed_positions, maintained_positions


def _filter_side_capital_amount(positions: pl.DataFrame, side: str) -> float:
    positions = positions.clone()

    filtered_positions = positions.filter(pl.col("side") == side.upper())

    if filtered_positions.height == 0:
        return 0.0

    try:
        side_capital_amount = filtered_positions.select(pl.sum("dollar_amount")).item()
        return float(side_capital_amount or 0)

    except Exception:  # noqa: BLE001
        return 0.0


def _collect_portfolio_positions(
    long_positions: pl.DataFrame,
    short_positions: pl.DataFrame,
    maintained_positions: pl.DataFrame,
) -> pl.DataFrame:
    long_positions = long_positions.clone()
    short_positions = short_positions.clone()
    maintained_positions = maintained_positions.clone()

    portfolio_components = []

    if long_positions.height > 0:
        portfolio_components.append(long_positions)
    if short_positions.height > 0:
        portfolio_components.append(short_positions)
    if maintained_positions.height > 0:
        portfolio_components.append(
            maintained_positions.with_columns(pl.col("timestamp").cast(pl.Float64))
        )

    if len(portfolio_components) == 0:
        logger.warning(
            "No portfolio components available",
            long_positions_count=long_positions.height,
            short_positions_count=short_positions.height,
            maintained_positions_count=maintained_positions.height,
        )
        message = (
            "No portfolio components to create an optimal portfolio. "
            f"Long positions: {long_positions.height}, "
            f"Short positions: {short_positions.height}, "
            f"Maintained positions: {maintained_positions.height}. "
            "This may indicate insufficient predictions after filtering."
        )
        raise InsufficientPredictionsError(message)

    optimal_portfolio = pl.concat(portfolio_components)

    return optimal_portfolio.select(
        "ticker",
        pl.col("timestamp").cast(pl.Float64),
        "side",
        "dollar_amount",
        "action",
    ).sort(["ticker", "side"])
