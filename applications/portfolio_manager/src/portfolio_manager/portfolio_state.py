import os
from datetime import datetime

import httpx
import numpy as np
import polars as pl
import structlog

from .enums import PositionSide
from .risk_management import Z_SCORE_HOLD_THRESHOLD, Z_SCORE_STOP_LOSS
from .statistical_arbitrage import CORRELATION_WINDOW_DAYS, compute_spread_zscore

logger = structlog.get_logger()

DATA_MANAGER_BASE_URL = os.getenv(
    "FUND_DATA_MANAGER_BASE_URL", "http://data-manager:8080"
)

_MINIMUM_PAIR_PRICE_ROWS = 30

_PRIOR_PORTFOLIO_SCHEMA: dict[str, type] = {
    "ticker": pl.String,
    "timestamp": pl.Int64,
    "side": pl.String,
    "dollar_amount": pl.Float64,
    "action": pl.String,
    "pair_id": pl.String,
}


async def get_prior_portfolio() -> pl.DataFrame:
    empty = pl.DataFrame(schema=_PRIOR_PORTFOLIO_SCHEMA)
    async with httpx.AsyncClient(timeout=60.0) as client:
        response = await client.get(url=f"{DATA_MANAGER_BASE_URL}/portfolios")
    response.raise_for_status()

    response_text = response.text.strip()
    if not response_text or response_text == "[]":
        logger.info("Prior portfolio is empty")
        return empty

    try:
        prior_portfolio_data = response.json()

        if not prior_portfolio_data:
            return empty

        prior_portfolio = pl.DataFrame(
            prior_portfolio_data, schema=_PRIOR_PORTFOLIO_SCHEMA
        )

        if prior_portfolio.is_empty():
            return empty

        logger.info("Retrieved prior portfolio", count=prior_portfolio.height)
        return prior_portfolio  # noqa: TRY300

    except (
        ValueError,
        pl.exceptions.PolarsError,
    ) as e:
        logger.exception("Failed to parse prior portfolio JSON", error=str(e))
        raise


async def save_portfolio(portfolio: pl.DataFrame, current_timestamp: datetime) -> bool:
    try:
        async with httpx.AsyncClient(timeout=60.0) as client:
            response = await client.post(
                url=f"{DATA_MANAGER_BASE_URL}/portfolios",
                json={
                    "timestamp": current_timestamp.isoformat(),
                    "data": portfolio.to_dicts(),
                },
            )
        response.raise_for_status()
        logger.info("Saved portfolio state", count=portfolio.height)
        return True  # noqa: TRY300
    except Exception as e:
        logger.exception("Failed to save portfolio state", error=str(e))
        return False


def evaluate_prior_pairs(
    prior_portfolio: pl.DataFrame,
    historical_prices: pl.DataFrame,
) -> set[str]:
    held_tickers: set[str] = set()

    if prior_portfolio.is_empty():
        return held_tickers

    pair_ids = prior_portfolio["pair_id"].unique(maintain_order=False).sort().to_list()

    for pair_id in pair_ids:
        pair_rows = prior_portfolio.filter(pl.col("pair_id") == pair_id)

        long_rows = pair_rows.filter(pl.col("side") == PositionSide.LONG.value)
        short_rows = pair_rows.filter(pl.col("side") == PositionSide.SHORT.value)

        if long_rows.is_empty() or short_rows.is_empty():
            logger.warning("Malformed prior pair, closing normally", pair_id=pair_id)
            continue

        long_ticker = long_rows["ticker"][0]
        short_ticker = short_rows["ticker"][0]

        pair_price_matrix = (
            historical_prices.filter(
                pl.col("ticker").is_in([long_ticker, short_ticker])
            )
            .pivot(
                on="ticker",
                index="timestamp",
                values="close_price",
                aggregate_function="last",
            )
            .sort("timestamp")
            .drop_nulls()
        )

        if (
            long_ticker not in pair_price_matrix.columns
            or short_ticker not in pair_price_matrix.columns
        ):
            logger.warning(
                "Missing price data for prior pair, closing normally",
                pair_id=pair_id,
            )
            continue

        pair_price_matrix = pair_price_matrix.tail(CORRELATION_WINDOW_DAYS)

        if pair_price_matrix.height < _MINIMUM_PAIR_PRICE_ROWS:
            logger.warning(
                "Insufficient price history for prior pair, closing normally",
                pair_id=pair_id,
            )
            continue

        long_prices = pair_price_matrix[long_ticker].to_numpy()
        short_prices = pair_price_matrix[short_ticker].to_numpy()

        if np.any(long_prices <= 0) or np.any(short_prices <= 0):
            logger.warning(
                "Non-positive prices in prior pair, closing normally",
                pair_id=pair_id,
            )
            continue

        log_prices_long = np.log(long_prices)
        log_prices_short = np.log(short_prices)

        current_z, _ = compute_spread_zscore(log_prices_long, log_prices_short)

        if np.isnan(current_z):
            logger.warning(
                "NaN z-score for prior pair, closing normally",
                pair_id=pair_id,
            )
            continue

        absolute_z_score = abs(current_z)

        if Z_SCORE_HOLD_THRESHOLD <= absolute_z_score < Z_SCORE_STOP_LOSS:
            held_tickers.add(long_ticker)
            held_tickers.add(short_ticker)
            logger.info(
                "Holding prior pair, spread still mean-reverting",
                pair_id=pair_id,
                z_score=current_z,
            )
        elif absolute_z_score < Z_SCORE_HOLD_THRESHOLD:
            logger.info(
                "Closing prior pair to realize profit, spread converged",
                pair_id=pair_id,
                z_score=current_z,
            )
        else:
            logger.info(
                "Closing prior pair on stop-loss, spread diverged",
                pair_id=pair_id,
                z_score=current_z,
            )

    return held_tickers
