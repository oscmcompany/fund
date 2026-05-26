import io
import os
from datetime import UTC, datetime, timedelta
from typing import Any

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

_PRIOR_ALLOCATION_SCHEMA: dict[str, type] = {
    "ticker": pl.String,
    "timestamp": pl.Int64,
    "side": pl.String,
    "dollar_amount": pl.Float64,
    "action": pl.String,
    "pair_id": pl.String,
    "entry_price": pl.Float64,
    "quantity": pl.Int64,
    "notional": pl.Float64,
}


async def get_prior_allocation() -> pl.DataFrame:
    empty = pl.DataFrame(schema=_PRIOR_ALLOCATION_SCHEMA)
    async with httpx.AsyncClient(timeout=60.0) as client:
        response = await client.get(url=f"{DATA_MANAGER_BASE_URL}/portfolios")
    response.raise_for_status()

    response_text = response.text.strip()
    if not response_text or response_text == "[]":
        logger.info("Prior allocation is empty")
        return empty

    try:
        prior_allocation_data = response.json()

        if not prior_allocation_data:
            return empty

        prior_allocation = pl.DataFrame(
            prior_allocation_data, schema=_PRIOR_ALLOCATION_SCHEMA
        )

        if prior_allocation.is_empty():
            return empty

        logger.info("Retrieved prior allocation", count=prior_allocation.height)
        return prior_allocation  # noqa: TRY300

    except (
        ValueError,
        pl.exceptions.PolarsError,
    ) as e:
        logger.exception("Failed to parse prior allocation JSON", error=str(e))
        raise


async def save_allocation(
    allocation: pl.DataFrame, current_timestamp: datetime
) -> bool:
    try:
        async with httpx.AsyncClient(timeout=60.0) as client:
            response = await client.post(
                url=f"{DATA_MANAGER_BASE_URL}/portfolios",
                json={
                    "timestamp": current_timestamp.isoformat(),
                    "data": allocation.to_dicts(),
                },
            )
        response.raise_for_status()
        logger.info("Saved allocation state", count=allocation.height)
        return True  # noqa: TRY300
    except Exception as e:
        logger.exception("Failed to save allocation state", error=str(e))
        return False


async def save_performance_snapshot(snapshot: dict[str, Any]) -> bool:
    try:
        timestamp_millis = snapshot["timestamp"]
        timestamp_seconds, timestamp_millis_remainder = divmod(timestamp_millis, 1000)
        snapshot_timestamp = datetime.fromtimestamp(timestamp_seconds, tz=UTC).replace(
            microsecond=timestamp_millis_remainder * 1000
        )
        async with httpx.AsyncClient(timeout=60.0) as client:
            response = await client.post(
                url=f"{DATA_MANAGER_BASE_URL}/performance/snapshots",
                json={
                    "timestamp": snapshot_timestamp.isoformat(),
                    "data": snapshot,
                },
            )
        response.raise_for_status()
        logger.info("Saved performance snapshot")
        return True  # noqa: TRY300
    except Exception as e:
        logger.exception("Failed to save performance snapshot", error=str(e))
        return False


get_prior_portfolio = get_prior_allocation
save_portfolio = save_allocation


async def save_closed_pair(record: dict[str, Any]) -> bool:
    try:
        closed_timestamp_millis = record["closed_timestamp"]
        closed_timestamp_seconds, closed_millis_remainder = divmod(
            closed_timestamp_millis, 1000
        )
        closed_timestamp = datetime.fromtimestamp(
            closed_timestamp_seconds, tz=UTC
        ).replace(microsecond=closed_millis_remainder * 1000)
        async with httpx.AsyncClient(timeout=60.0) as client:
            response = await client.post(
                url=f"{DATA_MANAGER_BASE_URL}/performance/closed-pairs",
                json={
                    "timestamp": closed_timestamp.isoformat(),
                    "data": record,
                },
            )
        response.raise_for_status()
        logger.info("Saved closed pair record")
        return True  # noqa: TRY300
    except Exception as e:
        logger.exception("Failed to save closed pair record", error=str(e))
        return False


async def get_last_portfolio_value() -> float | None:
    try:
        now = datetime.now(tz=UTC)
        seven_days_ago = now - timedelta(days=7)

        async with httpx.AsyncClient(timeout=60.0) as client:
            response = await client.get(
                url=f"{DATA_MANAGER_BASE_URL}/performance/snapshots",
                params={
                    "start_timestamp": seven_days_ago.isoformat(),
                    "end_timestamp": now.isoformat(),
                },
            )

        response.raise_for_status()

        dataframe = pl.read_parquet(io.BytesIO(response.content))

        if dataframe.height == 0:
            return None

        return float(dataframe["portfolio_value"][-1])

    except Exception as e:
        logger.exception("Failed to retrieve last portfolio value", error=str(e))
        return None


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
