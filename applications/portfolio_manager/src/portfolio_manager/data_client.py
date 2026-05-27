from datetime import datetime, timedelta

import polars as pl
import structlog
from internal.database import get_pool

from .exceptions import PriceDataUnavailableError

logger = structlog.get_logger(__name__)


async def fetch_historical_prices(
    reference_date: datetime,
    lookback_days: int = 120,
    # TODO(#876): remove datamanager_base_url in Phase 9  # noqa: FIX002
    datamanager_base_url: str = "",  # noqa: ARG001
) -> pl.DataFrame:
    start_timestamp = reference_date - timedelta(days=lookback_days)

    try:
        pool = await get_pool()
        async with pool.connection() as connection:
            result = await connection.execute(
                """SELECT ticker,
                          EXTRACT(EPOCH FROM timestamp)::bigint * 1000 AS timestamp,
                          close_price
                   FROM equity_bars
                   WHERE timestamp >= %s AND timestamp <= %s
                   ORDER BY ticker, timestamp""",
                (start_timestamp, reference_date),
            )
            rows = await result.fetchall()
    except Exception as error:
        message = f"Failed to fetch historical prices from database: {error}"
        raise PriceDataUnavailableError(message) from error

    if not rows:
        return pl.DataFrame(
            schema={
                "ticker": pl.String,
                "timestamp": pl.Int64,
                "close_price": pl.Float64,
            }
        )

    dataframe = pl.DataFrame(
        {
            "ticker": [row[0] for row in rows],
            "timestamp": [row[1] for row in rows],
            "close_price": [row[2] for row in rows],
        }
    )
    cleaned = dataframe.drop_nulls(subset=["close_price"])
    deduped = cleaned.sort("timestamp").unique(
        subset=["ticker", "timestamp"], keep="last", maintain_order=True
    )
    duplicates_removed = cleaned.height - deduped.height
    if duplicates_removed > 0:
        logger.warning(
            "Removed duplicate ticker-timestamp rows from historical prices",
            duplicates_removed=duplicates_removed,
        )
    return deduped


async def fetch_equity_details(
    # TODO(#876): remove datamanager_base_url in Phase 9  # noqa: FIX002
    datamanager_base_url: str = "",  # noqa: ARG001
) -> pl.DataFrame:
    try:
        pool = await get_pool()
        async with pool.connection() as connection:
            result = await connection.execute(
                "SELECT ticker, sector FROM equity_details ORDER BY ticker"
            )
            rows = await result.fetchall()
    except Exception as error:
        message = f"Failed to fetch equity details from database: {error}"
        raise PriceDataUnavailableError(message) from error

    if not rows:
        return pl.DataFrame(schema={"ticker": pl.String, "sector": pl.String})

    return pl.DataFrame(
        {
            "ticker": [row[0] for row in rows],
            "sector": [row[1] for row in rows],
        }
    )


async def fetch_spy_prices(
    reference_date: datetime,
    lookback_days: int = 90,
    # TODO(#876): remove datamanager_base_url in Phase 9  # noqa: FIX002
    datamanager_base_url: str = "",  # noqa: ARG001
) -> pl.DataFrame:
    start_timestamp = reference_date - timedelta(days=lookback_days)

    try:
        pool = await get_pool()
        async with pool.connection() as connection:
            result = await connection.execute(
                """SELECT ticker,
                          EXTRACT(EPOCH FROM timestamp)::bigint * 1000 AS timestamp,
                          close_price
                   FROM equity_bars
                   WHERE ticker = 'SPY'
                     AND timestamp >= %s AND timestamp <= %s
                   ORDER BY timestamp""",
                (start_timestamp, reference_date),
            )
            rows = await result.fetchall()
    except Exception as error:
        message = f"Failed to fetch SPY prices from database: {error}"
        raise PriceDataUnavailableError(message) from error

    if not rows:
        return pl.DataFrame(
            schema={
                "ticker": pl.String,
                "timestamp": pl.Int64,
                "close_price": pl.Float64,
            }
        )

    dataframe = pl.DataFrame(
        {
            "ticker": [row[0] for row in rows],
            "timestamp": [row[1] for row in rows],
            "close_price": [row[2] for row in rows],
        }
    )
    cleaned = dataframe.drop_nulls(subset=["close_price"])
    deduped = cleaned.sort("timestamp").unique(
        subset=["ticker", "timestamp"], keep="last", maintain_order=True
    )
    duplicates_removed = cleaned.height - deduped.height
    if duplicates_removed > 0:
        logger.warning(
            "Removed duplicate ticker-timestamp rows from SPY prices",
            duplicates_removed=duplicates_removed,
        )
    return deduped
