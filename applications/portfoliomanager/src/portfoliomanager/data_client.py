import io
from datetime import datetime, timedelta

import polars as pl
import requests

from .exceptions import PriceDataUnavailableError


def fetch_historical_prices(
    datamanager_base_url: str,
    reference_date: datetime,
    lookback_days: int = 90,
) -> pl.DataFrame:
    start_timestamp = reference_date - timedelta(days=lookback_days)

    try:
        response = requests.get(
            url=f"{datamanager_base_url}/equity-bars",
            params={
                "start_timestamp": start_timestamp.isoformat(),
                "end_timestamp": reference_date.isoformat(),
            },
            timeout=120,
        )
        response.raise_for_status()
    except requests.HTTPError as error:
        message = f"Failed to fetch historical prices from data manager: {error}"
        raise PriceDataUnavailableError(message) from error
    except requests.RequestException as error:
        message = f"Network error fetching historical prices from data manager: {error}"
        raise PriceDataUnavailableError(message) from error

    dataframe = pl.read_parquet(io.BytesIO(response.content))
    return dataframe.select(["ticker", "timestamp", "close_price"]).drop_nulls(
        subset=["close_price"]
    )


def fetch_equity_details(datamanager_base_url: str) -> pl.DataFrame:
    try:
        response = requests.get(
            url=f"{datamanager_base_url}/equity-details",
            timeout=60,
        )
        response.raise_for_status()
    except requests.HTTPError as error:
        message = f"Failed to fetch equity details from data manager: {error}"
        raise PriceDataUnavailableError(message) from error
    except requests.RequestException as error:
        message = f"Network error fetching equity details from data manager: {error}"
        raise PriceDataUnavailableError(message) from error

    dataframe = pl.read_csv(io.BytesIO(response.content))
    return dataframe.select(["ticker", "sector"])
