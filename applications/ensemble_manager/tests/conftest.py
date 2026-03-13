from collections.abc import Callable
from datetime import UTC, date, datetime, timedelta

import polars as pl
import pytest

SATURDAY_WEEKDAY = 5


@pytest.fixture
def make_raw_data() -> Callable[..., pl.DataFrame]:
    def _make_raw_data(
        tickers: list[str] | None = None,
        days: int = 60,
        start_date: date | None = None,
    ) -> pl.DataFrame:
        tickers = tickers or ["AAPL", "GOOG"]
        start = start_date or date(2024, 1, 2)
        rows = []
        for ticker in tickers:
            for day_offset in range(days):
                current_date = start + timedelta(days=day_offset)
                if current_date.weekday() >= SATURDAY_WEEKDAY:
                    continue
                timestamp = int(
                    datetime(
                        current_date.year,
                        current_date.month,
                        current_date.day,
                        tzinfo=UTC,
                    ).timestamp()
                    * 1000
                )
                close = 100.0 + day_offset * 0.5
                rows.append(
                    {
                        "ticker": ticker,
                        "timestamp": timestamp,
                        "open_price": close - 1.0,
                        "high_price": close + 1.0,
                        "low_price": close - 2.0,
                        "close_price": close,
                        "volume": 1_000_000,
                        "volume_weighted_average_price": close + 0.1,
                        "sector": "Technology",
                        "industry": "Software",
                    }
                )
        return pl.DataFrame(rows)

    return _make_raw_data
