from datetime import UTC, date, datetime, timedelta

import polars as pl
import pytest
from equitypricemodel.trainer import DEFAULT_CONFIGURATION, train_model


SATURDAY_WEEKDAY = 5


def _make_raw_data(
    tickers: list[str] | None = None,
    days: int = 90,
) -> pl.DataFrame:
    tickers = tickers or ["AAPL", "GOOG"]
    start = date(2024, 1, 2)
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


def test_train_model_returns_model_and_data() -> None:
    training_data = _make_raw_data()
    model, data = train_model(training_data)
    assert model is not None
    assert data is not None
    assert hasattr(data, "scaler")
    assert hasattr(data, "mappings")


def test_train_model_uses_custom_configuration() -> None:
    training_data = _make_raw_data()
    custom_config = dict(DEFAULT_CONFIGURATION)
    custom_config["epoch_count"] = 1
    custom_config["hidden_size"] = 32
    model, data = train_model(training_data, configuration=custom_config)
    assert model.hidden_size == 32


def test_train_model_raises_on_insufficient_data() -> None:
    short_data = _make_raw_data(tickers=["AAPL"], days=5)
    with pytest.raises(ValueError):
        train_model(short_data)


def test_train_model_uses_default_configuration() -> None:
    training_data = _make_raw_data()
    model, _ = train_model(training_data)
    assert model.hidden_size == DEFAULT_CONFIGURATION["hidden_size"]
    assert model.output_length == DEFAULT_CONFIGURATION["output_length"]
