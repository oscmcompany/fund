import io
from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock, patch

import polars as pl
import pytest
import requests
from portfolio_manager.data_client import (
    fetch_equity_details,
    fetch_historical_prices,
    fetch_spy_prices,
)
from portfolio_manager.exceptions import PriceDataUnavailableError


def _make_parquet_bytes(dataframe: pl.DataFrame) -> bytes:
    buffer = io.BytesIO()
    dataframe.write_parquet(buffer)
    return buffer.getvalue()


def _make_csv_bytes(dataframe: pl.DataFrame) -> bytes:
    return dataframe.write_csv().encode()


def test_fetch_historical_prices_returns_expected_columns() -> None:
    raw = pl.DataFrame(
        {
            "ticker": ["AAPL", "MSFT"],
            "timestamp": ["2024-01-01", "2024-01-01"],
            "close_price": [150.0, 300.0],
            "extra_column": [1, 2],
        }
    )
    mock_response = MagicMock()
    mock_response.content = _make_parquet_bytes(raw)
    mock_response.raise_for_status.return_value = None

    with patch(
        "portfolio_manager.data_client.requests.get", return_value=mock_response
    ):
        result = fetch_historical_prices(
            "http://localhost", datetime(2024, 1, 2, tzinfo=UTC)
        )

    assert result.columns == ["ticker", "timestamp", "close_price"]
    assert result.height == raw.height


def test_fetch_historical_prices_drops_null_close_prices() -> None:
    raw = pl.DataFrame(
        {
            "ticker": ["AAPL", "MSFT", "GOOG"],
            "timestamp": ["2024-01-01", "2024-01-01", "2024-01-01"],
            "close_price": [150.0, None, 200.0],
        }
    )
    mock_response = MagicMock()
    mock_response.content = _make_parquet_bytes(raw)
    mock_response.raise_for_status.return_value = None

    with patch(
        "portfolio_manager.data_client.requests.get", return_value=mock_response
    ):
        result = fetch_historical_prices(
            "http://localhost", datetime(2024, 1, 2, tzinfo=UTC)
        )

    assert result.height == raw.height - 1
    assert "MSFT" not in result["ticker"].to_list()


def test_fetch_historical_prices_sends_correct_query_params() -> None:
    reference_date = datetime(2024, 4, 1, tzinfo=UTC)
    raw = pl.DataFrame({"ticker": [], "timestamp": [], "close_price": []})
    mock_response = MagicMock()
    mock_response.content = _make_parquet_bytes(raw)
    mock_response.raise_for_status.return_value = None

    with patch(
        "portfolio_manager.data_client.requests.get", return_value=mock_response
    ) as mock_get:
        fetch_historical_prices(
            "http://datamanager:8080", reference_date, lookback_days=90
        )

    expected_start = (reference_date - timedelta(days=90)).isoformat()
    mock_get.assert_called_once_with(
        url="http://datamanager:8080/equity-bars",
        params={
            "start_timestamp": expected_start,
            "end_timestamp": reference_date.isoformat(),
        },
        timeout=120,
    )


def test_fetch_historical_prices_raises_on_http_error() -> None:
    mock_response = MagicMock()
    mock_response.raise_for_status.side_effect = requests.HTTPError("500 Server Error")

    with (
        patch("portfolio_manager.data_client.requests.get", return_value=mock_response),
        pytest.raises(PriceDataUnavailableError),
    ):
        fetch_historical_prices("http://localhost", datetime(2024, 1, 1, tzinfo=UTC))


def test_fetch_historical_prices_raises_on_network_error() -> None:
    with (
        patch(
            "portfolio_manager.data_client.requests.get",
            side_effect=requests.RequestException("Connection refused"),
        ),
        pytest.raises(PriceDataUnavailableError),
    ):
        fetch_historical_prices("http://localhost", datetime(2024, 1, 1, tzinfo=UTC))


def test_fetch_equity_details_returns_expected_columns() -> None:
    raw = pl.DataFrame(
        {
            "ticker": ["AAPL", "MSFT"],
            "sector": ["Technology", "Technology"],
            "extra_column": ["foo", "bar"],
        }
    )
    mock_response = MagicMock()
    mock_response.content = _make_csv_bytes(raw)
    mock_response.raise_for_status.return_value = None

    with patch(
        "portfolio_manager.data_client.requests.get", return_value=mock_response
    ):
        result = fetch_equity_details("http://localhost")

    assert result.columns == ["ticker", "sector"]
    assert result.height == raw.height


def test_fetch_equity_details_raises_on_http_error() -> None:
    mock_response = MagicMock()
    mock_response.raise_for_status.side_effect = requests.HTTPError("404 Not Found")

    with (
        patch("portfolio_manager.data_client.requests.get", return_value=mock_response),
        pytest.raises(PriceDataUnavailableError),
    ):
        fetch_equity_details("http://localhost")


def test_fetch_equity_details_raises_on_network_error() -> None:
    with (
        patch(
            "portfolio_manager.data_client.requests.get",
            side_effect=requests.RequestException("Timeout"),
        ),
        pytest.raises(PriceDataUnavailableError),
    ):
        fetch_equity_details("http://localhost")


def test_fetch_spy_prices_returns_expected_columns() -> None:
    raw = pl.DataFrame(
        {
            "ticker": ["SPY", "SPY"],
            "timestamp": ["2024-01-01", "2024-01-02"],
            "close_price": [450.0, 452.0],
            "extra_column": [1, 2],
        }
    )
    mock_response = MagicMock()
    mock_response.content = _make_parquet_bytes(raw)
    mock_response.raise_for_status.return_value = None

    with patch(
        "portfolio_manager.data_client.requests.get", return_value=mock_response
    ):
        result = fetch_spy_prices("http://localhost", datetime(2024, 1, 3, tzinfo=UTC))

    assert result.columns == ["ticker", "timestamp", "close_price"]
    assert result.height == raw.height


def test_fetch_spy_prices_drops_null_close_prices() -> None:
    raw = pl.DataFrame(
        {
            "ticker": ["SPY", "SPY"],
            "timestamp": ["2024-01-01", "2024-01-02"],
            "close_price": [450.0, None],
        }
    )
    mock_response = MagicMock()
    mock_response.content = _make_parquet_bytes(raw)
    mock_response.raise_for_status.return_value = None

    with patch(
        "portfolio_manager.data_client.requests.get", return_value=mock_response
    ):
        result = fetch_spy_prices("http://localhost", datetime(2024, 1, 3, tzinfo=UTC))

    assert result.height == 1


def test_fetch_spy_prices_sends_correct_query_params() -> None:
    reference_date = datetime(2024, 4, 1, tzinfo=UTC)
    raw = pl.DataFrame({"ticker": [], "timestamp": [], "close_price": []})
    mock_response = MagicMock()
    mock_response.content = _make_parquet_bytes(raw)
    mock_response.raise_for_status.return_value = None

    with patch(
        "portfolio_manager.data_client.requests.get", return_value=mock_response
    ) as mock_get:
        fetch_spy_prices("http://datamanager:8080", reference_date, lookback_days=90)

    expected_start = (reference_date - timedelta(days=90)).isoformat()
    mock_get.assert_called_once_with(
        url="http://datamanager:8080/equity-bars",
        params={
            "tickers": "SPY",
            "start_timestamp": expected_start,
            "end_timestamp": reference_date.isoformat(),
        },
        timeout=120,
    )


def test_fetch_spy_prices_raises_on_http_error() -> None:
    mock_response = MagicMock()
    mock_response.raise_for_status.side_effect = requests.HTTPError("500 Server Error")

    with (
        patch("portfolio_manager.data_client.requests.get", return_value=mock_response),
        pytest.raises(PriceDataUnavailableError),
    ):
        fetch_spy_prices("http://localhost", datetime(2024, 1, 1, tzinfo=UTC))


def test_fetch_spy_prices_raises_on_network_error() -> None:
    with (
        patch(
            "portfolio_manager.data_client.requests.get",
            side_effect=requests.RequestException("Connection refused"),
        ),
        pytest.raises(PriceDataUnavailableError),
    ):
        fetch_spy_prices("http://localhost", datetime(2024, 1, 1, tzinfo=UTC))
