import asyncio
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from portfolio_manager.data_client import (
    fetch_equity_details,
    fetch_historical_prices,
    fetch_spy_prices,
)
from portfolio_manager.exceptions import PriceDataUnavailableError


def _make_pool_mock(rows: list) -> MagicMock:
    mock_result = AsyncMock()
    mock_result.fetchall.return_value = rows
    mock_connection = MagicMock()
    mock_connection.execute = AsyncMock(return_value=mock_result)
    mock_connection.__aenter__ = AsyncMock(return_value=mock_connection)
    mock_connection.__aexit__ = AsyncMock(return_value=None)
    mock_pool = MagicMock()
    mock_pool.connection.return_value = mock_connection
    return mock_pool


# --- fetch_historical_prices ---


def test_fetch_historical_prices_returns_expected_columns() -> None:
    rows = [
        ("AAPL", 1704067200000, 150.0),
        ("MSFT", 1704067200000, 300.0),
    ]
    mock_pool = _make_pool_mock(rows)

    with patch(
        "portfolio_manager.data_client.get_pool", AsyncMock(return_value=mock_pool)
    ):
        result = asyncio.run(fetch_historical_prices(datetime(2024, 1, 2, tzinfo=UTC)))

    assert result.columns == ["ticker", "timestamp", "close_price"]
    assert result.height == 2  # noqa: PLR2004


def test_fetch_historical_prices_drops_null_close_prices() -> None:
    rows = [
        ("AAPL", 1704067200000, 150.0),
        ("MSFT", 1704067200000, None),
        ("GOOG", 1704067200000, 200.0),
    ]
    mock_pool = _make_pool_mock(rows)

    with patch(
        "portfolio_manager.data_client.get_pool", AsyncMock(return_value=mock_pool)
    ):
        result = asyncio.run(fetch_historical_prices(datetime(2024, 1, 2, tzinfo=UTC)))

    assert result.height == 2  # noqa: PLR2004
    assert "MSFT" not in result["ticker"].to_list()


def test_fetch_historical_prices_returns_empty_dataframe_when_no_rows() -> None:
    mock_pool = _make_pool_mock([])

    with patch(
        "portfolio_manager.data_client.get_pool", AsyncMock(return_value=mock_pool)
    ):
        result = asyncio.run(fetch_historical_prices(datetime(2024, 1, 2, tzinfo=UTC)))

    assert result.is_empty()
    assert result.columns == ["ticker", "timestamp", "close_price"]


def test_fetch_historical_prices_deduplicates_ticker_timestamp() -> None:
    rows = [
        ("AAPL", 1704067200000, 149.0),
        ("AAPL", 1704067200000, 150.0),
    ]
    mock_pool = _make_pool_mock(rows)

    with patch(
        "portfolio_manager.data_client.get_pool", AsyncMock(return_value=mock_pool)
    ):
        result = asyncio.run(fetch_historical_prices(datetime(2024, 1, 2, tzinfo=UTC)))

    assert result.height == 1
    assert result["close_price"][0] == pytest.approx(150.0)


def test_fetch_historical_prices_raises_on_db_error() -> None:
    mock_connection = MagicMock()
    mock_connection.execute = AsyncMock(side_effect=RuntimeError("connection refused"))
    mock_connection.__aenter__ = AsyncMock(return_value=mock_connection)
    mock_connection.__aexit__ = AsyncMock(return_value=None)
    mock_pool = MagicMock()
    mock_pool.connection.return_value = mock_connection

    with (
        patch(
            "portfolio_manager.data_client.get_pool",
            AsyncMock(return_value=mock_pool),
        ),
        pytest.raises(PriceDataUnavailableError),
    ):
        asyncio.run(fetch_historical_prices(datetime(2024, 1, 1, tzinfo=UTC)))


def test_fetch_historical_prices_filters_by_tickers_when_provided() -> None:
    rows = [
        ("AAPL", 1704067200000, 150.0),
        ("MSFT", 1704067200000, 300.0),
    ]
    mock_pool = _make_pool_mock(rows)

    with patch(
        "portfolio_manager.data_client.get_pool", AsyncMock(return_value=mock_pool)
    ):
        result = asyncio.run(
            fetch_historical_prices(
                datetime(2024, 1, 2, tzinfo=UTC),
                tickers=["AAPL", "MSFT"],
            )
        )

    assert result.height == 2  # noqa: PLR2004
    call_args = (
        mock_pool.connection.return_value.__aenter__.return_value.execute.call_args
    )
    assert "ANY(%s)" in call_args[0][0]


def test_fetch_historical_prices_accepts_datamanager_base_url_shim() -> None:
    mock_pool = _make_pool_mock([])

    with patch(
        "portfolio_manager.data_client.get_pool", AsyncMock(return_value=mock_pool)
    ):
        result = asyncio.run(
            fetch_historical_prices(
                datetime(2024, 1, 2, tzinfo=UTC),
                datamanager_base_url="http://ignored",
            )
        )

    assert result.is_empty()


# --- fetch_equity_details ---


def test_fetch_equity_details_returns_expected_columns() -> None:
    rows = [
        ("AAPL", "Technology"),
        ("MSFT", "Technology"),
    ]
    mock_pool = _make_pool_mock(rows)

    with patch(
        "portfolio_manager.data_client.get_pool", AsyncMock(return_value=mock_pool)
    ):
        result = asyncio.run(fetch_equity_details())

    assert result.columns == ["ticker", "sector"]
    assert result.height == 2  # noqa: PLR2004


def test_fetch_equity_details_returns_empty_dataframe_when_no_rows() -> None:
    mock_pool = _make_pool_mock([])

    with patch(
        "portfolio_manager.data_client.get_pool", AsyncMock(return_value=mock_pool)
    ):
        result = asyncio.run(fetch_equity_details())

    assert result.is_empty()
    assert result.columns == ["ticker", "sector"]


def test_fetch_equity_details_raises_on_db_error() -> None:
    mock_connection = MagicMock()
    mock_connection.execute = AsyncMock(side_effect=RuntimeError("db error"))
    mock_connection.__aenter__ = AsyncMock(return_value=mock_connection)
    mock_connection.__aexit__ = AsyncMock(return_value=None)
    mock_pool = MagicMock()
    mock_pool.connection.return_value = mock_connection

    with (
        patch(
            "portfolio_manager.data_client.get_pool",
            AsyncMock(return_value=mock_pool),
        ),
        pytest.raises(PriceDataUnavailableError),
    ):
        asyncio.run(fetch_equity_details())


def test_fetch_equity_details_accepts_datamanager_base_url_shim() -> None:
    mock_pool = _make_pool_mock([])

    with patch(
        "portfolio_manager.data_client.get_pool", AsyncMock(return_value=mock_pool)
    ):
        result = asyncio.run(
            fetch_equity_details(datamanager_base_url="http://ignored")
        )

    assert result.is_empty()


# --- fetch_spy_prices ---


def test_fetch_spy_prices_returns_expected_columns() -> None:
    rows = [
        ("SPY", 1704067200000, 450.0),
        ("SPY", 1704153600000, 452.0),
    ]
    mock_pool = _make_pool_mock(rows)

    with patch(
        "portfolio_manager.data_client.get_pool", AsyncMock(return_value=mock_pool)
    ):
        result = asyncio.run(fetch_spy_prices(datetime(2024, 1, 3, tzinfo=UTC)))

    assert result.columns == ["ticker", "timestamp", "close_price"]
    assert result.height == 2  # noqa: PLR2004


def test_fetch_spy_prices_drops_null_close_prices() -> None:
    rows = [
        ("SPY", 1704067200000, 450.0),
        ("SPY", 1704153600000, None),
    ]
    mock_pool = _make_pool_mock(rows)

    with patch(
        "portfolio_manager.data_client.get_pool", AsyncMock(return_value=mock_pool)
    ):
        result = asyncio.run(fetch_spy_prices(datetime(2024, 1, 3, tzinfo=UTC)))

    assert result.height == 1


def test_fetch_spy_prices_returns_empty_dataframe_when_no_rows() -> None:
    mock_pool = _make_pool_mock([])

    with patch(
        "portfolio_manager.data_client.get_pool", AsyncMock(return_value=mock_pool)
    ):
        result = asyncio.run(fetch_spy_prices(datetime(2024, 1, 3, tzinfo=UTC)))

    assert result.is_empty()
    assert result.columns == ["ticker", "timestamp", "close_price"]


def test_fetch_spy_prices_deduplicates_timestamp() -> None:
    rows = [
        ("SPY", 1704067200000, 449.0),
        ("SPY", 1704067200000, 450.0),
    ]
    mock_pool = _make_pool_mock(rows)

    with patch(
        "portfolio_manager.data_client.get_pool", AsyncMock(return_value=mock_pool)
    ):
        result = asyncio.run(fetch_spy_prices(datetime(2024, 1, 3, tzinfo=UTC)))

    assert result.height == 1
    assert result["close_price"][0] == pytest.approx(450.0)


def test_fetch_spy_prices_raises_on_db_error() -> None:
    mock_connection = MagicMock()
    mock_connection.execute = AsyncMock(side_effect=RuntimeError("timeout"))
    mock_connection.__aenter__ = AsyncMock(return_value=mock_connection)
    mock_connection.__aexit__ = AsyncMock(return_value=None)
    mock_pool = MagicMock()
    mock_pool.connection.return_value = mock_connection

    with (
        patch(
            "portfolio_manager.data_client.get_pool",
            AsyncMock(return_value=mock_pool),
        ),
        pytest.raises(PriceDataUnavailableError),
    ):
        asyncio.run(fetch_spy_prices(datetime(2024, 1, 1, tzinfo=UTC)))


def test_fetch_spy_prices_accepts_datamanager_base_url_shim() -> None:
    mock_pool = _make_pool_mock([])

    with patch(
        "portfolio_manager.data_client.get_pool", AsyncMock(return_value=mock_pool)
    ):
        result = asyncio.run(
            fetch_spy_prices(
                datetime(2024, 1, 3, tzinfo=UTC),
                datamanager_base_url="http://ignored",
            )
        )

    assert result.is_empty()
