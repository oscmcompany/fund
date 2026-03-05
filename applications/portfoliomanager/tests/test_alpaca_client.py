from unittest.mock import MagicMock, patch

import pytest
from portfoliomanager.alpaca_client import AlpacaAccount, AlpacaClient
from portfoliomanager.enums import TradeSide
from portfoliomanager.exceptions import (
    AssetNotShortableError,
    InsufficientBuyingPowerError,
)


class _FakeAPIError(Exception):
    """Lightweight stand-in for alpaca.common.exceptions.APIError used in tests."""

    def __init__(
        self,
        message: str,
        status_code: int | None = None,
        code: str | None = None,
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.code = code
        self.message: str | None = message


def _make_client() -> tuple[AlpacaClient, MagicMock]:
    trading_patch = patch("portfoliomanager.alpaca_client.TradingClient")
    data_patch = patch("portfoliomanager.alpaca_client.StockHistoricalDataClient")
    mock_trading_cls = trading_patch.start()
    data_patch.start()
    client = AlpacaClient(api_key="test", api_secret="test", is_paper=True)  # noqa: S106
    trading_patch.stop()
    data_patch.stop()
    return client, mock_trading_cls.return_value


def _make_mock_asset(
    symbol: str,
    shortable: bool,  # noqa: FBT001
    easy_to_borrow: bool,  # noqa: FBT001
) -> MagicMock:
    asset = MagicMock()
    asset.symbol = symbol
    asset.shortable = shortable
    asset.easy_to_borrow = easy_to_borrow
    return asset


EXPECTED_CASH = 1000.0
EXPECTED_BUYING_POWER = 2000.0


def test_alpaca_account_stores_values() -> None:
    account = AlpacaAccount(
        cash_amount=EXPECTED_CASH, buying_power=EXPECTED_BUYING_POWER
    )

    assert account.cash_amount == EXPECTED_CASH
    assert account.buying_power == EXPECTED_BUYING_POWER


@patch("portfoliomanager.alpaca_client.time.sleep")
def test_get_account_returns_account(mock_sleep: MagicMock) -> None:
    client, mock_trading = _make_client()
    mock_account = MagicMock()
    expected_cash_amount = 5000.0
    expected_account_buying_power = 10000.0
    mock_account.cash = "5000.00"
    mock_account.buying_power = "10000.00"
    mock_trading.get_account.return_value = mock_account

    result = client.get_account()

    assert result.cash_amount == expected_cash_amount
    assert result.buying_power == expected_account_buying_power
    mock_sleep.assert_called_once_with(client.rate_limit_sleep)


@patch("portfoliomanager.alpaca_client.time.sleep")
def test_open_position_buy_submits_order(mock_sleep: MagicMock) -> None:
    client, mock_trading = _make_client()

    client.open_position(ticker="aapl", side=TradeSide.BUY, dollar_amount=500.0)

    mock_trading.submit_order.assert_called_once()
    mock_sleep.assert_called_once_with(client.rate_limit_sleep)


@patch("portfoliomanager.alpaca_client.time.sleep")
def test_open_position_sell_submits_order(mock_sleep: MagicMock) -> None:
    client, mock_trading = _make_client()

    client.open_position(ticker="aapl", side=TradeSide.SELL, dollar_amount=500.0)

    mock_trading.submit_order.assert_called_once()
    mock_sleep.assert_called_once_with(client.rate_limit_sleep)


def test_open_position_raises_value_error_for_zero_amount() -> None:
    client, _ = _make_client()

    with pytest.raises(ValueError, match="non-positive dollar_amount"):
        client.open_position(ticker="AAPL", side=TradeSide.BUY, dollar_amount=0.0)


def test_open_position_raises_value_error_for_negative_amount() -> None:
    client, _ = _make_client()

    with pytest.raises(ValueError, match="non-positive dollar_amount"):
        client.open_position(ticker="AAPL", side=TradeSide.BUY, dollar_amount=-100.0)


@patch("portfoliomanager.alpaca_client.APIError", _FakeAPIError)
def test_open_position_raises_insufficient_buying_power_error() -> None:
    client, mock_trading = _make_client()
    mock_trading.submit_order.side_effect = _FakeAPIError("insufficient buying power")

    with pytest.raises(InsufficientBuyingPowerError):
        client.open_position(ticker="AAPL", side=TradeSide.BUY, dollar_amount=500.0)


@patch("portfoliomanager.alpaca_client.APIError", _FakeAPIError)
def test_open_position_raises_insufficient_buying_power_error_on_buying_power_keyword() -> (  # noqa: E501
    None
):
    client, mock_trading = _make_client()
    mock_trading.submit_order.side_effect = _FakeAPIError("buying_power exceeded")

    with pytest.raises(InsufficientBuyingPowerError):
        client.open_position(ticker="AAPL", side=TradeSide.BUY, dollar_amount=500.0)


@patch("portfoliomanager.alpaca_client.APIError", _FakeAPIError)
def test_open_position_raises_asset_not_shortable_error_on_cannot_be_sold_short() -> (
    None
):
    client, mock_trading = _make_client()
    mock_trading.submit_order.side_effect = _FakeAPIError("cannot be sold short")

    with pytest.raises(AssetNotShortableError):
        client.open_position(ticker="AAPL", side=TradeSide.SELL, dollar_amount=500.0)


@patch("portfoliomanager.alpaca_client.APIError", _FakeAPIError)
def test_open_position_raises_asset_not_shortable_error_on_not_shortable_keyword() -> (
    None
):
    client, mock_trading = _make_client()
    mock_trading.submit_order.side_effect = _FakeAPIError("asset not shortable")

    with pytest.raises(AssetNotShortableError):
        client.open_position(ticker="AAPL", side=TradeSide.SELL, dollar_amount=500.0)


@patch("portfoliomanager.alpaca_client.APIError", _FakeAPIError)
def test_open_position_reraises_other_api_errors() -> None:
    client, mock_trading = _make_client()
    mock_trading.submit_order.side_effect = _FakeAPIError("some unhandled error")

    with pytest.raises(_FakeAPIError):
        client.open_position(ticker="AAPL", side=TradeSide.BUY, dollar_amount=500.0)


@patch("portfoliomanager.alpaca_client.time.sleep")
def test_close_position_returns_true_on_success(mock_sleep: MagicMock) -> None:
    client, mock_trading = _make_client()

    result = client.close_position(ticker="aapl")

    assert result is True
    mock_trading.close_position.assert_called_once()
    mock_sleep.assert_called_once_with(client.rate_limit_sleep)


@patch("portfoliomanager.alpaca_client.APIError", _FakeAPIError)
def test_close_position_returns_false_on_404_status_code() -> None:
    client, mock_trading = _make_client()
    mock_trading.close_position.side_effect = _FakeAPIError(
        "not found", status_code=404
    )

    result = client.close_position(ticker="AAPL")

    assert result is False


@patch("portfoliomanager.alpaca_client.APIError", _FakeAPIError)
def test_close_position_returns_false_on_position_not_found_error_code() -> None:
    client, mock_trading = _make_client()
    mock_trading.close_position.side_effect = _FakeAPIError(
        "position error", code="position_not_found"
    )

    result = client.close_position(ticker="AAPL")

    assert result is False


@patch("portfoliomanager.alpaca_client.APIError", _FakeAPIError)
def test_close_position_returns_false_on_position_not_found_in_message() -> None:
    client, mock_trading = _make_client()
    mock_trading.close_position.side_effect = _FakeAPIError("position not found")

    result = client.close_position(ticker="AAPL")

    assert result is False


@patch("portfoliomanager.alpaca_client.APIError", _FakeAPIError)
def test_close_position_returns_false_on_position_does_not_exist_in_message() -> None:
    client, mock_trading = _make_client()
    mock_trading.close_position.side_effect = _FakeAPIError("position does not exist")

    result = client.close_position(ticker="AAPL")

    assert result is False


@patch("portfoliomanager.alpaca_client.APIError", _FakeAPIError)
def test_close_position_falls_back_to_str_when_message_attribute_is_none() -> None:
    client, mock_trading = _make_client()
    error = _FakeAPIError("position not found")
    error.message = None  # force the str(e) fallback branch
    mock_trading.close_position.side_effect = error

    result = client.close_position(ticker="AAPL")

    assert result is False


@patch("portfoliomanager.alpaca_client.APIError", _FakeAPIError)
def test_close_position_reraises_other_api_errors() -> None:
    client, mock_trading = _make_client()
    mock_trading.close_position.side_effect = _FakeAPIError("some unhandled error")

    with pytest.raises(_FakeAPIError):
        client.close_position(ticker="AAPL")


def test_get_shortable_tickers_excludes_non_shortable() -> None:
    client, mock_trading = _make_client()
    mock_trading.get_all_assets.return_value = [
        _make_mock_asset("AAPL", shortable=True, easy_to_borrow=True),
        _make_mock_asset("MSFT", shortable=False, easy_to_borrow=True),
    ]

    result = client.get_shortable_tickers(["AAPL", "MSFT"])

    assert result == {"AAPL"}


def test_get_shortable_tickers_excludes_not_easy_to_borrow() -> None:
    client, mock_trading = _make_client()
    mock_trading.get_all_assets.return_value = [
        _make_mock_asset("AAPL", shortable=True, easy_to_borrow=True),
        _make_mock_asset("TSLA", shortable=True, easy_to_borrow=False),
    ]

    result = client.get_shortable_tickers(["AAPL", "TSLA"])

    assert result == {"AAPL"}


def test_get_shortable_tickers_returns_only_input_tickers() -> None:
    client, mock_trading = _make_client()
    mock_trading.get_all_assets.return_value = [
        _make_mock_asset("AAPL", shortable=True, easy_to_borrow=True),
        _make_mock_asset("MSFT", shortable=True, easy_to_borrow=True),
        _make_mock_asset("GOOG", shortable=True, easy_to_borrow=True),
    ]

    result = client.get_shortable_tickers(["AAPL", "MSFT"])

    assert result == {"AAPL", "MSFT"}
    assert "GOOG" not in result


def test_get_shortable_tickers_returns_empty_set_when_none_pass() -> None:
    client, mock_trading = _make_client()
    mock_trading.get_all_assets.return_value = [
        _make_mock_asset("AAPL", shortable=False, easy_to_borrow=True),
        _make_mock_asset("MSFT", shortable=True, easy_to_borrow=False),
    ]

    result = client.get_shortable_tickers(["AAPL", "MSFT"])

    assert result == set()
