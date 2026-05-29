from unittest.mock import MagicMock, call, patch

import pytest
from alpaca.trading.enums import AssetClass, AssetStatus, OrderSide, PositionIntent
from alpaca.trading.requests import GetAssetsRequest
from portfolio_manager.alpaca_client import (
    AlpacaAccount,
    AlpacaClient,
    _is_transient_error,
)
from portfolio_manager.enums import TradeSide
from portfolio_manager.exceptions import (
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
    trading_patch = patch("portfolio_manager.alpaca_client.TradingClient")
    data_patch = patch("portfolio_manager.alpaca_client.StockHistoricalDataClient")
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
EXPECTED_EQUITY = 3000.0


@patch("portfolio_manager.alpaca_client.time.sleep")
def test_is_market_open_returns_true_when_market_is_open(
    mock_sleep: MagicMock,
) -> None:
    client, mock_trading = _make_client()
    mock_clock = MagicMock()
    mock_clock.is_open = True
    mock_trading.get_clock.return_value = mock_clock

    result = client.is_market_open()

    assert result is True
    mock_sleep.assert_called_once_with(client.rate_limit_sleep)


@patch("portfolio_manager.alpaca_client.time.sleep")
def test_is_market_open_returns_false_when_market_is_closed(
    mock_sleep: MagicMock,
) -> None:
    client, mock_trading = _make_client()
    mock_clock = MagicMock()
    mock_clock.is_open = False
    mock_trading.get_clock.return_value = mock_clock

    result = client.is_market_open()

    assert result is False
    mock_sleep.assert_called_once_with(client.rate_limit_sleep)


def test_alpaca_account_stores_values() -> None:
    account = AlpacaAccount(
        cash_amount=EXPECTED_CASH,
        buying_power=EXPECTED_BUYING_POWER,
        equity=EXPECTED_EQUITY,
    )

    assert account.cash_amount == EXPECTED_CASH
    assert account.buying_power == EXPECTED_BUYING_POWER
    assert account.equity == EXPECTED_EQUITY


@patch("portfolio_manager.alpaca_client.time.sleep")
def test_get_account_returns_account(mock_sleep: MagicMock) -> None:
    client, mock_trading = _make_client()
    mock_account = MagicMock()
    expected_cash_amount = 5000.0
    expected_account_buying_power = 10000.0
    expected_account_equity = 15000.0
    mock_account.cash = "5000.00"
    mock_account.buying_power = "10000.00"
    mock_account.equity = "15000.00"
    mock_trading.get_account.return_value = mock_account

    result = client.get_account()

    assert result.cash_amount == expected_cash_amount
    assert result.buying_power == expected_account_buying_power
    assert result.equity == expected_account_equity
    mock_sleep.assert_called_once_with(client.rate_limit_sleep)


@patch("portfolio_manager.alpaca_client.time.sleep")
def test_open_position_buy_submits_order(mock_sleep: MagicMock) -> None:
    client, mock_trading = _make_client()

    client.open_position(
        ticker="aapl", side=TradeSide.BUY, dollar_amount=500.0, entry_price=50.0
    )

    mock_trading.submit_order.assert_called_once()
    mock_sleep.assert_called_once_with(client.rate_limit_sleep)


_EXPECTED_BUY_NOTIONAL = 500.0


@patch("portfolio_manager.alpaca_client.time.sleep")
def test_open_position_buy_uses_notional(mock_sleep: MagicMock) -> None:
    client, mock_trading = _make_client()

    client.open_position(
        ticker="AAPL",
        side=TradeSide.BUY,
        dollar_amount=_EXPECTED_BUY_NOTIONAL,
        entry_price=50.0,
    )

    submitted = mock_trading.submit_order.call_args
    order_request = submitted[1]["order_data"] if submitted[1] else submitted[0][0]
    assert order_request.side == OrderSide.BUY
    assert order_request.notional == _EXPECTED_BUY_NOTIONAL
    assert not hasattr(order_request, "qty") or order_request.qty is None
    mock_sleep.assert_called_once_with(client.rate_limit_sleep)


@patch("portfolio_manager.alpaca_client.time.sleep")
def test_open_position_sell_submits_order(mock_sleep: MagicMock) -> None:
    client, mock_trading = _make_client()

    client.open_position(
        ticker="aapl", side=TradeSide.SELL, dollar_amount=500.0, entry_price=50.0
    )

    mock_trading.submit_order.assert_called_once()
    mock_sleep.assert_called_once_with(client.rate_limit_sleep)


_EXPECTED_SELL_QUANTITY = 10  # int(500.0 / 50.0)


@patch("portfolio_manager.alpaca_client.time.sleep")
def test_open_position_sell_uses_qty_not_notional(mock_sleep: MagicMock) -> None:
    client, mock_trading = _make_client()

    client.open_position(
        ticker="AAPL",
        side=TradeSide.SELL,
        dollar_amount=500.0,
        entry_price=50.0,
        quantity=_EXPECTED_SELL_QUANTITY,
    )

    submitted = mock_trading.submit_order.call_args
    order_request = submitted[1]["order_data"] if submitted[1] else submitted[0][0]
    assert order_request.side == OrderSide.SELL
    assert order_request.qty == _EXPECTED_SELL_QUANTITY
    assert not hasattr(order_request, "notional") or order_request.notional is None
    mock_sleep.assert_called_once_with(client.rate_limit_sleep)


@patch("portfolio_manager.alpaca_client.time.sleep")
def test_open_position_buy_includes_position_intent_buy_to_open(
    mock_sleep: MagicMock,
) -> None:
    client, mock_trading = _make_client()

    client.open_position(
        ticker="AAPL", side=TradeSide.BUY, dollar_amount=500.0, entry_price=50.0
    )

    submitted = mock_trading.submit_order.call_args
    order_request = submitted[1]["order_data"] if submitted[1] else submitted[0][0]
    assert order_request.position_intent == PositionIntent.BUY_TO_OPEN
    mock_sleep.assert_called_once_with(client.rate_limit_sleep)


@patch("portfolio_manager.alpaca_client.time.sleep")
def test_open_position_sell_includes_position_intent_sell_to_open(
    mock_sleep: MagicMock,
) -> None:
    client, mock_trading = _make_client()

    client.open_position(
        ticker="AAPL",
        side=TradeSide.SELL,
        dollar_amount=500.0,
        entry_price=50.0,
        quantity=10,
    )

    submitted = mock_trading.submit_order.call_args
    order_request = submitted[1]["order_data"] if submitted[1] else submitted[0][0]
    assert order_request.position_intent == PositionIntent.SELL_TO_OPEN
    mock_sleep.assert_called_once_with(client.rate_limit_sleep)


def test_open_position_sell_raises_value_error_for_zero_qty() -> None:
    client, _ = _make_client()

    # dollar_amount < entry_price means qty would be 0
    with pytest.raises(ValueError, match="less than one share"):
        client.open_position(
            ticker="AAPL", side=TradeSide.SELL, dollar_amount=10.0, entry_price=100.0
        )


def test_open_position_raises_value_error_for_zero_amount() -> None:
    client, _ = _make_client()

    with pytest.raises(ValueError, match="non-positive dollar_amount"):
        client.open_position(
            ticker="AAPL", side=TradeSide.BUY, dollar_amount=0.0, entry_price=50.0
        )


def test_open_position_raises_value_error_for_negative_amount() -> None:
    client, _ = _make_client()

    with pytest.raises(ValueError, match="non-positive dollar_amount"):
        client.open_position(
            ticker="AAPL", side=TradeSide.BUY, dollar_amount=-100.0, entry_price=50.0
        )


def test_open_position_raises_value_error_for_zero_entry_price() -> None:
    client, _ = _make_client()

    with pytest.raises(ValueError, match="entry_price must be positive"):
        client.open_position(
            ticker="AAPL", side=TradeSide.SELL, dollar_amount=100.0, entry_price=0.0
        )


def test_open_position_raises_value_error_for_negative_entry_price() -> None:
    client, _ = _make_client()

    with pytest.raises(ValueError, match="entry_price must be positive"):
        client.open_position(
            ticker="AAPL", side=TradeSide.BUY, dollar_amount=100.0, entry_price=-50.0
        )


@patch("portfolio_manager.alpaca_client.time.sleep")
def test_get_open_positions_returns_position_list(mock_sleep: MagicMock) -> None:
    client, mock_trading = _make_client()
    expected_quantity = 10.0
    expected_market_value = 1500.0
    expected_unrealized_profit_and_loss = 50.0
    mock_position = MagicMock()
    mock_position.symbol = "AAPL"
    mock_position.side = "long"
    mock_position.qty = str(int(expected_quantity))
    mock_position.market_value = str(expected_market_value)
    mock_position.unrealized_pl = str(expected_unrealized_profit_and_loss)
    mock_trading.get_all_positions.return_value = [mock_position]

    result = client.get_open_positions()

    assert len(result) == 1
    assert result[0]["ticker"] == "AAPL"
    assert result[0]["side"] == "long"
    assert result[0]["quantity"] == expected_quantity
    assert result[0]["market_value"] == expected_market_value
    assert result[0]["unrealized_profit_and_loss"] == (
        expected_unrealized_profit_and_loss
    )
    mock_sleep.assert_called_once_with(client.rate_limit_sleep)


@patch("portfolio_manager.alpaca_client.APIError", _FakeAPIError)
def test_open_position_raises_insufficient_buying_power_error() -> None:
    client, mock_trading = _make_client()
    mock_trading.submit_order.side_effect = _FakeAPIError("insufficient buying power")

    with pytest.raises(InsufficientBuyingPowerError):
        client.open_position(
            ticker="AAPL", side=TradeSide.BUY, dollar_amount=500.0, entry_price=50.0
        )


@patch("portfolio_manager.alpaca_client.APIError", _FakeAPIError)
def test_open_position_raises_insufficient_buying_power_error_on_buying_power_keyword() -> (  # noqa: E501
    None
):
    client, mock_trading = _make_client()
    mock_trading.submit_order.side_effect = _FakeAPIError("buying_power exceeded")

    with pytest.raises(InsufficientBuyingPowerError):
        client.open_position(
            ticker="AAPL", side=TradeSide.BUY, dollar_amount=500.0, entry_price=50.0
        )


@patch("portfolio_manager.alpaca_client.APIError", _FakeAPIError)
def test_open_position_raises_asset_not_shortable_error_on_cannot_be_sold_short() -> (
    None
):
    client, mock_trading = _make_client()
    mock_trading.submit_order.side_effect = _FakeAPIError("cannot be sold short")

    with pytest.raises(AssetNotShortableError):
        client.open_position(
            ticker="AAPL", side=TradeSide.SELL, dollar_amount=500.0, entry_price=50.0
        )


@patch("portfolio_manager.alpaca_client.APIError", _FakeAPIError)
def test_open_position_raises_asset_not_shortable_error_on_not_shortable_keyword() -> (
    None
):
    client, mock_trading = _make_client()
    mock_trading.submit_order.side_effect = _FakeAPIError("asset not shortable")

    with pytest.raises(AssetNotShortableError):
        client.open_position(
            ticker="AAPL", side=TradeSide.SELL, dollar_amount=500.0, entry_price=50.0
        )


@patch("portfolio_manager.alpaca_client.APIError", _FakeAPIError)
def test_open_position_raises_asset_not_shortable_error_on_not_allowed_to_short() -> (
    None
):
    client, mock_trading = _make_client()
    mock_trading.submit_order.side_effect = _FakeAPIError(
        "account is not allowed to short"
    )

    with pytest.raises(AssetNotShortableError):
        client.open_position(
            ticker="AAPL", side=TradeSide.SELL, dollar_amount=500.0, entry_price=50.0
        )


@patch("portfolio_manager.alpaca_client.APIError", _FakeAPIError)
def test_open_position_reraises_other_api_errors() -> None:
    client, mock_trading = _make_client()
    mock_trading.submit_order.side_effect = _FakeAPIError("some unhandled error")

    with pytest.raises(_FakeAPIError):
        client.open_position(
            ticker="AAPL", side=TradeSide.BUY, dollar_amount=500.0, entry_price=50.0
        )


@patch("portfolio_manager.alpaca_client.time.sleep")
def test_close_position_returns_true_on_success(mock_sleep: MagicMock) -> None:
    client, mock_trading = _make_client()

    result = client.close_position(ticker="aapl")

    assert result is True
    mock_trading.close_position.assert_called_once()
    mock_sleep.assert_called_once_with(client.rate_limit_sleep)


@patch("portfolio_manager.alpaca_client.APIError", _FakeAPIError)
def test_close_position_returns_false_on_404_status_code() -> None:
    client, mock_trading = _make_client()
    mock_trading.close_position.side_effect = _FakeAPIError(
        "not found", status_code=404
    )

    result = client.close_position(ticker="AAPL")

    assert result is False


@patch("portfolio_manager.alpaca_client.APIError", _FakeAPIError)
def test_close_position_returns_false_on_position_not_found_error_code() -> None:
    client, mock_trading = _make_client()
    mock_trading.close_position.side_effect = _FakeAPIError(
        "position error", code="position_not_found"
    )

    result = client.close_position(ticker="AAPL")

    assert result is False


@patch("portfolio_manager.alpaca_client.APIError", _FakeAPIError)
def test_close_position_returns_false_on_position_not_found_in_message() -> None:
    client, mock_trading = _make_client()
    mock_trading.close_position.side_effect = _FakeAPIError("position not found")

    result = client.close_position(ticker="AAPL")

    assert result is False


@patch("portfolio_manager.alpaca_client.APIError", _FakeAPIError)
def test_close_position_returns_false_on_position_does_not_exist_in_message() -> None:
    client, mock_trading = _make_client()
    mock_trading.close_position.side_effect = _FakeAPIError("position does not exist")

    result = client.close_position(ticker="AAPL")

    assert result is False


@patch("portfolio_manager.alpaca_client.APIError", _FakeAPIError)
def test_close_position_falls_back_to_str_when_message_attribute_is_none() -> None:
    client, mock_trading = _make_client()
    error = _FakeAPIError("position not found")
    error.message = None  # force the str(e) fallback branch
    mock_trading.close_position.side_effect = error

    result = client.close_position(ticker="AAPL")

    assert result is False


@patch("portfolio_manager.alpaca_client.APIError", _FakeAPIError)
def test_close_position_reraises_other_api_errors() -> None:
    client, mock_trading = _make_client()
    mock_trading.close_position.side_effect = _FakeAPIError("some unhandled error")

    with pytest.raises(_FakeAPIError):
        client.close_position(ticker="AAPL")


def _make_mock_position(
    symbol: str,
    side: str,
    qty: str,
    market_value: str,
    unrealized_pl: str,
) -> MagicMock:
    position = MagicMock()
    position.symbol = symbol
    position.side = side
    position.qty = qty
    position.market_value = market_value
    position.unrealized_pl = unrealized_pl
    return position


@patch("portfolio_manager.alpaca_client.time.sleep")
def test_get_open_positions_returns_position_dicts(mock_sleep: MagicMock) -> None:
    client, mock_trading = _make_client()
    mock_trading.get_all_positions.return_value = [
        _make_mock_position("AAPL", "long", "10.0", "1500.00", "50.00"),
        _make_mock_position("MSFT", "short", "5.0", "2000.00", "-30.00"),
    ]

    result = client.get_open_positions()

    expected_positions = 2
    assert len(result) == expected_positions
    assert result[0] == {
        "ticker": "AAPL",
        "side": "long",
        "quantity": 10.0,
        "market_value": 1500.0,
        "unrealized_profit_and_loss": 50.0,
    }
    assert result[1] == {
        "ticker": "MSFT",
        "side": "short",
        "quantity": 5.0,
        "market_value": 2000.0,
        "unrealized_profit_and_loss": -30.0,
    }
    mock_sleep.assert_called_once_with(client.rate_limit_sleep)


@patch("portfolio_manager.alpaca_client.time.sleep")
def test_get_open_positions_returns_empty_list_when_no_positions(
    mock_sleep: MagicMock,
) -> None:
    client, mock_trading = _make_client()
    mock_trading.get_all_positions.return_value = []

    result = client.get_open_positions()

    assert result == []
    mock_sleep.assert_called_once_with(client.rate_limit_sleep)


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


def test_get_shortable_tickers_does_not_pass_attributes_to_api() -> None:
    client, mock_trading = _make_client()
    mock_trading.get_all_assets.return_value = [
        _make_mock_asset("AAPL", shortable=True, easy_to_borrow=True),
    ]

    client.get_shortable_tickers(["AAPL"])

    expected_request = GetAssetsRequest(
        asset_class=AssetClass.US_EQUITY,
        status=AssetStatus.ACTIVE,
    )
    mock_trading.get_all_assets.assert_called_once_with(expected_request)


# --- _is_transient_error predicate tests ---


def test_is_transient_error_returns_true_for_429() -> None:
    error = _FakeAPIError("rate limited", status_code=429)
    assert _is_transient_error(error) is True


def test_is_transient_error_returns_true_for_500() -> None:
    error = _FakeAPIError("internal server error", status_code=500)
    assert _is_transient_error(error) is True


def test_is_transient_error_returns_true_for_os_error() -> None:
    error = OSError("connection reset")
    assert _is_transient_error(error) is True


def test_is_transient_error_returns_false_for_permanent_api_error() -> None:
    error = _FakeAPIError("bad request", status_code=400)
    assert _is_transient_error(error) is False


def test_is_transient_error_returns_false_for_non_api_error() -> None:
    error = ValueError("invalid value")
    assert _is_transient_error(error) is False


# --- Retry behavior tests ---


@patch("portfolio_manager.alpaca_client.time.sleep")
@patch("tenacity.nap.time.sleep")
def test_get_account_retries_on_transient_error(
    mock_tenacity_sleep: MagicMock,
    mock_rate_limit_sleep: MagicMock,
) -> None:
    client, mock_trading = _make_client()
    transient_error = _FakeAPIError("server error", status_code=500)
    mock_account = MagicMock()
    mock_account.cash = "5000.00"
    mock_account.buying_power = "10000.00"
    mock_account.equity = "15000.00"
    mock_trading.get_account.side_effect = [transient_error, mock_account]

    result = client.get_account()

    expected_cash = 5000.0
    expected_attempts = 2
    assert result.cash_amount == expected_cash
    assert mock_trading.get_account.call_count == expected_attempts
    assert mock_tenacity_sleep is not None
    assert mock_rate_limit_sleep.called


@patch("portfolio_manager.alpaca_client.APIError", _FakeAPIError)
def test_open_position_does_not_retry_on_transient_error() -> None:
    client, mock_trading = _make_client()
    transient_error = _FakeAPIError("service unavailable", status_code=503)
    mock_trading.submit_order.side_effect = transient_error

    with pytest.raises(_FakeAPIError, match="service unavailable"):
        client.open_position(
            ticker="AAPL", side=TradeSide.BUY, dollar_amount=500.0, entry_price=50.0
        )

    assert mock_trading.submit_order.call_count == 1


@patch("portfolio_manager.alpaca_client.APIError", _FakeAPIError)
@patch("portfolio_manager.alpaca_client.time.sleep")
@patch("tenacity.nap.time.sleep")
def test_close_position_retries_on_transient_error(
    mock_tenacity_sleep: MagicMock,
    mock_rate_limit_sleep: MagicMock,
) -> None:
    client, mock_trading = _make_client()
    transient_error = _FakeAPIError("bad gateway", status_code=502)
    mock_trading.close_position.side_effect = [transient_error, None]

    result = client.close_position(ticker="AAPL")

    expected_attempts = 2
    assert result is True
    assert mock_trading.close_position.call_count == expected_attempts
    assert mock_tenacity_sleep is not None
    assert mock_rate_limit_sleep.called


@patch("portfolio_manager.alpaca_client.time.sleep")
@patch("tenacity.nap.time.sleep")
def test_get_account_raises_after_retries_exhausted(
    mock_tenacity_sleep: MagicMock,
    mock_rate_limit_sleep: MagicMock,
) -> None:
    client, mock_trading = _make_client()
    transient_error = _FakeAPIError("server error", status_code=500)
    mock_trading.get_account.side_effect = transient_error

    with pytest.raises(_FakeAPIError, match="server error"):
        client.get_account()

    expected_attempts = 3
    assert mock_trading.get_account.call_count == expected_attempts
    assert mock_tenacity_sleep is not None
    assert call(client.rate_limit_sleep) not in mock_rate_limit_sleep.call_args_list
