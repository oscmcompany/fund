from unittest.mock import MagicMock

from portfolio_manager.alpaca_client import AlpacaAccount, AlpacaClient
from portfolio_manager.configuration import Configuration
from portfolio_manager.enums import TradeSide
from portfolio_manager.exceptions import (
    AssetNotShortableError,
    InsufficientBuyingPowerError,
)
from portfolio_manager.trade_execution import (
    execute_close_positions,
    execute_open_positions,
)

_DEFAULT_CONFIG = Configuration()


def _make_mock_client() -> MagicMock:
    client = MagicMock(spec=AlpacaClient)
    client.get_account.return_value = AlpacaAccount(
        cash_amount=10000.0, buying_power=20000.0, equity=50000.0
    )
    return client


def _long(
    ticker: str = "AAPL",
    dollar_amount: float = 1000.0,
    entry_price: float = 100.0,
) -> dict:
    return {
        "ticker": ticker,
        "side": TradeSide.BUY,
        "dollar_amount": dollar_amount,
        "entry_price": entry_price,
        "quantity": None,
        "notional": dollar_amount,
    }


def _short(
    ticker: str = "MSFT",
    dollar_amount: float = 1000.0,
    entry_price: float = 100.0,
    quantity: int | None = 10,
) -> dict:
    return {
        "ticker": ticker,
        "side": TradeSide.SELL,
        "dollar_amount": dollar_amount,
        "entry_price": entry_price,
        "quantity": quantity,
        "notional": None,
    }


# --- execute_close_positions ---


def test_execute_close_positions_returns_success_when_position_closed() -> None:
    client = _make_mock_client()
    client.close_position.return_value = True

    results, count = execute_close_positions(client, [{"ticker": "AAPL"}])

    assert count == 1
    assert results[0]["status"] == "success"
    assert results[0]["action"] == "close"
    assert results[0]["ticker"] == "AAPL"


def test_execute_close_positions_returns_skipped_when_position_not_found() -> None:
    client = _make_mock_client()
    client.close_position.return_value = False

    results, count = execute_close_positions(client, [{"ticker": "AAPL"}])

    assert count == 0
    assert results[0]["status"] == "skipped"
    assert results[0]["reason"] == "position_not_found"


def test_execute_close_positions_returns_failed_on_exception() -> None:
    client = _make_mock_client()
    client.close_position.side_effect = RuntimeError("network error")

    results, count = execute_close_positions(client, [{"ticker": "AAPL"}])

    assert count == 0
    assert results[0]["status"] == "failed"
    assert "network error" in results[0]["error"]


def test_execute_close_positions_returns_empty_for_no_positions() -> None:
    client = _make_mock_client()
    results, count = execute_close_positions(client, [])
    assert results == []
    assert count == 0


def test_execute_close_positions_handles_multiple_positions() -> None:
    client = _make_mock_client()
    client.close_position.side_effect = [True, False, RuntimeError("err")]

    results, count = execute_close_positions(
        client,
        [{"ticker": "AAPL"}, {"ticker": "MSFT"}, {"ticker": "GOOG"}],
    )

    assert count == 1
    assert results[0]["status"] == "success"
    assert results[1]["status"] == "skipped"
    assert results[2]["status"] == "failed"


# --- execute_open_positions ---


def test_execute_open_positions_opens_long_successfully() -> None:
    client = _make_mock_client()
    results, count = execute_open_positions(
        client, [_long()], 10000.0, 50000.0, _DEFAULT_CONFIG
    )
    assert count == 1
    assert results[0]["status"] == "success"
    assert results[0]["side"] == TradeSide.BUY


def test_execute_open_positions_refreshes_buying_power_after_open() -> None:
    client = _make_mock_client()
    client.get_account.return_value = AlpacaAccount(
        cash_amount=9000.0, buying_power=18000.0, equity=50000.0
    )
    execute_open_positions(client, [_long()], 10000.0, 50000.0, _DEFAULT_CONFIG)
    client.get_account.assert_called_once()


def test_execute_open_positions_uses_buying_power_estimate_when_refresh_fails() -> None:
    client = _make_mock_client()
    client.get_account.side_effect = RuntimeError("account unavailable")

    results, count = execute_open_positions(
        client, [_long(dollar_amount=500.0)], 10000.0, 50000.0, _DEFAULT_CONFIG
    )

    assert count == 1
    assert results[0]["status"] == "success"


def test_execute_open_positions_skips_long_when_insufficient_buying_power() -> None:
    client = _make_mock_client()
    results, count = execute_open_positions(
        client, [_long(dollar_amount=5000.0)], 100.0, 50000.0, _DEFAULT_CONFIG
    )
    assert count == 0
    assert results[0]["status"] == "skipped"
    assert results[0]["reason"] == "insufficient_buying_power"


def test_execute_open_positions_handles_insufficient_buying_power_error() -> None:
    client = _make_mock_client()
    client.open_position.side_effect = InsufficientBuyingPowerError("not enough")

    results, count = execute_open_positions(
        client, [_long()], 10000.0, 50000.0, _DEFAULT_CONFIG
    )

    assert count == 0
    assert results[0]["status"] == "skipped"
    assert results[0]["reason"] == "insufficient_buying_power"


def test_execute_open_positions_handles_generic_exception() -> None:
    client = _make_mock_client()
    client.open_position.side_effect = RuntimeError("unexpected error")

    results, count = execute_open_positions(
        client, [_long()], 10000.0, 50000.0, _DEFAULT_CONFIG
    )

    assert count == 0
    assert results[0]["status"] == "failed"
    assert "unexpected error" in results[0]["error"]


def test_execute_open_positions_opens_short_overnight_standard_rate() -> None:
    config = Configuration(
        hold_overnight=True,
        short_buying_power_buffer=1.03,
        overnight_margin_rate_standard=0.30,
        low_price_threshold=5.0,
    )
    client = _make_mock_client()
    # entry_price=100.0 >= low_price_threshold=5.0 → standard rate
    # buying_power_cost = 10 * 100 * (1.03 + 0.30) = 1330
    results, count = execute_open_positions(
        client, [_short(entry_price=100.0, quantity=10)], 5000.0, 50000.0, config
    )
    assert count == 1
    assert results[0]["status"] == "success"


def test_execute_open_positions_opens_short_overnight_low_price_rate() -> None:
    config = Configuration(
        hold_overnight=True,
        short_buying_power_buffer=1.03,
        overnight_margin_rate_low_price=1.00,
        low_price_threshold=5.0,
    )
    client = _make_mock_client()
    # entry_price=3.0 < low_price_threshold=5.0 → low price rate
    # buying_power_cost = 100 * 3.0 * (1.03 + 1.00) = 609
    results, count = execute_open_positions(
        client,
        [_short(entry_price=3.0, dollar_amount=300.0, quantity=100)],
        10000.0,
        50000.0,
        config,
    )
    assert count == 1
    assert results[0]["status"] == "success"


def test_execute_open_positions_opens_short_intraday_no_margin() -> None:
    config = Configuration(hold_overnight=False, short_buying_power_buffer=1.03)
    client = _make_mock_client()
    # buying_power_cost = 10 * 100 * 1.03 = 1030
    results, count = execute_open_positions(
        client, [_short(entry_price=100.0, quantity=10)], 2000.0, 50000.0, config
    )
    assert count == 1
    assert results[0]["status"] == "success"


def test_execute_open_positions_skips_short_with_insufficient_equity() -> None:
    config = Configuration(minimum_short_equity=2000.0)
    client = _make_mock_client()

    results, count = execute_open_positions(client, [_short()], 10000.0, 500.0, config)

    assert count == 0
    assert results[0]["status"] == "skipped"
    assert results[0]["reason"] == "insufficient_equity_for_short"


def test_execute_open_positions_skips_short_with_zero_quantity() -> None:
    client = _make_mock_client()
    # quantity=None, dollar_amount=50 < entry_price=100 → int(0.5) = 0
    results, count = execute_open_positions(
        client,
        [_short(dollar_amount=50.0, entry_price=100.0, quantity=None)],
        10000.0,
        50000.0,
        _DEFAULT_CONFIG,
    )
    assert count == 0
    assert results[0]["status"] == "skipped"
    assert results[0]["reason"] == "zero_short_quantity"


def test_execute_open_positions_uses_precomputed_quantity_for_short() -> None:
    client = _make_mock_client()
    results, count = execute_open_positions(
        client,
        [_short(quantity=5, dollar_amount=500.0, entry_price=100.0)],
        10000.0,
        50000.0,
        _DEFAULT_CONFIG,
    )
    assert count == 1
    assert results[0]["status"] == "success"


def test_execute_open_positions_skips_short_when_insufficient_buying_power() -> None:
    config = Configuration(hold_overnight=False, short_buying_power_buffer=1.03)
    client = _make_mock_client()
    # buying_power_cost = 10 * 100 * 1.03 = 1030 > initial=100
    results, count = execute_open_positions(
        client,
        [_short(entry_price=100.0, quantity=10)],
        100.0,
        50000.0,
        config,
    )
    assert count == 0
    assert results[0]["status"] == "skipped"
    assert results[0]["reason"] == "insufficient_buying_power"


def test_execute_open_positions_handles_asset_not_shortable_error() -> None:
    client = _make_mock_client()
    client.open_position.side_effect = AssetNotShortableError("not shortable")

    results, count = execute_open_positions(
        client, [_short(quantity=10)], 10000.0, 50000.0, _DEFAULT_CONFIG
    )

    assert count == 0
    assert results[0]["status"] == "skipped"
    assert results[0]["reason"] == "not_shortable"


def test_execute_open_positions_submits_longs_before_shorts() -> None:
    client = _make_mock_client()
    submitted_sides: list[TradeSide] = []

    def capture_side(
        ticker: str,  # noqa: ARG001
        side: TradeSide,
        dollar_amount: float,  # noqa: ARG001
        entry_price: float,  # noqa: ARG001
        quantity: int | None = None,  # noqa: ARG001
    ) -> None:
        submitted_sides.append(side)

    client.open_position.side_effect = capture_side

    positions = [_short(ticker="MSFT", quantity=10), _long(ticker="AAPL")]
    execute_open_positions(client, positions, 10000.0, 50000.0, _DEFAULT_CONFIG)

    assert submitted_sides[0] == TradeSide.BUY
    assert submitted_sides[1] == TradeSide.SELL


def test_execute_open_positions_returns_empty_for_no_positions() -> None:
    client = _make_mock_client()
    results, count = execute_open_positions(
        client, [], 10000.0, 50000.0, _DEFAULT_CONFIG
    )
    assert results == []
    assert count == 0
