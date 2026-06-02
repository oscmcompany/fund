from unittest.mock import MagicMock, patch

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
    pair_id: str = "pair-1",
) -> dict:
    return {
        "ticker": ticker,
        "pair_id": pair_id,
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
    pair_id: str = "pair-1",
) -> dict:
    return {
        "ticker": ticker,
        "pair_id": pair_id,
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


def test_execute_open_positions_opens_pair_successfully() -> None:
    client = _make_mock_client()
    positions = [_long(ticker="AAPL"), _short(ticker="MSFT", quantity=10)]
    results, count = execute_open_positions(
        client, positions, 10000.0, 50000.0, _DEFAULT_CONFIG
    )
    assert count == 2  # noqa: PLR2004
    assert results[0]["status"] == "success"
    assert results[0]["side"] == TradeSide.BUY
    assert results[1]["status"] == "success"
    assert results[1]["side"] == TradeSide.SELL


def test_execute_open_positions_refreshes_buying_power_after_each_leg() -> None:
    client = _make_mock_client()
    client.get_account.return_value = AlpacaAccount(
        cash_amount=9000.0, buying_power=18000.0, equity=50000.0
    )
    positions = [_long(ticker="AAPL"), _short(ticker="MSFT", quantity=10)]
    execute_open_positions(client, positions, 10000.0, 50000.0, _DEFAULT_CONFIG)
    # get_account called once per successful leg
    assert client.get_account.call_count == 2  # noqa: PLR2004


def test_execute_open_positions_uses_buying_power_estimate_when_refresh_fails() -> None:
    client = _make_mock_client()
    client.get_account.side_effect = RuntimeError("account unavailable")

    positions = [
        _long(ticker="AAPL", dollar_amount=500.0),
        _short(ticker="MSFT", quantity=10),
    ]
    results, count = execute_open_positions(
        client, positions, 10000.0, 50000.0, _DEFAULT_CONFIG
    )

    assert count == 2  # noqa: PLR2004
    assert results[0]["status"] == "success"
    assert results[1]["status"] == "success"


def test_execute_open_positions_skips_long_when_insufficient_buying_power() -> None:
    client = _make_mock_client()
    positions = [
        _long(ticker="AAPL", dollar_amount=5000.0),
        _short(ticker="MSFT", quantity=10),
    ]
    results, count = execute_open_positions(
        client, positions, 100.0, 50000.0, _DEFAULT_CONFIG
    )
    assert count == 0
    assert results[0]["status"] == "skipped"
    assert results[0]["reason"] == "insufficient_buying_power"
    # Short is skipped as a consequence of long failing
    assert results[1]["status"] == "skipped"
    assert results[1]["reason"] == "long_leg_failed"


def test_execute_open_positions_handles_insufficient_buying_power_error() -> None:
    client = _make_mock_client()
    # Long raises the error; short should not be attempted
    client.open_position.side_effect = [
        InsufficientBuyingPowerError("not enough"),
        None,
    ]

    positions = [_long(ticker="AAPL"), _short(ticker="MSFT", quantity=10)]
    results, count = execute_open_positions(
        client, positions, 10000.0, 50000.0, _DEFAULT_CONFIG
    )

    assert count == 0
    assert results[0]["status"] == "skipped"
    assert results[0]["reason"] == "insufficient_buying_power"
    assert results[1]["status"] == "skipped"
    assert results[1]["reason"] == "long_leg_failed"
    client.open_position.assert_called_once()


def test_execute_open_positions_handles_generic_exception_on_long() -> None:
    client = _make_mock_client()
    client.open_position.side_effect = [RuntimeError("unexpected error"), None]

    positions = [_long(ticker="AAPL"), _short(ticker="MSFT", quantity=10)]
    results, count = execute_open_positions(
        client, positions, 10000.0, 50000.0, _DEFAULT_CONFIG
    )

    assert count == 0
    assert results[0]["status"] == "failed"
    assert "unexpected error" in results[0]["error"]
    assert results[1]["status"] == "skipped"
    assert results[1]["reason"] == "long_leg_failed"
    client.open_position.assert_called_once()


def test_execute_open_positions_handles_generic_exception_on_short() -> None:
    client = _make_mock_client()
    client.open_position.side_effect = [None, RuntimeError("unexpected error")]

    positions = [_long(ticker="AAPL"), _short(ticker="MSFT", quantity=10)]
    results, count = execute_open_positions(
        client, positions, 10000.0, 50000.0, _DEFAULT_CONFIG
    )

    assert count == 1
    assert results[0]["status"] == "success"
    assert results[1]["status"] == "failed"
    assert "unexpected error" in results[1]["error"]


def test_execute_open_positions_opens_short_with_buffer_cost() -> None:
    config = Configuration(short_buying_power_buffer=1.03)
    client = _make_mock_client()
    # short buying_power_cost = 10 * 100 * 1.03 = 1030
    positions = [
        _long(ticker="AAPL", dollar_amount=1000.0),
        _short(ticker="MSFT", entry_price=100.0, quantity=10),
    ]
    results, count = execute_open_positions(client, positions, 5000.0, 50000.0, config)
    assert count == 2  # noqa: PLR2004
    assert results[1]["status"] == "success"
    assert results[1]["side"] == TradeSide.SELL


def test_execute_open_positions_skips_short_with_insufficient_equity() -> None:
    config = Configuration(minimum_short_equity=2000.0)
    client = _make_mock_client()

    positions = [_long(ticker="AAPL"), _short(ticker="MSFT")]
    results, count = execute_open_positions(client, positions, 10000.0, 500.0, config)

    assert count == 1  # long succeeded
    assert results[0]["status"] == "success"
    assert results[1]["status"] == "skipped"
    assert results[1]["reason"] == "insufficient_equity_for_short"


def test_execute_open_positions_skips_short_with_zero_quantity() -> None:
    client = _make_mock_client()
    # quantity=None, dollar_amount=50 < entry_price=100 → int(0.5) = 0
    positions = [
        _long(ticker="AAPL"),
        _short(ticker="MSFT", dollar_amount=50.0, entry_price=100.0, quantity=None),
    ]
    results, count = execute_open_positions(
        client, positions, 10000.0, 50000.0, _DEFAULT_CONFIG
    )
    assert count == 1  # long succeeded
    assert results[0]["status"] == "success"
    assert results[1]["status"] == "skipped"
    assert results[1]["reason"] == "zero_short_quantity"


def test_execute_open_positions_uses_precomputed_quantity_for_short() -> None:
    client = _make_mock_client()
    positions = [
        _long(ticker="AAPL"),
        _short(ticker="MSFT", quantity=5, dollar_amount=500.0, entry_price=100.0),
    ]
    results, count = execute_open_positions(
        client, positions, 10000.0, 50000.0, _DEFAULT_CONFIG
    )
    assert count == 2  # noqa: PLR2004
    assert results[1]["status"] == "success"


def test_execute_open_positions_skips_short_when_insufficient_buying_power() -> None:
    config = Configuration(short_buying_power_buffer=1.03)
    client = _make_mock_client()
    # short buying_power_cost = 10 * 100 * 1.03 = 1030
    # Long (1000) fits; after refresh short (1030) exceeds the new 100 balance
    client.get_account.return_value = AlpacaAccount(
        cash_amount=100.0, buying_power=100.0, equity=50000.0
    )
    positions = [
        _long(ticker="AAPL", dollar_amount=1000.0),
        _short(ticker="MSFT", entry_price=100.0, quantity=10),
    ]
    results, count = execute_open_positions(client, positions, 10000.0, 50000.0, config)
    assert count == 1  # long succeeded
    assert results[0]["status"] == "success"
    assert results[1]["status"] == "skipped"
    assert results[1]["reason"] == "insufficient_buying_power"


def test_execute_open_positions_handles_asset_not_shortable_error() -> None:
    client = _make_mock_client()
    client.open_position.side_effect = [None, AssetNotShortableError("not shortable")]

    positions = [_long(ticker="AAPL"), _short(ticker="MSFT", quantity=10)]
    results, count = execute_open_positions(
        client, positions, 10000.0, 50000.0, _DEFAULT_CONFIG
    )

    assert count == 1  # long succeeded
    assert results[0]["status"] == "success"
    assert results[1]["status"] == "skipped"
    assert results[1]["reason"] == "not_shortable"


def test_execute_open_positions_submits_long_before_short_within_pair() -> None:
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

    # Pass short first to confirm ordering is driven by pair logic, not input order
    positions = [_short(ticker="MSFT", quantity=10), _long(ticker="AAPL")]
    execute_open_positions(client, positions, 10000.0, 50000.0, _DEFAULT_CONFIG)

    assert submitted_sides[0] == TradeSide.BUY
    assert submitted_sides[1] == TradeSide.SELL


def test_execute_open_positions_skips_short_when_long_leg_fails() -> None:
    client = _make_mock_client()
    client.open_position.side_effect = InsufficientBuyingPowerError("not enough")

    positions = [_long(ticker="AAPL"), _short(ticker="MSFT", quantity=10)]
    results, count = execute_open_positions(
        client, positions, 10000.0, 50000.0, _DEFAULT_CONFIG
    )

    assert count == 0
    assert results[1]["ticker"] == "MSFT"
    assert results[1]["status"] == "skipped"
    assert results[1]["reason"] == "long_leg_failed"
    # open_position called only once — for the long leg
    client.open_position.assert_called_once()


def test_execute_open_positions_skips_incomplete_pair() -> None:
    client = _make_mock_client()
    # Only a long leg — no matching short in the same pair_id
    results, count = execute_open_positions(
        client, [_long(ticker="AAPL")], 10000.0, 50000.0, _DEFAULT_CONFIG
    )
    assert count == 0
    assert results == []
    client.open_position.assert_not_called()


def test_execute_open_positions_returns_empty_for_no_positions() -> None:
    client = _make_mock_client()
    results, count = execute_open_positions(
        client, [], 10000.0, 50000.0, _DEFAULT_CONFIG
    )
    assert results == []
    assert count == 0


def test_execute_open_positions_logs_dangling_long_when_short_fails() -> None:
    client = _make_mock_client()
    client.open_position.side_effect = [None, RuntimeError("short failed")]

    positions = [_long(ticker="AAPL"), _short(ticker="MSFT", quantity=10)]
    with patch("portfolio_manager.trade_execution.logger") as mock_logger:
        results, count = execute_open_positions(
            client, positions, 10000.0, 50000.0, _DEFAULT_CONFIG
        )

    assert count == 1
    assert results[0]["status"] == "success"
    assert results[1]["status"] == "failed"
    mock_logger.warning.assert_called_once()
    warning_kwargs = mock_logger.warning.call_args[1]
    assert warning_kwargs["pair_id"] == "pair-1"
    assert warning_kwargs["long_ticker"] == "AAPL"
    assert warning_kwargs["short_ticker"] == "MSFT"
    assert warning_kwargs["short_status"] == "failed"
    assert warning_kwargs["short_reason"] == "short failed"


def test_execute_open_positions_logs_dangling_long_when_short_skipped() -> None:
    config = Configuration(minimum_short_equity=2000.0)
    client = _make_mock_client()

    positions = [_long(ticker="AAPL"), _short(ticker="MSFT")]
    with patch("portfolio_manager.trade_execution.logger") as mock_logger:
        results, _ = execute_open_positions(client, positions, 10000.0, 500.0, config)

    assert results[0]["status"] == "success"
    assert results[1]["status"] == "skipped"
    mock_logger.warning.assert_called()
    dangling_calls = [
        warning_call
        for warning_call in mock_logger.warning.call_args_list
        if "dangling" in warning_call[0][0]
    ]
    assert len(dangling_calls) == 1
    warning_kwargs = dangling_calls[0][1]
    assert warning_kwargs["pair_id"] == "pair-1"
    assert warning_kwargs["long_ticker"] == "AAPL"
    assert warning_kwargs["short_ticker"] == "MSFT"
    assert warning_kwargs["short_status"] == "skipped"
    assert warning_kwargs["short_reason"] == "insufficient_equity_for_short"


def test_execute_open_positions_uses_computed_short_qty_when_quantity_is_none() -> None:
    client = _make_mock_client()
    submitted_quantities: list[int | None] = []

    def capture_quantity(
        ticker: str,  # noqa: ARG001
        side: TradeSide,  # noqa: ARG001
        dollar_amount: float,  # noqa: ARG001
        entry_price: float,  # noqa: ARG001
        quantity: int | None = None,
    ) -> None:
        submitted_quantities.append(quantity)

    client.open_position.side_effect = capture_quantity

    # quantity=None; fallback = int(300 / 30) = 10
    positions = [
        _long(ticker="AAPL"),
        _short(ticker="MSFT", dollar_amount=300.0, entry_price=30.0, quantity=None),
    ]
    _results, count = execute_open_positions(
        client, positions, 10000.0, 50000.0, _DEFAULT_CONFIG
    )

    expected_short_qty = 10  # int(300 / 30)
    expected_count = 2  # both legs succeed
    assert count == expected_count
    # Long uses position.get("quantity") → None
    assert submitted_quantities[0] is None
    # Short uses computed short_qty = int(300 / 30) = 10, not None
    assert submitted_quantities[1] == expected_short_qty
