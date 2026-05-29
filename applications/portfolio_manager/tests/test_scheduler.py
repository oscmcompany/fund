import asyncio
import os
from unittest.mock import AsyncMock, MagicMock, patch

import polars as pl
import pytest
from portfolio_manager.alpaca_client import AlpacaAccount
from portfolio_manager.configuration import Configuration
from portfolio_manager.scheduler import (
    _event_listener_loop,
    _handle_eod_snapshot_requested,
    _handle_equity_bars_synced,
    _handle_intraday_check,
    _status_logger_loop,
    spawn_event_listener,
    spawn_status_logger,
)

# --- _handle_equity_bars_synced ---


def test_handle_equity_bars_synced_emits_predictions_requested() -> None:
    mock_emit = AsyncMock()

    async def run() -> None:
        with patch("portfolio_manager.scheduler.emit_event", mock_emit):
            await _handle_equity_bars_synced()

    asyncio.run(run())
    mock_emit.assert_called_once_with("predictions_requested", {})


def test_handle_equity_bars_synced_handles_emit_exception() -> None:
    mock_emit = AsyncMock(side_effect=Exception("db error"))

    async def run() -> None:
        with (
            patch("portfolio_manager.scheduler.emit_event", mock_emit),
            patch("portfolio_manager.scheduler.logger") as mock_logger,
        ):
            await _handle_equity_bars_synced()
            mock_logger.exception.assert_called_once()

    asyncio.run(run())


# --- _handle_intraday_check ---


def test_handle_intraday_check_skips_when_lock_held() -> None:
    mock_alpaca = MagicMock()
    mock_run_rebalance = AsyncMock()

    async def run() -> None:
        lock = asyncio.Lock()
        await lock.acquire()
        with patch("portfolio_manager.scheduler.run_rebalance", mock_run_rebalance):
            await _handle_intraday_check(mock_alpaca, Configuration(), lock)

    asyncio.run(run())
    mock_run_rebalance.assert_not_called()


def test_handle_intraday_check_skips_when_market_closed() -> None:
    mock_alpaca = MagicMock()
    mock_alpaca.is_market_open.return_value = False
    mock_run_rebalance = AsyncMock()

    async def run() -> None:
        lock = asyncio.Lock()
        with patch("portfolio_manager.scheduler.run_rebalance", mock_run_rebalance):
            await _handle_intraday_check(mock_alpaca, Configuration(), lock)

    asyncio.run(run())
    mock_run_rebalance.assert_not_called()


def test_handle_intraday_check_skips_when_no_predictions_available() -> None:
    mock_alpaca = MagicMock()
    mock_alpaca.is_market_open.return_value = True
    mock_run_rebalance = AsyncMock()

    async def run() -> None:
        lock = asyncio.Lock()
        with (
            patch("portfolio_manager.scheduler.run_rebalance", mock_run_rebalance),
            patch(
                "portfolio_manager.scheduler.get_latest_predictions_correlation_id",
                AsyncMock(return_value=None),
            ),
        ):
            await _handle_intraday_check(mock_alpaca, Configuration(), lock)

    asyncio.run(run())
    mock_run_rebalance.assert_not_called()


def test_handle_intraday_check_skips_when_all_pairs_held() -> None:
    mock_alpaca = MagicMock()
    mock_alpaca.is_market_open.return_value = True
    mock_run_rebalance = AsyncMock()

    prior_allocation = pl.DataFrame(
        {"ticker": ["AAPL", "MSFT"], "side": ["long", "short"]}
    )
    # Both tickers are in held_tickers so all pairs continue.
    held_tickers = {"AAPL", "MSFT"}

    async def run() -> None:
        lock = asyncio.Lock()
        with (
            patch("portfolio_manager.scheduler.run_rebalance", mock_run_rebalance),
            patch(
                "portfolio_manager.scheduler.get_latest_predictions_correlation_id",
                AsyncMock(return_value="cid-1"),
            ),
            patch(
                "portfolio_manager.scheduler.get_prior_allocation",
                AsyncMock(return_value=prior_allocation),
            ),
            patch(
                "portfolio_manager.scheduler.fetch_historical_prices",
                AsyncMock(return_value=pl.DataFrame()),
            ),
            patch(
                "portfolio_manager.scheduler.fetch_live_quote_mid_prices",
                AsyncMock(return_value={"AAPL": 150.0, "MSFT": 300.0}),
            ),
            patch(
                "portfolio_manager.scheduler.evaluate_held_pairs_from_quotes",
                return_value=held_tickers,
            ),
        ):
            await _handle_intraday_check(mock_alpaca, Configuration(), lock)

    asyncio.run(run())
    mock_run_rebalance.assert_not_called()

def test_handle_predictions_completed_skips_when_market_closed() -> None:
    mock_alpaca = MagicMock()
    mock_alpaca.is_market_open.return_value = False
    mock_run_rebalance = AsyncMock()

def test_handle_intraday_check_calls_run_rebalance_when_some_pairs_closing() -> None:
    mock_alpaca = MagicMock()
    mock_alpaca.is_market_open.return_value = True
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_run_rebalance = AsyncMock(return_value=mock_response)

    prior_allocation = pl.DataFrame(
        {"ticker": ["AAPL", "MSFT"], "side": ["long", "short"]}
    )
    # Only AAPL is held; MSFT pair is closing.
    held_tickers: set[str] = {"AAPL"}

    async def run() -> None:
        lock = asyncio.Lock()
        with (
            patch("portfolio_manager.scheduler.run_rebalance", mock_run_rebalance),
            patch(
                "portfolio_manager.scheduler.get_latest_predictions_correlation_id",
                AsyncMock(return_value="cid-2"),
            ),
            patch(
                "portfolio_manager.scheduler.get_prior_allocation",
                AsyncMock(return_value=prior_allocation),
            ),
            patch(
                "portfolio_manager.scheduler.fetch_historical_prices",
                AsyncMock(return_value=pl.DataFrame()),
            ),
            patch(
                "portfolio_manager.scheduler.fetch_live_quote_mid_prices",
                AsyncMock(return_value={"AAPL": 150.0, "MSFT": 300.0}),
            ),
            patch(
                "portfolio_manager.scheduler.evaluate_held_pairs_from_quotes",
                return_value=held_tickers,
            ),
        ):
            await _handle_intraday_check(mock_alpaca, Configuration(), lock)

    asyncio.run(run())
    mock_run_rebalance.assert_called_once()
    _, kwargs = mock_run_rebalance.call_args
    assert kwargs.get("held_tickers") == held_tickers


def test_handle_intraday_check_calls_run_rebalance_when_no_prior_allocation() -> None:
    mock_alpaca = MagicMock()
    mock_alpaca.is_market_open.return_value = True
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_run_rebalance = AsyncMock(return_value=mock_response)

    async def run() -> None:
        lock = asyncio.Lock()
        with (
            patch("portfolio_manager.scheduler.run_rebalance", mock_run_rebalance),
            patch(
                "portfolio_manager.scheduler.get_latest_predictions_correlation_id",
                AsyncMock(return_value="cid-3"),
            ),
            patch(
                "portfolio_manager.scheduler.get_prior_allocation",
                AsyncMock(return_value=pl.DataFrame({"ticker": [], "side": []})),
            ),
        ):
            await _handle_intraday_check(mock_alpaca, Configuration(), lock)

    asyncio.run(run())
    mock_run_rebalance.assert_called_once()
    _, kwargs = mock_run_rebalance.call_args
    assert kwargs.get("held_tickers") == set()


def test_handle_intraday_check_passes_correlation_id_to_run_rebalance() -> None:
    mock_alpaca = MagicMock()
    mock_alpaca.is_market_open.return_value = True
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_run_rebalance = AsyncMock(return_value=mock_response)

    async def run() -> None:
        lock = asyncio.Lock()
        with (
            patch("portfolio_manager.scheduler.run_rebalance", mock_run_rebalance),
            patch(
                "portfolio_manager.scheduler.get_latest_predictions_correlation_id",
                AsyncMock(return_value="abc-123"),
            ),
            patch(
                "portfolio_manager.scheduler.get_prior_allocation",
                AsyncMock(return_value=pl.DataFrame({"ticker": [], "side": []})),
            ),
        ):
            await _handle_intraday_check(mock_alpaca, Configuration(), lock)

    asyncio.run(run())
    mock_run_rebalance.assert_called_once()
    assert mock_run_rebalance.call_args.args[2] == "abc-123"

# --- _handle_intraday_check ---

def test_handle_intraday_check_skips_when_historical_prices_fetch_fails() -> None:
    mock_alpaca = MagicMock()
    mock_alpaca.is_market_open.return_value = True
    mock_run_rebalance = AsyncMock()

    prior_allocation = pl.DataFrame(
        {"ticker": ["AAPL", "MSFT"], "side": ["long", "short"]}
    )

    async def run() -> None:
        lock = asyncio.Lock()
        with (
            patch("portfolio_manager.scheduler.run_rebalance", mock_run_rebalance),
            patch(
                "portfolio_manager.scheduler.get_latest_predictions_correlation_id",
                AsyncMock(return_value="cid-4"),
            ),
            patch(
                "portfolio_manager.scheduler.get_prior_allocation",
                AsyncMock(return_value=prior_allocation),
            ),
            patch(
                "portfolio_manager.scheduler.fetch_historical_prices",
                AsyncMock(side_effect=Exception("db error")),
            ),
        ):
            await _handle_intraday_check(mock_alpaca, Configuration(), lock)

    asyncio.run(run())
    mock_run_rebalance.assert_not_called()


def test_handle_intraday_check_skips_when_live_prices_fetch_fails() -> None:
    mock_alpaca = MagicMock()
    mock_alpaca.is_market_open.return_value = True
    mock_run_rebalance = AsyncMock()

    prior_allocation = pl.DataFrame(
        {"ticker": ["AAPL", "MSFT"], "side": ["long", "short"]}
    )

    async def run() -> None:
        lock = asyncio.Lock()
        with (
            patch("portfolio_manager.scheduler.run_rebalance", mock_run_rebalance),
            patch(
                "portfolio_manager.scheduler.get_latest_predictions_correlation_id",
                AsyncMock(return_value="cid-5"),
            ),
            patch(
                "portfolio_manager.scheduler.get_prior_allocation",
                AsyncMock(return_value=prior_allocation),
            ),
            patch(
                "portfolio_manager.scheduler.fetch_historical_prices",
                AsyncMock(return_value=pl.DataFrame()),
            ),
            patch(
                "portfolio_manager.scheduler.fetch_live_quote_mid_prices",
                AsyncMock(side_effect=Exception("quotes unavailable")),
            ),
        ):
            await _handle_intraday_check(mock_alpaca, Configuration(), lock)

    asyncio.run(run())
    mock_run_rebalance.assert_not_called()


def test_handle_intraday_check_logs_warning_on_non_200() -> None:
    mock_alpaca = MagicMock()
    mock_alpaca.is_market_open.return_value = True
    mock_response = MagicMock()
    mock_response.status_code = 500
    mock_run_rebalance = AsyncMock(return_value=mock_response)

    async def run() -> None:
        lock = asyncio.Lock()
        with (
            patch("portfolio_manager.scheduler.run_rebalance", mock_run_rebalance),
            patch(
                "portfolio_manager.scheduler.get_latest_predictions_correlation_id",
                AsyncMock(return_value="cid-6"),
            ),
            patch(
                "portfolio_manager.scheduler.get_prior_allocation",
                AsyncMock(return_value=pl.DataFrame({"ticker": [], "side": []})),
            ),
            patch("portfolio_manager.scheduler.logger") as mock_logger,
        ):
            await _handle_intraday_check(mock_alpaca, Configuration(), lock)
            mock_logger.warning.assert_called_once()

    asyncio.run(run())


def test_handle_intraday_check_handles_market_open_exception() -> None:
    mock_alpaca = MagicMock()
    mock_alpaca.is_market_open.side_effect = Exception("market check failed")
    mock_run_rebalance = AsyncMock()

    async def run() -> None:
        lock = asyncio.Lock()
        with patch("portfolio_manager.scheduler.run_rebalance", mock_run_rebalance):
            await _handle_intraday_check(mock_alpaca, Configuration(), lock)

    asyncio.run(run())
    mock_run_rebalance.assert_not_called()


# --- _event_listener_loop ---


def test_event_listener_loop_exits_without_database_url() -> None:
    mock_alpaca = MagicMock()

    async def run() -> None:
        lock = asyncio.Lock()
        env = {key: val for key, val in os.environ.items() if key != "DATABASE_URL"}
        with patch.dict("os.environ", env, clear=True):
            await _event_listener_loop(mock_alpaca, Configuration(), lock)

    asyncio.run(run())


def test_event_listener_loop_reconnects_on_error() -> None:
    mock_alpaca = MagicMock()
    call_count = {"count": 0}

    async def run() -> None:
        lock = asyncio.Lock()

        async def fake_listen_for_events(_channel: str, _handler: object) -> None:
            call_count["count"] += 1
            if call_count["count"] == 1:
                message = "connection dropped"
                raise ConnectionError(message)
            raise asyncio.CancelledError

        with (
            patch.dict("os.environ", {"DATABASE_URL": "postgresql://localhost/test"}),
            patch(
                "portfolio_manager.scheduler.listen_for_events",
                side_effect=fake_listen_for_events,
            ),
            patch(
                "portfolio_manager.scheduler.asyncio.sleep",
                AsyncMock(return_value=None),
            ),
        ):
            await _event_listener_loop(mock_alpaca, Configuration(), lock)

    asyncio.run(run())
    assert call_count["count"] == 2  # noqa: PLR2004


def test_event_listener_loop_dispatches_equity_bars_synced() -> None:
    mock_alpaca = MagicMock()
    captured_handler: list = []

    async def run() -> None:
        lock = asyncio.Lock()

        async def fake_listen_for_events(_channel: str, handler: object) -> None:
            captured_handler.append(handler)
            raise asyncio.CancelledError

        with (
            patch.dict("os.environ", {"DATABASE_URL": "postgresql://localhost/test"}),
            patch(
                "portfolio_manager.scheduler.listen_for_events",
                side_effect=fake_listen_for_events,
            ),
            patch(
                "portfolio_manager.scheduler.update_consumer_offset",
                AsyncMock(return_value=None),
            ),
            patch(
                "portfolio_manager.scheduler._handle_equity_bars_synced",
                AsyncMock(),
            ) as mock_handle,
        ):
            await _event_listener_loop(mock_alpaca, Configuration(), lock)
            if captured_handler:
                await captured_handler[0]("equity_bars_synced", 42, {})
            mock_handle.assert_called_once()

    asyncio.run(run())


def test_event_listener_loop_dispatches_intraday_check() -> None:
    mock_alpaca = MagicMock()
    captured_handler: list = []

    async def run() -> None:
        lock = asyncio.Lock()

        async def fake_listen_for_events(_channel: str, handler: object) -> None:
            captured_handler.append(handler)
            raise asyncio.CancelledError

        with (
            patch.dict("os.environ", {"DATABASE_URL": "postgresql://localhost/test"}),
            patch(
                "portfolio_manager.scheduler.listen_for_events",
                side_effect=fake_listen_for_events,
            ),
            patch(
                "portfolio_manager.scheduler.update_consumer_offset",
                AsyncMock(return_value=None),
            ),
            patch(
                "portfolio_manager.scheduler._handle_intraday_check",
                AsyncMock(),
            ) as mock_handle,
        ):
            await _event_listener_loop(mock_alpaca, Configuration(), lock)
            if captured_handler:
                await captured_handler[0]("intraday_check", 43, {})
            mock_handle.assert_called_once()

    asyncio.run(run())


def test_spawn_event_listener_creates_task() -> None:
    mock_alpaca = MagicMock()

    async def run() -> asyncio.Task:
        lock = asyncio.Lock()
        with (
            patch.dict("os.environ", {}),
            patch(
                "portfolio_manager.scheduler.listen_for_events",
                AsyncMock(side_effect=asyncio.CancelledError),
            ),
        ):
            task = await spawn_event_listener(mock_alpaca, Configuration(), lock)
            assert isinstance(task, asyncio.Task)
            task.cancel()
            return task

    asyncio.run(run())


def test_spawn_status_logger_creates_task() -> None:
    mock_alpaca = MagicMock()
    mock_alpaca.get_account.side_effect = asyncio.CancelledError

    async def run() -> None:
        task = await spawn_status_logger(mock_alpaca)
        assert isinstance(task, asyncio.Task)
        task.cancel()

    asyncio.run(run())


# --- _status_logger_loop ---


def _make_mock_alpaca_for_status(
    positions: list[dict[str, object]] | None = None,
) -> MagicMock:
    mock_alpaca = MagicMock()
    mock_alpaca.get_account.return_value = AlpacaAccount(
        cash_amount=10000.0, buying_power=20000.0, equity=30000.0
    )
    mock_alpaca.get_open_positions.return_value = positions or []
    return mock_alpaca


def test_status_logger_loop_logs_account_status() -> None:
    mock_alpaca = _make_mock_alpaca_for_status(
        positions=[
            {
                "ticker": "AAPL",
                "side": "long",
                "quantity": 10.0,
                "market_value": 1500.0,
                "unrealized_profit_and_loss": 50.0,
            }
        ]
    )

    async def run() -> None:
        await _status_logger_loop(mock_alpaca)

    with (
        patch(
            "asyncio.sleep",
            AsyncMock(side_effect=[asyncio.CancelledError()]),
        ),
        patch("portfolio_manager.scheduler.logger") as mock_logger,
    ):
        asyncio.run(run())

    mock_logger.info.assert_any_call(
        "Account status",
        cash_amount=10000.0,
        buying_power=20000.0,
        position_count=1,
    )
    mock_logger.debug.assert_any_call(
        "Account positions",
        positions=mock_alpaca.get_open_positions.return_value,
    )


def test_status_logger_loop_handles_exception_without_crashing() -> None:
    mock_alpaca = MagicMock()
    mock_alpaca.get_account.side_effect = [
        Exception("api error"),
        AlpacaAccount(cash_amount=5000.0, buying_power=10000.0, equity=15000.0),
    ]
    mock_alpaca.get_open_positions.return_value = []

    async def run() -> None:
        await _status_logger_loop(mock_alpaca)

    with (
        patch(
            "asyncio.sleep",
            AsyncMock(side_effect=[None, asyncio.CancelledError()]),
        ),
        patch("portfolio_manager.scheduler.logger") as mock_logger,
    ):
        asyncio.run(run())

    mock_logger.exception.assert_called_once()
    mock_logger.info.assert_any_call(
        "Account status",
        cash_amount=5000.0,
        buying_power=10000.0,
        position_count=0,
    )
    mock_logger.debug.assert_any_call("Account positions", positions=[])


def test_status_logger_loop_exits_on_cancellation() -> None:
    mock_alpaca = _make_mock_alpaca_for_status()

    async def run() -> None:
        await _status_logger_loop(mock_alpaca)

    with (
        patch(
            "asyncio.sleep",
            AsyncMock(side_effect=[asyncio.CancelledError()]),
        ),
        patch("portfolio_manager.scheduler.logger") as mock_logger,
    ):
        asyncio.run(run())

    mock_logger.info.assert_any_call("Status logger cancelled")


# --- _handle_eod_snapshot_requested ---


def test_handle_eod_snapshot_saves_eod_snapshot() -> None:
    mock_alpaca = MagicMock()
    mock_alpaca.get_account.return_value = AlpacaAccount(
        cash_amount=500.0,
        buying_power=1000.0,
        equity=10500.0,
    )

    mock_save = AsyncMock(return_value=True)
    previous_value = 10000.0

    async def run() -> None:
        with (
            patch(
                "portfolio_manager.scheduler.get_last_portfolio_value",
                AsyncMock(return_value=previous_value),
            ),
            patch("portfolio_manager.scheduler.save_performance_snapshot", mock_save),
        ):
            await _handle_eod_snapshot_requested(mock_alpaca)

    asyncio.run(run())

    mock_save.assert_called_once()
    call_args = mock_save.call_args
    snapshot = call_args[0][0]
    assert call_args[1]["snapshot_type"] == "eod"
    assert snapshot["portfolio_value"] == pytest.approx(10500.0)
    assert snapshot["gross_return"] == pytest.approx(0.05)
    assert snapshot["net_return"] == pytest.approx(0.05)
    assert snapshot["total_slippage_cost"] == 0.0


def test_handle_eod_snapshot_uses_zero_return_when_no_previous_value() -> None:
    mock_alpaca = MagicMock()
    mock_alpaca.get_account.return_value = AlpacaAccount(
        cash_amount=500.0,
        buying_power=1000.0,
        equity=10500.0,
    )

    mock_save = AsyncMock(return_value=True)

    async def run() -> None:
        with (
            patch(
                "portfolio_manager.scheduler.get_last_portfolio_value",
                AsyncMock(return_value=None),
            ),
            patch("portfolio_manager.scheduler.save_performance_snapshot", mock_save),
        ):
            await _handle_eod_snapshot_requested(mock_alpaca)

    asyncio.run(run())

    call_args = mock_save.call_args
    snapshot = call_args[0][0]
    assert snapshot["gross_return"] == pytest.approx(0.0)


def test_handle_eod_snapshot_raises_when_save_fails() -> None:
    mock_alpaca = MagicMock()
    mock_alpaca.get_account.return_value = AlpacaAccount(
        cash_amount=500.0,
        buying_power=1000.0,
        equity=10500.0,
    )

    async def run() -> None:
        with (
            patch(
                "portfolio_manager.scheduler.get_last_portfolio_value",
                AsyncMock(return_value=None),
            ),
            patch(
                "portfolio_manager.scheduler.save_performance_snapshot",
                AsyncMock(return_value=False),
            ),
            pytest.raises(
                RuntimeError, match="Failed to persist EOD performance snapshot"
            ),
        ):
            await _handle_eod_snapshot_requested(mock_alpaca)

    asyncio.run(run())


def test_handle_eod_snapshot_raises_on_account_error() -> None:
    mock_alpaca = MagicMock()
    mock_alpaca.get_account.side_effect = RuntimeError("alpaca down")

    async def run() -> None:
        with (
            patch("portfolio_manager.scheduler.logger") as mock_logger,
            pytest.raises(RuntimeError, match="alpaca down"),
        ):
            await _handle_eod_snapshot_requested(mock_alpaca)
        mock_logger.exception.assert_called_once()

    asyncio.run(run())
