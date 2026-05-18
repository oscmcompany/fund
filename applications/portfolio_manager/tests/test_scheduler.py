import asyncio
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, MagicMock, patch
from zoneinfo import ZoneInfo

from portfolio_manager.alpaca_client import AlpacaAccount
from portfolio_manager.scheduler import (
    _already_rebalanced_today,
    _rebalance_loop,
    _seconds_until_next_rebalance,
    _status_logger_loop,
)

_EASTERN = ZoneInfo("America/New_York")


def _make_eastern_datetime(
    weekday_offset: int,
    hour: int,
    minute: int = 0,
) -> datetime:
    """Return a datetime in Eastern time for a known weekday (Monday=0).

    2026-03-23 is a Monday; weekday_offset advances from there.
    """
    base = datetime(2026, 3, 23, hour, minute, 0, tzinfo=_EASTERN)
    return base + timedelta(days=weekday_offset)


# --- _seconds_until_next_rebalance ---


def test_seconds_until_next_rebalance_returns_positive_value() -> None:
    now_utc = _make_eastern_datetime(weekday_offset=0, hour=9).astimezone(UTC)
    with patch("portfolio_manager.scheduler.datetime") as mock_dt:
        mock_dt.now.return_value = now_utc
        result = _seconds_until_next_rebalance()
    assert result > 0


def test_seconds_until_next_rebalance_targets_10am_eastern_on_weekday() -> None:
    # Monday at 09:00 ET — should target same day at 10:00 ET
    now_utc = _make_eastern_datetime(weekday_offset=0, hour=9).astimezone(UTC)

    with patch("portfolio_manager.scheduler.datetime") as mock_dt:
        mock_dt.now.return_value = now_utc
        wait = _seconds_until_next_rebalance()

    assert 0 < wait <= 3600  # noqa: PLR2004


def test_seconds_until_next_rebalance_moves_to_next_day_after_window() -> None:
    # Monday at 11:00 ET — should target Tuesday at 10:00 ET (~23 hours away)
    now_utc = _make_eastern_datetime(weekday_offset=0, hour=11).astimezone(UTC)

    with patch("portfolio_manager.scheduler.datetime") as mock_dt:
        mock_dt.now.return_value = now_utc
        wait = _seconds_until_next_rebalance()

    twenty_two_hours = 22 * 3600
    twenty_four_hours = 24 * 3600
    assert twenty_two_hours < wait < twenty_four_hours


def test_seconds_until_next_rebalance_skips_saturday_to_monday() -> None:
    # Saturday at 09:00 ET — should skip to Monday at 10:00 ET (~49 hours away)
    now_utc = _make_eastern_datetime(weekday_offset=5, hour=9).astimezone(UTC)

    with patch("portfolio_manager.scheduler.datetime") as mock_dt:
        mock_dt.now.return_value = now_utc
        wait = _seconds_until_next_rebalance()

    forty_eight_hours = 48 * 3600
    fifty_two_hours = 52 * 3600
    assert forty_eight_hours < wait < fifty_two_hours


def test_seconds_until_next_rebalance_skips_sunday_to_monday() -> None:
    # Sunday at 09:00 ET — should skip to Monday at 10:00 ET (~25 hours away)
    now_utc = _make_eastern_datetime(weekday_offset=6, hour=9).astimezone(UTC)

    with patch("portfolio_manager.scheduler.datetime") as mock_dt:
        mock_dt.now.return_value = now_utc
        wait = _seconds_until_next_rebalance()

    twenty_four_hours = 24 * 3600
    twenty_eight_hours = 28 * 3600
    assert twenty_four_hours < wait < twenty_eight_hours


# --- _already_rebalanced_today ---


def _make_mock_http_client(mock_response: MagicMock) -> AsyncMock:
    mock_client = AsyncMock()
    mock_client.__aenter__.return_value = mock_client
    mock_client.get.return_value = mock_response
    return mock_client


def test_already_rebalanced_today_returns_true_when_todays_portfolio_exists() -> None:
    frozen_now = datetime(2026, 3, 23, 10, 30, 0, tzinfo=_EASTERN)
    data = [{"ticker": "AAPL", "timestamp": int(frozen_now.timestamp() * 1000)}]
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = data
    mock_client = _make_mock_http_client(mock_response)

    with patch("portfolio_manager.scheduler.datetime") as mock_dt:
        mock_dt.now.return_value = frozen_now
        mock_dt.fromtimestamp.side_effect = datetime.fromtimestamp
        with patch(
            "portfolio_manager.scheduler.httpx.AsyncClient", return_value=mock_client
        ):
            result = asyncio.run(_already_rebalanced_today("http://data-manager:8080"))

    assert result is True


def test_already_rebalanced_today_returns_false_when_portfolio_is_from_yesterday() -> (
    None
):
    frozen_now = datetime(2026, 3, 23, 10, 30, 0, tzinfo=_EASTERN)
    yesterday = frozen_now - timedelta(days=1)
    data = [{"ticker": "AAPL", "timestamp": int(yesterday.timestamp() * 1000)}]
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = data
    mock_client = _make_mock_http_client(mock_response)

    with patch("portfolio_manager.scheduler.datetime") as mock_dt:
        mock_dt.now.return_value = frozen_now
        mock_dt.fromtimestamp.side_effect = datetime.fromtimestamp
        with patch(
            "portfolio_manager.scheduler.httpx.AsyncClient", return_value=mock_client
        ):
            result = asyncio.run(_already_rebalanced_today("http://data-manager:8080"))

    assert result is False


def test_already_rebalanced_today_handles_eastern_utc_day_boundary() -> None:
    # Timestamp recorded at Monday 20:30 ET (= Tuesday 00:30 UTC).
    # "now" is also Monday 20:30 ET — should detect already rebalanced for that ET day.
    monday_eastern = datetime(2026, 3, 23, 20, 30, 0, tzinfo=_EASTERN)
    data = [{"ticker": "AAPL", "timestamp": int(monday_eastern.timestamp() * 1000)}]
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = data
    mock_client = _make_mock_http_client(mock_response)

    with patch("portfolio_manager.scheduler.datetime") as mock_dt:
        mock_dt.now.return_value = monday_eastern
        mock_dt.fromtimestamp.side_effect = datetime.fromtimestamp
        with patch(
            "portfolio_manager.scheduler.httpx.AsyncClient", return_value=mock_client
        ):
            result = asyncio.run(_already_rebalanced_today("http://data-manager:8080"))

    assert result is True


def test_already_rebalanced_today_returns_false_on_empty_response() -> None:
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = []
    mock_client = _make_mock_http_client(mock_response)

    with patch(
        "portfolio_manager.scheduler.httpx.AsyncClient", return_value=mock_client
    ):
        result = asyncio.run(_already_rebalanced_today("http://data-manager:8080"))

    assert result is False


def test_already_rebalanced_today_returns_true_on_error_status() -> None:
    mock_response = MagicMock()
    mock_response.status_code = 500
    mock_client = _make_mock_http_client(mock_response)

    with patch(
        "portfolio_manager.scheduler.httpx.AsyncClient", return_value=mock_client
    ):
        result = asyncio.run(_already_rebalanced_today("http://data-manager:8080"))

    assert result is True


def test_already_rebalanced_today_returns_true_on_request_exception() -> None:
    mock_client = AsyncMock()
    mock_client.__aenter__.return_value = mock_client
    mock_client.get.side_effect = Exception("network error")

    with patch(
        "portfolio_manager.scheduler.httpx.AsyncClient", return_value=mock_client
    ):
        result = asyncio.run(_already_rebalanced_today("http://data-manager:8080"))

    assert result is True


def test_already_rebalanced_today_returns_false_when_timestamp_field_is_missing() -> (
    None
):
    data = [{"ticker": "AAPL"}]
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = data
    mock_client = _make_mock_http_client(mock_response)

    with patch(
        "portfolio_manager.scheduler.httpx.AsyncClient", return_value=mock_client
    ):
        result = asyncio.run(_already_rebalanced_today("http://data-manager:8080"))

    assert result is False


# --- _rebalance_loop ---


def _run_loop(
    mock_alpaca: MagicMock,
    mock_sleep_side_effect: list[BaseException | None],
    frozen_now: datetime,
    *,
    already_rebalanced: bool = False,
    market_open: bool = True,
) -> AsyncMock:
    """Run one pass of the rebalance loop and return the run_rebalance mock."""
    mock_alpaca.is_market_open.return_value = market_open

    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_run_rebalance = AsyncMock(return_value=mock_response)

    async def run() -> None:
        lock = asyncio.Lock()
        await _rebalance_loop(mock_alpaca, "http://data-manager:8080", lock)

    with (
        patch("portfolio_manager.scheduler.datetime") as mock_dt,
        patch(
            "portfolio_manager.scheduler._already_rebalanced_today",
            AsyncMock(return_value=already_rebalanced),
        ),
        patch("portfolio_manager.scheduler.run_rebalance", mock_run_rebalance),
        patch("asyncio.sleep", AsyncMock(side_effect=mock_sleep_side_effect)),
    ):
        mock_dt.now.return_value = frozen_now
        mock_dt.fromtimestamp.side_effect = datetime.fromtimestamp
        asyncio.run(run())

    return mock_run_rebalance


def test_rebalance_loop_fires_immediately_on_catch_up() -> None:
    # Tuesday 10:05 AM ET — past the 10:00 trigger, not yet rebalanced today.
    # Catch-up fires without sleeping; first sleep after the rebalance stops the loop.
    frozen_now = _make_eastern_datetime(weekday_offset=1, hour=10, minute=5).astimezone(
        UTC
    )
    mock_alpaca = MagicMock()

    mock_run_rebalance = _run_loop(
        mock_alpaca=mock_alpaca,
        frozen_now=frozen_now,
        mock_sleep_side_effect=[asyncio.CancelledError()],
        already_rebalanced=False,
        market_open=True,
    )

    mock_run_rebalance.assert_called_once()


def test_rebalance_loop_fires_after_sleeping_when_started_before_window() -> None:
    # Monday 09:00 AM ET — before 10:00, so the loop sleeps first, then rebalances.
    # First sleep resolves normally; second sleep stops the loop.
    frozen_now = _make_eastern_datetime(weekday_offset=0, hour=9).astimezone(UTC)
    mock_alpaca = MagicMock()

    mock_run_rebalance = _run_loop(
        mock_alpaca=mock_alpaca,
        frozen_now=frozen_now,
        mock_sleep_side_effect=[None, asyncio.CancelledError()],
        already_rebalanced=False,
        market_open=True,
    )

    mock_run_rebalance.assert_called_once()


def test_rebalance_loop_skips_when_market_is_closed() -> None:
    # Tuesday 10:05 AM ET — catch-up fires, but market is closed → no rebalance.
    frozen_now = _make_eastern_datetime(weekday_offset=1, hour=10, minute=5).astimezone(
        UTC
    )
    mock_alpaca = MagicMock()

    mock_run_rebalance = _run_loop(
        mock_alpaca=mock_alpaca,
        frozen_now=frozen_now,
        mock_sleep_side_effect=[asyncio.CancelledError()],
        already_rebalanced=False,
        market_open=False,
    )

    mock_run_rebalance.assert_not_called()


def test_rebalance_loop_skips_when_already_rebalanced_today() -> None:
    # Tuesday 10:05 AM ET — catch-up check finds today's rebalance already done.
    frozen_now = _make_eastern_datetime(weekday_offset=1, hour=10, minute=5).astimezone(
        UTC
    )
    mock_alpaca = MagicMock()

    mock_run_rebalance = _run_loop(
        mock_alpaca=mock_alpaca,
        frozen_now=frozen_now,
        mock_sleep_side_effect=[asyncio.CancelledError()],
        already_rebalanced=True,
        market_open=True,
    )

    mock_run_rebalance.assert_not_called()


def test_rebalance_loop_skips_on_weekend_in_loop() -> None:
    # Saturday 09:00 AM ET — catch_up=False (weekend), loop sleeps, in-loop weekend
    # check skips, second sleep cancels.
    frozen_now = _make_eastern_datetime(weekday_offset=5, hour=9).astimezone(UTC)
    mock_alpaca = MagicMock()

    mock_run_rebalance = _run_loop(
        mock_alpaca=mock_alpaca,
        frozen_now=frozen_now,
        mock_sleep_side_effect=[None, asyncio.CancelledError()],
        already_rebalanced=False,
        market_open=True,
    )

    mock_run_rebalance.assert_not_called()


def test_rebalance_loop_skips_when_already_rebalanced_in_loop() -> None:
    # Monday 09:00 AM ET — before the 10:00 window so catch_up=False; after
    # sleeping, the in-loop already-rebalanced check returns True → skip.
    frozen_now = _make_eastern_datetime(weekday_offset=0, hour=9).astimezone(UTC)
    mock_alpaca = MagicMock()

    mock_run_rebalance = _run_loop(
        mock_alpaca=mock_alpaca,
        frozen_now=frozen_now,
        mock_sleep_side_effect=[None, asyncio.CancelledError()],
        already_rebalanced=True,
        market_open=True,
    )

    mock_run_rebalance.assert_not_called()


def test_rebalance_loop_skips_when_lock_is_held() -> None:
    # Tuesday 10:05 AM ET — catch-up fires but the rebalance lock is already held.
    frozen_now = _make_eastern_datetime(weekday_offset=1, hour=10, minute=5).astimezone(
        UTC
    )
    mock_alpaca = MagicMock()
    mock_alpaca.is_market_open.return_value = True
    mock_run_rebalance = AsyncMock()

    async def run() -> None:
        lock = asyncio.Lock()
        await lock.acquire()
        await _rebalance_loop(mock_alpaca, "http://data-manager:8080", lock)

    with (
        patch("portfolio_manager.scheduler.datetime") as mock_dt,
        patch(
            "portfolio_manager.scheduler._already_rebalanced_today",
            AsyncMock(return_value=False),
        ),
        patch("portfolio_manager.scheduler.run_rebalance", mock_run_rebalance),
        patch("asyncio.sleep", AsyncMock(side_effect=[asyncio.CancelledError()])),
    ):
        mock_dt.now.return_value = frozen_now
        mock_dt.fromtimestamp.side_effect = datetime.fromtimestamp
        asyncio.run(run())

    mock_run_rebalance.assert_not_called()


def test_rebalance_loop_handles_market_open_exception() -> None:
    # Tuesday 10:05 AM ET — catch-up fires; is_market_open raises; error logged.
    frozen_now = _make_eastern_datetime(weekday_offset=1, hour=10, minute=5).astimezone(
        UTC
    )
    mock_alpaca = MagicMock()
    mock_alpaca.is_market_open.side_effect = Exception("market check failed")
    mock_run_rebalance = AsyncMock()

    async def run() -> None:
        lock = asyncio.Lock()
        await _rebalance_loop(mock_alpaca, "http://data-manager:8080", lock)

    with (
        patch("portfolio_manager.scheduler.datetime") as mock_dt,
        patch(
            "portfolio_manager.scheduler._already_rebalanced_today",
            AsyncMock(return_value=False),
        ),
        patch("portfolio_manager.scheduler.run_rebalance", mock_run_rebalance),
        patch("asyncio.sleep", AsyncMock(side_effect=[asyncio.CancelledError()])),
    ):
        mock_dt.now.return_value = frozen_now
        mock_dt.fromtimestamp.side_effect = datetime.fromtimestamp
        asyncio.run(run())

    mock_run_rebalance.assert_not_called()


def test_rebalance_loop_handles_run_rebalance_exception() -> None:
    # Tuesday 10:05 AM ET — catch-up fires; run_rebalance raises; error logged.
    frozen_now = _make_eastern_datetime(weekday_offset=1, hour=10, minute=5).astimezone(
        UTC
    )
    mock_alpaca = MagicMock()
    mock_alpaca.is_market_open.return_value = True
    mock_run_rebalance = AsyncMock(side_effect=Exception("rebalance failed"))

    async def run() -> None:
        lock = asyncio.Lock()
        await _rebalance_loop(mock_alpaca, "http://data-manager:8080", lock)

    with (
        patch("portfolio_manager.scheduler.datetime") as mock_dt,
        patch(
            "portfolio_manager.scheduler._already_rebalanced_today",
            AsyncMock(return_value=False),
        ),
        patch("portfolio_manager.scheduler.run_rebalance", mock_run_rebalance),
        patch("asyncio.sleep", AsyncMock(side_effect=[asyncio.CancelledError()])),
    ):
        mock_dt.now.return_value = frozen_now
        mock_dt.fromtimestamp.side_effect = datetime.fromtimestamp
        asyncio.run(run())

    mock_run_rebalance.assert_called_once()


def test_rebalance_loop_logs_warning_on_non_200_response() -> None:
    # Tuesday 10:05 AM ET — catch-up fires; run_rebalance returns non-200.
    frozen_now = _make_eastern_datetime(weekday_offset=1, hour=10, minute=5).astimezone(
        UTC
    )
    mock_alpaca = MagicMock()
    mock_alpaca.is_market_open.return_value = True
    mock_response = MagicMock()
    mock_response.status_code = 500
    mock_run_rebalance = AsyncMock(return_value=mock_response)

    async def run() -> None:
        lock = asyncio.Lock()
        await _rebalance_loop(mock_alpaca, "http://data-manager:8080", lock)

    with (
        patch("portfolio_manager.scheduler.datetime") as mock_dt,
        patch(
            "portfolio_manager.scheduler._already_rebalanced_today",
            AsyncMock(return_value=False),
        ),
        patch("portfolio_manager.scheduler.run_rebalance", mock_run_rebalance),
        patch("asyncio.sleep", AsyncMock(side_effect=[asyncio.CancelledError()])),
        patch("portfolio_manager.scheduler.logger") as mock_logger,
    ):
        mock_dt.now.return_value = frozen_now
        mock_dt.fromtimestamp.side_effect = datetime.fromtimestamp
        asyncio.run(run())

    mock_run_rebalance.assert_called_once()
    mock_logger.warning.assert_called_once()


def test_rebalance_loop_retries_after_unexpected_error() -> None:
    # Monday 09:00 AM ET — before window so catch_up=False; first sleep raises an
    # unexpected error caught by the outer except, loop retries; second sleep cancels.
    frozen_now = _make_eastern_datetime(weekday_offset=0, hour=9).astimezone(UTC)
    mock_alpaca = MagicMock()
    mock_alpaca.is_market_open.return_value = True
    mock_run_rebalance = AsyncMock()

    async def run() -> None:
        lock = asyncio.Lock()
        await _rebalance_loop(mock_alpaca, "http://data-manager:8080", lock)

    with (
        patch("portfolio_manager.scheduler.datetime") as mock_dt,
        patch(
            "portfolio_manager.scheduler._already_rebalanced_today",
            AsyncMock(return_value=False),
        ),
        patch("portfolio_manager.scheduler.run_rebalance", mock_run_rebalance),
        patch(
            "asyncio.sleep",
            AsyncMock(
                side_effect=[RuntimeError("unexpected"), asyncio.CancelledError()]
            ),
        ),
    ):
        mock_dt.now.return_value = frozen_now
        mock_dt.fromtimestamp.side_effect = datetime.fromtimestamp
        asyncio.run(run())

    mock_run_rebalance.assert_not_called()


# --- _status_logger_loop ---


def _make_mock_alpaca_for_status(
    positions: list[dict[str, object]] | None = None,
) -> MagicMock:
    mock_alpaca = MagicMock()
    mock_alpaca.get_account.return_value = AlpacaAccount(
        cash_amount=10000.0, buying_power=20000.0
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
        AlpacaAccount(cash_amount=5000.0, buying_power=10000.0),
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
