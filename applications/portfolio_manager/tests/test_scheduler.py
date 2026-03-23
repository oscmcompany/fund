from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock, patch
from zoneinfo import ZoneInfo

from portfolio_manager.scheduler import (
    _already_rebalanced_today,
    _seconds_until_next_rebalance,
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


def test_already_rebalanced_today_returns_true_when_todays_portfolio_exists() -> None:
    frozen_now = datetime(2026, 3, 23, 10, 30, 0, tzinfo=_EASTERN)
    data = [{"ticker": "AAPL", "timestamp": frozen_now.timestamp()}]
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = data

    with patch("portfolio_manager.scheduler.datetime") as mock_dt:
        mock_dt.now.return_value = frozen_now
        mock_dt.fromtimestamp.side_effect = datetime.fromtimestamp
        with patch(
            "portfolio_manager.scheduler.requests.get", return_value=mock_response
        ):
            result = _already_rebalanced_today("http://data-manager:8080")

    assert result is True


def test_already_rebalanced_today_returns_false_when_portfolio_is_from_yesterday() -> (
    None
):
    frozen_now = datetime(2026, 3, 23, 10, 30, 0, tzinfo=_EASTERN)
    yesterday = frozen_now - timedelta(days=1)
    data = [{"ticker": "AAPL", "timestamp": yesterday.timestamp()}]
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = data

    with patch("portfolio_manager.scheduler.datetime") as mock_dt:
        mock_dt.now.return_value = frozen_now
        mock_dt.fromtimestamp.side_effect = datetime.fromtimestamp
        with patch(
            "portfolio_manager.scheduler.requests.get", return_value=mock_response
        ):
            result = _already_rebalanced_today("http://data-manager:8080")

    assert result is False


def test_already_rebalanced_today_handles_eastern_utc_day_boundary() -> None:
    # Timestamp recorded at Monday 20:30 ET (= Tuesday 00:30 UTC).
    # "now" is also Monday 20:30 ET — should detect already rebalanced for that ET day.
    monday_eastern = datetime(2026, 3, 23, 20, 30, 0, tzinfo=_EASTERN)
    data = [{"ticker": "AAPL", "timestamp": monday_eastern.timestamp()}]
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = data

    with patch("portfolio_manager.scheduler.datetime") as mock_dt:
        mock_dt.now.return_value = monday_eastern
        mock_dt.fromtimestamp.side_effect = datetime.fromtimestamp
        with patch(
            "portfolio_manager.scheduler.requests.get", return_value=mock_response
        ):
            result = _already_rebalanced_today("http://data-manager:8080")

    assert result is True


def test_already_rebalanced_today_returns_false_on_empty_response() -> None:
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = []

    with patch("portfolio_manager.scheduler.requests.get", return_value=mock_response):
        result = _already_rebalanced_today("http://data-manager:8080")

    assert result is False


def test_already_rebalanced_today_returns_true_on_error_status() -> None:
    mock_response = MagicMock()
    mock_response.status_code = 500

    with patch("portfolio_manager.scheduler.requests.get", return_value=mock_response):
        result = _already_rebalanced_today("http://data-manager:8080")

    assert result is True


def test_already_rebalanced_today_returns_true_on_request_exception() -> None:
    with patch(
        "portfolio_manager.scheduler.requests.get",
        side_effect=Exception("network error"),
    ):
        result = _already_rebalanced_today("http://data-manager:8080")

    assert result is True


def test_already_rebalanced_today_returns_false_when_timestamp_field_is_missing() -> (
    None
):
    data = [{"ticker": "AAPL"}]
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = data

    with patch("portfolio_manager.scheduler.requests.get", return_value=mock_response):
        result = _already_rebalanced_today("http://data-manager:8080")

    assert result is False
