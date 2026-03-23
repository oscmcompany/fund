import asyncio
from datetime import UTC, datetime, timedelta
from zoneinfo import ZoneInfo

import requests
import sentry_sdk
import structlog

from .alpaca_client import AlpacaClient
from .rebalance import run_rebalance

logger = structlog.get_logger()

_EASTERN = ZoneInfo("America/New_York")
_background_tasks: set[asyncio.Task] = set()
_REBALANCE_HOUR = 10
_REBALANCE_MINUTE = 0
_WEEKEND_WEEKDAY_MIN = 5


def _seconds_until_next_rebalance() -> float:
    now = datetime.now(tz=UTC)
    now_eastern = now.astimezone(_EASTERN)
    target_eastern = now_eastern.replace(
        hour=_REBALANCE_HOUR,
        minute=_REBALANCE_MINUTE,
        second=0,
        microsecond=0,
    )
    if now_eastern >= target_eastern:
        target_eastern = target_eastern + timedelta(days=1)
    # Skip weekends; market clock handles holidays
    while target_eastern.weekday() >= _WEEKEND_WEEKDAY_MIN:
        target_eastern = target_eastern + timedelta(days=1)
    target = target_eastern.astimezone(UTC)
    return (target - now).total_seconds()


def _already_rebalanced_today(datamanager_base_url: str) -> bool:
    today = datetime.now(tz=UTC).date()
    try:
        response = requests.get(
            url=f"{datamanager_base_url}/portfolios",
            timeout=60,
        )
        if response.status_code >= 400:  # noqa: PLR2004
            return False
        data = response.json()
        if not data:
            return False
        for row in data:
            timestamp_value = row.get("timestamp")
            if timestamp_value is not None:
                row_date = datetime.fromtimestamp(float(timestamp_value), tz=UTC).date()
                if row_date == today:
                    return True
    except Exception as error:
        logger.exception(
            "Failed to check prior portfolio for idempotency guard", error=str(error)
        )
    return False


async def spawn_rebalance_scheduler(
    alpaca_client: AlpacaClient,
    datamanager_base_url: str,
) -> None:
    task = asyncio.create_task(_rebalance_loop(alpaca_client, datamanager_base_url))
    _background_tasks.add(task)
    task.add_done_callback(_background_tasks.discard)


async def _rebalance_loop(
    alpaca_client: AlpacaClient,
    datamanager_base_url: str,
) -> None:
    while True:
        wait_seconds = _seconds_until_next_rebalance()
        logger.info(
            "Waiting for next portfolio rebalance",
            seconds_until_rebalance=int(wait_seconds),
        )
        await asyncio.sleep(wait_seconds)

        if not alpaca_client.is_market_open():
            logger.info("Market is closed, skipping scheduled rebalance")
            continue

        if _already_rebalanced_today(datamanager_base_url):
            logger.info("Portfolio already rebalanced today, skipping")
            continue

        logger.info("Starting scheduled portfolio rebalance")
        try:
            await run_rebalance(alpaca_client)
        except Exception as error:
            logger.exception("Scheduled portfolio rebalance failed", error=str(error))
            sentry_sdk.capture_exception(error)
