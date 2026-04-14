import asyncio
from datetime import UTC, datetime, timedelta
from zoneinfo import ZoneInfo

import httpx
import sentry_sdk
import structlog
from fastapi import status

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


async def _already_rebalanced_today(data_manager_base_url: str) -> bool:
    today = datetime.now(tz=_EASTERN).date()
    try:
        async with httpx.AsyncClient(timeout=60.0) as client:
            response = await client.get(url=f"{data_manager_base_url}/portfolios")
        if response.status_code >= 400:  # noqa: PLR2004
            logger.warning(
                "Data manager error for portfolio check, skipping rebalance",
                status_code=response.status_code,
            )
            return True
        data = response.json()
        if not data:
            return False
        for row in data:
            timestamp_value = row.get("timestamp")
            if timestamp_value is not None:
                row_date = datetime.fromtimestamp(
                    float(timestamp_value), tz=_EASTERN
                ).date()
                if row_date == today:
                    return True
    except Exception as error:
        logger.exception(
            "Failed to check prior portfolio for idempotency guard, skipping rebalance",
            error=str(error),
        )
        return True
    return False


async def spawn_rebalance_scheduler(
    alpaca_client: AlpacaClient,
    data_manager_base_url: str,
    rebalance_lock: asyncio.Lock,
) -> asyncio.Task:
    task = asyncio.create_task(
        _rebalance_loop(alpaca_client, data_manager_base_url, rebalance_lock)
    )
    _background_tasks.add(task)
    task.add_done_callback(_background_tasks.discard)
    return task


async def _rebalance_loop(  # noqa: C901
    alpaca_client: AlpacaClient,
    data_manager_base_url: str,
    rebalance_lock: asyncio.Lock,
) -> None:
    now_eastern = datetime.now(tz=UTC).astimezone(_EASTERN)
    catch_up = (
        now_eastern.weekday() < _WEEKEND_WEEKDAY_MIN
        and now_eastern.hour >= _REBALANCE_HOUR
        and not await _already_rebalanced_today(data_manager_base_url)
    )
    if catch_up:
        logger.info("Missed rebalance window detected, running immediately")

    while True:
        try:
            if not catch_up:
                wait_seconds = _seconds_until_next_rebalance()
                logger.info(
                    "Waiting for next portfolio rebalance",
                    seconds_until_rebalance=int(wait_seconds),
                )
                await asyncio.sleep(wait_seconds)
            catch_up = False

            now_eastern = datetime.now(tz=UTC).astimezone(_EASTERN)
            if now_eastern.weekday() >= _WEEKEND_WEEKDAY_MIN:
                logger.info("Weekend detected, skipping scheduled rebalance")
                continue

            try:
                market_open = alpaca_client.is_market_open()
            except Exception as error:
                logger.exception("Failed to check market open status", error=str(error))
                sentry_sdk.capture_exception(error)
                continue

            if not market_open:
                logger.info("Market is closed, skipping scheduled rebalance")
                continue

            if await _already_rebalanced_today(data_manager_base_url):
                logger.info("Portfolio already rebalanced today, skipping")
                continue

            if rebalance_lock.locked():
                logger.info("Rebalance already in progress, skipping scheduled run")
                continue

            logger.info("Starting scheduled portfolio rebalance")
            try:
                async with rebalance_lock:
                    response = await run_rebalance(alpaca_client)
                if response.status_code != status.HTTP_200_OK:
                    logger.warning(
                        "Scheduled rebalance completed with non-200 status",
                        status_code=response.status_code,
                    )
            except Exception as error:
                logger.exception(
                    "Scheduled portfolio rebalance failed", error=str(error)
                )
                sentry_sdk.capture_exception(error)
        except asyncio.CancelledError:
            logger.info("Rebalance scheduler cancelled")
            return
        except Exception as error:
            logger.exception(
                "Unexpected error in rebalance loop, retrying next cycle",
                error=str(error),
            )
            sentry_sdk.capture_exception(error)
