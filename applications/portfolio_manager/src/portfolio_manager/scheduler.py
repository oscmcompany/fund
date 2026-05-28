import asyncio
import os
from datetime import UTC, datetime
from typing import Any

import structlog
from fastapi import status
from internal.database import emit_event, listen_for_events, update_consumer_offset

from .alpaca_client import AlpacaClient
from .configuration import Configuration
from .data_client import fetch_historical_prices, fetch_live_quote_mid_prices
from .portfolio_state import evaluate_held_pairs_from_quotes, get_prior_allocation
from .rebalance import get_latest_predictions_correlation_id, run_rebalance

logger = structlog.get_logger()

_background_tasks: set[asyncio.Task] = set()


async def spawn_event_listener(
    alpaca_client: AlpacaClient,
    configuration: Configuration,
    rebalance_lock: asyncio.Lock,
) -> asyncio.Task:
    task = asyncio.create_task(
        _event_listener_loop(alpaca_client, configuration, rebalance_lock)
    )
    _background_tasks.add(task)
    task.add_done_callback(_background_tasks.discard)
    return task


_STATUS_LOG_INTERVAL_SECONDS = 60


async def spawn_status_logger(
    alpaca_client: AlpacaClient,
) -> asyncio.Task:
    task = asyncio.create_task(_status_logger_loop(alpaca_client))
    _background_tasks.add(task)
    task.add_done_callback(_background_tasks.discard)
    return task


async def _status_logger_loop(alpaca_client: AlpacaClient) -> None:
    loop = asyncio.get_running_loop()
    while True:
        try:
            account = await loop.run_in_executor(None, alpaca_client.get_account)
            positions = await loop.run_in_executor(
                None, alpaca_client.get_open_positions
            )
            logger.info(
                "Account status",
                cash_amount=account.cash_amount,
                buying_power=account.buying_power,
                position_count=len(positions),
            )
            logger.debug("Account positions", positions=positions)
            await asyncio.sleep(_STATUS_LOG_INTERVAL_SECONDS)
        except asyncio.CancelledError:
            logger.info("Status logger cancelled")
            return
        except Exception as error:
            logger.exception("Status logger error", error=str(error))
            try:
                await asyncio.sleep(_STATUS_LOG_INTERVAL_SECONDS)
            except asyncio.CancelledError:
                logger.info("Status logger cancelled")
                return


async def _handle_equity_bars_synced() -> None:
    try:
        await emit_event("predictions_requested", {})
        logger.info("Emitted predictions_requested after equity bars sync")
    except Exception as error:
        logger.exception("Failed to emit predictions_requested event", error=str(error))


async def _handle_intraday_check(  # noqa: C901, PLR0911
    alpaca_client: AlpacaClient,
    configuration: Configuration,
    rebalance_lock: asyncio.Lock,
) -> None:
    if rebalance_lock.locked():
        logger.info("Rebalance already in progress, skipping intraday_check")
        return

    try:
        market_open = alpaca_client.is_market_open()
    except Exception as error:
        logger.exception("Failed to check market open status", error=str(error))
        return

    if not market_open:
        logger.info("Market is closed, skipping rebalance on intraday_check")
        return

    correlation_id = await get_latest_predictions_correlation_id()
    if not correlation_id:
        logger.info("No predictions available for intraday_check, skipping")
        return

    prior_allocation = await get_prior_allocation()
    if not prior_allocation.is_empty():
        current_timestamp = datetime.now(tz=UTC)
        try:
            equity_bars = await fetch_historical_prices(
                reference_date=current_timestamp
            )
        except Exception as error:
            logger.exception(
                "Failed to fetch historical prices for intraday check",
                error=str(error),
            )
            return
        prior_tickers = prior_allocation["ticker"].to_list()
        try:
            live_mid_prices = await fetch_live_quote_mid_prices(prior_tickers)
        except Exception as error:
            logger.exception(
                "Failed to fetch live quote mid-prices for intraday check",
                error=str(error),
            )
            return
        held_tickers = evaluate_held_pairs_from_quotes(
            prior_allocation, equity_bars, live_mid_prices
        )
        if held_tickers >= set(prior_tickers):
            logger.info("All held pairs continuing, skipping intraday rebalance")
            return
    else:
        held_tickers = set()

    logger.info("Starting intraday rebalance check", correlation_id=correlation_id)
    try:
        async with rebalance_lock:
            response = await run_rebalance(
                alpaca_client,
                configuration,
                correlation_id,
                trigger_reason="intraday_check",
                held_tickers=held_tickers,
            )
        if response.status_code != status.HTTP_200_OK:
            logger.warning(
                "Intraday rebalance completed with non-200 status",
                status_code=response.status_code,
            )
    except Exception as error:
        logger.exception("Intraday portfolio rebalance failed", error=str(error))


async def _event_listener_loop(
    alpaca_client: AlpacaClient,
    configuration: Configuration,
    rebalance_lock: asyncio.Lock,
) -> None:
    if not os.environ.get("DATABASE_URL"):
        logger.info("Event listener disabled, no DATABASE_URL configured")
        return

    consumer_name = "portfolio-manager"

    while True:
        try:

            async def handler(
                event_type: str, event_id: int, _payload: dict[str, Any]
            ) -> None:
                if event_type == "equity_bars_synced":
                    logger.info("Received equity_bars_synced event")
                    task = asyncio.create_task(_handle_equity_bars_synced())
                    _background_tasks.add(task)
                    task.add_done_callback(_background_tasks.discard)
                    await update_consumer_offset(consumer_name, event_id)
                elif event_type == "intraday_check":
                    logger.info("Received intraday_check event")
                    task = asyncio.create_task(
                        _handle_intraday_check(
                            alpaca_client, configuration, rebalance_lock
                        )
                    )
                    _background_tasks.add(task)
                    task.add_done_callback(_background_tasks.discard)
                    await update_consumer_offset(consumer_name, event_id)

            await listen_for_events("events", handler)
        except asyncio.CancelledError:
            return
        except Exception:
            logger.exception("Event listener error, reconnecting in 30s")
            await asyncio.sleep(30)
