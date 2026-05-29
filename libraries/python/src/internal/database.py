import asyncio
import json
import os
from collections.abc import Awaitable, Callable, Generator
from contextlib import contextmanager
from typing import Any

import structlog
from psycopg import AsyncConnection, Connection, sql
from psycopg.types.json import Jsonb
from psycopg_pool import AsyncConnectionPool

logger = structlog.get_logger()

_pool: AsyncConnectionPool | None = None
_pool_lock: asyncio.Lock = asyncio.Lock()


def _get_database_url() -> str:
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        message = "DATABASE_URL environment variable is not set"
        raise ValueError(message)
    return database_url


async def get_pool() -> AsyncConnectionPool:
    """Return a lazily-initialized async connection pool."""
    global _pool  # noqa: PLW0603
    if _pool is not None:
        return _pool
    async with _pool_lock:
        if _pool is not None:
            return _pool
        database_url = _get_database_url()
        pool = AsyncConnectionPool(
            conninfo=database_url, min_size=1, max_size=5, open=False
        )
        await pool.open()
        _pool = pool
        logger.info("Async connection pool opened")
    return _pool


async def close_pool() -> None:
    """Close the async connection pool if open."""
    global _pool  # noqa: PLW0603
    async with _pool_lock:
        if _pool is not None:
            await _pool.close()
            _pool = None
            logger.info("Async connection pool closed")


@contextmanager
def get_connection() -> Generator[Connection, None, None]:
    """Synchronous connection context manager for scripts and one-off queries."""
    database_url = _get_database_url()
    connection = Connection.connect(conninfo=database_url)
    try:
        yield connection
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()


async def emit_event(event_type: str, payload: dict[str, Any]) -> None:
    """Call the emit_event() PG function to insert an event row and fire pg_notify."""
    pool = await get_pool()
    async with pool.connection() as connection:
        await connection.execute(
            "SELECT emit_event(%s, %s)",
            (event_type, Jsonb(payload)),
        )


async def listen_for_events(
    channel: str,
    handler: Callable[[str, int, dict[str, Any]], Awaitable[None]],
) -> None:
    """Listen on a pg_notify channel and invoke handler with each notification.

    Runs until cancelled or the PostgreSQL connection drops. Callers should wrap
    this in a retry loop to reconnect on connection loss. Opens a dedicated
    autocommit connection per call so concurrent listeners do not share a single
    notifies() stream.

    The handler receives (event_type, event_id, payload) parsed from the JSON
    NOTIFY payload emitted by the notify_event() trigger. Handler exceptions are
    logged and the loop continues.
    """
    database_url = _get_database_url()
    async with await AsyncConnection.connect(
        conninfo=database_url, autocommit=True
    ) as connection:
        await connection.execute(sql.SQL("LISTEN {}").format(sql.Identifier(channel)))
        logger.info("Listening on channel", channel=channel)
        async for notification in connection.notifies():
            try:
                data = json.loads(notification.payload)
                if not isinstance(data, dict):
                    logger.warning(
                        "Ignoring malformed event notification",
                        channel=channel,
                        payload=notification.payload,
                    )
                    continue
                event_type = data.get("event_type")
                event_id = data.get("event_id")
                event_payload = data.get("payload") or {}
                if (
                    not isinstance(event_type, str)
                    or not isinstance(event_id, int)
                    or not isinstance(event_payload, dict)
                ):
                    logger.warning(
                        "Ignoring malformed event notification",
                        channel=channel,
                        payload=notification.payload,
                    )
                    continue
                await handler(event_type, event_id, event_payload)
            except Exception:
                logger.exception(
                    "Event handler failed",
                    channel=channel,
                    payload=notification.payload,
                )


async def get_consumer_offset(consumer_name: str) -> int:
    """Return the last processed event ID for a consumer, or 0 if not yet recorded."""
    pool = await get_pool()
    async with pool.connection() as connection:
        result = await connection.execute(
            "SELECT last_event_id FROM event_consumer_offsets WHERE consumer_name = %s",
            (consumer_name,),
        )
        row = await result.fetchone()
        return row[0] if row else 0


async def update_consumer_offset(consumer_name: str, last_event_id: int) -> None:
    """Upsert the last processed event ID for a consumer."""
    pool = await get_pool()
    async with pool.connection() as connection:
        await connection.execute(
            """
            INSERT INTO event_consumer_offsets
                (consumer_name, last_event_id, updated_at)
            VALUES (%s, %s, now())
            ON CONFLICT (consumer_name) DO UPDATE SET
                last_event_id = GREATEST(
                    event_consumer_offsets.last_event_id,
                    EXCLUDED.last_event_id
                ),
                updated_at = EXCLUDED.updated_at
            """,
            (consumer_name, last_event_id),
        )
