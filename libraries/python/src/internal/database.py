import asyncio
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

_listen_connection: AsyncConnection | None = None
_listen_connection_lock: asyncio.Lock = asyncio.Lock()


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


async def get_listen_connection() -> AsyncConnection:
    """Return a lazily-initialized dedicated async connection for LISTEN/NOTIFY.

    This connection runs in autocommit mode and must not be used for regular
    queries — psycopg3 requires a dedicated connection for LISTEN.
    """
    global _listen_connection  # noqa: PLW0603
    if _listen_connection is not None:
        return _listen_connection
    async with _listen_connection_lock:
        if _listen_connection is not None:
            return _listen_connection
        database_url = _get_database_url()
        connection = await AsyncConnection.connect(
            conninfo=database_url, autocommit=True
        )
        _listen_connection = connection
        logger.info("Listen connection opened")
    return _listen_connection


async def close_listen_connection() -> None:
    """Close the dedicated listen connection if open."""
    global _listen_connection  # noqa: PLW0603
    async with _listen_connection_lock:
        if _listen_connection is not None:
            await _listen_connection.close()
            _listen_connection = None
            logger.info("Listen connection closed")


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
    handler: Callable[[str], Awaitable[None]],
) -> None:
    """Listen on a pg_notify channel and invoke handler with each notification payload.

    Runs until cancelled. Intended to be used as a long-running asyncio task.
    The handler receives the raw notification payload string (event_type for the
    'events' channel).
    """
    connection = await get_listen_connection()
    await connection.execute(sql.SQL("LISTEN {}").format(sql.Identifier(channel)))
    logger.info("Listening on channel", channel=channel)
    async for notification in connection.notifies():
        await handler(notification.payload)


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
                last_event_id = EXCLUDED.last_event_id,
                updated_at = EXCLUDED.updated_at
            """,
            (consumer_name, last_event_id),
        )
