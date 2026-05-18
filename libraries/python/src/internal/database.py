import asyncio
import os
from collections.abc import Generator
from contextlib import contextmanager

import structlog
from psycopg import Connection
from psycopg_pool import AsyncConnectionPool

logger = structlog.get_logger()

_pool: AsyncConnectionPool | None = None
_pool_lock: asyncio.Lock | None = None


def _get_database_url() -> str:
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        message = "DATABASE_URL environment variable is not set"
        raise ValueError(message)
    return database_url


async def get_pool() -> AsyncConnectionPool:
    """Return a lazily-initialized async connection pool."""
    global _pool, _pool_lock  # noqa: PLW0603
    if _pool_lock is None:
        _pool_lock = asyncio.Lock()
    if _pool is None:
        async with _pool_lock:
            if _pool is None:
                database_url = _get_database_url()
                _pool = AsyncConnectionPool(
                    conninfo=database_url, min_size=1, max_size=5, open=False
                )
                await _pool.open()
                logger.info("Async connection pool opened")
    return _pool


async def close_pool() -> None:
    """Close the async connection pool if open."""
    global _pool, _pool_lock  # noqa: PLW0603
    if _pool_lock is None:
        _pool_lock = asyncio.Lock()
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
