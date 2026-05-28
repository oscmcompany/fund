import asyncio
import json
from collections.abc import AsyncGenerator
from unittest.mock import AsyncMock, MagicMock, patch

import internal.database as database_module
import pytest


@pytest.fixture(autouse=True)
def reset_pool(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(database_module, "_pool", None)


def test_get_database_url_raises_when_env_var_missing() -> None:
    with (
        patch.dict("os.environ", {}, clear=True),
        pytest.raises(ValueError, match="DATABASE_URL environment variable is not set"),
    ):
        database_module._get_database_url()  # noqa: SLF001


def test_get_pool_creates_pool_and_opens_it() -> None:
    async def _run() -> None:
        mock_pool = AsyncMock()
        with (
            patch.dict("os.environ", {"DATABASE_URL": "postgresql://localhost/test"}),
            patch(
                "internal.database.AsyncConnectionPool", return_value=mock_pool
            ) as mock_cls,
        ):
            result = await database_module.get_pool()
            mock_cls.assert_called_once()
            mock_pool.open.assert_awaited_once()
            assert result is mock_pool

    asyncio.run(_run())


def test_get_pool_returns_existing_pool_without_reinitializing() -> None:
    async def _run() -> None:
        existing_pool = AsyncMock()
        database_module._pool = existing_pool  # noqa: SLF001
        with patch("internal.database.AsyncConnectionPool") as mock_cls:
            result = await database_module.get_pool()
            mock_cls.assert_not_called()
            assert result is existing_pool

    asyncio.run(_run())


def test_close_pool_closes_and_clears_pool() -> None:
    async def _run() -> None:
        mock_pool = AsyncMock()
        database_module._pool = mock_pool  # noqa: SLF001
        await database_module.close_pool()
        mock_pool.close.assert_awaited_once()
        assert database_module._pool is None  # noqa: SLF001

    asyncio.run(_run())


def test_close_pool_does_nothing_when_pool_is_none() -> None:
    async def _run() -> None:
        await database_module.close_pool()

    asyncio.run(_run())


def test_get_connection_commits_on_success() -> None:
    mock_conn = MagicMock()
    with (
        patch.dict("os.environ", {"DATABASE_URL": "postgresql://localhost/test"}),
        patch("internal.database.Connection.connect", return_value=mock_conn),
    ):
        with database_module.get_connection() as connection:
            assert connection is mock_conn
        mock_conn.commit.assert_called_once()
        mock_conn.close.assert_called_once()


def test_get_connection_rolls_back_and_reraises_on_exception() -> None:
    mock_conn = MagicMock()
    message = "test error"
    with (  # noqa: SIM117
        patch.dict("os.environ", {"DATABASE_URL": "postgresql://localhost/test"}),
        patch("internal.database.Connection.connect", return_value=mock_conn),
    ):
        with pytest.raises(RuntimeError, match=message):
            with database_module.get_connection():
                raise RuntimeError(message)
    mock_conn.rollback.assert_called_once()
    mock_conn.close.assert_called_once()


def test_emit_event_executes_pg_function_via_pool() -> None:
    async def _run() -> None:
        mock_conn = AsyncMock()
        mock_pool = MagicMock()
        mock_pool.connection.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_pool.connection.return_value.__aexit__ = AsyncMock(return_value=None)

        with patch("internal.database.get_pool", AsyncMock(return_value=mock_pool)):
            await database_module.emit_event("rebalance_completed", {"key": "value"})

        mock_conn.execute.assert_awaited_once()
        sql, _ = mock_conn.execute.call_args[0]
        assert "emit_event" in sql

    asyncio.run(_run())


def test_listen_for_events_invokes_handler_with_parsed_notification() -> None:
    async def _run() -> None:
        received: list[tuple[str, int, dict]] = []

        async def handler(event_type: str, event_id: int, payload: dict) -> None:
            received.append((event_type, event_id, payload))

        notification = MagicMock()
        notification.payload = json.dumps(
            {"event_type": "test_event", "event_id": 42, "payload": {"x": 1}}
        )

        mock_conn = AsyncMock()

        async def _notifies() -> AsyncGenerator[MagicMock, None]:
            yield notification

        mock_conn.notifies = MagicMock(return_value=_notifies())

        mock_cm = AsyncMock()
        mock_cm.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_cm.__aexit__ = AsyncMock(return_value=None)

        with (
            patch.dict("os.environ", {"DATABASE_URL": "postgresql://localhost/test"}),
            patch(
                "internal.database.AsyncConnection.connect",
                AsyncMock(return_value=mock_cm),
            ),
        ):
            await database_module.listen_for_events("test_channel", handler)

        assert len(received) == 1
        assert received[0] == ("test_event", 42, {"x": 1})

    asyncio.run(_run())


def test_listen_for_events_logs_handler_exception_and_continues() -> None:
    async def _run() -> None:
        call_count = 0

        async def failing_handler(
            _event_type: str, _event_id: int, _payload: dict
        ) -> None:
            nonlocal call_count
            call_count += 1
            message = "handler failure"
            raise RuntimeError(message)

        notification = MagicMock()
        notification.payload = json.dumps(
            {"event_type": "test_event", "event_id": 1, "payload": {}}
        )

        mock_conn = AsyncMock()

        async def _notifies() -> AsyncGenerator[MagicMock, None]:
            yield notification
            yield notification

        mock_conn.notifies = MagicMock(return_value=_notifies())

        mock_cm = AsyncMock()
        mock_cm.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_cm.__aexit__ = AsyncMock(return_value=None)

        with (
            patch.dict("os.environ", {"DATABASE_URL": "postgresql://localhost/test"}),
            patch(
                "internal.database.AsyncConnection.connect",
                AsyncMock(return_value=mock_cm),
            ),
        ):
            await database_module.listen_for_events("test_channel", failing_handler)

        assert call_count == 2  # noqa: PLR2004

    asyncio.run(_run())


def test_get_consumer_offset_returns_last_event_id_when_row_exists() -> None:
    async def _run() -> None:
        mock_result = AsyncMock()
        mock_result.fetchone = AsyncMock(return_value=(99,))
        mock_conn = AsyncMock()
        mock_conn.execute = AsyncMock(return_value=mock_result)
        mock_pool = MagicMock()
        mock_pool.connection.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_pool.connection.return_value.__aexit__ = AsyncMock(return_value=None)

        with patch("internal.database.get_pool", AsyncMock(return_value=mock_pool)):
            result = await database_module.get_consumer_offset("my_consumer")

        assert result == 99  # noqa: PLR2004

    asyncio.run(_run())


def test_get_consumer_offset_returns_zero_when_no_row() -> None:
    async def _run() -> None:
        mock_result = AsyncMock()
        mock_result.fetchone = AsyncMock(return_value=None)
        mock_conn = AsyncMock()
        mock_conn.execute = AsyncMock(return_value=mock_result)
        mock_pool = MagicMock()
        mock_pool.connection.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_pool.connection.return_value.__aexit__ = AsyncMock(return_value=None)

        with patch("internal.database.get_pool", AsyncMock(return_value=mock_pool)):
            result = await database_module.get_consumer_offset("unknown_consumer")

        assert result == 0

    asyncio.run(_run())


def test_update_consumer_offset_executes_upsert() -> None:
    async def _run() -> None:
        mock_conn = AsyncMock()
        mock_pool = MagicMock()
        mock_pool.connection.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_pool.connection.return_value.__aexit__ = AsyncMock(return_value=None)

        with patch("internal.database.get_pool", AsyncMock(return_value=mock_pool)):
            await database_module.update_consumer_offset("my_consumer", 42)

        mock_conn.execute.assert_awaited_once()
        sql, _ = mock_conn.execute.call_args[0]
        assert "INSERT INTO event_consumer_offsets" in sql

    asyncio.run(_run())
