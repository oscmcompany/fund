import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import portfolio_manager.server as server_module
import pytest
from fastapi import Response, status
from portfolio_manager.server import (
    _lifespan,
    application,
    create_portfolio,
    health_check,
    metrics_endpoint,
)


def test_health_check_returns_200() -> None:
    response = health_check()

    assert response.status_code == status.HTTP_200_OK


def test_metrics_endpoint_returns_response() -> None:
    response = metrics_endpoint()

    assert isinstance(response, Response)
    assert response.status_code == status.HTTP_200_OK


def test_lifespan_raises_when_api_key_missing() -> None:
    mock_app = MagicMock()

    async def run() -> None:
        async with _lifespan(mock_app):
            pass

    with (
        patch.object(server_module, "ALPACA_API_KEY_ID", ""),
        patch.object(server_module, "ALPACA_API_SECRET", "secret"),
        pytest.raises(ValueError, match="ALPACA_API_KEY_ID and ALPACA_API_SECRET"),
    ):
        asyncio.run(run())


def test_lifespan_raises_when_api_secret_missing() -> None:
    mock_app = MagicMock()

    async def run() -> None:
        async with _lifespan(mock_app):
            pass

    with (
        patch.object(server_module, "ALPACA_API_KEY_ID", "key"),
        patch.object(server_module, "ALPACA_API_SECRET", ""),
        pytest.raises(ValueError, match="ALPACA_API_KEY_ID and ALPACA_API_SECRET"),
    ):
        asyncio.run(run())


def test_lifespan_initializes_client_and_scheduler() -> None:
    mock_app = MagicMock()
    mock_alpaca = MagicMock()

    async def run() -> None:
        # Use an already-done future: cancel() is a no-op on done futures,
        # and await returns None without raising CancelledError.
        future: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        future.set_result(None)

        with (
            patch.object(server_module, "ALPACA_API_KEY_ID", "test-key"),
            patch.object(server_module, "ALPACA_API_SECRET", "test-secret"),
            patch(
                "portfolio_manager.server.AlpacaClient", return_value=mock_alpaca
            ) as mock_alpaca_client,
            patch(
                "portfolio_manager.server.spawn_rebalance_scheduler",
                AsyncMock(return_value=future),
            ) as mock_spawn_scheduler,
        ):
            async with _lifespan(mock_app):
                pass

        mock_alpaca_client.assert_called_once()
        mock_spawn_scheduler.assert_awaited_once()

    asyncio.run(run())


def test_create_portfolio_returns_conflict_when_lock_held() -> None:
    async def run() -> Response:
        with patch(
            "portfolio_manager.server.asyncio.wait_for",
            AsyncMock(side_effect=TimeoutError),
        ):
            return await create_portfolio()

    response = asyncio.run(run())

    assert response.status_code == status.HTTP_409_CONFLICT


def test_create_portfolio_calls_run_rebalance() -> None:
    mock_alpaca = MagicMock()
    mock_response = MagicMock(spec=Response)
    mock_response.status_code = status.HTTP_200_OK
    mock_run = AsyncMock(return_value=mock_response)
    mock_lock = MagicMock()
    mock_lock.release = MagicMock()

    application.state.alpaca_client = mock_alpaca

    async def run() -> Response:
        # Patch wait_for to simulate a successful lock acquisition so we can verify
        # that run_rebalance is called and the lock is released in the finally block.
        with (
            patch(
                "portfolio_manager.server.asyncio.wait_for",
                AsyncMock(return_value=True),
            ),
            patch("portfolio_manager.server._rebalance_lock", mock_lock),
            patch("portfolio_manager.server.run_rebalance", mock_run),
        ):
            return await create_portfolio()

    response = asyncio.run(run())

    assert response.status_code == status.HTTP_200_OK
    mock_run.assert_called_once_with(mock_alpaca)
    mock_lock.release.assert_called_once()
