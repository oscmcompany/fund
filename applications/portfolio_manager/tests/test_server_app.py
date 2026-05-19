import asyncio
import json
from unittest.mock import AsyncMock, MagicMock, patch

import portfolio_manager.server as server_module
import pytest
from alpaca.common.exceptions import APIError
from fastapi import Response, status
from portfolio_manager.configuration import Configuration
from portfolio_manager.server import (
    _lifespan,
    application,
    create_portfolio,
    health_check,
    metrics_endpoint,
)


def _save_health_state() -> tuple[object, object]:
    return (
        getattr(application.state, "alpaca_client", None),
        getattr(application.state, "scheduler_task", None),
    )


def _restore_health_state(saved: tuple[object, object]) -> None:
    previous_client, previous_scheduler = saved
    if previous_client is not None:
        application.state.alpaca_client = previous_client
    elif hasattr(application.state, "alpaca_client"):
        del application.state.alpaca_client
    if previous_scheduler is not None:
        application.state.scheduler_task = previous_scheduler
    elif hasattr(application.state, "scheduler_task"):
        del application.state.scheduler_task


def test_health_check_returns_503_when_dependencies_missing() -> None:
    saved = _save_health_state()
    if hasattr(application.state, "alpaca_client"):
        del application.state.alpaca_client
    if hasattr(application.state, "scheduler_task"):
        del application.state.scheduler_task
    try:
        response = health_check()

        assert response.status_code == status.HTTP_503_SERVICE_UNAVAILABLE
        body = json.loads(bytes(response.body))
        assert body["status"] == "degraded"
        assert body["checks"]["alpaca_client"] == "error"
        assert body["checks"]["scheduler"] == "error"
    finally:
        _restore_health_state(saved)


def test_health_check_returns_200_when_healthy() -> None:
    saved = _save_health_state()
    mock_alpaca = MagicMock()
    mock_alpaca.get_account = MagicMock()
    application.state.alpaca_client = mock_alpaca
    mock_scheduler = MagicMock()
    mock_scheduler.done.return_value = False
    application.state.scheduler_task = mock_scheduler
    try:
        response = health_check()

        assert response.status_code == status.HTTP_200_OK
        body = json.loads(bytes(response.body))
        assert body["status"] == "ok"
        assert body["checks"]["alpaca_client"] == "ok"
        assert body["checks"]["scheduler"] == "ok"
    finally:
        _restore_health_state(saved)


def test_health_check_returns_503_when_alpaca_client_raises() -> None:
    saved = _save_health_state()
    mock_alpaca = MagicMock()
    mock_alpaca.get_account.side_effect = APIError("forbidden")
    application.state.alpaca_client = mock_alpaca
    mock_scheduler = MagicMock()
    mock_scheduler.done.return_value = False
    application.state.scheduler_task = mock_scheduler
    try:
        response = health_check()

        assert response.status_code == status.HTTP_503_SERVICE_UNAVAILABLE
        body = json.loads(bytes(response.body))
        assert body["status"] == "degraded"
        assert body["checks"]["alpaca_client"] == "error"
        assert body["checks"]["scheduler"] == "ok"
    finally:
        _restore_health_state(saved)


def test_health_check_returns_503_when_alpaca_network_error() -> None:
    saved = _save_health_state()
    mock_alpaca = MagicMock()
    mock_alpaca.get_account.side_effect = ConnectionError("connection refused")
    application.state.alpaca_client = mock_alpaca
    mock_scheduler = MagicMock()
    mock_scheduler.done.return_value = False
    application.state.scheduler_task = mock_scheduler
    try:
        response = health_check()

        assert response.status_code == status.HTTP_503_SERVICE_UNAVAILABLE
        body = json.loads(bytes(response.body))
        assert body["checks"]["alpaca_client"] == "error"
    finally:
        _restore_health_state(saved)


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
        lock = asyncio.Lock()
        await lock.acquire()
        with patch("portfolio_manager.server._rebalance_lock", lock):
            return await create_portfolio()

    response = asyncio.run(run())

    assert response.status_code == status.HTTP_409_CONFLICT


def test_create_portfolio_calls_run_rebalance() -> None:
    mock_alpaca = MagicMock()
    mock_response = MagicMock(spec=Response)
    mock_response.status_code = status.HTTP_200_OK
    mock_run = AsyncMock(return_value=mock_response)

    mock_configuration = Configuration()
    application.state.alpaca_client = mock_alpaca
    application.state.configuration = mock_configuration

    async def run() -> Response:
        lock = asyncio.Lock()
        with (
            patch("portfolio_manager.server._rebalance_lock", lock),
            patch("portfolio_manager.server.run_rebalance", mock_run),
        ):
            return await create_portfolio()

    response = asyncio.run(run())

    assert response.status_code == status.HTTP_200_OK
    mock_run.assert_called_once_with(mock_alpaca, mock_configuration)
