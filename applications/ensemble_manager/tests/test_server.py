import asyncio
import json
import os
import tempfile
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import polars as pl
import pytest
from botocore.exceptions import ClientError
from ensemble_manager.server import (
    _artifact_polling_task,
    _compute_predictions,
    _fetch_equity_details,
    _inference_lock,
    _insert_predictions,
    _prepare_inference_data,
    _resolve_artifact_key,
    _run_predictions_from_event,
    _sync_run_metadata,
    application,
    cleanup_model_directory,
    download_and_extract_artifacts,
    find_latest_artifact_key,
)
from fastapi import FastAPI, status
from fastapi.testclient import TestClient


def test_resolve_artifact_key_uses_latest_by_default() -> None:
    mock_s3 = MagicMock()

    with (
        patch.dict("os.environ", {}, clear=False),
        patch("ensemble_manager.server.find_latest_artifact_key") as mock_find,
    ):
        if "MODEL_VERSION" in os.environ:
            del os.environ["MODEL_VERSION"]
        mock_find.return_value = "artifacts/model-2026/output/model.tar.gz"
        result = _resolve_artifact_key(
            s3_client=mock_s3,
            bucket="test-bucket",
            artifact_path="artifacts/",
        )

    assert result == "artifacts/model-2026/output/model.tar.gz"
    mock_find.assert_called_once()


def test_resolve_artifact_key_uses_env_version() -> None:
    mock_s3 = MagicMock()

    with patch.dict(
        "os.environ",
        {"MODEL_VERSION": "equitypricemodel-trainer-2026-01-15"},
    ):
        result = _resolve_artifact_key(
            s3_client=mock_s3,
            bucket="test-bucket",
            artifact_path="artifacts/",
        )

    expected = "artifacts/equitypricemodel-trainer-2026-01-15/output/model.tar.gz"
    assert result == expected


def test_resolve_artifact_key_env_tar_gz_value() -> None:
    mock_s3 = MagicMock()

    with patch.dict(
        "os.environ",
        {"MODEL_VERSION": "custom/path/model.tar.gz"},
    ):
        result = _resolve_artifact_key(
            s3_client=mock_s3,
            bucket="test-bucket",
            artifact_path="artifacts/",
        )

    assert result == "custom/path/model.tar.gz"


def test_resolve_artifact_key_explicit_tar_gz_path() -> None:
    mock_s3 = MagicMock()

    with patch.dict("os.environ", {"MODEL_VERSION": "latest"}):
        result = _resolve_artifact_key(
            s3_client=mock_s3,
            bucket="test-bucket",
            artifact_path="artifacts/specific/model.tar.gz",
        )

    assert result == "artifacts/specific/model.tar.gz"


def test_run_predictions_from_event_skips_when_locked() -> None:
    async def run() -> None:
        await _inference_lock.acquire()
        try:
            await _run_predictions_from_event(MagicMock())
        finally:
            _inference_lock.release()

    asyncio.run(run())


def test_compute_predictions_all_tickers_dropped_returns_none() -> None:
    data = pl.DataFrame({"ticker": ["TSLA"], "close_price": [15.0]})
    empty_data = pl.DataFrame({"ticker": pl.Series([], dtype=pl.Utf8)})
    mock_tide_data = MagicMock()
    mock_tide_data.mappings = {"ticker": {"AAPL": 0, "MSFT": 1}}

    with (
        patch("ensemble_manager.server.Data.load") as mock_data_load,
        patch("ensemble_manager.server.filter_to_trained_tickers") as mock_filter,
        patch("ensemble_manager.server.prediction_errors_total") as mock_errors,
    ):
        mock_data_load.return_value = mock_tide_data
        mock_filter.return_value = empty_data
        mock_errors_instance = MagicMock()
        mock_errors.labels.return_value = mock_errors_instance

        result = _compute_predictions(
            MagicMock(), "/fake/dir", data, __import__("datetime").datetime.now()
        )

    assert result is None
    mock_errors.labels.assert_called_with(stage="ticker_filtering")
    mock_errors_instance.inc.assert_called_once()


def test_compute_predictions_apply_preprocessing_fails_returns_none() -> None:
    data = pl.DataFrame({"ticker": ["AAPL"], "close_price": [15.0]})
    mock_tide_data = MagicMock()
    mock_tide_data.mappings = {"ticker": {"AAPL": 0}}
    mock_tide_data.apply_and_set_data.side_effect = ValueError("unknown category")

    with (
        patch("ensemble_manager.server.Data.load") as mock_data_load,
        patch("ensemble_manager.server.filter_to_trained_tickers") as mock_filter,
        patch("ensemble_manager.server.prediction_errors_total") as mock_errors,
    ):
        mock_data_load.return_value = mock_tide_data
        mock_filter.return_value = data
        mock_errors_instance = MagicMock()
        mock_errors.labels.return_value = mock_errors_instance

        result = _compute_predictions(
            MagicMock(), "/fake/dir", data, __import__("datetime").datetime.now()
        )

    assert result is None
    mock_errors.labels.assert_called_with(stage="apply_preprocessing")
    mock_errors_instance.inc.assert_called_once()


def test_health_check_returns_503_when_model_not_loaded() -> None:
    previous_model = getattr(application.state, "tide_model", None)
    previous_directory = getattr(application.state, "model_directory", None)
    application.state.tide_model = None
    application.state.model_directory = "/nonexistent"
    try:
        client = TestClient(application, raise_server_exceptions=False)
        with patch("ensemble_manager.server.Model.load"):
            response = client.get("/health")
        assert response.status_code == status.HTTP_503_SERVICE_UNAVAILABLE
        body = response.json()
        assert body["status"] == "degraded"
        assert body["checks"]["model"] == "error"
    finally:
        application.state.tide_model = previous_model
        application.state.model_directory = previous_directory


def test_health_check_returns_200_when_model_loaded() -> None:
    previous_model = getattr(application.state, "tide_model", None)
    previous_directory = getattr(application.state, "model_directory", None)
    with tempfile.TemporaryDirectory() as model_dir:
        application.state.tide_model = MagicMock()
        application.state.model_directory = model_dir
        try:
            client = TestClient(application, raise_server_exceptions=False)
            with patch("ensemble_manager.server.Model.load"):
                response = client.get("/health")
            assert response.status_code == status.HTTP_200_OK
            body = response.json()
            assert body["status"] == "ok"
            assert body["checks"]["model"] == "ok"
            assert body["checks"]["model_directory"] == "ok"
        finally:
            application.state.tide_model = previous_model
            application.state.model_directory = previous_directory


def test_metrics_endpoint_returns_200() -> None:
    client = TestClient(application, raise_server_exceptions=False)
    with patch("ensemble_manager.server.Model.load"):
        response = client.get("/metrics")
    assert response.status_code == status.HTTP_200_OK


def test_find_latest_artifact_key_no_folders_raises() -> None:
    mock_s3 = MagicMock()
    mock_paginator = MagicMock()
    mock_paginator.paginate.return_value = [{"CommonPrefixes": []}]
    mock_s3.get_paginator.return_value = mock_paginator

    with pytest.raises(ValueError, match="No artifact folders found"):
        find_latest_artifact_key(
            s3_client=mock_s3,
            bucket="test-bucket",
            prefix="artifacts/",
        )


def test_find_latest_artifact_key_no_artifact_in_folders_raises() -> None:
    mock_s3 = MagicMock()
    mock_paginator = MagicMock()
    mock_paginator.paginate.return_value = [
        {"CommonPrefixes": [{"Prefix": "artifacts/2026-01-01/"}]}
    ]
    mock_s3.get_paginator.return_value = mock_paginator
    mock_s3.head_object.side_effect = ClientError(
        error_response={"Error": {"Code": "404", "Message": "Not Found"}},
        operation_name="HeadObject",
    )

    with pytest.raises(ValueError, match=r"No model\.tar\.gz found"):
        find_latest_artifact_key(
            s3_client=mock_s3,
            bucket="test-bucket",
            prefix="artifacts/",
        )


def test_find_latest_artifact_key_returns_latest_folder_artifact() -> None:
    mock_s3 = MagicMock()
    mock_paginator = MagicMock()
    mock_paginator.paginate.return_value = [
        {
            "CommonPrefixes": [
                {"Prefix": "artifacts/2026-01-01/"},
                {"Prefix": "artifacts/2026-02-01/"},
            ]
        }
    ]
    mock_s3.get_paginator.return_value = mock_paginator
    mock_s3.head_object.return_value = {}

    result = find_latest_artifact_key(
        s3_client=mock_s3,
        bucket="test-bucket",
        prefix="artifacts/",
    )

    assert result == "artifacts/2026-02-01/output/model.tar.gz"


def test_cleanup_model_directory_removes_existing_directory() -> None:
    with tempfile.TemporaryDirectory() as parent_dir:
        target = Path(parent_dir) / "model_artifacts"
        target.mkdir()
        (target / "model.bin").write_text("data")

        cleanup_model_directory(str(target))

        assert not target.exists()


def test_cleanup_model_directory_skips_dot() -> None:
    cleanup_model_directory(".")


def test_prepare_inference_data_returns_consolidated_dataframe() -> None:
    equity_bars = pl.DataFrame(
        {
            "ticker": ["AAPL", "AAPL", "MSFT"],
            "timestamp": [1_700_000_000_000, 1_700_086_400_000, 1_700_000_000_000],
            "open_price": [150.0, 151.0, 300.0],
            "high_price": [155.0, 156.0, 305.0],
            "low_price": [149.0, 150.0, 299.0],
            "close_price": [152.0, 153.0, 302.0],
            "volume": [2_000_000, 2_100_000, 1_500_000],
            "volume_weighted_average_price": [151.5, 152.5, 301.0],
            "transactions": [None, None, None],
        }
    )
    equity_details = pl.DataFrame(
        {
            "ticker": ["AAPL", "MSFT"],
            "sector": ["TECHNOLOGY", "TECHNOLOGY"],
            "industry": ["SOFTWARE", "SOFTWARE"],
        }
    )

    result = _prepare_inference_data(equity_bars, equity_details)

    assert "ticker" in result.columns
    assert "close_price" in result.columns
    assert "sector" in result.columns
    assert "industry" in result.columns
    assert set(result["ticker"].to_list()).issubset({"AAPL", "MSFT"})


# --- Artifact polling and hot-swap tests ---

_SWAP_ITERATIONS = 2


def _make_cancel_sleep(max_iterations: int = 1):  # noqa: ANN202
    iteration = {"count": 0}

    async def controlled_sleep(_seconds: float) -> None:
        iteration["count"] += 1
        if iteration["count"] > max_iterations:
            raise asyncio.CancelledError

    return controlled_sleep


def test_artifact_polling_detects_new_key_and_swaps() -> None:
    async def _run() -> None:
        app = MagicMock(spec=FastAPI)
        app.state = MagicMock()
        app.state.current_artifact_key = "old/output/model.tar.gz"
        app.state.model_directory = "/old/dir"

        mock_model = MagicMock()

        with (
            patch.dict(
                "os.environ",
                {"AWS_S3_MODEL_ARTIFACTS_BUCKET_NAME": "bucket"},
            ),
            patch("ensemble_manager.server.boto3.client"),
            patch(
                "ensemble_manager.server.asyncio.sleep",
                side_effect=_make_cancel_sleep(_SWAP_ITERATIONS),
            ),
            patch("ensemble_manager.server.asyncio.to_thread") as mock_to_thread,
            patch("ensemble_manager.server.cleanup_model_directory"),
            patch("ensemble_manager.server._sync_run_metadata"),
            patch("ensemble_manager.server.model_load_timestamp"),
        ):

            async def _fake(func: object, *_a: object, **_kw: object) -> object:
                if func is _resolve_artifact_key:
                    return "new/output/model.tar.gz"
                return mock_model

            mock_to_thread.side_effect = _fake

            with pytest.raises(asyncio.CancelledError):
                await _artifact_polling_task(app)

        assert app.state.tide_model == mock_model
        assert app.state.current_artifact_key == "new/output/model.tar.gz"

    asyncio.run(_run())


def test_artifact_polling_same_key_no_swap() -> None:
    async def _run() -> None:
        app = MagicMock(spec=FastAPI)
        app.state = MagicMock()
        app.state.current_artifact_key = "current/output/model.tar.gz"

        with (
            patch.dict(
                "os.environ",
                {"AWS_S3_MODEL_ARTIFACTS_BUCKET_NAME": "bucket"},
            ),
            patch("ensemble_manager.server.boto3.client"),
            patch(
                "ensemble_manager.server.asyncio.sleep",
                side_effect=_make_cancel_sleep(),
            ),
            patch("ensemble_manager.server.asyncio.to_thread") as mock_to_thread,
        ):

            async def _fake(_func: object, *_a: object, **_kw: object) -> str:
                return "current/output/model.tar.gz"

            mock_to_thread.side_effect = _fake

            with pytest.raises(asyncio.CancelledError):
                await _artifact_polling_task(app)

        assert app.state.current_artifact_key == "current/output/model.tar.gz"

    asyncio.run(_run())


def test_artifact_polling_download_failure_cleans_up() -> None:
    async def _run() -> None:
        app = MagicMock(spec=FastAPI)
        app.state = MagicMock()
        app.state.current_artifact_key = "old/output/model.tar.gz"

        with (
            patch.dict(
                "os.environ",
                {"AWS_S3_MODEL_ARTIFACTS_BUCKET_NAME": "bucket"},
            ),
            patch("ensemble_manager.server.boto3.client"),
            patch(
                "ensemble_manager.server.asyncio.sleep",
                side_effect=_make_cancel_sleep(),
            ),
            patch("ensemble_manager.server.asyncio.to_thread") as mock_to_thread,
            patch("ensemble_manager.server.cleanup_model_directory") as mock_cleanup,
        ):

            async def _fake(func: object, *_a: object, **_kw: object) -> object:
                if func is _resolve_artifact_key:
                    return "new/output/model.tar.gz"
                if func is download_and_extract_artifacts:
                    message = "S3 download failed"
                    raise ConnectionError(message)
                return None

            mock_to_thread.side_effect = _fake

            with pytest.raises(asyncio.CancelledError):
                await _artifact_polling_task(app)

        assert app.state.current_artifact_key == "old/output/model.tar.gz"
        mock_cleanup.assert_called()

    asyncio.run(_run())


def test_artifact_polling_transient_s3_error_continues() -> None:
    async def _run() -> None:
        app = MagicMock(spec=FastAPI)
        app.state = MagicMock()
        app.state.current_artifact_key = "old/output/model.tar.gz"

        with (
            patch.dict(
                "os.environ",
                {"AWS_S3_MODEL_ARTIFACTS_BUCKET_NAME": "bucket"},
            ),
            patch("ensemble_manager.server.boto3.client"),
            patch(
                "ensemble_manager.server.asyncio.sleep",
                side_effect=_make_cancel_sleep(),
            ),
            patch("ensemble_manager.server.asyncio.to_thread") as mock_to_thread,
        ):

            async def _fake(_func: object, *_a: object, **_kw: object) -> None:
                raise ClientError(
                    error_response={"Error": {"Code": "503", "Message": "Slow Down"}},
                    operation_name="ListObjectsV2",
                )

            mock_to_thread.side_effect = _fake

            with pytest.raises(asyncio.CancelledError):
                await _artifact_polling_task(app)

        assert app.state.current_artifact_key == "old/output/model.tar.gz"

    asyncio.run(_run())


def test_artifact_polling_no_bucket_returns_immediately() -> None:
    async def _run() -> None:
        app = MagicMock(spec=FastAPI)

        with patch.dict("os.environ", {}, clear=False):
            if "AWS_S3_MODEL_ARTIFACTS_BUCKET_NAME" in os.environ:
                del os.environ["AWS_S3_MODEL_ARTIFACTS_BUCKET_NAME"]
            await _artifact_polling_task(app)

    asyncio.run(_run())


def test_sync_run_metadata_skips_without_database_url() -> None:
    async def _run() -> None:
        mock_s3 = MagicMock()
        with patch.dict("os.environ", {}, clear=False):
            if "DATABASE_URL" in os.environ:
                del os.environ["DATABASE_URL"]
            await _sync_run_metadata(
                s3_client=mock_s3,
                bucket="test-bucket",
                artifact_key="artifacts/2026/output/model.tar.gz",
            )

        mock_s3.get_object.assert_not_called()

    asyncio.run(_run())


def test_sync_run_metadata_handles_missing_metadata_json() -> None:
    async def _run() -> None:
        mock_s3 = MagicMock()
        mock_s3.get_object.side_effect = ClientError(
            error_response={"Error": {"Code": "404", "Message": "Not Found"}},
            operation_name="GetObject",
        )

        with patch.dict(
            "os.environ",
            {"DATABASE_URL": "postgresql://localhost/test"},
        ):
            await _sync_run_metadata(
                s3_client=mock_s3,
                bucket="test-bucket",
                artifact_key="artifacts/2026/output/model.tar.gz",
            )

    asyncio.run(_run())


def test_sync_run_metadata_handles_empty_run_id() -> None:
    async def _run() -> None:
        mock_s3 = MagicMock()
        mock_body = MagicMock()
        mock_body.read.return_value = json.dumps({"some_key": "value"}).encode()
        mock_s3.get_object.return_value = {"Body": mock_body}

        with (
            patch.dict(
                "os.environ",
                {"DATABASE_URL": "postgresql://localhost/test"},
            ),
            patch("ensemble_manager.server.get_pool") as mock_pool,
        ):
            await _sync_run_metadata(
                s3_client=mock_s3,
                bucket="test-bucket",
                artifact_key="artifacts/2026/output/model.tar.gz",
            )

        mock_pool.assert_not_called()

    asyncio.run(_run())


# --- _fetch_equity_details ---


def test_fetch_equity_details_returns_dataframe() -> None:
    csv_content = (
        b"ticker,sector,industry\nAAPL,TECHNOLOGY,SOFTWARE\nMSFT,TECHNOLOGY,SOFTWARE\n"
    )
    mock_body = MagicMock()
    mock_body.read.return_value = csv_content
    mock_s3 = MagicMock()
    mock_s3.get_object.return_value = {"Body": mock_body}

    async def _run() -> None:
        result = await _fetch_equity_details(mock_s3, "test-bucket")
        assert "ticker" in result.columns
        assert "sector" in result.columns
        assert len(result) == 2  # noqa: PLR2004

    asyncio.run(_run())


def test_fetch_equity_details_raises_on_s3_error() -> None:
    mock_s3 = MagicMock()
    mock_s3.get_object.side_effect = Exception("S3 error")

    async def _run() -> None:
        with pytest.raises(Exception, match="S3 error"):
            await _fetch_equity_details(mock_s3, "test-bucket")

    asyncio.run(_run())


# --- _insert_predictions ---


def test_insert_predictions_executes_upsert() -> None:
    predictions = pl.DataFrame(
        {
            "ticker": ["AAPL"],
            "timestamp": [1_700_000_000_000],
            "quantile_10": [140.0],
            "quantile_50": [150.0],
            "quantile_90": [160.0],
        }
    )
    mock_cursor = AsyncMock()
    mock_connection = MagicMock()
    mock_connection.cursor.return_value.__aenter__ = AsyncMock(return_value=mock_cursor)
    mock_connection.cursor.return_value.__aexit__ = AsyncMock(return_value=None)
    mock_pool = MagicMock()
    mock_pool.connection.return_value.__aenter__ = AsyncMock(
        return_value=mock_connection
    )
    mock_pool.connection.return_value.__aexit__ = AsyncMock(return_value=None)

    async def _run() -> None:
        with patch(
            "ensemble_manager.server.get_pool", AsyncMock(return_value=mock_pool)
        ):
            await _insert_predictions(predictions, "corr-id", "run-id")

    asyncio.run(_run())
    mock_cursor.executemany.assert_awaited_once()
