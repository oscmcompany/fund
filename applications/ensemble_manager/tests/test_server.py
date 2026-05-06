import io
import os
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, call, patch

import polars as pl
import pytest
from botocore.exceptions import ClientError
from ensemble_manager.server import (
    _resolve_artifact_key,
    application,
    cleanup_model_directory,
    find_latest_artifact_key,
    parse_responses,
)
from fastapi import status
from fastapi.testclient import TestClient


def test_create_predictions_some_tickers_dropped_continues() -> None:
    data = pl.DataFrame(
        {
            "ticker": ["AAPL", "AAPL"],
            "close_price": [15.0, 20.0],
        }
    )
    mock_tide_data = MagicMock()
    mock_tide_data.mappings = {"ticker": {"AAPL": 0, "MSFT": 1}}

    application.state.model_directory = "/fake/model/dir"

    with (
        patch("ensemble_manager.server.requests.get") as mock_requests_get,
        patch("ensemble_manager.server.parse_responses") as mock_parse,
        patch("ensemble_manager.server.Data.load") as mock_data_load,
        patch("ensemble_manager.server.Model.load"),
        patch("ensemble_manager.server.filter_to_trained_tickers") as mock_filter,
        patch("ensemble_manager.server.prediction_errors_total") as mock_errors,
    ):
        mock_response = MagicMock()
        mock_response.raise_for_status.return_value = None
        mock_requests_get.return_value = mock_response
        mock_parse.return_value = data
        mock_data_load.return_value = mock_tide_data
        mock_filter.return_value = data

        mock_tide_data.get_dataset.return_value = MagicMock(__len__=lambda _: 0)

        client = TestClient(application, raise_server_exceptions=False)
        response = client.post("/predictions")

    assert response.status_code == status.HTTP_500_INTERNAL_SERVER_ERROR
    assert call(stage="ticker_filtering") not in mock_errors.labels.call_args_list


def test_create_predictions_all_tickers_dropped_returns_500() -> None:
    data = pl.DataFrame(
        {
            "ticker": ["TSLA", "TSLA"],
            "close_price": [15.0, 20.0],
        }
    )
    empty_data = pl.DataFrame({"ticker": pl.Series([], dtype=pl.Utf8)})
    mock_tide_data = MagicMock()
    mock_tide_data.mappings = {"ticker": {"AAPL": 0, "MSFT": 1}}

    application.state.model_directory = "/fake/model/dir"

    with (
        patch("ensemble_manager.server.requests.get") as mock_requests_get,
        patch("ensemble_manager.server.parse_responses") as mock_parse,
        patch("ensemble_manager.server.Data.load") as mock_data_load,
        patch("ensemble_manager.server.Model.load"),
        patch("ensemble_manager.server.filter_to_trained_tickers") as mock_filter,
        patch("ensemble_manager.server.prediction_errors_total") as mock_errors,
    ):
        mock_response = MagicMock()
        mock_response.raise_for_status.return_value = None
        mock_requests_get.return_value = mock_response
        mock_parse.return_value = data
        mock_data_load.return_value = mock_tide_data
        mock_filter.return_value = empty_data

        mock_errors_instance = MagicMock()
        mock_errors.labels.return_value = mock_errors_instance

        client = TestClient(application, raise_server_exceptions=False)
        response = client.post("/predictions")

    assert response.status_code == status.HTTP_500_INTERNAL_SERVER_ERROR
    mock_errors.labels.assert_called_with(stage="ticker_filtering")
    mock_errors_instance.inc.assert_called_once()


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


def test_create_predictions_fetch_equity_bars_fails_returns_500() -> None:
    application.state.model_directory = "/fake/model/dir"

    with (
        patch("ensemble_manager.server.requests.get") as mock_requests_get,
        patch("ensemble_manager.server.prediction_errors_total") as mock_errors,
    ):
        mock_errors_instance = MagicMock()
        mock_errors.labels.return_value = mock_errors_instance
        mock_requests_get.side_effect = Exception("connection refused")

        client = TestClient(application, raise_server_exceptions=False)
        response = client.post("/predictions")

    assert response.status_code == status.HTTP_500_INTERNAL_SERVER_ERROR
    mock_errors.labels.assert_called_with(stage="fetch_equity_bars")
    mock_errors_instance.inc.assert_called_once()


def test_create_predictions_fetch_equity_details_fails_returns_500() -> None:
    application.state.model_directory = "/fake/model/dir"

    with (
        patch("ensemble_manager.server.requests.get") as mock_requests_get,
        patch("ensemble_manager.server.prediction_errors_total") as mock_errors,
    ):
        mock_errors_instance = MagicMock()
        mock_errors.labels.return_value = mock_errors_instance

        equity_bars_response = MagicMock()
        equity_bars_response.raise_for_status.return_value = None
        mock_requests_get.side_effect = [
            equity_bars_response,
            Exception("equity details unavailable"),
        ]

        client = TestClient(application, raise_server_exceptions=False)
        response = client.post("/predictions")

    assert response.status_code == status.HTTP_500_INTERNAL_SERVER_ERROR
    mock_errors.labels.assert_called_with(stage="fetch_equity_details")
    mock_errors_instance.inc.assert_called_once()


def test_create_predictions_parse_responses_fails_returns_500() -> None:
    application.state.model_directory = "/fake/model/dir"

    with (
        patch("ensemble_manager.server.requests.get") as mock_requests_get,
        patch("ensemble_manager.server.parse_responses") as mock_parse,
        patch("ensemble_manager.server.prediction_errors_total") as mock_errors,
    ):
        mock_errors_instance = MagicMock()
        mock_errors.labels.return_value = mock_errors_instance

        mock_response = MagicMock()
        mock_response.raise_for_status.return_value = None
        mock_requests_get.return_value = mock_response
        mock_parse.side_effect = Exception("parse failed")

        client = TestClient(application, raise_server_exceptions=False)
        response = client.post("/predictions")

    assert response.status_code == status.HTTP_500_INTERNAL_SERVER_ERROR
    mock_errors.labels.assert_called_with(stage="parse_responses")
    mock_errors_instance.inc.assert_called_once()


def test_create_predictions_apply_preprocessing_fails_returns_500() -> None:
    data = pl.DataFrame(
        {
            "ticker": ["AAPL", "AAPL"],
            "close_price": [15.0, 20.0],
        }
    )
    mock_tide_data = MagicMock()
    mock_tide_data.mappings = {"ticker": {"AAPL": 0, "MSFT": 1}}
    mock_tide_data.apply_and_set_data.side_effect = ValueError("unknown category")

    application.state.model_directory = "/fake/model/dir"

    with (
        patch("ensemble_manager.server.requests.get") as mock_requests_get,
        patch("ensemble_manager.server.parse_responses") as mock_parse,
        patch("ensemble_manager.server.Data.load") as mock_data_load,
        patch("ensemble_manager.server.Model.load"),
        patch("ensemble_manager.server.filter_to_trained_tickers") as mock_filter,
        patch("ensemble_manager.server.prediction_errors_total") as mock_errors,
    ):
        mock_response = MagicMock()
        mock_response.raise_for_status.return_value = None
        mock_requests_get.return_value = mock_response
        mock_parse.return_value = data
        mock_data_load.return_value = mock_tide_data
        mock_filter.return_value = data

        mock_errors_instance = MagicMock()
        mock_errors.labels.return_value = mock_errors_instance

        client = TestClient(application, raise_server_exceptions=False)
        response = client.post("/predictions")

    assert response.status_code == status.HTTP_500_INTERNAL_SERVER_ERROR
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


def test_parse_responses_returns_consolidated_dataframe() -> None:
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
            "transactions": [100, 110, 90],
        }
    )
    equity_bars_bytes = io.BytesIO()
    equity_bars.write_parquet(equity_bars_bytes)

    equity_details_csv = (
        b"ticker,sector,industry\nAAPL,TECHNOLOGY,SOFTWARE\nMSFT,TECHNOLOGY,SOFTWARE\n"
    )

    equity_bars_response = MagicMock()
    equity_bars_response.content = equity_bars_bytes.getvalue()

    equity_details_response = MagicMock()
    equity_details_response.content = equity_details_csv

    result = parse_responses(
        equity_bars_response=equity_bars_response,
        equity_details_response=equity_details_response,
    )

    assert "ticker" in result.columns
    assert "close_price" in result.columns
    assert "sector" in result.columns
    assert "industry" in result.columns
    assert set(result["ticker"].to_list()).issubset({"AAPL", "MSFT"})
