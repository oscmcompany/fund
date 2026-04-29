from unittest.mock import MagicMock, call, patch

import polars as pl
from botocore.exceptions import ClientError
from ensemble_manager.server import _resolve_artifact_key, application
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
        client.post("/predictions")

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
    mock_ssm = MagicMock()
    mock_ssm.get_parameter.side_effect = ClientError(
        error_response={
            "Error": {
                "Code": "ParameterNotFound",
                "Message": "Parameter not found",
            }
        },
        operation_name="GetParameter",
    )

    with (
        patch("ensemble_manager.server.boto3") as mock_boto3,
        patch("ensemble_manager.server.find_latest_artifact_key") as mock_find,
    ):
        mock_boto3.client.return_value = mock_ssm
        mock_find.return_value = "artifacts/model-2026/output/model.tar.gz"
        result = _resolve_artifact_key(
            s3_client=mock_s3,
            bucket="test-bucket",
            artifact_path="artifacts/",
        )

    assert result == "artifacts/model-2026/output/model.tar.gz"
    mock_find.assert_called_once()


def test_resolve_artifact_key_uses_ssm_version() -> None:
    mock_s3 = MagicMock()
    mock_ssm = MagicMock()
    mock_ssm.get_parameter.return_value = {
        "Parameter": {"Value": "equitypricemodel-trainer-2026-01-15"}
    }

    with patch("ensemble_manager.server.boto3") as mock_boto3:
        mock_boto3.client.return_value = mock_ssm
        result = _resolve_artifact_key(
            s3_client=mock_s3,
            bucket="test-bucket",
            artifact_path="artifacts/",
        )

    expected = "artifacts/equitypricemodel-trainer-2026-01-15/output/model.tar.gz"
    assert result == expected


def test_resolve_artifact_key_ssm_tar_gz_value() -> None:
    mock_s3 = MagicMock()
    mock_ssm = MagicMock()
    mock_ssm.get_parameter.return_value = {
        "Parameter": {"Value": "custom/path/model.tar.gz"}
    }

    with patch("ensemble_manager.server.boto3") as mock_boto3:
        mock_boto3.client.return_value = mock_ssm
        result = _resolve_artifact_key(
            s3_client=mock_s3,
            bucket="test-bucket",
            artifact_path="artifacts/",
        )

    assert result == "custom/path/model.tar.gz"


def test_resolve_artifact_key_explicit_tar_gz_path() -> None:
    mock_s3 = MagicMock()
    mock_ssm = MagicMock()
    mock_ssm.get_parameter.return_value = {"Parameter": {"Value": "latest"}}

    with patch("ensemble_manager.server.boto3") as mock_boto3:
        mock_boto3.client.return_value = mock_ssm
        result = _resolve_artifact_key(
            s3_client=mock_s3,
            bucket="test-bucket",
            artifact_path="artifacts/specific/model.tar.gz",
        )

    assert result == "artifacts/specific/model.tar.gz"
