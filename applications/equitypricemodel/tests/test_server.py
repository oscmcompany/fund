from unittest.mock import MagicMock, patch

import pytest
from botocore.exceptions import ClientError
from equitypricemodel.server import (
    MODEL_VERSION_SSM_PARAMETER,
    _resolve_artifact_key,
)


def _make_ssm_response(value: str) -> dict:
    return {"Parameter": {"Value": value}}


def _make_client_error(code: str) -> ClientError:
    return ClientError(
        error_response={"Error": {"Code": code, "Message": code}},
        operation_name="GetParameter",
    )


@patch("equitypricemodel.server.find_latest_artifact_key")
@patch("equitypricemodel.server.boto3.client")
def test_resolve_artifact_key_pinned_version(
    mock_boto_client: MagicMock,
    mock_find_latest: MagicMock,
) -> None:
    ssm_mock = MagicMock()
    ssm_mock.get_parameter.return_value = _make_ssm_response(
        "equitypricemodel-trainer-2026-01-14-15-00-26-204"
    )
    mock_boto_client.return_value = ssm_mock

    s3_mock = MagicMock()
    result = _resolve_artifact_key(
        s3_client=s3_mock,
        bucket="my-bucket",
        artifact_path="artifacts/",
    )

    expected = (
        "artifacts/equitypricemodel-trainer-2026-01-14-15-00-26-204/output/model.tar.gz"
    )
    assert result == expected
    mock_find_latest.assert_not_called()


@patch("equitypricemodel.server.find_latest_artifact_key")
@patch("equitypricemodel.server.boto3.client")
def test_resolve_artifact_key_pinned_version_normalizes_path(
    mock_boto_client: MagicMock,
    mock_find_latest: MagicMock,
) -> None:
    ssm_mock = MagicMock()
    ssm_mock.get_parameter.return_value = _make_ssm_response("some-version")
    mock_boto_client.return_value = ssm_mock

    s3_mock = MagicMock()
    result = _resolve_artifact_key(
        s3_client=s3_mock,
        bucket="my-bucket",
        artifact_path="artifacts",  # no trailing slash
    )

    assert result == "artifacts/some-version/output/model.tar.gz"
    mock_find_latest.assert_not_called()


@patch("equitypricemodel.server.find_latest_artifact_key")
@patch("equitypricemodel.server.boto3.client")
def test_resolve_artifact_key_sentinel_latest_falls_back(
    mock_boto_client: MagicMock,
    mock_find_latest: MagicMock,
) -> None:
    ssm_mock = MagicMock()
    ssm_mock.get_parameter.return_value = _make_ssm_response("latest")
    mock_boto_client.return_value = ssm_mock

    mock_find_latest.return_value = "artifacts/newest/output/model.tar.gz"
    s3_mock = MagicMock()

    result = _resolve_artifact_key(
        s3_client=s3_mock,
        bucket="my-bucket",
        artifact_path="artifacts/",
    )

    assert result == "artifacts/newest/output/model.tar.gz"
    mock_find_latest.assert_called_once_with(
        s3_client=s3_mock,
        bucket="my-bucket",
        prefix="artifacts/",
    )


@patch("equitypricemodel.server.find_latest_artifact_key")
@patch("equitypricemodel.server.boto3.client")
def test_resolve_artifact_key_empty_value_falls_back(
    mock_boto_client: MagicMock,
    mock_find_latest: MagicMock,
) -> None:
    ssm_mock = MagicMock()
    ssm_mock.get_parameter.return_value = _make_ssm_response("")
    mock_boto_client.return_value = ssm_mock

    mock_find_latest.return_value = "artifacts/newest/output/model.tar.gz"
    s3_mock = MagicMock()

    result = _resolve_artifact_key(
        s3_client=s3_mock,
        bucket="my-bucket",
        artifact_path="artifacts/",
    )

    assert result == "artifacts/newest/output/model.tar.gz"
    mock_find_latest.assert_called_once()


@patch("equitypricemodel.server.find_latest_artifact_key")
@patch("equitypricemodel.server.boto3.client")
def test_resolve_artifact_key_ssm_parameter_not_found_falls_back(
    mock_boto_client: MagicMock,
    mock_find_latest: MagicMock,
) -> None:
    ssm_mock = MagicMock()
    ssm_mock.get_parameter.side_effect = _make_client_error("ParameterNotFound")
    mock_boto_client.return_value = ssm_mock

    mock_find_latest.return_value = "artifacts/newest/output/model.tar.gz"
    s3_mock = MagicMock()

    result = _resolve_artifact_key(
        s3_client=s3_mock,
        bucket="my-bucket",
        artifact_path="artifacts/",
    )

    assert result == "artifacts/newest/output/model.tar.gz"
    mock_find_latest.assert_called_once()


@patch("equitypricemodel.server.find_latest_artifact_key")
@patch("equitypricemodel.server.boto3.client")
def test_resolve_artifact_key_ssm_access_denied_falls_back(
    mock_boto_client: MagicMock,
    mock_find_latest: MagicMock,
) -> None:
    ssm_mock = MagicMock()
    ssm_mock.get_parameter.side_effect = _make_client_error("AccessDeniedException")
    mock_boto_client.return_value = ssm_mock

    mock_find_latest.return_value = "artifacts/newest/output/model.tar.gz"
    s3_mock = MagicMock()

    result = _resolve_artifact_key(
        s3_client=s3_mock,
        bucket="my-bucket",
        artifact_path="artifacts/",
    )

    assert result == "artifacts/newest/output/model.tar.gz"
    mock_find_latest.assert_called_once()


@patch("equitypricemodel.server.boto3.client")
def test_resolve_artifact_key_reads_correct_ssm_parameter(
    mock_boto_client: MagicMock,
) -> None:
    ssm_mock = MagicMock()
    ssm_mock.get_parameter.return_value = _make_ssm_response("some-version")
    mock_boto_client.return_value = ssm_mock

    s3_mock = MagicMock()
    _resolve_artifact_key(
        s3_client=s3_mock,
        bucket="my-bucket",
        artifact_path="artifacts/",
    )

    ssm_mock.get_parameter.assert_called_once_with(Name=MODEL_VERSION_SSM_PARAMETER)


@pytest.mark.parametrize(
    ("version", "artifact_path", "expected_key"),
    [
        (
            "v1",
            "artifacts/",
            "artifacts/v1/output/model.tar.gz",
        ),
        (
            "/v1",  # leading slash stripped
            "artifacts/",
            "artifacts/v1/output/model.tar.gz",
        ),
        (
            "v1",
            "artifacts",  # no trailing slash
            "artifacts/v1/output/model.tar.gz",
        ),
    ],
)
@patch("equitypricemodel.server.find_latest_artifact_key")
@patch("equitypricemodel.server.boto3.client")
def test_resolve_artifact_key_path_normalization(
    mock_boto_client: MagicMock,
    mock_find_latest: MagicMock,
    version: str,
    artifact_path: str,
    expected_key: str,
) -> None:
    ssm_mock = MagicMock()
    ssm_mock.get_parameter.return_value = _make_ssm_response(version)
    mock_boto_client.return_value = ssm_mock

    s3_mock = MagicMock()
    result = _resolve_artifact_key(
        s3_client=s3_mock,
        bucket="my-bucket",
        artifact_path=artifact_path,
    )

    assert result == expected_key
    mock_find_latest.assert_not_called()
