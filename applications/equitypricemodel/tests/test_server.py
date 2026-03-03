from unittest.mock import MagicMock, patch

from botocore.exceptions import ClientError
from equitypricemodel.server import _resolve_artifact_key


def test_resolve_artifact_key_uses_latest_by_default() -> None:
    mock_s3 = MagicMock()
    mock_ssm = MagicMock()
    mock_ssm.get_parameter.side_effect = ClientError.__new__(ClientError)

    with (
        patch("equitypricemodel.server.boto3") as mock_boto3,
        patch("equitypricemodel.server.find_latest_artifact_key") as mock_find,
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

    with patch("equitypricemodel.server.boto3") as mock_boto3:
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

    with patch("equitypricemodel.server.boto3") as mock_boto3:
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

    with patch("equitypricemodel.server.boto3") as mock_boto3:
        mock_boto3.client.return_value = mock_ssm
        result = _resolve_artifact_key(
            s3_client=mock_s3,
            bucket="test-bucket",
            artifact_path="artifacts/specific/model.tar.gz",
        )

    assert result == "artifacts/specific/model.tar.gz"
