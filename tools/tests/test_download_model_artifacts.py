from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

from tools.download_model_artifacts import download_model_artifacts


def test_download_model_artifacts_github_actions_selects_latest() -> None:
    mock_s3_client = MagicMock()
    mock_s3_client.list_objects_v2.return_value = {
        "Contents": [
            {
                "Key": "artifacts/run_20250601/output/model.tar.gz",
                "LastModified": datetime(2025, 6, 1, tzinfo=UTC),
            }
        ]
    }

    mock_tar = MagicMock()

    with (
        patch(
            "tools.download_model_artifacts.boto3.client",
            return_value=mock_s3_client,
        ),
        patch("tools.download_model_artifacts.os.makedirs"),
        patch("tools.download_model_artifacts.tarfile.open") as mock_tarfile_open,
    ):
        mock_tarfile_open.return_value.__enter__.return_value = mock_tar

        download_model_artifacts(
            application_name="equitypricemodel",
            artifacts_bucket="test-artifacts-bucket",
            github_actions_check=True,
        )

    mock_s3_client.list_objects_v2.assert_called_once_with(
        Bucket="test-artifacts-bucket",
        Prefix="artifacts/equitypricemodel",
    )
    mock_s3_client.download_file.assert_called_once_with(
        Bucket="test-artifacts-bucket",
        Key="artifacts/run_20250601/output/model.tar.gz",
        Filename="applications/equitypricemodel/src/equitypricemodel/model.tar.gz",
    )
    mock_tar.extractall.assert_called_once()
