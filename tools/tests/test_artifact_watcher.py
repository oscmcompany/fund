import os
from pathlib import Path
from unittest.mock import MagicMock, patch

from tools.artifact_watcher import (
    get_latest_artifact_key,
    read_last_key,
    restart_ensemble_manager,
    write_last_key,
)


def test_get_latest_artifact_key_returns_latest(tmp_path: Path) -> None:
    mock_s3 = MagicMock()
    mock_paginator = MagicMock()
    mock_paginator.paginate.return_value = [
        {
            "CommonPrefixes": [
                {"Prefix": "artifacts/tide/2026-01-01/"},
                {"Prefix": "artifacts/tide/2026-02-01/"},
            ]
        }
    ]
    mock_s3.get_paginator.return_value = mock_paginator
    mock_s3.head_object.return_value = {}

    with patch("tools.artifact_watcher.boto3") as mock_boto3:
        mock_boto3.client.return_value = mock_s3
        result = get_latest_artifact_key(
            bucket="test-bucket",
            prefix="artifacts/tide/",
        )

    assert result == "artifacts/tide/2026-02-01/output/model.tar.gz"


def test_get_latest_artifact_key_returns_none_when_empty() -> None:
    mock_s3 = MagicMock()
    mock_paginator = MagicMock()
    mock_paginator.paginate.return_value = [{"CommonPrefixes": []}]
    mock_s3.get_paginator.return_value = mock_paginator

    with patch("tools.artifact_watcher.boto3") as mock_boto3:
        mock_boto3.client.return_value = mock_s3
        result = get_latest_artifact_key(
            bucket="test-bucket",
            prefix="artifacts/tide/",
        )

    assert result is None


def test_read_last_key_returns_none_when_no_file(tmp_path: Path) -> None:
    with patch("tools.artifact_watcher.STATE_FILE", tmp_path / "nonexistent"):
        result = read_last_key()

    assert result is None


def test_write_and_read_last_key(tmp_path: Path) -> None:
    state_file = tmp_path / "last-key"
    with patch("tools.artifact_watcher.STATE_FILE", state_file):
        write_last_key("artifacts/tide/2026-02-01/output/model.tar.gz")
        result = read_last_key()

    assert result == "artifacts/tide/2026-02-01/output/model.tar.gz"


def test_restart_ensemble_manager_sends_sigterm() -> None:
    with patch("tools.artifact_watcher.subprocess.run") as mock_run:
        mock_run.return_value = MagicMock(stdout="12345\n")
        with patch("tools.artifact_watcher.os.kill") as mock_kill:
            restart_ensemble_manager()
            mock_kill.assert_called_once_with(12345, 15)


def test_restart_ensemble_manager_no_process() -> None:
    with patch("tools.artifact_watcher.subprocess.run") as mock_run:
        mock_run.return_value = MagicMock(stdout="")
        with patch("tools.artifact_watcher.os.kill") as mock_kill:
            restart_ensemble_manager()
            mock_kill.assert_not_called()
