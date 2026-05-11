from pathlib import Path
from typing import Any, cast
from unittest.mock import MagicMock, patch

import botocore.exceptions
import pytest
from tools.artifact_watcher import (
    get_latest_artifact_key,
    read_last_key,
    restart_ensemble_manager,
    run,
    write_last_key,
)

CLIENT_ERROR_RESPONSE = cast(
    "Any",
    {"Error": {"Code": "404", "Message": "Not Found"}},
)


def test_get_latest_artifact_key_returns_latest() -> None:
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


def test_get_latest_artifact_key_skips_client_error_folders() -> None:
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
    mock_s3.head_object.side_effect = [
        botocore.exceptions.ClientError(CLIENT_ERROR_RESPONSE, "HeadObject"),
        {},
    ]

    with patch("tools.artifact_watcher.boto3") as mock_boto3:
        mock_boto3.client.return_value = mock_s3
        result = get_latest_artifact_key(
            bucket="test-bucket",
            prefix="artifacts/tide/",
        )

    assert result == "artifacts/tide/2026-01-01/output/model.tar.gz"


def test_get_latest_artifact_key_all_folders_error() -> None:
    mock_s3 = MagicMock()
    mock_paginator = MagicMock()
    mock_paginator.paginate.return_value = [
        {"CommonPrefixes": [{"Prefix": "artifacts/tide/2026-01-01/"}]}
    ]
    mock_s3.get_paginator.return_value = mock_paginator
    mock_s3.head_object.side_effect = botocore.exceptions.ClientError(
        CLIENT_ERROR_RESPONSE, "HeadObject"
    )

    with patch("tools.artifact_watcher.boto3") as mock_boto3:
        mock_boto3.client.return_value = mock_s3
        result = get_latest_artifact_key(
            bucket="test-bucket",
            prefix="artifacts/tide/",
        )

    assert result is None


def test_run_exits_when_bucket_not_set(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("AWS_S3_MODEL_ARTIFACTS_BUCKET_NAME", raising=False)
    run()


@patch("tools.artifact_watcher.time.sleep", side_effect=StopIteration)
@patch(
    "tools.artifact_watcher.get_latest_artifact_key",
    return_value=None,
)
@patch("tools.artifact_watcher.read_last_key", return_value=None)
def test_run_no_artifacts_found(
    _mock_read: MagicMock,  # noqa: PT019
    mock_get: MagicMock,
    _mock_sleep: MagicMock,  # noqa: PT019
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("AWS_S3_MODEL_ARTIFACTS_BUCKET_NAME", "test-bucket")

    with pytest.raises(StopIteration):
        run()

    mock_get.assert_called_once()


@patch("tools.artifact_watcher.time.sleep", side_effect=StopIteration)
@patch("tools.artifact_watcher.write_last_key")
@patch(
    "tools.artifact_watcher.get_latest_artifact_key",
    return_value="artifacts/tide/2026-01-01/output/model.tar.gz",
)
@patch("tools.artifact_watcher.read_last_key", return_value=None)
def test_run_initial_artifact_detected(
    _mock_read: MagicMock,  # noqa: PT019
    _mock_get: MagicMock,  # noqa: PT019
    mock_write: MagicMock,
    _mock_sleep: MagicMock,  # noqa: PT019
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("AWS_S3_MODEL_ARTIFACTS_BUCKET_NAME", "test-bucket")

    with pytest.raises(StopIteration):
        run()

    mock_write.assert_called_once_with("artifacts/tide/2026-01-01/output/model.tar.gz")


@patch("tools.artifact_watcher.time.sleep", side_effect=StopIteration)
@patch("tools.artifact_watcher.restart_ensemble_manager")
@patch("tools.artifact_watcher.write_last_key")
@patch(
    "tools.artifact_watcher.get_latest_artifact_key",
    return_value="artifacts/tide/2026-02-01/output/model.tar.gz",
)
@patch(
    "tools.artifact_watcher.read_last_key",
    return_value="artifacts/tide/2026-01-01/output/model.tar.gz",
)
def test_run_new_artifact_triggers_restart(
    _mock_read: MagicMock,  # noqa: PT019
    _mock_get: MagicMock,  # noqa: PT019
    mock_write: MagicMock,
    mock_restart: MagicMock,
    _mock_sleep: MagicMock,  # noqa: PT019
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("AWS_S3_MODEL_ARTIFACTS_BUCKET_NAME", "test-bucket")

    with pytest.raises(StopIteration):
        run()

    mock_restart.assert_called_once()
    mock_write.assert_called_once_with("artifacts/tide/2026-02-01/output/model.tar.gz")


@patch("tools.artifact_watcher.time.sleep", side_effect=StopIteration)
@patch("tools.artifact_watcher.write_last_key")
@patch(
    "tools.artifact_watcher.get_latest_artifact_key",
    return_value="artifacts/tide/2026-01-01/output/model.tar.gz",
)
@patch(
    "tools.artifact_watcher.read_last_key",
    return_value="artifacts/tide/2026-01-01/output/model.tar.gz",
)
def test_run_same_artifact_no_action(
    _mock_read: MagicMock,  # noqa: PT019
    _mock_get: MagicMock,  # noqa: PT019
    mock_write: MagicMock,
    _mock_sleep: MagicMock,  # noqa: PT019
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("AWS_S3_MODEL_ARTIFACTS_BUCKET_NAME", "test-bucket")

    with pytest.raises(StopIteration):
        run()

    mock_write.assert_not_called()


@patch(
    "tools.artifact_watcher.time.sleep",
    side_effect=[None, StopIteration],
)
@patch(
    "tools.artifact_watcher.get_latest_artifact_key",
    side_effect=RuntimeError("S3 error"),
)
@patch("tools.artifact_watcher.read_last_key", return_value=None)
def test_run_handles_exception_in_poll(
    _mock_read: MagicMock,  # noqa: PT019
    _mock_get: MagicMock,  # noqa: PT019
    _mock_sleep: MagicMock,  # noqa: PT019
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("AWS_S3_MODEL_ARTIFACTS_BUCKET_NAME", "test-bucket")

    with pytest.raises(StopIteration):
        run()
