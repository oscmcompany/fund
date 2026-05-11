from unittest.mock import MagicMock, patch

import pytest
from tide.register_blocks import register_blocks

EXPECTED_BUCKET_COUNT = 2


@patch("tide.register_blocks.S3Bucket")
@patch("tide.register_blocks.AwsCredentials")
def test_register_blocks_creates_both_buckets(
    mock_credentials_cls: MagicMock,
    mock_s3_bucket_cls: MagicMock,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("AWS_S3_DATA_BUCKET_NAME", "my-data")
    monkeypatch.setenv("AWS_S3_MODEL_ARTIFACTS_BUCKET_NAME", "my-artifacts")
    monkeypatch.setenv("AWS_REGION", "us-west-2")

    mock_creds = MagicMock()
    mock_credentials_cls.return_value = mock_creds

    mock_block = MagicMock()
    mock_s3_bucket_cls.return_value = mock_block

    register_blocks()

    mock_credentials_cls.assert_called_once_with(region_name="us-west-2")
    assert mock_s3_bucket_cls.call_count == EXPECTED_BUCKET_COUNT
    mock_s3_bucket_cls.assert_any_call(bucket_name="my-data", credentials=mock_creds)
    mock_s3_bucket_cls.assert_any_call(
        bucket_name="my-artifacts", credentials=mock_creds
    )
    assert mock_block.save.call_count == EXPECTED_BUCKET_COUNT
    mock_block.save.assert_any_call("data-bucket", overwrite=True)
    mock_block.save.assert_any_call("artifact-bucket", overwrite=True)


@patch("tide.register_blocks.S3Bucket")
@patch("tide.register_blocks.AwsCredentials")
def test_register_blocks_defaults_region(
    mock_credentials_cls: MagicMock,
    mock_s3_bucket_cls: MagicMock,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("AWS_S3_DATA_BUCKET_NAME", "data")
    monkeypatch.setenv("AWS_S3_MODEL_ARTIFACTS_BUCKET_NAME", "artifacts")
    monkeypatch.delenv("AWS_REGION", raising=False)

    mock_s3_bucket_cls.return_value = MagicMock()

    register_blocks()

    mock_credentials_cls.assert_called_once_with(region_name="us-east-1")


def test_register_blocks_exits_when_data_bucket_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("AWS_S3_DATA_BUCKET_NAME", raising=False)
    monkeypatch.setenv("AWS_S3_MODEL_ARTIFACTS_BUCKET_NAME", "artifacts")

    with pytest.raises(SystemExit, match="1"):
        register_blocks()


def test_register_blocks_exits_when_artifact_bucket_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("AWS_S3_DATA_BUCKET_NAME", "data")
    monkeypatch.delenv("AWS_S3_MODEL_ARTIFACTS_BUCKET_NAME", raising=False)

    with pytest.raises(SystemExit, match="1"):
        register_blocks()


def test_register_blocks_exits_when_both_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("AWS_S3_DATA_BUCKET_NAME", raising=False)
    monkeypatch.delenv("AWS_S3_MODEL_ARTIFACTS_BUCKET_NAME", raising=False)

    with pytest.raises(SystemExit, match="1"):
        register_blocks()
