import io
from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import polars as pl
import pytest
from tide.workflow import (
    prepare_data,
    train_tide_model,
    training_pipeline,
)

LOOKBACK_DAYS = 30


@patch("tide.workflow.S3Bucket")
@patch("tide.workflow.prepare_training_data")
def test_prepare_data_calls_prepare_training_data(
    mock_prepare: MagicMock,
    mock_s3_bucket: MagicMock,
) -> None:
    mock_data_block = MagicMock()
    mock_data_block.bucket_name = "data-bucket"
    mock_artifact_block = MagicMock()
    mock_artifact_block.bucket_name = "artifacts-bucket"
    block_map = {"data-bucket": mock_data_block, "artifact-bucket": mock_artifact_block}
    mock_s3_bucket.load.side_effect = lambda name: block_map[name]

    start_date = datetime(2024, 1, 1, tzinfo=UTC)
    end_date = datetime(2024, 1, 31, tzinfo=UTC)
    mock_prepare.return_value = "s3://artifacts-bucket/training/output.parquet"
    result = prepare_data.fn(
        start_date=start_date,
        end_date=end_date,
    )
    mock_prepare.assert_called_once()
    assert result == "training/output.parquet"


@patch("tide.workflow.S3Bucket")
@patch("tide.workflow.prepare_training_data")
def test_prepare_data_passes_output_key(
    mock_prepare: MagicMock,
    mock_s3_bucket: MagicMock,
) -> None:
    mock_data_block = MagicMock()
    mock_data_block.bucket_name = "data-bucket"
    mock_artifact_block = MagicMock()
    mock_artifact_block.bucket_name = "artifacts-bucket"
    block_map = {"data-bucket": mock_data_block, "artifact-bucket": mock_artifact_block}
    mock_s3_bucket.load.side_effect = lambda name: block_map[name]

    start_date = datetime(2024, 1, 1, tzinfo=UTC)
    end_date = datetime(2024, 1, 31, tzinfo=UTC)
    mock_prepare.return_value = "s3://artifacts-bucket/custom/key.parquet"
    prepare_data.fn(
        start_date=start_date,
        end_date=end_date,
        output_key="custom/key.parquet",
    )
    call_kwargs = mock_prepare.call_args.kwargs
    assert call_kwargs["output_key"] == "custom/key.parquet"


@patch("tide.workflow.end_run")
@patch("tide.workflow.start_run")
@patch("tide.workflow.S3Bucket")
def test_train_tide_model_downloads_trains_uploads(
    mock_s3_bucket: MagicMock,
    mock_start_run: MagicMock,
    mock_end_run: MagicMock,
) -> None:
    mock_artifact_block = MagicMock()
    mock_artifact_block.bucket_name = "artifacts-bucket"
    mock_s3 = MagicMock()
    mock_session = mock_artifact_block.credentials.get_boto3_session
    mock_session.return_value.client.return_value = mock_s3
    mock_s3_bucket.load.return_value = mock_artifact_block

    sample_data = pl.DataFrame(
        {
            "ticker": ["AAPL"],
            "timestamp": [1000000],
            "open_price": [100.0],
            "high_price": [101.0],
            "low_price": [99.0],
            "close_price": [100.5],
            "volume": [1000000],
            "volume_weighted_average_price": [100.3],
            "sector": ["Technology"],
            "industry": ["Software"],
        }
    )
    parquet_buffer = io.BytesIO()
    sample_data.write_parquet(parquet_buffer)
    parquet_bytes = parquet_buffer.getvalue()
    mock_s3.get_object.return_value = {"Body": MagicMock(read=lambda: parquet_bytes)}

    mock_model = MagicMock()
    mock_data = MagicMock()

    with patch("tide.trainer.train_model") as mock_train:
        mock_train.return_value = (mock_model, mock_data, [0.5, 0.3, 0.2])
        result = train_tide_model.fn(
            training_data_key="training/data.parquet",
        )

    assert result.startswith("s3://artifacts-bucket/artifacts/")
    mock_s3.get_object.assert_called_once()
    mock_s3.put_object.assert_called_once()
    mock_train.assert_called_once()
    assert "checkpoint_directory" in mock_train.call_args.kwargs
    mock_start_run.assert_called_once()
    mock_end_run.assert_called_once_with()


@patch("tide.workflow.end_run")
@patch("tide.workflow.start_run")
@patch("tide.workflow.S3Bucket")
def test_train_tide_model_calls_end_run_failed_on_error(
    mock_s3_bucket: MagicMock,
    mock_start_run: MagicMock,
    mock_end_run: MagicMock,
) -> None:
    mock_artifact_block = MagicMock()
    mock_artifact_block.bucket_name = "artifacts-bucket"
    mock_s3 = MagicMock()
    mock_session = mock_artifact_block.credentials.get_boto3_session
    mock_session.return_value.client.return_value = mock_s3
    mock_s3_bucket.load.return_value = mock_artifact_block

    sample_data = pl.DataFrame(
        {
            "ticker": ["AAPL"],
            "timestamp": [1000000],
            "open_price": [100.0],
            "high_price": [101.0],
            "low_price": [99.0],
            "close_price": [100.5],
            "volume": [1000000],
            "volume_weighted_average_price": [100.3],
            "sector": ["Technology"],
            "industry": ["Software"],
        }
    )
    parquet_buffer = io.BytesIO()
    sample_data.write_parquet(parquet_buffer)
    parquet_bytes = parquet_buffer.getvalue()
    mock_s3.get_object.return_value = {"Body": MagicMock(read=lambda: parquet_bytes)}

    with patch("tide.trainer.train_model") as mock_train:
        mock_train.side_effect = RuntimeError("Training failed")
        with pytest.raises(RuntimeError, match="Training failed"):
            train_tide_model.fn(training_data_key="training/data.parquet")

    mock_start_run.assert_called_once()
    mock_end_run.assert_called_once_with(status="FAILED")


@patch("tide.workflow.S3Bucket")
def test_prepare_data_raises_on_missing_blocks(
    mock_s3_bucket: MagicMock,
) -> None:
    mock_s3_bucket.load.side_effect = ValueError("Block not found")
    with pytest.raises(ValueError, match="not found"):
        prepare_data.fn(
            start_date=datetime(2024, 1, 1, tzinfo=UTC),
            end_date=datetime(2024, 1, 31, tzinfo=UTC),
        )


@patch("tide.workflow.train_tide_model", return_value="s3://bucket/model")
@patch("tide.workflow.prepare_data", return_value="training/data.parquet")
@patch("tide.workflow.get_training_date_range")
def test_training_pipeline_threads_data_key(
    mock_date_range: MagicMock,
    mock_prepare: MagicMock,
    mock_train: MagicMock,
) -> None:
    start_date = datetime(2024, 1, 1, tzinfo=UTC)
    end_date = datetime(2024, 1, 31, tzinfo=UTC)
    mock_date_range.return_value = (start_date, end_date)

    result = training_pipeline.fn(
        lookback_days=LOOKBACK_DAYS,
    )

    mock_date_range.assert_called_once_with(LOOKBACK_DAYS)
    mock_prepare.assert_called_once_with(
        start_date,
        end_date,
        "training/filtered_tide_training_data.parquet",
    )
    mock_train.assert_called_once_with(
        "training/data.parquet",
    )
    assert result == "s3://bucket/model"


def test_training_pipeline_rejects_nonpositive_lookback() -> None:
    with pytest.raises(ValueError, match="lookback_days must be positive"):
        training_pipeline.fn(
            lookback_days=0,
        )
