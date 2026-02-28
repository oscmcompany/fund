import io
from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock, patch

import polars as pl

from tools.flows.training_flow import (
    prepare_data,
    sync_equity_bars,
    sync_equity_details,
    train_tide_model,
    training_pipeline,
)


@patch("tools.flows.training_flow.sync_equity_bars_data")
def test_sync_equity_bars_calls_sync_with_date_range(mock_sync: MagicMock) -> None:
    sync_equity_bars.fn(base_url="http://example.com", lookback_days=30)
    mock_sync.assert_called_once()
    call_kwargs = mock_sync.call_args
    assert call_kwargs.kwargs["base_url"] == "http://example.com"
    start, end = call_kwargs.kwargs["date_range"]
    assert (end - start).days == 30


@patch("tools.flows.training_flow.sync_equity_details_data")
def test_sync_equity_details_calls_sync(mock_sync: MagicMock) -> None:
    sync_equity_details.fn(base_url="http://example.com")
    mock_sync.assert_called_once_with(base_url="http://example.com")


@patch("tools.flows.training_flow.prepare_training_data")
def test_prepare_data_calls_prepare_training_data(mock_prepare: MagicMock) -> None:
    mock_prepare.return_value = "training/output.parquet"
    result = prepare_data.fn(
        data_bucket="data-bucket",
        artifacts_bucket="artifacts-bucket",
        lookback_days=30,
    )
    mock_prepare.assert_called_once()
    assert result == "training/output.parquet"


@patch("tools.flows.training_flow.prepare_training_data")
def test_prepare_data_passes_output_key(mock_prepare: MagicMock) -> None:
    mock_prepare.return_value = "custom/key.parquet"
    prepare_data.fn(
        data_bucket="data-bucket",
        artifacts_bucket="artifacts-bucket",
        lookback_days=30,
        output_key="custom/key.parquet",
    )
    call_kwargs = mock_prepare.call_args.kwargs
    assert call_kwargs["output_key"] == "custom/key.parquet"


@patch("tools.flows.training_flow.boto3")
def test_train_tide_model_downloads_trains_uploads(mock_boto3: MagicMock) -> None:
    mock_s3 = MagicMock()
    mock_boto3.client.return_value = mock_s3

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

    with patch(
        "tools.flows.training_flow.train_tide_model.__wrapped__",
        side_effect=None,
    ):
        with patch("equitypricemodel.trainer.train_model") as mock_train:
            mock_train.return_value = (mock_model, mock_data)
            result = train_tide_model.fn(
                artifacts_bucket="artifacts-bucket",
                training_data_key="training/data.parquet",
            )

    assert result.startswith("s3://artifacts-bucket/artifacts/")
    mock_s3.get_object.assert_called_once()
    mock_s3.put_object.assert_called_once()


@patch("tools.flows.training_flow.train_tide_model", return_value="s3://bucket/model")
@patch("tools.flows.training_flow.prepare_data", return_value="training/data.parquet")
@patch("tools.flows.training_flow.sync_equity_details")
@patch("tools.flows.training_flow.sync_equity_bars")
def test_training_pipeline_threads_data_key(
    mock_bars: MagicMock,
    mock_details: MagicMock,
    mock_prepare: MagicMock,
    mock_train: MagicMock,
) -> None:
    result = training_pipeline.fn(
        base_url="http://example.com",
        data_bucket="data-bucket",
        artifacts_bucket="artifacts-bucket",
        lookback_days=30,
    )
    mock_bars.assert_called_once_with("http://example.com", 30)
    mock_details.assert_called_once_with("http://example.com")
    mock_prepare.assert_called_once()
    mock_train.assert_called_once_with(
        "artifacts-bucket",
        "training/filtered_tide_training_data.parquet",
    )
    assert result == "s3://bucket/model"
