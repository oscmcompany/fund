import io
from collections.abc import Callable
from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import polars as pl
import pytest
from tide.workflow import (
    DEFAULT_CONFIGURATION,
    prepare_data,
    sync_equity_bars,
    sync_equity_details,
    train_model,
    train_tide_model,
    training_pipeline,
)

LOOKBACK_DAYS = 30
PARTIAL_HIDDEN_SIZE = 16


@patch("tide.workflow.sync_equity_bars_data")
def test_sync_equity_bars_calls_sync_with_date_range(mock_sync: MagicMock) -> None:
    start_date = datetime(2024, 1, 1, tzinfo=UTC)
    end_date = datetime(2024, 1, 31, tzinfo=UTC)
    sync_equity_bars.fn(
        base_url="http://example.com",
        start_date=start_date,
        end_date=end_date,
    )
    mock_sync.assert_called_once()
    call_kwargs = mock_sync.call_args
    assert call_kwargs.kwargs["base_url"] == "http://example.com"
    start, end = call_kwargs.kwargs["date_range"]
    assert start == start_date
    assert end == end_date


@patch("tide.workflow.sync_equity_details_data")
def test_sync_equity_details_calls_sync(mock_sync: MagicMock) -> None:
    sync_equity_details.fn(base_url="http://example.com")
    mock_sync.assert_called_once_with(base_url="http://example.com")


@patch("tide.workflow.sync_equity_details_data")
def test_sync_equity_details_ignores_not_implemented(mock_sync: MagicMock) -> None:
    mock_sync.side_effect = RuntimeError("Sync failed with status 501: not implemented")
    sync_equity_details.fn(base_url="http://example.com")
    mock_sync.assert_called_once_with(base_url="http://example.com")


@patch("tide.workflow.sync_equity_details_data")
def test_sync_equity_details_raises_non_501_errors(mock_sync: MagicMock) -> None:
    mock_sync.side_effect = RuntimeError("Sync failed with status 500: failure")
    with pytest.raises(RuntimeError, match="status 500"):
        sync_equity_details.fn(base_url="http://example.com")


@patch("tide.workflow.prepare_training_data")
def test_prepare_data_calls_prepare_training_data(mock_prepare: MagicMock) -> None:
    start_date = datetime(2024, 1, 1, tzinfo=UTC)
    end_date = datetime(2024, 1, 31, tzinfo=UTC)
    mock_prepare.return_value = "s3://artifacts-bucket/training/output.parquet"
    result = prepare_data.fn(
        data_bucket="data-bucket",
        artifacts_bucket="artifacts-bucket",
        start_date=start_date,
        end_date=end_date,
    )
    mock_prepare.assert_called_once()
    assert result == "training/output.parquet"


@patch("tide.workflow.prepare_training_data")
def test_prepare_data_passes_output_key(mock_prepare: MagicMock) -> None:
    start_date = datetime(2024, 1, 1, tzinfo=UTC)
    end_date = datetime(2024, 1, 31, tzinfo=UTC)
    mock_prepare.return_value = "s3://artifacts-bucket/custom/key.parquet"
    prepare_data.fn(
        data_bucket="data-bucket",
        artifacts_bucket="artifacts-bucket",
        start_date=start_date,
        end_date=end_date,
        output_key="custom/key.parquet",
    )
    call_kwargs = mock_prepare.call_args.kwargs
    assert call_kwargs["output_key"] == "custom/key.parquet"


@patch("tide.workflow.boto3")
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

    with patch("tide.workflow.train_model") as mock_train:
        mock_train.return_value = (mock_model, mock_data)
        result = train_tide_model.fn(
            artifacts_bucket="artifacts-bucket",
            training_data_key="training/data.parquet",
        )

    assert result.startswith("s3://artifacts-bucket/artifacts/")
    mock_s3.get_object.assert_called_once()
    mock_s3.put_object.assert_called_once()
    mock_train.assert_called_once()
    assert "checkpoint_directory" in mock_train.call_args.kwargs


@patch("tide.workflow.train_tide_model", return_value="s3://bucket/model")
@patch("tide.workflow.prepare_data", return_value="training/data.parquet")
@patch("tide.workflow.sync_equity_details")
@patch("tide.workflow.sync_equity_bars")
@patch("tide.workflow.get_training_date_range")
def test_training_pipeline_threads_data_key(
    mock_date_range: MagicMock,
    mock_bars: MagicMock,
    mock_details: MagicMock,
    mock_prepare: MagicMock,
    mock_train: MagicMock,
) -> None:
    start_date = datetime(2024, 1, 1, tzinfo=UTC)
    end_date = datetime(2024, 1, 31, tzinfo=UTC)
    mock_date_range.return_value = (start_date, end_date)

    result = training_pipeline.fn(
        base_url="http://example.com",
        data_bucket="data-bucket",
        artifacts_bucket="artifacts-bucket",
        lookback_days=LOOKBACK_DAYS,
    )

    mock_date_range.assert_called_once_with(LOOKBACK_DAYS)
    mock_bars.assert_called_once_with("http://example.com", start_date, end_date)
    mock_details.assert_called_once_with("http://example.com")
    mock_prepare.assert_called_once_with(
        "data-bucket",
        "artifacts-bucket",
        start_date,
        end_date,
        "training/filtered_tide_training_data.parquet",
    )
    mock_train.assert_called_once_with(
        "artifacts-bucket",
        "training/data.parquet",
    )
    assert result == "s3://bucket/model"


def test_train_model_returns_model_and_data(
    make_raw_data: Callable[..., pl.DataFrame],
) -> None:
    training_data = make_raw_data(days=90)
    model, data = train_model(training_data, configuration={"epoch_count": 1})
    assert model is not None
    assert data is not None
    assert hasattr(data, "scaler")
    assert hasattr(data, "mappings")


def test_train_model_uses_custom_configuration(
    make_raw_data: Callable[..., pl.DataFrame],
) -> None:
    training_data = make_raw_data(days=90)
    custom_config = dict(DEFAULT_CONFIGURATION)
    custom_config["epoch_count"] = 1
    custom_config["hidden_size"] = PARTIAL_HIDDEN_SIZE * 2
    model, _data = train_model(training_data, configuration=custom_config)
    assert model.hidden_size == PARTIAL_HIDDEN_SIZE * 2


def test_train_model_raises_on_insufficient_data(
    make_raw_data: Callable[..., pl.DataFrame],
) -> None:
    short_data = make_raw_data(tickers=["AAPL"], days=5)
    with pytest.raises(ValueError, match="Total days available"):
        train_model(short_data)


def test_train_model_uses_default_configuration(
    make_raw_data: Callable[..., pl.DataFrame],
) -> None:
    training_data = make_raw_data(days=90)
    model, _ = train_model(training_data, configuration={"epoch_count": 1})
    assert model.hidden_size == DEFAULT_CONFIGURATION["hidden_size"]
    assert model.output_length == DEFAULT_CONFIGURATION["output_length"]


def test_train_model_merges_partial_configuration(
    make_raw_data: Callable[..., pl.DataFrame],
) -> None:
    training_data = make_raw_data(days=90)
    model, _ = train_model(
        training_data,
        configuration={
            "epoch_count": 1,
            "hidden_size": PARTIAL_HIDDEN_SIZE,
        },
    )
    assert model.hidden_size == PARTIAL_HIDDEN_SIZE
    assert model.output_length == DEFAULT_CONFIGURATION["output_length"]
