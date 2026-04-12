import io
from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock, patch

import polars as pl
from tide.tasks import (
    MINIMUM_CLOSE_PRICE,
    MINIMUM_VOLUME,
    consolidate_data,
    filter_equity_bars,
    prepare_training_data,
    read_categories_from_s3,
    read_equity_bars_from_s3,
    write_training_data_to_s3,
)

_TARGET_DATE = datetime(2025, 6, 1, tzinfo=UTC)

_SAMPLE_EQUITY_BARS = pl.DataFrame(
    {
        "ticker": ["AAPL"],
        "timestamp": [int(_TARGET_DATE.timestamp()) * 1000],
        "open_price": [148.0],
        "high_price": [152.0],
        "low_price": [147.0],
        "close_price": [150.0],
        "volume": [1_000_000],
        "volume_weighted_average_price": [151.0],
        "transactions": [5_000],
    }
)

_SAMPLE_CATEGORIES = pl.DataFrame(
    {
        "ticker": ["AAPL"],
        "sector": ["Technology"],
        "industry": ["Consumer Electronics"],
    }
)


def _to_parquet_bytes(data: pl.DataFrame) -> bytes:
    buffer = io.BytesIO()
    data.write_parquet(buffer)
    return buffer.getvalue()


def _to_csv_bytes(data: pl.DataFrame) -> bytes:
    return data.write_csv().encode()


def test_filter_equity_bars_keeps_rows_above_thresholds() -> None:
    data = pl.DataFrame(
        {
            "ticker": ["AAPL", "LOW"],
            "close_price": [MINIMUM_CLOSE_PRICE + 1.0, 0.5],
            "volume": [MINIMUM_VOLUME + 1, 50_000],
        }
    )

    result = filter_equity_bars(data)

    assert len(result) == 1
    assert result["close_price"][0] == MINIMUM_CLOSE_PRICE + 1.0


def test_filter_equity_bars_excludes_preferred_stocks() -> None:
    data = pl.DataFrame(
        {
            "ticker": ["AAPL", "JPMpC", "NEEpR"],
            "close_price": [
                MINIMUM_CLOSE_PRICE + 1.0,
                MINIMUM_CLOSE_PRICE + 1.0,
                MINIMUM_CLOSE_PRICE + 1.0,
            ],
            "volume": [MINIMUM_VOLUME + 1, MINIMUM_VOLUME + 1, MINIMUM_VOLUME + 1],
        }
    )

    result = filter_equity_bars(data)

    assert len(result) == 1
    assert result["ticker"][0] == "AAPL"


def test_filter_equity_bars_excludes_warrants() -> None:
    data = pl.DataFrame(
        {
            "ticker": ["AAPL", "RALw", "FTVw", "DDw"],
            "close_price": [
                MINIMUM_CLOSE_PRICE + 1.0,
                MINIMUM_CLOSE_PRICE + 1.0,
                MINIMUM_CLOSE_PRICE + 1.0,
                MINIMUM_CLOSE_PRICE + 1.0,
            ],
            "volume": [
                MINIMUM_VOLUME + 1,
                MINIMUM_VOLUME + 1,
                MINIMUM_VOLUME + 1,
                MINIMUM_VOLUME + 1,
            ],
        }
    )

    result = filter_equity_bars(data)

    assert len(result) == 1
    assert result["ticker"][0] == "AAPL"


def test_filter_equity_bars_empty_input_returns_empty() -> None:
    data = pl.DataFrame({"ticker": [], "close_price": [], "volume": []}).cast(
        {"ticker": pl.String, "close_price": pl.Float64, "volume": pl.Int64}
    )

    result = filter_equity_bars(data)

    assert len(result) == 0


def test_consolidate_data_joins_on_ticker_and_retains_columns() -> None:
    result = consolidate_data(_SAMPLE_EQUITY_BARS, _SAMPLE_CATEGORIES)

    assert len(result) == 1
    assert result["ticker"][0] == "AAPL"
    assert "sector" in result.columns
    assert "industry" in result.columns


def test_consolidate_data_excludes_tickers_with_null_sector_or_industry() -> None:
    categories = pl.DataFrame(
        {
            "ticker": ["AAPL", "MSFT", "GOOG"],
            "sector": ["Technology", None, "Technology"],
            "industry": ["Consumer Electronics", "Software", None],
        }
    )
    equity_bars = pl.DataFrame(
        {
            "ticker": ["AAPL", "MSFT", "GOOG"],
            "timestamp": [
                int(_TARGET_DATE.timestamp()) * 1000,
                int(_TARGET_DATE.timestamp()) * 1000,
                int(_TARGET_DATE.timestamp()) * 1000,
            ],
            "open_price": [148.0, 200.0, 100.0],
            "high_price": [152.0, 205.0, 105.0],
            "low_price": [147.0, 198.0, 99.0],
            "close_price": [150.0, 202.0, 102.0],
            "volume": [1_000_000, 500_000, 750_000],
            "volume_weighted_average_price": [151.0, 201.0, 101.0],
            "transactions": [5_000, 2_000, 3_000],
        }
    )

    result = consolidate_data(equity_bars, categories)

    assert len(result) == 1
    assert result["ticker"][0] == "AAPL"


def test_consolidate_data_excludes_unmatched_tickers() -> None:
    categories = pl.DataFrame(
        {
            "ticker": ["MSFT"],
            "sector": ["Technology"],
            "industry": ["Software"],
        }
    )

    result = consolidate_data(_SAMPLE_EQUITY_BARS, categories)

    assert len(result) == 0


def test_read_equity_bars_from_s3_normalizes_column_types_across_files() -> None:
    day1 = _SAMPLE_EQUITY_BARS.with_columns(pl.col("volume").cast(pl.Float64))
    day2 = _SAMPLE_EQUITY_BARS.with_columns(pl.col("volume").cast(pl.Int64))

    mock_body_1 = MagicMock()
    mock_body_1.read.return_value = _to_parquet_bytes(day1)
    mock_body_2 = MagicMock()
    mock_body_2.read.return_value = _to_parquet_bytes(day2)

    mock_s3_client = MagicMock()
    mock_s3_client.get_object.side_effect = [
        {"Body": mock_body_1},
        {"Body": mock_body_2},
    ]

    result = read_equity_bars_from_s3(
        s3_client=mock_s3_client,
        bucket_name="test-bucket",
        start_date=_TARGET_DATE,
        end_date=_TARGET_DATE + timedelta(days=1),
    )

    expected_rows = len(day1) + len(day2)
    assert len(result) == expected_rows
    assert result["volume"].dtype == pl.Int64


def test_read_equity_bars_from_s3_returns_dataframe() -> None:
    parquet_bytes = _to_parquet_bytes(_SAMPLE_EQUITY_BARS)

    mock_body = MagicMock()
    mock_body.read.return_value = parquet_bytes
    mock_s3_client = MagicMock()
    mock_s3_client.get_object.return_value = {"Body": mock_body}

    result = read_equity_bars_from_s3(
        s3_client=mock_s3_client,
        bucket_name="test-bucket",
        start_date=_TARGET_DATE,
        end_date=_TARGET_DATE,
    )

    assert len(result) == 1
    assert result["ticker"][0] == "AAPL"
    mock_s3_client.get_object.assert_called_once()


def test_read_categories_from_s3_returns_dataframe() -> None:
    csv_bytes = _to_csv_bytes(_SAMPLE_CATEGORIES)

    mock_body = MagicMock()
    mock_body.read.return_value = csv_bytes
    mock_s3_client = MagicMock()
    mock_s3_client.get_object.return_value = {"Body": mock_body}

    result = read_categories_from_s3(
        s3_client=mock_s3_client,
        bucket_name="test-bucket",
    )

    assert len(result) == 1
    assert result["ticker"][0] == "AAPL"
    mock_s3_client.get_object.assert_called_once_with(
        Bucket="test-bucket",
        Key="equity/details/details.csv",
    )


def test_write_training_data_to_s3_returns_s3_uri() -> None:
    mock_s3_client = MagicMock()

    result = write_training_data_to_s3(
        s3_client=mock_s3_client,
        bucket_name="test-bucket",
        data=_SAMPLE_EQUITY_BARS,
        output_key="training/data.parquet",
    )

    assert result == "s3://test-bucket/training/data.parquet"
    mock_s3_client.put_object.assert_called_once()
    call_kwargs = mock_s3_client.put_object.call_args.kwargs
    assert call_kwargs["Bucket"] == "test-bucket"
    assert call_kwargs["Key"] == "training/data.parquet"


def test_prepare_training_data_succeeds_when_raw_data_contains_preferred_tickers() -> (
    None
):
    preferred_ticker_bars = pl.DataFrame(
        {
            "ticker": ["AAPL", "DLNGpB"],
            "timestamp": [
                int(_TARGET_DATE.timestamp()) * 1000,
                int(_TARGET_DATE.timestamp()) * 1000,
            ],
            "open_price": [148.0, 20.0],
            "high_price": [152.0, 21.0],
            "low_price": [147.0, 19.0],
            "close_price": [150.0, 20.5],
            "volume": [1_000_000, 500_000],
            "volume_weighted_average_price": [151.0, 20.3],
            "transactions": [5_000, 1_000],
        }
    )
    parquet_bytes = _to_parquet_bytes(preferred_ticker_bars)
    csv_bytes = _to_csv_bytes(_SAMPLE_CATEGORIES)

    mock_body_bars = MagicMock()
    mock_body_bars.read.return_value = parquet_bytes
    mock_body_categories = MagicMock()
    mock_body_categories.read.return_value = csv_bytes

    mock_s3_client = MagicMock()
    mock_s3_client.get_object.side_effect = [
        {"Body": mock_body_bars},
        {"Body": mock_body_categories},
    ]

    with patch("tide.tasks.boto3.client", return_value=mock_s3_client):
        result = prepare_training_data(
            data_bucket_name="test-data-bucket",
            model_artifacts_bucket_name="test-artifacts-bucket",
            start_date=_TARGET_DATE,
            end_date=_TARGET_DATE,
        )

    assert result.startswith("s3://test-artifacts-bucket/")


def test_prepare_training_data_returns_s3_uri() -> None:
    parquet_bytes = _to_parquet_bytes(_SAMPLE_EQUITY_BARS)
    csv_bytes = _to_csv_bytes(_SAMPLE_CATEGORIES)

    mock_body_bars = MagicMock()
    mock_body_bars.read.return_value = parquet_bytes
    mock_body_categories = MagicMock()
    mock_body_categories.read.return_value = csv_bytes

    mock_s3_client = MagicMock()
    mock_s3_client.get_object.side_effect = [
        {"Body": mock_body_bars},
        {"Body": mock_body_categories},
    ]

    with patch("tide.tasks.boto3.client", return_value=mock_s3_client):
        result = prepare_training_data(
            data_bucket_name="test-data-bucket",
            model_artifacts_bucket_name="test-artifacts-bucket",
            start_date=_TARGET_DATE,
            end_date=_TARGET_DATE,
        )

    assert result.startswith("s3://test-artifacts-bucket/")
    mock_s3_client.put_object.assert_called_once()
