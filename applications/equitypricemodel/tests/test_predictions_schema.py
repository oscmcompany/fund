from datetime import UTC, datetime, timedelta

import polars as pl
import pytest
from equitypricemodel.predictions_schema import predictions_schema
from pandera.errors import SchemaError


def test_predictions_schema_valid_data() -> None:
    base_date = datetime(2024, 1, 1, tzinfo=UTC)

    valid_data = pl.DataFrame(
        {
            "ticker": ["AAPL"],
            "timestamp": [base_date.timestamp()],
            "quantile_10": [100.0],
            "quantile_50": [150.0],
            "quantile_90": [200.0],
        }
    )

    validated_df = predictions_schema.validate(valid_data)
    assert validated_df.shape == (1, 5)


def test_predictions_schema_ticker_lowercase_fails() -> None:
    base_date = datetime(2024, 1, 1, tzinfo=UTC)

    data = pl.DataFrame(
        {
            "ticker": ["aapl"],  # lowercase should fail
            "timestamp": [base_date.timestamp()],
            "quantile_10": [100.0],
            "quantile_50": [150.0],
            "quantile_90": [200.0],
        }
    )

    with pytest.raises(SchemaError):
        predictions_schema.validate(data)


def test_predictions_schema_negative_timestamp_fails() -> None:
    data = pl.DataFrame(
        {
            "ticker": ["AAPL"],
            "timestamp": [-1.0],  # negative timestamp should fail
            "quantile_10": [100.0],
            "quantile_50": [150.0],
            "quantile_90": [200.0],
        }
    )

    with pytest.raises(SchemaError):
        predictions_schema.validate(data)


def test_predictions_schema_duplicate_ticker_timestamp_fails() -> None:
    base_date = datetime(2024, 1, 1, tzinfo=UTC)

    data = pl.DataFrame(
        {
            "ticker": ["AAPL", "AAPL"],  # duplicate ticker + timestamp
            "timestamp": [base_date.timestamp(), base_date.timestamp()],
            "quantile_10": [100.0, 100.0],
            "quantile_50": [150.0, 150.0],
            "quantile_90": [200.0, 200.0],
        }
    )

    with pytest.raises(SchemaError):
        predictions_schema.validate(data)


def test_predictions_schema_multiple_tickers_same_dates() -> None:
    base_date = datetime(2024, 1, 1, tzinfo=UTC)

    valid_data = pl.DataFrame(
        {
            "ticker": ["AAPL", "GOOGL"],
            "timestamp": [base_date.timestamp(), base_date.timestamp()],
            "quantile_10": [100.0, 100.0],
            "quantile_50": [150.0, 150.0],
            "quantile_90": [200.0, 200.0],
        }
    )

    validated_df = predictions_schema.validate(valid_data)
    assert validated_df.shape == (2, 5)


def test_predictions_schema_multiple_tickers_different_dates_fails() -> None:
    base_date = datetime(2024, 1, 1, tzinfo=UTC)

    data = pl.DataFrame(
        {
            "ticker": ["AAPL", "GOOGL"],
            "timestamp": [
                base_date.timestamp(),
                (base_date + timedelta(days=1)).timestamp(),  # different date
            ],
            "quantile_10": [100.0, 100.0],
            "quantile_50": [150.0, 150.0],
            "quantile_90": [200.0, 200.0],
        }
    )

    with pytest.raises(
        SchemaError, match="Expected all tickers to have the same dates"
    ):
        predictions_schema.validate(data)


def test_predictions_schema_wrong_date_count_per_ticker_fails() -> None:
    base_date = datetime(2024, 1, 1, tzinfo=UTC)

    data = pl.DataFrame(
        {
            "ticker": ["AAPL"] * 2,  # 2 dates instead of 1
            "timestamp": [
                base_date.timestamp(),
                (base_date + timedelta(days=1)).timestamp(),
            ],
            "quantile_10": [100.0, 100.0],
            "quantile_50": [150.0, 150.0],
            "quantile_90": [200.0, 200.0],
        }
    )

    with pytest.raises(SchemaError, match="Each ticker must have exactly"):
        predictions_schema.validate(data)


def test_predictions_schema_float_quantile_values() -> None:
    base_date = datetime(2024, 1, 1, tzinfo=UTC)

    valid_data = pl.DataFrame(
        {
            "ticker": ["AAPL"],
            "timestamp": [base_date.timestamp()],
            "quantile_10": [100.5],
            "quantile_50": [150.1],
            "quantile_90": [200.9],
        }
    )

    validated_df = predictions_schema.validate(valid_data)
    assert validated_df.shape == (1, 5)
