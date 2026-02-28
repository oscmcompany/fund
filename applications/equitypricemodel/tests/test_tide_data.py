import json
import tempfile
from datetime import UTC, date, datetime, timedelta
from unittest.mock import patch

import numpy as np
import polars as pl
import pytest
from equitypricemodel.tide_data import (
    CONTINUOUS_COLUMNS,
    CleanData,
    Data,
    EngineerFeatures,
    ExpandDateRange,
    FillNulls,
    Pipeline,
    ScaleAndEncode,
    ValidateColumns,
)

SATURDAY_WEEKDAY = 5
FRIDAY_WEEKDAY = 4
EXPECTED_ENCODER_CONTINUOUS_FEATURES = 7
EXPECTED_ENCODER_CATEGORICAL_FEATURES = 6
EXPECTED_STATIC_CATEGORICAL_FEATURES = 3


def _make_raw_data(
    tickers: list[str] | None = None,
    days: int = 60,
    start_date: date | None = None,
) -> pl.DataFrame:
    tickers = tickers or ["AAPL", "GOOG"]
    start = start_date or date(2024, 1, 2)
    rows = []
    for ticker in tickers:
        for day_offset in range(days):
            current_date = start + timedelta(days=day_offset)
            if current_date.weekday() >= SATURDAY_WEEKDAY:
                continue
            timestamp = int(
                datetime(
                    current_date.year,
                    current_date.month,
                    current_date.day,
                    tzinfo=UTC,
                ).timestamp()
                * 1000
            )
            close = 100.0 + day_offset * 0.5
            rows.append(
                {
                    "ticker": ticker,
                    "timestamp": timestamp,
                    "open_price": close - 1.0,
                    "high_price": close + 1.0,
                    "low_price": close - 2.0,
                    "close_price": close,
                    "volume": 1_000_000,
                    "volume_weighted_average_price": close + 0.1,
                    "sector": "Technology",
                    "industry": "Software",
                }
            )
    return pl.DataFrame(rows)


def test_validate_columns_accepts_valid_data() -> None:
    data = _make_raw_data()
    result = ValidateColumns().run(data)
    assert result.height == data.height


def test_validate_columns_rejects_missing_column() -> None:
    data = _make_raw_data().drop("ticker")
    with pytest.raises(ValueError, match="Expected columns"):
        ValidateColumns().run(data)


def test_validate_columns_rejects_extra_column() -> None:
    data = _make_raw_data().with_columns(pl.lit(1).alias("extra"))
    with pytest.raises(ValueError, match="Expected columns"):
        ValidateColumns().run(data)


def test_expand_date_range_fills_gaps() -> None:
    data = _make_raw_data(tickers=["AAPL"], days=10)
    expanded = ExpandDateRange().run(data)
    unique_dates = expanded.select("date").unique().height
    min_date: date = expanded.select(pl.col("date").min()).item()
    max_date: date = expanded.select(pl.col("date").max()).item()
    expected_dates = (max_date - min_date).days + 1
    assert unique_dates == expected_dates


def test_fill_nulls_replaces_null_prices() -> None:
    data = _make_raw_data(tickers=["AAPL"], days=10)
    expanded = ExpandDateRange().run(data)
    filled = FillNulls().run(expanded)
    null_count = filled.select(pl.col("open_price").is_null().sum()).item()
    assert null_count == 0


def test_fill_nulls_sets_holidays_for_missing_weekdays() -> None:
    data = _make_raw_data(tickers=["AAPL"], days=10)
    expanded = ExpandDateRange().run(data)
    filled = FillNulls().run(expanded)
    weekday_nulls = filled.filter(
        (pl.col("date").dt.weekday() <= FRIDAY_WEEKDAY) & pl.col("is_holiday").is_null()
    )
    assert weekday_nulls.height == 0


def test_engineer_features_adds_calendar_columns() -> None:
    data = _make_raw_data(tickers=["AAPL"], days=10)
    expanded = ExpandDateRange().run(data)
    filled = FillNulls().run(expanded)
    featured = EngineerFeatures().run(filled)
    expected_columns = {
        "day_of_week",
        "day_of_month",
        "day_of_year",
        "month",
        "year",
        "time_idx",
        "daily_return",
    }
    assert expected_columns.issubset(set(featured.columns))


def test_engineer_features_time_idx_is_dense_rank() -> None:
    data = _make_raw_data(tickers=["AAPL"], days=10)
    expanded = ExpandDateRange().run(data)
    filled = FillNulls().run(expanded)
    featured = EngineerFeatures().run(filled)
    time_indices = featured.sort("timestamp").select("time_idx").to_series().to_list()
    assert time_indices == sorted(time_indices)
    assert time_indices[0] == 1


def test_clean_data_removes_unknown_tickers() -> None:
    data = _make_raw_data(tickers=["AAPL"], days=10)
    expanded = ExpandDateRange().run(data)
    filled = FillNulls().run(expanded)
    featured = EngineerFeatures().run(filled)
    # Add an UNKNOWN ticker row
    unknown_row = featured.head(1).with_columns(pl.lit("UNKNOWN").alias("ticker"))
    featured_with_unknown = pl.concat([featured, unknown_row])
    cleaned = CleanData().run(featured_with_unknown)
    assert cleaned.filter(pl.col("ticker") == "UNKNOWN").height == 0


def test_clean_data_removes_nan_daily_return() -> None:
    data = _make_raw_data(tickers=["AAPL"], days=10)
    expanded = ExpandDateRange().run(data)
    filled = FillNulls().run(expanded)
    featured = EngineerFeatures().run(filled)
    cleaned = CleanData().run(featured)
    nan_count = cleaned.filter(pl.col("daily_return").is_nan()).height
    null_count = cleaned.filter(pl.col("daily_return").is_null()).height
    assert nan_count == 0
    assert null_count == 0


def test_scale_and_encode_produces_scaler_and_mappings() -> None:
    data = _make_raw_data(tickers=["AAPL"], days=60)
    pipeline = Pipeline()
    result = pipeline.run_to("clean_data", data)
    stage = ScaleAndEncode()
    encoded = stage.run(result)
    assert stage.scaler is not None
    assert stage.mappings is not None
    assert "ticker" in stage.mappings
    assert "sector" in stage.mappings
    assert encoded.height > 0


def test_pipeline_run_produces_complete_output() -> None:
    data = _make_raw_data()
    pipeline = Pipeline()
    result = pipeline.run(data)
    assert result.height > 0
    scale_stage = pipeline.get_stage("scale_and_encode")
    assert isinstance(scale_stage, ScaleAndEncode)
    assert scale_stage.scaler is not None
    assert scale_stage.mappings is not None


def test_pipeline_run_to_stops_at_stage() -> None:
    data = _make_raw_data()
    pipeline = Pipeline()
    result = pipeline.run_to("fill_nulls", data)
    assert "day_of_week" not in result.columns


def test_pipeline_get_stage_raises_for_unknown() -> None:
    pipeline = Pipeline()
    with pytest.raises(ValueError, match="not found"):
        pipeline.get_stage("nonexistent")


def test_pipeline_snapshot_roundtrip(tmp_path: str) -> None:
    data = _make_raw_data(tickers=["AAPL"], days=10)
    pipeline = Pipeline()
    result = pipeline.run_to("fill_nulls", data)
    snapshot_path = f"{tmp_path}/snapshot.parquet"
    Pipeline.snapshot(result, snapshot_path)
    loaded = Pipeline.load_snapshot(snapshot_path)
    assert loaded.equals(result)


def test_data_preprocess_sets_attributes() -> None:
    raw = _make_raw_data()
    data = Data()
    data.preprocess_and_set_data(raw)
    assert hasattr(data, "data")
    assert hasattr(data, "scaler")
    assert hasattr(data, "mappings")
    assert data.data.height > 0


def test_data_preprocess_output_columns_match_schema() -> None:
    raw = _make_raw_data()
    data = Data()
    data.preprocess_and_set_data(raw)
    expected_columns = {
        "ticker",
        "timestamp",
        "open_price",
        "high_price",
        "low_price",
        "close_price",
        "volume",
        "volume_weighted_average_price",
        "sector",
        "industry",
        "date",
        "is_holiday",
        "day_of_week",
        "day_of_month",
        "day_of_year",
        "month",
        "year",
        "time_idx",
        "daily_return",
    }
    assert expected_columns == set(data.data.columns)


def test_data_save_and_load_roundtrip() -> None:
    raw = _make_raw_data()
    data = Data()
    data.preprocess_and_set_data(raw)
    with tempfile.TemporaryDirectory() as tmpdir:
        data.save(tmpdir)
        loaded = Data.load(tmpdir)
        # JSON roundtrip converts bool keys to strings, so compare via JSON
        assert json.dumps(loaded.mappings, sort_keys=True) == json.dumps(
            data.mappings, sort_keys=True, default=str
        )
        assert loaded.continuous_columns == data.continuous_columns
        assert loaded.categorical_columns == data.categorical_columns
        assert loaded.static_categorical_columns == data.static_categorical_columns


def test_data_get_dimensions() -> None:
    raw = _make_raw_data()
    data = Data()
    data.preprocess_and_set_data(raw)
    dimensions = data.get_dimensions()
    assert (
        dimensions["encoder_continuous_features"]
        == EXPECTED_ENCODER_CONTINUOUS_FEATURES
    )
    assert (
        dimensions["encoder_categorical_features"]
        == EXPECTED_ENCODER_CATEGORICAL_FEATURES
    )
    assert (
        dimensions["static_categorical_features"]
        == EXPECTED_STATIC_CATEGORICAL_FEATURES
    )


def test_data_get_batches_train() -> None:
    raw = _make_raw_data(days=90)
    data = Data()
    data.preprocess_and_set_data(raw)
    batches = data.get_batches(
        data_type="train",
        validation_split=0.8,
        input_length=35,
        output_length=7,
        batch_size=32,
    )
    assert len(batches) > 0
    batch = batches[0]
    assert "encoder_continuous_features" in batch
    assert "encoder_categorical_features" in batch
    assert "decoder_categorical_features" in batch
    assert "static_categorical_features" in batch
    assert "targets" in batch


def test_scale_and_encode_raises_on_nan_scaler() -> None:
    data = _make_raw_data(tickers=["AAPL"], days=60)
    pipeline = Pipeline()
    cleaned = pipeline.run_to("clean_data", data)
    stage = ScaleAndEncode()
    # Inject NaN into continuous column to trigger NaN in scaler means
    nan_data = cleaned.with_columns(pl.lit(float("nan")).alias("close_price"))
    with pytest.raises(ValueError, match="Scaler has NaN values"):
        stage.run(nan_data)


def test_pipeline_run_to_raises_for_unknown_stage() -> None:
    pipeline = Pipeline()
    data = _make_raw_data()
    with pytest.raises(ValueError, match="Unknown stage"):
        pipeline.run_to("nonexistent_stage", data)
