import json
import tempfile
from collections.abc import Callable
from pathlib import Path

import polars as pl
import pytest
from tide.tide_data import (
    CleanData,
    Data,
    EngineerFeatures,
    Pipeline,
    ScaleAndEncode,
    TrainingDataset,
    ValidateColumns,
)

EXPECTED_PAST_CONTINUOUS_FEATURES = 7
EXPECTED_INPUT_LENGTH = 35
EXPECTED_OUTPUT_LENGTH = 5
EXPECTED_PAST_CATEGORICAL_FEATURES = 5
EXPECTED_STATIC_CATEGORICAL_FEATURES = 3


def test_validate_columns_accepts_valid_data(
    make_raw_data: Callable[..., pl.DataFrame],
) -> None:
    data = make_raw_data()
    result = ValidateColumns().run(data)
    assert result.height == data.height


def test_validate_columns_rejects_missing_column(
    make_raw_data: Callable[..., pl.DataFrame],
) -> None:
    data = make_raw_data().drop("ticker")
    with pytest.raises(ValueError, match="Expected columns"):
        ValidateColumns().run(data)


def test_validate_columns_rejects_extra_column(
    make_raw_data: Callable[..., pl.DataFrame],
) -> None:
    data = make_raw_data().with_columns(pl.lit(1).alias("extra"))
    with pytest.raises(ValueError, match="Expected columns"):
        ValidateColumns().run(data)


def test_engineer_features_adds_calendar_columns(
    make_raw_data: Callable[..., pl.DataFrame],
) -> None:
    data = make_raw_data(tickers=["AAPL"], days=10)
    featured = EngineerFeatures().run(data)
    expected_columns = {
        "day_of_week",
        "day_of_month",
        "day_of_year",
        "month",
        "year",
        "time_idx",
        "daily_return",
        "date",
    }
    assert expected_columns.issubset(set(featured.columns))


def test_engineer_features_time_idx_is_dense_rank(
    make_raw_data: Callable[..., pl.DataFrame],
) -> None:
    data = make_raw_data(tickers=["AAPL"], days=10)
    featured = EngineerFeatures().run(data)
    time_indices = featured.sort("timestamp").select("time_idx").to_series().to_list()
    assert time_indices == sorted(time_indices)
    assert time_indices[0] == 1


def test_clean_data_removes_unknown_tickers(
    make_raw_data: Callable[..., pl.DataFrame],
) -> None:
    data = make_raw_data(tickers=["AAPL"], days=10)
    featured = EngineerFeatures().run(data)
    unknown_row = featured.head(1).with_columns(pl.lit("UNKNOWN").alias("ticker"))
    featured_with_unknown = pl.concat([featured, unknown_row])
    cleaned = CleanData().run(featured_with_unknown)
    assert cleaned.filter(pl.col("ticker") == "UNKNOWN").height == 0


def test_clean_data_removes_nan_daily_return(
    make_raw_data: Callable[..., pl.DataFrame],
) -> None:
    data = make_raw_data(tickers=["AAPL"], days=10)
    featured = EngineerFeatures().run(data)
    cleaned = CleanData().run(featured)
    nan_count = cleaned.filter(pl.col("daily_return").is_nan()).height
    null_count = cleaned.filter(pl.col("daily_return").is_null()).height
    assert nan_count == 0
    assert null_count == 0


def test_scale_and_encode_produces_scaler_and_mappings(
    make_raw_data: Callable[..., pl.DataFrame],
) -> None:
    data = make_raw_data(tickers=["AAPL"], days=60)
    pipeline = Pipeline()
    result = pipeline.run_to("clean_data", data)
    stage = ScaleAndEncode()
    encoded = stage.run(result)
    assert stage.scaler is not None
    assert stage.mappings is not None
    assert "ticker" in stage.mappings
    assert "sector" in stage.mappings
    assert encoded.height > 0


def test_pipeline_run_produces_complete_output(
    make_raw_data: Callable[..., pl.DataFrame],
) -> None:
    data = make_raw_data()
    pipeline = Pipeline()
    result = pipeline.run(data)
    assert result.height > 0
    scale_stage = pipeline.get_stage("scale_and_encode")
    assert isinstance(scale_stage, ScaleAndEncode)
    assert scale_stage.scaler is not None
    assert scale_stage.mappings is not None


def test_pipeline_run_to_stops_at_stage(
    make_raw_data: Callable[..., pl.DataFrame],
) -> None:
    data = make_raw_data()
    pipeline = Pipeline()
    result = pipeline.run_to("validate_columns", data)
    assert "day_of_week" not in result.columns


def test_pipeline_get_stage_raises_for_unknown() -> None:
    pipeline = Pipeline()
    with pytest.raises(ValueError, match="not found"):
        pipeline.get_stage("nonexistent")


def test_pipeline_snapshot_roundtrip(
    tmp_path: Path, make_raw_data: Callable[..., pl.DataFrame]
) -> None:
    data = make_raw_data(tickers=["AAPL"], days=10)
    pipeline = Pipeline()
    result = pipeline.run_to("engineer_features", data)
    snapshot_path = str(tmp_path / "snapshot.parquet")
    Pipeline.snapshot(result, snapshot_path)
    loaded = Pipeline.load_snapshot(snapshot_path)
    assert loaded.equals(result)


def test_data_preprocess_sets_attributes(
    make_raw_data: Callable[..., pl.DataFrame],
) -> None:
    raw = make_raw_data()
    data = Data()
    data.preprocess_and_set_data(raw)
    assert hasattr(data, "data")
    assert hasattr(data, "scaler")
    assert hasattr(data, "mappings")
    assert data.data.height > 0


def test_data_preprocess_output_columns_match_schema(
    make_raw_data: Callable[..., pl.DataFrame],
) -> None:
    raw = make_raw_data()
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
        "day_of_week",
        "day_of_month",
        "day_of_year",
        "month",
        "year",
        "time_idx",
        "daily_return",
    }
    assert expected_columns == set(data.data.columns)


def test_data_save_and_load_roundtrip(
    make_raw_data: Callable[..., pl.DataFrame],
) -> None:
    raw = make_raw_data()
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


def test_data_get_dimensions(
    make_raw_data: Callable[..., pl.DataFrame],
) -> None:
    raw = make_raw_data()
    data = Data()
    data.preprocess_and_set_data(raw)
    dimensions = data.get_dimensions()
    assert dimensions["past_continuous_features"] == EXPECTED_PAST_CONTINUOUS_FEATURES
    assert dimensions["past_categorical_features"] == EXPECTED_PAST_CATEGORICAL_FEATURES
    assert (
        dimensions["static_categorical_features"]
        == EXPECTED_STATIC_CATEGORICAL_FEATURES
    )


def test_data_get_dataset_train(
    make_raw_data: Callable[..., pl.DataFrame],
) -> None:
    raw = make_raw_data(days=90)
    data = Data()
    data.preprocess_and_set_data(raw)
    dataset = data.get_dataset(
        data_type="train",
        validation_split=0.8,
        input_length=35,
        output_length=5,
    )
    assert isinstance(dataset, TrainingDataset)
    assert len(dataset) > 0
    assert dataset.past_continuous.shape[1] == EXPECTED_INPUT_LENGTH
    assert dataset.past_categorical.shape[1] == EXPECTED_INPUT_LENGTH
    assert dataset.future_categorical.shape[1] == EXPECTED_OUTPUT_LENGTH
    assert dataset.static_categorical.shape[2] == EXPECTED_STATIC_CATEGORICAL_FEATURES
    assert dataset.targets is not None


def test_data_get_dataset_validate(
    make_raw_data: Callable[..., pl.DataFrame],
) -> None:
    # Need enough days so the 20% validation split has > input_length+output_length days
    raw = make_raw_data(days=300)
    data = Data()
    data.preprocess_and_set_data(raw)
    dataset = data.get_dataset(
        data_type="validate",
        validation_split=0.8,
        input_length=35,
        output_length=5,
    )
    assert isinstance(dataset, TrainingDataset)
    assert dataset.targets is not None


def test_data_get_dataset_predict_has_no_targets(
    make_raw_data: Callable[..., pl.DataFrame],
) -> None:
    raw = make_raw_data(days=90)
    data = Data()
    data.preprocess_and_set_data(raw)
    dataset = data.get_dataset(
        data_type="predict",
        input_length=35,
        output_length=5,
    )
    assert isinstance(dataset, TrainingDataset)
    assert dataset.targets is None


def test_data_get_dataset_invalid_type(
    make_raw_data: Callable[..., pl.DataFrame],
) -> None:
    raw = make_raw_data(days=90)
    data = Data()
    data.preprocess_and_set_data(raw)
    with pytest.raises(ValueError, match="Invalid data type"):
        data.get_dataset(data_type="invalid")


def test_scale_and_encode_raises_on_nan_scaler(
    make_raw_data: Callable[..., pl.DataFrame],
) -> None:
    data = make_raw_data(tickers=["AAPL"], days=60)
    pipeline = Pipeline()
    cleaned = pipeline.run_to("clean_data", data)
    stage = ScaleAndEncode()
    # Inject NaN into continuous column to trigger NaN in scaler means
    nan_data = cleaned.with_columns(pl.lit(float("nan")).alias("close_price"))
    with pytest.raises(ValueError, match="Scaler has NaN values"):
        stage.run(nan_data)


def test_pipeline_run_to_raises_for_unknown_stage(
    make_raw_data: Callable[..., pl.DataFrame],
) -> None:
    pipeline = Pipeline()
    data = make_raw_data()
    with pytest.raises(ValueError, match="Unknown stage"):
        pipeline.run_to("nonexistent_stage", data)


def test_pipeline_honors_explicit_empty_stage_list(
    make_raw_data: Callable[..., pl.DataFrame],
) -> None:
    data = make_raw_data()
    pipeline = Pipeline(stages=[])
    result = pipeline.run(data)
    assert result.equals(data)
