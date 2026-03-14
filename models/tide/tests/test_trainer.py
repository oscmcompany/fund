from collections.abc import Callable

import polars as pl
import pytest
from tide.trainer import DEFAULT_CONFIGURATION, train_model

PARTIAL_HIDDEN_SIZE = 16


def test_train_model_returns_model_and_data(
    make_raw_data: Callable[..., pl.DataFrame],
) -> None:
    training_data = make_raw_data(days=90)
    model, data = train_model(training_data)
    assert model is not None
    assert data is not None
    assert hasattr(data, "scaler")
    assert hasattr(data, "mappings")


def test_train_model_uses_custom_configuration(
    make_raw_data: Callable[..., pl.DataFrame],
) -> None:
    training_data = make_raw_data(days=90)
    custom_config = dict(DEFAULT_CONFIGURATION)
    custom_hidden_size = 32
    custom_config["epoch_count"] = 1
    custom_config["hidden_size"] = custom_hidden_size
    model, _data = train_model(training_data, configuration=custom_config)
    assert model.hidden_size == custom_hidden_size


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
    model, _ = train_model(training_data)
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
