import json
import os
from datetime import date, datetime, timedelta
from typing import cast

import numpy as np
import pandera.polars as pa
import polars as pl
import structlog
from tinygrad.tensor import Tensor

logger = structlog.get_logger()


class Scaler:
    def __init__(self) -> None:
        pass

    def fit(self, data: pl.DataFrame) -> None:
        self.means = data.mean()
        self.standard_deviations = data.std()
        self.standard_deviations = self.standard_deviations.select(
            pl.all().replace(0, 1e-8)
        )  # avoid division by zero

    def transform(self, data: pl.DataFrame) -> pl.DataFrame:
        return (data - self.means) / self.standard_deviations

    def inverse_transform(self, data: pl.DataFrame) -> pl.DataFrame:
        return data * self.standard_deviations + self.means


class Data:
    """Time-series dense encoder data preprocessing and postprocessing."""

    def __init__(self) -> None:
        pass

    def preprocess_and_set_data(self, data: pl.DataFrame) -> None:
        data = data.clone()

        raw_columns = (
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
        )

        if set(data.columns) != set(raw_columns):
            message = f"Expected columns {raw_columns} but got {data.columns}"
            raise ValueError(message)

        self.continuous_columns = [
            "open_price",
            "high_price",
            "low_price",
            "close_price",
            "volume",
            "volume_weighted_average_price",
            "daily_return",
        ]

        self.categorical_columns = [
            "day_of_week",
            "day_of_month",
            "day_of_year",
            "month",
            "year",
            "is_holiday",
        ]

        self.static_categorical_columns = [
            "ticker",
            "sector",
            "industry",
        ]

        data = data.with_columns(
            pl.col("timestamp")
            .cast(pl.Datetime(time_unit="ms"))
            .dt.date()
            .alias("date"),
            pl.lit(False).alias("is_holiday"),  # noqa: FBT003
        )

        tickers = data.select(pl.col("ticker").unique())

        minimum_date: date = data.select(pl.col("date").min()).item()
        maximum_date: date = data.select(pl.col("date").max()).item()

        dates = pl.DataFrame(
            {
                "date": pl.date_range(
                    minimum_date,
                    maximum_date,
                    "1d",
                    eager=True,
                )
            }
        )

        dates_and_tickers = tickers.join(dates, how="cross")

        data = dates_and_tickers.join(data, on=["ticker", "date"], how="left")

        friday_number = 4

        # set is_holiday value for missing weekdays
        data = (
            data.with_columns(pl.col("date").dt.weekday().alias("temporary_weekday"))
            .with_columns(
                pl.when(
                    pl.col("is_holiday").is_null()
                    & (pl.col("temporary_weekday") <= friday_number)
                )
                .then(True)  # noqa: FBT003
                .when(
                    pl.col("is_holiday").is_null()
                    & (pl.col("temporary_weekday") > friday_number)
                )
                .then(False)  # noqa: FBT003
                .otherwise(pl.col("is_holiday"))  # keep existing values
                .alias("is_holiday")
            )
            .drop("temporary_weekday")
        )

        # ensure all rows have values instead of nulls
        data = data.with_columns(
            [
                pl.col("open_price").fill_null(0.0),
                pl.col("high_price").fill_null(0.0),
                pl.col("low_price").fill_null(0.0),
                pl.col("close_price").fill_null(0.0),
                pl.col("volume").fill_null(0.0),
                pl.col("volume_weighted_average_price").fill_null(0.0),
                pl.col("sector").fill_null("NOT AVAILABLE"),
                pl.col("industry").fill_null("NOT AVAILABLE"),
                pl.col("ticker").fill_null("UNKNOWN"),
                pl.col("timestamp").fill_null(
                    pl.col("date")
                    .cast(pl.Datetime)
                    .dt.replace_time_zone("UTC")
                    .cast(pl.Int64)
                    .floordiv(1000)
                ),
            ]
        )

        # compute new calendar columns
        data = data.with_columns(
            pl.col("date").dt.weekday().alias("day_of_week").cast(pl.Int64),
            pl.col("date").dt.day().alias("day_of_month").cast(pl.Int64),
            pl.col("date").dt.ordinal_day().alias("day_of_year").cast(pl.Int64),
            pl.col("date").dt.month().alias("month").cast(pl.Int64),
            pl.col("date").dt.year().alias("year").cast(pl.Int64),
        )

        # add time index column
        data = data.sort(["ticker", "timestamp"]).with_columns(
            pl.col("timestamp")
            .rank("dense")
            .over("ticker")
            .cast(pl.Int64)
            .alias("time_idx")
        )

        data = data.with_columns(
            pl.col("close_price").pct_change().over("ticker").alias("daily_return")
        )

        data = data.with_columns(
            [
                pl.col("ticker").str.to_uppercase(),
                pl.col("sector").str.to_uppercase(),
                pl.col("industry").str.to_uppercase(),
            ]
        )

        data = data.filter(pl.col("ticker") != "UNKNOWN")

        data = data.filter(
            pl.col("daily_return").is_not_null()
            & pl.col("daily_return").is_not_nan()
            & pl.col("daily_return").is_finite()
        )

        data = data.unique(subset=["ticker", "timestamp"])

        for col in self.continuous_columns:
            nan_count = data.filter(pl.col(col).is_nan()).height
            null_count = data.filter(pl.col(col).is_null()).height
            inf_count = data.filter(~pl.col(col).is_finite()).height
            if nan_count > 0 or null_count > 0 or inf_count > 0:
                logger.warning(
                    "Invalid values in continuous column before scaling",
                    column=col,
                    nan_count=nan_count,
                    null_count=null_count,
                    inf_count=inf_count,
                )
                # Filter out rows with invalid values to prevent downstream errors
                data = data.filter(
                    pl.col(col).is_not_nan()
                    & pl.col(col).is_not_null()
                    & pl.col(col).is_finite()
                )

        data_validated = data_schema.validate(data)
        data = cast(
            "pl.DataFrame",
            data_validated.collect()
            if isinstance(data_validated, pl.LazyFrame)
            else data_validated,
        )

        self.scaler = Scaler()

        self.scaler.fit(data[self.continuous_columns])

        for col in self.continuous_columns:
            mean_val = self.scaler.means[col].item()
            std_val = self.scaler.standard_deviations[col].item()
            if np.isnan(mean_val) or np.isnan(std_val):
                logger.error(
                    "Scaler has NaN values",
                    column=col,
                    mean=mean_val,
                    std=std_val,
                )

        data = data.with_columns(  # scale continuous columns
            *[
                (pl.col(col) - self.scaler.means[col])
                / self.scaler.standard_deviations[col]
                for col in self.continuous_columns
            ]
        )

        for col in self.continuous_columns:
            nan_count = data.filter(pl.col(col).is_nan()).height
            if nan_count > 0:
                logger.error(
                    "NaN values after scaling",
                    column=col,
                    nan_count=nan_count,
                    total_rows=data.height,
                )

        mapping_columns = [
            "ticker",
            "sector",
            "industry",
            "is_holiday",
        ]

        mappings: dict[str, dict[str, int]] = {}

        for column in mapping_columns:
            data, mapping = self._create_mapping_and_encoding(data, column)
            mappings[column] = mapping

        self.mappings = mappings

        self.data = data

    def _create_mapping_and_encoding(
        self,
        data: pl.DataFrame,
        column: str,
    ) -> tuple[pl.DataFrame, dict]:
        unique_values = data[column].unique().to_list()

        mapping = {val: idx for idx, val in enumerate(unique_values)}

        data = data.with_columns(
            pl.col(column).replace(mapping).cast(pl.Int32).alias(column)
        )

        return data, mapping

    def _get_training_and_validation_data(
        self,
        validation_split: float = 0.8,
    ) -> tuple[pl.DataFrame, pl.DataFrame]:
        if validation_split in {0.0, 1.0}:
            message = "Validation split must be between 0.0 and 1.0 (exclusive)."
            raise ValueError(message)

        minimum_date: date = self.data.select(self.data["date"].min()).item()
        maximum_date: date = self.data.select(self.data["date"].max()).item()

        time_difference = maximum_date - minimum_date
        split_date = minimum_date + (time_difference * validation_split)

        training_data = self.data.filter(pl.col("date") <= split_date)
        validation_data = self.data.filter(pl.col("date") > split_date)

        return training_data, validation_data

    def _get_prediction_data(
        self,
        maximum_encoder_length: int = 35,
    ) -> pl.DataFrame:
        return (
            self.data.sort("timestamp")
            .group_by("ticker")
            .agg(pl.col("*").tail(maximum_encoder_length))
            .explode([col for col in self.data.columns if col != "ticker"])
        )

    def get_dimensions(self) -> dict[str, int]:
        return {
            "encoder_categorical_features": len(self.categorical_columns),
            "encoder_continuous_features": len(self.continuous_columns),
            "decoder_categorical_features": len(self.categorical_columns),
            "decoder_continuous_features": 0,  # not using decoder_continuous_features for now  # noqa: E501
            "static_categorical_features": len(self.static_categorical_columns),
            "static_continuous_features": 0,  # not using static_continuous_features for now  # noqa: E501
        }

    def get_batches(  # noqa: C901
        self,
        data_type: str = "train",  # "train", "validate", or "predict"
        validation_split: float = 0.8,
        input_length: int = 35,
        output_length: int = 7,
        batch_size: int = 32,
    ) -> list[dict[str, Tensor]]:
        if data_type not in {"train", "validate", "predict"}:
            message = f"Invalid data type: {data_type}. Must be 'train', 'validate', or 'predict'."  # noqa: E501
            raise ValueError(message)

        if data_type == "train":
            self.batch_data, _ = self._get_training_and_validation_data(
                validation_split
            )

        elif data_type == "validate":
            _, self.batch_data = self._get_training_and_validation_data(
                validation_split
            )

        elif data_type == "predict":
            self.batch_data = self._get_prediction_data(input_length + output_length)

        minimum_date: date = self.batch_data.select(
            self.batch_data["date"].min()
        ).item()
        maximum_date: date = self.batch_data.select(
            self.batch_data["date"].max()
        ).item()

        total_days = (maximum_date - minimum_date).days + 1
        required_days = input_length + output_length

        if total_days < required_days:
            message = (
                f"Total days available: {total_days}, required days: {required_days}."
            )
            raise ValueError(message)

        # collect all samples first
        samples = []
        has_targets = data_type in {"train", "validate"}

        # Partition by ticker once upfront (much faster than filtering per ticker)
        logger.info("partitioning_data_by_ticker")
        ticker_groups = self.batch_data.sort("time_idx").partition_by(
            "ticker", as_dict=True
        )
        total_tickers = len(ticker_groups)
        logger.info("batch_creation_started", total_tickers=total_tickers)

        for idx, ticker_df in enumerate(ticker_groups.values()):
            if idx % 25 == 0:
                logger.info(
                    "batch_progress", ticker_idx=idx, total_tickers=total_tickers
                )

            # Convert to numpy once per ticker (avoid repeated DataFrame operations)
            # Use float32 for GPU compatibility (Metal doesn't support float64)
            cat_array = ticker_df[self.categorical_columns].to_numpy().astype(np.int32)
            cont_array = (
                ticker_df[self.continuous_columns].to_numpy().astype(np.float32)
            )
            static_array = (
                ticker_df[self.static_categorical_columns]
                .head(1)
                .to_numpy()
                .astype(np.int32)
            )
            target_array = (
                ticker_df[["daily_return"]].to_numpy().astype(np.float32)
                if has_targets
                else None
            )

            num_rows = len(ticker_df)
            num_windows = num_rows - input_length - output_length + 1

            if num_windows <= 0:
                continue

            # For prediction, only use the last window (most recent data)
            # For training/validation, use all windows
            window_indices = (
                [num_windows - 1] if data_type == "predict" else range(num_windows)
            )

            # Use numpy slicing (much faster than DataFrame slicing)
            for i in window_indices:
                sample = {
                    "encoder_categorical": cat_array[i : i + input_length].copy(),
                    "encoder_continuous": cont_array[i : i + input_length].copy(),
                    "decoder_categorical": cat_array[
                        i + input_length : i + input_length + output_length
                    ].copy(),
                    "static_categorical": static_array.copy(),
                }

                if has_targets and target_array is not None:
                    sample["targets"] = target_array[
                        i + input_length : i + input_length + output_length
                    ].copy()

                samples.append(sample)

        logger.info("sample_collection_complete", total_samples=len(samples))

        # now batch the samples
        logger.info("batching_samples", batch_size=batch_size)
        batches = []
        for i in range(0, len(samples), batch_size):
            batch_samples = samples[i : i + batch_size]

            batch = {
                "encoder_categorical_features": Tensor(
                    np.stack([s["encoder_categorical"] for s in batch_samples])
                ),
                "encoder_continuous_features": Tensor(
                    np.stack([s["encoder_continuous"] for s in batch_samples])
                ),
                "decoder_categorical_features": Tensor(
                    np.stack([s["decoder_categorical"] for s in batch_samples])
                ),
                "static_categorical_features": Tensor(
                    np.stack([s["static_categorical"] for s in batch_samples])
                ),
            }

            if data_type in {"train", "validate"}:
                batch["targets"] = Tensor(
                    np.stack([s["targets"] for s in batch_samples])
                )

            batches.append(batch)

        logger.info("batch_creation_complete", total_batches=len(batches))
        return batches

    def save(self, directory_path: str) -> None:
        os.makedirs(directory_path, exist_ok=True)  # noqa: PTH103

        with open(os.path.join(directory_path, "tide_data_mappings.json"), "w") as f:  # noqa: PTH118, PTH123
            json.dump(self.mappings, f)

        with open(os.path.join(directory_path, "tide_data_scaler.json"), "w") as f:  # noqa: PTH118, PTH123
            # convert DataFrames to dictionary of scalars (first row)
            means_dict = {
                col: self.scaler.means[col].item() for col in self.scaler.means.columns
            }
            stdevs_dict = {
                col: self.scaler.standard_deviations[col].item()
                for col in self.scaler.standard_deviations.columns
            }

            json.dump(
                {
                    "means": means_dict,
                    "standard_deviations": stdevs_dict,
                    "continuous_columns": self.continuous_columns,
                    "categorical_columns": self.categorical_columns,
                    "static_categorical_columns": self.static_categorical_columns,
                },
                f,
            )

    @classmethod
    def load(cls, directory_path: str) -> "Data":
        data = cls()
        with open(os.path.join(directory_path, "tide_data_mappings.json")) as f:  # noqa: PTH118, PTH123
            data.mappings = json.load(f)

        with open(os.path.join(directory_path, "tide_data_scaler.json")) as f:  # noqa: PTH118, PTH123
            scaler_data = json.load(f)

        data.scaler = Scaler()
        data.scaler.means = pl.DataFrame(scaler_data["means"])
        data.scaler.standard_deviations = pl.DataFrame(
            scaler_data["standard_deviations"]
        )

        data.continuous_columns = scaler_data["continuous_columns"]
        data.categorical_columns = scaler_data["categorical_columns"]
        data.static_categorical_columns = scaler_data["static_categorical_columns"]

        return data

    def postprocess_predictions(
        self,
        input_batch: dict[
            str, Tensor
        ],  # batch dictionary with static_categorical_features
        predictions: Tensor,  # quantiles
        current_datetime: datetime,
    ) -> pl.DataFrame:
        predictions_array = predictions.numpy()

        batch_size, output_length, _num_quantiles = predictions_array.shape

        ticker_reverse_mapping = {v: k for k, v in self.mappings["ticker"].items()}

        rows = []
        for batch_idx in range(batch_size):
            ticker_encoded = int(
                input_batch["static_categorical_features"][batch_idx, 0, 0].item()
            )
            ticker_str = ticker_reverse_mapping[ticker_encoded]

            for time_idx in range(output_length):
                timestamp = int(
                    (current_datetime + timedelta(days=time_idx))
                    .replace(
                        hour=0,
                        minute=0,
                        second=0,
                        microsecond=0,
                    )
                    .timestamp()
                )

                quantile_values = predictions_array[batch_idx, time_idx, :]

                daily_return_mean = self.scaler.means["daily_return"].item()
                daily_return_standard_deviation = self.scaler.standard_deviations[
                    "daily_return"
                ].item()

                unscaled_quantiles = (
                    quantile_values * daily_return_standard_deviation
                ) + daily_return_mean

                row = {
                    "ticker": ticker_str,
                    "timestamp": timestamp,
                    "quantile_10": unscaled_quantiles[0],
                    "quantile_50": unscaled_quantiles[1],
                    "quantile_90": unscaled_quantiles[2],
                }
                rows.append(row)

        return pl.DataFrame(rows)


data_schema = pa.DataFrameSchema(
    {
        "ticker": pa.Column(
            dtype=str,
            checks=pa.Check.str_matches(r"^[A-Z0-9.\-]+$"),
        ),
        "timestamp": pa.Column(
            dtype=int,
            checks=pa.Check.greater_than(0),
        ),
        "open_price": pa.Column(
            dtype=float,
            checks=pa.Check.greater_than_or_equal_to(
                0
            ),  # zeros are allowed for missing days
        ),
        "high_price": pa.Column(
            dtype=float,
            checks=pa.Check.greater_than_or_equal_to(0),
        ),
        "low_price": pa.Column(
            dtype=float,
            checks=pa.Check.greater_than_or_equal_to(0),
        ),
        "close_price": pa.Column(
            dtype=float,
            checks=pa.Check.greater_than_or_equal_to(0),
        ),
        "volume": pa.Column(
            dtype=int,
            checks=pa.Check.greater_than_or_equal_to(0),
        ),
        "volume_weighted_average_price": pa.Column(
            dtype=float,
            checks=pa.Check.greater_than_or_equal_to(0),
        ),
        "sector": pa.Column(dtype=str),
        "industry": pa.Column(dtype=str),
        "date": pa.Column(dtype=date),
        "day_of_week": pa.Column(dtype=int),
        "day_of_month": pa.Column(dtype=int),
        "day_of_year": pa.Column(dtype=int),
        "month": pa.Column(dtype=int),
        "year": pa.Column(dtype=int),
        "is_holiday": pa.Column(dtype=bool),
        "time_idx": pa.Column(dtype=int),
        "daily_return": pa.Column(dtype=float),
    },
    coerce=True,
)
