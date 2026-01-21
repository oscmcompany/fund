import pandera.polars as pa
import polars as pl
from pandera.polars import PolarsData


def check_dates_count_per_ticker(
    data: PolarsData,
    dates_count: int = 7,
) -> bool:
    grouped = data.lazyframe.group_by("ticker").agg(
        pl.col("timestamp").unique().alias("unique_dates")
    )

    unique_dates_per_ticker = grouped.collect()["unique_dates"].to_list()

    if not all(len(dates) == dates_count for dates in unique_dates_per_ticker):
        message = f"Each ticker must have exactly {dates_count} unique dates, found: {unique_dates_per_ticker}"  # noqa: E501
        raise ValueError(message)

    return True


def check_same_dates_per_ticker(data: PolarsData) -> bool:
    grouped = data.lazyframe.group_by("ticker").agg(
        pl.col("timestamp").unique().alias("unique_dates")
    )

    unique_dates_per_ticker = grouped.collect()["unique_dates"].to_list()

    if len(unique_dates_per_ticker) > 1:
        first_ticker_dates = set(unique_dates_per_ticker[0])
        for dates in unique_dates_per_ticker[1:]:
            if set(dates) != first_ticker_dates:
                message = f"Expected all tickers to have the same dates, mismatch between: {first_ticker_dates} and: {set(dates)}"  # noqa: E501
                raise ValueError(message)

    return True


def check_monotonic_quantiles(data: PolarsData) -> bool:
    lazy_frame = data.lazyframe.collect()

    if (
        not (lazy_frame["quantile_10"] <= lazy_frame["quantile_50"]).all()
        or not (lazy_frame["quantile_50"] <= lazy_frame["quantile_90"]).all()
    ):
        message = "Quantiles must be monotonic: q10 ≤ q50 ≤ q90"
        raise ValueError(message)

    return True


predictions_schema = pa.DataFrameSchema(
    columns={
        "ticker": pa.Column(
            dtype=str,
            checks=[
                pa.Check(
                    lambda s: s.upper() == s,
                    error="Ticker must be uppercase",
                    element_wise=True,
                )
            ],
        ),
        "timestamp": pa.Column(
            dtype=pl.Int64,
            checks=[pa.Check.greater_than(0)],
        ),
        "quantile_10": pa.Column(dtype=float),
        "quantile_50": pa.Column(dtype=float),
        "quantile_90": pa.Column(dtype=float),
    },
    coerce=True,
    checks=[
        # Model outputs 7 days, but server.py filters to 7th day before validation
        pa.Check(
            check_fn=lambda data: check_dates_count_per_ticker(
                data=data, dates_count=1
            ),
            name="check_dates_count_per_ticker",
            error="Each ticker must have expected date count",
        ),
        pa.Check(
            check_fn=lambda data: check_same_dates_per_ticker(data=data),
            name="check_same_dates_per_ticker",
            error="All tickers must have same date values",
        ),
        pa.Check(
            check_fn=lambda data: check_monotonic_quantiles(data=data),
            name="quantile_monotonic",
            error="Quantiles must be monotonic: q10 ≤ q50 ≤ q90",
        ),
    ],
    unique=["ticker", "timestamp"],
    name="prediction",
)
