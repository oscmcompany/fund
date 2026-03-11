from typing import cast

import pandera.polars as pa
import polars as pl
from pandera.polars import PolarsData

from .enums import PositionAction, PositionSide


def is_uppercase(data: PolarsData) -> pl.LazyFrame:
    return data.lazyframe.select(
        pl.col(data.key).str.to_uppercase() == pl.col(data.key)
    )


def check_position_side_counts(
    data: PolarsData,
    total_positions_count: int = 20,  # 10 long and 10 short
) -> bool:
    counts = cast(
        "pl.DataFrame",
        data.lazyframe.select(
            pl.len().alias("total_count"),
            (pl.col("side") == PositionSide.LONG.value).sum().alias("long_count"),
            (pl.col("side") == PositionSide.SHORT.value).sum().alias("short_count"),
        ).collect(),
    )
    total_count = counts.get_column("total_count").item()
    long_count = counts.get_column("long_count").item()
    short_count = counts.get_column("short_count").item()
    side_count = total_positions_count // 2
    if long_count != side_count:
        message = f"Expected {side_count} long side positions, found: {long_count}"
        raise ValueError(message)

    if short_count != side_count:
        message = f"Expected {side_count} short side positions, found: {short_count}"
        raise ValueError(message)

    if total_count != total_positions_count:
        message = (
            f"Expected {total_positions_count} total positions, found: {total_count}"
        )
        raise ValueError(message)

    return True


def check_position_side_sums(
    data: PolarsData,
    maximum_imbalance_percentage: float = 0.05,  # 5%
) -> bool:
    sums = cast(
        "pl.DataFrame",
        data.lazyframe.select(
            pl.when(pl.col("side") == PositionSide.LONG.value)
            .then(pl.col("dollar_amount"))
            .otherwise(0.0)
            .sum()
            .alias("long_sum"),
            pl.when(pl.col("side") == PositionSide.SHORT.value)
            .then(pl.col("dollar_amount"))
            .otherwise(0.0)
            .sum()
            .alias("short_sum"),
        ).collect(),
    )

    long_sum = float(sums.get_column("long_sum").fill_null(0.0).item())
    short_sum = float(sums.get_column("short_sum").fill_null(0.0).item())
    total_sum = long_sum + short_sum

    if total_sum <= 0.0:
        message = "Total dollar amount must be > 0 to assess imbalance"
        raise ValueError(message)

    if abs(long_sum - short_sum) / total_sum > maximum_imbalance_percentage:
        message = (
            "Expected long and short dollar amount sums to be within "
            f"{maximum_imbalance_percentage * 100}%, "
            f"found long: {long_sum}, short: {short_sum}"
        )
        raise ValueError(message)

    return True


def check_pair_tickers_different(data: PolarsData) -> bool:
    result = cast(
        "pl.DataFrame",
        data.lazyframe.select(
            (pl.col("long_ticker") != pl.col("short_ticker"))
            .all()
            .alias("all_different")
        ).collect(),
    )
    if not result.get_column("all_different").item():
        message = "long_ticker and short_ticker must be different for every pair"
        raise ValueError(message)
    return True


pairs_schema = pa.DataFrameSchema(
    {
        "pair_id": pa.Column(dtype=str),
        "long_ticker": pa.Column(dtype=str, checks=[pa.Check(is_uppercase)]),
        "short_ticker": pa.Column(dtype=str, checks=[pa.Check(is_uppercase)]),
        "z_score": pa.Column(dtype=float, checks=[pa.Check.greater_than(0)]),
        "hedge_ratio": pa.Column(dtype=float),
        "signal_strength": pa.Column(
            dtype=float, checks=[pa.Check.greater_than_or_equal_to(0)]
        ),
        "long_realized_volatility": pa.Column(
            dtype=float, checks=[pa.Check.greater_than(0)]
        ),
        "short_realized_volatility": pa.Column(
            dtype=float, checks=[pa.Check.greater_than(0)]
        ),
    },
    unique=["pair_id"],
    coerce=True,
    checks=[
        pa.Check(
            check_fn=lambda df: check_pair_tickers_different(df),
            error="Long and short tickers must be different for every pair",
        ),
    ],
)


portfolio_schema = pa.DataFrameSchema(
    {
        "ticker": pa.Column(
            dtype=str,
            checks=[pa.Check(is_uppercase)],
        ),
        "timestamp": pa.Column(
            dtype=pl.Float64,
            checks=[pa.Check.greater_than(0)],
        ),
        "side": pa.Column(
            dtype=str,
            checks=[
                pa.Check.isin(
                    [
                        PositionSide.LONG.value.upper(),
                        PositionSide.SHORT.value.upper(),
                    ]
                ),
                pa.Check(is_uppercase),
            ],
        ),
        "dollar_amount": pa.Column(
            dtype=float,
            checks=[pa.Check.greater_than(0)],
        ),
        "action": pa.Column(
            dtype=str,
            checks=[
                pa.Check.isin(
                    [
                        PositionAction.OPEN_POSITION.value,
                        PositionAction.CLOSE_POSITION.value,
                        PositionAction.UNSPECIFIED.value,
                    ]
                ),
                pa.Check(is_uppercase),
            ],
            required=False,
        ),
        "pair_id": pa.Column(dtype=str),
    },
    unique=["ticker"],
    coerce=True,
    checks=[
        pa.Check(
            check_fn=check_position_side_counts,
            error="Each side must have expected position counts",
        ),
        pa.Check(
            check_fn=check_position_side_sums,
            error="Position side sums must be approximately equal",
        ),
    ],
)
