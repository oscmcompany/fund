import pandera.polars as pa
import polars as pl

equity_bars_schema = pa.DataFrameSchema(
    {
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
        # Unix milliseconds (Int64). Massive sends bar timestamps natively in
        # milliseconds; Alpaca RFC-3339 strings resolve to the same precision for
        # OHLCV bars. Use internal.timestamps.to_timestamp_milliseconds() to
        # produce this value. Do not use int(dt.timestamp()) — that produces seconds.
        "timestamp": pa.Column(
            dtype=pl.Int64,
            checks=[pa.Check.greater_than(0)],
        ),
        "open_price": pa.Column(
            dtype=float,
            checks=[
                pa.Check.greater_than(0)
            ],  # raw data will not have missing days and therefore no zero values
        ),
        "high_price": pa.Column(
            dtype=float,
            checks=[pa.Check.greater_than(0)],
        ),
        "low_price": pa.Column(
            dtype=float,
            checks=[pa.Check.greater_than(0)],
        ),
        "close_price": pa.Column(
            dtype=float,
            checks=[pa.Check.greater_than(0)],
        ),
        # Whole share units (Int64). Massive sends volume as a float but bar
        # volumes are always whole shares; fractional values are rounded on
        # ingestion in the data manager.
        "volume": pa.Column(
            dtype=int,
            checks=[pa.Check.greater_than_or_equal_to(0)],
        ),
        "volume_weighted_average_price": pa.Column(
            dtype=float,
            nullable=True,
            checks=[pa.Check.greater_than_or_equal_to(0)],
        ),
        "transactions": pa.Column(
            dtype=int,
            nullable=True,
            checks=[pa.Check.greater_than_or_equal_to(0)],
        ),
    },
    unique=["ticker", "timestamp"],
    strict="filter",  # allows DuckDB partion columns
    ordered=True,
    name="equity_bar",
    coerce=True,
)
