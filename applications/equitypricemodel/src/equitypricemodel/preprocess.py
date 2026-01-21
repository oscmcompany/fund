import polars as pl


def filter_equity_bars(
    data: pl.DataFrame,
    minimum_average_close_price: float = 10.0,
    minimum_average_volume: float = 1_000_000.0,
) -> pl.DataFrame:
    valid_tickers = (
        data.group_by("ticker")
        .agg(
            average_close_price=pl.col("close_price").mean(),
            average_volume=pl.col("volume").mean(),
        )
        .filter(
            (pl.col("average_close_price") > minimum_average_close_price)
            & (pl.col("average_volume") > minimum_average_volume)
        )
        .select("ticker")
    )

    return data.join(valid_tickers, on="ticker", how="semi")
