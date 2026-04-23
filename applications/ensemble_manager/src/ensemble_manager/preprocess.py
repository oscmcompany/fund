import polars as pl
import structlog

logger = structlog.get_logger()


def filter_to_trained_tickers(
    data: pl.DataFrame,
    trained_tickers: set[str],
) -> pl.DataFrame:
    input_tickers = set(data["ticker"].unique().to_list())
    dropped_tickers = input_tickers - trained_tickers

    if dropped_tickers:
        logger.warning(
            "Dropping tickers not in trained set",
            dropped_count=len(dropped_tickers),
            dropped_tickers=sorted(dropped_tickers),
        )

    return data.filter(pl.col("ticker").is_in(trained_tickers))


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
