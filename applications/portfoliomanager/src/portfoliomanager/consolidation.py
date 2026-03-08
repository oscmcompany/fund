import polars as pl

VOLATILITY_WINDOW_DAYS = 20


def compute_ticker_volatility(historical_prices: pl.DataFrame) -> pl.DataFrame:
    return (
        historical_prices.sort(["ticker", "timestamp"])
        .with_columns(
            pl.col("close_price").pct_change().over("ticker").alias("daily_return")
        )
        .group_by("ticker")
        .agg(
            pl.col("daily_return")
            .drop_nulls()
            .tail(VOLATILITY_WINDOW_DAYS)
            .std()
            .alias("realized_volatility")
        )
    )


def consolidate_predictions(
    model_predictions: dict[str, pl.DataFrame],
    historical_prices: pl.DataFrame,
    equity_details: pl.DataFrame,
) -> pl.DataFrame:
    if not model_predictions:
        message = "model_predictions must not be empty"
        raise ValueError(message)

    required_columns = {
        "ticker",
        "timestamp",
        "quantile_10",
        "quantile_50",
        "quantile_90",
    }
    per_model_signals = []

    for model_name, predictions_df in model_predictions.items():
        missing_columns = required_columns - set(predictions_df.columns)
        if missing_columns:
            message = (
                f"Model '{model_name}' predictions missing required columns: "
                f"{missing_columns}"
            )
            raise ValueError(message)

        signals = predictions_df.with_columns(
            pl.col("quantile_50").alias("alpha"),
            (1.0 / (1.0 + (pl.col("quantile_90") - pl.col("quantile_10")))).alias(
                "raw_confidence"
            ),
        ).select(["ticker", "alpha", "raw_confidence"])

        per_model_signals.append(signals)

    blended = (
        pl.concat(per_model_signals)
        .group_by("ticker")
        .agg(
            pl.col("alpha").mean().alias("ensemble_alpha"),
            pl.col("raw_confidence").mean().alias("raw_confidence"),
        )
    )

    maximum_raw_confidence = blended["raw_confidence"].max() or 1.0
    blended = blended.with_columns(
        (pl.col("raw_confidence") / maximum_raw_confidence).alias("ensemble_confidence")
    ).drop("raw_confidence")

    ticker_volatility = compute_ticker_volatility(historical_prices)

    return (
        blended.join(ticker_volatility, on="ticker", how="left")
        .join(equity_details.select(["ticker", "sector"]), on="ticker", how="left")
        .with_columns(pl.col("sector").fill_null("NOT AVAILABLE"))
        .drop_nulls(subset=["realized_volatility"])
        .select(
            [
                "ticker",
                "ensemble_alpha",
                "ensemble_confidence",
                "realized_volatility",
                "sector",
            ]
        )
    )
