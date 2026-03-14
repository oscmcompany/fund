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
            .sort_by("timestamp")
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
    """Blend model predictions into per-ticker trading signals.

    Output columns:
      ensemble_alpha       - mean expected forward return across all models;
                             drives the long/short direction decision in pair selection
      ensemble_confidence  - how certain the ensemble is (0-1, normalized to the
                             most confident ticker); derived from the quantile spread
                             width: a narrow spread signals high model agreement
      realized_volatility  - trailing daily return volatility used for volatility-
                             parity position sizing
      sector               - GICS sector for pair eligibility filtering
    """
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

        latest_predictions = predictions_df.sort("timestamp").group_by("ticker").last()
        signals = latest_predictions.with_columns(
            pl.col("quantile_50").alias("alpha"),
            (
                1.0
                / (
                    1.0
                    + (pl.col("quantile_90") - pl.col("quantile_10")).clip(
                        lower_bound=0.0
                    )
                )
            ).alias("raw_confidence"),
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
