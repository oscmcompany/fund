import numpy as np
import polars as pl
import scipy.stats

BETA_WINDOW_DAYS = 60

_TRADING_DAYS_PER_YEAR = 252
_MINIMUM_RETURN_COUNT = 2


def compute_market_betas(
    historical_prices: pl.DataFrame,
    spy_prices: pl.DataFrame,
    window_days: int = BETA_WINDOW_DAYS,
) -> pl.DataFrame:
    """Compute market beta for each ticker against SPY over the trailing window.

    market_beta measures the stock's sensitivity to broad market moves: values
    near 1.0 track the market closely, >1.0 amplify moves, and negative values
    indicate counter-cyclical or inverse behavior (e.g. gold miners).
    """
    spy_close = (
        spy_prices.sort("timestamp").tail(window_days + 1)["close_price"].to_numpy()
    )

    if len(spy_close) < _MINIMUM_RETURN_COUNT + 1 or np.any(spy_close <= 0):
        return pl.DataFrame(schema={"ticker": pl.String, "market_beta": pl.Float64})

    spy_returns = np.diff(np.log(spy_close))
    tickers = historical_prices["ticker"].unique().to_list()
    results = []

    for ticker in tickers:
        ticker_close = (
            historical_prices.filter(pl.col("ticker") == ticker)
            .sort("timestamp")
            .tail(window_days + 1)["close_price"]
            .to_numpy()
        )
        if len(ticker_close) < _MINIMUM_RETURN_COUNT or np.any(ticker_close <= 0):
            continue

        ticker_returns = np.diff(np.log(ticker_close))
        count = min(len(spy_returns), len(ticker_returns))
        if count < _MINIMUM_RETURN_COUNT:
            continue

        slope, _, _, _, _ = scipy.stats.linregress(
            spy_returns[-count:], ticker_returns[-count:]
        )
        results.append({"ticker": ticker, "market_beta": float(slope)})

    if not results:
        return pl.DataFrame(schema={"ticker": pl.String, "market_beta": pl.Float64})

    return pl.DataFrame(results)


# Validates beta neutralization in tests; retained for future beta reporting.
def compute_portfolio_beta(
    portfolio: pl.DataFrame,
    market_betas: pl.DataFrame,
) -> float:
    """Compute the net market exposure of the full portfolio.

    Sums the dollar-weighted beta across all positions (positive for LONG,
    negative for SHORT). A value near 0.0 means market risk has been hedged out.
    """
    beta_lookup = dict(
        zip(
            market_betas["ticker"].to_list(),
            market_betas["market_beta"].to_list(),
            strict=False,
        )
    )

    total_gross = portfolio["dollar_amount"].sum()
    if np.isclose(total_gross, 0.0):
        return 0.0

    net_beta = 0.0
    for row in portfolio.iter_rows(named=True):
        beta = beta_lookup.get(row["ticker"], 0.0)
        sign = 1.0 if row["side"] == "LONG" else -1.0
        net_beta += sign * (row["dollar_amount"] / total_gross) * beta

    return net_beta
