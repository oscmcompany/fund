from typing import TypedDict

import numpy as np
import polars as pl


class RegimeResult(TypedDict):
    state: str
    confidence: float


REGIME_WINDOW_DAYS = 60
REGIME_VOLATILITY_THRESHOLD = 0.20
REGIME_AUTOCORRELATION_THRESHOLD = 0.0

_TRADING_DAYS_PER_YEAR = 252
_MINIMUM_RETURN_COUNT = 2


def classify_regime(
    spy_prices: pl.DataFrame,
    window_days: int = REGIME_WINDOW_DAYS,
) -> RegimeResult:
    spy_close = (
        spy_prices.sort("timestamp").tail(window_days + 1)["close_price"].to_numpy()
    )

    if len(spy_close) < _MINIMUM_RETURN_COUNT:
        return {"state": "trending", "confidence": 0.0}

    returns = np.diff(np.log(spy_close))
    realized_volatility = float(
        np.std(returns, ddof=1) * np.sqrt(_TRADING_DAYS_PER_YEAR)
    )

    if len(returns) >= _MINIMUM_RETURN_COUNT:
        autocorrelation = float(np.corrcoef(returns[:-1], returns[1:])[0, 1])
    else:
        autocorrelation = 0.0

    low_volatility = realized_volatility < REGIME_VOLATILITY_THRESHOLD
    mean_reverting_signal = autocorrelation < REGIME_AUTOCORRELATION_THRESHOLD

    if low_volatility and mean_reverting_signal:
        volatility_margin = (
            REGIME_VOLATILITY_THRESHOLD - realized_volatility
        ) / REGIME_VOLATILITY_THRESHOLD
        autocorrelation_margin = min(1.0, -autocorrelation)
        confidence = float(
            np.clip((volatility_margin + autocorrelation_margin) / 2.0, 0.0, 1.0)
        )
        return {"state": "mean_reversion", "confidence": confidence}

    excess_volatility = max(
        0.0,
        (realized_volatility - REGIME_VOLATILITY_THRESHOLD)
        / REGIME_VOLATILITY_THRESHOLD,
    )
    excess_autocorrelation = max(
        0.0, autocorrelation - REGIME_AUTOCORRELATION_THRESHOLD
    )
    confidence = float(
        np.clip((excess_volatility + excess_autocorrelation) / 2.0, 0.0, 1.0)
    )
    return {"state": "trending", "confidence": confidence}
