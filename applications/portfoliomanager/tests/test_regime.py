import numpy as np
import polars as pl
from portfoliomanager.regime import (
    REGIME_AUTOCORRELATION_THRESHOLD,
    REGIME_VOLATILITY_THRESHOLD,
    REGIME_WINDOW_DAYS,
    classify_regime,
)


def _make_spy_prices_from_returns(
    returns: list[float], base_price: float = 100.0
) -> pl.DataFrame:
    prices = [base_price]
    for ret in returns:
        prices.append(prices[-1] * (1.0 + ret))
    return pl.DataFrame(
        {
            "ticker": ["SPY"] * len(prices),
            "timestamp": list(range(len(prices))),
            "close_price": prices,
        }
    )


def _make_low_vol_negative_autocorr_spy_prices() -> pl.DataFrame:
    # Alternating small returns → annualized vol ≈ 0.008 * sqrt(252) ≈ 0.13 < 0.20
    # Lag-1 autocorr ≈ -1.0 (strongly negative)
    n_returns = REGIME_WINDOW_DAYS + 1
    returns = [0.008 if i % 2 == 0 else -0.008 for i in range(n_returns)]
    return _make_spy_prices_from_returns(returns)


def _make_high_vol_negative_autocorr_spy_prices() -> pl.DataFrame:
    # Alternating large returns → annualized vol ≈ 0.025 * sqrt(252) ≈ 0.40 > 0.20
    # Lag-1 autocorr ≈ -1.0 (strongly negative)
    n_returns = REGIME_WINDOW_DAYS + 1
    returns = [0.025 if i % 2 == 0 else -0.025 for i in range(n_returns)]
    return _make_spy_prices_from_returns(returns)


def _make_low_vol_positive_autocorr_spy_prices() -> pl.DataFrame:
    # Sine wave with period 20 → lag-1 autocorr ≈ cos(2π/20) ≈ 0.95 > 0
    # Amplitude 0.003 → annualized vol ≈ 0.003/sqrt(2)*sqrt(252) ≈ 0.034 < 0.20
    n_returns = REGIME_WINDOW_DAYS + 1
    returns = [0.003 * np.sin(2 * np.pi * i / 20) for i in range(n_returns)]
    return _make_spy_prices_from_returns(returns)


def test_classify_regime_returns_mean_reversion_for_low_vol_negative_autocorr() -> None:
    spy_prices = _make_low_vol_negative_autocorr_spy_prices()
    result = classify_regime(spy_prices)
    assert result["state"] == "mean_reversion"


def test_classify_regime_returns_trending_for_high_vol_negative_autocorr() -> None:
    spy_prices = _make_high_vol_negative_autocorr_spy_prices()
    result = classify_regime(spy_prices)
    assert result["state"] == "trending"


def test_classify_regime_returns_trending_for_low_vol_positive_autocorr() -> None:
    spy_prices = _make_low_vol_positive_autocorr_spy_prices()
    result = classify_regime(spy_prices)
    assert result["state"] == "trending"


def test_classify_regime_confidence_is_in_valid_range() -> None:
    for spy_prices in [
        _make_low_vol_negative_autocorr_spy_prices(),
        _make_high_vol_negative_autocorr_spy_prices(),
        _make_low_vol_positive_autocorr_spy_prices(),
    ]:
        result = classify_regime(spy_prices)
        assert 0.0 <= result["confidence"] <= 1.0


def test_classify_regime_returns_trending_for_insufficient_data() -> None:
    spy_prices = pl.DataFrame(
        {"ticker": ["SPY"], "timestamp": [0], "close_price": [100.0]}
    )
    result = classify_regime(spy_prices)
    assert result["state"] == "trending"
    assert result["confidence"] == 0.0


def test_classify_regime_returns_trending_for_exactly_one_return() -> None:
    # Exactly 2 prices yields 1 return, which is below the minimum return count
    spy_prices = pl.DataFrame(
        {"ticker": ["SPY", "SPY"], "timestamp": [0, 1], "close_price": [100.0, 101.0]}
    )
    result = classify_regime(spy_prices)
    assert result["state"] == "trending"
    assert result["confidence"] == 0.0


def test_classify_regime_returns_trending_for_exactly_two_returns() -> None:
    # 3 prices yields 2 returns; np.corrcoef on 1-element arrays produces NaN,
    # so the guard must catch len(returns) == 2 before the autocorrelation step.
    spy_prices = pl.DataFrame(
        {
            "ticker": ["SPY", "SPY", "SPY"],
            "timestamp": [0, 1, 2],
            "close_price": [100.0, 101.0, 102.0],
        }
    )
    result = classify_regime(spy_prices)
    assert result["state"] == "trending"
    assert result["confidence"] == 0.0


def test_classify_regime_mean_reversion_confidence_is_positive() -> None:
    spy_prices = _make_low_vol_negative_autocorr_spy_prices()
    result = classify_regime(spy_prices)
    assert result["confidence"] > 0.0


def test_classify_regime_uses_module_constants() -> None:
    # Verify constants have the values discussed in design
    assert REGIME_VOLATILITY_THRESHOLD == 0.20  # noqa: PLR2004
    assert REGIME_AUTOCORRELATION_THRESHOLD == 0.0
    assert REGIME_WINDOW_DAYS == 60  # noqa: PLR2004
