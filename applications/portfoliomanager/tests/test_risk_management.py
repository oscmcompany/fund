from datetime import UTC, datetime

import polars as pl
import pytest
from portfoliomanager.beta import compute_portfolio_beta
from portfoliomanager.exceptions import InsufficientPairsError
from portfoliomanager.portfolio_schema import portfolio_schema
from portfoliomanager.risk_management import (
    REQUIRED_PAIRS,
    size_pairs_with_volatility_parity,
)

_CURRENT_TIMESTAMP = datetime(2025, 1, 15, 9, 30, tzinfo=UTC)


def _make_candidate_pairs(
    count: int = 10,
    long_vols: list[float] | None = None,
    short_vols: list[float] | None = None,
) -> pl.DataFrame:
    if long_vols is None:
        long_vols = [0.02] * count
    if short_vols is None:
        short_vols = [0.02] * count
    return pl.DataFrame(
        {
            "pair_id": [f"TICK{i:02d}A-TICK{i:02d}B" for i in range(count)],
            "long_ticker": [f"TICK{i:02d}A" for i in range(count)],
            "short_ticker": [f"TICK{i:02d}B" for i in range(count)],
            "z_score": [2.5] * count,
            "hedge_ratio": [1.0] * count,
            "signal_strength": [0.1] * count,
            "long_realized_volatility": long_vols,
            "short_realized_volatility": short_vols,
        }
    )


def _make_neutral_market_betas(count: int = 10) -> pl.DataFrame:
    tickers = []
    for i in range(count):
        tickers.extend([f"TICK{i:02d}A", f"TICK{i:02d}B"])
    return pl.DataFrame({"ticker": tickers, "market_beta": [1.0] * (count * 2)})


def _make_asymmetric_market_betas() -> pl.DataFrame:
    """Pairs 0-7: long_beta=2.0, short_beta=1.0.
    Pairs 8-9: long_beta=1.0, short_beta=2.0.
    """
    tickers = []
    betas = []
    for i in range(8):
        tickers.extend([f"TICK{i:02d}A", f"TICK{i:02d}B"])
        betas.extend([2.0, 1.0])
    for i in range(8, 10):
        tickers.extend([f"TICK{i:02d}A", f"TICK{i:02d}B"])
        betas.extend([1.0, 2.0])
    return pl.DataFrame({"ticker": tickers, "market_beta": betas})


def test_size_pairs_with_volatility_parity_long_equals_short_dollar_totals() -> None:
    pairs = _make_candidate_pairs()
    result = size_pairs_with_volatility_parity(
        pairs,
        maximum_capital=10000.0,
        current_timestamp=_CURRENT_TIMESTAMP,
        market_betas=_make_neutral_market_betas(),
    )
    long_sum = result.filter(pl.col("side") == "LONG")["dollar_amount"].sum()
    short_sum = result.filter(pl.col("side") == "SHORT")["dollar_amount"].sum()
    assert long_sum == pytest.approx(short_sum)


def test_size_pairs_with_volatility_parity_lower_volatility_receives_more_capital() -> (
    None
):
    long_vols = [0.01] + [0.04] * 9
    short_vols = [0.01] + [0.04] * 9
    pairs = _make_candidate_pairs(long_vols=long_vols, short_vols=short_vols)
    result = size_pairs_with_volatility_parity(
        pairs,
        maximum_capital=10000.0,
        current_timestamp=_CURRENT_TIMESTAMP,
        market_betas=_make_neutral_market_betas(),
    )
    long_df = result.filter(pl.col("side") == "LONG")
    low_vol_amount = long_df.filter(pl.col("ticker") == "TICK00A")[
        "dollar_amount"
    ].item()
    high_vol_amount = long_df.filter(pl.col("ticker") == "TICK01A")[
        "dollar_amount"
    ].item()
    assert low_vol_amount > high_vol_amount


def test_size_pairs_with_volatility_parity_raises_insufficient_pairs_error() -> None:
    pairs = _make_candidate_pairs(count=REQUIRED_PAIRS - 1)
    with pytest.raises(InsufficientPairsError):
        size_pairs_with_volatility_parity(
            pairs,
            maximum_capital=10000.0,
            current_timestamp=_CURRENT_TIMESTAMP,
            market_betas=_make_neutral_market_betas(count=REQUIRED_PAIRS - 1),
        )


def test_size_pairs_with_volatility_parity_output_passes_portfolio_schema_validate() -> (  # noqa: E501
    None
):
    pairs = _make_candidate_pairs()
    result = size_pairs_with_volatility_parity(
        pairs,
        maximum_capital=10000.0,
        current_timestamp=_CURRENT_TIMESTAMP,
        market_betas=_make_neutral_market_betas(),
    )
    validated = portfolio_schema.validate(result)
    assert validated.height == REQUIRED_PAIRS * 2


def test_size_pairs_with_volatility_parity_exposure_scale_halves_dollar_amounts() -> (
    None
):
    pairs = _make_candidate_pairs()
    market_betas = _make_neutral_market_betas()

    full_result = size_pairs_with_volatility_parity(
        pairs,
        maximum_capital=10000.0,
        current_timestamp=_CURRENT_TIMESTAMP,
        market_betas=market_betas,
        exposure_scale=1.0,
    )
    half_result = size_pairs_with_volatility_parity(
        pairs,
        maximum_capital=10000.0,
        current_timestamp=_CURRENT_TIMESTAMP,
        market_betas=market_betas,
        exposure_scale=0.5,
    )

    full_amounts = full_result.sort(["ticker", "side"])["dollar_amount"].to_list()
    half_amounts = half_result.sort(["ticker", "side"])["dollar_amount"].to_list()

    for full, half in zip(full_amounts, half_amounts, strict=False):
        assert half == pytest.approx(full * 0.5)


def test_size_pairs_with_volatility_parity_beta_neutral_reduces_portfolio_beta() -> (
    None
):
    # Pairs 0-7: long_beta=2.0, short_beta=1.0 (net positive contribution)
    # Pairs 8-9: long_beta=1.0, short_beta=2.0 (net negative contribution)
    # Equal vol-parity weights produce portfolio beta ≠ 0; optimizer drives it toward 0
    pairs = _make_candidate_pairs()
    asymmetric_betas = _make_asymmetric_market_betas()
    neutral_betas = _make_neutral_market_betas()

    beta_neutral_result = size_pairs_with_volatility_parity(
        pairs,
        maximum_capital=10000.0,
        current_timestamp=_CURRENT_TIMESTAMP,
        market_betas=asymmetric_betas,
    )
    vol_parity_result = size_pairs_with_volatility_parity(
        pairs,
        maximum_capital=10000.0,
        current_timestamp=_CURRENT_TIMESTAMP,
        market_betas=neutral_betas,
    )

    beta_neutral_beta = abs(
        compute_portfolio_beta(beta_neutral_result, asymmetric_betas)
    )
    vol_parity_beta = abs(compute_portfolio_beta(vol_parity_result, asymmetric_betas))

    assert beta_neutral_beta < vol_parity_beta
