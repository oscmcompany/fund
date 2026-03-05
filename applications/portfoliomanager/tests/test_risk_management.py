from datetime import UTC, datetime

import polars as pl
import pytest
from portfoliomanager.exceptions import InsufficientPairsError
from portfoliomanager.portfolio_schema import portfolio_schema
from portfoliomanager.risk_management import (
    MINIMUM_PAIRS_REQUIRED,
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


def test_size_pairs_with_volatility_parity_long_equals_short_dollar_totals() -> None:
    pairs = _make_candidate_pairs()
    result = size_pairs_with_volatility_parity(
        pairs, maximum_capital=10000.0, current_timestamp=_CURRENT_TIMESTAMP
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
        pairs, maximum_capital=10000.0, current_timestamp=_CURRENT_TIMESTAMP
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
    pairs = _make_candidate_pairs(count=MINIMUM_PAIRS_REQUIRED - 1)
    with pytest.raises(InsufficientPairsError):
        size_pairs_with_volatility_parity(
            pairs, maximum_capital=10000.0, current_timestamp=_CURRENT_TIMESTAMP
        )


def test_size_pairs_with_volatility_parity_output_passes_portfolio_schema_validate() -> (  # noqa: E501
    None
):
    pairs = _make_candidate_pairs()
    result = size_pairs_with_volatility_parity(
        pairs, maximum_capital=10000.0, current_timestamp=_CURRENT_TIMESTAMP
    )
    validated = portfolio_schema.validate(result)
    assert validated.height == MINIMUM_PAIRS_REQUIRED * 2
