from datetime import UTC, datetime

import polars as pl
import pytest
from portfolio_manager.beta import compute_portfolio_beta
from portfolio_manager.exceptions import InsufficientPairsError
from portfolio_manager.portfolio_schema import portfolio_schema
from portfolio_manager.risk_management import (
    REQUIRED_PAIRS,
    size_pairs_with_volatility_parity,
)

_CURRENT_TIMESTAMP = datetime(2025, 1, 15, 9, 30, tzinfo=UTC)
_DEFAULT_ENTRY_PRICE = 10.0


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


def _make_entry_prices(
    count: int = 10, price: float = _DEFAULT_ENTRY_PRICE
) -> dict[str, float]:
    prices: dict[str, float] = {}
    for i in range(count):
        prices[f"TICK{i:02d}A"] = price
        prices[f"TICK{i:02d}B"] = price
    return prices


def test_size_pairs_with_volatility_parity_short_uses_whole_share_dollar_amount() -> (
    None
):
    pairs = _make_candidate_pairs()
    result = size_pairs_with_volatility_parity(
        pairs,
        maximum_capital=10000.0,
        current_timestamp=_CURRENT_TIMESTAMP,
        market_betas=_make_neutral_market_betas(),
        entry_prices=_make_entry_prices(price=10.0),
    )
    short_rows = result.filter(pl.col("side") == "SHORT")
    for row in short_rows.iter_rows(named=True):
        entry_price = row["entry_price"]
        dollar_amount = row["dollar_amount"]
        quantity = int(dollar_amount / entry_price)
        # dollar_amount must equal quantity * entry_price (whole-share adjusted)
        assert dollar_amount == pytest.approx(quantity * entry_price)


def test_size_pairs_with_volatility_parity_long_matches_short_dollar_amount() -> None:
    pairs = _make_candidate_pairs()
    result = size_pairs_with_volatility_parity(
        pairs,
        maximum_capital=10000.0,
        current_timestamp=_CURRENT_TIMESTAMP,
        market_betas=_make_neutral_market_betas(),
        entry_prices=_make_entry_prices(price=10.0),
    )
    long_df = result.filter(pl.col("side") == "LONG").sort("pair_id")
    short_df = result.filter(pl.col("side") == "SHORT").sort("pair_id")
    # Long dollar_amount is balanced to match the short's whole-share-adjusted amount.
    for long_row, short_row in zip(
        long_df.iter_rows(named=True), short_df.iter_rows(named=True), strict=True
    ):
        assert long_row["dollar_amount"] == pytest.approx(short_row["dollar_amount"])


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
        entry_prices=_make_entry_prices(),
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
            entry_prices=_make_entry_prices(count=REQUIRED_PAIRS - 1),
        )


def test_size_pairs_with_volatility_parity_raises_when_partial_zero_qty() -> None:
    # pair 0 has very low volatility → gets ~100% of capital, non-zero qty.
    # pairs 1-9 have very high volatility → get almost nothing, zero qty.
    # After the zero-qty filter, 1 pair remains (0 < 1 < REQUIRED_PAIRS=10);
    # should raise InsufficientPairsError rather than returning a truncated DataFrame.
    # maximum_per_pair_dollar = (100/1.0)*1.0*2.0/10 = 20; price=10 passes pre-filter.
    low_vol = 0.001
    high_vol = 1000.0
    long_vols = [low_vol] + [high_vol] * 9
    short_vols = [low_vol] + [high_vol] * 9
    pairs = _make_candidate_pairs(long_vols=long_vols, short_vols=short_vols)
    with pytest.raises(InsufficientPairsError):
        size_pairs_with_volatility_parity(
            pairs,
            maximum_capital=100.0,
            current_timestamp=_CURRENT_TIMESTAMP,
            market_betas=_make_neutral_market_betas(),
            entry_prices=_make_entry_prices(price=10.0),
        )


def test_size_pairs_with_volatility_parity_raises_when_short_price_too_high() -> None:
    # maximum_per_pair_dollar = (10000/2.03) * 1.0 * 2.0 / 10 ≈ 985.2
    # With price=1001 > 985.2, all pairs are infeasible.
    pairs = _make_candidate_pairs()
    with pytest.raises(InsufficientPairsError):
        size_pairs_with_volatility_parity(
            pairs,
            maximum_capital=10000.0,
            current_timestamp=_CURRENT_TIMESTAMP,
            market_betas=_make_neutral_market_betas(),
            entry_prices=_make_entry_prices(price=1001.0),
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
        entry_prices=_make_entry_prices(),
    )
    validated = portfolio_schema.validate(result)
    assert validated.height == REQUIRED_PAIRS * 2


def test_size_pairs_with_volatility_parity_exposure_scale_halves_dollar_amounts() -> (
    None
):
    pairs = _make_candidate_pairs()
    market_betas = _make_neutral_market_betas()
    entry_prices = _make_entry_prices(price=10.0)
    # maximum_capital=2030 gives per-pair = 2030/2.03/10 = 100.0 exactly at price=10
    # (capital_divisor = 1 + 1.03 = 2.03), so exposure_scale=0.5 halves to 50.0
    # without any whole-share rounding loss.
    maximum_capital = 2030.0

    full_result = size_pairs_with_volatility_parity(
        pairs,
        maximum_capital=maximum_capital,
        current_timestamp=_CURRENT_TIMESTAMP,
        market_betas=market_betas,
        entry_prices=entry_prices,
        exposure_scale=1.0,
    )
    half_result = size_pairs_with_volatility_parity(
        pairs,
        maximum_capital=maximum_capital,
        current_timestamp=_CURRENT_TIMESTAMP,
        market_betas=market_betas,
        entry_prices=entry_prices,
        exposure_scale=0.5,
    )

    # Both long and short legs are whole-share adjusted and balanced to each other;
    # with price=10 and maximum_capital=3030 the halving is exact for both.
    full_long = (
        full_result.filter(pl.col("side") == "LONG")
        .sort("ticker")["dollar_amount"]
        .to_list()
    )
    half_long = (
        half_result.filter(pl.col("side") == "LONG")
        .sort("ticker")["dollar_amount"]
        .to_list()
    )
    for full, half in zip(full_long, half_long, strict=True):
        assert half == pytest.approx(full * 0.5)

    full_short = (
        full_result.filter(pl.col("side") == "SHORT")
        .sort("ticker")["dollar_amount"]
        .to_list()
    )
    half_short = (
        half_result.filter(pl.col("side") == "SHORT")
        .sort("ticker")["dollar_amount"]
        .to_list()
    )
    for full, half in zip(full_short, half_short, strict=True):
        assert half == pytest.approx(full * 0.5)


def test_size_pairs_with_volatility_parity_beta_neutral_reduces_portfolio_beta() -> (
    None
):
    # Pairs 0-7: long_beta=2.0, short_beta=1.0 (net positive contribution)
    # Pairs 8-9: long_beta=1.0, short_beta=2.0 (net negative contribution)
    # Equal vol-parity weights produce portfolio beta != 0; optimizer drives it toward 0
    pairs = _make_candidate_pairs()
    asymmetric_betas = _make_asymmetric_market_betas()
    neutral_betas = _make_neutral_market_betas()
    entry_prices = _make_entry_prices(price=10.0)

    beta_neutral_result = size_pairs_with_volatility_parity(
        pairs,
        maximum_capital=10000.0,
        current_timestamp=_CURRENT_TIMESTAMP,
        market_betas=asymmetric_betas,
        entry_prices=entry_prices,
    )
    vol_parity_result = size_pairs_with_volatility_parity(
        pairs,
        maximum_capital=10000.0,
        current_timestamp=_CURRENT_TIMESTAMP,
        market_betas=neutral_betas,
        entry_prices=entry_prices,
    )

    beta_neutral_beta = abs(
        compute_portfolio_beta(beta_neutral_result, asymmetric_betas)
    )
    vol_parity_beta = abs(compute_portfolio_beta(vol_parity_result, asymmetric_betas))

    assert beta_neutral_beta < vol_parity_beta


def test_size_pairs_with_volatility_parity_output_includes_entry_price() -> None:
    pairs = _make_candidate_pairs()
    result = size_pairs_with_volatility_parity(
        pairs,
        maximum_capital=10000.0,
        current_timestamp=_CURRENT_TIMESTAMP,
        market_betas=_make_neutral_market_betas(),
        entry_prices=_make_entry_prices(price=10.0),
    )
    assert "entry_price" in result.columns
    assert result["entry_price"].is_null().sum() == 0
    for price in result["entry_price"].to_list():
        assert price == pytest.approx(10.0)


def test_size_pairs_with_volatility_parity_output_includes_quantity_and_notional() -> (
    None
):
    pairs = _make_candidate_pairs()
    result = size_pairs_with_volatility_parity(
        pairs,
        maximum_capital=10000.0,
        current_timestamp=_CURRENT_TIMESTAMP,
        market_betas=_make_neutral_market_betas(),
        entry_prices=_make_entry_prices(price=10.0),
    )
    assert "quantity" in result.columns
    assert "notional" in result.columns

    short_rows = result.filter(pl.col("side") == "SHORT")
    assert short_rows["quantity"].is_null().sum() == 0
    assert short_rows["notional"].is_null().sum() == short_rows.height

    long_rows = result.filter(pl.col("side") == "LONG")
    assert long_rows["quantity"].is_null().sum() == long_rows.height
    assert long_rows["notional"].is_null().sum() == 0


def test_size_pairs_with_volatility_parity_filters_pairs_missing_long_entry_price() -> (
    None
):
    # 11 candidate pairs; pair 10's long ticker is absent from entry_prices.
    # The prefilter must remove it so sizing completes cleanly with 10 pairs
    # and no long position carries entry_price=0.
    pairs = _make_candidate_pairs(count=11)
    market_betas = _make_neutral_market_betas(count=11)
    entry_prices = _make_entry_prices(count=10)  # pair 10's long ticker omitted

    result = size_pairs_with_volatility_parity(
        pairs,
        maximum_capital=10000.0,
        current_timestamp=_CURRENT_TIMESTAMP,
        market_betas=market_betas,
        entry_prices=entry_prices,
    )

    long_rows = result.filter(pl.col("side") == "LONG")
    assert long_rows.height == REQUIRED_PAIRS
    for price in long_rows["entry_price"].to_list():
        assert price is not None
        assert price > 0
