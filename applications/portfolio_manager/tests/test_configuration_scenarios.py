"""Integration tests for Configuration class scenarios.

Each test exercises one or more Configuration fields through the full
sizing-then-execution pipeline, verifying that portfolio_manager behaves
correctly for every combination of settings that operators can configure.

The scenarios are grouped as follows:

  * Default configuration values
  * Buying-power threshold boundaries for intraday shorts
  * Custom minimum_short_equity
  * Custom short_buying_power_buffer
  * Full sizing -> execution integration
  * Rebalance cycle: close existing positions and replace with new ones
"""

from datetime import UTC, datetime
from typing import Any
from unittest.mock import MagicMock

import polars as pl
import pytest
from portfolio_manager.alpaca_client import AlpacaAccount, AlpacaClient
from portfolio_manager.configuration import Configuration
from portfolio_manager.enums import TradeSide
from portfolio_manager.risk_management import (
    REQUIRED_PAIRS,
    size_pairs_with_volatility_parity,
)
from portfolio_manager.trade_execution import (
    execute_close_positions,
    execute_open_positions,
)

_TIMESTAMP = datetime(2025, 6, 1, 9, 30, tzinfo=UTC)

# ── shared helpers ────────────────────────────────────────────────────────────


def _make_mock_client(
    buying_power: float = 50_000.0,
    equity: float = 50_000.0,
) -> MagicMock:
    client = MagicMock(spec=AlpacaClient)
    client.get_account.return_value = AlpacaAccount(
        cash_amount=buying_power,
        buying_power=buying_power,
        equity=equity,
    )
    return client


def _long(
    ticker: str = "AAPL",
    dollar_amount: float = 1_000.0,
    entry_price: float = 100.0,
    pair_id: str = "pair-1",
) -> dict[str, Any]:
    return {
        "ticker": ticker,
        "pair_id": pair_id,
        "side": TradeSide.BUY,
        "dollar_amount": dollar_amount,
        "entry_price": entry_price,
        "quantity": None,
        "notional": dollar_amount,
    }


def _short(
    ticker: str = "MSFT",
    dollar_amount: float = 1_000.0,
    entry_price: float = 100.0,
    quantity: int | None = 10,
    pair_id: str = "pair-1",
) -> dict[str, Any]:
    return {
        "ticker": ticker,
        "pair_id": pair_id,
        "side": TradeSide.SELL,
        "dollar_amount": dollar_amount,
        "entry_price": entry_price,
        "quantity": quantity,
        "notional": None,
    }


def _make_candidate_pairs(count: int = REQUIRED_PAIRS, offset: int = 0) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "pair_id": [
                f"TICK{i + offset:02d}A-TICK{i + offset:02d}B" for i in range(count)
            ],
            "long_ticker": [f"TICK{i + offset:02d}A" for i in range(count)],
            "short_ticker": [f"TICK{i + offset:02d}B" for i in range(count)],
            "z_score": [2.5] * count,
            "hedge_ratio": [1.0] * count,
            "signal_strength": [0.1] * count,
            "long_realized_volatility": [0.02] * count,
            "short_realized_volatility": [0.02] * count,
        }
    )


def _make_market_betas(count: int = REQUIRED_PAIRS, offset: int = 0) -> pl.DataFrame:
    tickers = []
    for i in range(count):
        tickers.extend([f"TICK{i + offset:02d}A", f"TICK{i + offset:02d}B"])
    return pl.DataFrame({"ticker": tickers, "market_beta": [1.0] * (count * 2)})


def _make_entry_prices(
    count: int = REQUIRED_PAIRS, price: float = 10.0, offset: int = 0
) -> dict[str, float]:
    prices: dict[str, float] = {}
    for i in range(count):
        prices[f"TICK{i + offset:02d}A"] = price
        prices[f"TICK{i + offset:02d}B"] = price
    return prices


def _sized_to_open_positions(sized: pl.DataFrame) -> list[dict[str, Any]]:
    return [
        {
            "ticker": row["ticker"],
            "pair_id": row["pair_id"],
            "side": TradeSide.BUY if row["side"] == "LONG" else TradeSide.SELL,
            "dollar_amount": row["dollar_amount"],
            "entry_price": row["entry_price"],
            "quantity": row["quantity"],
            "notional": row["notional"],
        }
        for row in sized.iter_rows(named=True)
    ]


# ── 1. default configuration values ──────────────────────────────────────────


def test_default_configuration_values() -> None:
    config = Configuration()

    assert config.minimum_short_equity == pytest.approx(2_000.0)
    assert config.short_buying_power_buffer == pytest.approx(1.03)


# ── 2. intraday buying-power threshold ───────────────────────────────────────
#
# Short cost = qty * price * buffer
#            = 10  * 100  * 1.03    = 1_030.0


def test_intraday_exact_buying_power_cost_executes() -> None:
    config = Configuration(short_buying_power_buffer=1.03)
    short_buying_power_cost = 10 * 100.0 * 1.03  # 1_030.0
    client = _make_mock_client(buying_power=short_buying_power_cost, equity=50_000.0)
    positions = [
        _long(ticker="AAPL", dollar_amount=1_000.0),
        _short(ticker="MSFT", entry_price=100.0, quantity=10),
    ]
    results, count = execute_open_positions(
        client, positions, 50_000.0, 50_000.0, config
    )

    assert count == 2  # noqa: PLR2004
    assert results[1]["status"] == "success"
    assert results[1]["side"] == TradeSide.SELL


def test_intraday_buying_power_one_cent_short_skips() -> None:
    config = Configuration(short_buying_power_buffer=1.03)
    short_buying_power_cost = 10 * 100.0 * 1.03  # 1_030.0
    client = _make_mock_client(
        buying_power=short_buying_power_cost - 0.01, equity=50_000.0
    )
    positions = [
        _long(ticker="AAPL", dollar_amount=1_000.0),
        _short(ticker="MSFT", entry_price=100.0, quantity=10),
    ]
    results, count = execute_open_positions(
        client, positions, 50_000.0, 50_000.0, config
    )

    assert count == 1  # long succeeded
    assert results[0]["status"] == "success"
    assert results[1]["status"] == "skipped"
    assert results[1]["reason"] == "insufficient_buying_power"


# ── 3. custom minimum_short_equity ───────────────────────────────────────────


def test_custom_minimum_short_equity_rejects_below_threshold() -> None:
    config = Configuration(minimum_short_equity=10_000.0)
    client = _make_mock_client(buying_power=50_000.0, equity=50_000.0)
    positions = [_long(ticker="AAPL"), _short(ticker="MSFT")]

    results, count = execute_open_positions(
        client, positions, 50_000.0, 9_999.0, config
    )

    assert count == 1  # long succeeded
    assert results[0]["status"] == "success"
    assert results[1]["status"] == "skipped"
    assert results[1]["reason"] == "insufficient_equity_for_short"


def test_custom_minimum_short_equity_accepts_at_threshold() -> None:
    config = Configuration(minimum_short_equity=10_000.0)
    client = _make_mock_client(buying_power=50_000.0, equity=50_000.0)
    positions = [_long(ticker="AAPL"), _short(ticker="MSFT")]

    results, count = execute_open_positions(
        client, positions, 50_000.0, 10_000.0, config
    )

    assert count == 2  # noqa: PLR2004
    assert results[1]["status"] == "success"


# ── 4. custom short_buying_power_buffer ──────────────────────────────────────
#
# buffer=1.05 cost = 10 * 100 * 1.05 = 1_050.0
# buffer=1.03 cost = 10 * 100 * 1.03 = 1_030.0
# Post-refresh buying_power=1_040 sits between the two costs.


def test_custom_short_buying_power_buffer_higher_buffer_skips_position() -> None:
    config = Configuration(short_buying_power_buffer=1.05)
    distinguishing_buying_power = 1_040.0
    client = _make_mock_client(
        buying_power=distinguishing_buying_power, equity=50_000.0
    )
    positions = [
        _long(ticker="AAPL", dollar_amount=1_000.0),
        _short(ticker="MSFT", entry_price=100.0, quantity=10),
    ]
    results, _count = execute_open_positions(
        client, positions, 50_000.0, 50_000.0, config
    )

    # 1.05 -> cost=1_050 > 1_040 -> short skipped.
    assert results[1]["status"] == "skipped"
    assert results[1]["reason"] == "insufficient_buying_power"


def test_custom_short_buying_power_buffer_lower_buffer_succeeds() -> None:
    config = Configuration(short_buying_power_buffer=1.03)
    distinguishing_buying_power = 1_040.0
    client = _make_mock_client(
        buying_power=distinguishing_buying_power, equity=50_000.0
    )
    positions = [
        _long(ticker="AAPL", dollar_amount=1_000.0),
        _short(ticker="MSFT", entry_price=100.0, quantity=10),
    ]
    results, _count = execute_open_positions(
        client, positions, 50_000.0, 50_000.0, config
    )

    # 1.03 -> cost=1_030 <= 1_040 -> short succeeds.
    assert results[1]["status"] == "success"


# ── 5. full sizing -> execution integration ───────────────────────────────────


def test_integration_sizing_and_execution_opens_all_pairs() -> None:
    config = Configuration(
        short_buying_power_buffer=1.03,
        minimum_short_equity=2_000.0,
    )
    pairs = _make_candidate_pairs()
    market_betas = _make_market_betas()
    entry_prices = _make_entry_prices(price=10.0)
    maximum_capital = 10_000.0

    sized = size_pairs_with_volatility_parity(
        pairs,
        maximum_capital=maximum_capital,
        current_timestamp=_TIMESTAMP,
        market_betas=market_betas,
        entry_prices=entry_prices,
        short_buying_power_buffer=config.short_buying_power_buffer,
    )

    open_positions = _sized_to_open_positions(sized)
    client = _make_mock_client(buying_power=maximum_capital * 10, equity=50_000.0)
    results, _count = execute_open_positions(
        client, open_positions, maximum_capital * 10, 50_000.0, config
    )

    succeeded = [result for result in results if result["status"] == "success"]
    assert len(succeeded) == REQUIRED_PAIRS * 2


# ── 6. rebalance cycle: close existing positions and replace ──────────────────
#
# These tests model what run_rebalance does across two consecutive cycles:
#   1. An initial portfolio is established (pairs opened).
#   2. On the next rebalance some pairs are closed and replaced with new ones.


def test_rebalance_full_replacement_cycle() -> None:
    """All 10 initial pairs are closed; 10 new pairs from fresh tickers are opened."""
    config = Configuration(
        short_buying_power_buffer=1.03,
    )
    maximum_capital = 10_000.0
    client = _make_mock_client(buying_power=maximum_capital * 10, equity=50_000.0)

    # Cycle 1: establish initial portfolio (TICK00-TICK09).
    initial_sized = size_pairs_with_volatility_parity(
        _make_candidate_pairs(offset=0),
        maximum_capital=maximum_capital,
        current_timestamp=_TIMESTAMP,
        market_betas=_make_market_betas(offset=0),
        entry_prices=_make_entry_prices(price=10.0, offset=0),
        short_buying_power_buffer=config.short_buying_power_buffer,
    )
    _cycle1_results, cycle1_count = execute_open_positions(
        client,
        _sized_to_open_positions(initial_sized),
        maximum_capital * 10,
        50_000.0,
        config,
    )
    assert cycle1_count == REQUIRED_PAIRS * 2

    # Cycle 2: close all initial positions, open 10 replacement pairs (TICK10-TICK19).
    close_positions = [
        {"ticker": row["ticker"]} for row in initial_sized.iter_rows(named=True)
    ]
    replacement_sized = size_pairs_with_volatility_parity(
        _make_candidate_pairs(offset=REQUIRED_PAIRS),
        maximum_capital=maximum_capital,
        current_timestamp=_TIMESTAMP,
        market_betas=_make_market_betas(offset=REQUIRED_PAIRS),
        entry_prices=_make_entry_prices(price=10.0, offset=REQUIRED_PAIRS),
        short_buying_power_buffer=config.short_buying_power_buffer,
    )

    _close_results, closed_count = execute_close_positions(client, close_positions)
    refreshed_buying_power = maximum_capital * 10
    _cycle2_results, cycle2_count = execute_open_positions(
        client,
        _sized_to_open_positions(replacement_sized),
        refreshed_buying_power,
        50_000.0,
        config,
    )

    assert closed_count == REQUIRED_PAIRS * 2
    assert cycle2_count == REQUIRED_PAIRS * 2


def test_rebalance_partial_replacement_cycle() -> None:
    """5 of the initial 10 pairs are held; 5 are closed and replaced with 5 new pairs.

    In run_rebalance, held pairs are excluded from both close_positions and
    open_positions.  The 5 new pairs are drawn from a fresh sizing call that
    excludes held tickers from consolidated_signals.  Because sizing requires
    REQUIRED_PAIRS feasible pairs, the new sizing uses a full set of 10
    candidate pairs sourced from tickers not already in the portfolio.
    """
    config = Configuration(
        short_buying_power_buffer=1.03,
    )
    maximum_capital = 10_000.0
    client = _make_mock_client(buying_power=maximum_capital * 10, equity=50_000.0)

    # Cycle 1: establish initial 10 pairs (TICK00-TICK09).
    initial_sized = size_pairs_with_volatility_parity(
        _make_candidate_pairs(offset=0),
        maximum_capital=maximum_capital,
        current_timestamp=_TIMESTAMP,
        market_betas=_make_market_betas(offset=0),
        entry_prices=_make_entry_prices(price=10.0, offset=0),
        short_buying_power_buffer=config.short_buying_power_buffer,
    )
    execute_open_positions(
        client,
        _sized_to_open_positions(initial_sized),
        maximum_capital * 10,
        50_000.0,
        config,
    )

    # Cycle 2: TICK00-TICK04 are held (no action); TICK05-TICK09 are closed.
    # New pairs TICK10-TICK19 replace the 5 closing pairs.
    closing_pairs = REQUIRED_PAIRS // 2
    held_pair_ids = {f"TICK{i:02d}A-TICK{i:02d}B" for i in range(REQUIRED_PAIRS // 2)}
    closing_rows = initial_sized.filter(~pl.col("pair_id").is_in(held_pair_ids))
    close_positions = [
        {"ticker": row["ticker"]} for row in closing_rows.iter_rows(named=True)
    ]
    replacement_sized = size_pairs_with_volatility_parity(
        _make_candidate_pairs(offset=REQUIRED_PAIRS),
        maximum_capital=maximum_capital,
        current_timestamp=_TIMESTAMP,
        market_betas=_make_market_betas(offset=REQUIRED_PAIRS),
        entry_prices=_make_entry_prices(price=10.0, offset=REQUIRED_PAIRS),
        short_buying_power_buffer=config.short_buying_power_buffer,
    )
    replacement_pair_ids = {
        f"TICK{i + REQUIRED_PAIRS:02d}A-TICK{i + REQUIRED_PAIRS:02d}B"
        for i in range(closing_pairs)
    }
    replacement_rows = replacement_sized.filter(
        pl.col("pair_id").is_in(replacement_pair_ids)
    )

    _close_results, closed_count = execute_close_positions(client, close_positions)
    refreshed_buying_power = maximum_capital * 10
    _open_results, opened_count = execute_open_positions(
        client,
        _sized_to_open_positions(replacement_rows),
        refreshed_buying_power,
        50_000.0,
        config,
    )

    assert closed_count == closing_pairs * 2  # 5 pairs * 2 legs = 10 tickers
    assert opened_count == closing_pairs * 2  # 5 replacement pairs opened


def test_rebalance_capital_recycling_closes_enable_new_opens() -> None:
    """Closing short positions releases their margin reserve, restoring buying power
    sufficient to open the next batch of pairs.

    This mirrors the run_rebalance pattern where get_account() is called between
    the close and open phases so that freed margin is accounted for before sizing
    the new open orders.
    """
    config = Configuration(
        short_buying_power_buffer=1.03,
        minimum_short_equity=2_000.0,
    )
    maximum_capital = 10_000.0

    new_sized = size_pairs_with_volatility_parity(
        _make_candidate_pairs(offset=REQUIRED_PAIRS),
        maximum_capital=maximum_capital,
        current_timestamp=_TIMESTAMP,
        market_betas=_make_market_betas(offset=REQUIRED_PAIRS),
        entry_prices=_make_entry_prices(price=10.0, offset=REQUIRED_PAIRS),
        short_buying_power_buffer=config.short_buying_power_buffer,
    )
    new_open_positions = _sized_to_open_positions(new_sized)

    # Demonstrate that opening the new pairs fails when buying power is too low.
    low_buying_power = 1.0
    constrained_client = _make_mock_client(
        buying_power=low_buying_power, equity=50_000.0
    )
    _failed_results, failed_count = execute_open_positions(
        constrained_client, new_open_positions, low_buying_power, 50_000.0, config
    )
    assert failed_count == 0
    skipped = [result for result in _failed_results if result["status"] == "skipped"]
    buying_power_reasons = {"insufficient_buying_power", "long_leg_failed"}
    assert all(result["reason"] in buying_power_reasons for result in skipped)

    # Now simulate the rebalance cycle: close prior pairs, then refresh account.
    close_positions = [
        {"ticker": f"TICK{i:02d}{'A' if leg == 0 else 'B'}"}
        for i in range(REQUIRED_PAIRS)
        for leg in range(2)
    ]
    recycling_client = _make_mock_client(buying_power=low_buying_power, equity=50_000.0)
    _close_results, closed_count = execute_close_positions(
        recycling_client, close_positions
    )
    assert closed_count == REQUIRED_PAIRS * 2

    restored_buying_power = maximum_capital * 10
    recycling_client.get_account.return_value = AlpacaAccount(
        cash_amount=restored_buying_power,
        buying_power=restored_buying_power,
        equity=50_000.0,
    )

    _open_results, opened_count = execute_open_positions(
        recycling_client,
        new_open_positions,
        restored_buying_power,
        50_000.0,
        config,
    )
    assert opened_count == REQUIRED_PAIRS * 2
