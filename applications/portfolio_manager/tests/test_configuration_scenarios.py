"""Integration tests for Configuration class scenarios.

Each test exercises one or more Configuration fields through the full
sizing-then-execution pipeline, verifying that portfolio_manager behaves
correctly for every combination of settings that operators can configure.

The scenarios are grouped as follows:

  * Default configuration values
  * Buying-power threshold boundaries for overnight standard-rate shorts
  * Buying-power threshold boundaries for overnight low-price-rate shorts
  * Buying-power threshold boundaries for intraday shorts (no margin)
  * Intraday costs less buying power than overnight for the same position
  * low_price_threshold boundary (price at / below threshold)
  * Custom minimum_short_equity
  * Custom short_buying_power_buffer
  * PDT-constrained overnight mode vs future intraday rebalancing
  * Full sizing -> execution integration (overnight and intraday)
  * Rebalance cycle: close existing positions and replace with new ones
"""

from datetime import UTC, datetime
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
) -> dict:
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
) -> dict:
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


def _sized_to_open_positions(sized: pl.DataFrame) -> list[dict]:
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

    assert config.hold_overnight is True
    assert config.minimum_short_equity == pytest.approx(2_000.0)
    assert config.short_buying_power_buffer == pytest.approx(1.03)
    assert config.overnight_margin_rate_standard == pytest.approx(0.30)
    assert config.overnight_margin_rate_low_price == pytest.approx(1.00)
    assert config.low_price_threshold == pytest.approx(5.00)


# ── 2. overnight standard-rate buying-power threshold ────────────────────────
#
# Short cost = qty * price * (buffer + standard_rate)
#            = 10  * 100  * (1.03  + 0.30         ) = 1_330.0
# Entry price (100) >= low_price_threshold (5) -> standard rate applies.
# The check in _try_open_single_position is strict (>), so cost == remaining succeeds.


def test_overnight_standard_rate_exact_buying_power_cost_executes() -> None:
    config = Configuration(
        hold_overnight=True,
        short_buying_power_buffer=1.03,
        overnight_margin_rate_standard=0.30,
        low_price_threshold=5.0,
    )
    short_buying_power_cost = 10 * 100.0 * (1.03 + 0.30)  # 1_330.0
    # After long opens, get_account() returns buying_power equal to the exact short
    # cost; the strict > check means cost == remaining is not a skip.
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


def test_overnight_standard_rate_buying_power_one_cent_short_skips() -> None:
    config = Configuration(
        hold_overnight=True,
        short_buying_power_buffer=1.03,
        overnight_margin_rate_standard=0.30,
        low_price_threshold=5.0,
    )
    short_buying_power_cost = 10 * 100.0 * (1.03 + 0.30)  # 1_330.0
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


# ── 3. overnight low-price-rate buying-power threshold ───────────────────────
#
# Short cost = qty * price * (buffer + low_price_rate)
#            = 10  * 3.0  * (1.03  + 1.00           ) = 60.9
# Entry price (3.0) < low_price_threshold (5.0) -> low-price rate applies.


def test_overnight_low_price_rate_exact_buying_power_cost_executes() -> None:
    config = Configuration(
        hold_overnight=True,
        short_buying_power_buffer=1.03,
        overnight_margin_rate_low_price=1.00,
        low_price_threshold=5.0,
    )
    short_buying_power_cost = 10 * 3.0 * (1.03 + 1.00)  # 60.9
    client = _make_mock_client(buying_power=short_buying_power_cost, equity=50_000.0)
    positions = [
        _long(ticker="AAPL", dollar_amount=30.0),
        _short(ticker="MSFT", entry_price=3.0, dollar_amount=30.0, quantity=10),
    ]
    results, count = execute_open_positions(
        client, positions, 50_000.0, 50_000.0, config
    )

    assert count == 2  # noqa: PLR2004
    assert results[1]["status"] == "success"
    assert results[1]["side"] == TradeSide.SELL


def test_overnight_low_price_rate_buying_power_one_cent_short_skips() -> None:
    config = Configuration(
        hold_overnight=True,
        short_buying_power_buffer=1.03,
        overnight_margin_rate_low_price=1.00,
        low_price_threshold=5.0,
    )
    short_buying_power_cost = 10 * 3.0 * (1.03 + 1.00)  # 60.9
    client = _make_mock_client(
        buying_power=short_buying_power_cost - 0.01, equity=50_000.0
    )
    positions = [
        _long(ticker="AAPL", dollar_amount=30.0),
        _short(ticker="MSFT", entry_price=3.0, dollar_amount=30.0, quantity=10),
    ]
    results, count = execute_open_positions(
        client, positions, 50_000.0, 50_000.0, config
    )

    assert count == 1  # long succeeded
    assert results[0]["status"] == "success"
    assert results[1]["status"] == "skipped"
    assert results[1]["reason"] == "insufficient_buying_power"


# ── 4. intraday buying-power threshold (no margin reserve) ───────────────────
#
# Short cost = qty * price * buffer  (hold_overnight=False -> no margin term)
#            = 10  * 100  * 1.03    = 1_030.0


def test_intraday_exact_buying_power_cost_executes() -> None:
    config = Configuration(hold_overnight=False, short_buying_power_buffer=1.03)
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
    config = Configuration(hold_overnight=False, short_buying_power_buffer=1.03)
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


# ── 5. intraday costs less buying power than overnight ───────────────────────
#
# Overnight cost: 10 * 100 * (1.03 + 0.30) = 1_330.0 -> skips at 1_100
# Intraday cost:  10 * 100 *  1.03         = 1_030.0 -> succeeds at 1_100


def test_intraday_has_lower_short_buying_power_cost_than_overnight() -> None:
    buying_power_between_costs = 1_100.0

    overnight_config = Configuration(
        hold_overnight=True,
        short_buying_power_buffer=1.03,
        overnight_margin_rate_standard=0.30,
        low_price_threshold=5.0,
    )
    intraday_config = Configuration(
        hold_overnight=False,
        short_buying_power_buffer=1.03,
    )
    positions = [
        _long(ticker="AAPL", dollar_amount=1_000.0),
        _short(ticker="MSFT", entry_price=100.0, quantity=10),
    ]

    overnight_client = _make_mock_client(
        buying_power=buying_power_between_costs, equity=50_000.0
    )
    overnight_results, overnight_count = execute_open_positions(
        overnight_client, positions, 50_000.0, 50_000.0, overnight_config
    )

    intraday_client = _make_mock_client(
        buying_power=buying_power_between_costs, equity=50_000.0
    )
    intraday_results, intraday_count = execute_open_positions(
        intraday_client, positions, 50_000.0, 50_000.0, intraday_config
    )

    assert overnight_results[1]["status"] == "skipped"
    assert overnight_results[1]["reason"] == "insufficient_buying_power"
    assert intraday_results[1]["status"] == "success"
    assert intraday_count > overnight_count


# ── 6. low_price_threshold boundary ──────────────────────────────────────────
#
# The condition in trade_execution is `entry_price < low_price_threshold` (strict).
# At price == threshold the standard rate applies; just below, the low-price rate.
#
# Standard rate cost  (price=5.0, qty=10): 10 * 5.0 * (1.03 + 0.30) =  66.5
# Low-price rate cost (price=5.0, qty=10): 10 * 5.0 * (1.03 + 1.00) = 101.5
# Post-refresh buying_power=70 sits between the two, distinguishing the branches.


def test_low_price_threshold_price_at_threshold_uses_standard_rate() -> None:
    config = Configuration(
        hold_overnight=True,
        short_buying_power_buffer=1.03,
        overnight_margin_rate_standard=0.30,
        overnight_margin_rate_low_price=1.00,
        low_price_threshold=5.0,
    )
    distinguishing_buying_power = 70.0  # > 66.5 (standard) but < 101.5 (low-price)
    client = _make_mock_client(
        buying_power=distinguishing_buying_power, equity=50_000.0
    )
    positions = [
        _long(ticker="AAPL", dollar_amount=50.0),
        _short(ticker="MSFT", entry_price=5.0, dollar_amount=50.0, quantity=10),
    ]
    results, _count = execute_open_positions(
        client, positions, 50_000.0, 50_000.0, config
    )

    # Standard rate (0.30) -> cost=66.5 <= 70 -> short succeeds.
    assert results[1]["status"] == "success"


def test_low_price_threshold_price_just_below_threshold_uses_low_price_rate() -> None:
    config = Configuration(
        hold_overnight=True,
        short_buying_power_buffer=1.03,
        overnight_margin_rate_standard=0.30,
        overnight_margin_rate_low_price=1.00,
        low_price_threshold=5.0,
    )
    distinguishing_buying_power = 70.0
    client = _make_mock_client(
        buying_power=distinguishing_buying_power, equity=50_000.0
    )
    positions = [
        _long(ticker="AAPL", dollar_amount=49.9),
        # entry_price=4.99 < threshold=5.0 -> low-price rate (1.00)
        # cost = 10 * 4.99 * (1.03 + 1.00) ~= 101.3
        _short(ticker="MSFT", entry_price=4.99, dollar_amount=49.9, quantity=10),
    ]
    results, _count = execute_open_positions(
        client, positions, 50_000.0, 50_000.0, config
    )

    # Low-price rate (1.00) -> cost~=101.3 > 70 -> short skipped.
    assert results[1]["status"] == "skipped"
    assert results[1]["reason"] == "insufficient_buying_power"


# ── 7. custom minimum_short_equity ───────────────────────────────────────────


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


# ── 8. custom short_buying_power_buffer ──────────────────────────────────────
#
# buffer=1.05 intraday cost = 10 * 100 * 1.05 = 1_050.0
# buffer=1.03 intraday cost = 10 * 100 * 1.03 = 1_030.0
# Post-refresh buying_power=1_040 sits between the two costs.


def test_custom_short_buying_power_buffer_higher_buffer_skips_position() -> None:
    config = Configuration(hold_overnight=False, short_buying_power_buffer=1.05)
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
    config = Configuration(hold_overnight=False, short_buying_power_buffer=1.03)
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


# ── 9. PDT overnight mode vs future intraday rebalancing ─────────────────────
#
# Under PDT (Pattern Day Trader) rules, accounts under $25k must hold positions
# overnight; intraday rebalancing is restricted. hold_overnight=True models this
# constraint. When intraday rebalancing becomes available, hold_overnight=False
# omits the overnight maintenance margin from the capital divisor:
#   overnight:  divisor = 1 + buffer + max(standard, low_price) = 3.03
#   intraday:   divisor = 1 + buffer                            = 2.03
# The same maximum_capital therefore produces larger per-pair allocations intraday.


def test_pdt_overnight_config_reserves_more_capital_than_intraday() -> None:
    pairs = _make_candidate_pairs()
    market_betas = _make_market_betas()
    entry_prices = _make_entry_prices(price=10.0)
    maximum_capital = 10_000.0

    overnight_result = size_pairs_with_volatility_parity(
        pairs,
        maximum_capital=maximum_capital,
        current_timestamp=_TIMESTAMP,
        market_betas=market_betas,
        entry_prices=entry_prices,
        hold_overnight=True,
        overnight_margin_rate_standard=0.30,
        overnight_margin_rate_low_price=1.00,
        short_buying_power_buffer=1.03,
    )
    intraday_result = size_pairs_with_volatility_parity(
        pairs,
        maximum_capital=maximum_capital,
        current_timestamp=_TIMESTAMP,
        market_betas=market_betas,
        entry_prices=entry_prices,
        hold_overnight=False,
        short_buying_power_buffer=1.03,
    )

    overnight_total = overnight_result["dollar_amount"].sum()
    intraday_total = intraday_result["dollar_amount"].sum()
    assert overnight_total is not None
    assert intraday_total is not None
    assert float(intraday_total) > float(overnight_total)


# ── 10. full sizing -> execution integration ──────────────────────────────────


def test_integration_overnight_sizing_and_execution_opens_all_pairs() -> None:
    config = Configuration(
        hold_overnight=True,
        short_buying_power_buffer=1.03,
        overnight_margin_rate_standard=0.30,
        overnight_margin_rate_low_price=1.00,
        low_price_threshold=5.0,
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
        hold_overnight=config.hold_overnight,
        overnight_margin_rate_standard=config.overnight_margin_rate_standard,
        overnight_margin_rate_low_price=config.overnight_margin_rate_low_price,
        short_buying_power_buffer=config.short_buying_power_buffer,
    )

    open_positions = _sized_to_open_positions(sized)
    # Generous buying power so the mock refresh never blocks any position.
    client = _make_mock_client(buying_power=maximum_capital * 10, equity=50_000.0)
    results, _count = execute_open_positions(
        client, open_positions, maximum_capital * 10, 50_000.0, config
    )

    succeeded = [result for result in results if result["status"] == "success"]
    assert len(succeeded) == REQUIRED_PAIRS * 2


def test_integration_intraday_sizing_and_execution_opens_all_pairs() -> None:
    # Models the expected behaviour when PDT restrictions are lifted and
    # intraday rebalancing becomes available (hold_overnight=False).
    config = Configuration(
        hold_overnight=False,
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
        hold_overnight=config.hold_overnight,
        short_buying_power_buffer=config.short_buying_power_buffer,
    )

    open_positions = _sized_to_open_positions(sized)
    client = _make_mock_client(buying_power=maximum_capital * 10, equity=50_000.0)
    results, _count = execute_open_positions(
        client, open_positions, maximum_capital * 10, 50_000.0, config
    )

    succeeded = [result for result in results if result["status"] == "success"]
    assert len(succeeded) == REQUIRED_PAIRS * 2


def test_integration_intraday_sizing_produces_larger_positions_than_overnight() -> None:
    pairs = _make_candidate_pairs()
    market_betas = _make_market_betas()
    entry_prices = _make_entry_prices(price=10.0)
    maximum_capital = 10_000.0

    overnight_sized = size_pairs_with_volatility_parity(
        pairs,
        maximum_capital=maximum_capital,
        current_timestamp=_TIMESTAMP,
        market_betas=market_betas,
        entry_prices=entry_prices,
        hold_overnight=True,
        overnight_margin_rate_standard=0.30,
        overnight_margin_rate_low_price=1.00,
        short_buying_power_buffer=1.03,
    )
    intraday_sized = size_pairs_with_volatility_parity(
        pairs,
        maximum_capital=maximum_capital,
        current_timestamp=_TIMESTAMP,
        market_betas=market_betas,
        entry_prices=entry_prices,
        hold_overnight=False,
        short_buying_power_buffer=1.03,
    )

    overnight_total = overnight_sized.filter(pl.col("side") == "SHORT")[
        "dollar_amount"
    ].sum()
    intraday_total = intraday_sized.filter(pl.col("side") == "SHORT")[
        "dollar_amount"
    ].sum()
    assert overnight_total is not None
    assert intraday_total is not None
    assert float(intraday_total) > float(overnight_total)


# ── 11. rebalance cycle: close existing positions and replace ─────────────────
#
# These tests model what run_rebalance does across two consecutive cycles:
#   1. An initial portfolio is established (pairs opened).
#   2. On the next rebalance some pairs are closed and replaced with new ones.
#
# The mock client's get_account() return value is updated between the close and
# open phases to reflect Alpaca's behaviour of restoring buying power once short
# positions are covered (margin released).


def test_rebalance_full_replacement_cycle() -> None:
    """All 10 initial pairs are closed; 10 new pairs from fresh tickers are opened."""
    config = Configuration(
        hold_overnight=True,
        short_buying_power_buffer=1.03,
        overnight_margin_rate_standard=0.30,
        overnight_margin_rate_low_price=1.00,
        low_price_threshold=5.0,
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
        hold_overnight=config.hold_overnight,
        overnight_margin_rate_standard=config.overnight_margin_rate_standard,
        overnight_margin_rate_low_price=config.overnight_margin_rate_low_price,
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
        hold_overnight=config.hold_overnight,
        overnight_margin_rate_standard=config.overnight_margin_rate_standard,
        overnight_margin_rate_low_price=config.overnight_margin_rate_low_price,
        short_buying_power_buffer=config.short_buying_power_buffer,
    )

    _close_results, closed_count = execute_close_positions(client, close_positions)
    # Simulate account refresh between close and open phases (mirrors run_rebalance).
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
        hold_overnight=True,
        short_buying_power_buffer=1.03,
        overnight_margin_rate_standard=0.30,
        overnight_margin_rate_low_price=1.00,
        low_price_threshold=5.0,
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
        hold_overnight=config.hold_overnight,
        overnight_margin_rate_standard=config.overnight_margin_rate_standard,
        overnight_margin_rate_low_price=config.overnight_margin_rate_low_price,
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
    held_pair_ids = {f"TICK{i:02d}A-TICK{i:02d}B" for i in range(5)}
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
        hold_overnight=config.hold_overnight,
        overnight_margin_rate_standard=config.overnight_margin_rate_standard,
        overnight_margin_rate_low_price=config.overnight_margin_rate_low_price,
        short_buying_power_buffer=config.short_buying_power_buffer,
    )

    _close_results, closed_count = execute_close_positions(client, close_positions)
    refreshed_buying_power = maximum_capital * 10
    _open_results, opened_count = execute_open_positions(
        client,
        _sized_to_open_positions(replacement_sized),
        refreshed_buying_power,
        50_000.0,
        config,
    )

    closing_pairs = REQUIRED_PAIRS // 2
    assert closed_count == closing_pairs * 2  # 5 pairs * 2 legs = 10 tickers
    assert opened_count == REQUIRED_PAIRS * 2  # 10 new pairs fully opened


def test_rebalance_capital_recycling_closes_enable_new_opens() -> None:
    """Closing short positions releases their margin reserve, restoring buying power
    sufficient to open the next batch of pairs.

    This mirrors the run_rebalance pattern where get_account() is called between
    the close and open phases so that freed margin is accounted for before sizing
    the new open orders.
    """
    config = Configuration(
        hold_overnight=True,
        short_buying_power_buffer=1.03,
        overnight_margin_rate_standard=0.30,
        overnight_margin_rate_low_price=1.00,
        low_price_threshold=5.0,
        minimum_short_equity=2_000.0,
    )
    maximum_capital = 10_000.0

    # Size the new replacement pairs before either phase runs.
    new_sized = size_pairs_with_volatility_parity(
        _make_candidate_pairs(offset=REQUIRED_PAIRS),
        maximum_capital=maximum_capital,
        current_timestamp=_TIMESTAMP,
        market_betas=_make_market_betas(offset=REQUIRED_PAIRS),
        entry_prices=_make_entry_prices(price=10.0, offset=REQUIRED_PAIRS),
        hold_overnight=config.hold_overnight,
        overnight_margin_rate_standard=config.overnight_margin_rate_standard,
        overnight_margin_rate_low_price=config.overnight_margin_rate_low_price,
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

    # After closes Alpaca releases the margin reserve; simulate the account refresh
    # that run_rebalance performs between the close and open phases.
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
