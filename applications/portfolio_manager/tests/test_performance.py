import random
from datetime import UTC, datetime

import polars as pl
import pytest
from portfolio_manager.performance import (
    build_closed_pair_record,
    build_performance_snapshot,
    compute_calmar_ratio,
    compute_max_drawdown,
    compute_period_return,
    compute_portfolio_value,
    compute_realized_pnl,
    compute_sharpe_ratio,
    compute_sortino_ratio,
    compute_spy_relative_return,
    compute_win_rate,
)

_EXPECTED_OPEN_PAIR_COUNT = 8
_EXPECTED_HOLDING_DAYS = 2


def _make_positions(
    tickers: list[str],
    sides: list[str],
    dollar_amounts: list[float],
    entry_prices: list[float],
) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "ticker": tickers,
            "side": sides,
            "dollar_amount": dollar_amounts,
            "entry_price": entry_prices,
        }
    )


def _make_current_prices(tickers: list[str], prices: list[float]) -> pl.DataFrame:
    return pl.DataFrame({"ticker": tickers, "close_price": prices})


# --- compute_portfolio_value ---


def test_compute_portfolio_value_long_position_current_price_above_entry() -> None:
    positions = _make_positions(["AAPL"], ["LONG"], [1000.0], [100.0])
    current_prices = _make_current_prices(["AAPL"], [120.0])
    result = compute_portfolio_value(positions, current_prices, cash=0.0)
    assert result == pytest.approx(1200.0)


def test_compute_portfolio_value_short_position_price_above_entry_reduces_value() -> (
    None
):
    positions = _make_positions(["MSFT"], ["SHORT"], [1000.0], [100.0])
    current_prices = _make_current_prices(["MSFT"], [120.0])
    # SHORT: 1000 * (2.0 - 120/100) = 1000 * 0.8 = 800
    result = compute_portfolio_value(positions, current_prices, cash=0.0)
    assert result == pytest.approx(800.0)


def test_compute_portfolio_value_empty_positions_returns_just_cash() -> None:
    positions = pl.DataFrame(
        {"ticker": [], "side": [], "dollar_amount": [], "entry_price": []},
        schema={
            "ticker": pl.String,
            "side": pl.String,
            "dollar_amount": pl.Float64,
            "entry_price": pl.Float64,
        },
    )
    current_prices = _make_current_prices(["AAPL"], [100.0])
    result = compute_portfolio_value(positions, current_prices, cash=500.0)
    assert result == pytest.approx(500.0)


def test_compute_portfolio_value_missing_ticker_in_prices_is_skipped() -> None:
    positions = _make_positions(
        ["AAPL", "MSFT"], ["LONG", "LONG"], [1000.0, 1000.0], [100.0, 200.0]
    )
    # MSFT has no price entry
    current_prices = _make_current_prices(["AAPL"], [100.0])
    result = compute_portfolio_value(positions, current_prices, cash=0.0)
    # Only AAPL counted: 1000 * (100/100) = 1000
    assert result == pytest.approx(1000.0)


def test_compute_portfolio_value_includes_cash() -> None:
    positions = _make_positions(["AAPL"], ["LONG"], [1000.0], [100.0])
    current_prices = _make_current_prices(["AAPL"], [100.0])
    result = compute_portfolio_value(positions, current_prices, cash=250.0)
    assert result == pytest.approx(1250.0)


# --- compute_period_return ---


def test_compute_period_return_returns_correct_fraction() -> None:
    result = compute_period_return(current_value=1100.0, previous_value=1000.0)
    assert result == pytest.approx(0.1)


def test_compute_period_return_returns_zero_when_previous_value_is_zero() -> None:
    result = compute_period_return(current_value=500.0, previous_value=0.0)
    assert result == 0.0


def test_compute_period_return_negative_return() -> None:
    result = compute_period_return(current_value=900.0, previous_value=1000.0)
    assert result == pytest.approx(-0.1)


# --- compute_realized_pnl ---


def test_compute_realized_pnl_long_profit_price_went_up() -> None:
    closing_pair = _make_positions(["AAPL"], ["LONG"], [1000.0], [100.0])
    current_prices = _make_current_prices(["AAPL"], [110.0])
    total_pnl, return_pct = compute_realized_pnl(closing_pair, current_prices)
    # pnl = 1000 * (110/100 - 1) = 100
    assert total_pnl == pytest.approx(100.0)
    assert return_pct == pytest.approx(0.1)


def test_compute_realized_pnl_short_profit_price_went_down() -> None:
    closing_pair = _make_positions(["MSFT"], ["SHORT"], [1000.0], [100.0])
    current_prices = _make_current_prices(["MSFT"], [90.0])
    total_pnl, return_pct = compute_realized_pnl(closing_pair, current_prices)
    # pnl = 1000 * (1 - 90/100) = 100
    assert total_pnl == pytest.approx(100.0)
    assert return_pct == pytest.approx(0.1)


def test_compute_realized_pnl_mixed_pair() -> None:
    closing_pair = _make_positions(
        ["AAPL", "MSFT"],
        ["LONG", "SHORT"],
        [1000.0, 1000.0],
        [100.0, 100.0],
    )
    current_prices = _make_current_prices(["AAPL", "MSFT"], [110.0, 110.0])
    total_pnl, return_pct = compute_realized_pnl(closing_pair, current_prices)
    # AAPL long: 1000 * (110/100 - 1) = 100
    # MSFT short: 1000 * (1 - 110/100) = -100
    assert total_pnl == pytest.approx(0.0)
    assert return_pct == pytest.approx(0.0)


def test_compute_realized_pnl_empty_closing_pair() -> None:
    closing_pair = pl.DataFrame(
        {"ticker": [], "side": [], "dollar_amount": [], "entry_price": []},
        schema={
            "ticker": pl.String,
            "side": pl.String,
            "dollar_amount": pl.Float64,
            "entry_price": pl.Float64,
        },
    )
    current_prices = _make_current_prices(["AAPL"], [100.0])
    total_pnl, return_pct = compute_realized_pnl(closing_pair, current_prices)
    assert total_pnl == 0.0
    assert return_pct == 0.0


# --- compute_sharpe_ratio ---


def test_compute_sharpe_ratio_returns_none_for_fewer_than_20_returns() -> None:
    returns = [0.01] * 19
    assert compute_sharpe_ratio(returns) is None


def test_compute_sharpe_ratio_returns_none_for_20_identical_returns() -> None:
    returns = [0.01] * 20
    result = compute_sharpe_ratio(returns)
    # Std of identical values is 0, so returns None
    assert result is None


def test_compute_sharpe_ratio_returns_float_for_varied_returns_with_positive_mean() -> (
    None
):
    random.seed(42)
    returns = [0.01 + random.gauss(0, 0.001) for _ in range(30)]
    result = compute_sharpe_ratio(returns)
    assert result is not None
    assert isinstance(result, float)


def test_compute_sharpe_ratio_returns_none_when_all_returns_are_identical() -> None:
    returns = [0.005] * 25
    assert compute_sharpe_ratio(returns) is None


# --- compute_sortino_ratio ---


def test_compute_sortino_ratio_returns_none_for_fewer_than_20_returns() -> None:
    returns = [0.01, -0.01] * 9  # 18 returns
    assert compute_sortino_ratio(returns) is None


def test_compute_sortino_ratio_returns_float_when_there_are_downside_returns() -> None:
    returns = [0.01, -0.005] * 15  # 30 returns with mixed sign
    result = compute_sortino_ratio(returns)
    assert result is not None
    assert isinstance(result, float)


def test_compute_sortino_ratio_returns_none_when_no_downside_returns() -> None:
    returns = [0.01] * 25
    assert compute_sortino_ratio(returns) is None


# --- compute_max_drawdown ---


def test_compute_max_drawdown_returns_none_for_fewer_than_2_values() -> None:
    assert compute_max_drawdown([100.0]) is None
    assert compute_max_drawdown([]) is None


def test_compute_max_drawdown_computes_known_drawdown() -> None:
    # Peak = 100, then drops to 80 → drawdown = (100-80)/100 = 0.20
    result = compute_max_drawdown([100.0, 80.0])
    assert result == pytest.approx(0.20)


def test_compute_max_drawdown_returns_zero_for_monotonically_increasing_values() -> (
    None
):
    result = compute_max_drawdown([100.0, 110.0, 120.0, 130.0])
    assert result == pytest.approx(0.0)


def test_compute_max_drawdown_finds_maximum_among_multiple_drawdowns() -> None:
    # First drawdown: 100 → 90 = 10%
    # Second drawdown: 110 → 80 = ~27.3%
    result = compute_max_drawdown([100.0, 90.0, 110.0, 80.0])
    expected = (110.0 - 80.0) / 110.0
    assert result == pytest.approx(expected)


# --- compute_calmar_ratio ---


def test_compute_calmar_ratio_returns_none_when_max_drawdown_is_zero() -> None:
    assert compute_calmar_ratio(annual_return=0.15, maximum_drawdown=0.0) is None


def test_compute_calmar_ratio_returns_correct_value() -> None:
    result = compute_calmar_ratio(annual_return=0.15, maximum_drawdown=0.05)
    assert result == pytest.approx(3.0)


# --- compute_win_rate ---


def test_compute_win_rate_returns_none_for_empty_list() -> None:
    assert compute_win_rate([]) is None


def test_compute_win_rate_correct_fraction_for_mixed_wins_and_losses() -> None:
    pnl_list = [100.0, -50.0, 200.0, -30.0]
    result = compute_win_rate(pnl_list)
    assert result == pytest.approx(0.5)


def test_compute_win_rate_all_wins() -> None:
    pnl_list = [10.0, 20.0, 30.0]
    assert compute_win_rate(pnl_list) == pytest.approx(1.0)


def test_compute_win_rate_no_wins() -> None:
    pnl_list = [-10.0, -20.0]
    assert compute_win_rate(pnl_list) == pytest.approx(0.0)


# --- compute_spy_relative_return ---


def test_compute_spy_relative_return_correct_subtraction() -> None:
    result = compute_spy_relative_return(portfolio_return=0.15, spy_return=0.10)
    assert result == pytest.approx(0.05)


def test_compute_spy_relative_return_negative_when_underperforming() -> None:
    result = compute_spy_relative_return(portfolio_return=0.05, spy_return=0.10)
    assert result == pytest.approx(-0.05)


# --- build_performance_snapshot ---


def test_build_performance_snapshot_returns_dict_with_all_required_keys() -> None:
    timestamp = datetime(2025, 6, 15, 12, 0, 0, tzinfo=UTC)
    result = build_performance_snapshot(
        portfolio_value=100000.0,
        cash=5000.0,
        spy_close=550.0,
        period_return=0.02,
        open_pair_count=8,
        timestamp=timestamp,
    )
    assert "timestamp" in result
    assert "portfolio_value" in result
    assert "cash_balance" in result
    assert "spy_close" in result
    assert "period_return_pct" in result
    assert "open_pair_count" in result


def test_build_performance_snapshot_timestamp_converted_to_milliseconds() -> None:
    timestamp = datetime(2025, 6, 15, 12, 0, 0, tzinfo=UTC)
    result = build_performance_snapshot(
        portfolio_value=100000.0,
        cash=5000.0,
        spy_close=550.0,
        period_return=0.02,
        open_pair_count=8,
        timestamp=timestamp,
    )
    expected_ms = int(timestamp.timestamp() * 1000)
    assert result["timestamp"] == expected_ms


def test_build_performance_snapshot_correct_values() -> None:
    timestamp = datetime(2025, 6, 15, 12, 0, 0, tzinfo=UTC)
    result = build_performance_snapshot(
        portfolio_value=100000.0,
        cash=5000.0,
        spy_close=550.0,
        period_return=0.02,
        open_pair_count=_EXPECTED_OPEN_PAIR_COUNT,
        timestamp=timestamp,
    )
    assert result["portfolio_value"] == pytest.approx(100000.0)
    assert result["cash_balance"] == pytest.approx(5000.0)
    assert result["spy_close"] == pytest.approx(550.0)
    assert result["period_return_pct"] == pytest.approx(0.02)
    assert result["open_pair_count"] == _EXPECTED_OPEN_PAIR_COUNT


# --- build_closed_pair_record ---


def test_build_closed_pair_record_returns_dict_with_all_required_keys() -> None:
    result = build_closed_pair_record(
        pair_id="AAPL-MSFT",
        long_ticker="AAPL",
        short_ticker="MSFT",
        entry_timestamp=1735689600000,
        closed_timestamp=1735776000000,
        dollar_amount=2000.0,
        realized_pnl=100.0,
        return_pct=0.05,
    )
    assert "closed_timestamp" in result
    assert "pair_id" in result
    assert "long_ticker" in result
    assert "short_ticker" in result
    assert "entry_timestamp" in result
    assert "dollar_amount" in result
    assert "realized_pnl" in result
    assert "return_pct" in result
    assert "holding_days" in result


def test_build_closed_pair_record_holding_days_computed_correctly() -> None:
    # 1 day = 86400000 ms
    entry_timestamp = 1735689600000
    closed_timestamp = entry_timestamp + (2 * 86400000)  # 2 days later
    result = build_closed_pair_record(
        pair_id="AAPL-MSFT",
        long_ticker="AAPL",
        short_ticker="MSFT",
        entry_timestamp=entry_timestamp,
        closed_timestamp=closed_timestamp,
        dollar_amount=2000.0,
        realized_pnl=100.0,
        return_pct=0.05,
    )
    assert result["holding_days"] == _EXPECTED_HOLDING_DAYS


def test_build_closed_pair_record_correct_values() -> None:
    result = build_closed_pair_record(
        pair_id="AAPL-MSFT",
        long_ticker="AAPL",
        short_ticker="MSFT",
        entry_timestamp=1735689600000,
        closed_timestamp=1735776000000,
        dollar_amount=2000.0,
        realized_pnl=100.0,
        return_pct=0.05,
    )
    assert result["pair_id"] == "AAPL-MSFT"
    assert result["long_ticker"] == "AAPL"
    assert result["short_ticker"] == "MSFT"
    assert result["dollar_amount"] == pytest.approx(2000.0)
    assert result["realized_pnl"] == pytest.approx(100.0)
    assert result["return_pct"] == pytest.approx(0.05)
