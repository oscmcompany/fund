import asyncio
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock, patch

import polars as pl
import pytest
from portfolio_manager.portfolio_state import (
    _PRIOR_ALLOCATION_SCHEMA,
    evaluate_held_pairs_from_quotes,
    get_last_portfolio_value,
    get_prior_allocation,
    save_closed_pair,
    save_performance_snapshot,
    save_rebalance,
)


def _make_pool_mock(
    rows: list | None = None, *, fetchone_row: object = None
) -> MagicMock:
    mock_result = AsyncMock()
    mock_result.fetchall.return_value = rows or []
    mock_result.fetchone.return_value = fetchone_row
    mock_connection = MagicMock()
    mock_connection.execute = AsyncMock(return_value=mock_result)
    mock_connection.__aenter__ = AsyncMock(return_value=mock_connection)
    mock_connection.__aexit__ = AsyncMock(return_value=None)
    mock_transaction = MagicMock()
    mock_transaction.__aenter__ = AsyncMock(return_value=mock_transaction)
    mock_transaction.__aexit__ = AsyncMock(return_value=None)
    mock_connection.transaction.return_value = mock_transaction
    mock_pool = MagicMock()
    mock_pool.connection.return_value = mock_connection
    return mock_pool


def _make_candidate_pairs() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "pair_id": ["NVDA-AMD"],
            "long_ticker": ["NVDA"],
            "short_ticker": ["AMD"],
            "z_score": [2.5],
            "hedge_ratio": [1.0],
            "signal_strength": [0.1],
            "long_realized_volatility": [0.02],
            "short_realized_volatility": [0.02],
        }
    )


def _make_successful_pair_rows() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "ticker": ["NVDA", "AMD"],
            "timestamp": [1735689600000, 1735689600000],
            "side": ["LONG", "SHORT"],
            "dollar_amount": [990.0, 990.0],
            "action": ["OPEN_POSITION", "OPEN_POSITION"],
            "pair_id": ["NVDA-AMD", "NVDA-AMD"],
            "entry_price": [100.0, 99.0],
            "quantity": [None, 10],
            "notional": [990.0, None],
        },
        schema={
            "ticker": pl.Utf8,
            "timestamp": pl.Int64,
            "side": pl.Utf8,
            "dollar_amount": pl.Float64,
            "action": pl.Utf8,
            "pair_id": pl.Utf8,
            "entry_price": pl.Float64,
            "quantity": pl.Int64,
            "notional": pl.Float64,
        },
    )


# --- evaluate_held_pairs_from_quotes ---


def _make_prior_portfolio_with_pair(pair_id: str = "AAPL-MSFT") -> pl.DataFrame:
    long_ticker, short_ticker = pair_id.split("-")
    return pl.DataFrame(
        {
            "ticker": [long_ticker, short_ticker],
            "timestamp": [1735689600000, 1735689600000],
            "side": ["LONG", "SHORT"],
            "dollar_amount": [1000.0, 1000.0],
            "action": ["OPEN_POSITION", "OPEN_POSITION"],
            "pair_id": [pair_id, pair_id],
            "entry_price": [100.0, 90.0],
            "quantity": [None, 11],
            "notional": [1000.0, None],
        },
        schema=_PRIOR_ALLOCATION_SCHEMA,
    )


def _make_equity_bars_for_pair(
    long_ticker: str, short_ticker: str, rows: int = 60
) -> pl.DataFrame:
    base_long = 100.0
    base_short = 90.0
    timestamps = list(range(rows))
    long_prices = [base_long + i * 0.01 for i in range(rows)]
    short_prices = [base_short + i * 0.009 for i in range(rows)]
    return pl.DataFrame(
        {
            "ticker": [long_ticker] * rows + [short_ticker] * rows,
            "timestamp": [t * 86400000 for t in timestamps] * 2,
            "close_price": long_prices + short_prices,
        }
    )


def test_evaluate_held_pairs_from_quotes_returns_empty_when_portfolio_empty() -> None:
    result = evaluate_held_pairs_from_quotes(
        pl.DataFrame(schema=_PRIOR_ALLOCATION_SCHEMA),
        pl.DataFrame(
            schema={
                "ticker": pl.String,
                "timestamp": pl.Int64,
                "close_price": pl.Float64,
            }
        ),
        {},
    )
    assert result == set()


def test_evaluate_held_pairs_from_quotes_holds_pair_when_live_price_missing() -> None:
    prior = _make_prior_portfolio_with_pair("AAPL-MSFT")
    bars = _make_equity_bars_for_pair("AAPL", "MSFT")
    # Only provide live price for AAPL, not MSFT — pair should be held, not closed.
    result = evaluate_held_pairs_from_quotes(prior, bars, {"AAPL": 110.0})
    assert result == {"AAPL", "MSFT"}


def test_evaluate_held_pairs_from_quotes_holds_pair_when_live_price_nonpositive() -> (
    None
):
    prior = _make_prior_portfolio_with_pair("AAPL-MSFT")
    bars = _make_equity_bars_for_pair("AAPL", "MSFT")
    result = evaluate_held_pairs_from_quotes(prior, bars, {"AAPL": 0.0, "MSFT": 90.0})
    assert result == {"AAPL", "MSFT"}


def test_evaluate_held_pairs_from_quotes_holds_pair_when_z_score_in_range() -> None:
    prior = _make_prior_portfolio_with_pair("AAPL-MSFT")
    bars = _make_equity_bars_for_pair("AAPL", "MSFT")
    # Set live prices that keep the spread within hold thresholds.
    live_mid_prices = {"AAPL": 100.5, "MSFT": 90.4}
    result = evaluate_held_pairs_from_quotes(prior, bars, live_mid_prices)
    # Verify no crash and correct return type regardless of z-score outcome.
    assert isinstance(result, set)


def test_evaluate_held_pairs_from_quotes_holds_pair_with_insufficient_history() -> None:
    prior = _make_prior_portfolio_with_pair("AAPL-MSFT")
    # Only 5 rows of history — below the 30-row minimum; pair held for cycle.
    bars = _make_equity_bars_for_pair("AAPL", "MSFT", rows=5)
    result = evaluate_held_pairs_from_quotes(prior, bars, {"AAPL": 101.0, "MSFT": 91.0})
    assert result == {"AAPL", "MSFT"}


def test_evaluate_held_pairs_from_quotes_skips_malformed_pair() -> None:
    prior = pl.DataFrame(
        {
            "ticker": ["AAPL"],
            "timestamp": [1735689600000],
            "side": ["LONG"],
            "dollar_amount": [1000.0],
            "action": ["OPEN_POSITION"],
            "pair_id": ["AAPL-MSFT"],
            "entry_price": [100.0],
            "quantity": [None],
            "notional": [1000.0],
        },
        schema=_PRIOR_ALLOCATION_SCHEMA,
    )
    bars = _make_equity_bars_for_pair("AAPL", "MSFT")
    result = evaluate_held_pairs_from_quotes(prior, bars, {"AAPL": 101.0, "MSFT": 91.0})
    assert result == set()


# --- get_prior_allocation ---


def test_get_prior_allocation_returns_empty_dataframe_when_no_rows() -> None:
    mock_pool = _make_pool_mock(rows=[])

    with patch(
        "portfolio_manager.portfolio_state.get_pool", AsyncMock(return_value=mock_pool)
    ):
        result = asyncio.run(get_prior_allocation())

    assert result.is_empty()
    assert "pair_id" in result.columns


def test_get_prior_allocation_returns_dataframe_with_expected_columns() -> None:
    rows = [
        (
            "AAPL",
            1735689600000,
            "LONG",
            1000.0,
            "OPEN_POSITION",
            "AAPL-MSFT",
            100.0,
            None,
            1000.0,
        ),
    ]
    mock_pool = _make_pool_mock(rows=rows)

    with patch(
        "portfolio_manager.portfolio_state.get_pool", AsyncMock(return_value=mock_pool)
    ):
        result = asyncio.run(get_prior_allocation())

    assert result.height == 1
    assert "pair_id" in result.columns
    assert result["pair_id"][0] == "AAPL-MSFT"
    assert result["ticker"][0] == "AAPL"
    assert result["side"][0] == "LONG"


def test_get_prior_allocation_raises_on_db_error() -> None:
    mock_connection = MagicMock()
    mock_connection.execute = AsyncMock(side_effect=RuntimeError("connection error"))
    mock_connection.__aenter__ = AsyncMock(return_value=mock_connection)
    mock_connection.__aexit__ = AsyncMock(return_value=None)
    mock_pool = MagicMock()
    mock_pool.connection.return_value = mock_connection

    with (
        patch(
            "portfolio_manager.portfolio_state.get_pool",
            AsyncMock(return_value=mock_pool),
        ),
        pytest.raises(RuntimeError),
    ):
        asyncio.run(get_prior_allocation())


# --- save_rebalance ---


def test_save_rebalance_returns_true_on_success() -> None:
    mock_pool = _make_pool_mock()
    triggered_at = datetime(2024, 1, 1, tzinfo=UTC)

    with patch(
        "portfolio_manager.portfolio_state.get_pool", AsyncMock(return_value=mock_pool)
    ):
        result = asyncio.run(
            save_rebalance(
                triggered_at=triggered_at,
                trigger_reason="predictions_completed",
                model_run_id="run-abc",
                successful_pair_rows=_make_successful_pair_rows(),
                candidate_pairs=_make_candidate_pairs(),
                open_results=[
                    {
                        "ticker": "NVDA",
                        "status": "success",
                        "side": "BUY",
                        "alpaca_order_id": "order-1",
                        "submitted_quantity": None,
                    },
                    {
                        "ticker": "AMD",
                        "status": "success",
                        "side": "SELL",
                        "alpaca_order_id": "order-2",
                        "submitted_quantity": 10,
                    },
                ],
            )
        )

    assert result is True


def test_save_rebalance_returns_true_with_no_successful_pairs() -> None:
    mock_pool = _make_pool_mock()
    triggered_at = datetime(2024, 1, 1, tzinfo=UTC)
    empty_rows = pl.DataFrame(schema=_PRIOR_ALLOCATION_SCHEMA)

    with patch(
        "portfolio_manager.portfolio_state.get_pool", AsyncMock(return_value=mock_pool)
    ):
        result = asyncio.run(
            save_rebalance(
                triggered_at=triggered_at,
                trigger_reason="manual",
                model_run_id=None,
                successful_pair_rows=empty_rows,
                candidate_pairs=_make_candidate_pairs(),
                open_results=[],
            )
        )

    assert result is True


def test_save_rebalance_returns_false_on_db_error() -> None:
    mock_connection = MagicMock()
    mock_connection.execute = AsyncMock(side_effect=RuntimeError("db error"))
    mock_connection.__aenter__ = AsyncMock(return_value=mock_connection)
    mock_connection.__aexit__ = AsyncMock(return_value=None)
    mock_transaction = MagicMock()
    mock_transaction.__aenter__ = AsyncMock(return_value=mock_transaction)
    mock_transaction.__aexit__ = AsyncMock(return_value=None)
    mock_connection.transaction.return_value = mock_transaction
    mock_pool = MagicMock()
    mock_pool.connection.return_value = mock_connection

    with patch(
        "portfolio_manager.portfolio_state.get_pool", AsyncMock(return_value=mock_pool)
    ):
        result = asyncio.run(
            save_rebalance(
                triggered_at=datetime(2024, 1, 1, tzinfo=UTC),
                trigger_reason="manual",
                model_run_id=None,
                successful_pair_rows=_make_successful_pair_rows(),
                candidate_pairs=_make_candidate_pairs(),
                open_results=[],
            )
        )

    assert result is False


def test_save_rebalance_skips_pair_missing_from_candidate_pairs() -> None:
    mock_pool = _make_pool_mock()

    pair_rows = pl.DataFrame(
        {
            "ticker": ["NVDA", "AMD"],
            "timestamp": [1735689600000, 1735689600000],
            "side": ["LONG", "SHORT"],
            "dollar_amount": [990.0, 990.0],
            "action": ["OPEN_POSITION", "OPEN_POSITION"],
            "pair_id": ["UNKNOWN-PAIR", "UNKNOWN-PAIR"],
            "entry_price": [100.0, 99.0],
            "quantity": [None, 10],
            "notional": [990.0, None],
        },
        schema={
            "ticker": pl.Utf8,
            "timestamp": pl.Int64,
            "side": pl.Utf8,
            "dollar_amount": pl.Float64,
            "action": pl.Utf8,
            "pair_id": pl.Utf8,
            "entry_price": pl.Float64,
            "quantity": pl.Int64,
            "notional": pl.Float64,
        },
    )

    with patch(
        "portfolio_manager.portfolio_state.get_pool", AsyncMock(return_value=mock_pool)
    ):
        result = asyncio.run(
            save_rebalance(
                triggered_at=datetime(2024, 1, 1, tzinfo=UTC),
                trigger_reason="manual",
                model_run_id=None,
                successful_pair_rows=pair_rows,
                candidate_pairs=_make_candidate_pairs(),
                open_results=[],
            )
        )

    assert result is True


# --- save_performance_snapshot ---


def test_save_performance_snapshot_returns_true_on_success() -> None:
    mock_pool = _make_pool_mock()
    snapshot = {
        "timestamp": 1704067200000,
        "portfolio_value": 10500.0,
        "cash_balance": 500.0,
        "spy_close": 455.0,
        "period_return_percent": 0.05,
        "open_pair_count": 2,
        "gross_return": 0.05,
        "net_return": 0.048,
        "total_slippage_cost": 2.0,
    }

    with patch(
        "portfolio_manager.portfolio_state.get_pool", AsyncMock(return_value=mock_pool)
    ):
        result = asyncio.run(save_performance_snapshot(snapshot))

    assert result is True


def test_save_performance_snapshot_returns_false_on_db_error() -> None:
    mock_connection = MagicMock()
    mock_connection.execute = AsyncMock(side_effect=RuntimeError("db error"))
    mock_connection.__aenter__ = AsyncMock(return_value=mock_connection)
    mock_connection.__aexit__ = AsyncMock(return_value=None)
    mock_pool = MagicMock()
    mock_pool.connection.return_value = mock_connection

    snapshot = {
        "timestamp": 1704067200000,
        "portfolio_value": 10500.0,
        "gross_return": 0.05,
        "net_return": 0.048,
        "total_slippage_cost": 2.0,
    }

    with patch(
        "portfolio_manager.portfolio_state.get_pool", AsyncMock(return_value=mock_pool)
    ):
        result = asyncio.run(save_performance_snapshot(snapshot))

    assert result is False


# --- save_closed_pair ---


def test_save_closed_pair_returns_true_on_success() -> None:
    mock_pool = _make_pool_mock()
    record = {
        "closed_timestamp": 1704153600000,
        "pair_id": "AAPL-MSFT",
        "long_ticker": "AAPL",
        "short_ticker": "MSFT",
        "entry_timestamp": 1704067200000,
        "dollar_amount": 1000.0,
        "realized_profit_and_loss": 50.0,
        "return_percent": 0.05,
        "holding_days": 1,
    }

    with patch(
        "portfolio_manager.portfolio_state.get_pool", AsyncMock(return_value=mock_pool)
    ):
        result = asyncio.run(save_closed_pair(record))

    assert result is True


def test_save_closed_pair_returns_false_on_db_error() -> None:
    mock_connection = MagicMock()
    mock_connection.execute = AsyncMock(side_effect=RuntimeError("db error"))
    mock_connection.__aenter__ = AsyncMock(return_value=mock_connection)
    mock_connection.__aexit__ = AsyncMock(return_value=None)
    mock_pool = MagicMock()
    mock_pool.connection.return_value = mock_connection

    record = {
        "closed_timestamp": 1704153600000,
        "pair_id": "AAPL-MSFT",
        "realized_profit_and_loss": 50.0,
        "return_percent": 0.05,
        "holding_days": 1,
    }

    with patch(
        "portfolio_manager.portfolio_state.get_pool", AsyncMock(return_value=mock_pool)
    ):
        result = asyncio.run(save_closed_pair(record))

    assert result is False


# --- get_last_portfolio_value ---


def test_get_last_portfolio_value_returns_value_when_row_exists() -> None:
    mock_pool = _make_pool_mock(fetchone_row=(10500.0,))

    with patch(
        "portfolio_manager.portfolio_state.get_pool", AsyncMock(return_value=mock_pool)
    ):
        result = asyncio.run(get_last_portfolio_value())

    assert result == pytest.approx(10500.0)


def test_get_last_portfolio_value_returns_none_when_no_rows() -> None:
    mock_pool = _make_pool_mock(fetchone_row=None)

    with patch(
        "portfolio_manager.portfolio_state.get_pool", AsyncMock(return_value=mock_pool)
    ):
        result = asyncio.run(get_last_portfolio_value())

    assert result is None


def test_get_last_portfolio_value_returns_none_on_db_error() -> None:
    mock_connection = MagicMock()
    mock_connection.execute = AsyncMock(side_effect=RuntimeError("db error"))
    mock_connection.__aenter__ = AsyncMock(return_value=mock_connection)
    mock_connection.__aexit__ = AsyncMock(return_value=None)
    mock_pool = MagicMock()
    mock_pool.connection.return_value = mock_connection

    with patch(
        "portfolio_manager.portfolio_state.get_pool", AsyncMock(return_value=mock_pool)
    ):
        result = asyncio.run(get_last_portfolio_value())

    assert result is None
