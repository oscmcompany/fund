import asyncio
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock, patch

import polars as pl
import pytest
from fastapi import status
from portfolio_manager.portfolio_state import (
    _PRIOR_ALLOCATION_SCHEMA,
    evaluate_prior_pairs,
)
from portfolio_manager.rebalance import (
    _prune_pairs_with_invalid_entry_price,
    _record_performance,
    get_latest_predictions_correlation_id,
    get_optimal_portfolio,
    get_raw_predictions,
    run_rebalance,
)
from portfolio_manager.trade_execution import get_positions


def _make_prior_portfolio(pairs: list[dict]) -> pl.DataFrame:
    rows = []
    for pair in pairs:
        rows.append(
            {
                "ticker": pair["long_ticker"],
                "timestamp": 1735689600000,
                "side": "LONG",
                "dollar_amount": 1000.0,
                "action": "OPEN_POSITION",
                "pair_id": pair["pair_id"],
                "entry_price": 100.0,
            }
        )
        rows.append(
            {
                "ticker": pair["short_ticker"],
                "timestamp": 1735689600000,
                "side": "SHORT",
                "dollar_amount": 1000.0,
                "action": "OPEN_POSITION",
                "pair_id": pair["pair_id"],
                "entry_price": 100.0,
            }
        )
    return pl.DataFrame(rows, schema=_PRIOR_ALLOCATION_SCHEMA)


def _make_historical_prices(tickers: list[str], n_rows: int = 65) -> pl.DataFrame:
    rows = []
    for ticker in tickers:
        rows.extend(
            {
                "ticker": ticker,
                "timestamp": float(i),
                "close_price": 100.0 + (i * 0.1),
            }
            for i in range(n_rows)
        )
    return pl.DataFrame(rows)


def _make_optimal_portfolio() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "ticker": ["NVDA", "AMD"],
            "timestamp": [1735689600000, 1735689600000],
            "side": ["LONG", "SHORT"],
            "dollar_amount": [
                990.0,
                990.0,
            ],  # long matched to short's whole-share amount
            "action": ["OPEN_POSITION", "OPEN_POSITION"],
            "pair_id": ["NVDA-AMD", "NVDA-AMD"],
            "entry_price": [100.0, 99.0],
            # quantity: null for LONG, whole-share count for SHORT (int(990/99)=10)
            "quantity": [None, 10],
            # notional: dollar amount for LONG, null for SHORT
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


# --- evaluate_prior_pairs ---


def test_evaluate_prior_pairs_returns_empty_set_for_empty_portfolio() -> None:
    empty_portfolio = pl.DataFrame(schema=_PRIOR_ALLOCATION_SCHEMA)
    historical_prices = _make_historical_prices(["AAPL", "MSFT"])
    result = evaluate_prior_pairs(empty_portfolio, historical_prices)
    assert result == set()


def test_evaluate_prior_pairs_holds_pair_in_intermediate_zone() -> None:
    prior = _make_prior_portfolio(
        [{"pair_id": "AAPL-MSFT", "long_ticker": "AAPL", "short_ticker": "MSFT"}]
    )
    historical_prices = _make_historical_prices(["AAPL", "MSFT"])
    with patch(
        "portfolio_manager.portfolio_state.compute_spread_zscore",
        return_value=(2.0, 1.0),
    ):
        result = evaluate_prior_pairs(prior, historical_prices)
    assert "AAPL" in result
    assert "MSFT" in result


def test_evaluate_prior_pairs_holds_pair_at_lower_bound_of_hold_zone() -> None:
    prior = _make_prior_portfolio(
        [{"pair_id": "AAPL-MSFT", "long_ticker": "AAPL", "short_ticker": "MSFT"}]
    )
    historical_prices = _make_historical_prices(["AAPL", "MSFT"])
    with patch(
        "portfolio_manager.portfolio_state.compute_spread_zscore",
        return_value=(0.5, 1.0),
    ):
        result = evaluate_prior_pairs(prior, historical_prices)
    assert "AAPL" in result
    assert "MSFT" in result


def test_evaluate_prior_pairs_does_not_hold_converged_pair() -> None:
    prior = _make_prior_portfolio(
        [{"pair_id": "AAPL-MSFT", "long_ticker": "AAPL", "short_ticker": "MSFT"}]
    )
    historical_prices = _make_historical_prices(["AAPL", "MSFT"])
    with patch(
        "portfolio_manager.portfolio_state.compute_spread_zscore",
        return_value=(0.2, 1.0),
    ):
        result = evaluate_prior_pairs(prior, historical_prices)
    assert "AAPL" not in result
    assert "MSFT" not in result


def test_evaluate_prior_pairs_does_not_hold_stop_loss_pair() -> None:
    prior = _make_prior_portfolio(
        [{"pair_id": "AAPL-MSFT", "long_ticker": "AAPL", "short_ticker": "MSFT"}]
    )
    historical_prices = _make_historical_prices(["AAPL", "MSFT"])
    with patch(
        "portfolio_manager.portfolio_state.compute_spread_zscore",
        return_value=(5.0, 1.0),
    ):
        result = evaluate_prior_pairs(prior, historical_prices)
    assert "AAPL" not in result
    assert "MSFT" not in result


def test_evaluate_prior_pairs_does_not_hold_pair_at_stop_loss_boundary() -> None:
    prior = _make_prior_portfolio(
        [{"pair_id": "AAPL-MSFT", "long_ticker": "AAPL", "short_ticker": "MSFT"}]
    )
    historical_prices = _make_historical_prices(["AAPL", "MSFT"])
    with patch(
        "portfolio_manager.portfolio_state.compute_spread_zscore",
        return_value=(4.0, 1.0),
    ):
        result = evaluate_prior_pairs(prior, historical_prices)
    assert "AAPL" not in result
    assert "MSFT" not in result


def test_evaluate_prior_pairs_handles_negative_z_score_in_hold_zone() -> None:
    prior = _make_prior_portfolio(
        [{"pair_id": "AAPL-MSFT", "long_ticker": "AAPL", "short_ticker": "MSFT"}]
    )
    historical_prices = _make_historical_prices(["AAPL", "MSFT"])
    with patch(
        "portfolio_manager.portfolio_state.compute_spread_zscore",
        return_value=(-2.0, 1.0),
    ):
        result = evaluate_prior_pairs(prior, historical_prices)
    assert "AAPL" in result
    assert "MSFT" in result


def test_evaluate_prior_pairs_skips_malformed_pair_missing_long_leg() -> None:
    prior = pl.DataFrame(
        {
            "ticker": ["MSFT"],
            "timestamp": [1735689600000],
            "side": ["SHORT"],
            "dollar_amount": [1000.0],
            "action": ["OPEN_POSITION"],
            "pair_id": ["AAPL-MSFT"],
            "entry_price": [100.0],
            "quantity": [None],
            "notional": [None],
        },
        schema=_PRIOR_ALLOCATION_SCHEMA,
    )
    historical_prices = _make_historical_prices(["AAPL", "MSFT"])
    result = evaluate_prior_pairs(prior, historical_prices)
    assert result == set()


def test_evaluate_prior_pairs_skips_pair_with_insufficient_price_history() -> None:
    prior = _make_prior_portfolio(
        [{"pair_id": "AAPL-MSFT", "long_ticker": "AAPL", "short_ticker": "MSFT"}]
    )
    historical_prices = pl.DataFrame(
        {
            "ticker": ["AAPL", "MSFT"],
            "timestamp": [1.0, 1.0],
            "close_price": [100.0, 100.0],
        }
    )
    result = evaluate_prior_pairs(prior, historical_prices)
    assert result == set()


def test_evaluate_prior_pairs_skips_pair_missing_from_price_data() -> None:
    prior = _make_prior_portfolio(
        [{"pair_id": "AAPL-MSFT", "long_ticker": "AAPL", "short_ticker": "MSFT"}]
    )
    historical_prices = _make_historical_prices(["AAPL"])  # MSFT missing
    result = evaluate_prior_pairs(prior, historical_prices)
    assert result == set()


def test_evaluate_prior_pairs_skips_pair_with_non_positive_prices() -> None:
    prior = _make_prior_portfolio(
        [{"pair_id": "AAPL-MSFT", "long_ticker": "AAPL", "short_ticker": "MSFT"}]
    )
    n_rows = 65
    last_row = n_rows - 1
    rows = []
    for i in range(n_rows):
        rows.append(
            {
                "ticker": "AAPL",
                "timestamp": float(i),
                "close_price": 0.0 if i == last_row else 100.0,
            }
        )
        rows.append({"ticker": "MSFT", "timestamp": float(i), "close_price": 100.0})
    historical_prices = pl.DataFrame(rows)
    result = evaluate_prior_pairs(prior, historical_prices)
    assert result == set()


def test_evaluate_prior_pairs_skips_pair_with_nan_z_score() -> None:
    prior = _make_prior_portfolio(
        [{"pair_id": "AAPL-MSFT", "long_ticker": "AAPL", "short_ticker": "MSFT"}]
    )
    historical_prices = _make_historical_prices(["AAPL", "MSFT"])
    with patch(
        "portfolio_manager.portfolio_state.compute_spread_zscore",
        return_value=(float("nan"), 1.0),
    ):
        result = evaluate_prior_pairs(prior, historical_prices)
    assert result == set()


def test_evaluate_prior_pairs_holds_multiple_pairs_independently() -> None:
    prior = _make_prior_portfolio(
        [
            {"pair_id": "AAPL-MSFT", "long_ticker": "AAPL", "short_ticker": "MSFT"},
            {"pair_id": "GOOGL-AMZN", "long_ticker": "GOOGL", "short_ticker": "AMZN"},
        ]
    )
    historical_prices = _make_historical_prices(["AAPL", "MSFT", "GOOGL", "AMZN"])

    # pair_ids are sorted: "AAPL-MSFT" < "GOOGL-AMZN"
    # first call → AAPL-MSFT (z=2.0, held), second → GOOGL-AMZN (z=0.2, closed)
    with patch(
        "portfolio_manager.portfolio_state.compute_spread_zscore",
        side_effect=[(2.0, 1.0), (0.2, 1.0)],
    ):
        result = evaluate_prior_pairs(prior, historical_prices)

    assert result == {"AAPL", "MSFT"}


# --- pair-level entry price filtering ---


def test_pair_filtering_drops_both_legs_when_one_leg_has_no_entry_price() -> None:
    portfolio = pl.DataFrame(
        {
            "ticker": ["AAPL", "MSFT", "NVDA", "AMD"],
            "pair_id": ["AAPL-MSFT", "AAPL-MSFT", "NVDA-AMD", "NVDA-AMD"],
            "side": ["LONG", "SHORT", "LONG", "SHORT"],
            "dollar_amount": [1000.0, 1000.0, 1000.0, 1000.0],
            "entry_price": [100.0, None, 200.0, 150.0],
        }
    )
    filtered = _prune_pairs_with_invalid_entry_price(portfolio)
    # AAPL-MSFT pair dropped entirely; only NVDA-AMD survives
    assert filtered.height == 2  # noqa: PLR2004
    assert "AAPL" not in filtered["ticker"].to_list()
    assert "MSFT" not in filtered["ticker"].to_list()
    assert "NVDA" in filtered["ticker"].to_list()
    assert "AMD" in filtered["ticker"].to_list()


def test_pair_filtering_retains_all_rows_when_no_prices_are_missing() -> None:
    portfolio = pl.DataFrame(
        {
            "ticker": ["AAPL", "MSFT"],
            "pair_id": ["AAPL-MSFT", "AAPL-MSFT"],
            "side": ["LONG", "SHORT"],
            "dollar_amount": [1000.0, 1000.0],
            "entry_price": [100.0, 200.0],
        }
    )
    filtered = _prune_pairs_with_invalid_entry_price(portfolio)
    assert filtered.height == 2  # noqa: PLR2004


def test_pair_filtering_drops_both_legs_when_one_leg_has_nonpositive_entry_price() -> (
    None
):
    portfolio = pl.DataFrame(
        {
            "ticker": ["AAPL", "MSFT", "NVDA", "AMD"],
            "pair_id": ["AAPL-MSFT", "AAPL-MSFT", "NVDA-AMD", "NVDA-AMD"],
            "side": ["LONG", "SHORT", "LONG", "SHORT"],
            "dollar_amount": [1000.0, 1000.0, 1000.0, 1000.0],
            "entry_price": [100.0, 0.0, 200.0, 150.0],
        }
    )
    filtered = _prune_pairs_with_invalid_entry_price(portfolio)
    # AAPL-MSFT pair dropped entirely due to zero entry_price; only NVDA-AMD survives
    assert filtered.height == 2  # noqa: PLR2004
    assert "AAPL" not in filtered["ticker"].to_list()
    assert "MSFT" not in filtered["ticker"].to_list()
    assert "NVDA" in filtered["ticker"].to_list()
    assert "AMD" in filtered["ticker"].to_list()


# --- get_positions ---


def test_get_positions_excludes_held_tickers_from_close_list() -> None:
    prior_tickers = ["AAPL", "MSFT", "GOOGL"]
    held_tickers = {"AAPL", "MSFT"}
    optimal = _make_optimal_portfolio()
    _, close_positions = get_positions(prior_tickers, held_tickers, optimal)
    close_ticker_list = [p["ticker"] for p in close_positions]
    assert "AAPL" not in close_ticker_list
    assert "MSFT" not in close_ticker_list
    assert "GOOGL" in close_ticker_list


def test_get_positions_includes_all_non_held_prior_tickers_in_close_list() -> None:
    prior_tickers = ["AAPL", "MSFT", "GOOGL", "AMZN"]
    held_tickers = {"AAPL"}
    optimal = _make_optimal_portfolio()
    _, close_positions = get_positions(prior_tickers, held_tickers, optimal)
    close_ticker_list = [p["ticker"] for p in close_positions]
    assert "MSFT" in close_ticker_list
    assert "GOOGL" in close_ticker_list
    assert "AMZN" in close_ticker_list
    assert len(close_positions) == 3  # noqa: PLR2004


def test_get_positions_closes_all_prior_tickers_when_held_set_is_empty() -> None:
    prior_tickers = ["AAPL", "MSFT"]
    held_tickers: set[str] = set()
    optimal = _make_optimal_portfolio()
    _, close_positions = get_positions(prior_tickers, held_tickers, optimal)
    close_ticker_list = [p["ticker"] for p in close_positions]
    assert "AAPL" in close_ticker_list
    assert "MSFT" in close_ticker_list
    assert len(close_positions) == 2  # noqa: PLR2004


def test_get_positions_returns_correct_open_positions() -> None:
    prior_tickers: list[str] = []
    held_tickers: set[str] = set()
    optimal = _make_optimal_portfolio()
    open_positions, _ = get_positions(prior_tickers, held_tickers, optimal)
    assert len(open_positions) == 2  # noqa: PLR2004
    tickers = [p["ticker"] for p in open_positions]
    assert "NVDA" in tickers
    assert "AMD" in tickers
    # entry_price, quantity, and notional must flow through for order submission
    for position in open_positions:
        assert "entry_price" in position
        assert position["entry_price"] is not None
    nvda = next(p for p in open_positions if p["ticker"] == "NVDA")
    amd = next(p for p in open_positions if p["ticker"] == "AMD")
    assert nvda["quantity"] is None
    assert nvda["notional"] == pytest.approx(990.0)
    assert amd["quantity"] == 10  # noqa: PLR2004
    assert amd["notional"] is None


@pytest.mark.parametrize(
    ("held_tickers", "expected_close_count"),
    [
        (set(), 3),
        ({"AAPL"}, 2),
        ({"AAPL", "MSFT"}, 1),
        ({"AAPL", "MSFT", "GOOGL"}, 0),
    ],
)
def test_get_positions_close_count_matches_non_held_prior_tickers(
    held_tickers: set[str],
    expected_close_count: int,
) -> None:
    prior_tickers = ["AAPL", "MSFT", "GOOGL"]
    optimal = _make_optimal_portfolio()
    _, close_positions = get_positions(prior_tickers, held_tickers, optimal)
    assert len(close_positions) == expected_close_count


# --- run_rebalance: account refresh after close ---


@patch(
    "portfolio_manager.rebalance.execute_open_positions",
    return_value=([], 0),
)
@patch("portfolio_manager.rebalance._record_performance", new_callable=AsyncMock)
@patch(
    "portfolio_manager.rebalance.save_rebalance",
    new_callable=AsyncMock,
    return_value=True,
)
@patch("portfolio_manager.rebalance.get_optimal_portfolio")
@patch(
    "portfolio_manager.rebalance.classify_regime",
    return_value={"state": "mean_reversion", "confidence": 0.8},
)
@patch(
    "portfolio_manager.rebalance.compute_market_betas",
    return_value=pl.DataFrame({"ticker": ["NVDA", "AMD"], "market_beta": [1.0, 1.0]}),
)
@patch("portfolio_manager.rebalance.pairs_schema")
@patch(
    "portfolio_manager.rebalance.select_pairs",
    return_value=pl.DataFrame(
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
    ),
)
@patch("portfolio_manager.rebalance.get_prior_allocation", new_callable=AsyncMock)
@patch(
    "portfolio_manager.rebalance.consolidate_predictions",
    return_value=pl.DataFrame({"ticker": ["NVDA", "AMD"]}),
)
@patch(
    "portfolio_manager.rebalance.get_raw_predictions",
    new_callable=AsyncMock,
    return_value=(pl.DataFrame({"ticker": ["NVDA"]}), None),
)
@patch(
    "portfolio_manager.rebalance.fetch_spy_prices",
    new_callable=AsyncMock,
    return_value=pl.DataFrame(),
)
@patch(
    "portfolio_manager.rebalance.fetch_equity_details",
    new_callable=AsyncMock,
    return_value=pl.DataFrame(),
)
@patch(
    "portfolio_manager.rebalance.fetch_historical_prices",
    new_callable=AsyncMock,
    return_value=pl.DataFrame(
        schema={"ticker": pl.Utf8, "timestamp": pl.Float64, "close_price": pl.Float64}
    ),
)
def test_run_rebalance_refreshes_account_after_closing_positions(
    _mock_hist: MagicMock,  # noqa: PT019
    _mock_equity: MagicMock,  # noqa: PT019
    _mock_spy: MagicMock,  # noqa: PT019
    _mock_predictions: AsyncMock,  # noqa: PT019
    _mock_consolidate: MagicMock,  # noqa: PT019
    mock_prior_portfolio: AsyncMock,
    _mock_select: MagicMock,  # noqa: PT019
    mock_pairs_schema: MagicMock,
    _mock_betas: MagicMock,  # noqa: PT019
    _mock_regime: MagicMock,  # noqa: PT019
    mock_optimal_portfolio: MagicMock,
    _mock_save: AsyncMock,  # noqa: PT019
    _mock_record: AsyncMock,  # noqa: PT019
    _mock_execute_open: MagicMock,  # noqa: PT019
) -> None:
    optimal = _make_optimal_portfolio()
    mock_optimal_portfolio.return_value = optimal
    mock_pairs_schema.validate.side_effect = lambda df: df
    mock_prior_portfolio.return_value = pl.DataFrame(schema=_PRIOR_ALLOCATION_SCHEMA)

    mock_account = MagicMock()
    mock_account.cash_amount = 10000.0
    mock_account.buying_power = 10000.0
    mock_account.equity = 50000.0

    mock_client = MagicMock()
    mock_client.get_account.return_value = mock_account
    mock_client.get_shortable_tickers.return_value = ["NVDA", "AMD"]

    asyncio.run(run_rebalance(mock_client))

    # get_account is called exactly twice: at startup and after close positions.
    # execute_open_positions is frozen so its get_account calls don't inflate the count.
    assert mock_client.get_account.call_count == 2  # noqa: PLR2004


@patch("portfolio_manager.rebalance._record_performance", new_callable=AsyncMock)
@patch(
    "portfolio_manager.rebalance.save_rebalance",
    new_callable=AsyncMock,
    return_value=True,
)
@patch("portfolio_manager.rebalance.get_optimal_portfolio")
@patch(
    "portfolio_manager.rebalance.classify_regime",
    return_value={"state": "mean_reversion", "confidence": 0.8},
)
@patch(
    "portfolio_manager.rebalance.compute_market_betas",
    return_value=pl.DataFrame({"ticker": ["NVDA", "AMD"], "market_beta": [1.0, 1.0]}),
)
@patch("portfolio_manager.rebalance.pairs_schema")
@patch(
    "portfolio_manager.rebalance.select_pairs",
    return_value=pl.DataFrame(
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
    ),
)
@patch("portfolio_manager.rebalance.get_prior_allocation", new_callable=AsyncMock)
@patch(
    "portfolio_manager.rebalance.consolidate_predictions",
    return_value=pl.DataFrame({"ticker": ["NVDA", "AMD"]}),
)
@patch(
    "portfolio_manager.rebalance.get_raw_predictions",
    new_callable=AsyncMock,
    return_value=(pl.DataFrame({"ticker": ["NVDA"]}), None),
)
@patch(
    "portfolio_manager.rebalance.fetch_spy_prices",
    new_callable=AsyncMock,
    return_value=pl.DataFrame(),
)
@patch(
    "portfolio_manager.rebalance.fetch_equity_details",
    new_callable=AsyncMock,
    return_value=pl.DataFrame(),
)
@patch(
    "portfolio_manager.rebalance.fetch_historical_prices",
    new_callable=AsyncMock,
    return_value=pl.DataFrame(
        schema={"ticker": pl.Utf8, "timestamp": pl.Float64, "close_price": pl.Float64}
    ),
)
def test_run_rebalance_returns_500_when_account_refresh_fails(
    _mock_hist: MagicMock,  # noqa: PT019
    _mock_equity: MagicMock,  # noqa: PT019
    _mock_spy: MagicMock,  # noqa: PT019
    _mock_predictions: AsyncMock,  # noqa: PT019
    _mock_consolidate: MagicMock,  # noqa: PT019
    mock_prior_portfolio: AsyncMock,
    _mock_select: MagicMock,  # noqa: PT019
    mock_pairs_schema: MagicMock,
    _mock_betas: MagicMock,  # noqa: PT019
    _mock_regime: MagicMock,  # noqa: PT019
    mock_optimal_portfolio: MagicMock,
    _mock_save: AsyncMock,  # noqa: PT019
    _mock_record: AsyncMock,  # noqa: PT019
) -> None:
    optimal = _make_optimal_portfolio()
    mock_optimal_portfolio.return_value = optimal
    mock_pairs_schema.validate.side_effect = lambda df: df
    mock_prior_portfolio.return_value = pl.DataFrame(schema=_PRIOR_ALLOCATION_SCHEMA)

    mock_account = MagicMock()
    mock_account.cash_amount = 10000.0
    mock_account.buying_power = 10000.0
    mock_account.equity = 50000.0

    mock_client = MagicMock()
    # First call (startup) succeeds; second call (post-close refresh) raises.
    mock_client.get_account.side_effect = [mock_account, RuntimeError("network error")]
    mock_client.get_shortable_tickers.return_value = ["NVDA", "AMD"]

    response = asyncio.run(run_rebalance(mock_client))

    assert response.status_code == status.HTTP_500_INTERNAL_SERVER_ERROR
    _mock_save.assert_not_called()


@patch("portfolio_manager.rebalance.execute_open_positions")
@patch("portfolio_manager.rebalance._record_performance", new_callable=AsyncMock)
@patch(
    "portfolio_manager.rebalance.save_rebalance",
    new_callable=AsyncMock,
    return_value=True,
)
@patch("portfolio_manager.rebalance.get_optimal_portfolio")
@patch(
    "portfolio_manager.rebalance.classify_regime",
    return_value={"state": "mean_reversion", "confidence": 0.8},
)
@patch(
    "portfolio_manager.rebalance.compute_market_betas",
    return_value=pl.DataFrame({"ticker": ["NVDA", "AMD"], "market_beta": [1.0, 1.0]}),
)
@patch("portfolio_manager.rebalance.pairs_schema")
@patch(
    "portfolio_manager.rebalance.select_pairs",
    return_value=pl.DataFrame(
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
    ),
)
@patch("portfolio_manager.rebalance.get_prior_allocation", new_callable=AsyncMock)
@patch(
    "portfolio_manager.rebalance.consolidate_predictions",
    return_value=pl.DataFrame({"ticker": ["NVDA", "AMD"]}),
)
@patch(
    "portfolio_manager.rebalance.get_raw_predictions",
    new_callable=AsyncMock,
    return_value=(pl.DataFrame({"ticker": ["NVDA"]}), None),
)
@patch(
    "portfolio_manager.rebalance.fetch_spy_prices",
    new_callable=AsyncMock,
    return_value=pl.DataFrame(),
)
@patch(
    "portfolio_manager.rebalance.fetch_equity_details",
    new_callable=AsyncMock,
    return_value=pl.DataFrame(),
)
@patch(
    "portfolio_manager.rebalance.fetch_historical_prices",
    new_callable=AsyncMock,
    return_value=pl.DataFrame(
        schema={"ticker": pl.Utf8, "timestamp": pl.Float64, "close_price": pl.Float64}
    ),
)
def test_run_rebalance_saves_only_opened_rows(
    _mock_hist: MagicMock,  # noqa: PT019
    _mock_equity: MagicMock,  # noqa: PT019
    _mock_spy: MagicMock,  # noqa: PT019
    _mock_predictions: AsyncMock,  # noqa: PT019
    _mock_consolidate: MagicMock,  # noqa: PT019
    mock_prior_portfolio: AsyncMock,
    _mock_select: MagicMock,  # noqa: PT019
    mock_pairs_schema: MagicMock,
    _mock_betas: MagicMock,  # noqa: PT019
    _mock_regime: MagicMock,  # noqa: PT019
    mock_optimal_portfolio: MagicMock,
    mock_save: AsyncMock,
    _mock_record: AsyncMock,  # noqa: PT019
    mock_execute_open: MagicMock,
) -> None:
    optimal = _make_optimal_portfolio()
    mock_optimal_portfolio.return_value = optimal
    mock_pairs_schema.validate.side_effect = lambda df: df
    mock_prior_portfolio.return_value = pl.DataFrame(schema=_PRIOR_ALLOCATION_SCHEMA)
    # AMD (short) is skipped; only NVDA (long) opened successfully.
    mock_execute_open.return_value = (
        [
            {
                "ticker": "NVDA",
                "action": "open",
                "side": "BUY",
                "dollar_amount": 990.0,
                "status": "success",
            },
            {
                "ticker": "AMD",
                "action": "open",
                "side": "SELL",
                "dollar_amount": 990.0,
                "status": "skipped",
                "reason": "not_shortable",
            },
        ],
        1,
    )

    mock_account = MagicMock()
    mock_account.cash_amount = 10000.0
    mock_account.buying_power = 10000.0
    mock_account.equity = 50000.0

    mock_client = MagicMock()
    mock_client.get_account.return_value = mock_account
    mock_client.get_shortable_tickers.return_value = ["NVDA", "AMD"]

    asyncio.run(run_rebalance(mock_client))

    saved_df = mock_save.call_args.kwargs["successful_pair_rows"]
    saved_tickers = saved_df["ticker"].to_list()
    assert "NVDA" not in saved_tickers
    assert "AMD" not in saved_tickers


@patch("portfolio_manager.rebalance.execute_open_positions")
@patch("portfolio_manager.rebalance._record_performance", new_callable=AsyncMock)
@patch(
    "portfolio_manager.rebalance.save_rebalance",
    new_callable=AsyncMock,
    return_value=True,
)
@patch("portfolio_manager.rebalance.get_optimal_portfolio")
@patch(
    "portfolio_manager.rebalance.classify_regime",
    return_value={"state": "mean_reversion", "confidence": 0.8},
)
@patch(
    "portfolio_manager.rebalance.compute_market_betas",
    return_value=pl.DataFrame({"ticker": ["NVDA", "AMD"], "market_beta": [1.0, 1.0]}),
)
@patch("portfolio_manager.rebalance.pairs_schema")
@patch(
    "portfolio_manager.rebalance.select_pairs",
    return_value=pl.DataFrame(
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
    ),
)
@patch("portfolio_manager.rebalance.get_prior_allocation", new_callable=AsyncMock)
@patch(
    "portfolio_manager.rebalance.consolidate_predictions",
    return_value=pl.DataFrame({"ticker": ["NVDA", "AMD"]}),
)
@patch(
    "portfolio_manager.rebalance.get_raw_predictions",
    new_callable=AsyncMock,
    return_value=(pl.DataFrame({"ticker": ["NVDA"]}), None),
)
@patch(
    "portfolio_manager.rebalance.fetch_spy_prices",
    new_callable=AsyncMock,
    return_value=pl.DataFrame(),
)
@patch(
    "portfolio_manager.rebalance.fetch_equity_details",
    new_callable=AsyncMock,
    return_value=pl.DataFrame(),
)
@patch(
    "portfolio_manager.rebalance.fetch_historical_prices",
    new_callable=AsyncMock,
    return_value=pl.DataFrame(
        schema={"ticker": pl.Utf8, "timestamp": pl.Float64, "close_price": pl.Float64}
    ),
)
def test_run_rebalance_saves_complete_pairs_when_both_legs_succeed(
    _mock_hist: MagicMock,  # noqa: PT019
    _mock_equity: MagicMock,  # noqa: PT019
    _mock_spy: MagicMock,  # noqa: PT019
    _mock_predictions: AsyncMock,  # noqa: PT019
    _mock_consolidate: MagicMock,  # noqa: PT019
    mock_prior_portfolio: AsyncMock,
    _mock_select: MagicMock,  # noqa: PT019
    mock_pairs_schema: MagicMock,
    _mock_betas: MagicMock,  # noqa: PT019
    _mock_regime: MagicMock,  # noqa: PT019
    mock_optimal_portfolio: MagicMock,
    mock_save: AsyncMock,
    _mock_record: AsyncMock,  # noqa: PT019
    mock_execute_open: MagicMock,
) -> None:
    optimal = _make_optimal_portfolio()
    mock_optimal_portfolio.return_value = optimal
    mock_pairs_schema.validate.side_effect = lambda df: df
    mock_prior_portfolio.return_value = pl.DataFrame(schema=_PRIOR_ALLOCATION_SCHEMA)
    # Both legs succeed — the full pair should be saved.
    mock_execute_open.return_value = (
        [
            {
                "ticker": "NVDA",
                "action": "open",
                "side": "BUY",
                "dollar_amount": 990.0,
                "status": "success",
            },
            {
                "ticker": "AMD",
                "action": "open",
                "side": "SELL",
                "dollar_amount": 990.0,
                "status": "success",
            },
        ],
        2,
    )

    mock_account = MagicMock()
    mock_account.cash_amount = 10000.0
    mock_account.buying_power = 10000.0
    mock_account.equity = 50000.0

    mock_client = MagicMock()
    mock_client.get_account.return_value = mock_account
    mock_client.get_shortable_tickers.return_value = ["NVDA", "AMD"]

    asyncio.run(run_rebalance(mock_client))

    saved_df = mock_save.call_args.kwargs["successful_pair_rows"]
    saved_tickers = saved_df["ticker"].to_list()
    assert "NVDA" in saved_tickers
    assert "AMD" in saved_tickers


# --- run_rebalance: empty predictions ---


@patch("portfolio_manager.rebalance.consolidate_predictions")
@patch(
    "portfolio_manager.rebalance.get_raw_predictions",
    new_callable=AsyncMock,
    return_value=(pl.DataFrame(), None),
)
@patch(
    "portfolio_manager.rebalance.fetch_spy_prices",
    new_callable=AsyncMock,
    return_value=pl.DataFrame(),
)
@patch(
    "portfolio_manager.rebalance.fetch_equity_details",
    new_callable=AsyncMock,
    return_value=pl.DataFrame(),
)
@patch(
    "portfolio_manager.rebalance.fetch_historical_prices",
    new_callable=AsyncMock,
    return_value=pl.DataFrame(),
)
def test_run_rebalance_empty_predictions_returns_200(
    _mock_hist: MagicMock,  # noqa: PT019
    _mock_equity: MagicMock,  # noqa: PT019
    _mock_spy: MagicMock,  # noqa: PT019
    _mock_predictions: AsyncMock,  # noqa: PT019
    mock_consolidate: MagicMock,
) -> None:
    mock_account = MagicMock()
    mock_account.cash_amount = 10000.0
    mock_account.buying_power = 10000.0
    mock_account.equity = 10000.0

    mock_client = MagicMock()
    mock_client.get_account.return_value = mock_account

    response = asyncio.run(run_rebalance(mock_client))

    assert response.status_code == status.HTTP_200_OK
    mock_consolidate.assert_not_called()


# --- get_latest_predictions_correlation_id ---


def _make_mock_pool(execute_result: object) -> MagicMock:
    mock_result = AsyncMock()
    mock_result.fetchone = AsyncMock(return_value=execute_result)
    mock_connection = AsyncMock()
    mock_connection.execute = AsyncMock(return_value=mock_result)
    mock_pool = MagicMock()
    mock_pool.connection.return_value.__aenter__ = AsyncMock(
        return_value=mock_connection
    )
    mock_pool.connection.return_value.__aexit__ = AsyncMock(return_value=None)
    return mock_pool


def test_get_latest_predictions_correlation_id_returns_id_when_row_exists() -> None:
    mock_pool = _make_mock_pool(("test-uuid-value",))

    with patch(
        "portfolio_manager.rebalance.get_pool", AsyncMock(return_value=mock_pool)
    ):
        result = asyncio.run(get_latest_predictions_correlation_id())

    assert result == "test-uuid-value"


def test_get_latest_predictions_correlation_id_returns_none_when_no_rows() -> None:
    mock_pool = _make_mock_pool(None)

    with patch(
        "portfolio_manager.rebalance.get_pool", AsyncMock(return_value=mock_pool)
    ):
        result = asyncio.run(get_latest_predictions_correlation_id())

    assert result is None


def test_get_latest_predictions_correlation_id_filters_to_current_date() -> None:
    mock_pool = _make_mock_pool(None)

    with patch(
        "portfolio_manager.rebalance.get_pool", AsyncMock(return_value=mock_pool)
    ) as _:
        mock_connection = mock_pool.connection.return_value.__aenter__.return_value
        asyncio.run(get_latest_predictions_correlation_id())

    sql = mock_connection.execute.call_args[0][0]
    assert "CURRENT_DATE" in sql


# --- get_raw_predictions ---


def _make_mock_pool_with_fetchall(rows: list) -> MagicMock:
    mock_result = AsyncMock()
    mock_result.fetchall = AsyncMock(return_value=rows)
    mock_connection = AsyncMock()
    mock_connection.execute = AsyncMock(return_value=mock_result)
    mock_pool = MagicMock()
    mock_pool.connection.return_value.__aenter__ = AsyncMock(
        return_value=mock_connection
    )
    mock_pool.connection.return_value.__aexit__ = AsyncMock(return_value=None)
    return mock_pool


def test_get_raw_predictions_returns_dataframe_with_correlation_id() -> None:
    rows = [
        ("AAPL", 1_700_000_000_000, 140.0, 150.0, 160.0, "run-abc"),
        ("MSFT", 1_700_000_000_000, 280.0, 300.0, 320.0, "run-abc"),
    ]
    mock_pool = _make_mock_pool_with_fetchall(rows)

    with patch(
        "portfolio_manager.rebalance.get_pool", AsyncMock(return_value=mock_pool)
    ):
        result, model_run_id = asyncio.run(get_raw_predictions("abc-correlation-id"))

    assert len(result) == 2  # noqa: PLR2004
    assert "ticker" in result.columns
    assert "quantile_50" in result.columns
    assert model_run_id == "run-abc"


def test_get_raw_predictions_returns_dataframe_without_correlation_id() -> None:
    rows = [("AAPL", 1_700_000_000_000, 140.0, 150.0, 160.0, "run-xyz")]
    mock_pool = _make_mock_pool_with_fetchall(rows)

    with patch(
        "portfolio_manager.rebalance.get_pool", AsyncMock(return_value=mock_pool)
    ):
        result, _ = asyncio.run(get_raw_predictions())

    assert len(result) == 1


def test_get_raw_predictions_returns_empty_dataframe_when_no_rows() -> None:
    mock_pool = _make_mock_pool_with_fetchall([])

    with patch(
        "portfolio_manager.rebalance.get_pool", AsyncMock(return_value=mock_pool)
    ):
        result, model_run_id = asyncio.run(get_raw_predictions("no-results-id"))

    assert len(result) == 0
    assert "ticker" in result.columns
    assert "quantile_10" in result.columns
    assert model_run_id is None


def test_get_raw_predictions_without_correlation_id_filters_to_current_date() -> None:
    mock_pool = _make_mock_pool_with_fetchall([])

    with patch(
        "portfolio_manager.rebalance.get_pool", AsyncMock(return_value=mock_pool)
    ):
        mock_connection = mock_pool.connection.return_value.__aenter__.return_value
        asyncio.run(get_raw_predictions())

    sql = mock_connection.execute.call_args[0][0]
    assert "CURRENT_DATE" in sql


def test_get_raw_predictions_returns_empty_when_predictions_are_stale() -> None:
    # Simulate the CURRENT_DATE filter excluding yesterday's predictions by
    # returning no rows from the database.
    mock_pool = _make_mock_pool_with_fetchall([])

    with patch(
        "portfolio_manager.rebalance.get_pool", AsyncMock(return_value=mock_pool)
    ):
        result, model_run_id = asyncio.run(get_raw_predictions())

    assert result.is_empty()
    assert model_run_id is None


# --- get_optimal_portfolio ---


def test_get_optimal_portfolio_returns_schema_validated_portfolio() -> None:
    mock_sized = _make_optimal_portfolio()
    with (
        patch(
            "portfolio_manager.rebalance.size_pairs_with_volatility_parity",
            return_value=mock_sized,
        ),
        patch("portfolio_manager.rebalance.portfolio_schema") as mock_schema,
    ):
        mock_schema.validate.side_effect = lambda df: df
        result = get_optimal_portfolio(
            candidate_pairs=pl.DataFrame(),
            maximum_capital=10000.0,
            current_timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            market_betas=pl.DataFrame(),
            entry_prices={},
            exposure_scale=1.0,
            short_buying_power_buffer=0.1,
        )
    assert result is mock_sized
    mock_schema.validate.assert_called_once_with(mock_sized)


# --- _record_performance ---


def test_record_performance_with_empty_prior_allocation() -> None:
    async def _run() -> None:
        mock_account = MagicMock()
        mock_account.cash_amount = 10000.0

        historical_prices = pl.DataFrame(
            {"ticker": ["AAPL"], "timestamp": [1.0], "close_price": [100.0]}
        )
        final_allocation = pl.DataFrame(schema=_PRIOR_ALLOCATION_SCHEMA)

        with (
            patch(
                "portfolio_manager.rebalance.get_last_portfolio_value",
                new_callable=AsyncMock,
                return_value=None,
            ),
            patch(
                "portfolio_manager.rebalance.compute_portfolio_value",
                return_value=10000.0,
            ),
            patch(
                "portfolio_manager.rebalance.build_performance_snapshot",
                return_value={},
            ),
            patch(
                "portfolio_manager.rebalance.save_performance_snapshot",
                new_callable=AsyncMock,
                return_value=True,
            ),
        ):
            await _record_performance(
                prior_allocation=pl.DataFrame(schema=_PRIOR_ALLOCATION_SCHEMA),
                held_tickers=set(),
                final_allocation=final_allocation,
                historical_prices=historical_prices,
                spy_prices=pl.DataFrame(),
                account=mock_account,
                current_timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            )

    asyncio.run(_run())


def test_record_performance_closes_pairs_and_records_snapshot() -> None:
    async def _run() -> None:
        mock_account = MagicMock()
        mock_account.cash_amount = 10000.0

        historical_prices = pl.DataFrame(
            {
                "ticker": ["AAPL", "MSFT"],
                "timestamp": [1.0, 1.0],
                "close_price": [100.0, 200.0],
            }
        )
        spy_prices = pl.DataFrame(
            {"ticker": ["SPY"], "timestamp": [1.0], "close_price": [450.0]}
        )
        prior_allocation = _make_prior_portfolio(
            [{"pair_id": "AAPL-MSFT", "long_ticker": "AAPL", "short_ticker": "MSFT"}]
        )
        final_allocation = pl.DataFrame(schema=_PRIOR_ALLOCATION_SCHEMA)

        with (
            patch(
                "portfolio_manager.rebalance.compute_realized_profit_and_loss",
                return_value=(50.0, 0.05),
            ),
            patch(
                "portfolio_manager.rebalance.build_closed_pair_record", return_value={}
            ),
            patch(
                "portfolio_manager.rebalance.save_closed_pair",
                new_callable=AsyncMock,
                return_value=True,
            ),
            patch(
                "portfolio_manager.rebalance.get_last_portfolio_value",
                new_callable=AsyncMock,
                return_value=9500.0,
            ),
            patch(
                "portfolio_manager.rebalance.compute_portfolio_value",
                return_value=10000.0,
            ),
            patch(
                "portfolio_manager.rebalance.compute_period_return", return_value=0.05
            ),
            patch(
                "portfolio_manager.rebalance.build_performance_snapshot",
                return_value={},
            ),
            patch(
                "portfolio_manager.rebalance.save_performance_snapshot",
                new_callable=AsyncMock,
                return_value=True,
            ),
        ):
            await _record_performance(
                prior_allocation=prior_allocation,
                held_tickers=set(),
                final_allocation=final_allocation,
                historical_prices=historical_prices,
                spy_prices=spy_prices,
                account=mock_account,
                current_timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            )

    asyncio.run(_run())


# --- run_rebalance: 207 and _record_performance exception paths ---


@patch("portfolio_manager.rebalance.emit_event", new_callable=AsyncMock)
@patch("portfolio_manager.rebalance.execute_open_positions")
@patch("portfolio_manager.rebalance._record_performance", new_callable=AsyncMock)
@patch(
    "portfolio_manager.rebalance.save_rebalance",
    new_callable=AsyncMock,
    return_value=False,
)
@patch("portfolio_manager.rebalance.get_optimal_portfolio")
@patch(
    "portfolio_manager.rebalance.classify_regime",
    return_value={"state": "mean_reversion", "confidence": 0.8},
)
@patch(
    "portfolio_manager.rebalance.compute_market_betas",
    return_value=pl.DataFrame({"ticker": ["NVDA", "AMD"], "market_beta": [1.0, 1.0]}),
)
@patch("portfolio_manager.rebalance.pairs_schema")
@patch(
    "portfolio_manager.rebalance.select_pairs",
    return_value=pl.DataFrame(
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
    ),
)
@patch("portfolio_manager.rebalance.get_prior_allocation", new_callable=AsyncMock)
@patch(
    "portfolio_manager.rebalance.consolidate_predictions",
    return_value=pl.DataFrame({"ticker": ["NVDA", "AMD"]}),
)
@patch(
    "portfolio_manager.rebalance.get_raw_predictions",
    new_callable=AsyncMock,
    return_value=(pl.DataFrame({"ticker": ["NVDA"]}), None),
)
@patch(
    "portfolio_manager.rebalance.fetch_spy_prices",
    new_callable=AsyncMock,
    return_value=pl.DataFrame(),
)
@patch(
    "portfolio_manager.rebalance.fetch_equity_details",
    new_callable=AsyncMock,
    return_value=pl.DataFrame(),
)
@patch(
    "portfolio_manager.rebalance.fetch_historical_prices",
    new_callable=AsyncMock,
    return_value=pl.DataFrame(
        schema={"ticker": pl.Utf8, "timestamp": pl.Float64, "close_price": pl.Float64}
    ),
)
def test_run_rebalance_returns_207_when_save_allocation_fails(
    _mock_hist: MagicMock,  # noqa: PT019
    _mock_equity: MagicMock,  # noqa: PT019
    _mock_spy: MagicMock,  # noqa: PT019
    _mock_predictions: AsyncMock,  # noqa: PT019
    _mock_consolidate: MagicMock,  # noqa: PT019
    mock_prior_portfolio: AsyncMock,
    _mock_select: MagicMock,  # noqa: PT019
    mock_pairs_schema: MagicMock,
    _mock_betas: MagicMock,  # noqa: PT019
    _mock_regime: MagicMock,  # noqa: PT019
    mock_optimal_portfolio: MagicMock,
    _mock_save: AsyncMock,  # noqa: PT019
    _mock_record: AsyncMock,  # noqa: PT019
    mock_execute_open: MagicMock,
    _mock_emit: AsyncMock,  # noqa: PT019
) -> None:
    optimal = _make_optimal_portfolio()
    mock_optimal_portfolio.return_value = optimal
    mock_pairs_schema.validate.side_effect = lambda df: df
    mock_prior_portfolio.return_value = pl.DataFrame(schema=_PRIOR_ALLOCATION_SCHEMA)
    mock_execute_open.return_value = (
        [
            {
                "ticker": "NVDA",
                "action": "open",
                "side": "BUY",
                "dollar_amount": 990.0,
                "status": "success",
            },
            {
                "ticker": "AMD",
                "action": "open",
                "side": "SELL",
                "dollar_amount": 990.0,
                "status": "success",
            },
        ],
        2,
    )

    mock_account = MagicMock()
    mock_account.cash_amount = 10000.0
    mock_account.buying_power = 10000.0
    mock_account.equity = 50000.0

    mock_client = MagicMock()
    mock_client.get_account.return_value = mock_account
    mock_client.get_shortable_tickers.return_value = ["NVDA", "AMD"]

    response = asyncio.run(run_rebalance(mock_client))

    assert response.status_code == status.HTTP_207_MULTI_STATUS
    _mock_emit.assert_not_called()


@patch("portfolio_manager.rebalance.execute_open_positions")
@patch("portfolio_manager.rebalance._record_performance", new_callable=AsyncMock)
@patch(
    "portfolio_manager.rebalance.save_rebalance",
    new_callable=AsyncMock,
    return_value=True,
)
@patch("portfolio_manager.rebalance.get_optimal_portfolio")
@patch(
    "portfolio_manager.rebalance.classify_regime",
    return_value={"state": "mean_reversion", "confidence": 0.8},
)
@patch(
    "portfolio_manager.rebalance.compute_market_betas",
    return_value=pl.DataFrame({"ticker": ["NVDA", "AMD"], "market_beta": [1.0, 1.0]}),
)
@patch("portfolio_manager.rebalance.pairs_schema")
@patch(
    "portfolio_manager.rebalance.select_pairs",
    return_value=pl.DataFrame(
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
    ),
)
@patch("portfolio_manager.rebalance.get_prior_allocation", new_callable=AsyncMock)
@patch(
    "portfolio_manager.rebalance.consolidate_predictions",
    return_value=pl.DataFrame({"ticker": ["NVDA", "AMD"]}),
)
@patch(
    "portfolio_manager.rebalance.get_raw_predictions",
    new_callable=AsyncMock,
    return_value=(pl.DataFrame({"ticker": ["NVDA"]}), None),
)
@patch(
    "portfolio_manager.rebalance.fetch_spy_prices",
    new_callable=AsyncMock,
    return_value=pl.DataFrame(),
)
@patch(
    "portfolio_manager.rebalance.fetch_equity_details",
    new_callable=AsyncMock,
    return_value=pl.DataFrame(),
)
@patch(
    "portfolio_manager.rebalance.fetch_historical_prices",
    new_callable=AsyncMock,
    return_value=pl.DataFrame(
        schema={"ticker": pl.Utf8, "timestamp": pl.Float64, "close_price": pl.Float64}
    ),
)
def test_run_rebalance_returns_200_when_record_performance_raises(
    _mock_hist: MagicMock,  # noqa: PT019
    _mock_equity: MagicMock,  # noqa: PT019
    _mock_spy: MagicMock,  # noqa: PT019
    _mock_predictions: AsyncMock,  # noqa: PT019
    _mock_consolidate: MagicMock,  # noqa: PT019
    mock_prior_portfolio: AsyncMock,
    _mock_select: MagicMock,  # noqa: PT019
    mock_pairs_schema: MagicMock,
    _mock_betas: MagicMock,  # noqa: PT019
    _mock_regime: MagicMock,  # noqa: PT019
    mock_optimal_portfolio: MagicMock,
    _mock_save: AsyncMock,  # noqa: PT019
    mock_record: AsyncMock,
    mock_execute_open: MagicMock,
) -> None:
    optimal = _make_optimal_portfolio()
    mock_optimal_portfolio.return_value = optimal
    mock_pairs_schema.validate.side_effect = lambda df: df
    mock_prior_portfolio.return_value = pl.DataFrame(schema=_PRIOR_ALLOCATION_SCHEMA)
    mock_record.side_effect = RuntimeError("metrics failure")
    mock_execute_open.return_value = (
        [
            {
                "ticker": "NVDA",
                "action": "open",
                "side": "BUY",
                "dollar_amount": 990.0,
                "status": "success",
            },
            {
                "ticker": "AMD",
                "action": "open",
                "side": "SELL",
                "dollar_amount": 990.0,
                "status": "success",
            },
        ],
        2,
    )

    mock_account = MagicMock()
    mock_account.cash_amount = 10000.0
    mock_account.buying_power = 10000.0
    mock_account.equity = 50000.0

    mock_client = MagicMock()
    mock_client.get_account.return_value = mock_account
    mock_client.get_shortable_tickers.return_value = ["NVDA", "AMD"]

    with patch("portfolio_manager.rebalance.emit_event", new_callable=AsyncMock):
        response = asyncio.run(run_rebalance(mock_client))

    assert response.status_code == status.HTTP_200_OK
