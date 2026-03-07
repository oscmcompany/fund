import json
from unittest.mock import MagicMock, patch

import polars as pl
import pytest
from portfoliomanager.server import (
    _PRIOR_PORTFOLIO_SCHEMA,
    evaluate_prior_pairs,
    get_positions,
    get_prior_portfolio,
)


def _make_prior_portfolio(pairs: list[dict]) -> pl.DataFrame:
    rows = []
    for pair in pairs:
        rows.append(
            {
                "ticker": pair["long_ticker"],
                "timestamp": 1735689600.0,
                "side": "LONG",
                "dollar_amount": 1000.0,
                "action": "OPEN_POSITION",
                "pair_id": pair["pair_id"],
            }
        )
        rows.append(
            {
                "ticker": pair["short_ticker"],
                "timestamp": 1735689600.0,
                "side": "SHORT",
                "dollar_amount": 1000.0,
                "action": "OPEN_POSITION",
                "pair_id": pair["pair_id"],
            }
        )
    return pl.DataFrame(rows, schema=_PRIOR_PORTFOLIO_SCHEMA)


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
            "timestamp": [1735689600.0, 1735689600.0],
            "side": ["LONG", "SHORT"],
            "dollar_amount": [1000.0, 1000.0],
            "action": ["OPEN_POSITION", "OPEN_POSITION"],
            "pair_id": ["NVDA-AMD", "NVDA-AMD"],
        }
    )


# --- evaluate_prior_pairs ---


def test_evaluate_prior_pairs_returns_empty_set_for_empty_portfolio() -> None:
    empty_portfolio = pl.DataFrame(schema=_PRIOR_PORTFOLIO_SCHEMA)
    historical_prices = _make_historical_prices(["AAPL", "MSFT"])
    result = evaluate_prior_pairs(empty_portfolio, historical_prices)
    assert result == set()


def test_evaluate_prior_pairs_holds_pair_in_intermediate_zone() -> None:
    prior = _make_prior_portfolio(
        [{"pair_id": "AAPL-MSFT", "long_ticker": "AAPL", "short_ticker": "MSFT"}]
    )
    historical_prices = _make_historical_prices(["AAPL", "MSFT"])
    with patch(
        "portfoliomanager.server.compute_spread_zscore", return_value=(2.0, 1.0)
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
        "portfoliomanager.server.compute_spread_zscore", return_value=(0.5, 1.0)
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
        "portfoliomanager.server.compute_spread_zscore", return_value=(0.2, 1.0)
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
        "portfoliomanager.server.compute_spread_zscore", return_value=(5.0, 1.0)
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
        "portfoliomanager.server.compute_spread_zscore", return_value=(-2.0, 1.0)
    ):
        result = evaluate_prior_pairs(prior, historical_prices)
    assert "AAPL" in result
    assert "MSFT" in result


def test_evaluate_prior_pairs_skips_malformed_pair_missing_long_leg() -> None:
    prior = pl.DataFrame(
        {
            "ticker": ["MSFT"],
            "timestamp": [1735689600.0],
            "side": ["SHORT"],
            "dollar_amount": [1000.0],
            "action": ["OPEN_POSITION"],
            "pair_id": ["AAPL-MSFT"],
        },
        schema=_PRIOR_PORTFOLIO_SCHEMA,
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
    rows = []
    for i in range(65):
        rows.append(
            {
                "ticker": "AAPL",
                "timestamp": float(i),
                "close_price": 0.0 if i == 0 else 100.0,
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
        "portfoliomanager.server.compute_spread_zscore",
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
        "portfoliomanager.server.compute_spread_zscore",
        side_effect=[(2.0, 1.0), (0.2, 1.0)],
    ):
        result = evaluate_prior_pairs(prior, historical_prices)

    assert result == {"AAPL", "MSFT"}


# --- get_prior_portfolio ---


def test_get_prior_portfolio_returns_empty_dataframe_on_empty_array_response() -> None:
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.text = "[]"
    with patch("portfoliomanager.server.requests.get", return_value=mock_response):
        result = get_prior_portfolio()
    assert result.is_empty()
    assert "pair_id" in result.columns


def test_get_prior_portfolio_returns_dataframe_with_pair_id_on_success() -> None:
    data = [
        {
            "ticker": "AAPL",
            "timestamp": 1735689600.0,
            "side": "LONG",
            "dollar_amount": 1000.0,
            "action": "OPEN_POSITION",
            "pair_id": "AAPL-MSFT",
        }
    ]
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.text = json.dumps(data)
    mock_response.json.return_value = data
    with patch("portfoliomanager.server.requests.get", return_value=mock_response):
        result = get_prior_portfolio()
    assert result.height == 1
    assert "pair_id" in result.columns
    assert result["pair_id"][0] == "AAPL-MSFT"


def test_get_prior_portfolio_returns_empty_dataframe_on_error_response() -> None:
    mock_response = MagicMock()
    mock_response.status_code = 500
    with patch("portfoliomanager.server.requests.get", return_value=mock_response):
        result = get_prior_portfolio()
    assert result.is_empty()
    assert "pair_id" in result.columns


def test_get_prior_portfolio_returns_empty_dataframe_on_whitespace_response() -> None:
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.text = "  "
    with patch("portfoliomanager.server.requests.get", return_value=mock_response):
        result = get_prior_portfolio()
    assert result.is_empty()


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
