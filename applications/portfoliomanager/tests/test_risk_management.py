from datetime import UTC, datetime

import polars as pl
import pytest
from portfoliomanager.exceptions import InsufficientPredictionsError
from portfoliomanager.risk_management import (
    add_predictions_zscore_ranked_columns,
    create_optimal_portfolio,
)


def test_add_predictions_zscore_ranked_columns_zscore_calculation() -> None:
    predictions = pl.DataFrame(
        {
            "ticker": ["A", "B", "C"],
            "quantile_10": [0.0, 0.0, 0.0],
            "quantile_50": [0.05, 0.10, 0.15],  # 5%, 10%, 15% expected returns
            "quantile_90": [0.20, 0.20, 0.20],
        }
    )

    result = add_predictions_zscore_ranked_columns(predictions)

    assert result["ticker"][0] == "C"  # highest expected return
    assert result["ticker"][2] == "A"  # lowest expected return

    assert "z_score_return" in result.columns
    assert "inter_quartile_range" in result.columns
    assert "composite_score" in result.columns


def test_add_predictions_zscore_ranked_columns_inter_quartile_range_calculation() -> (
    None
):
    predictions = pl.DataFrame(
        {
            "ticker": ["A", "B"],
            "quantile_10": [0.05, 0.10],
            "quantile_50": [0.10, 0.15],
            "quantile_90": [0.15, 0.30],  # a has narrow range, b has wide range
        }
    )

    result = add_predictions_zscore_ranked_columns(predictions)

    assert result["ticker"][0] == "B"  # higher expected return ranks first
    assert result["inter_quartile_range"][0] == pytest.approx(
        0.20
    )  # 0.30 - 0.10 (B's range)
    assert result["inter_quartile_range"][1] == pytest.approx(
        0.10
    )  # 0.15 - 0.05 (A's range)


def test_add_predictions_zscore_ranked_columns_single_prediction() -> None:
    predictions = pl.DataFrame(
        {
            "ticker": ["AAPL"],
            "quantile_10": [0.05],
            "quantile_50": [0.10],
            "quantile_90": [0.15],
        }
    )

    result = add_predictions_zscore_ranked_columns(predictions)

    assert len(result) == 1
    assert result["z_score_return"][0] == 0.0  # single value has z-score of 0


def test_create_optimal_portfolio_fresh_start_no_prior_tickers() -> None:
    """Test portfolio creation with no prior portfolio (fresh start)."""
    current_timestamp = datetime(2024, 1, 15, 9, 30, tzinfo=UTC)

    # Create 30 predictions with varying scores
    predictions = pl.DataFrame(
        {
            "ticker": [f"TICK{i:02d}" for i in range(30)],
            "quantile_10": [0.0] * 30,
            "quantile_50": [i * 0.01 for i in range(30)],  # 0%, 1%, 2%, ..., 29%
            "quantile_90": [0.05] * 30,  # Low uncertainty (IQR = 0.05 < 0.1 threshold)
        }
    )

    # Rank and sort predictions
    ranked_predictions = add_predictions_zscore_ranked_columns(predictions)

    result = create_optimal_portfolio(
        current_predictions=ranked_predictions,
        prior_portfolio_tickers=[],  # No prior portfolio
        maximum_capital=10000.0,
        current_timestamp=current_timestamp,
    )

    # Should create 20 positions (10 long, 10 short)
    assert len(result) == 20  # noqa: PLR2004
    assert result.filter(pl.col("side") == "LONG").height == 10  # noqa: PLR2004
    assert result.filter(pl.col("side") == "SHORT").height == 10  # noqa: PLR2004

    # All positions should have action=OPEN_POSITION
    assert all(action == "OPEN_POSITION" for action in result["action"].to_list())

    # Equal dollar allocation: 50% to longs, 50% to shorts
    long_capital = result.filter(pl.col("side") == "LONG")["dollar_amount"].sum()
    short_capital = result.filter(pl.col("side") == "SHORT")["dollar_amount"].sum()
    assert long_capital == pytest.approx(5000.0)
    assert short_capital == pytest.approx(5000.0)

    # Each position should get (capital / 2) / 10
    expected_amount = 500.0
    assert all(
        amount == pytest.approx(expected_amount)
        for amount in result["dollar_amount"].to_list()
    )

    # Top 10 should be long (highest composite scores)
    long_tickers = result.filter(pl.col("side") == "LONG")["ticker"].to_list()
    expected_long = [f"TICK{i:02d}" for i in range(29, 19, -1)]  # TICK29 to TICK20
    assert set(long_tickers) == set(expected_long)

    # Bottom 10 should be short (lowest composite scores)
    short_tickers = result.filter(pl.col("side") == "SHORT")["ticker"].to_list()
    expected_short = [f"TICK{i:02d}" for i in range(10)]  # TICK00 to TICK09
    assert set(short_tickers) == set(expected_short)


def test_create_optimal_portfolio_with_prior_ticker_exclusion() -> None:
    """Test that prior portfolio tickers are excluded to avoid PDT violations."""
    current_timestamp = datetime(2024, 1, 15, 9, 30, tzinfo=UTC)

    # Create 30 predictions
    predictions = pl.DataFrame(
        {
            "ticker": [f"TICK{i:02d}" for i in range(30)],
            "quantile_10": [0.0] * 30,
            "quantile_50": [i * 0.01 for i in range(30)],
            "quantile_90": [0.05] * 30,  # Low uncertainty (IQR = 0.05 < 0.1 threshold)
        }
    )

    # Rank and sort predictions
    ranked_predictions = add_predictions_zscore_ranked_columns(predictions)

    # Exclude the top 5 tickers from prior portfolio
    prior_tickers = ["TICK29", "TICK28", "TICK27", "TICK26", "TICK25"]

    result = create_optimal_portfolio(
        current_predictions=ranked_predictions,
        prior_portfolio_tickers=prior_tickers,
        maximum_capital=10000.0,
        current_timestamp=current_timestamp,
    )

    # Should still create 20 positions
    assert len(result) == 20  # noqa: PLR2004

    # None of the prior tickers should appear in the new portfolio
    result_tickers = result["ticker"].to_list()
    for ticker in prior_tickers:
        assert ticker not in result_tickers

    # Since top 5 are excluded, next 10 should be long (TICK24 to TICK15)
    long_tickers = result.filter(pl.col("side") == "LONG")["ticker"].to_list()
    expected_long = [f"TICK{i:02d}" for i in range(24, 14, -1)]
    assert set(long_tickers) == set(expected_long)


def test_create_optimal_portfolio_high_uncertainty_exclusions() -> None:
    """Test that high uncertainty predictions are excluded."""
    current_timestamp = datetime(2024, 1, 15, 9, 30, tzinfo=UTC)

    # Create 25 predictions: 20 low uncertainty, 5 high uncertainty
    high_uncertainty_count = 5
    tickers = [f"TICK{i:02d}" for i in range(25)]
    predictions = pl.DataFrame(
        {
            "ticker": tickers,
            "quantile_10": [0.0] * 25,
            "quantile_50": [i * 0.01 for i in range(25)],
            # First 5 have high uncertainty (IQR > 0.1), rest have low uncertainty
            "quantile_90": [0.50] * high_uncertainty_count + [0.08] * 20,
            "z_score_return": [float(i) for i in range(25)],
            "inter_quartile_range": [0.50] * high_uncertainty_count + [0.08] * 20,
            "composite_score": [
                float(i) / 1.5 if i < high_uncertainty_count else float(i) / 1.08
                for i in range(25)
            ],
            "action": ["UNSPECIFIED"] * 25,
        }
    )

    result = create_optimal_portfolio(
        current_predictions=predictions,
        prior_portfolio_tickers=[],
        maximum_capital=10000.0,
        current_timestamp=current_timestamp,
    )

    # Should create 20 positions from the 20 low-uncertainty tickers
    assert len(result) == 20  # noqa: PLR2004

    # None of the high uncertainty tickers should appear
    result_tickers = result["ticker"].to_list()
    for i in range(high_uncertainty_count):
        assert f"TICK{i:02d}" not in result_tickers


def test_create_optimal_portfolio_insufficient_after_exclusions() -> None:
    """Test that InsufficientPredictionsError is raised when fewer than 20 available."""
    current_timestamp = datetime(2024, 1, 15, 9, 30, tzinfo=UTC)

    # Create 25 predictions: 15 high uncertainty, 5 prior portfolio, only 5 available
    predictions = pl.DataFrame(
        {
            "ticker": [f"TICK{i:02d}" for i in range(25)],
            "quantile_10": [0.0] * 25,
            "quantile_50": [i * 0.01 for i in range(25)],
            # First 15 have high uncertainty (IQR > 0.1)
            "quantile_90": [0.50] * 15 + [0.08] * 10,
            "z_score_return": [float(i) for i in range(25)],
            "inter_quartile_range": [0.50] * 15 + [0.08] * 10,
            "composite_score": [float(i) / 1.5 for i in range(25)],
            "action": ["UNSPECIFIED"] * 25,
        }
    )

    # Exclude 5 more tickers as prior portfolio (from the low-uncertainty ones)
    prior_tickers = [f"TICK{i:02d}" for i in range(15, 20)]

    # Should raise InsufficientPredictionsError (only 5 available, need 20)
    with pytest.raises(InsufficientPredictionsError) as exc_info:
        create_optimal_portfolio(
            current_predictions=predictions,
            prior_portfolio_tickers=prior_tickers,
            maximum_capital=10000.0,
            current_timestamp=current_timestamp,
        )

    assert "Only 5 predictions available" in str(exc_info.value)
    assert "need 20" in str(exc_info.value)


def test_create_optimal_portfolio_equal_capital_allocation() -> None:
    """Test that capital is allocated equally across positions."""
    current_timestamp = datetime(2024, 1, 15, 9, 30, tzinfo=UTC)

    predictions = pl.DataFrame(
        {
            "ticker": [f"TICK{i:02d}" for i in range(30)],
            "quantile_10": [0.0] * 30,
            "quantile_50": [i * 0.01 for i in range(30)],
            "quantile_90": [0.05] * 30,  # Low uncertainty (IQR = 0.05 < 0.1 threshold)
        }
    )

    # Rank and sort predictions
    ranked_predictions = add_predictions_zscore_ranked_columns(predictions)

    # Test with different capital amounts
    for capital in [10000.0, 25000.0, 50000.0]:
        result = create_optimal_portfolio(
            current_predictions=ranked_predictions,
            prior_portfolio_tickers=[],
            maximum_capital=capital,
            current_timestamp=current_timestamp,
        )

        expected_per_position = capital / 20
        for amount in result["dollar_amount"].to_list():
            assert amount == pytest.approx(expected_per_position)

        # Long and short should be equal
        long_sum = result.filter(pl.col("side") == "LONG")["dollar_amount"].sum()
        short_sum = result.filter(pl.col("side") == "SHORT")["dollar_amount"].sum()
        assert long_sum == pytest.approx(short_sum)
        assert long_sum == pytest.approx(capital / 2)


def test_create_optimal_portfolio_head_tail_selection() -> None:
    """Test that top 10 are long, bottom 10 are short based on composite scores."""
    current_timestamp = datetime(2024, 1, 15, 9, 30, tzinfo=UTC)

    # Create predictions with known composite scores (via quantile_50 values)
    tickers = [f"TICK{i:02d}" for i in range(30)]

    predictions = pl.DataFrame(
        {
            "ticker": tickers,
            "quantile_10": [0.0] * 30,
            "quantile_50": [i * 0.01 for i in range(30)],  # 0, 0.01, 0.02, ..., 0.29
            "quantile_90": [0.05] * 30,  # Low uncertainty (IQR = 0.05 < 0.1 threshold)
        }
    )

    # Rank and sort predictions (will sort by composite score descending)
    ranked_predictions = add_predictions_zscore_ranked_columns(predictions)

    result = create_optimal_portfolio(
        current_predictions=ranked_predictions,
        prior_portfolio_tickers=[],
        maximum_capital=10000.0,
        current_timestamp=current_timestamp,
    )

    # Top 10 (highest composite scores: 29, 28, ..., 20) should be LONG
    long_tickers = result.filter(pl.col("side") == "LONG")["ticker"].to_list()
    expected_long = [f"TICK{i:02d}" for i in range(29, 19, -1)]
    assert set(long_tickers) == set(expected_long)

    # Bottom 10 (lowest composite scores: 0, 1, ..., 9) should be SHORT
    short_tickers = result.filter(pl.col("side") == "SHORT")["ticker"].to_list()
    expected_short = [f"TICK{i:02d}" for i in range(10)]
    assert set(short_tickers) == set(expected_short)
