from datetime import UTC, datetime

import polars as pl
import pytest
from portfoliomanager.risk_management import (
    add_equity_bars_returns_and_realized_volatility_columns,
    add_portfolio_action_column,
    add_portfolio_performance_columns,
    add_predictions_zscore_ranked_columns,
    create_optimal_portfolio,
)


def test_add_portfolio_action_column_same_day_positions_locked() -> None:
    current_datetime = datetime(2024, 1, 15, 0, 0, 0, 0, tzinfo=UTC)
    positions = pl.DataFrame(
        {
            "ticker": ["AAPL", "GOOGL"],
            "timestamp": [
                datetime(2024, 1, 15, 9, 30, tzinfo=UTC).timestamp(),
                datetime(2024, 1, 15, 14, 0, tzinfo=UTC).timestamp(),
            ],
            "side": ["LONG", "SHORT"],
            "dollar_amount": [1000.0, 1000.0],
        }
    )

    result = add_portfolio_action_column(positions, current_datetime)

    assert all(action == "PDT_LOCKED" for action in result["action"].to_list())
    assert len(result) == 2  # noqa: PLR2004


def test_add_portfolio_action_column_previous_day_positions_unlocked() -> None:
    current_datetime = datetime(2024, 1, 15, 0, 0, 0, 0, tzinfo=UTC)
    positions = pl.DataFrame(
        {
            "ticker": ["AAPL", "GOOGL"],
            "timestamp": [
                datetime(2024, 1, 14, 9, 30, tzinfo=UTC).timestamp(),
                datetime(2024, 1, 13, 14, 0, tzinfo=UTC).timestamp(),
            ],
            "side": ["LONG", "SHORT"],
            "dollar_amount": [1000.0, 1000.0],
        }
    )

    result = add_portfolio_action_column(positions, current_datetime)

    assert all(action == "UNSPECIFIED" for action in result["action"].to_list())
    assert len(result) == 2  # noqa: PLR2004


def test_add_portfolio_action_column_mixed_dates() -> None:
    current_datetime = datetime(2024, 1, 15, 0, 0, 0, 0, tzinfo=UTC)
    positions = pl.DataFrame(
        {
            "ticker": ["AAPL", "GOOGL", "TSLA"],
            "timestamp": [
                datetime(2024, 1, 15, 9, 30, tzinfo=UTC).timestamp(),  # same day
                datetime(2024, 1, 14, 14, 0, tzinfo=UTC).timestamp(),  # previous day
                datetime(2024, 1, 15, 16, 0, tzinfo=UTC).timestamp(),  # same day
            ],
            "side": ["LONG", "SHORT", "LONG"],
            "dollar_amount": [1000.0, 1000.0, 1000.0],
        }
    )

    result = add_portfolio_action_column(positions, current_datetime)

    expected_actions = ["PDT_LOCKED", "UNSPECIFIED", "PDT_LOCKED"]
    assert result["action"].to_list() == expected_actions


def test_add_portfolio_action_column_empty_dataframe() -> None:
    current_datetime = datetime(2024, 1, 15, 0, 0, 0, 0, tzinfo=UTC)
    positions = pl.DataFrame(
        {"ticker": [], "timestamp": [], "side": [], "dollar_amount": []}
    )

    result = add_portfolio_action_column(positions, current_datetime)

    assert len(result) == 0
    assert "action" in result.columns


def test_add_equity_bars_returns_and_realized_volatility_columns_sufficient_data_success() -> (  # noqa: E501
    None
):
    equity_bars = pl.DataFrame(
        {
            "ticker": ["AAPL"] * 35,
            "timestamp": [
                datetime(2024, 1, i + 1, tzinfo=UTC).timestamp() for i in range(31)
            ]
            + [datetime(2024, 2, i + 1, tzinfo=UTC).timestamp() for i in range(4)],
            "close_price": list(range(100, 135)),  # increasing prices
        }
    )

    result = add_equity_bars_returns_and_realized_volatility_columns(equity_bars)

    assert "daily_returns" in result.columns
    assert "log_daily_returns" in result.columns
    assert "realized_volatility" in result.columns
    assert len(result) == 35  # noqa: PLR2004


def test_add_equity_bars_returns_and_realized_volatility_columns_insufficient_data_raises_error() -> (  # noqa: E501
    None
):
    equity_bars = pl.DataFrame(
        {
            "ticker": ["AAPL"] * 25,  # only 25 bars, need 30
            "timestamp": [
                datetime(2024, 1, i + 1, tzinfo=UTC).timestamp() for i in range(25)
            ],
            "close_price": list(range(100, 125)),
        }
    )

    with pytest.raises(ValueError, match="Tickers with insufficient data"):
        add_equity_bars_returns_and_realized_volatility_columns(equity_bars)


def test_add_equity_bars_returns_and_realized_volatility_columns_multiple_tickers_mixed_data() -> (  # noqa: E501
    None
):
    equity_bars = pl.DataFrame(
        {
            "ticker": ["AAPL"] * 35 + ["GOOGL"] * 25,  # AAPL has enough, GOOGL does not
            "timestamp": [
                datetime(2024, 1, i + 1, tzinfo=UTC).timestamp() for i in range(31)
            ]
            + [datetime(2024, 2, i + 1, tzinfo=UTC).timestamp() for i in range(4)]
            + [datetime(2024, 2, i + 1, tzinfo=UTC).timestamp() for i in range(25)],
            "close_price": list(range(100, 135)) + list(range(200, 225)),
        }
    )

    with pytest.raises(ValueError, match="GOOGL"):
        add_equity_bars_returns_and_realized_volatility_columns(equity_bars)


def test_add_equity_bars_returns_grouped_per_ticker() -> None:
    base_timestamp = datetime(2024, 1, 1, tzinfo=UTC).timestamp()

    aapl_data = []
    googl_data = []

    for i in range(30):
        timestamp = base_timestamp + (i * 86400)
        aapl_price = 100.0 + i  # AAPL prices increase
        googl_price = 200.0 - i * 0.5  # GOOGL prices decrease slightly

        aapl_data.append(
            {"ticker": "AAPL", "timestamp": timestamp, "close_price": aapl_price}
        )
        googl_data.append(
            {"ticker": "GOOGL", "timestamp": timestamp, "close_price": googl_price}
        )

    all_data = []
    for i in range(30):
        all_data.append(aapl_data[i])
        all_data.append(googl_data[i])

    equity_bars = pl.DataFrame(all_data)

    out = add_equity_bars_returns_and_realized_volatility_columns(equity_bars)
    aapl = out.filter(pl.col("ticker") == "AAPL").sort("timestamp")
    googl = out.filter(pl.col("ticker") == "GOOGL").sort("timestamp")

    aapl_returns = aapl["daily_returns"].to_list()
    googl_returns = googl["daily_returns"].to_list()

    assert aapl_returns[0] is None
    assert aapl_returns[1] == pytest.approx(0.01, abs=1e-9)
    assert googl_returns[0] is None
    assert googl_returns[1] == pytest.approx(-0.0025, abs=1e-9)

    aapl_log_returns = aapl["log_daily_returns"].to_list()
    googl_log_returns = googl["log_daily_returns"].to_list()

    assert aapl_log_returns[0] is None
    assert aapl_log_returns[1] == pytest.approx(0.00995, abs=1e-4)
    assert googl_log_returns[0] is None
    assert googl_log_returns[1] == pytest.approx(-0.00251, abs=1e-4)


def test_add_equity_bars_returns_and_realized_volatility_columns_null_prices_handled() -> (  # noqa: E501
    None
):
    equity_bars = pl.DataFrame(
        {
            "ticker": ["AAPL"] * 35,
            "timestamp": [
                datetime(2024, 1, i + 1, tzinfo=UTC).timestamp() for i in range(31)
            ]
            + [datetime(2024, 2, i + 1, tzinfo=UTC).timestamp() for i in range(4)],
            "close_price": [
                100.0,
                None,
                102.0,
                *list(range(103, 135)),
            ],
        }
    )

    result = add_equity_bars_returns_and_realized_volatility_columns(equity_bars)

    daily_returns = result["daily_returns"].to_list()
    assert daily_returns[1] is None  # second row should be null


def test_add_portfolio_performance_columns_long_position_outperforming() -> None:
    base_timestamp = datetime(2024, 1, 10, tzinfo=UTC).timestamp()

    positions = pl.DataFrame(
        {
            "ticker": ["AAPL"],
            "timestamp": [base_timestamp],
            "side": ["LONG"],
            "dollar_amount": [1000.0],
            "action": ["UNSPECIFIED"],
        }
    )

    original_predictions = pl.DataFrame(
        {
            "ticker": ["AAPL"],
            "timestamp": [base_timestamp],
            "quantile_10": [-0.05],  # -5% lower threshold
            "quantile_90": [0.15],  # +15% upper threshold
        }
    )

    raw_equity_bars = pl.DataFrame(
        {
            "ticker": ["AAPL"] * 30,
            "timestamp": [base_timestamp + (i * 86400) for i in range(30)],
            "close_price": [100.0 + (20.0 * i / 29) for i in range(30)],
        }
    )

    original_equity_bars = add_equity_bars_returns_and_realized_volatility_columns(
        raw_equity_bars
    )
    current_timestamp = datetime.fromtimestamp(base_timestamp + (29 * 86400), tz=UTC)

    result = add_portfolio_performance_columns(
        positions, original_predictions, original_equity_bars, current_timestamp
    )

    assert result["action"][0] == "MAINTAIN_POSITION"  # 20% > 15% threshold


def test_add_portfolio_performance_columns_long_position_underperforming() -> None:
    base_timestamp = datetime(2024, 1, 10, tzinfo=UTC).timestamp()

    positions = pl.DataFrame(
        {
            "ticker": ["AAPL"],
            "timestamp": [base_timestamp],
            "side": ["LONG"],
            "dollar_amount": [1000.0],
            "action": ["UNSPECIFIED"],
        }
    )

    original_predictions = pl.DataFrame(
        {
            "ticker": ["AAPL"],
            "timestamp": [base_timestamp],
            "quantile_10": [-0.05],  # -5% lower threshold
            "quantile_90": [0.15],  # +15% upper threshold
        }
    )

    raw_equity_bars = pl.DataFrame(
        {
            "ticker": ["AAPL"] * 30,
            "timestamp": [base_timestamp + (i * 86400) for i in range(30)],
            "close_price": [100.0 - (10.0 * i / 29) for i in range(30)],
        }
    )

    original_equity_bars = add_equity_bars_returns_and_realized_volatility_columns(
        raw_equity_bars
    )
    current_timestamp = datetime.fromtimestamp(base_timestamp + (29 * 86400), tz=UTC)

    result = add_portfolio_performance_columns(
        positions, original_predictions, original_equity_bars, current_timestamp
    )

    assert result["action"][0] == "CLOSE_POSITION"  # -10% < -5% threshold


def test_add_portfolio_performance_columns_short_position_outperforming() -> None:
    base_timestamp = datetime(2024, 1, 10, tzinfo=UTC).timestamp()

    positions = pl.DataFrame(
        {
            "ticker": ["AAPL"],
            "timestamp": [base_timestamp],
            "side": ["SHORT"],
            "dollar_amount": [1000.0],
            "action": ["UNSPECIFIED"],
        }
    )

    original_predictions = pl.DataFrame(
        {
            "ticker": ["AAPL"],
            "timestamp": [base_timestamp],
            "quantile_10": [-0.05],  # -5% lower threshold
            "quantile_90": [0.15],  # +15% upper threshold
        }
    )

    raw_equity_bars = pl.DataFrame(
        {
            "ticker": ["AAPL"] * 30,
            "timestamp": [base_timestamp + (i * 86400) for i in range(30)],
            "close_price": [100.0 - (10.0 * i / 29) for i in range(30)],
        }
    )

    original_equity_bars = add_equity_bars_returns_and_realized_volatility_columns(
        raw_equity_bars
    )
    current_timestamp = datetime.fromtimestamp(base_timestamp + (29 * 86400), tz=UTC)

    result = add_portfolio_performance_columns(
        positions, original_predictions, original_equity_bars, current_timestamp
    )

    assert (
        result["action"][0] == "MAINTAIN_POSITION"
    )  # -10% <= -5% threshold (good for short)


def test_add_portfolio_performance_columns_pdt_locked_position_maintained() -> None:
    base_timestamp = datetime(2024, 1, 10, tzinfo=UTC).timestamp()

    positions = pl.DataFrame(
        {
            "ticker": ["AAPL"],
            "timestamp": [base_timestamp],
            "side": ["LONG"],
            "dollar_amount": [1000.0],
            "action": ["PDT_LOCKED"],  # pdt locked
        }
    )

    original_predictions = pl.DataFrame(
        {
            "ticker": ["AAPL"],
            "timestamp": [base_timestamp],
            "quantile_10": [-0.05],
            "quantile_90": [0.15],
        }
    )

    raw_equity_bars = pl.DataFrame(
        {
            "ticker": ["AAPL"] * 30,
            "timestamp": [base_timestamp + (i * 86400) for i in range(30)],
            "close_price": [100.0 - (20.0 * i / 29) for i in range(30)],
        }
    )

    original_equity_bars = add_equity_bars_returns_and_realized_volatility_columns(
        raw_equity_bars
    )
    current_timestamp = datetime.fromtimestamp(base_timestamp + (29 * 86400), tz=UTC)

    result = add_portfolio_performance_columns(
        positions, original_predictions, original_equity_bars, current_timestamp
    )

    assert result["action"][0] == "PDT_LOCKED"  # pdt locked overrides performance


def test_add_portfolio_performance_columns_multiple_tickers_independent() -> None:
    current_timestamp = datetime(2024, 1, 10, tzinfo=UTC).timestamp()

    positions = pl.DataFrame(
        {
            "ticker": ["AAPL", "GOOGL"],
            "timestamp": [current_timestamp, current_timestamp],
            "side": ["LONG", "LONG"],
            "dollar_amount": [1000.0, 1000.0],
            "action": ["UNSPECIFIED", "UNSPECIFIED"],
        }
    )

    predictions = pl.DataFrame(
        {
            "ticker": ["AAPL", "GOOGL"],
            "timestamp": [current_timestamp, current_timestamp],
            "quantile_10": [-0.05, -0.05],
            "quantile_90": [0.15, 0.15],
        }
    )

    aapl_data = []
    googl_data = []

    for i in range(30):
        timestamp = current_timestamp + (i * 86400)
        aapl_price = 100.0 + (20.0 * i / 29)
        googl_price = 200.0 - (20.0 * i / 29)

        aapl_data.append(
            {"ticker": "AAPL", "timestamp": timestamp, "close_price": aapl_price}
        )
        googl_data.append(
            {"ticker": "GOOGL", "timestamp": timestamp, "close_price": googl_price}
        )

    all_data = []
    for i in range(30):
        all_data.append(aapl_data[i])
        all_data.append(googl_data[i])

    raw_equity_bars = pl.DataFrame(all_data)

    equity_bars = add_equity_bars_returns_and_realized_volatility_columns(
        raw_equity_bars
    )

    out = add_portfolio_performance_columns(
        positions,
        predictions,
        equity_bars,
        datetime.fromtimestamp(current_timestamp + (29 * 86400), tz=UTC),  # 30th day
    )

    assert out.filter(pl.col("ticker") == "AAPL")["action"][0] == "MAINTAIN_POSITION"
    assert out.filter(pl.col("ticker") == "GOOGL")["action"][0] == "CLOSE_POSITION"


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


def test_create_optimal_portfolio_fresh_start_no_existing_positions() -> None:
    predictions = pl.DataFrame(
        {
            "ticker": [f"STOCK{i}" for i in range(25)],
            "quantile_10": [0.0] * 25,
            "quantile_50": [0.1] * 25,
            "quantile_90": [0.2] * 25,
            "composite_score": list(range(25, 0, -1)),  # descending scores
            "inter_quartile_range": [0.1] * 25,  # low uncertainty
        }
    )

    positions = pl.DataFrame(
        {
            "ticker": [],
            "timestamp": [],
            "side": [],
            "dollar_amount": [],
            "action": [],
        }
    )

    result = create_optimal_portfolio(
        predictions, positions, 20000.0, datetime.now(tz=UTC)
    )

    assert len(result) == 20  # 10 long + 10 short  # noqa: PLR2004
    assert result.filter(pl.col("side") == "LONG").height == 10  # noqa: PLR2004
    assert result.filter(pl.col("side") == "SHORT").height == 10  # noqa: PLR2004

    long_total = result.filter(pl.col("side") == "LONG")["dollar_amount"].sum()
    short_total = result.filter(pl.col("side") == "SHORT")["dollar_amount"].sum()
    assert abs(long_total - short_total) < 0.01  # noqa: PLR2004


def test_create_optimal_portfolio_some_maintained_positions() -> None:
    predictions = pl.DataFrame(
        {
            "ticker": [f"STOCK{i}" for i in range(25)],
            "quantile_10": [0.0] * 25,
            "quantile_50": [0.1] * 25,
            "quantile_90": [0.2] * 25,
            "composite_score": list(range(25, 0, -1)),
            "inter_quartile_range": [0.1] * 25,
        }
    )

    positions = pl.DataFrame(
        {
            "ticker": ["STOCK1", "STOCK2", "STOCK24"],
            "timestamp": [
                datetime(2024, 1, 10, tzinfo=UTC).timestamp(),
                datetime(2024, 1, 11, tzinfo=UTC).timestamp(),
                datetime(2024, 1, 12, tzinfo=UTC).timestamp(),
            ],
            "side": ["LONG", "LONG", "SHORT"],
            "dollar_amount": [1000.0, 1000.0, 1000.0],
            "action": ["MAINTAIN_POSITION", "MAINTAIN_POSITION", "MAINTAIN_POSITION"],
        }
    )

    result = create_optimal_portfolio(
        predictions, positions, 20000.0, datetime.now(tz=UTC)
    )

    assert len(result) == 20  # noqa: PLR2004
    assert "STOCK1" in result["ticker"].to_list()
    assert "STOCK2" in result["ticker"].to_list()
    assert "STOCK24" in result["ticker"].to_list()


def test_create_optimal_portfolio_high_uncertainty_exclusions() -> None:
    predictions = pl.DataFrame(
        {
            "ticker": ["HIGH_UNCERT", "LOW_UNCERT1", "LOW_UNCERT2"],
            "quantile_10": [0.0, 0.0, 0.0],
            "quantile_50": [0.1, 0.1, 0.1],
            "quantile_90": [0.2, 0.2, 0.2],
            "composite_score": [10.0, 5.0, 1.0],
            "inter_quartile_range": [0.8, 0.1, 0.1],  # first one too uncertain
        }
    )

    positions = pl.DataFrame(
        {
            "ticker": [],
            "timestamp": [],
            "side": [],
            "dollar_amount": [],
            "action": [],
        }
    )

    result = create_optimal_portfolio(
        predictions, positions, 20000.0, datetime.now(tz=UTC)
    )

    assert "HIGH_UNCERT" not in result["ticker"].to_list()
    assert len(result) == 2  # only 2 available predictions  # noqa: PLR2004


def test_create_optimal_portfolio_all_positions_maintained_no_new_needed() -> None:
    predictions = pl.DataFrame(
        {
            "ticker": [f"STOCK{i}" for i in range(25)],
            "quantile_10": [0.0] * 25,
            "quantile_50": [0.1] * 25,
            "quantile_90": [0.2] * 25,
            "composite_score": list(range(25, 0, -1)),
            "inter_quartile_range": [0.1] * 25,
        }
    )

    positions = pl.DataFrame(
        {
            "ticker": [f"MAINTAINED{i}" for i in range(20)],
            "timestamp": [datetime(2024, 1, 10, tzinfo=UTC).timestamp()] * 20,
            "side": ["LONG"] * 10 + ["SHORT"] * 10,
            "dollar_amount": [500.0] * 20,
            "action": ["MAINTAIN_POSITION"] * 20,
        }
    )

    result = create_optimal_portfolio(
        predictions, positions, 20000.0, datetime.now(tz=UTC)
    )

    assert len(result) == 20  # all maintained positions  # noqa: PLR2004
    expected_timestamp = datetime(2024, 1, 10, tzinfo=UTC).timestamp()
    assert all(ts == expected_timestamp for ts in result["timestamp"].to_list())


def test_create_optimal_portfolio_capital_rebalancing_with_closed_positions() -> None:
    predictions = pl.DataFrame(
        {
            "ticker": [f"NEW{i}" for i in range(15)],
            "quantile_10": [0.0] * 15,
            "quantile_50": [0.1] * 15,
            "quantile_90": [0.2] * 15,
            "composite_score": list(range(15, 0, -1)),
            "inter_quartile_range": [0.1] * 15,
        }
    )

    positions = pl.DataFrame(
        {
            "ticker": ["MAINTAINED1", "MAINTAINED2", "CLOSED1", "CLOSED2"],
            "timestamp": [
                datetime(2024, 1, 10, tzinfo=UTC).timestamp(),
                datetime(2024, 1, 11, tzinfo=UTC).timestamp(),
                datetime(2024, 1, 12, tzinfo=UTC).timestamp(),
                datetime(2024, 1, 13, tzinfo=UTC).timestamp(),
            ],
            "side": ["LONG", "SHORT", "LONG", "SHORT"],
            "dollar_amount": [800.0, 1200.0, 500.0, 500.0],  # uneven amounts
            "action": [
                "MAINTAIN_POSITION",
                "MAINTAIN_POSITION",
                "CLOSE_POSITION",
                "CLOSE_POSITION",
            ],
        }
    )

    result = create_optimal_portfolio(
        predictions, positions, 20000.0, datetime.now(tz=UTC)
    )

    # 2 maintained + 15 new (limited by available predictions)
    # even though this isn't a realistic scenario
    assert len(result) == 17  # noqa: PLR2004

    maintained = result.filter(
        pl.col("timestamp").is_in(
            [
                datetime(2024, 1, 10, tzinfo=UTC).timestamp(),
                datetime(2024, 1, 11, tzinfo=UTC).timestamp(),
            ]
        )
    )
    assert len(maintained) == 2  # noqa: PLR2004

    long_total = result.filter(pl.col("side") == "LONG")["dollar_amount"].sum()
    short_total = result.filter(pl.col("side") == "SHORT")["dollar_amount"].sum()
    assert abs(long_total - short_total) < 0.01  # noqa: PLR2004


def test_create_optimal_portfolio_mixed_closed_and_maintained_positions() -> None:
    predictions = pl.DataFrame(
        {
            "ticker": [f"STOCK{i:02d}" for i in range(30)],
            "quantile_10": [0.0] * 30,
            "quantile_50": [0.05] * 30,
            "quantile_90": [0.1] * 30,
            "composite_score": list(range(30, 0, -1)),
            "inter_quartile_range": [0.05]
            * 30,  # all acceptable uncertainty (below 0.1 threshold)
        }
    )

    positions = pl.DataFrame(
        {
            "ticker": ["OLD1", "OLD2", "OLD3", "OLD4", "OLD5"],
            "timestamp": [
                datetime(2024, 1, 10, tzinfo=UTC).timestamp(),
                datetime(2024, 1, 11, tzinfo=UTC).timestamp(),
                datetime(2024, 1, 12, tzinfo=UTC).timestamp(),
                datetime(2024, 1, 13, tzinfo=UTC).timestamp(),
                datetime(2024, 1, 14, tzinfo=UTC).timestamp(),
            ],
            "side": ["LONG", "LONG", "SHORT", "SHORT", "LONG"],
            "dollar_amount": [1000.0, 1000.0, 1000.0, 1000.0, 1000.0],
            "action": [
                "CLOSE_POSITION",
                "MAINTAIN_POSITION",
                "MAINTAIN_POSITION",
                "CLOSE_POSITION",
                "MAINTAIN_POSITION",
            ],
        }
    )

    result = create_optimal_portfolio(
        predictions,
        positions,
        20000.0,
        datetime.now(tz=UTC),
    )

    assert len(result) == 20  # noqa: PLR2004

    maintained_tickers = ["OLD2", "OLD3", "OLD5"]
    for ticker in maintained_tickers:
        assert ticker in result["ticker"].to_list()

    closed_tickers = ["OLD1", "OLD4"]
    for ticker in closed_tickers:
        assert ticker not in result["ticker"].to_list()

    assert "ticker" in result.columns
    assert "timestamp" in result.columns
    assert "side" in result.columns
    assert "dollar_amount" in result.columns
    assert "action" in result.columns
    assert len(result.columns) == 5  # noqa: PLR2004

    sorted_result = result.sort(["ticker", "side"])
    assert sorted_result.equals(result)


def test_add_portfolio_performance_columns_rebalancing_closes_shorts_when_longs_stopped() -> (  # noqa: E501
    None
):
    """Test that when longs hit stop-loss, best-performing shorts are closed for rebalancing."""  # noqa: E501
    base_timestamp = datetime(2024, 1, 10, tzinfo=UTC).timestamp()

    positions = pl.DataFrame(
        {
            "ticker": ["LONG1", "LONG2", "LONG3", "SHORT1", "SHORT2", "SHORT3"],
            "timestamp": [base_timestamp] * 6,
            "side": ["LONG", "LONG", "LONG", "SHORT", "SHORT", "SHORT"],
            "dollar_amount": [1000.0] * 6,
            "action": ["UNSPECIFIED"] * 6,
        }
    )

    predictions = pl.DataFrame(
        {
            "ticker": ["LONG1", "LONG2", "LONG3", "SHORT1", "SHORT2", "SHORT3"],
            "timestamp": [base_timestamp] * 6,
            "quantile_10": [-0.05] * 6,
            "quantile_90": [0.15] * 6,
        }
    )

    raw_equity_bars = []
    for ticker in ["LONG1", "LONG2", "LONG3", "SHORT1", "SHORT2", "SHORT3"]:
        for i in range(30):
            timestamp = base_timestamp + (i * 86400)
            if ticker in ["LONG1", "LONG2", "LONG3"]:
                price = 100.0 - (10.0 * i / 29)  # longs losing -10%
            elif ticker == "SHORT1":
                price = 100.0 - (5.0 * i / 29)  # short1 falling -5% (best for short)
            elif ticker == "SHORT2":
                price = 100.0 - (2.0 * i / 29)  # short2 falling -2% (second best)
            else:  # SHORT3
                price = 100.0 - (1.0 * i / 29)  # short3 falling -1% (worst)

            raw_equity_bars.append(
                {"ticker": ticker, "timestamp": timestamp, "close_price": price}
            )

    equity_bars = add_equity_bars_returns_and_realized_volatility_columns(
        pl.DataFrame(raw_equity_bars)
    )
    current_timestamp = datetime.fromtimestamp(base_timestamp + (29 * 86400), tz=UTC)

    result = add_portfolio_performance_columns(
        positions, predictions, equity_bars, current_timestamp
    )

    # 3 longs should be closed (hit stop-loss at -10% < -5%)
    closed_longs = result.filter(
        (pl.col("side") == "LONG") & (pl.col("action") == "CLOSE_POSITION")
    )
    assert closed_longs.height == 3  # noqa: PLR2004

    # 3 shorts should also be closed by rebalancing
    # None of the shorts hit stop-loss (all have negative returns, not >= +15%)
    # Rebalancing closes 3 shorts to match the 3 closed longs
    closed_shorts = result.filter(
        (pl.col("side") == "SHORT") & (pl.col("action") == "CLOSE_POSITION")
    )
    assert closed_shorts.height == 3  # noqa: PLR2004

    # Verify best-performing shorts were closed (most negative cumulative return)
    # SHORT1 (-5%), SHORT2 (-2%), SHORT3 (-1%) all closed by rebalancing
    closed_short_tickers = closed_shorts["ticker"].to_list()
    assert "SHORT1" in closed_short_tickers
    assert "SHORT2" in closed_short_tickers
    assert "SHORT3" in closed_short_tickers


def test_add_portfolio_performance_columns_rebalancing_closes_longs_when_shorts_stopped() -> (  # noqa: E501
    None
):
    """Test that when shorts hit stop-loss, best-performing longs are closed for rebalancing."""  # noqa: E501
    base_timestamp = datetime(2024, 1, 10, tzinfo=UTC).timestamp()

    positions = pl.DataFrame(
        {
            "ticker": ["LONG1", "LONG2", "LONG3", "SHORT1", "SHORT2"],
            "timestamp": [base_timestamp] * 5,
            "side": ["LONG", "LONG", "LONG", "SHORT", "SHORT"],
            "dollar_amount": [1000.0] * 5,
            "action": ["UNSPECIFIED"] * 5,
        }
    )

    predictions = pl.DataFrame(
        {
            "ticker": ["LONG1", "LONG2", "LONG3", "SHORT1", "SHORT2"],
            "timestamp": [base_timestamp] * 5,
            "quantile_10": [-0.05] * 5,
            "quantile_90": [0.15] * 5,
        }
    )

    raw_equity_bars = []
    for ticker in ["LONG1", "LONG2", "LONG3", "SHORT1", "SHORT2"]:
        for i in range(30):
            timestamp = base_timestamp + (i * 86400)
            if ticker == "LONG1":
                price = 100.0 + (10.0 * i / 29)  # long1 gaining +10%
            elif ticker == "LONG2":
                price = 100.0 + (20.0 * i / 29)  # long2 gaining +20% (best performer)
            elif ticker == "LONG3":
                price = 100.0 + (5.0 * i / 29)  # long3 gaining +5%
            else:  # SHORT1, SHORT2
                price = 100.0 + (20.0 * i / 29)  # shorts gaining +20% (hit stop-loss)

            raw_equity_bars.append(
                {"ticker": ticker, "timestamp": timestamp, "close_price": price}
            )

    equity_bars = add_equity_bars_returns_and_realized_volatility_columns(
        pl.DataFrame(raw_equity_bars)
    )
    current_timestamp = datetime.fromtimestamp(base_timestamp + (29 * 86400), tz=UTC)

    result = add_portfolio_performance_columns(
        positions, predictions, equity_bars, current_timestamp
    )

    # 2 shorts should be closed (hit stop-loss at +20% > +15%)
    closed_shorts = result.filter(
        (pl.col("side") == "SHORT") & (pl.col("action") == "CLOSE_POSITION")
    )
    assert closed_shorts.height == 2  # noqa: PLR2004

    # 2 longs should also be closed for rebalancing (best performers)
    closed_longs = result.filter(
        (pl.col("side") == "LONG") & (pl.col("action") == "CLOSE_POSITION")
    )
    assert closed_longs.height == 2  # noqa: PLR2004

    # Verify that best-performing longs were closed
    closed_long_tickers = closed_longs["ticker"].to_list()
    assert "LONG2" in closed_long_tickers  # best performer (+20%)
    assert "LONG1" in closed_long_tickers  # second best (+10%)


def test_add_portfolio_performance_columns_no_rebalancing_when_equal_closures() -> None:
    """Test that no additional rebalancing occurs when equal positions are closed."""
    base_timestamp = datetime(2024, 1, 10, tzinfo=UTC).timestamp()

    positions = pl.DataFrame(
        {
            "ticker": ["LONG1", "LONG2", "SHORT1", "SHORT2"],
            "timestamp": [base_timestamp] * 4,
            "side": ["LONG", "LONG", "SHORT", "SHORT"],
            "dollar_amount": [1000.0] * 4,
            "action": ["UNSPECIFIED"] * 4,
        }
    )

    predictions = pl.DataFrame(
        {
            "ticker": ["LONG1", "LONG2", "SHORT1", "SHORT2"],
            "timestamp": [base_timestamp] * 4,
            "quantile_10": [-0.05] * 4,
            "quantile_90": [0.15] * 4,
        }
    )

    raw_equity_bars = []
    for ticker in ["LONG1", "LONG2", "SHORT1", "SHORT2"]:
        for i in range(30):
            timestamp = base_timestamp + (i * 86400)
            if ticker in ["LONG1", "LONG2"]:
                price = 100.0 - (10.0 * i / 29)  # longs losing -10%
            else:  # SHORT1, SHORT2
                price = 100.0 + (20.0 * i / 29)  # shorts gaining +20%

            raw_equity_bars.append(
                {"ticker": ticker, "timestamp": timestamp, "close_price": price}
            )

    equity_bars = add_equity_bars_returns_and_realized_volatility_columns(
        pl.DataFrame(raw_equity_bars)
    )
    current_timestamp = datetime.fromtimestamp(base_timestamp + (29 * 86400), tz=UTC)

    result = add_portfolio_performance_columns(
        positions, predictions, equity_bars, current_timestamp
    )

    # 2 longs and 2 shorts should be closed (no rebalancing needed)
    closed_longs = result.filter(
        (pl.col("side") == "LONG") & (pl.col("action") == "CLOSE_POSITION")
    ).height
    closed_shorts = result.filter(
        (pl.col("side") == "SHORT") & (pl.col("action") == "CLOSE_POSITION")
    ).height

    assert closed_longs == 2  # noqa: PLR2004
    assert closed_shorts == 2  # noqa: PLR2004


def test_add_portfolio_performance_columns_rebalancing_respects_pdt_locked_positions() -> (  # noqa: E501
    None
):
    """Test that rebalancing does not close PDT-locked positions."""
    base_timestamp = datetime(2024, 1, 10, tzinfo=UTC).timestamp()

    positions = pl.DataFrame(
        {
            "ticker": ["LONG1", "LONG2", "SHORT1", "SHORT2", "SHORT3"],
            "timestamp": [base_timestamp] * 5,
            "side": ["LONG", "LONG", "SHORT", "SHORT", "SHORT"],
            "dollar_amount": [1000.0] * 5,
            "action": [
                "UNSPECIFIED",
                "UNSPECIFIED",
                "PDT_LOCKED",
                "UNSPECIFIED",
                "UNSPECIFIED",
            ],
        }
    )

    predictions = pl.DataFrame(
        {
            "ticker": ["LONG1", "LONG2", "SHORT1", "SHORT2", "SHORT3"],
            "timestamp": [base_timestamp] * 5,
            "quantile_10": [-0.05] * 5,
            "quantile_90": [0.15] * 5,
        }
    )

    raw_equity_bars = []
    for ticker in ["LONG1", "LONG2", "SHORT1", "SHORT2", "SHORT3"]:
        for i in range(30):
            timestamp = base_timestamp + (i * 86400)
            if ticker in ["LONG1", "LONG2"]:
                price = 100.0 - (10.0 * i / 29)  # longs losing -10%
            elif ticker == "SHORT1":
                price = 100.0 - (10.0 * i / 29)  # short1 -10% (best, but PDT locked)
            elif ticker == "SHORT2":
                price = 100.0 - (5.0 * i / 29)  # short2 -5% (second best)
            else:  # SHORT3
                price = 100.0 - (2.0 * i / 29)  # short3 -2% (worst)

            raw_equity_bars.append(
                {"ticker": ticker, "timestamp": timestamp, "close_price": price}
            )

    equity_bars = add_equity_bars_returns_and_realized_volatility_columns(
        pl.DataFrame(raw_equity_bars)
    )
    current_timestamp = datetime.fromtimestamp(base_timestamp + (29 * 86400), tz=UTC)

    result = add_portfolio_performance_columns(
        positions, predictions, equity_bars, current_timestamp
    )

    # 2 longs should be closed (both hit stop-loss at -10% < -5%)
    closed_longs = result.filter(
        (pl.col("side") == "LONG") & (pl.col("action") == "CLOSE_POSITION")
    )
    assert closed_longs.height == 2  # noqa: PLR2004

    # 2 shorts should be closed for rebalancing (not SHORT1 since it's PDT locked)
    # Rebalancing closes SHORT2 and SHORT3 to match the 2 closed longs
    closed_shorts = result.filter(
        (pl.col("side") == "SHORT") & (pl.col("action") == "CLOSE_POSITION")
    )
    assert closed_shorts.height == 2  # noqa: PLR2004

    # SHORT1 should remain PDT_LOCKED, not be closed
    short1_action = result.filter(pl.col("ticker") == "SHORT1")["action"][0]
    assert short1_action == "PDT_LOCKED"

    # SHORT2 and SHORT3 should be closed (SHORT1 skipped due to PDT_LOCKED)
    closed_short_tickers = closed_shorts["ticker"].to_list()
    assert "SHORT2" in closed_short_tickers
    assert "SHORT3" in closed_short_tickers
