import polars as pl
from equitypricemodel.preprocess import filter_equity_bars


def test_filter_equity_bars_above_thresholds() -> None:
    data = pl.DataFrame(
        {
            "ticker": ["AAPL", "AAPL", "AAPL"],
            "close_price": [15.0, 20.0, 25.0],
            "volume": [
                1_500_000.0,
                2_000_000.0,
                2_500_000.0,
            ],
        }
    )

    result = filter_equity_bars(data)

    assert len(result) == 3  # noqa: PLR2004 all rows for AAPL returned
    assert result["ticker"].unique().to_list() == ["AAPL"]


def test_filter_equity_bars_below_price_threshold() -> None:
    data = pl.DataFrame(
        {
            "ticker": ["AAPL", "AAPL", "AAPL"],
            "close_price": [5.0, 8.0, 9.0],
            "volume": [
                1_500_000.0,
                2_000_000.0,
                2_500_000.0,
            ],
        }
    )

    result = filter_equity_bars(data)

    assert len(result) == 0


def test_filter_equity_bars_below_volume_threshold() -> None:
    data = pl.DataFrame(
        {
            "ticker": ["AAPL", "AAPL", "AAPL"],
            "close_price": [15.0, 20.0, 25.0],
            "volume": [500_000.0, 600_000.0, 700_000.0],
        }
    )

    result = filter_equity_bars(data)

    assert len(result) == 0


def test_filter_equity_bars_below_both_thresholds() -> None:
    data = pl.DataFrame(
        {
            "ticker": ["AAPL", "AAPL", "AAPL"],
            "close_price": [5.0, 6.0, 7.0],
            "volume": [500_000.0, 600_000.0, 700_000.0],
        }
    )

    result = filter_equity_bars(data)

    assert len(result) == 0


def test_filter_equity_bars_at_exact_thresholds() -> None:
    data = pl.DataFrame(
        {
            "ticker": ["AAPL", "AAPL", "AAPL"],
            "close_price": [
                10.0,
                10.0,
                10.0,
            ],
            "volume": [
                1_000_000.0,
                1_000_000.0,
                1_000_000.0,
            ],
        }
    )

    result = filter_equity_bars(data)

    assert len(result) == 0


def test_filter_equity_bars_just_above_thresholds() -> None:
    data = pl.DataFrame(
        {
            "ticker": ["AAPL", "AAPL", "AAPL"],
            "close_price": [10.01, 10.01, 10.01],
            "volume": [
                1_000_001.0,
                1_000_001.0,
                1_000_001.0,
            ],
        }
    )

    result = filter_equity_bars(data)

    assert len(result) == 3  # noqa: PLR2004 all rows for AAPL returned
    assert result["ticker"].unique().to_list() == ["AAPL"]


def test_filter_equity_bars_empty_dataframe() -> None:
    data = pl.DataFrame(
        {
            "ticker": [],
            "close_price": [],
            "volume": [],
        }
    )

    result = filter_equity_bars(data)

    assert len(result) == 0


def test_filter_equity_bars_single_row() -> None:
    data = pl.DataFrame(
        {
            "ticker": ["AAPL"],
            "close_price": [15.0],
            "volume": [1_500_000.0],
        }
    )

    result = filter_equity_bars(data)

    assert len(result) == 1
    assert result["ticker"][0] == "AAPL"


def test_filter_equity_bars_mixed_values() -> None:
    data = pl.DataFrame(
        {
            "ticker": ["AAPL", "AAPL"],
            "close_price": [5.0, 25.0],
            "volume": [
                500_000.0,
                1_500_000.0,
            ],
        }
    )

    result = filter_equity_bars(data)

    assert len(result) == 0


def test_filter_equity_bars_multiple_tickers() -> None:
    data = pl.DataFrame(
        {
            "ticker": ["AAPL", "AAPL", "AAPL", "GOOGL", "GOOGL", "TSLA", "TSLA"],
            "close_price": [
                15.0,
                20.0,
                25.0,
                5.0,
                6.0,
                12.0,
                18.0,
            ],
            "volume": [
                1_500_000.0,
                2_000_000.0,
                2_500_000.0,
                2_000_000.0,
                3_000_000.0,
                800_000.0,
                900_000.0,
            ],
        }
    )

    result = filter_equity_bars(data)

    assert len(result) == 3  # noqa: PLR2004 all 3 AAPL rows returned
    assert result["ticker"].unique().to_list() == ["AAPL"]


def test_filter_equity_bars_data_immutability() -> None:
    original_data = pl.DataFrame(
        {
            "ticker": ["AAPL", "AAPL", "AAPL"],
            "close_price": [15.0, 20.0, 25.0],
            "volume": [1_500_000.0, 2_000_000.0, 2_500_000.0],
        }
    )

    original_tickers = original_data["ticker"].to_list()
    original_close_prices = original_data["close_price"].to_list()
    original_volumes = original_data["volume"].to_list()

    filter_equity_bars(original_data)

    assert original_data["ticker"].to_list() == original_tickers
    assert original_data["close_price"].to_list() == original_close_prices
    assert original_data["volume"].to_list() == original_volumes
