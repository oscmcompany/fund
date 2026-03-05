from datetime import date, timedelta

import polars as pl
import pytest
from portfoliomanager.consolidation import consolidate_predictions


def _make_historical_prices(tickers: list[str], days: int = 25) -> pl.DataFrame:
    base_date = date(2024, 1, 1)
    rows = [
        {
            "ticker": ticker,
            "timestamp": (base_date + timedelta(days=day)).isoformat(),
            "close_price": 100.0 + day,
        }
        for ticker in tickers
        for day in range(days)
    ]
    return pl.DataFrame(rows)


def _make_predictions(
    tickers: list[str],
    q10: float = 0.0,
    q50: float = 0.1,
    q90: float = 0.2,
) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "ticker": tickers,
            "timestamp": ["2024-01-25"] * len(tickers),
            "quantile_10": [q10] * len(tickers),
            "quantile_50": [q50] * len(tickers),
            "quantile_90": [q90] * len(tickers),
        }
    )


def _make_equity_details(
    tickers: list[str], sector: str = "Technology"
) -> pl.DataFrame:
    return pl.DataFrame({"ticker": tickers, "sector": [sector] * len(tickers)})


def test_consolidate_predictions_single_model_output_columns() -> None:
    tickers = ["AAPL", "MSFT"]
    result = consolidate_predictions(
        model_predictions={"tide": _make_predictions(tickers)},
        historical_prices=_make_historical_prices(tickers),
        equity_details=_make_equity_details(tickers),
    )

    assert result.columns == [
        "ticker",
        "ensemble_alpha",
        "ensemble_confidence",
        "realized_volatility",
        "sector",
    ]
    assert result.height == len(tickers)


def test_consolidate_predictions_confidence_normalized_to_one() -> None:
    tickers = ["AAPL", "MSFT", "GOOG"]
    predictions = pl.DataFrame(
        {
            "ticker": tickers,
            "timestamp": ["2024-01-25"] * 3,
            "quantile_10": [0.0, 0.05, 0.10],
            "quantile_50": [0.10, 0.15, 0.20],
            "quantile_90": [0.20, 0.30, 0.50],  # IQRs: 0.20, 0.25, 0.40
        }
    )

    result = consolidate_predictions(
        model_predictions={"tide": predictions},
        historical_prices=_make_historical_prices(tickers),
        equity_details=_make_equity_details(tickers),
    )

    assert result["ensemble_confidence"].max() == pytest.approx(1.0)
    assert all(value > 0 for value in result["ensemble_confidence"].to_list())


def test_consolidate_predictions_two_models_blended_alpha_is_arithmetic_mean() -> None:
    tickers = ["AAPL", "MSFT"]
    predictions_a = pl.DataFrame(
        {
            "ticker": tickers,
            "timestamp": ["2024-01-25"] * 2,
            "quantile_10": [0.0, 0.0],
            "quantile_50": [0.10, 0.20],
            "quantile_90": [0.05, 0.05],
        }
    )
    predictions_b = pl.DataFrame(
        {
            "ticker": tickers,
            "timestamp": ["2024-01-25"] * 2,
            "quantile_10": [0.0, 0.0],
            "quantile_50": [0.20, 0.30],
            "quantile_90": [0.05, 0.05],
        }
    )

    result = consolidate_predictions(
        model_predictions={"model_a": predictions_a, "model_b": predictions_b},
        historical_prices=_make_historical_prices(tickers),
        equity_details=_make_equity_details(tickers),
    )

    result_sorted = result.sort("ticker")
    aapl_alpha = result_sorted.filter(pl.col("ticker") == "AAPL")[
        "ensemble_alpha"
    ].item()
    msft_alpha = result_sorted.filter(pl.col("ticker") == "MSFT")[
        "ensemble_alpha"
    ].item()

    assert aapl_alpha == pytest.approx((0.10 + 0.20) / 2)
    assert msft_alpha == pytest.approx((0.20 + 0.30) / 2)


def test_consolidate_predictions_drops_tickers_with_no_price_history() -> None:
    tickers = ["AAPL", "MSFT"]

    result = consolidate_predictions(
        model_predictions={"tide": _make_predictions(tickers)},
        historical_prices=_make_historical_prices(["AAPL"]),  # MSFT has no history
        equity_details=_make_equity_details(tickers),
    )

    assert result.height == 1
    assert result["ticker"][0] == "AAPL"


def test_consolidate_predictions_fills_missing_sector_with_not_available() -> None:
    tickers = ["AAPL", "MSFT"]

    result = consolidate_predictions(
        model_predictions={"tide": _make_predictions(tickers)},
        historical_prices=_make_historical_prices(tickers),
        equity_details=_make_equity_details(["AAPL"]),  # MSFT has no sector
    )

    result_sorted = result.sort("ticker")
    aapl_sector = result_sorted.filter(pl.col("ticker") == "AAPL")["sector"].item()
    msft_sector = result_sorted.filter(pl.col("ticker") == "MSFT")["sector"].item()

    assert aapl_sector == "Technology"
    assert msft_sector == "NOT AVAILABLE"


def test_consolidate_predictions_raises_on_empty_model_dict() -> None:
    with pytest.raises(ValueError, match="must not be empty"):
        consolidate_predictions(
            model_predictions={},
            historical_prices=pl.DataFrame(),
            equity_details=pl.DataFrame(),
        )


def test_consolidate_predictions_raises_on_missing_required_columns() -> None:
    bad_predictions = pl.DataFrame(
        {
            "ticker": ["AAPL"],
            "timestamp": ["2024-01-25"],
            "quantile_10": [0.0],
            # quantile_50 and quantile_90 missing
        }
    )

    with pytest.raises(ValueError, match="missing required columns"):
        consolidate_predictions(
            model_predictions={"tide": bad_predictions},
            historical_prices=pl.DataFrame(),
            equity_details=pl.DataFrame(),
        )
