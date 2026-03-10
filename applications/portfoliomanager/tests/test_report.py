import polars as pl
from portfoliomanager.report import (
    _SEPARATOR,
    _W_TICKER,
    format_beta_report,
    format_consolidation_report,
    format_pairs_report,
    format_portfolio_report,
    format_regime_report,
)


def _make_market_betas(tickers: list[str], betas: list[float]) -> pl.DataFrame:
    return pl.DataFrame({"ticker": tickers, "market_beta": betas})


def _make_signals(
    tickers: list[str],
    alphas: list[float],
    confidences: list[float],
) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "ticker": tickers,
            "ensemble_alpha": alphas,
            "ensemble_confidence": confidences,
            "realized_volatility": [0.01] * len(tickers),
            "sector": ["Tech"] * len(tickers),
        }
    )


def _make_pairs(
    long_ticker: str = "AAPL",
    short_ticker: str = "MSFT",
    z_score: float = 2.5,
    signal_strength: float = 0.10,
    hedge_ratio: float = 0.8,
) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "pair_id": [f"{long_ticker}-{short_ticker}"],
            "long_ticker": [long_ticker],
            "short_ticker": [short_ticker],
            "z_score": [z_score],
            "signal_strength": [signal_strength],
            "hedge_ratio": [hedge_ratio],
            "long_realized_volatility": [0.01],
            "short_realized_volatility": [0.01],
        }
    )


def _make_portfolio(
    pair_id: str = "AAPL-MSFT",
    long_ticker: str = "AAPL",
    short_ticker: str = "MSFT",
    dollar_amount: float = 5000.0,
) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "ticker": [long_ticker, short_ticker],
            "timestamp": [0.0, 0.0],
            "side": ["LONG", "SHORT"],
            "dollar_amount": [dollar_amount, dollar_amount],
            "action": ["open_position", "open_position"],
            "pair_id": [pair_id, pair_id],
        }
    )


# --- format_regime_report ---


def test_format_regime_report_contains_state() -> None:
    result = format_regime_report(
        {"state": "mean_reversion", "confidence": 0.75}, exposure_scale=1.0
    )
    assert "mean_reversion" in result


def test_format_regime_report_contains_confidence() -> None:
    result = format_regime_report(
        {"state": "trending", "confidence": 0.42}, exposure_scale=0.5
    )
    assert "0.420" in result


def test_format_regime_report_contains_exposure_scale() -> None:
    result = format_regime_report(
        {"state": "mean_reversion", "confidence": 0.5}, exposure_scale=0.5
    )
    assert "0.5x" in result


def test_format_regime_report_contains_separator() -> None:
    result = format_regime_report({"state": "trending", "confidence": 0.1}, 1.0)
    assert _SEPARATOR in result


# --- format_beta_report ---


def test_format_beta_report_contains_ticker_count() -> None:
    betas = _make_market_betas(["AAPL", "MSFT"], [1.1, 0.9])
    result = format_beta_report(betas)
    assert "2" in result


def test_format_beta_report_empty_returns_no_data_message() -> None:
    result = format_beta_report(
        pl.DataFrame(schema={"ticker": pl.String, "market_beta": pl.Float64})
    )
    assert "No betas computed" in result


def test_format_beta_report_shows_highest_and_lowest() -> None:
    betas = _make_market_betas(["AAPL", "MSFT", "TSLA"], [1.1, 0.9, 2.5])
    result = format_beta_report(betas)
    assert "Highest" in result
    assert "Lowest" in result


# --- format_consolidation_report ---


def test_format_consolidation_report_contains_signal_count() -> None:
    signals = _make_signals(["AAPL", "MSFT"], [0.05, -0.03], [0.8, 0.6])
    result = format_consolidation_report(signals, input_ticker_count=2)
    assert "Signals computed:         2" in result


def test_format_consolidation_report_uses_input_ticker_count() -> None:
    signals = _make_signals(["AAPL"], [0.05], [0.8])
    result = format_consolidation_report(signals, input_ticker_count=50)
    assert "50" in result


def test_format_consolidation_report_high_confidence_count() -> None:
    signals = _make_signals(["AAPL", "MSFT"], [0.05, -0.03], [0.9, 0.3])
    result = format_consolidation_report(signals, input_ticker_count=2)
    # Only AAPL has confidence >= 0.5
    assert "High confidence (>=0.5):  1" in result


# --- format_pairs_report ---


def test_format_pairs_report_empty_returns_no_pairs_message() -> None:
    result = format_pairs_report(
        pl.DataFrame(
            schema={
                "pair_id": pl.String,
                "long_ticker": pl.String,
                "short_ticker": pl.String,
                "z_score": pl.Float64,
                "signal_strength": pl.Float64,
                "hedge_ratio": pl.Float64,
                "long_realized_volatility": pl.Float64,
                "short_realized_volatility": pl.Float64,
            }
        )
    )
    assert "No qualifying pairs found" in result


def test_format_pairs_report_contains_pair_id() -> None:
    pairs = _make_pairs()
    result = format_pairs_report(pairs)
    assert "AAPL-MSFT" in result


def test_format_pairs_report_header_aligns_with_separator() -> None:
    pairs = _make_pairs()
    lines = format_pairs_report(pairs).splitlines()
    header_line = next(line for line in lines if "long_ticker" in line)
    # The column separator row starts with two spaces followed by dashes and spaces
    # (distinct from the full-width _SEPARATOR which has no leading spaces)
    separator_line = next(
        line for line in lines if line.startswith("  -") and "long_ticker" not in line
    )
    assert header_line.startswith("  ")
    assert separator_line.startswith("  ")


def test_format_pairs_report_column_widths_fit_headers() -> None:
    # "long_ticker" is 11 chars; column must be at least that wide
    assert len("long_ticker") <= _W_TICKER
    # "short_ticker" is 12 chars
    assert len("short_ticker") <= _W_TICKER


# --- format_portfolio_report ---


def test_format_portfolio_report_shows_capital() -> None:
    portfolio = _make_portfolio()
    pairs = _make_pairs()
    betas = _make_market_betas(["AAPL", "MSFT"], [1.0, 1.0])
    result = format_portfolio_report(portfolio, pairs, betas, 100_000.0, 1.0)
    assert "$100,000" in result


def test_format_portfolio_report_shows_zero_imbalance() -> None:
    portfolio = _make_portfolio(dollar_amount=5000.0)
    pairs = _make_pairs()
    betas = _make_market_betas(["AAPL", "MSFT"], [1.0, 1.0])
    result = format_portfolio_report(portfolio, pairs, betas, 10_000.0, 1.0)
    assert "0.00%" in result


def test_format_portfolio_report_shows_pair_id() -> None:
    portfolio = _make_portfolio(pair_id="AAPL-MSFT")
    pairs = _make_pairs()
    betas = _make_market_betas(["AAPL", "MSFT"], [1.0, 1.0])
    result = format_portfolio_report(portfolio, pairs, betas, 10_000.0, 1.0)
    assert "AAPL-MSFT" in result


def test_format_portfolio_report_shows_long_and_short_labels() -> None:
    portfolio = _make_portfolio()
    pairs = _make_pairs()
    betas = _make_market_betas(["AAPL", "MSFT"], [1.0, 1.0])
    result = format_portfolio_report(portfolio, pairs, betas, 10_000.0, 1.0)
    assert "LONG" in result
    assert "SHORT" in result
