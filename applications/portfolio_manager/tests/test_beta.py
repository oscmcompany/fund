import numpy as np
import polars as pl
import pytest
from portfolio_manager.beta import compute_market_betas, compute_portfolio_beta


def _make_spy_prices(
    log_returns: list[float], base_price: float = 100.0
) -> pl.DataFrame:
    prices = [base_price]
    for log_return in log_returns:
        prices.append(prices[-1] * np.exp(log_return))
    return pl.DataFrame(
        {
            "ticker": ["SPY"] * len(prices),
            "timestamp": list(range(len(prices))),
            "close_price": prices,
        }
    )


def _make_historical_prices(
    ticker_log_returns: dict[str, list[float]],
    base_price: float = 100.0,
) -> pl.DataFrame:
    rows = []
    for ticker, log_returns in ticker_log_returns.items():
        prices = [base_price]
        for log_return in log_returns:
            prices.append(prices[-1] * np.exp(log_return))
        for i, price in enumerate(prices):
            rows.append({"ticker": ticker, "timestamp": i, "close_price": price})
    return pl.DataFrame(rows)


def test_compute_market_betas_returns_expected_columns() -> None:
    spy_log_returns = [0.01 * ((-1) ** i) for i in range(61)]
    spy_prices = _make_spy_prices(spy_log_returns)
    historical_prices = _make_historical_prices({"AAPL": spy_log_returns})

    result = compute_market_betas(historical_prices, spy_prices)

    assert result.columns == ["ticker", "market_beta"]
    assert result.height == 1


def test_compute_market_betas_returns_correct_beta_for_known_data() -> None:
    # Deterministic log returns with clear relationships
    spy_log_returns = [0.01 * np.sin(2 * np.pi * i / 10) for i in range(61)]
    ticker_a_log_returns = spy_log_returns  # beta ≈ 1.0
    ticker_b_log_returns = [2.0 * r for r in spy_log_returns]  # beta ≈ 2.0

    spy_prices = _make_spy_prices(spy_log_returns)
    historical_prices = _make_historical_prices(
        {"AAPL": ticker_a_log_returns, "MSFT": ticker_b_log_returns}
    )

    result = compute_market_betas(historical_prices, spy_prices)
    betas = dict(
        zip(result["ticker"].to_list(), result["market_beta"].to_list(), strict=False)
    )

    assert betas["AAPL"] == pytest.approx(1.0, abs=0.01)
    assert betas["MSFT"] == pytest.approx(2.0, abs=0.01)


def test_compute_market_betas_drops_tickers_with_insufficient_data() -> None:
    spy_log_returns = [0.01 * ((-1) ** i) for i in range(61)]
    spy_prices = _make_spy_prices(spy_log_returns)

    # AAPL has enough data; MSFT has only 1 price (0 returns)
    historical_prices = pl.DataFrame(
        [
            *[
                {"ticker": "AAPL", "timestamp": i, "close_price": 100.0 + i}
                for i in range(62)
            ],
            {"ticker": "MSFT", "timestamp": 0, "close_price": 200.0},
        ]
    )

    result = compute_market_betas(historical_prices, spy_prices)
    tickers = result["ticker"].to_list()

    assert "AAPL" in tickers
    assert "MSFT" not in tickers


def test_compute_market_betas_returns_empty_dataframe_for_insufficient_spy_data() -> (
    None
):
    spy_prices = pl.DataFrame(
        {"ticker": ["SPY"], "timestamp": [0], "close_price": [100.0]}
    )
    historical_prices = _make_historical_prices({"AAPL": [0.01, 0.02, -0.01, 0.03]})

    result = compute_market_betas(historical_prices, spy_prices)

    assert result.is_empty()
    assert result.columns == ["ticker", "market_beta"]


def test_compute_portfolio_beta_returns_zero_for_balanced_portfolio() -> None:
    portfolio = pl.DataFrame(
        {
            "ticker": ["AAPL", "MSFT"],
            "side": ["LONG", "SHORT"],
            "dollar_amount": [1000.0, 1000.0],
        }
    )
    market_betas = pl.DataFrame({"ticker": ["AAPL", "MSFT"], "market_beta": [1.5, 1.5]})

    result = compute_portfolio_beta(portfolio, market_betas)

    assert result == pytest.approx(0.0, abs=1e-10)


def test_compute_portfolio_beta_returns_positive_for_long_high_beta() -> None:
    portfolio = pl.DataFrame(
        {
            "ticker": ["AAPL", "MSFT"],
            "side": ["LONG", "SHORT"],
            "dollar_amount": [1000.0, 1000.0],
        }
    )
    market_betas = pl.DataFrame({"ticker": ["AAPL", "MSFT"], "market_beta": [2.0, 0.5]})

    result = compute_portfolio_beta(portfolio, market_betas)

    # net = +1*(1000/2000)*2.0 + -1*(1000/2000)*0.5 = 0.5*(2.0-0.5) = 0.75
    assert result == pytest.approx(0.75, abs=1e-10)


def test_compute_portfolio_beta_returns_zero_for_zero_gross_exposure() -> None:
    portfolio = pl.DataFrame(
        {
            "ticker": ["AAPL", "MSFT"],
            "side": ["LONG", "SHORT"],
            "dollar_amount": [0.0, 0.0],
        }
    )
    market_betas = pl.DataFrame({"ticker": ["AAPL", "MSFT"], "market_beta": [1.5, 1.5]})

    result = compute_portfolio_beta(portfolio, market_betas)

    assert result == 0.0


def test_compute_portfolio_beta_uses_zero_for_missing_tickers() -> None:
    portfolio = pl.DataFrame(
        {
            "ticker": ["AAPL", "MSFT"],
            "side": ["LONG", "SHORT"],
            "dollar_amount": [1000.0, 1000.0],
        }
    )
    market_betas = pl.DataFrame({"ticker": ["AAPL"], "market_beta": [2.0]})

    result = compute_portfolio_beta(portfolio, market_betas)

    # MSFT has no beta → treated as 0.0
    # net = +1*(1000/2000)*2.0 + -1*(1000/2000)*0.0 = 1.0
    assert result == pytest.approx(1.0, abs=1e-10)
