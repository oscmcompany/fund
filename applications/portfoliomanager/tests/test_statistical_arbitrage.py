import numpy as np
import polars as pl
from portfoliomanager.statistical_arbitrage import (
    _PAIRS_OUTPUT_SCHEMA,
    CONFIDENCE_THRESHOLD,
    CORRELATION_WINDOW_DAYS,
    Z_SCORE_ENTRY_THRESHOLD,
    build_price_matrix,
    compute_spread_zscore,
    select_pairs,
)


def _make_cointegrated_prices(
    rng: np.random.Generator,
    n: int = CORRELATION_WINDOW_DAYS + 5,
    entry_deviation: float = 0.15,
) -> tuple[np.ndarray, np.ndarray]:
    """Return (prices_a, prices_b) where A is pushed expensive at the end."""
    common = np.cumsum(rng.normal(0, 0.01, n))
    idio_a = np.cumsum(rng.normal(0, 0.005, n))
    idio_b = np.cumsum(rng.normal(0, 0.005, n))
    log_a = np.log(100.0) + common + idio_a
    log_b = np.log(50.0) + common + idio_b
    log_a[-1] += entry_deviation
    return np.exp(log_a), np.exp(log_b)


def _make_historical_prices(
    tickers: list[str],
    price_arrays: list[np.ndarray],
    n: int,
) -> pl.DataFrame:
    rows = [
        {
            "ticker": ticker,
            "timestamp": float(t),
            "close_price": float(price_arrays[i][t]),
        }
        for i, ticker in enumerate(tickers)
        for t in range(n)
    ]
    return pl.DataFrame(rows)


def _make_signals(
    tickers: list[str],
    alphas: list[float] | None = None,
    confidences: list[float] | None = None,
    volatilities: list[float] | None = None,
) -> pl.DataFrame:
    count = len(tickers)
    return pl.DataFrame(
        {
            "ticker": tickers,
            "ensemble_alpha": alphas or [0.5] * count,
            "ensemble_confidence": confidences or [0.8] * count,
            "realized_volatility": volatilities or [0.01] * count,
        }
    )


def test_build_price_matrix_correct_shape_and_columns() -> None:
    rng = np.random.default_rng(42)
    n = CORRELATION_WINDOW_DAYS + 5
    tickers = ["AAPL", "MSFT"]
    prices_a, prices_b = _make_cointegrated_prices(rng, n=n)
    historical = _make_historical_prices(tickers, [prices_a, prices_b], n)

    result = build_price_matrix(historical, tickers)

    assert "timestamp" in result.columns
    assert "AAPL" in result.columns
    assert "MSFT" in result.columns
    assert result.height == n


def test_build_price_matrix_excludes_tickers_with_insufficient_history() -> None:
    rng = np.random.default_rng(42)
    n = CORRELATION_WINDOW_DAYS + 5
    prices_a, _ = _make_cointegrated_prices(rng, n=n)
    short_prices = np.exp(np.cumsum(rng.normal(0, 0.01, 10))) * 50.0

    rows = [
        {"ticker": "AAPL", "timestamp": float(t), "close_price": float(prices_a[t])}
        for t in range(n)
    ] + [
        {"ticker": "MSFT", "timestamp": float(t), "close_price": float(short_prices[t])}
        for t in range(10)
    ]
    historical = pl.DataFrame(rows)

    result = build_price_matrix(historical, ["AAPL", "MSFT"])

    assert "AAPL" in result.columns
    assert "MSFT" not in result.columns


def test_compute_spread_zscore_z_positive_when_a_is_expensive() -> None:
    n = CORRELATION_WINDOW_DAYS + 5
    # Deterministic: A above B with small oscillation, pushed further at end
    log_b = np.linspace(3.9, 4.0, n)
    log_a = log_b + 0.05 * np.sin(np.arange(n, dtype=float) * 0.3) + 0.2
    log_a[-1] += 0.5  # push A expensive at end

    z_score, _ = compute_spread_zscore(log_a, log_b)

    assert z_score > Z_SCORE_ENTRY_THRESHOLD
    assert z_score > 0  # A is expensive → z > 0 → code assigns long=B, short=A


def test_select_pairs_output_columns_match_schema() -> None:
    rng = np.random.default_rng(1)
    n = CORRELATION_WINDOW_DAYS + 5
    tickers = ["AAPL", "MSFT"]
    prices_a, prices_b = _make_cointegrated_prices(rng, n=n)
    historical = _make_historical_prices(tickers, [prices_a, prices_b], n)
    signals = _make_signals(tickers)

    result = select_pairs(signals, historical)

    assert list(result.columns) == list(_PAIRS_OUTPUT_SCHEMA.keys())


def test_select_pairs_no_ticker_in_more_than_one_pair() -> None:
    rng = np.random.default_rng(2)
    n = CORRELATION_WINDOW_DAYS + 5
    # Three independent cointegrated pairs: (A,B), (C,D), (E,F)
    tickers = ["A", "B", "C", "D", "E", "F"]
    price_arrays: list[np.ndarray] = []
    for _ in range(3):
        pa, pb = _make_cointegrated_prices(rng, n=n)
        price_arrays.extend([pa, pb])
    historical = _make_historical_prices(tickers, price_arrays, n)
    signals = _make_signals(tickers)

    result = select_pairs(signals, historical)

    all_used = result["long_ticker"].to_list() + result["short_ticker"].to_list()
    assert len(all_used) == len(set(all_used))


def test_select_pairs_excludes_tickers_below_confidence_threshold() -> None:
    rng = np.random.default_rng(3)
    n = CORRELATION_WINDOW_DAYS + 5
    prices_a, prices_b = _make_cointegrated_prices(rng, n=n)
    tickers = ["AAPL", "MSFT"]
    historical = _make_historical_prices(tickers, [prices_a, prices_b], n)
    # Both tickers below threshold → filtered_signals.height < 2
    signals = _make_signals(
        tickers,
        confidences=[CONFIDENCE_THRESHOLD - 0.1, CONFIDENCE_THRESHOLD - 0.1],
    )

    result = select_pairs(signals, historical)

    assert result.height == 0
    assert list(result.columns) == list(_PAIRS_OUTPUT_SCHEMA.keys())


def test_select_pairs_returns_empty_dataframe_when_only_one_eligible_ticker() -> None:
    rng = np.random.default_rng(4)
    n = CORRELATION_WINDOW_DAYS + 5
    prices_a, _ = _make_cointegrated_prices(rng, n=n)
    historical = _make_historical_prices(["AAPL"], [prices_a], n)
    signals = _make_signals(["AAPL"])

    result = select_pairs(signals, historical)

    assert result.height == 0
    assert list(result.columns) == list(_PAIRS_OUTPUT_SCHEMA.keys())


def test_select_pairs_pair_id_matches_long_short_tickers() -> None:
    rng = np.random.default_rng(5)
    n = CORRELATION_WINDOW_DAYS + 5
    tickers = ["AAPL", "MSFT"]
    prices_a, prices_b = _make_cointegrated_prices(rng, n=n)
    historical = _make_historical_prices(tickers, [prices_a, prices_b], n)
    signals = _make_signals(tickers)

    result = select_pairs(signals, historical)

    for row in result.iter_rows(named=True):
        assert row["pair_id"] == f"{row['long_ticker']}-{row['short_ticker']}"


def test_select_pairs_result_never_exceeds_target_pair_count() -> None:
    rng = np.random.default_rng(6)
    n = CORRELATION_WINDOW_DAYS + 5
    tickers = ["T0", "T1", "T2", "T3", "T4", "T5"]
    price_arrays: list[np.ndarray] = []
    for _ in range(3):
        pa, pb = _make_cointegrated_prices(rng, n=n)
        price_arrays.extend([pa, pb])
    historical = _make_historical_prices(tickers, price_arrays, n)
    signals = _make_signals(tickers)

    target = 2
    result = select_pairs(signals, historical, target_pair_count=target)

    assert result.height <= target


def test_compute_spread_zscore_z_negative_when_a_is_cheap() -> None:
    n = CORRELATION_WINDOW_DAYS + 5
    # A below B, pushed further down → spread (a - slope*b) negative → z < 0
    log_b = np.linspace(3.9, 4.0, n)
    log_a = log_b + 0.05 * np.sin(np.arange(n, dtype=float) * 0.3) + 0.2
    log_a[-1] -= 0.7  # push A down at end (B unchanged, no leverage distortion)

    z_score, _ = compute_spread_zscore(log_a, log_b)

    assert abs(z_score) > Z_SCORE_ENTRY_THRESHOLD
    assert (
        z_score < 0
    )  # A is cheap relative to B → z < 0 → code assigns long=A, short=B


def test_compute_log_returns_excludes_tickers_with_zero_price() -> None:
    rng = np.random.default_rng(7)
    n = CORRELATION_WINDOW_DAYS + 5

    # AAPL: valid prices around 100 for all n rows
    aapl_prices = 100.0 + rng.normal(0, 1.0, n)

    # ZERO: mostly valid prices, but one row within the 60-day window is zero
    zero_prices = 80.0 + rng.normal(0, 1.0, n)
    zero_prices[-1] = 0.0  # zero within the tail(CORRELATION_WINDOW_DAYS) window

    rows = [
        {"ticker": "AAPL", "timestamp": float(t), "close_price": float(aapl_prices[t])}
        for t in range(n)
    ] + [
        {"ticker": "ZERO", "timestamp": float(t), "close_price": float(zero_prices[t])}
        for t in range(n)
    ]
    historical = pl.DataFrame(rows)
    signals = _make_signals(["AAPL", "ZERO"])

    result = select_pairs(signals, historical)

    # ZERO is excluded by the <= 0 guard in _compute_log_returns, leaving only AAPL.
    # With fewer than _MINIMUM_TICKER_COUNT valid tickers, select_pairs returns empty.
    assert result.height == 0
    assert list(result.columns) == list(_PAIRS_OUTPUT_SCHEMA.keys())
