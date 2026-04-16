import numpy as np
import polars as pl
import scipy.stats

Z_SCORE_ENTRY_THRESHOLD = 2.0
CORRELATION_MINIMUM = 0.5
CORRELATION_MAXIMUM = 0.95
CONFIDENCE_THRESHOLD = 0.5
CORRELATION_WINDOW_DAYS = 60
TARGET_PAIR_COUNT = 10
_MINIMUM_TICKER_COUNT = 2

_PAIRS_OUTPUT_SCHEMA: dict[str, type] = {
    # Human-readable identifier combining both tickers (e.g. "AAPL-MSFT")
    "pair_id": pl.String,
    # The leg to buy: the ticker whose price is relatively cheap vs its historical
    # spread
    "long_ticker": pl.String,
    # The leg to sell short: the ticker whose price is relatively expensive vs the
    # spread
    "short_ticker": pl.String,
    # Standard deviations the current spread has diverged from its mean; higher values
    # indicate a more stretched and potentially higher-conviction mean-reversion
    # opportunity
    "z_score": pl.Float64,
    # Cointegration regression slope: shares of the short leg per share of the long leg
    # needed to neutralize the spread; negative values indicate an inverse relationship
    "hedge_ratio": pl.Float64,
    # Absolute difference in ensemble_alpha between the two legs; measures how strongly
    # the model disagrees on their relative forward returns
    "signal_strength": pl.Float64,
    "long_realized_volatility": pl.Float64,
    "short_realized_volatility": pl.Float64,
}


def build_price_matrix(
    historical_prices: pl.DataFrame,
    tickers: list[str],
) -> pl.DataFrame:
    price_matrix = (
        historical_prices.filter(pl.col("ticker").is_in(tickers))
        .pivot(on="ticker", index="timestamp", values="close_price")
        .sort("timestamp")
    )
    ticker_columns = [col for col in price_matrix.columns if col != "timestamp"]
    valid_columns = ["timestamp"] + [
        col
        for col in ticker_columns
        if price_matrix[col].tail(CORRELATION_WINDOW_DAYS).drop_nulls().len()
        >= CORRELATION_WINDOW_DAYS
    ]
    return price_matrix.select(valid_columns)


def compute_spread_zscore(
    log_prices_a: np.ndarray,
    log_prices_b: np.ndarray,
) -> tuple[float, float]:
    slope: float = float(np.polyfit(log_prices_b, log_prices_a, 1)[0])
    spread = log_prices_a - slope * log_prices_b
    current_z_score: float = float(scipy.stats.zscore(spread)[-1])
    return current_z_score, slope


def _compute_log_returns(
    window: pl.DataFrame,
    ticker_columns: list[str],
) -> dict[str, np.ndarray]:
    log_returns: dict[str, np.ndarray] = {}
    for col in ticker_columns:
        prices = window[col].to_numpy()
        if (
            np.any(np.isnan(prices))
            or np.any(prices <= 0)
            or len(prices) < _MINIMUM_TICKER_COUNT
        ):
            continue
        returns = np.diff(np.log(prices))
        if np.isclose(np.std(returns), 0.0):
            continue
        log_returns[col] = returns
    return log_returns


def _build_candidate_pairs(
    valid_tickers: list[str],
    correlation_matrix: np.ndarray,
    window: pl.DataFrame,
    signals_lookup: dict[str, dict[str, float]],
) -> list[dict]:
    candidate_pairs = []
    for i in range(len(valid_tickers)):
        for j in range(i + 1, len(valid_tickers)):
            correlation = correlation_matrix[i, j]
            if not (CORRELATION_MINIMUM <= abs(correlation) <= CORRELATION_MAXIMUM):
                continue

            ticker_a = valid_tickers[i]
            ticker_b = valid_tickers[j]

            log_prices_a = np.log(window[ticker_a].to_numpy())
            log_prices_b = np.log(window[ticker_b].to_numpy())

            current_z_score, hedge_ratio = compute_spread_zscore(
                log_prices_a, log_prices_b
            )

            if (
                np.isnan(current_z_score)
                or np.isnan(hedge_ratio)
                or np.isinf(hedge_ratio)
            ):
                continue

            if abs(current_z_score) < Z_SCORE_ENTRY_THRESHOLD:
                continue

            # z > 0: A is expensive → short A, long B
            if current_z_score > 0:
                long_ticker, short_ticker = ticker_b, ticker_a
            else:
                long_ticker, short_ticker = ticker_a, ticker_b

            signal_strength = abs(
                signals_lookup[long_ticker]["ensemble_alpha"]
                - signals_lookup[short_ticker]["ensemble_alpha"]
            )

            candidate_pairs.append(
                {
                    "pair_id": f"{long_ticker}-{short_ticker}",
                    "long_ticker": long_ticker,
                    "short_ticker": short_ticker,
                    "z_score": abs(current_z_score),
                    "hedge_ratio": hedge_ratio,
                    "signal_strength": signal_strength,
                    "long_realized_volatility": float(
                        signals_lookup[long_ticker]["realized_volatility"]
                    ),
                    "short_realized_volatility": float(
                        signals_lookup[short_ticker]["realized_volatility"]
                    ),
                    "_rank_score": abs(current_z_score) * signal_strength,
                }
            )
    return candidate_pairs


def _select_greedy_pairs(
    pairs_df: pl.DataFrame,
    target_pair_count: int,
) -> list[dict]:
    used_tickers: set[str] = set()
    selected_pairs = []
    for row in pairs_df.iter_rows(named=True):
        if row["long_ticker"] in used_tickers or row["short_ticker"] in used_tickers:
            continue
        used_tickers.add(row["long_ticker"])
        used_tickers.add(row["short_ticker"])
        selected_pairs.append(row)
        if len(selected_pairs) >= target_pair_count:
            break
    return selected_pairs


def select_pairs(
    consolidated_signals: pl.DataFrame,
    historical_prices: pl.DataFrame,
    target_pair_count: int = TARGET_PAIR_COUNT,
) -> pl.DataFrame:
    empty_result = pl.DataFrame(schema=_PAIRS_OUTPUT_SCHEMA)

    filtered_signals = consolidated_signals.filter(
        (pl.col("ensemble_confidence") >= CONFIDENCE_THRESHOLD)
        & (pl.col("realized_volatility") > 0)
    )

    if filtered_signals.height < _MINIMUM_TICKER_COUNT:
        return empty_result

    eligible_tickers = filtered_signals["ticker"].to_list()
    price_matrix = build_price_matrix(historical_prices, eligible_tickers)
    ticker_columns = [col for col in price_matrix.columns if col != "timestamp"]

    if len(ticker_columns) < _MINIMUM_TICKER_COUNT:
        return empty_result

    correlation_window = price_matrix.tail(CORRELATION_WINDOW_DAYS)
    log_returns = _compute_log_returns(correlation_window, ticker_columns)
    valid_tickers = list(log_returns.keys())

    if len(valid_tickers) < _MINIMUM_TICKER_COUNT:
        return empty_result

    returns_matrix = np.array([log_returns[ticker] for ticker in valid_tickers])
    correlation_matrix = np.corrcoef(returns_matrix)

    signals_lookup = {
        row["ticker"]: {
            "ensemble_alpha": row["ensemble_alpha"],
            "realized_volatility": row["realized_volatility"],
        }
        for row in filtered_signals.iter_rows(named=True)
    }

    candidate_pairs = _build_candidate_pairs(
        valid_tickers, correlation_matrix, correlation_window, signals_lookup
    )

    if not candidate_pairs:
        return empty_result

    pairs_df = pl.DataFrame(candidate_pairs).sort("_rank_score", descending=True)
    selected_pairs = _select_greedy_pairs(pairs_df, target_pair_count)

    if not selected_pairs:
        return empty_result

    return pl.DataFrame(selected_pairs).select(list(_PAIRS_OUTPUT_SCHEMA.keys()))
