from datetime import datetime

import numpy as np
import polars as pl
import scipy.optimize
import structlog
from internal.timestamps import to_timestamp_milliseconds

from .enums import PositionAction, PositionSide
from .exceptions import InsufficientPairsError

logger = structlog.get_logger()

# Minimum number of pairs required to construct a diversified portfolio.
REQUIRED_PAIRS = 10
# Z-score magnitude below which a pair is held without action (mean-reversion zone).
Z_SCORE_HOLD_THRESHOLD = 0.5
# Z-score magnitude at which a pair is force-closed regardless of direction
# (spread has diverged beyond the expected mean-reversion window).
Z_SCORE_STOP_LOSS = 4.0
# Relative bounds applied to each pair's weight by the beta-neutral SLSQP optimizer.
# A pair can be weighted as low as 0.5x or as high as 2.0x its equal-allocation share,
# keeping the optimizer anchored near volatility-parity while allowing beta reduction.
BETA_WEIGHT_LOWER_BOUND = 0.5
BETA_WEIGHT_UPPER_BOUND = 2.0


def _apply_beta_neutral_weights(
    pairs: pl.DataFrame,
    market_betas: pl.DataFrame,
    volatility_parity_weights: np.ndarray,
) -> np.ndarray:
    beta_lookup = {
        row["ticker"]: row["market_beta"] for row in market_betas.iter_rows(named=True)
    }

    pair_net_betas = np.array(
        [
            beta_lookup.get(row["long_ticker"], 0.0)
            - beta_lookup.get(row["short_ticker"], 0.0)
            for row in pairs.iter_rows(named=True)
        ]
    )

    def objective(weights: np.ndarray) -> float:
        total = float(np.sum(weights))
        net_beta = float(np.dot(weights, pair_net_betas) / total)
        return net_beta**2

    bounds = [
        (BETA_WEIGHT_LOWER_BOUND * weight, BETA_WEIGHT_UPPER_BOUND * weight)
        for weight in volatility_parity_weights
    ]
    target_total = float(np.sum(volatility_parity_weights))
    constraints = [{"type": "eq", "fun": lambda w: float(np.sum(w)) - target_total}]

    result = scipy.optimize.minimize(
        objective,
        x0=volatility_parity_weights.copy(),
        method="SLSQP",
        bounds=bounds,
        constraints=constraints,
    )

    if result.success:
        return np.array(result.x)

    logger.warning("Beta-neutral optimizer did not converge, using vol-parity weights")
    return volatility_parity_weights


def size_pairs_with_volatility_parity(  # noqa: PLR0913
    candidate_pairs: pl.DataFrame,
    maximum_capital: float,
    current_timestamp: datetime,
    market_betas: pl.DataFrame,
    entry_prices: dict[str, float],
    exposure_scale: float = 1.0,
    # Defaults mirror Configuration fields.
    short_buying_power_buffer: float = 1.03,
) -> pl.DataFrame:
    """Size pairs so each contributes equal risk, then optimize for beta neutrality.

    Pairs with lower realized spread volatility receive more capital so that every
    pair contributes the same risk to the portfolio (volatility parity). A SLSQP
    optimizer then nudges the weights to drive net portfolio beta toward zero.
    Each pair is dollar-neutral: the short leg is floored to whole shares (Alpaca
    does not support fractional short sells), and the long notional is matched to
    the short's whole-share-adjusted amount so each pair is exactly balanced.
    The capital split accounts for Alpaca's short buying power reservation
    (ask * short_buying_power_buffer * qty) so the total buying power consumed
    equals maximum_capital.
    The exposure_scale parameter is a regime-driven multiplier (1.0x for
    mean_reversion, 0.5x for trending) applied before the final dollar amounts.
    """
    # Pre-filter: remove pairs whose short leg cannot fill at least 1 whole share
    # at the maximum possible per-pair allocation the optimizer can assign.
    # Upper bound uses BETA_WEIGHT_UPPER_BOUND to stay conservative without
    # discarding pairs that the optimizer might assign a higher-than-equal weight.
    # REQUIRED_PAIRS (the target pair count) is used as the divisor rather than
    # feasible_pairs.height so the bound is stable before filtering; pairs whose
    # short price exceeds this upper bound cannot be afforded at any weight the
    # optimizer assigns within BETA_WEIGHT_UPPER_BOUND of an equal allocation.
    capital_divisor = 1.0 + short_buying_power_buffer
    maximum_per_pair_dollar = (
        (maximum_capital / capital_divisor)
        * exposure_scale
        * BETA_WEIGHT_UPPER_BOUND
        / REQUIRED_PAIRS
    )
    long_prices = [
        entry_prices.get(ticker, 0.0)
        for ticker in candidate_pairs["long_ticker"].to_list()
    ]
    short_prices = [
        entry_prices.get(ticker, 0.0)
        for ticker in candidate_pairs["short_ticker"].to_list()
    ]
    feasible_pairs = (
        candidate_pairs.with_columns(
            pl.Series("_long_entry_price", long_prices),
            pl.Series("_short_entry_price", short_prices),
        )
        .filter(
            (pl.col("_long_entry_price") > 0)
            & (pl.col("_short_entry_price") > 0)
            & (pl.col("_short_entry_price") <= maximum_per_pair_dollar)
        )
        .drop("_long_entry_price", "_short_entry_price")
    )

    if feasible_pairs.height < REQUIRED_PAIRS:
        message = (
            f"Only {feasible_pairs.height} pairs available after whole-share short "
            f"constraint filter, need at least {REQUIRED_PAIRS}."
        )
        raise InsufficientPairsError(message)

    pairs = (
        feasible_pairs.head(REQUIRED_PAIRS)
        .with_columns(
            (
                (
                    pl.col("long_realized_volatility")
                    + pl.col("short_realized_volatility")
                )
                / 2.0
            ).alias("pair_volatility")
        )
        .with_columns(
            (1.0 / pl.col("pair_volatility").clip(lower_bound=1e-8)).alias(
                "inverse_volatility_weight"
            )
        )
    )

    total_weight = pairs["inverse_volatility_weight"].sum()
    volatility_parity_weights = (
        pairs["inverse_volatility_weight"] / total_weight
    ).to_numpy()

    adjusted_weights = _apply_beta_neutral_weights(
        pairs, market_betas, volatility_parity_weights
    )
    if np.isclose(adjusted_weights.sum(), 0.0):
        adjusted_weights = volatility_parity_weights / volatility_parity_weights.sum()
    else:
        adjusted_weights = adjusted_weights / adjusted_weights.sum()

    dollar_amounts = (
        adjusted_weights * (maximum_capital / capital_divisor) * exposure_scale
    )
    pairs = pairs.with_columns(pl.Series("dollar_amount", dollar_amounts))

    # Apply whole-share constraint to short legs: floor to the nearest whole share.
    # Long legs retain the full notional amount (fractional shares are supported).
    short_prices_for_pairs = [
        entry_prices.get(ticker, 0.0) for ticker in pairs["short_ticker"].to_list()
    ]
    pairs = (
        pairs.with_columns(pl.Series("_short_entry_price", short_prices_for_pairs))
        .with_columns(
            (pl.col("dollar_amount") / pl.col("_short_entry_price"))
            .cast(pl.Int64)
            .alias("_short_qty")
        )
        .with_columns(
            (
                pl.col("_short_qty").cast(pl.Float64) * pl.col("_short_entry_price")
            ).alias("_short_dollar_amount")
        )
    )

    # Drop any pairs where the optimizer assigned too little capital for even 1 share.
    zero_qty_count = int(pairs.filter(pl.col("_short_qty") == 0).height)
    if zero_qty_count > 0:
        logger.warning(
            "Dropped pairs with zero short quantity after optimization",
            count=zero_qty_count,
        )
        pairs = pairs.filter(pl.col("_short_qty") > 0)

    if 0 < pairs.height < REQUIRED_PAIRS:
        message = (
            f"Only {pairs.height} viable pairs remain after zero-quantity filter, "
            f"need at least {REQUIRED_PAIRS}."
        )
        raise InsufficientPairsError(message)

    if pairs.height == 0:
        message = "No viable pairs remain after whole-share short constraint."
        raise InsufficientPairsError(message)

    logger.info(
        "Sized pairs with volatility parity",
        pair_count=pairs.height,
        total_capital=maximum_capital,
        exposure_scale=exposure_scale,
    )

    timestamp_val = to_timestamp_milliseconds(current_timestamp)

    long_entry_prices = [
        entry_prices.get(ticker, 0.0) for ticker in pairs["long_ticker"].to_list()
    ]
    # Long notional is matched to the short's whole-share-adjusted dollar amount so
    # each pair is exactly dollar-neutral. quantity is null for long legs because
    # Alpaca BUY orders use notional (fractional shares are supported).
    long_positions = pairs.select(
        pl.col("long_ticker").alias("ticker"),
        pl.lit(timestamp_val).cast(pl.Int64).alias("timestamp"),
        pl.lit(PositionSide.LONG.value).alias("side"),
        pl.col("_short_dollar_amount").alias("dollar_amount"),
        pl.lit(PositionAction.OPEN_POSITION.value).alias("action"),
        pl.col("pair_id"),
    ).with_columns(
        pl.Series("entry_price", long_entry_prices),
        pl.lit(None).cast(pl.Int64).alias("quantity"),
        pl.col("dollar_amount").alias("notional"),
    )

    # Short legs carry the pre-computed whole-share quantity. notional is null because
    # Alpaca SELL orders must use qty (fractional short sells are not supported).
    short_positions = pairs.select(
        pl.col("short_ticker").alias("ticker"),
        pl.lit(timestamp_val).cast(pl.Int64).alias("timestamp"),
        pl.lit(PositionSide.SHORT.value).alias("side"),
        pl.col("_short_dollar_amount").alias("dollar_amount"),
        pl.lit(PositionAction.OPEN_POSITION.value).alias("action"),
        pl.col("pair_id"),
        pl.col("_short_entry_price").alias("entry_price"),
        pl.col("_short_qty").alias("quantity"),
        pl.lit(None).cast(pl.Float64).alias("notional"),
    )

    return pl.concat([long_positions, short_positions]).sort(["ticker", "side"])
