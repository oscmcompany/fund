import uuid
from datetime import UTC, datetime
from typing import Any

import numpy as np
import polars as pl
import structlog
from internal.database import get_pool

from .enums import PositionSide
from .risk_management import Z_SCORE_HOLD_THRESHOLD, Z_SCORE_STOP_LOSS
from .statistical_arbitrage import CORRELATION_WINDOW_DAYS, compute_spread_zscore

logger = structlog.get_logger()

_MINIMUM_PAIR_PRICE_ROWS = 30

_PRIOR_ALLOCATION_SCHEMA: dict[str, type] = {
    "ticker": pl.String,
    "timestamp": pl.Int64,
    "side": pl.String,
    "dollar_amount": pl.Float64,
    "action": pl.String,
    "pair_id": pl.String,
    "entry_price": pl.Float64,
    "quantity": pl.Int64,
    "notional": pl.Float64,
}


async def already_rebalanced_today() -> bool:
    # TODO(Phase 7): remove once intraday check replaces daily guard  # noqa: FIX002
    try:
        pool = await get_pool()
        async with pool.connection() as connection:
            result = await connection.execute(
                """SELECT COUNT(*) FROM equity_rebalance_sessions
                   WHERE triggered_at >= CURRENT_DATE::timestamptz
                     AND triggered_at < (CURRENT_DATE + INTERVAL '1 day')::timestamptz
                     AND status = 'completed'"""
            )
            row = await result.fetchone()
        return bool(row and row[0] > 0)
    except Exception as error:
        logger.exception("Failed to check daily rebalance status", error=str(error))
        return False


async def get_prior_allocation() -> pl.DataFrame:
    empty = pl.DataFrame(schema=_PRIOR_ALLOCATION_SCHEMA)
    try:
        pool = await get_pool()
        async with pool.connection() as connection:
            result = await connection.execute(
                """SELECT ea.ticker,
                          EXTRACT(EPOCH FROM
                              ea.generated_at)::bigint * 1000 AS timestamp,
                          ea.side,
                          ea.dollar_amount::double precision,
                          ea.action,
                          ep.pair_id,
                          ea.entry_price::double precision,
                          ea.quantity::bigint,
                          ea.notional::double precision
                   FROM equity_allocations ea
                   JOIN equity_pairs ep ON ea.equity_pair_id = ep.id
                   WHERE ep.status = 'open'
                   ORDER BY ea.ticker"""
            )
            rows = await result.fetchall()
    except Exception as error:
        logger.exception(
            "Failed to fetch prior allocation from database", error=str(error)
        )
        raise

    if not rows:
        logger.info("Prior allocation is empty")
        return empty

    dataframe = pl.DataFrame(
        {
            "ticker": [row[0] for row in rows],
            "timestamp": [row[1] for row in rows],
            "side": [row[2] for row in rows],
            "dollar_amount": [row[3] for row in rows],
            "action": [row[4] for row in rows],
            "pair_id": [row[5] for row in rows],
            "entry_price": [row[6] for row in rows],
            "quantity": [row[7] for row in rows],
            "notional": [row[8] for row in rows],
        },
        schema=_PRIOR_ALLOCATION_SCHEMA,
    )
    logger.info("Retrieved prior allocation", count=dataframe.height)
    return dataframe


async def save_rebalance(  # noqa: PLR0913
    triggered_at: datetime,
    trigger_reason: str,
    model_run_id: str | None,
    successful_pair_rows: pl.DataFrame,
    candidate_pairs: pl.DataFrame,
    open_results: list[dict[str, Any]],
) -> bool:
    try:
        session_id = str(uuid.uuid4())
        completed_at = datetime.now(tz=UTC)
        order_by_ticker = {
            r["ticker"]: r for r in open_results if r.get("status") == "success"
        }

        pool = await get_pool()
        async with pool.connection() as connection:  # noqa: SIM117
            async with connection.transaction():
                await connection.execute(
                    """INSERT INTO equity_rebalance_sessions
                       (id, triggered_at, trigger_reason,
                        model_run_id, completed_at, status)
                       VALUES (%s, %s, %s, %s, %s, %s)""",
                    (
                        session_id,
                        triggered_at,
                        trigger_reason,
                        model_run_id,
                        completed_at,
                        "completed",
                    ),
                )

                if not successful_pair_rows.is_empty():
                    pair_ids = successful_pair_rows["pair_id"].unique().to_list()
                    for pair_id_str in pair_ids:
                        pair_legs = successful_pair_rows.filter(
                            pl.col("pair_id") == pair_id_str
                        )
                        candidate_row = candidate_pairs.filter(
                            pl.col("pair_id") == pair_id_str
                        )
                        if candidate_row.is_empty():
                            logger.warning(
                                "Candidate pair not found for session, skipping",
                                pair_id=pair_id_str,
                            )
                            continue

                        long_legs = pair_legs.filter(
                            pl.col("side") == PositionSide.LONG.value
                        )
                        short_legs = pair_legs.filter(
                            pl.col("side") == PositionSide.SHORT.value
                        )
                        if long_legs.is_empty() or short_legs.is_empty():
                            logger.warning(
                                "Incomplete pair legs in session, skipping",
                                pair_id=pair_id_str,
                            )
                            continue

                        pair_uuid = str(uuid.uuid4())
                        await connection.execute(
                            """INSERT INTO equity_pairs
                               (id, rebalance_id, pair_id, long_ticker, short_ticker,
                                z_score, hedge_ratio, signal_strength,
                                status, opened_at)
                               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                            (
                                pair_uuid,
                                session_id,
                                pair_id_str,
                                long_legs["ticker"][0],
                                short_legs["ticker"][0],
                                float(candidate_row["z_score"][0]),
                                float(candidate_row["hedge_ratio"][0]),
                                float(candidate_row["signal_strength"][0]),
                                "open",
                                triggered_at,
                            ),
                        )

                        for leg in pair_legs.iter_rows(named=True):
                            allocation_uuid = str(uuid.uuid4())
                            ticker = leg["ticker"]
                            leg_entry_price = leg.get("entry_price")
                            leg_quantity = leg.get("quantity")
                            leg_notional = leg.get("notional")

                            await connection.execute(
                                """INSERT INTO equity_allocations
                                   (id, rebalance_id, equity_pair_id, generated_at,
                                    model_run_id, ticker, side, action, dollar_amount,
                                    entry_price, quantity, notional)
                                   VALUES (%s, %s, %s, %s,
                                           %s, %s, %s, %s, %s, %s, %s, %s)""",
                                (
                                    allocation_uuid,
                                    session_id,
                                    pair_uuid,
                                    triggered_at,
                                    model_run_id,
                                    ticker,
                                    leg["side"],
                                    leg.get("action", "OPEN_POSITION"),
                                    float(leg["dollar_amount"]),
                                    float(leg_entry_price)
                                    if leg_entry_price is not None
                                    else None,
                                    int(leg_quantity)
                                    if leg_quantity is not None
                                    else None,
                                    float(leg_notional)
                                    if leg_notional is not None
                                    else None,
                                ),
                            )

                            order_result = order_by_ticker.get(ticker)
                            if order_result and order_result.get("alpaca_order_id"):
                                order_side = order_result.get("side")
                                order_side_str = (
                                    order_side.value
                                    if hasattr(order_side, "value")
                                    else str(order_side)
                                )
                                submitted_qty = order_result.get("submitted_quantity")
                                if (
                                    submitted_qty is None
                                    and leg_entry_price
                                    and leg_entry_price > 0
                                    and leg_notional
                                ):
                                    submitted_qty = int(leg_notional / leg_entry_price)
                                if submitted_qty is not None and submitted_qty > 0:
                                    await connection.execute(
                                        """INSERT INTO equity_orders
                                           (id, allocation_id, submitted_at,
                                            ticker, side,
                                            quantity, order_type, alpaca_order_id)
                                           VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
                                        (
                                            str(uuid.uuid4()),
                                            allocation_uuid,
                                            triggered_at,
                                            ticker,
                                            order_side_str,
                                            float(submitted_qty),
                                            "market",
                                            order_result["alpaca_order_id"],
                                        ),
                                    )

        pair_count = (
            successful_pair_rows["pair_id"].n_unique()
            if not successful_pair_rows.is_empty()
            else 0
        )
        logger.info(
            "Saved rebalance session",
            trigger_reason=trigger_reason,
            pair_count=pair_count,
        )
        return True  # noqa: TRY300
    except Exception as error:
        logger.exception("Failed to save rebalance session", error=str(error))
        return False


async def save_performance_snapshot(snapshot: dict[str, Any]) -> bool:
    try:
        timestamp_seconds = snapshot["timestamp"] // 1000
        snapshot_date = datetime.fromtimestamp(timestamp_seconds, tz=UTC).date()

        pool = await get_pool()
        async with pool.connection() as connection:
            await connection.execute(
                """INSERT INTO equity_portfolio_snapshots
                   (snapshot_date, net_asset_value,
                    gross_return, net_return, total_slippage_cost)
                   VALUES (%s, %s, %s, %s, %s)
                   ON CONFLICT (snapshot_date) DO UPDATE
                   SET net_asset_value = EXCLUDED.net_asset_value,
                       gross_return = EXCLUDED.gross_return,
                       net_return = EXCLUDED.net_return,
                       total_slippage_cost = EXCLUDED.total_slippage_cost""",
                (
                    snapshot_date,
                    snapshot["portfolio_value"],
                    snapshot["gross_return"],
                    snapshot["net_return"],
                    snapshot["total_slippage_cost"],
                ),
            )
        logger.info("Saved performance snapshot")
        return True  # noqa: TRY300
    except Exception as error:
        logger.exception("Failed to save performance snapshot", error=str(error))
        return False


async def save_closed_pair(record: dict[str, Any]) -> bool:
    try:
        closed_at = datetime.fromtimestamp(record["closed_timestamp"] // 1000, tz=UTC)

        pool = await get_pool()
        async with pool.connection() as connection:
            await connection.execute(
                """UPDATE equity_pairs
                   SET status = 'closed',
                       closed_at = %s,
                       realized_profit_and_loss = %s,
                       return_percent = %s,
                       holding_days = %s
                   WHERE pair_id = %s
                     AND status = 'open'""",
                (
                    closed_at,
                    record["realized_profit_and_loss"],
                    record["return_percent"],
                    record["holding_days"],
                    record["pair_id"],
                ),
            )
        logger.info("Saved closed pair record")
        return True  # noqa: TRY300
    except Exception as error:
        logger.exception("Failed to save closed pair record", error=str(error))
        return False


async def get_last_portfolio_value() -> float | None:
    try:
        pool = await get_pool()
        async with pool.connection() as connection:
            result = await connection.execute(
                """SELECT net_asset_value::double precision
                   FROM equity_portfolio_snapshots
                   ORDER BY snapshot_date DESC
                   LIMIT 1"""
            )
            row = await result.fetchone()
        return float(row[0]) if row else None
    except Exception as error:
        logger.exception("Failed to retrieve last portfolio value", error=str(error))
        return None


def evaluate_prior_pairs(
    prior_portfolio: pl.DataFrame,
    historical_prices: pl.DataFrame,
) -> set[str]:
    held_tickers: set[str] = set()

    if prior_portfolio.is_empty():
        return held_tickers

    pair_ids = prior_portfolio["pair_id"].unique(maintain_order=False).sort().to_list()

    for pair_id in pair_ids:
        pair_rows = prior_portfolio.filter(pl.col("pair_id") == pair_id)

        long_rows = pair_rows.filter(pl.col("side") == PositionSide.LONG.value)
        short_rows = pair_rows.filter(pl.col("side") == PositionSide.SHORT.value)

        if long_rows.is_empty() or short_rows.is_empty():
            logger.warning("Malformed prior pair, closing normally", pair_id=pair_id)
            continue

        long_ticker = long_rows["ticker"][0]
        short_ticker = short_rows["ticker"][0]

        pair_price_matrix = (
            historical_prices.filter(
                pl.col("ticker").is_in([long_ticker, short_ticker])
            )
            .pivot(
                on="ticker",
                index="timestamp",
                values="close_price",
                aggregate_function="last",
            )
            .sort("timestamp")
            .drop_nulls()
        )

        if (
            long_ticker not in pair_price_matrix.columns
            or short_ticker not in pair_price_matrix.columns
        ):
            logger.warning(
                "Missing price data for prior pair, closing normally",
                pair_id=pair_id,
            )
            continue

        pair_price_matrix = pair_price_matrix.tail(CORRELATION_WINDOW_DAYS)

        if pair_price_matrix.height < _MINIMUM_PAIR_PRICE_ROWS:
            logger.warning(
                "Insufficient price history for prior pair, closing normally",
                pair_id=pair_id,
            )
            continue

        long_prices = pair_price_matrix[long_ticker].to_numpy()
        short_prices = pair_price_matrix[short_ticker].to_numpy()

        if np.any(long_prices <= 0) or np.any(short_prices <= 0):
            logger.warning(
                "Non-positive prices in prior pair, closing normally",
                pair_id=pair_id,
            )
            continue

        log_prices_long = np.log(long_prices)
        log_prices_short = np.log(short_prices)

        current_z, _ = compute_spread_zscore(log_prices_long, log_prices_short)

        if np.isnan(current_z):
            logger.warning(
                "NaN z-score for prior pair, closing normally",
                pair_id=pair_id,
            )
            continue

        absolute_z_score = abs(current_z)

        if Z_SCORE_HOLD_THRESHOLD <= absolute_z_score < Z_SCORE_STOP_LOSS:
            held_tickers.add(long_ticker)
            held_tickers.add(short_ticker)
            logger.info(
                "Holding prior pair, spread still mean-reverting",
                pair_id=pair_id,
                z_score=current_z,
            )
        elif absolute_z_score < Z_SCORE_HOLD_THRESHOLD:
            logger.info(
                "Closing prior pair to realize profit, spread converged",
                pair_id=pair_id,
                z_score=current_z,
            )
        else:
            logger.info(
                "Closing prior pair on stop-loss, spread diverged",
                pair_id=pair_id,
                z_score=current_z,
            )

    return held_tickers
