import polars as pl
import structlog

from . import metrics
from .alpaca_client import AlpacaClient
from .configuration import Configuration
from .enums import PositionSide, TradeSide
from .exceptions import AssetNotShortableError, InsufficientBuyingPowerError

logger = structlog.get_logger()


def get_positions(
    prior_allocation_tickers: list[str],
    held_tickers: set[str],
    optimal_portfolio: pl.DataFrame,
) -> tuple[list[dict], list[dict]]:
    close_positions = [
        {"ticker": ticker}
        for ticker in prior_allocation_tickers
        if ticker not in held_tickers
    ]

    open_positions = [
        {
            "ticker": row["ticker"],
            "pair_id": row["pair_id"],
            "side": (
                TradeSide.BUY
                if row["side"] == PositionSide.LONG.value
                else TradeSide.SELL
            ),
            "dollar_amount": row["dollar_amount"],
            "entry_price": row["entry_price"],
            "quantity": row.get("quantity"),
            "notional": row.get("notional"),
        }
        for row in optimal_portfolio.iter_rows(named=True)
    ]

    return open_positions, close_positions


def execute_close_positions(
    alpaca_client: AlpacaClient,
    close_positions: list[dict],
) -> tuple[list[dict], int]:
    close_results = []
    closed_count = 0

    for close_position in close_positions:
        try:
            was_closed = alpaca_client.close_position(
                ticker=close_position["ticker"],
            )
            if was_closed:
                closed_count += 1
                logger.info("Closed position", ticker=close_position["ticker"])
                metrics.trades_submitted_total.labels(
                    action="close", status="success"
                ).inc()
                close_results.append(
                    {
                        "ticker": close_position["ticker"],
                        "action": "close",
                        "status": "success",
                    }
                )
            else:
                logger.info(
                    "Position already closed or did not exist",
                    ticker=close_position["ticker"],
                )
                metrics.trades_submitted_total.labels(
                    action="close", status="skipped"
                ).inc()
                close_results.append(
                    {
                        "ticker": close_position["ticker"],
                        "action": "close",
                        "status": "skipped",
                        "reason": "position_not_found",
                    }
                )
        except Exception as e:
            logger.exception(
                "Failed to close position",
                ticker=close_position["ticker"],
                error=str(e),
            )
            metrics.trades_submitted_total.labels(action="close", status="failed").inc()
            close_results.append(
                {
                    "ticker": close_position["ticker"],
                    "action": "close",
                    "status": "failed",
                    "error": str(e),
                }
            )

    return close_results, closed_count


def _try_open_single_position(  # noqa: PLR0911
    alpaca_client: AlpacaClient,
    position: dict,
    remaining_buying_power: float,
    account_equity: float,
    configuration: Configuration,
) -> tuple[dict, bool, float]:
    """Attempt to open one leg of a pair. Returns (result, succeeded, updated_buying_power)."""  # noqa: E501
    ticker = position["ticker"]
    side = position["side"]
    dollar_amount = position["dollar_amount"]
    entry_price = position["entry_price"]

    base_result: dict = {
        "ticker": ticker,
        "action": "open",
        "side": side,
        "dollar_amount": dollar_amount,
    }

    if side == TradeSide.SELL:
        if account_equity < configuration.minimum_short_equity:
            logger.warning(
                "Skipping short position due to insufficient account equity",
                ticker=ticker,
                account_equity=account_equity,
                minimum_short_equity=configuration.minimum_short_equity,
            )
            metrics.trades_submitted_total.labels(action="open", status="skipped").inc()
            return (
                {
                    **base_result,
                    "status": "skipped",
                    "reason": "insufficient_equity_for_short",
                },
                False,
                remaining_buying_power,
            )

        pre_computed_qty = position.get("quantity")
        short_qty = (
            pre_computed_qty
            if pre_computed_qty is not None
            else int(dollar_amount / entry_price)
        )
        if short_qty == 0:
            logger.warning(
                "Skipping short position with zero quantity",
                ticker=ticker,
                dollar_amount=dollar_amount,
                entry_price=entry_price,
            )
            metrics.trades_submitted_total.labels(action="open", status="skipped").inc()
            return (
                {**base_result, "status": "skipped", "reason": "zero_short_quantity"},
                False,
                remaining_buying_power,
            )

        # Alpaca reserves ask * 1.03 * qty against buying power for short market orders.
        buying_power_cost = (
            short_qty * entry_price * configuration.short_buying_power_buffer
        )

        if configuration.hold_overnight:
            # Overnight maintenance margin: 30% for stocks >= $5, 100% for < $5.
            margin_rate = (
                configuration.overnight_margin_rate_low_price
                if entry_price < configuration.low_price_threshold
                else configuration.overnight_margin_rate_standard
            )
            overnight_margin_reserve = short_qty * entry_price * margin_rate
            buying_power_cost += overnight_margin_reserve
            logger.info(
                "Overnight margin reserve for short position",
                ticker=ticker,
                short_qty=short_qty,
                overnight_margin_reserve=overnight_margin_reserve,
                margin_rate=margin_rate,
            )
    else:
        buying_power_cost = dollar_amount

    if buying_power_cost > remaining_buying_power:
        logger.warning(
            "Skipping position due to insufficient buying power",
            ticker=ticker,
            side=side,
            buying_power_cost=buying_power_cost,
            remaining_buying_power=remaining_buying_power,
        )
        metrics.trades_submitted_total.labels(action="open", status="skipped").inc()
        return (
            {**base_result, "status": "skipped", "reason": "insufficient_buying_power"},
            False,
            remaining_buying_power,
        )

    try:
        alpaca_order_id = alpaca_client.open_position(
            ticker=ticker,
            side=side,
            dollar_amount=dollar_amount,
            entry_price=entry_price,
            quantity=short_qty if side == TradeSide.SELL else position.get("quantity"),
        )
        logger.info(
            "Opened position", ticker=ticker, side=side, dollar_amount=dollar_amount
        )
        metrics.trades_submitted_total.labels(action="open", status="success").inc()
        metrics.trade_dollar_amount_total.labels(side=side.value).inc(dollar_amount)
        # Refresh remaining buying power from the account after a successful order.
        try:
            account = alpaca_client.get_account()
            new_buying_power = account.buying_power
        except Exception:
            logger.exception(
                "Failed to refresh buying power from account, using estimate",
                ticker=ticker,
                deducting=buying_power_cost,
            )
            new_buying_power = remaining_buying_power - buying_power_cost
        submitted_quantity = short_qty if side == TradeSide.SELL else None
        return (  # noqa: TRY300
            {
                **base_result,
                "status": "success",
                "alpaca_order_id": alpaca_order_id,
                "submitted_quantity": submitted_quantity,
            },
            True,
            new_buying_power,
        )
    except InsufficientBuyingPowerError as e:
        logger.warning(
            "Insufficient buying power for position",
            ticker=ticker,
            side=side,
            dollar_amount=dollar_amount,
            error=str(e),
        )
        metrics.trades_submitted_total.labels(action="open", status="skipped").inc()
        return (
            {**base_result, "status": "skipped", "reason": "insufficient_buying_power"},
            False,
            remaining_buying_power,
        )
    except AssetNotShortableError as e:
        logger.warning(
            "Asset cannot be sold short", ticker=ticker, side=side, error=str(e)
        )
        metrics.trades_submitted_total.labels(action="open", status="skipped").inc()
        return (
            {**base_result, "status": "skipped", "reason": "not_shortable"},
            False,
            remaining_buying_power,
        )
    except Exception as e:
        logger.exception("Failed to open position", ticker=ticker, error=str(e))
        metrics.trades_submitted_total.labels(action="open", status="failed").inc()
        return (
            {**base_result, "status": "failed", "error": str(e)},
            False,
            remaining_buying_power,
        )


def execute_open_positions(  # noqa: C901, PLR0912, PLR0915
    alpaca_client: AlpacaClient,
    open_positions: list[dict],
    initial_buying_power: float,
    account_equity: float,
    configuration: Configuration,
) -> tuple[list[dict], int]:
    open_results: list[dict] = []
    opened_count = 0
    remaining_buying_power = initial_buying_power

    # Group positions by pair_id; within each pair, execute long before short so that
    # each pair is traded atomically and the short is skipped if the long fails,
    # preventing one-sided directional exposure.
    pairs: dict[str, dict[str, dict]] = {}
    for position in open_positions:
        pair_id = position["pair_id"]
        pairs.setdefault(pair_id, {})
        if position["side"] == TradeSide.BUY:
            pairs[pair_id]["long"] = position
        else:
            pairs[pair_id]["short"] = position

    skipped_incomplete_pairs = 0
    skipped_long_leg_failed = 0
    skipped_insufficient_buying_power = 0
    skipped_insufficient_equity = 0
    skipped_zero_quantity = 0
    skipped_not_shortable = 0

    for pair_id, legs in pairs.items():
        long_leg = legs.get("long")
        short_leg = legs.get("short")

        if long_leg is None or short_leg is None:
            logger.warning("Incomplete pair, skipping", pair_id=pair_id)
            skipped_incomplete_pairs += 1
            continue

        long_result, long_succeeded, remaining_buying_power = _try_open_single_position(
            alpaca_client,
            long_leg,
            remaining_buying_power,
            account_equity,
            configuration,
        )
        open_results.append(long_result)
        if long_succeeded:
            opened_count += 1
        else:
            reason = long_result.get("reason", "")
            if reason == "insufficient_buying_power":
                skipped_insufficient_buying_power += 1
            elif reason == "insufficient_equity_for_short":
                skipped_insufficient_equity += 1
            elif reason == "zero_short_quantity":
                skipped_zero_quantity += 1
            elif reason == "not_shortable":
                skipped_not_shortable += 1

            # Skip the short leg to avoid one-sided directional exposure.
            metrics.trades_submitted_total.labels(action="open", status="skipped").inc()
            open_results.append(
                {
                    "ticker": short_leg["ticker"],
                    "action": "open",
                    "side": short_leg["side"],
                    "dollar_amount": short_leg["dollar_amount"],
                    "status": "skipped",
                    "reason": "long_leg_failed",
                }
            )
            skipped_long_leg_failed += 1
            continue

        short_result, short_succeeded, remaining_buying_power = (
            _try_open_single_position(
                alpaca_client,
                short_leg,
                remaining_buying_power,
                account_equity,
                configuration,
            )
        )
        open_results.append(short_result)
        if short_succeeded:
            opened_count += 1
        else:
            reason = short_result.get("reason", "")
            if reason == "insufficient_buying_power":
                skipped_insufficient_buying_power += 1
            elif reason == "insufficient_equity_for_short":
                skipped_insufficient_equity += 1
            elif reason == "zero_short_quantity":
                skipped_zero_quantity += 1
            elif reason == "not_shortable":
                skipped_not_shortable += 1

    any_skipped = (
        skipped_insufficient_buying_power > 0
        or skipped_insufficient_equity > 0
        or skipped_zero_quantity > 0
        or skipped_not_shortable > 0
        or skipped_long_leg_failed > 0
        or skipped_incomplete_pairs > 0
    )
    if any_skipped:
        logger.info(
            "Some positions were skipped",
            skipped_insufficient_buying_power=skipped_insufficient_buying_power,
            skipped_insufficient_equity=skipped_insufficient_equity,
            skipped_zero_quantity=skipped_zero_quantity,
            skipped_not_shortable=skipped_not_shortable,
            skipped_long_leg_failed=skipped_long_leg_failed,
            skipped_incomplete_pairs=skipped_incomplete_pairs,
        )

    return open_results, opened_count
