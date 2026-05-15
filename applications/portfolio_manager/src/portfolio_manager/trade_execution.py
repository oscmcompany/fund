import polars as pl
import structlog

from . import metrics
from .alpaca_client import AlpacaClient
from .configuration import Configuration
from .enums import PositionSide, TradeSide
from .exceptions import AssetNotShortableError, InsufficientBuyingPowerError

logger = structlog.get_logger()


def get_positions(
    prior_portfolio_tickers: list[str],
    held_tickers: set[str],
    optimal_portfolio: pl.DataFrame,
) -> tuple[list[dict], list[dict]]:
    close_positions = [
        {"ticker": ticker}
        for ticker in prior_portfolio_tickers
        if ticker not in held_tickers
    ]

    open_positions = [
        {
            "ticker": row["ticker"],
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


def execute_open_positions(  # noqa: C901, PLR0915
    alpaca_client: AlpacaClient,
    open_positions: list[dict],
    initial_buying_power: float,
    account_equity: float,
    configuration: Configuration,
) -> tuple[list[dict], int]:
    open_results = []
    opened_count = 0
    remaining_buying_power = initial_buying_power
    skipped_insufficient_buying_power = 0
    skipped_not_shortable = 0

    # Submit all long (BUY) positions before short (SELL) positions so that long
    # legs are established first and buying power is accurately reflected when
    # evaluating each short leg.
    sorted_positions = sorted(
        open_positions,
        key=lambda position: 0 if position["side"] == TradeSide.BUY else 1,
    )

    for open_position in sorted_positions:
        ticker = open_position["ticker"]
        side = open_position["side"]
        dollar_amount = open_position["dollar_amount"]
        entry_price = open_position["entry_price"]

        if side == TradeSide.SELL:
            # Check minimum account equity required for short selling.
            if account_equity < configuration.minimum_short_equity:
                logger.warning(
                    "Skipping short position due to insufficient account equity",
                    ticker=ticker,
                    account_equity=account_equity,
                    minimum_short_equity=configuration.minimum_short_equity,
                )
                skipped_insufficient_buying_power += 1
                metrics.trades_submitted_total.labels(
                    action="open", status="skipped"
                ).inc()
                open_results.append(
                    {
                        "ticker": ticker,
                        "action": "open",
                        "side": side,
                        "dollar_amount": dollar_amount,
                        "status": "skipped",
                        "reason": "insufficient_equity_for_short",
                    }
                )
                continue

            pre_computed_qty = open_position.get("quantity")
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
                skipped_insufficient_buying_power += 1
                metrics.trades_submitted_total.labels(
                    action="open", status="skipped"
                ).inc()
                open_results.append(
                    {
                        "ticker": ticker,
                        "action": "open",
                        "side": side,
                        "dollar_amount": dollar_amount,
                        "status": "skipped",
                        "reason": "zero_short_quantity",
                    }
                )
                continue

            # Alpaca reserves ask * 1.03 * qty against buying power for short
            # market orders.
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
            skipped_insufficient_buying_power += 1
            metrics.trades_submitted_total.labels(action="open", status="skipped").inc()
            open_results.append(
                {
                    "ticker": ticker,
                    "action": "open",
                    "side": side,
                    "dollar_amount": dollar_amount,
                    "status": "skipped",
                    "reason": "insufficient_buying_power",
                }
            )
            continue

        try:
            alpaca_client.open_position(
                ticker=ticker,
                side=side,
                dollar_amount=dollar_amount,
                entry_price=entry_price,
                quantity=open_position.get("quantity"),
            )
            logger.info(
                "Opened position",
                ticker=ticker,
                side=side,
                dollar_amount=dollar_amount,
            )
            opened_count += 1
            metrics.trades_submitted_total.labels(action="open", status="success").inc()
            metrics.trade_dollar_amount_total.labels(side=side.value).inc(dollar_amount)
            # Refresh remaining buying power from the account after a successful order.
            try:
                account = alpaca_client.get_account()
                remaining_buying_power = account.buying_power
            except Exception:
                logger.exception(
                    "Failed to refresh buying power from account, using estimate",
                    ticker=ticker,
                    deducting=buying_power_cost,
                )
                remaining_buying_power -= buying_power_cost
            open_results.append(
                {
                    "ticker": ticker,
                    "action": "open",
                    "side": side,
                    "dollar_amount": dollar_amount,
                    "status": "success",
                }
            )
        except InsufficientBuyingPowerError as e:
            logger.warning(
                "Insufficient buying power for position",
                ticker=ticker,
                side=side,
                dollar_amount=dollar_amount,
                error=str(e),
            )
            skipped_insufficient_buying_power += 1
            metrics.trades_submitted_total.labels(action="open", status="skipped").inc()
            open_results.append(
                {
                    "ticker": ticker,
                    "action": "open",
                    "side": side,
                    "dollar_amount": dollar_amount,
                    "status": "skipped",
                    "reason": "insufficient_buying_power",
                }
            )
        except AssetNotShortableError as e:
            logger.warning(
                "Asset cannot be sold short",
                ticker=ticker,
                side=side,
                error=str(e),
            )
            skipped_not_shortable += 1
            metrics.trades_submitted_total.labels(action="open", status="skipped").inc()
            open_results.append(
                {
                    "ticker": ticker,
                    "action": "open",
                    "side": side,
                    "dollar_amount": dollar_amount,
                    "status": "skipped",
                    "reason": "not_shortable",
                }
            )
        except Exception as e:
            logger.exception(
                "Failed to open position",
                ticker=ticker,
                error=str(e),
            )
            metrics.trades_submitted_total.labels(action="open", status="failed").inc()
            open_results.append(
                {
                    "ticker": ticker,
                    "action": "open",
                    "side": side,
                    "dollar_amount": dollar_amount,
                    "status": "failed",
                    "error": str(e),
                }
            )

    if skipped_insufficient_buying_power > 0 or skipped_not_shortable > 0:
        logger.info(
            "Some positions were skipped",
            skipped_insufficient_buying_power=skipped_insufficient_buying_power,
            skipped_not_shortable=skipped_not_shortable,
        )

    return open_results, opened_count
