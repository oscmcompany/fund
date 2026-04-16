import polars as pl
import structlog

from . import metrics
from .alpaca_client import AlpacaClient
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


def execute_open_positions(
    alpaca_client: AlpacaClient,
    open_positions: list[dict],
    initial_buying_power: float,
) -> tuple[list[dict], int]:
    open_results = []
    opened_count = 0
    remaining_buying_power = initial_buying_power
    skipped_insufficient_buying_power = 0
    skipped_not_shortable = 0

    for open_position in open_positions:
        ticker = open_position["ticker"]
        side = open_position["side"]
        dollar_amount = open_position["dollar_amount"]

        if dollar_amount > remaining_buying_power:
            logger.warning(
                "Skipping position due to insufficient buying power",
                ticker=ticker,
                side=side,
                dollar_amount=dollar_amount,
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
            # Refresh remaining buying power from the account after a successful order
            try:
                account = alpaca_client.get_account()
                remaining_buying_power = account.buying_power
            except Exception:
                logger.exception(
                    "Failed to refresh buying power from account, using estimate",
                    ticker=ticker,
                    deducting=dollar_amount,
                )
                remaining_buying_power -= dollar_amount
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
