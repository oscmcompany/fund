import math
from datetime import datetime
from typing import Any

import polars as pl
import structlog

logger = structlog.get_logger()


def compute_portfolio_value(
    positions: pl.DataFrame,
    current_prices: pl.DataFrame,
    cash: float,
) -> float:
    if positions.is_empty():
        return cash

    joined = positions.join(current_prices, on="ticker", how="left")

    total_value = 0.0
    for row in joined.iter_rows(named=True):
        ticker = row["ticker"]
        close_price = row.get("close_price")
        if close_price is None:
            logger.warning("No current price for position, skipping", ticker=ticker)
            continue

        dollar_amount = row["dollar_amount"]
        entry_price = row["entry_price"]
        if entry_price is None or entry_price <= 0:
            logger.warning("No entry price for position, skipping", ticker=ticker)
            continue
        side = row["side"]

        if side == "LONG":
            position_value = dollar_amount * (close_price / entry_price)
        else:
            position_value = dollar_amount * (2.0 - close_price / entry_price)

        total_value += position_value

    return total_value + cash


def compute_period_return(current_value: float, previous_value: float) -> float:
    if previous_value == 0:
        return 0.0
    return (current_value - previous_value) / previous_value


def compute_realized_profit_and_loss(
    closing_pair: pl.DataFrame,
    current_prices: pl.DataFrame,
) -> tuple[float, float]:
    if closing_pair.is_empty():
        return (0.0, 0.0)

    joined = closing_pair.join(current_prices, on="ticker", how="left")

    total_profit_and_loss = 0.0
    total_invested = 0.0

    for row in joined.iter_rows(named=True):
        ticker = row["ticker"]
        close_price = row.get("close_price")
        dollar_amount = row["dollar_amount"]
        entry_price = row["entry_price"]
        if entry_price is None or entry_price <= 0:
            logger.warning(
                "No entry price for closing position, skipping", ticker=ticker
            )
            continue
        side = row["side"]

        if close_price is None:
            logger.warning(
                "No current price for closing position, skipping pnl", ticker=ticker
            )
            continue

        total_invested += dollar_amount

        if side == "LONG":
            position_profit_and_loss = dollar_amount * (close_price / entry_price - 1.0)
        else:
            position_profit_and_loss = dollar_amount * (1.0 - close_price / entry_price)

        total_profit_and_loss += position_profit_and_loss

    return_percent = (
        total_profit_and_loss / total_invested if total_invested > 0 else 0.0
    )

    return (total_profit_and_loss, return_percent)


def compute_sharpe_ratio(returns: list[float]) -> float | None:
    if len(returns) < 20:  # noqa: PLR2004
        return None

    mean_return = sum(returns) / len(returns)
    variance = sum((r - mean_return) ** 2 for r in returns) / (len(returns) - 1)
    standard_deviation = math.sqrt(variance)

    if standard_deviation == 0:
        return None

    return (mean_return / standard_deviation) * math.sqrt(252)


def compute_sortino_ratio(returns: list[float]) -> float | None:
    if len(returns) < 20:  # noqa: PLR2004
        return None

    mean_return = sum(returns) / len(returns)
    downside_returns = [r for r in returns if r < 0]

    if not downside_returns:
        return None

    downside_standard_deviation = math.sqrt(
        sum(r**2 for r in downside_returns) / len(returns)
    )

    if downside_standard_deviation == 0:
        return None

    return (mean_return / downside_standard_deviation) * math.sqrt(252)


def compute_maximum_drawdown(portfolio_values: list[float]) -> float | None:
    if len(portfolio_values) < 2:  # noqa: PLR2004
        return None

    peak = portfolio_values[0]
    if peak <= 0:
        return None
    maximum_drawdown = 0.0

    for value in portfolio_values[1:]:
        peak = max(peak, value)
        drawdown = (peak - value) / peak
        maximum_drawdown = max(maximum_drawdown, drawdown)

    return maximum_drawdown


def compute_calmar_ratio(
    annual_return: float,
    maximum_drawdown: float,
) -> float | None:
    if maximum_drawdown == 0.0:
        return None
    return annual_return / maximum_drawdown


def compute_win_rate(closed_pairs_profit_and_loss: list[float]) -> float | None:
    if not closed_pairs_profit_and_loss:
        return None
    winning_count = sum(
        1 for profit_and_loss in closed_pairs_profit_and_loss if profit_and_loss > 0
    )
    return winning_count / len(closed_pairs_profit_and_loss)


def compute_spy_relative_return(portfolio_return: float, spy_return: float) -> float:
    return portfolio_return - spy_return


def build_performance_snapshot(  # noqa: PLR0913
    portfolio_value: float,
    cash: float,
    spy_close: float,
    period_return: float,
    open_pair_count: int,
    timestamp: datetime,
    gross_return: float = 0.0,
    net_return: float = 0.0,
    total_slippage_cost: float = 0.0,
) -> dict[str, Any]:
    return {
        "timestamp": int(timestamp.timestamp() * 1000),
        "portfolio_value": portfolio_value,
        "cash_balance": cash,
        "spy_close": spy_close,
        "period_return_percent": period_return,
        "open_pair_count": open_pair_count,
        "gross_return": gross_return,
        "net_return": net_return,
        "total_slippage_cost": total_slippage_cost,
    }


def build_closed_pair_record(  # noqa: PLR0913
    pair_id: str,
    long_ticker: str,
    short_ticker: str,
    entry_timestamp: int,
    closed_timestamp: int,
    dollar_amount: float,
    realized_profit_and_loss: float,
    return_percent: float,
    close_reason: str = "rebalance",
) -> dict[str, Any]:
    holding_days = (closed_timestamp - entry_timestamp) // (1000 * 60 * 60 * 24)
    return {
        "closed_timestamp": closed_timestamp,
        "pair_id": pair_id,
        "long_ticker": long_ticker,
        "short_ticker": short_ticker,
        "entry_timestamp": entry_timestamp,
        "dollar_amount": dollar_amount,
        "realized_profit_and_loss": realized_profit_and_loss,
        "return_percent": return_percent,
        "holding_days": holding_days,
        "close_reason": close_reason,
    }
