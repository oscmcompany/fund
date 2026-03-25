import time

from fastapi import Response
from prometheus_client import Counter, Gauge, Histogram, generate_latest

rebalance_requests_total = Counter(
    "portfolio_rebalance_requests_total",
    "Total portfolio rebalance requests",
)
rebalance_errors_total = Counter(
    "portfolio_rebalance_errors_total",
    "Total rebalance requests that failed",
    ["stage"],
)
rebalance_duration_seconds = Histogram(
    "portfolio_rebalance_duration_seconds",
    "Time for full rebalance cycle",
    buckets=[5, 15, 30, 60, 120, 300],
)

trades_submitted_total = Counter(
    "portfolio_trades_submitted_total",
    "Total trade orders submitted to Alpaca",
    ["action", "status"],
)
trade_dollar_amount_total = Counter(
    "portfolio_trade_dollar_amount_total",
    "Cumulative dollar amount of trades submitted",
    ["side"],
)

pairs_selected_count = Gauge(
    "portfolio_pairs_selected_count",
    "Number of candidate pairs selected in last rebalance",
)
positions_opened_count = Gauge(
    "portfolio_positions_opened_count",
    "Number of positions opened in last rebalance",
)
positions_closed_count = Gauge(
    "portfolio_positions_closed_count",
    "Number of positions closed in last rebalance",
)
regime_state = Gauge(
    "portfolio_regime_state",
    "Current regime classification (1=mean_reversion, 0=other)",
)
exposure_scale_value = Gauge(
    "portfolio_exposure_scale",
    "Current exposure scale factor",
)
account_buying_power = Gauge(
    "portfolio_account_buying_power",
    "Account buying power at start of rebalance",
)
account_cash = Gauge(
    "portfolio_account_cash",
    "Account cash at start of rebalance",
)


def get_metrics() -> Response:
    return Response(
        content=generate_latest(),
        media_type="text/plain; version=0.0.4; charset=utf-8",
    )


def start_timer() -> float:
    return time.monotonic()


def observe_duration(start: float) -> None:
    rebalance_duration_seconds.observe(time.monotonic() - start)
