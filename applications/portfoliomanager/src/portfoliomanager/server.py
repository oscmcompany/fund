import io
import json
import os
from datetime import UTC, datetime
from typing import cast

import polars as pl
import requests
import sentry_sdk
import structlog
from fastapi import FastAPI, Response, status
from internal.equity_bars_schema import equity_bars_schema
from sentry_sdk.integrations.logging import LoggingIntegration

from .alpaca_client import AlpacaClient

sentry_sdk.init(
    dsn=os.environ.get("SENTRY_DSN"),
    environment=os.environ.get("ENVIRONMENT", "development"),
    traces_sample_rate=1.0,
    profiles_sample_rate=1.0,
    enable_tracing=True,
    propagate_traces=True,
    integrations=[
        LoggingIntegration(
            level=None,
            event_level="WARNING",
        ),
    ],
)

structlog.configure(
    processors=[
        structlog.stdlib.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.JSONRenderer(),
    ],
    wrapper_class=structlog.stdlib.BoundLogger,
    context_class=dict,
    logger_factory=structlog.stdlib.LoggerFactory(),
    cache_logger_on_first_use=True,
)

from .enums import PositionAction, PositionSide, TradeSide
from .exceptions import (
    AssetNotShortableError,
    InsufficientBuyingPowerError,
    InsufficientPredictionsError,
)
from .portfolio_schema import portfolio_schema
from .risk_management import (
    UNCERTAINTY_THRESHOLD,
    add_equity_bars_returns_and_realized_volatility_columns,
    add_portfolio_action_column,
    add_portfolio_performance_columns,
    add_predictions_zscore_ranked_columns,
    create_optimal_portfolio,
)

logger = structlog.get_logger()

application: FastAPI = FastAPI()

DATAMANAGER_BASE_URL = os.getenv("PSF_DATAMANAGER_BASE_URL", "http://datamanager:8080")
EQUITYPRICEMODEL_BASE_URL = os.getenv(
    "PSF_EQUITYPRICEMODEL_BASE_URL",
    "http://equitypricemodel:8080",
)

ALPACA_API_KEY_ID = os.getenv("ALPACA_API_KEY_ID", "")
ALPACA_API_SECRET = os.getenv("ALPACA_API_SECRET", "")

if not ALPACA_API_KEY_ID or not ALPACA_API_SECRET:
    logger.error(
        "Missing Alpaca credentials",
        api_key_id_set=bool(ALPACA_API_KEY_ID),
        api_secret_set=bool(ALPACA_API_SECRET),
    )
    message = (
        "ALPACA_API_KEY_ID and ALPACA_API_SECRET environment variables are required"
    )
    raise ValueError(message)

tolerance_env_value = os.getenv("ALPACA_PRICE_TOLERANCE_PERCENT", "1.0")
try:
    price_tolerance_percent = float(tolerance_env_value)
    if price_tolerance_percent < 0:
        message = (
            "ALPACA_PRICE_TOLERANCE_PERCENT must be non-negative, "
            f"got {price_tolerance_percent}"
        )
        raise ValueError(message)
except ValueError as e:
    logger.error(
        "Invalid ALPACA_PRICE_TOLERANCE_PERCENT",
        value=tolerance_env_value,
        error=str(e),
    )
    message = f"Invalid ALPACA_PRICE_TOLERANCE_PERCENT: {tolerance_env_value}"
    raise ValueError(message) from e

alpaca_client = AlpacaClient(
    api_key=ALPACA_API_KEY_ID,
    api_secret=ALPACA_API_SECRET,
    is_paper=os.getenv("ALPACA_IS_PAPER", "true").lower() == "true",
    price_tolerance_percent=price_tolerance_percent,
)

logger.info("Portfolio manager initialized", is_paper=alpaca_client.is_paper)


@application.get("/health")
def health_check() -> Response:
    return Response(status_code=status.HTTP_200_OK)


@application.post("/portfolio")
def create_portfolio() -> Response:  # noqa: PLR0911, PLR0912, PLR0915, C901
    current_timestamp = datetime.now(tz=UTC)
    logger.info("Starting portfolio rebalance", timestamp=current_timestamp.isoformat())

    try:
        account = alpaca_client.get_account()
        logger.info(
            "Retrieved account",
            cash_amount=account.cash_amount,
            buying_power=account.buying_power,
        )
    except Exception as e:
        logger.exception("Failed to retrieve account", error=str(e))
        return Response(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR)

    try:
        current_predictions = get_current_predictions()
        logger.info("Retrieved predictions", count=len(current_predictions))
    except Exception as e:
        logger.exception("Failed to retrieve predictions", error=str(e))
        return Response(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR)

    try:
        prior_portfolio = get_prior_portfolio(current_timestamp=current_timestamp)
        logger.info("Retrieved prior portfolio", count=len(prior_portfolio))
    except Exception as e:
        logger.exception("Failed to retrieve prior portfolio", error=str(e))
        return Response(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR)

    try:
        optimal_portfolio = get_optimal_portfolio(
            current_predictions=current_predictions,
            prior_portfolio=prior_portfolio,
            maximum_capital=float(account.cash_amount),
            current_timestamp=current_timestamp,
        )
        logger.info("Created optimal portfolio", count=len(optimal_portfolio))
    except InsufficientPredictionsError as e:
        logger.warning(
            "Insufficient predictions to create portfolio, no trades will be made",
            error=str(e),
            uncertainty_threshold=UNCERTAINTY_THRESHOLD,
            predictions_count=len(current_predictions),
            prior_portfolio_count=len(prior_portfolio),
        )
        return Response(
            status_code=status.HTTP_204_NO_CONTENT,
            headers={"X-Portfolio-Status": "insufficient-predictions"},
        )
    except Exception as e:
        logger.exception("Failed to create optimal portfolio", error=str(e))
        return Response(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR)

    try:
        open_positions, close_positions = get_positions(
            prior_portfolio=prior_portfolio,
            optimal_portfolio=optimal_portfolio,
        )
        logger.info(
            "Determined positions to open and close",
            open_count=len(open_positions),
            close_count=len(close_positions),
        )
    except Exception as e:
        logger.exception(
            "Failed to determine positions to open and close",
            error=str(e),
        )
        return Response(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR)

    close_results = []
    for close_position in close_positions:
        try:
            alpaca_client.close_position(
                ticker=close_position["ticker"],
            )
            logger.info("Closed position", ticker=close_position["ticker"])
            close_results.append(
                {
                    "ticker": close_position["ticker"],
                    "action": "close",
                    "status": "success",
                }
            )
        except Exception as e:
            logger.exception(
                "Failed to close position",
                ticker=close_position["ticker"],
                error=str(e),
            )
            close_results.append(
                {
                    "ticker": close_position["ticker"],
                    "action": "close",
                    "status": "failed",
                    "error": str(e),
                }
            )

    open_results = []
    remaining_buying_power = account.buying_power
    skipped_insufficient_buying_power = 0
    skipped_not_shortable = 0

    for open_position in open_positions:
        ticker = open_position["ticker"]
        side = open_position["side"]
        dollar_amount = open_position["dollar_amount"]

        # Check if we have enough buying power before attempting the order
        if dollar_amount > remaining_buying_power:
            logger.warning(
                "Skipping position due to insufficient buying power",
                ticker=ticker,
                side=side,
                dollar_amount=dollar_amount,
                remaining_buying_power=remaining_buying_power,
            )
            skipped_insufficient_buying_power += 1
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
            # Refresh remaining buying power from the account after a successful order
            try:
                account = alpaca_client.get_account()
                # Alpaca typically returns buying_power as a string; convert to float
                remaining_buying_power = float(account.buying_power)
            except Exception:
                # If refreshing from the account fails for any reason,
                # fall back to deducting the estimated dollar_amount
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

    all_results = close_results + open_results
    failed_trades = [r for r in all_results if r["status"] == "failed"]

    logger.info(
        "Portfolio rebalance completed",
        total_trades=len(all_results),
        failed_trades=len(failed_trades),
    )

    if failed_trades:
        return Response(status_code=status.HTTP_207_MULTI_STATUS)

    return Response(status_code=status.HTTP_200_OK)


def get_current_predictions() -> pl.DataFrame:
    current_predictions_response = requests.post(
        url=f"{EQUITYPRICEMODEL_BASE_URL}/predictions",
        timeout=180,
    )

    current_predictions_response.raise_for_status()

    current_predictions = pl.DataFrame(current_predictions_response.json()["data"])

    return add_predictions_zscore_ranked_columns(
        current_predictions=current_predictions
    )


def get_prior_portfolio(current_timestamp: datetime) -> pl.DataFrame:  # noqa: PLR0911
    empty_portfolio = pl.DataFrame(
        {
            "ticker": [],
            "timestamp": [],
            "side": [],
            "dollar_amount": [],
            "action": [],
        }
    )

    prior_portfolio_response = requests.get(
        url=f"{DATAMANAGER_BASE_URL}/portfolios",
        timeout=60,
    )

    if prior_portfolio_response.status_code == 404:
        logger.info(
            "No prior portfolio found - this is expected on first run",
            status_code=prior_portfolio_response.status_code,
        )
        return empty_portfolio

    if prior_portfolio_response.status_code >= 400:
        logger.warning(
            "Failed to fetch prior portfolio from datamanager",
            status_code=prior_portfolio_response.status_code,
        )
        return empty_portfolio

    # Handle empty or invalid JSON response
    response_text = prior_portfolio_response.text.strip()
    if not response_text or response_text == "[]":
        logger.info("Prior portfolio response is empty")
        return empty_portfolio

    try:
        prior_portfolio_data = prior_portfolio_response.json()
    except (ValueError, requests.exceptions.JSONDecodeError) as e:
        logger.warning(
            "Failed to parse prior portfolio JSON",
            error=str(e),
            response_text=response_text[:200] if response_text else "empty",
        )
        return empty_portfolio

    if not prior_portfolio_data:
        logger.info("Prior portfolio data is empty")
        return empty_portfolio

    prior_portfolio = pl.DataFrame(prior_portfolio_data)

    if prior_portfolio.is_empty():
        return empty_portfolio

    tickers = prior_portfolio["ticker"].unique().to_list()
    timestamps = prior_portfolio["timestamp"].cast(pl.Float64)
    start_timestamp = datetime.fromtimestamp(cast("float", timestamps.min()), tz=UTC)

    prior_equity_bars_response = requests.get(
        url=f"{DATAMANAGER_BASE_URL}/equity-bars",
        params={
            "tickers": ",".join(tickers),
            "start_timestamp": start_timestamp.isoformat(),
            "end_timestamp": current_timestamp.isoformat(),
        },
        timeout=60,
    )

    prior_equity_bars_response.raise_for_status()

    tickers_and_timestamps = [
        {"ticker": row[0], "timestamp": row[1]}
        for row in prior_portfolio[["ticker", "timestamp"]].rows()
    ]

    prior_predictions_response = requests.get(
        url=f"{DATAMANAGER_BASE_URL}/predictions",
        params={"tickers_and_timestamps": json.dumps(tickers_and_timestamps)},
        timeout=60,
    )

    prior_predictions_response.raise_for_status()

    prior_portfolio = add_portfolio_action_column(
        prior_portfolio=prior_portfolio,
        current_timestamp=current_timestamp,
    )

    prior_equity_bars = pl.read_parquet(io.BytesIO(prior_equity_bars_response.content))

    prior_equity_bars = equity_bars_schema.validate(prior_equity_bars)

    prior_equity_bars = add_equity_bars_returns_and_realized_volatility_columns(
        prior_equity_bars=prior_equity_bars
    )

    predictions_response_text = prior_predictions_response.text.strip()
    if not predictions_response_text or predictions_response_text == "[]":
        logger.warning(
            "Prior predictions response is empty, returning portfolio without performance data"
        )
        return add_portfolio_action_column(
            prior_portfolio=prior_portfolio,
            current_timestamp=current_timestamp,
        )

    try:
        prior_predictions_data = prior_predictions_response.json()
    except (ValueError, requests.exceptions.JSONDecodeError) as error:
        logger.exception(
            "Failed to retrieve prior portfolio",
            error=str(error),
        )
        return add_portfolio_action_column(
            prior_portfolio=prior_portfolio,
            current_timestamp=current_timestamp,
        )

    if not prior_predictions_data:
        logger.warning("Prior predictions data is empty")
        return add_portfolio_action_column(
            prior_portfolio=prior_portfolio,
            current_timestamp=current_timestamp,
        )

    prior_predictions = pl.DataFrame(prior_predictions_data).with_columns(
        pl.col("timestamp").cast(pl.Float64)
    )

    return add_portfolio_performance_columns(
        prior_portfolio=prior_portfolio,
        prior_equity_bars=prior_equity_bars,
        prior_predictions=prior_predictions,
        current_timestamp=current_timestamp,
    )


def get_optimal_portfolio(
    current_predictions: pl.DataFrame,
    prior_portfolio: pl.DataFrame,
    maximum_capital: float,
    current_timestamp: datetime,
) -> pl.DataFrame:
    optimal_portfolio = create_optimal_portfolio(
        current_predictions=current_predictions,
        prior_portfolio=prior_portfolio,
        maximum_capital=maximum_capital,
        current_timestamp=current_timestamp,
    )

    optimal_portfolio = portfolio_schema.validate(optimal_portfolio)

    save_portfolio_response = requests.post(
        url=f"{DATAMANAGER_BASE_URL}/portfolios",
        json={
            "timestamp": current_timestamp.isoformat(),
            "data": optimal_portfolio.to_dicts(),
        },
        timeout=60,
    )

    save_portfolio_response.raise_for_status()

    return optimal_portfolio


def get_positions(
    prior_portfolio: pl.DataFrame,
    optimal_portfolio: pl.DataFrame,
) -> tuple[list[dict], list[dict]]:
    prior_portfolio = prior_portfolio.clone()
    optimal_portfolio = optimal_portfolio.clone()

    close_positions = []
    if not prior_portfolio.is_empty():
        prior_tickers = set(prior_portfolio["ticker"].to_list())
        optimal_tickers = set(optimal_portfolio["ticker"].to_list())
        closing_tickers = prior_tickers - optimal_tickers

        if closing_tickers:
            close_positions = [
                {
                    "ticker": row["ticker"],
                    "side": (
                        TradeSide.SELL
                        if row["side"] == PositionSide.LONG.value
                        else TradeSide.BUY
                    ),
                    "dollar_amount": row["dollar_amount"],
                }
                for row in prior_portfolio.filter(
                    pl.col("ticker").is_in(list(closing_tickers))
                ).iter_rows(named=True)
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
        for row in optimal_portfolio.filter(
            pl.col("action") == PositionAction.OPEN_POSITION.value
        ).iter_rows(named=True)
    ]

    return open_positions, close_positions
