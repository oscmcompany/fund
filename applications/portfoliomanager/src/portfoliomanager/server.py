import logging
import os
from datetime import UTC, datetime

import httpx
import polars as pl
import requests
import sentry_sdk
import structlog
from fastapi import FastAPI, Response, status
from sentry_sdk.integrations.logging import LoggingIntegration

from .alpaca_client import AlpacaAccount, AlpacaClient

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
            event_level=logging.WARNING,
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

from .enums import PositionSide, TradeSide  # noqa: E402
from .exceptions import (  # noqa: E402
    AssetNotShortableError,
    InsufficientBuyingPowerError,
    InsufficientPredictionsError,
)
from .portfolio_schema import portfolio_schema  # noqa: E402
from .risk_management import (  # noqa: E402
    UNCERTAINTY_THRESHOLD,
    add_predictions_zscore_ranked_columns,
    create_optimal_portfolio,
)

logger = structlog.get_logger()

application: FastAPI = FastAPI()

DATAMANAGER_BASE_URL = os.getenv("FUND_DATAMANAGER_BASE_URL", "http://datamanager:8080")
HTTP_NOT_FOUND = 404
HTTP_BAD_REQUEST = 400
EQUITYPRICEMODEL_BASE_URL = os.getenv(
    "FUND_EQUITYPRICEMODEL_BASE_URL",
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

alpaca_client = AlpacaClient(
    api_key=ALPACA_API_KEY_ID,
    api_secret=ALPACA_API_SECRET,
    is_paper=os.getenv("ALPACA_IS_PAPER", "true").lower() == "true",
)

logger.info("Portfolio manager initialized", is_paper=alpaca_client.is_paper)


@application.get("/health")
def health_check() -> Response:
    return Response(status_code=status.HTTP_200_OK)


def _close_single_position(close_position: dict) -> dict:
    """Close a single position and return the result."""
    try:
        was_closed = alpaca_client.close_position(
            ticker=close_position["ticker"],
        )
        if was_closed:
            logger.info("Closed position", ticker=close_position["ticker"])
            return {
                "ticker": close_position["ticker"],
                "action": "close",
                "status": "success",
            }
        logger.info(
            "Position already closed or did not exist",
            ticker=close_position["ticker"],
        )
        return {
            "ticker": close_position["ticker"],
            "action": "close",
            "status": "skipped",
            "reason": "position_not_found",
        }
    except Exception as e:
        logger.exception(
            "Failed to close position",
            ticker=close_position["ticker"],
            error=str(e),
        )
        return {
            "ticker": close_position["ticker"],
            "action": "close",
            "status": "failed",
            "error": str(e),
        }


def _close_all_positions(close_positions: list[dict]) -> list[dict]:
    """Close all positions and return results."""
    return [_close_single_position(position) for position in close_positions]


def _open_single_position(
    open_position: dict, remaining_buying_power: float
) -> tuple[dict, float, str | None]:
    """
    Open a single position and return the result, updated buying power, and skip reason.

    Returns:
        tuple: (result_dict, new_buying_power, skip_reason)
        skip_reason is "insufficient_buying_power", "not_shortable", or None
    """
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
        return (
            {
                "ticker": ticker,
                "action": "open",
                "side": side,
                "dollar_amount": dollar_amount,
                "status": "skipped",
                "reason": "insufficient_buying_power",
            },
            remaining_buying_power,
            "insufficient_buying_power",
        )

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

        return (  # noqa: TRY300
            {
                "ticker": ticker,
                "action": "open",
                "side": side,
                "dollar_amount": dollar_amount,
                "status": "success",
            },
            remaining_buying_power,
            None,
        )
    except InsufficientBuyingPowerError as e:
        logger.warning(
            "Insufficient buying power for position",
            ticker=ticker,
            side=side,
            dollar_amount=dollar_amount,
            error=str(e),
        )
        return (
            {
                "ticker": ticker,
                "action": "open",
                "side": side,
                "dollar_amount": dollar_amount,
                "status": "skipped",
                "reason": "insufficient_buying_power",
            },
            remaining_buying_power,
            "insufficient_buying_power",
        )
    except AssetNotShortableError as e:
        logger.warning(
            "Asset cannot be sold short",
            ticker=ticker,
            side=side,
            error=str(e),
        )
        return (
            {
                "ticker": ticker,
                "action": "open",
                "side": side,
                "dollar_amount": dollar_amount,
                "status": "skipped",
                "reason": "not_shortable",
            },
            remaining_buying_power,
            "not_shortable",
        )
    except Exception as e:
        logger.exception(
            "Failed to open position",
            ticker=ticker,
            error=str(e),
        )
        return (
            {
                "ticker": ticker,
                "action": "open",
                "side": side,
                "dollar_amount": dollar_amount,
                "status": "failed",
                "error": str(e),
            },
            remaining_buying_power,
            None,
        )


def _open_all_positions(
    open_positions: list[dict], initial_buying_power: float
) -> tuple[list[dict], int, int]:
    """
    Open all positions and return results with skip counts.

    Returns:
        tuple: (results, skipped_insufficient_buying_power, skipped_not_shortable)
    """
    open_results = []
    remaining_buying_power = initial_buying_power
    skipped_insufficient_buying_power = 0
    skipped_not_shortable = 0

    for open_position in open_positions:
        result, remaining_buying_power, skip_reason = _open_single_position(
            open_position, remaining_buying_power
        )
        open_results.append(result)

        if skip_reason == "insufficient_buying_power":
            skipped_insufficient_buying_power += 1
        elif skip_reason == "not_shortable":
            skipped_not_shortable += 1

    if skipped_insufficient_buying_power > 0 or skipped_not_shortable > 0:
        logger.info(
            "Some positions were skipped",
            skipped_insufficient_buying_power=skipped_insufficient_buying_power,
            skipped_not_shortable=skipped_not_shortable,
        )

    return open_results, skipped_insufficient_buying_power, skipped_not_shortable


async def _prepare_portfolio_data(
    current_timestamp: datetime,
) -> tuple[AlpacaAccount, pl.DataFrame, list[str], pl.DataFrame] | Response:
    """
    Prepare all data needed for portfolio rebalancing.

    Returns either a tuple of (account, predictions, tickers, portfolio)
    or an error Response.
    """
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
        current_predictions = await get_current_predictions()
        logger.info("Retrieved predictions", count=len(current_predictions))
    except Exception as e:
        logger.exception("Failed to retrieve predictions", error=str(e))
        return Response(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR)

    try:
        prior_portfolio_tickers = get_prior_portfolio_tickers()
        logger.info(
            "Retrieved prior portfolio tickers", count=len(prior_portfolio_tickers)
        )
    except Exception as e:
        logger.exception("Failed to retrieve prior portfolio tickers", error=str(e))
        return Response(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR)

    try:
        optimal_portfolio = get_optimal_portfolio(
            current_predictions=current_predictions,
            prior_portfolio_tickers=prior_portfolio_tickers,
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
            prior_portfolio_tickers_count=len(prior_portfolio_tickers),
        )
        return Response(
            status_code=status.HTTP_204_NO_CONTENT,
            headers={"X-Portfolio-Status": "insufficient-predictions"},
        )
    except Exception as e:
        logger.exception("Failed to create optimal portfolio", error=str(e))
        return Response(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR)

    return account, current_predictions, prior_portfolio_tickers, optimal_portfolio


@application.post("/portfolio")
async def create_portfolio() -> Response:
    current_timestamp = datetime.now(tz=UTC)
    logger.info("Starting portfolio rebalance", timestamp=current_timestamp.isoformat())

    portfolio_data = await _prepare_portfolio_data(current_timestamp)
    if isinstance(portfolio_data, Response):
        return portfolio_data

    account, _, prior_portfolio_tickers, optimal_portfolio = portfolio_data

    try:
        open_positions, close_positions = get_positions(
            prior_portfolio_tickers=prior_portfolio_tickers,
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

    close_results = _close_all_positions(close_positions)
    open_results, _, _ = _open_all_positions(open_positions, account.buying_power)

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


async def get_current_predictions() -> pl.DataFrame:
    async with httpx.AsyncClient(timeout=60.0) as client:
        current_predictions_response = await client.post(
            url=f"{EQUITYPRICEMODEL_BASE_URL}/predictions",
        )

        current_predictions_response.raise_for_status()

        current_predictions = pl.DataFrame(current_predictions_response.json()["data"])

        return add_predictions_zscore_ranked_columns(
            current_predictions=current_predictions
        )


def get_prior_portfolio_tickers() -> list[str]:  # noqa: PLR0911
    """Fetch tickers from prior portfolio to exclude them (PDT avoidance)."""
    try:
        prior_portfolio_response = requests.get(
            url=f"{DATAMANAGER_BASE_URL}/portfolios",
            timeout=60,
        )

        # If no prior portfolio, return empty list
        if prior_portfolio_response.status_code == HTTP_NOT_FOUND:
            logger.info("No prior portfolio found, starting fresh")
            return []

        if prior_portfolio_response.status_code >= HTTP_BAD_REQUEST:
            logger.warning(
                "Failed to fetch prior portfolio from datamanager",
                status_code=prior_portfolio_response.status_code,
            )
            return []

        response_text = prior_portfolio_response.text.strip()
        if not response_text or response_text == "[]":
            logger.info("Prior portfolio is empty")
            return []

        prior_portfolio_data = prior_portfolio_response.json()

        if not prior_portfolio_data:
            return []

        prior_portfolio = pl.DataFrame(prior_portfolio_data)

        if prior_portfolio.is_empty():
            return []

        tickers = prior_portfolio["ticker"].unique().to_list()
        logger.info("Retrieved prior portfolio tickers", count=len(tickers))
        return tickers  # noqa: TRY300

    except (ValueError, requests.exceptions.JSONDecodeError) as e:
        logger.exception("Failed to parse prior portfolio JSON", error=str(e))
        return []


def get_optimal_portfolio(
    current_predictions: pl.DataFrame,
    prior_portfolio_tickers: list[str],
    maximum_capital: float,
    current_timestamp: datetime,
) -> pl.DataFrame:
    """Create optimal portfolio with prediction ranking and ticker exclusion."""
    optimal_portfolio = create_optimal_portfolio(
        current_predictions=current_predictions,
        prior_portfolio_tickers=prior_portfolio_tickers,
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
    prior_portfolio_tickers: list[str],
    optimal_portfolio: pl.DataFrame,
) -> tuple[list[dict], list[dict]]:
    """Get positions to close and open."""
    close_positions = [{"ticker": ticker} for ticker in prior_portfolio_tickers]

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
