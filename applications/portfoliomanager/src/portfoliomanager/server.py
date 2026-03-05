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

from .consolidation import consolidate_predictions  # noqa: E402
from .data_client import fetch_equity_details, fetch_historical_prices  # noqa: E402
from .enums import PositionSide, TradeSide  # noqa: E402
from .exceptions import (  # noqa: E402
    AssetNotShortableError,
    InsufficientBuyingPowerError,
    InsufficientPairsError,
)
from .portfolio_schema import pairs_schema, portfolio_schema  # noqa: E402
from .risk_management import size_pairs_with_volatility_parity  # noqa: E402
from .statistical_arbitrage import select_pairs  # noqa: E402

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


@application.post("/portfolio")
async def create_portfolio() -> Response:  # noqa: PLR0911, PLR0912, PLR0915, C901
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
        historical_prices = fetch_historical_prices(
            DATAMANAGER_BASE_URL, current_timestamp
        )
        equity_details = fetch_equity_details(DATAMANAGER_BASE_URL)
        logger.info(
            "Retrieved historical data",
            prices_count=historical_prices.height,
            equity_details_count=equity_details.height,
        )
    except Exception as e:
        logger.exception("Failed to retrieve historical data", error=str(e))
        return Response(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR)

    try:
        raw_predictions = await get_raw_predictions()
        logger.info("Retrieved predictions", count=len(raw_predictions))
    except Exception as e:
        logger.exception("Failed to retrieve predictions", error=str(e))
        return Response(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR)

    try:
        consolidated_signals = consolidate_predictions(
            model_predictions={"tide": raw_predictions},
            historical_prices=historical_prices,
            equity_details=equity_details,
        )
        logger.info("Consolidated signals", count=consolidated_signals.height)
    except Exception as e:
        logger.exception("Failed to consolidate predictions", error=str(e))
        return Response(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR)

    try:
        prior_portfolio_tickers = get_prior_portfolio_tickers()
        logger.info(
            "Retrieved prior portfolio tickers", count=len(prior_portfolio_tickers)
        )
    except Exception as e:
        logger.exception("Failed to retrieve prior portfolio tickers", error=str(e))
        return Response(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR)

    consolidated_signals = consolidated_signals.filter(
        ~pl.col("ticker").is_in(prior_portfolio_tickers)
    )

    try:
        shortable_tickers = alpaca_client.get_shortable_tickers(
            tickers=consolidated_signals["ticker"].to_list()
        )
        consolidated_signals = consolidated_signals.filter(
            pl.col("ticker").is_in(shortable_tickers)
        )
        logger.info("Filtered to shortable tickers", count=consolidated_signals.height)
    except Exception as e:
        logger.exception("Failed to retrieve shortable tickers", error=str(e))
        return Response(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR)

    try:
        candidate_pairs = select_pairs(
            consolidated_signals=consolidated_signals,
            historical_prices=historical_prices,
        )
        logger.info("Selected candidate pairs", count=candidate_pairs.height)
    except Exception as e:
        logger.exception("Failed to select candidate pairs", error=str(e))
        return Response(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR)

    if candidate_pairs.height > 0:
        try:
            candidate_pairs = pairs_schema.validate(candidate_pairs)
        except Exception as e:
            logger.exception("Candidate pairs failed schema validation", error=str(e))
            return Response(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR)

    regime = get_regime_state()
    logger.info("Current market regime", state=regime)

    try:
        optimal_portfolio = get_optimal_portfolio(
            candidate_pairs=candidate_pairs,
            maximum_capital=float(account.cash_amount),
            current_timestamp=current_timestamp,
        )
        logger.info("Created optimal portfolio", count=len(optimal_portfolio))
    except InsufficientPairsError as e:
        logger.warning(
            "Insufficient pairs to create portfolio, no trades will be made",
            error=str(e),
            candidate_pairs_count=candidate_pairs.height,
        )
        return Response(
            status_code=status.HTTP_204_NO_CONTENT,
            headers={"X-Portfolio-Status": "insufficient-pairs"},
        )
    except Exception as e:
        logger.exception("Failed to create optimal portfolio", error=str(e))
        return Response(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR)

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

    close_results = []
    for close_position in close_positions:
        try:
            was_closed = alpaca_client.close_position(
                ticker=close_position["ticker"],
            )
            if was_closed:
                logger.info("Closed position", ticker=close_position["ticker"])
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
                close_results.append(
                    {
                        "ticker": close_position["ticker"],
                        "action": "close",
                        "status": "skipped",
                        "reason": "position_not_found",
                    }
                )
        except Exception as e:  # noqa: PERF203
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


async def get_raw_predictions() -> pl.DataFrame:
    async with httpx.AsyncClient(timeout=60.0) as client:
        response = await client.post(
            url=f"{EQUITYPRICEMODEL_BASE_URL}/predictions",
        )
        response.raise_for_status()
        return pl.DataFrame(response.json()["data"])


def get_regime_state() -> str:
    """TODO: replace with regime.classify_regime in Phase 4."""
    return "mean_reversion"


def get_prior_portfolio_tickers() -> list[str]:  # noqa: PLR0911
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
                "Failed to fetch prior portfolio from data manager",
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
    candidate_pairs: pl.DataFrame,
    maximum_capital: float,
    current_timestamp: datetime,
) -> pl.DataFrame:
    optimal_portfolio = size_pairs_with_volatility_parity(
        candidate_pairs=candidate_pairs,
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
