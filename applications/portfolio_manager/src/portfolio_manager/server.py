import logging
import os
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from datetime import UTC, datetime

import httpx
import numpy as np
import polars as pl
import requests
import sentry_sdk
import structlog
from fastapi import FastAPI, Response, status
from sentry_sdk.integrations.logging import LoggingIntegration

from .alpaca_client import AlpacaClient

sentry_sdk.init(
    dsn=os.environ.get("SENTRY_DSN"),
    environment=os.environ.get("FUND_ENVIRONMENT", "development"),
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

from .beta import compute_market_betas  # noqa: E402
from .consolidation import consolidate_predictions  # noqa: E402
from .data_client import (  # noqa: E402
    fetch_equity_details,
    fetch_historical_prices,
    fetch_spy_prices,
)
from .enums import PositionSide, TradeSide  # noqa: E402
from .exceptions import (  # noqa: E402
    AssetNotShortableError,
    InsufficientBuyingPowerError,
    InsufficientPairsError,
)
from .portfolio_schema import pairs_schema, portfolio_schema  # noqa: E402
from .regime import classify_regime  # noqa: E402
from .risk_management import (  # noqa: E402
    Z_SCORE_HOLD_THRESHOLD,
    Z_SCORE_STOP_LOSS,
    size_pairs_with_volatility_parity,
)
from .statistical_arbitrage import (  # noqa: E402
    CORRELATION_WINDOW_DAYS,
    compute_spread_zscore,
    select_pairs,
)

logger = structlog.get_logger()

DATAMANAGER_BASE_URL = os.getenv("FUND_DATAMANAGER_BASE_URL", "http://datamanager:8080")
HTTP_BAD_REQUEST = 400
_MINIMUM_PAIR_PRICE_ROWS = 30
EQUITYPRICEMODEL_BASE_URL = os.getenv(
    "FUND_EQUITYPRICEMODEL_BASE_URL",
    "http://equitypricemodel:8080",
)

ALPACA_API_KEY_ID = os.getenv("ALPACA_API_KEY_ID", "")
ALPACA_API_SECRET = os.getenv("ALPACA_API_SECRET", "")


@asynccontextmanager
async def _lifespan(_app: FastAPI) -> AsyncGenerator[None, None]:
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
    _app.state.alpaca_client = AlpacaClient(
        api_key=ALPACA_API_KEY_ID,
        api_secret=ALPACA_API_SECRET,
        is_paper=os.getenv("ALPACA_IS_PAPER", "true").lower() == "true",
    )
    logger.info(
        "Portfolio manager initialized", is_paper=_app.state.alpaca_client.is_paper
    )
    yield


application: FastAPI = FastAPI(lifespan=_lifespan)

_PRIOR_PORTFOLIO_SCHEMA: dict[str, type] = {
    "ticker": pl.String,
    "timestamp": pl.Float64,
    "side": pl.String,
    "dollar_amount": pl.Float64,
    "action": pl.String,
    "pair_id": pl.String,
}


@application.get("/health")
def health_check() -> Response:
    return Response(status_code=status.HTTP_200_OK)


@application.post("/portfolio")
async def create_portfolio() -> Response:  # noqa: PLR0911, PLR0912, PLR0915, C901
    alpaca_client: AlpacaClient = application.state.alpaca_client
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
        spy_prices = fetch_spy_prices(DATAMANAGER_BASE_URL, current_timestamp)
        logger.info(
            "Retrieved historical data",
            prices_count=historical_prices.height,
            equity_details_count=equity_details.height,
            spy_prices_count=spy_prices.height,
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
        prior_portfolio = get_prior_portfolio()
        prior_portfolio_tickers = prior_portfolio["ticker"].unique().to_list()
        logger.info("Retrieved prior portfolio", count=len(prior_portfolio_tickers))
    except Exception as e:
        logger.exception("Failed to retrieve prior portfolio", error=str(e))
        return Response(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR)

    try:
        held_tickers = evaluate_prior_pairs(prior_portfolio, historical_prices)
        logger.info(
            "Evaluated prior pairs",
            held_count=len(held_tickers),
            closing_count=len(prior_portfolio_tickers) - len(held_tickers),
        )
    except Exception as e:
        logger.exception("Failed to evaluate prior pairs", error=str(e))
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

    try:
        market_betas = compute_market_betas(historical_prices, spy_prices)
        regime = classify_regime(spy_prices)
        # Binary scale is intentional; confidence reserved for future graduated scaling.
        exposure_scale = 1.0 if regime["state"] == "mean_reversion" else 0.5
        logger.info(
            "Computed market betas and regime",
            regime_state=regime["state"],
            regime_confidence=regime["confidence"],
            exposure_scale=exposure_scale,
        )
    except Exception as e:
        logger.exception("Failed to compute market betas or regime", error=str(e))
        return Response(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR)

    try:
        optimal_portfolio = get_optimal_portfolio(
            candidate_pairs=candidate_pairs,
            maximum_capital=float(account.cash_amount),
            current_timestamp=current_timestamp,
            market_betas=market_betas,
            exposure_scale=exposure_scale,
        )
        logger.info("Created optimal portfolio", count=len(optimal_portfolio))
    except InsufficientPairsError as e:
        logger.warning(
            "Insufficient pairs to create portfolio, no trades will be made",
            error=str(e),
            candidate_pairs_count=candidate_pairs.height,
        )
        return Response(
            status_code=status.HTTP_200_OK,
            content="Insufficient pairs to create portfolio, no trades will be made",
            media_type="text/plain",
        )
    except Exception as e:
        logger.exception("Failed to create optimal portfolio", error=str(e))
        return Response(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR)

    try:
        open_positions, close_positions = get_positions(
            prior_portfolio_tickers=prior_portfolio_tickers,
            held_tickers=held_tickers,
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


def get_prior_portfolio() -> pl.DataFrame:
    empty = pl.DataFrame(schema=_PRIOR_PORTFOLIO_SCHEMA)
    try:
        response = requests.get(
            url=f"{DATAMANAGER_BASE_URL}/portfolios",
            timeout=60,
        )

        if response.status_code >= HTTP_BAD_REQUEST:
            logger.warning(
                "Failed to fetch prior portfolio from data manager",
                status_code=response.status_code,
            )
            return empty

        response_text = response.text.strip()
        if not response_text or response_text == "[]":
            logger.info("Prior portfolio is empty")
            return empty

        prior_portfolio_data = response.json()

        if not prior_portfolio_data:
            return empty

        prior_portfolio = pl.DataFrame(
            prior_portfolio_data, schema=_PRIOR_PORTFOLIO_SCHEMA
        )

        if prior_portfolio.is_empty():
            return empty

        logger.info("Retrieved prior portfolio", count=prior_portfolio.height)
        return prior_portfolio  # noqa: TRY300

    except (
        ValueError,
        requests.exceptions.JSONDecodeError,
        pl.exceptions.PolarsError,
    ) as e:
        logger.exception("Failed to parse prior portfolio JSON", error=str(e))
        return empty


def evaluate_prior_pairs(
    prior_portfolio: pl.DataFrame,
    historical_prices: pl.DataFrame,
) -> set[str]:
    held_tickers: set[str] = set()

    if prior_portfolio.is_empty():
        return held_tickers

    pair_ids = prior_portfolio["pair_id"].unique(maintain_order=False).sort().to_list()

    for pair_id_value in pair_ids:
        pair_rows = prior_portfolio.filter(pl.col("pair_id") == pair_id_value)

        long_rows = pair_rows.filter(pl.col("side") == PositionSide.LONG.value)
        short_rows = pair_rows.filter(pl.col("side") == PositionSide.SHORT.value)

        if long_rows.is_empty() or short_rows.is_empty():
            logger.warning(
                "Malformed prior pair, closing normally", pair_id=pair_id_value
            )
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
                pair_id=pair_id_value,
            )
            continue

        pair_price_matrix = pair_price_matrix.tail(CORRELATION_WINDOW_DAYS)

        if pair_price_matrix.height < _MINIMUM_PAIR_PRICE_ROWS:
            logger.warning(
                "Insufficient price history for prior pair, closing normally",
                pair_id=pair_id_value,
            )
            continue

        long_prices = pair_price_matrix[long_ticker].to_numpy()
        short_prices = pair_price_matrix[short_ticker].to_numpy()

        if np.any(long_prices <= 0) or np.any(short_prices <= 0):
            logger.warning(
                "Non-positive prices in prior pair, closing normally",
                pair_id=pair_id_value,
            )
            continue

        log_prices_long = np.log(long_prices)
        log_prices_short = np.log(short_prices)

        current_z, _ = compute_spread_zscore(log_prices_long, log_prices_short)

        if np.isnan(current_z):
            logger.warning(
                "NaN z-score for prior pair, closing normally",
                pair_id=pair_id_value,
            )
            continue

        abs_z = abs(current_z)

        if Z_SCORE_HOLD_THRESHOLD <= abs_z < Z_SCORE_STOP_LOSS:
            held_tickers.add(long_ticker)
            held_tickers.add(short_ticker)
            logger.info(
                "Holding prior pair, spread still mean-reverting",
                pair_id=pair_id_value,
                z_score=current_z,
            )
        elif abs_z < Z_SCORE_HOLD_THRESHOLD:
            logger.info(
                "Closing prior pair to realize profit, spread converged",
                pair_id=pair_id_value,
                z_score=current_z,
            )
        else:
            logger.info(
                "Closing prior pair on stop-loss, spread diverged",
                pair_id=pair_id_value,
                z_score=current_z,
            )

    return held_tickers


def get_optimal_portfolio(
    candidate_pairs: pl.DataFrame,
    maximum_capital: float,
    current_timestamp: datetime,
    market_betas: pl.DataFrame,
    exposure_scale: float,
) -> pl.DataFrame:
    optimal_portfolio = size_pairs_with_volatility_parity(
        candidate_pairs=candidate_pairs,
        maximum_capital=maximum_capital,
        current_timestamp=current_timestamp,
        market_betas=market_betas,
        exposure_scale=exposure_scale,
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
