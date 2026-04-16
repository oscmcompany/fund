import os
from datetime import UTC, datetime

import httpx
import polars as pl
import structlog
from fastapi import Response, status

from . import metrics
from .alpaca_client import AlpacaClient
from .beta import compute_market_betas
from .consolidation import consolidate_predictions
from .data_client import fetch_equity_details, fetch_historical_prices, fetch_spy_prices
from .exceptions import InsufficientPairsError
from .portfolio_schema import pairs_schema, portfolio_schema
from .portfolio_state import (
    DATA_MANAGER_BASE_URL,
    evaluate_prior_pairs,
    get_prior_portfolio,
    save_portfolio,
)
from .regime import classify_regime
from .risk_management import size_pairs_with_volatility_parity
from .statistical_arbitrage import select_pairs
from .trade_execution import (
    execute_close_positions,
    execute_open_positions,
    get_positions,
)

logger = structlog.get_logger()

ENSEMBLE_MANAGER_BASE_URL = os.getenv(
    "FUND_ENSEMBLE_MANAGER_BASE_URL",
    "http://ensemble-manager:8080",
)


async def run_rebalance(alpaca_client: AlpacaClient) -> Response:  # noqa: PLR0911, PLR0912, PLR0915, C901
    metrics.rebalance_requests_total.inc()
    start = metrics.start_timer()
    current_timestamp = datetime.now(tz=UTC)
    logger.info("Starting portfolio rebalance", timestamp=current_timestamp.isoformat())

    try:
        account = alpaca_client.get_account()
        logger.info(
            "Retrieved account",
            cash_amount=account.cash_amount,
            buying_power=account.buying_power,
        )
        metrics.account_cash.set(float(account.cash_amount))
        metrics.account_buying_power.set(float(account.buying_power))
    except Exception as e:
        logger.exception("Failed to retrieve account", error=str(e))
        metrics.rebalance_errors_total.labels(stage="account").inc()
        metrics.observe_duration(start)
        return Response(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR)

    try:
        historical_prices = fetch_historical_prices(
            DATA_MANAGER_BASE_URL, current_timestamp
        )
        equity_details = fetch_equity_details(DATA_MANAGER_BASE_URL)
        spy_prices = fetch_spy_prices(DATA_MANAGER_BASE_URL, current_timestamp)
        logger.info(
            "Retrieved historical data",
            prices_count=historical_prices.height,
            equity_details_count=equity_details.height,
            spy_prices_count=spy_prices.height,
        )
    except Exception as e:
        logger.exception("Failed to retrieve historical data", error=str(e))
        metrics.rebalance_errors_total.labels(stage="historical_data").inc()
        metrics.observe_duration(start)
        return Response(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR)

    try:
        ensemble_predictions = await get_raw_predictions()
        logger.info("Retrieved predictions", count=len(ensemble_predictions))
    except Exception as e:
        logger.exception("Failed to retrieve predictions", error=str(e))
        metrics.rebalance_errors_total.labels(stage="predictions").inc()
        metrics.observe_duration(start)
        return Response(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR)

    try:
        consolidated_signals = consolidate_predictions(
            model_predictions={"tide": ensemble_predictions},
            historical_prices=historical_prices,
            equity_details=equity_details,
        )
        logger.info("Consolidated signals", count=consolidated_signals.height)
    except Exception as e:
        logger.exception("Failed to consolidate predictions", error=str(e))
        metrics.rebalance_errors_total.labels(stage="consolidate_signals").inc()
        metrics.observe_duration(start)
        return Response(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR)

    try:
        prior_portfolio = await get_prior_portfolio()
        prior_portfolio_tickers = prior_portfolio["ticker"].unique().to_list()
        logger.info("Retrieved prior portfolio", count=len(prior_portfolio_tickers))
    except Exception as e:
        logger.exception("Failed to retrieve prior portfolio", error=str(e))
        metrics.rebalance_errors_total.labels(stage="prior_portfolio").inc()
        metrics.observe_duration(start)
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
        metrics.rebalance_errors_total.labels(stage="evaluate_pairs").inc()
        metrics.observe_duration(start)
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
        metrics.rebalance_errors_total.labels(stage="shortable_tickers").inc()
        metrics.observe_duration(start)
        return Response(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR)

    try:
        candidate_pairs = select_pairs(
            consolidated_signals=consolidated_signals,
            historical_prices=historical_prices,
        )
        logger.info("Selected candidate pairs", count=candidate_pairs.height)
    except Exception as e:
        logger.exception("Failed to select candidate pairs", error=str(e))
        metrics.rebalance_errors_total.labels(stage="candidate_pairs").inc()
        metrics.observe_duration(start)
        return Response(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR)

    try:
        candidate_pairs = pairs_schema.validate(candidate_pairs)
        metrics.pairs_selected_count.set(candidate_pairs.height)
    except Exception as e:
        logger.exception("Candidate pairs failed schema validation", error=str(e))
        metrics.rebalance_errors_total.labels(stage="pairs_schema").inc()
        metrics.observe_duration(start)
        return Response(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR)

    try:
        market_betas = compute_market_betas(historical_prices, spy_prices)
        regime = classify_regime(spy_prices)
        # Binary scale is intentional; confidence reserved for future graduated scaling.
        exposure_scale = 1.0 if regime["state"] == "mean_reversion" else 0.5
        metrics.regime_state.set(int(regime["state"] == "mean_reversion"))
        metrics.exposure_scale_value.set(exposure_scale)
        logger.info(
            "Computed market betas and regime",
            regime_state=regime["state"],
            regime_confidence=regime["confidence"],
            exposure_scale=exposure_scale,
        )
    except Exception as e:
        logger.exception("Failed to compute market betas or regime", error=str(e))
        metrics.rebalance_errors_total.labels(stage="market_regime").inc()
        metrics.observe_duration(start)
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
        metrics.observe_duration(start)
        return Response(
            status_code=status.HTTP_200_OK,
            content="Insufficient pairs to create portfolio, no trades will be made",
            media_type="text/plain",
        )
    except Exception as e:
        logger.exception("Failed to create optimal portfolio", error=str(e))
        metrics.rebalance_errors_total.labels(stage="optimal_portfolio").inc()
        metrics.observe_duration(start)
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
        metrics.rebalance_errors_total.labels(stage="positions").inc()
        metrics.observe_duration(start)
        return Response(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR)

    close_results, closed_count = execute_close_positions(
        alpaca_client, close_positions
    )
    open_results, opened_count = execute_open_positions(
        alpaca_client, open_positions, account.buying_power
    )

    metrics.positions_opened_count.set(opened_count)
    metrics.positions_closed_count.set(closed_count)

    held_rows = prior_portfolio.filter(pl.col("ticker").is_in(held_tickers))
    final_portfolio = pl.concat([optimal_portfolio, held_rows])
    save_succeeded = await save_portfolio(final_portfolio, current_timestamp)

    all_results = close_results + open_results
    failed_trades = [r for r in all_results if r["status"] == "failed"]

    logger.info(
        "Portfolio rebalance completed",
        total_trades=len(all_results),
        failed_trades=len(failed_trades),
        save_succeeded=save_succeeded,
    )

    metrics.observe_duration(start)

    if failed_trades or not save_succeeded:
        return Response(status_code=status.HTTP_207_MULTI_STATUS)

    return Response(status_code=status.HTTP_200_OK)


async def get_raw_predictions() -> pl.DataFrame:
    async with httpx.AsyncClient(timeout=60.0) as client:
        response = await client.post(
            url=f"{ENSEMBLE_MANAGER_BASE_URL}/predictions",
        )
        response.raise_for_status()
        return pl.DataFrame(response.json()["data"])


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

    return portfolio_schema.validate(optimal_portfolio)
