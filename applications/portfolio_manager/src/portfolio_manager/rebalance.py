import os
from datetime import UTC, datetime

import httpx
import polars as pl
import structlog
from fastapi import Response, status

from . import metrics
from .alpaca_client import AlpacaAccount, AlpacaClient
from .beta import compute_market_betas
from .consolidation import consolidate_predictions
from .data_client import fetch_equity_details, fetch_historical_prices, fetch_spy_prices
from .exceptions import InsufficientPairsError
from .performance import (
    build_closed_pair_record,
    build_performance_snapshot,
    compute_period_return,
    compute_portfolio_value,
    compute_realized_profit_and_loss,
)
from .portfolio_schema import pairs_schema, portfolio_schema
from .portfolio_state import (
    DATA_MANAGER_BASE_URL,
    evaluate_prior_pairs,
    get_last_portfolio_value,
    get_prior_portfolio,
    save_closed_pair,
    save_performance_snapshot,
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

    latest_prices = (
        historical_prices.sort("timestamp", descending=True)
        .group_by("ticker")
        .agg(pl.col("close_price").first().alias("entry_price"))
    )
    optimal_portfolio = optimal_portfolio.join(latest_prices, on="ticker", how="left")

    null_entry_tickers = optimal_portfolio.filter(
        pl.col("entry_price").is_null()
    )["ticker"].to_list()
    if null_entry_tickers:
        for ticker in null_entry_tickers:
            logger.warning(
                "Missing entry price after join, excluding ticker from portfolio",
                ticker=ticker,
            )
        optimal_portfolio = optimal_portfolio.filter(
            pl.col("entry_price").is_not_null()
        )

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

    try:
        await _record_performance(
            prior_portfolio=prior_portfolio,
            held_tickers=held_tickers,
            final_portfolio=final_portfolio,
            historical_prices=historical_prices,
            spy_prices=spy_prices,
            account=account,
            current_timestamp=current_timestamp,
        )
    except Exception as e:
        logger.exception("Failed to record performance metrics", error=str(e))

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


async def _record_performance(  # noqa: PLR0913
    prior_portfolio: pl.DataFrame,
    held_tickers: set[str],
    final_portfolio: pl.DataFrame,
    historical_prices: pl.DataFrame,
    spy_prices: pl.DataFrame,
    account: AlpacaAccount,
    current_timestamp: datetime,
) -> None:
    current_prices = (
        historical_prices.sort("timestamp", descending=True)
        .group_by("ticker")
        .agg(pl.col("close_price").first())
    )

    closing_tickers = set(prior_portfolio["ticker"].to_list()) - held_tickers
    closing_pair_ids = (
        prior_portfolio.filter(pl.col("ticker").is_in(closing_tickers))["pair_id"]
        .unique()
        .to_list()
    )

    for pair_id in closing_pair_ids:
        pair_rows = prior_portfolio.filter(pl.col("pair_id") == pair_id)

        if "entry_price" not in pair_rows.columns or pair_rows[
            "entry_price"
        ].is_null().any():
            logger.warning(
                "Prior pair missing entry_price, skipping pnl calculation",
                pair_id=pair_id,
            )
            continue

        long_rows = pair_rows.filter(pl.col("side") == "LONG")
        short_rows = pair_rows.filter(pl.col("side") == "SHORT")

        if long_rows.is_empty() or short_rows.is_empty():
            continue

        realized_profit_and_loss, return_percent = compute_realized_profit_and_loss(
            pair_rows, current_prices
        )
        dollar_amount = float(pair_rows["dollar_amount"].sum())
        long_ticker = long_rows["ticker"][0]
        short_ticker = short_rows["ticker"][0]
        entry_timestamp = int(pair_rows["timestamp"][0])
        closed_timestamp = int(current_timestamp.timestamp() * 1000)

        record = build_closed_pair_record(
            pair_id=pair_id,
            long_ticker=long_ticker,
            short_ticker=short_ticker,
            entry_timestamp=entry_timestamp,
            closed_timestamp=closed_timestamp,
            dollar_amount=dollar_amount,
            realized_profit_and_loss=realized_profit_and_loss,
            return_percent=return_percent,
        )
        pair_saved = await save_closed_pair(record, current_timestamp)
        if not pair_saved:
            logger.warning("Failed to persist closed pair record", pair_id=pair_id)

    cash = float(account.cash_amount)
    portfolio_value = compute_portfolio_value(final_portfolio, current_prices, cash)
    previous_value = await get_last_portfolio_value()
    period_return = (
        compute_period_return(portfolio_value, previous_value)
        if previous_value is not None
        else 0.0
    )

    spy_close = 0.0
    if not spy_prices.is_empty():
        latest_spy = spy_prices.sort("timestamp", descending=True).head(1)
        spy_close = (
            float(latest_spy["close_price"][0])
            if latest_spy["close_price"][0] is not None
            else 0.0
        )

    open_pair_count = len(final_portfolio["pair_id"].unique().to_list())

    snapshot = build_performance_snapshot(
        portfolio_value=portfolio_value,
        cash=cash,
        spy_close=spy_close,
        period_return=period_return,
        open_pair_count=open_pair_count,
        timestamp=current_timestamp,
    )
    snapshot_saved = await save_performance_snapshot(snapshot, current_timestamp)
    if not snapshot_saved:
        logger.warning("Failed to persist performance snapshot")
