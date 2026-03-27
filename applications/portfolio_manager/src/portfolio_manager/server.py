import logging
import os
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

import sentry_sdk
import structlog
from fastapi import FastAPI, Response, status
from sentry_sdk.integrations.logging import LoggingIntegration

from .alpaca_client import AlpacaClient
from .metrics import (
    get_metrics,
)
from .rebalance import DATA_MANAGER_BASE_URL, run_rebalance
from .scheduler import spawn_rebalance_scheduler

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

logger = structlog.get_logger()

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
    _app.state.scheduler_task = await spawn_rebalance_scheduler(
        alpaca_client=_app.state.alpaca_client,
        data_manager_base_url=DATA_MANAGER_BASE_URL,
    )
    logger.info("Portfolio rebalance scheduler started")
    yield
    _app.state.scheduler_task.cancel()
    await _app.state.scheduler_task


application: FastAPI = FastAPI(lifespan=_lifespan)


@application.get("/health")
def health_check() -> Response:
    return Response(status_code=status.HTTP_200_OK)


@application.get("/metrics")
def metrics_endpoint() -> Response:
    return get_metrics()


@application.post("/portfolio")
async def create_portfolio() -> Response:
    return await run_rebalance(application.state.alpaca_client)
