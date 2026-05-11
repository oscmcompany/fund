import asyncio
import concurrent.futures
import json
import logging
import os
import sys
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from pathlib import Path

import requests
import structlog
from alpaca.common.exceptions import APIError
from fastapi import FastAPI, Response, status

from .alpaca_client import AlpacaClient
from .metrics import (
    get_metrics,
)
from .rebalance import DATA_MANAGER_BASE_URL, run_rebalance
from .scheduler import spawn_rebalance_scheduler

logging.basicConfig(
    level=logging.INFO, stream=sys.stdout, format="%(message)s", force=True
)

try:
    _error_log_path = Path("/var/log/fund/portfolio-manager-errors.log")
    _error_log_path.parent.mkdir(parents=True, exist_ok=True)
    _error_file_handler = logging.FileHandler(_error_log_path)
    _error_file_handler.setLevel(logging.ERROR)
    _error_file_handler.setFormatter(logging.Formatter("%(message)s"))
    logging.getLogger().addHandler(_error_file_handler)
except OSError:
    pass

structlog.configure(
    processors=[
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.JSONRenderer(),
    ],
    wrapper_class=structlog.stdlib.BoundLogger,
    context_class=dict,
    logger_factory=structlog.stdlib.LoggerFactory(),
    cache_logger_on_first_use=True,
)

structlog.contextvars.bind_contextvars(
    fund_profile=os.environ.get("FUND_PROFILE", "unknown")
)

logger = structlog.get_logger()

_rebalance_lock: asyncio.Lock = asyncio.Lock()

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
        rebalance_lock=_rebalance_lock,
    )
    logger.info("Portfolio rebalance scheduler started")
    yield
    _app.state.scheduler_task.cancel()
    await _app.state.scheduler_task


application: FastAPI = FastAPI(lifespan=_lifespan)


@application.get("/health")
def health_check() -> Response:
    checks: dict[str, str] = {}
    healthy = True

    alpaca_client = getattr(application.state, "alpaca_client", None)
    if alpaca_client is not None:
        try:
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
                future = executor.submit(alpaca_client.get_account)
                future.result(timeout=5)
            checks["alpaca_client"] = "ok"
        except (
            APIError,
            requests.RequestException,
            OSError,
            concurrent.futures.TimeoutError,
        ):
            checks["alpaca_client"] = "error"
            healthy = False
    else:
        checks["alpaca_client"] = "error"
        healthy = False

    scheduler = getattr(application.state, "scheduler_task", None)
    if scheduler and not scheduler.done():
        checks["scheduler"] = "ok"
    else:
        checks["scheduler"] = "error"
        healthy = False

    status_code = status.HTTP_200_OK if healthy else status.HTTP_503_SERVICE_UNAVAILABLE
    body = {"status": "ok" if healthy else "degraded", "checks": checks}
    return Response(
        content=json.dumps(body),
        status_code=status_code,
        media_type="application/json",
    )


@application.get("/metrics")
def metrics_endpoint() -> Response:
    return get_metrics()


@application.post("/portfolio")
async def create_portfolio() -> Response:
    if _rebalance_lock.locked():
        return Response(status_code=status.HTTP_409_CONFLICT)
    async with _rebalance_lock:
        return await run_rebalance(application.state.alpaca_client)
