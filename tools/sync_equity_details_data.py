"""Sync equity details by invoking the datamanager POST /equity-details endpoint."""

from __future__ import annotations

import sys

import requests
import structlog

logger = structlog.get_logger()


def sync_equity_details(base_url: str) -> None:
    """POST to the equity-details endpoint to trigger a sync."""
    url = f"{base_url}/equity-details"
    logger.info("Syncing equity details", url=url)

    try:
        response = requests.post(url, timeout=300)
        logger.info(
            "Sync completed",
            status_code=response.status_code,
            response=response.text,
        )

        if response.status_code >= 400:  # noqa: PLR2004
            logger.error(
                "Sync failed",
                status_code=response.status_code,
                response=response.text,
            )
            sys.exit(1)

    except requests.RequestException as error:
        logger.exception("HTTP request failed", error=f"{error}")
        sys.exit(1)


if __name__ == "__main__":
    if len(sys.argv) != 2:  # noqa: PLR2004
        logger.error(
            "Usage: python sync_equity_details_data.py <base_url>",
            args_received=len(sys.argv) - 1,
        )
        sys.exit(1)

    base_url = sys.argv[1]

    if not base_url:
        logger.error("Missing required positional argument: base_url")
        sys.exit(1)

    sync_equity_details(base_url=base_url)
