import sys

import requests
import structlog

logger = structlog.get_logger()


def sync_equity_details(base_url: str) -> tuple[int, str]:
    url = f"{base_url}/equity-details"
    response = requests.post(url, timeout=300)
    return response.status_code, response.text


def sync_equity_details_data(base_url: str) -> None:
    logger.info("Syncing equity details", url=f"{base_url}/equity-details")

    try:
        status_code, response_text = sync_equity_details(base_url)
    except requests.RequestException as error:
        logger.exception("HTTP request failed", error=f"{error}")
        raise

    logger.info(
        "Sync completed",
        status_code=status_code,
        response=response_text,
    )

    if status_code >= 400:  # noqa: PLR2004
        logger.error(
            "Sync failed",
            status_code=status_code,
            response=response_text,
        )
        message = "Sync failed"
        raise RuntimeError(message)


if __name__ == "__main__":
    if len(sys.argv) != 2:  # noqa: PLR2004
        logger.error(
            "Usage: python sync_equity_details_data.py <base_url>",
            args_received=len(sys.argv) - 1,
        )
        sys.exit(1)

    base_url = sys.argv[1]

    arguments = {
        "base_url": base_url,
    }

    for argument in [base_url]:
        if not argument:
            logger.error(
                "Missing required positional argument(s)",
                **arguments,
            )
            sys.exit(1)

    try:
        sync_equity_details_data(base_url=base_url)
    except Exception as e:
        logger.exception("Failed to sync equity details data", error=f"{e}")
        sys.exit(1)
