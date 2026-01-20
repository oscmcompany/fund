import json
import sys
import time
from datetime import UTC, datetime, timedelta

import requests
import structlog

logger = structlog.get_logger()


def validate_and_parse_dates(date_range_json: str) -> tuple[datetime, datetime]:
    try:
        date_range = json.loads(date_range_json)
    except json.JSONDecodeError as e:
        logger.exception("JSON decoding error", error=f"{e}")
        raise RuntimeError from e

    if "start_date" not in date_range or "end_date" not in date_range:
        logger.error("Missing required date fields", date_range=date_range)
        raise RuntimeError

    try:
        start_date = datetime.strptime(date_range["start_date"], "%Y-%m-%d").replace(
            tzinfo=UTC
        )
        end_date = datetime.strptime(date_range["end_date"], "%Y-%m-%d").replace(
            tzinfo=UTC
        )
    except ValueError as e:
        logger.exception(
            "Date parsing error",
            error=f"{e}",
            required_format="YYYY-MM-DD",
        )
        raise RuntimeError from e

    current_date = datetime.now(tz=UTC).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    maximum_lookback_days = 365 * 2  # two year limit

    minimum_allowed_date = current_date - timedelta(days=maximum_lookback_days)

    start_date = max(start_date, minimum_allowed_date)
    end_date = min(end_date, current_date)

    if start_date > end_date:
        logger.error(
            "Invalid date range after clamping",
            start_date=start_date.strftime("%Y-%m-%d"),
            end_date=end_date.strftime("%Y-%m-%d"),
        )
        raise RuntimeError

    return start_date, end_date


def sync_equity_bars_for_date(base_url: str, date: datetime) -> tuple[int, str]:
    url = f"{base_url}/equity-bars"
    date_string = date.strftime("%Y-%m-%dT00:00:00Z")

    response = requests.post(
        url,
        json={"date": date_string},
        headers={"Content-Type": "application/json"},
        timeout=60,
    )

    return response.status_code, response.text


def sync_equity_bars_data(
    base_url: str,
    date_range: tuple[datetime, datetime],
) -> None:
    start_date, end_date = date_range

    logger.info(
        "Backfilling equity bars",
        start_date=start_date.strftime("%Y-%m-%d"),
        end_date=end_date.strftime("%Y-%m-%d"),
    )
    logger.info("Data manager URL", base_url=f"{base_url}/equity-bars")

    current_date = start_date
    request_count = 0

    while current_date <= end_date:
        request_count += 1

        logger.info(
            "Syncing data started",
            request_count=request_count,
            date=current_date.strftime("%Y-%m-%d"),
        )

        try:
            status_code, response_text = sync_equity_bars_for_date(
                base_url,
                current_date,
            )
            logger.info(
                "Syncing data completed",
                status_code=status_code,
                response=response_text,
            )

            if status_code >= 400:  # noqa: PLR2004
                logger.error(
                    "Syncing data failed",
                    status_code=status_code,
                    response=response_text,
                )
        except requests.RequestException as e:
            logger.exception("HTTP request failed", error=f"{e}")

        current_date += timedelta(days=1)

        if current_date <= end_date:
            logger.info("Waiting 15 seconds before next request")
            time.sleep(15)

    logger.info("All dates processed", total_requests=request_count)


if __name__ == "__main__":
    if len(sys.argv) != 3:  # noqa: PLR2004
        logger.error(
            "Usage: python sync_equity_bars_data.py <base_url> <date_range_json>",
            args_received=len(sys.argv) - 1,
        )
        sys.exit(1)

    base_url = sys.argv[1]
    raw_date_range = sys.argv[2]

    arguments = {
        "base_url": base_url,
        "raw_date_range": raw_date_range,
    }

    for argument in [base_url, raw_date_range]:
        if not argument:
            logger.error(
                "Missing required positional argument(s)",
                **arguments,
            )
            sys.exit(1)

    try:
        date_range = validate_and_parse_dates(raw_date_range)
    except Exception as e:
        logger.exception("Failed to parse date range", error=f"{e}")
        sys.exit(1)

    try:
        sync_equity_bars_data(
            base_url=base_url,
            date_range=date_range,
        )
    except Exception as e:
        logger.exception("Failed to sync equity bars data", error=f"{e}")
        sys.exit(1)
