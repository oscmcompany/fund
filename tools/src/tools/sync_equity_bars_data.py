import json
import os
import re
import sys
import time
from datetime import UTC, date, datetime, timedelta
from typing import Any

import boto3
import requests
import structlog
from botocore.exceptions import BotoCoreError, ClientError

logger = structlog.get_logger()

DEFAULT_WAIT_SECONDS = 1.0
EQUITY_BARS_KEY_PATTERN = re.compile(
    r"^equity/bars/daily/year=(\d{4})/month=(\d{2})/day=(\d{2})/data\.parquet$"
)


def validate_and_parse_dates(date_range_json: str) -> tuple[datetime, datetime]:
    try:
        date_range = json.loads(date_range_json)
    except json.JSONDecodeError as e:
        raise RuntimeError from e

    if "start_date" not in date_range or "end_date" not in date_range:
        message = "Date range JSON must contain 'start_date' and 'end_date' fields"
        raise RuntimeError(message)

    try:
        start_date = datetime.strptime(date_range["start_date"], "%Y-%m-%d").replace(
            tzinfo=UTC
        )
        end_date = datetime.strptime(date_range["end_date"], "%Y-%m-%d").replace(
            tzinfo=UTC
        )
    except ValueError as e:
        raise RuntimeError from e

    current_date = datetime.now(tz=UTC).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    maximum_lookback_days = 365 * 2
    minimum_allowed_date = current_date - timedelta(days=maximum_lookback_days)

    start_date = max(start_date, minimum_allowed_date)
    end_date = min(end_date, current_date)

    if start_date > end_date:
        message = "Start date must be on or before end date"
        raise RuntimeError(message)

    return start_date, end_date


def build_equity_bars_key(target_date: date) -> str:
    return (
        "equity/bars/daily/"
        f"year={target_date:%Y}/month={target_date:%m}/day={target_date:%d}/data.parquet"
    )


def extract_date_from_key(key: str) -> date | None:
    match = EQUITY_BARS_KEY_PATTERN.match(key)
    if match is None:
        return None

    year, month, day = match.groups()
    return date(int(year), int(month), int(day))


def build_month_prefixes(start_date: date, end_date: date) -> list[str]:
    prefixes: list[str] = []
    current_date = start_date.replace(day=1)

    while current_date <= end_date:
        prefixes.append(
            f"equity/bars/daily/year={current_date:%Y}/month={current_date:%m}/"
        )
        if current_date.month == 12:
            current_date = current_date.replace(year=current_date.year + 1, month=1)
        else:
            current_date = current_date.replace(month=current_date.month + 1)

    return prefixes


def list_existing_equity_bar_dates(
    bucket_name: str,
    start_date: datetime,
    end_date: datetime,
) -> set[date]:
    s3_client = boto3.client("s3")
    existing_dates: set[date] = set()

    for prefix in build_month_prefixes(start_date.date(), end_date.date()):
        continuation_token: str | None = None

        while True:
            request_kwargs: dict[str, Any] = {"Bucket": bucket_name, "Prefix": prefix}
            if continuation_token is not None:
                request_kwargs["ContinuationToken"] = continuation_token

            response = s3_client.list_objects_v2(**request_kwargs)
            for item in response.get("Contents", []):
                key_date = extract_date_from_key(item["Key"])
                if key_date is not None:
                    existing_dates.add(key_date)

            if not response.get("IsTruncated"):
                break
            continuation_token = response["NextContinuationToken"]

    return {
        existing_date
        for existing_date in existing_dates
        if start_date.date() <= existing_date <= end_date.date()
    }


def build_dates_to_sync(
    start_date: datetime,
    end_date: datetime,
    bucket_name: str | None,
) -> list[datetime]:
    candidate_dates: list[datetime] = []
    current_date = start_date

    while current_date <= end_date:
        if current_date.weekday() < 5:
            candidate_dates.append(current_date)
        current_date += timedelta(days=1)

    if not bucket_name:
        return candidate_dates

    existing_dates = list_existing_equity_bar_dates(bucket_name, start_date, end_date)
    return [date_value for date_value in candidate_dates if date_value.date() not in existing_dates]


def sync_equity_bars_for_date(base_url: str, date_value: datetime) -> tuple[int, str]:
    url = f"{base_url}/equity-bars"
    date_string = date_value.strftime("%Y-%m-%dT00:00:00Z")

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
    wait_seconds: float = DEFAULT_WAIT_SECONDS,
) -> None:
    start_date, end_date = date_range
    bucket_name = os.getenv("AWS_S3_DATA_BUCKET_NAME")

    logger.info(
        "Reconciling equity bars",
        start_date=start_date.strftime("%Y-%m-%d"),
        end_date=end_date.strftime("%Y-%m-%d"),
        bucket_name=bucket_name,
    )
    logger.info("Data manager URL", base_url=f"{base_url}/equity-bars")

    try:
        dates_to_sync = build_dates_to_sync(start_date, end_date, bucket_name)
    except (BotoCoreError, ClientError) as error:
        logger.warning(
            "Falling back to weekday sync after S3 lookup failure",
            error=str(error),
            bucket_name=bucket_name,
        )
        dates_to_sync = build_dates_to_sync(start_date, end_date, None)

    if not dates_to_sync:
        logger.info("No missing equity bar dates detected")
        return

    logger.info("Syncing missing equity bar dates", total_requests=len(dates_to_sync))

    for request_count, current_date in enumerate(dates_to_sync, start=1):
        logger.info(
            "Syncing data started",
            request_count=request_count,
            date=current_date.strftime("%Y-%m-%d"),
        )

        try:
            status_code, response_text = sync_equity_bars_for_date(base_url, current_date)
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
        except requests.RequestException as error:
            logger.exception("HTTP request failed", error=f"{error}")

        if request_count < len(dates_to_sync):
            logger.info("Waiting before next request", wait_seconds=wait_seconds)
            time.sleep(wait_seconds)

    logger.info("All missing dates processed", total_requests=len(dates_to_sync))


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
    except Exception as error:
        logger.exception("Failed to parse date range", error=f"{error}")
        sys.exit(1)

    try:
        sync_equity_bars_data(base_url=base_url, date_range=date_range)
    except Exception as error:
        logger.exception("Failed to sync equity bars data", error=f"{error}")
        sys.exit(1)
