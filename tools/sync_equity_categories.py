"""Sync equity categories (sector/industry) from Polygon API to S3.

This script fetches ticker reference data from Polygon's API and uploads
a categories CSV to S3 for use in training data preparation.

The CSV contains: ticker, sector, industry
"""

import os
import sys
import time

import boto3
import polars as pl
import requests
import structlog

logger = structlog.get_logger()

POLYGON_BASE_URL = "https://api.polygon.io"


def fetch_all_tickers(api_key: str) -> list[dict]:
    """Fetch all US stock tickers from Polygon API with pagination."""
    logger.info("Fetching tickers from Polygon API")

    all_tickers = []
    url = f"{POLYGON_BASE_URL}/v3/reference/tickers"
    params = {
        "market": "stocks",
        "active": "true",
        "limit": 1000,
        "apiKey": api_key,
    }

    while url:
        logger.debug("Fetching page", url=url)

        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()

        data = response.json()
        results = data.get("results", [])
        all_tickers.extend(results)

        logger.info("Fetched tickers", count=len(results), total=len(all_tickers))

        next_url = data.get("next_url")
        if next_url:
            url = next_url
            params = {"apiKey": api_key}
            time.sleep(0.25)
        else:
            url = None

    logger.info("Finished fetching tickers", total=len(all_tickers))
    return all_tickers


def extract_categories(tickers: list[dict]) -> pl.DataFrame:
    """Extract ticker, sector, industry from ticker data."""
    logger.info("Extracting categories from ticker data")

    rows = []
    for ticker_data in tickers:
        ticker = ticker_data.get("ticker", "")
        # Polygon uses 'sic_description' for industry, but we can also check other fields
        # The primary_exchange and type fields help filter
        if ticker_data.get("type") not in ("CS", "ADRC"):  # Common Stock or ADR
            continue

        # Try to get sector/industry from various fields Polygon provides
        sector = ticker_data.get("sector", "")
        industry = ticker_data.get("industry", "")

        # Some tickers may not have sector/industry
        if not sector:
            sector = "NOT AVAILABLE"
        if not industry:
            industry = "NOT AVAILABLE"

        rows.append({
            "ticker": ticker.upper(),
            "sector": sector.upper(),
            "industry": industry.upper(),
        })

    dataframe = pl.DataFrame(rows)
    logger.info("Extracted categories", rows=dataframe.height)

    return dataframe


def upload_categories_to_s3(
    s3_client: boto3.client,
    bucket_name: str,
    categories: pl.DataFrame,
) -> str:
    """Upload categories CSV to S3."""
    key = "equity/details/categories.csv"

    logger.info(
        "Uploading categories to S3",
        bucket=bucket_name,
        key=key,
        rows=categories.height,
    )

    csv_bytes = categories.write_csv().encode("utf-8")

    s3_client.put_object(
        Bucket=bucket_name,
        Key=key,
        Body=csv_bytes,
        ContentType="text/csv",
    )

    s3_uri = f"s3://{bucket_name}/{key}"
    logger.info("Uploaded categories", s3_uri=s3_uri)

    return s3_uri


def sync_equity_categories(
    api_key: str,
    bucket_name: str,
) -> str:
    """Main function to sync equity categories."""
    logger.info("Syncing equity categories", bucket=bucket_name)

    tickers = fetch_all_tickers(api_key)
    categories = extract_categories(tickers)

    s3_client = boto3.client("s3")
    s3_uri = upload_categories_to_s3(s3_client, bucket_name, categories)

    return s3_uri


if __name__ == "__main__":
    api_key = os.getenv("POLYGON_API_KEY")
    bucket_name = os.getenv("AWS_S3_DATA_BUCKET")

    if not api_key:
        logger.error("POLYGON_API_KEY environment variable not set")
        sys.exit(1)

    if not bucket_name:
        logger.error("AWS_S3_DATA_BUCKET environment variable not set")
        sys.exit(1)

    try:
        output_uri = sync_equity_categories(
            api_key=api_key,
            bucket_name=bucket_name,
        )
        logger.info("Sync complete", output_uri=output_uri)

    except Exception as e:
        logger.exception("Failed to sync equity categories", error=str(e))
        sys.exit(1)
