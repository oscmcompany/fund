"""Seed MinIO with synthetic equity data for local training."""

import io
import os
import random
import sys
from datetime import UTC, date, datetime, timedelta

import boto3
import polars as pl
import structlog

logger = structlog.get_logger()

TICKERS = [
    "AAPL", "MSFT", "GOOG", "AMZN", "META",
    "NVDA", "TSLA", "JPM", "V", "JNJ",
    "WMT", "PG", "MA", "UNH", "HD",
    "DIS", "BAC", "XOM", "PFE", "KO",
]

SECTORS = {
    "AAPL": ("Technology", "Consumer Electronics"),
    "MSFT": ("Technology", "Software"),
    "GOOG": ("Technology", "Internet Services"),
    "AMZN": ("Consumer Cyclical", "Internet Retail"),
    "META": ("Technology", "Internet Services"),
    "NVDA": ("Technology", "Semiconductors"),
    "TSLA": ("Consumer Cyclical", "Auto Manufacturers"),
    "JPM": ("Financial Services", "Banks"),
    "V": ("Financial Services", "Credit Services"),
    "JNJ": ("Healthcare", "Drug Manufacturers"),
    "WMT": ("Consumer Defensive", "Discount Stores"),
    "PG": ("Consumer Defensive", "Household Products"),
    "MA": ("Financial Services", "Credit Services"),
    "UNH": ("Healthcare", "Health Care Plans"),
    "HD": ("Consumer Cyclical", "Home Improvement"),
    "DIS": ("Communication Services", "Entertainment"),
    "BAC": ("Financial Services", "Banks"),
    "XOM": ("Energy", "Oil And Gas"),
    "PFE": ("Healthcare", "Drug Manufacturers"),
    "KO": ("Consumer Defensive", "Beverages"),
}

SATURDAY = 5


def generate_equity_bars(
    tickers: list[str],
    start_date: date,
    days: int,
) -> dict[date, pl.DataFrame]:
    """Generate synthetic equity bars grouped by date."""
    random.seed(42)
    bars_by_date: dict[date, list[dict]] = {}

    for ticker in tickers:
        base_price = random.uniform(50.0, 500.0)
        base_volume = random.uniform(500_000, 10_000_000)

        for day_offset in range(days):
            current_date = start_date + timedelta(days=day_offset)
            if current_date.weekday() >= SATURDAY:
                continue

            daily_return = random.gauss(0.0005, 0.02)
            base_price *= 1 + daily_return
            close = round(base_price, 2)
            high = round(close * (1 + abs(random.gauss(0, 0.01))), 2)
            low = round(close * (1 - abs(random.gauss(0, 0.01))), 2)
            open_price = round(close * (1 + random.gauss(0, 0.005)), 2)
            volume = max(100_000, int(base_volume * (1 + random.gauss(0, 0.3))))
            vwap = round((high + low + close) / 3, 2)

            timestamp = int(
                datetime(
                    current_date.year,
                    current_date.month,
                    current_date.day,
                    tzinfo=UTC,
                ).timestamp()
                * 1000
            )

            row = {
                "ticker": ticker,
                "timestamp": timestamp,
                "open_price": open_price,
                "high_price": high,
                "low_price": low,
                "close_price": close,
                "volume": float(volume),
                "volume_weighted_average_price": vwap,
                "transactions": volume // 100,
            }

            bars_by_date.setdefault(current_date, []).append(row)

    return {
        d: pl.DataFrame(rows) for d, rows in bars_by_date.items()
    }


def generate_details_csv(tickers: list[str]) -> bytes:
    """Generate equity details CSV."""
    rows = []
    for ticker in tickers:
        sector, industry = SECTORS.get(ticker, ("Unknown", "Unknown"))
        rows.append({"ticker": ticker, "sector": sector, "industry": industry})
    df = pl.DataFrame(rows)
    buffer = io.BytesIO()
    df.write_csv(buffer)
    return buffer.getvalue()


def seed_minio(
    data_bucket: str,
    lookback_days: int = 365,
) -> None:
    """Upload synthetic equity data to MinIO."""
    s3_client = boto3.client("s3")

    end_date = datetime.now(tz=UTC).date()
    start_date = end_date - timedelta(days=lookback_days)

    logger.info(
        "Generating synthetic equity bars",
        tickers=len(TICKERS),
        start_date=str(start_date),
        end_date=str(end_date),
        days=lookback_days,
    )

    bars_by_date = generate_equity_bars(TICKERS, start_date, lookback_days)

    uploaded = 0
    for bar_date, df in sorted(bars_by_date.items()):
        key = (
            f"equity/bars/daily/"
            f"year={bar_date:%Y}/month={bar_date:%m}/day={bar_date:%d}/data.parquet"
        )
        buffer = io.BytesIO()
        df.write_parquet(buffer)
        s3_client.put_object(
            Bucket=data_bucket,
            Key=key,
            Body=buffer.getvalue(),
            ContentType="application/octet-stream",
        )
        uploaded += 1

    logger.info("Uploaded equity bars", files=uploaded)

    details_csv = generate_details_csv(TICKERS)
    s3_client.put_object(
        Bucket=data_bucket,
        Key="equity/details/details.csv",
        Body=details_csv,
        ContentType="text/csv",
    )
    logger.info("Uploaded equity details")

    logger.info("MinIO seeding complete")


if __name__ == "__main__":
    structlog.configure(
        processors=[
            structlog.stdlib.add_log_level,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.BoundLogger,
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=True,
    )

    data_bucket = os.getenv("AWS_S3_DATA_BUCKET_NAME", "fund-data")
    lookback_days = int(os.getenv("LOOKBACK_DAYS", "365"))

    try:
        seed_minio(data_bucket=data_bucket, lookback_days=lookback_days)
    except Exception:
        logger.exception("Failed to seed MinIO")
        sys.exit(1)
