import io
from datetime import datetime, timedelta
from typing import TYPE_CHECKING

import boto3
import polars as pl
import structlog
from botocore.exceptions import ClientError
from internal.equity_bars_schema import equity_bars_schema

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client

logger = structlog.get_logger()

MINIMUM_CLOSE_PRICE = 1.0
MINIMUM_VOLUME = 100_000

_COLUMN_TYPES: dict[str, type[pl.DataType]] = {
    "open_price": pl.Float64,
    "high_price": pl.Float64,
    "low_price": pl.Float64,
    "close_price": pl.Float64,
    "volume_weighted_average_price": pl.Float64,
    "volume": pl.Int64,
    "transactions": pl.Int64,
}


def read_equity_bars_from_s3(
    s3_client: "S3Client",
    bucket_name: str,
    start_date: datetime,
    end_date: datetime,
    batch_size_days: int = 30,
) -> pl.DataFrame:
    """Read equity bars parquet files from S3 for date range in batches."""
    logger.info(
        "Reading equity bars from S3",
        bucket=bucket_name,
        start_date=start_date.strftime("%Y-%m-%d"),
        end_date=end_date.strftime("%Y-%m-%d"),
    )

    all_dataframes = []
    current_date = start_date
    batch_dataframes: list[pl.DataFrame] = []
    days_in_batch = 0

    while current_date <= end_date:
        year = current_date.strftime("%Y")
        month = current_date.strftime("%m")
        day = current_date.strftime("%d")

        key = f"equity/bars/daily/year={year}/month={month}/day={day}/data.parquet"

        try:
            response = s3_client.get_object(Bucket=bucket_name, Key=key)
            parquet_bytes = response["Body"].read()
            dataframe = pl.read_parquet(parquet_bytes)
            dataframe = dataframe.with_columns(
                [
                    pl.col(col).cast(dtype)
                    for col, dtype in _COLUMN_TYPES.items()
                    if col in dataframe.columns
                ]
            )
            batch_dataframes.append(dataframe)
            logger.debug("Read parquet file", key=key, rows=dataframe.height)
        except s3_client.exceptions.NoSuchKey:
            logger.debug("No data for date", date=current_date.strftime("%Y-%m-%d"))
        except ClientError as e:
            logger.warning("Failed to read parquet file", key=key, error=str(e))

        current_date += timedelta(days=1)
        days_in_batch += 1

        if days_in_batch >= batch_size_days and batch_dataframes:
            all_dataframes.append(pl.concat(batch_dataframes))
            logger.debug("Processed batch", days=days_in_batch)
            batch_dataframes = []
            days_in_batch = 0

    if batch_dataframes:
        all_dataframes.append(pl.concat(batch_dataframes))

    if not all_dataframes:
        message = "No equity bars data found for date range"
        raise ValueError(message)

    combined = pl.concat(all_dataframes)
    logger.info("Combined equity bars", total_rows=combined.height)

    return combined


def read_categories_from_s3(
    s3_client: "S3Client",
    bucket_name: str,
) -> pl.DataFrame:
    """Read categories CSV from S3."""
    key = "equity/details/details.csv"

    logger.info("Reading categories from S3", bucket=bucket_name, key=key)

    response = s3_client.get_object(Bucket=bucket_name, Key=key)
    csv_bytes = response["Body"].read()
    categories = pl.read_csv(csv_bytes)

    logger.info("Read categories", rows=categories.height)

    return categories


def filter_equity_bars(
    data: pl.DataFrame,
    minimum_close_price: float = MINIMUM_CLOSE_PRICE,
    minimum_volume: int = MINIMUM_VOLUME,
) -> pl.DataFrame:
    """Filter equity bars by minimum price and volume thresholds."""
    logger.info(
        "Filtering equity bars",
        minimum_close_price=minimum_close_price,
        minimum_volume=minimum_volume,
        input_rows=data.height,
    )

    filtered = data.filter(
        (pl.col("close_price") >= minimum_close_price)
        & (pl.col("volume") >= minimum_volume)
        & ~pl.col("ticker").str.contains("[a-z]")
    )

    logger.info("Filtered equity bars", output_rows=filtered.height)

    return filtered


def consolidate_data(
    equity_bars: pl.DataFrame,
    categories: pl.DataFrame,
) -> pl.DataFrame:
    """Join equity bars with categories on ticker."""
    logger.info(
        "Consolidating data",
        equity_bars_rows=equity_bars.height,
        categories_rows=categories.height,
    )

    consolidated = equity_bars.join(categories, on="ticker", how="inner")

    retained_columns = [
        "ticker",
        "timestamp",
        "open_price",
        "high_price",
        "low_price",
        "close_price",
        "volume",
        "volume_weighted_average_price",
        "sector",
        "industry",
    ]

    available_columns = [col for col in retained_columns if col in consolidated.columns]
    missing_columns = [
        col for col in retained_columns if col not in consolidated.columns
    ]

    if missing_columns:
        logger.warning("Missing columns in consolidated data", missing=missing_columns)

    if "sector" in available_columns and "industry" in available_columns:
        result = consolidated.select(available_columns).filter(
            pl.col("sector").is_not_null() & pl.col("industry").is_not_null()
        )
    else:
        result = consolidated.select(available_columns)

    logger.info(
        "Consolidated data", output_rows=result.height, columns=available_columns
    )

    return result


def write_training_data_to_s3(
    s3_client: "S3Client",
    bucket_name: str,
    data: pl.DataFrame,
    output_key: str,
) -> str:
    """Write consolidated training data to S3 as parquet."""
    logger.info(
        "Writing training data to S3",
        bucket=bucket_name,
        key=output_key,
        rows=data.height,
    )

    buffer = io.BytesIO()
    data.write_parquet(buffer)
    parquet_bytes = buffer.getvalue()

    s3_client.put_object(
        Bucket=bucket_name,
        Key=output_key,
        Body=parquet_bytes,
        ContentType="application/octet-stream",
    )

    s3_uri = f"s3://{bucket_name}/{output_key}"
    logger.info("Wrote training data", s3_uri=s3_uri, size_bytes=len(parquet_bytes))

    return s3_uri


def prepare_training_data(  # noqa: PLR0913
    data_bucket_name: str,
    model_artifacts_bucket_name: str,
    start_date: datetime,
    end_date: datetime,
    output_key: str = "training/filtered_tide_training_data.parquet",
    s3_client: "S3Client | None" = None,
) -> tuple[str, dict]:
    """Main function to prepare training data."""
    logger.info(
        "Preparing training data",
        data_bucket=data_bucket_name,
        model_artifacts_bucket=model_artifacts_bucket_name,
        start_date=start_date.strftime("%Y-%m-%d"),
        end_date=end_date.strftime("%Y-%m-%d"),
    )

    if s3_client is None:
        s3_client = boto3.client("s3")

    equity_bars = read_equity_bars_from_s3(
        s3_client=s3_client,
        bucket_name=data_bucket_name,
        start_date=start_date,
        end_date=end_date,
    )

    raw_rows = equity_bars.height
    raw_tickers = equity_bars["ticker"].n_unique()

    categories = read_categories_from_s3(
        s3_client=s3_client,
        bucket_name=data_bucket_name,
    )

    filtered_bars = filter_equity_bars(equity_bars)

    equity_bars_schema.validate(filtered_bars)

    filtered_rows = filtered_bars.height
    filtered_tickers = filtered_bars["ticker"].n_unique()

    consolidated = consolidate_data(
        equity_bars=filtered_bars,
        categories=categories,
    )

    consolidated_rows = consolidated.height
    consolidated_tickers = consolidated["ticker"].n_unique()

    stage_counts = {
        "raw_rows": raw_rows,
        "raw_tickers": raw_tickers,
        "filtered_rows": filtered_rows,
        "filtered_tickers": filtered_tickers,
        "consolidated_rows": consolidated_rows,
        "consolidated_tickers": consolidated_tickers,
    }

    uri = write_training_data_to_s3(
        s3_client=s3_client,
        bucket_name=model_artifacts_bucket_name,
        data=consolidated,
        output_key=output_key,
    )

    return uri, stage_counts
