import io
import os
import sys
import tarfile
import tempfile
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, cast

# Add the tide package source directory to the path so the managed runner
# can resolve tide.tide_data and tide.tide_model after cloning the repo.
_tide_src = os.path.join(os.path.dirname(__file__), "..")  # noqa: PTH118, PTH120
if _tide_src not in sys.path:
    sys.path.insert(0, _tide_src)

import polars as pl
import structlog
from botocore.exceptions import ClientError
from prefect import flow, task
from prefect.client.schemas.objects import Flow, FlowRun, State
from prefect_aws.s3 import S3Bucket

logger = structlog.get_logger()

MINIMUM_CLOSE_PRICE = 1.0
MINIMUM_VOLUME = 100_000

DEFAULT_CONFIGURATION = {
    "architecture": "TiDE",
    "learning_rate": 0.003,
    "epoch_count": 20,
    "validation_split": 0.8,
    "input_length": 35,
    "output_length": 7,
    "hidden_size": 64,
    "num_encoder_layers": 2,
    "num_decoder_layers": 2,
    "dropout_rate": 0.1,
    "batch_size": 256,
}


def send_training_notification(flow: Flow, flow_run: FlowRun, state: State) -> None:
    """Send email notification via SES on training pipeline completion or failure."""
    sender_email = os.getenv("FUND_TRAINING_NOTIFICATION_SENDER_EMAIL", "").strip()
    recipient_emails_raw = os.getenv(
        "FUND_TRAINING_NOTIFICATION_RECIPIENT_EMAILS", ""
    ).strip()

    if not sender_email or not recipient_emails_raw:
        logger.warning(
            "Notification emails not configured, skipping",
            sender_email=sender_email,
            recipient_emails=recipient_emails_raw,
        )
        return

    recipient_emails = [
        email.strip() for email in recipient_emails_raw.split(",") if email.strip()
    ]
    if not recipient_emails:
        logger.warning(
            "Notification recipients are empty after parsing, skipping",
            recipient_emails_raw=recipient_emails_raw,
        )
        return

    state_name = state.name or "Unknown"
    is_failure = state.is_failed()

    duration_seconds = None
    if flow_run.start_time and flow_run.end_time:
        duration_seconds = (flow_run.end_time - flow_run.start_time).total_seconds()

    duration_text = (
        f"{duration_seconds:.0f} seconds" if duration_seconds is not None else "unknown"
    )

    subject = (
        f"Training pipeline {'FAILED' if is_failure else 'completed'}: "
        f"{flow.name}/{flow_run.name}"
    )

    body_parts = [
        f"Flow: {flow.name}",
        f"Run: {flow_run.name}",
        f"State: {state_name}",
        f"Duration: {duration_text}",
        f"Timestamp: {datetime.now(tz=UTC).isoformat()}",
    ]

    if is_failure and state.message:
        body_parts.append(f"\nError: {state.message}")

    body = "\n".join(body_parts)

    try:
        artifact_block = S3Bucket.load("artifact-bucket")
        ses_client = artifact_block.credentials.get_boto3_session().client("ses")
        ses_client.send_email(
            Source=sender_email,
            Destination={"ToAddresses": recipient_emails},
            Message={
                "Subject": {"Data": subject, "Charset": "UTF-8"},
                "Body": {"Text": {"Data": body, "Charset": "UTF-8"}},
            },
        )
        logger.info(
            "Training notification sent",
            recipients=recipient_emails,
            state=state_name,
        )
    except Exception:
        logger.exception(
            "Failed to send training notification",
            sender_email=sender_email,
            recipients=recipient_emails,
            state=state_name,
        )


def read_equity_bars_from_s3(
    s3_client: Any,
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
    s3_client: Any,
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

    result = consolidated.select(available_columns)

    logger.info(
        "Consolidated data", output_rows=result.height, columns=available_columns
    )

    return result


def write_training_data_to_s3(
    s3_client: Any,
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


def prepare_training_data(
    s3_client: Any,
    data_bucket_name: str,
    model_artifacts_bucket_name: str,
    start_date: datetime,
    end_date: datetime,
    output_key: str = "training/filtered_tide_training_data.parquet",
) -> str:
    """Read equity bars + categories from S3, filter, write consolidated parquet."""
    logger.info(
        "Preparing training data",
        data_bucket=data_bucket_name,
        model_artifacts_bucket=model_artifacts_bucket_name,
        start_date=start_date.strftime("%Y-%m-%d"),
        end_date=end_date.strftime("%Y-%m-%d"),
    )

    equity_bars = read_equity_bars_from_s3(
        s3_client=s3_client,
        bucket_name=data_bucket_name,
        start_date=start_date,
        end_date=end_date,
    )

    categories = read_categories_from_s3(
        s3_client=s3_client,
        bucket_name=data_bucket_name,
    )

    filtered_bars = filter_equity_bars(equity_bars)

    consolidated = consolidate_data(
        equity_bars=filtered_bars,
        categories=categories,
    )

    return write_training_data_to_s3(
        s3_client=s3_client,
        bucket_name=model_artifacts_bucket_name,
        data=consolidated,
        output_key=output_key,
    )


def train_model(
    training_data: pl.DataFrame,
    configuration: dict[str, Any] | None = None,
    checkpoint_directory: str | None = None,
) -> tuple[Any, Any]:
    """Train TiDE model and return model + data processor."""
    from tide.tide_data import Data  # noqa: PLC0415
    from tide.tide_model import Model  # noqa: PLC0415

    merged_configuration = dict(DEFAULT_CONFIGURATION)
    if configuration is not None:
        merged_configuration.update(configuration)
    configuration = merged_configuration

    logger.info("Configuration loaded", **configuration)

    logger.info("Initializing data processor")
    tide_data = Data()

    logger.info("Preprocessing training data")
    tide_data.preprocess_and_set_data(data=training_data)

    logger.info("Getting data dimensions")
    dimensions = tide_data.get_dimensions()
    logger.info("Data dimensions", **dimensions)

    logger.info("Creating training batches")
    train_batches = tide_data.get_batches(
        data_type="train",
        validation_split=float(configuration["validation_split"]),
        input_length=int(configuration["input_length"]),
        output_length=int(configuration["output_length"]),
        batch_size=int(configuration["batch_size"]),
    )

    logger.info("Training batches created", batch_count=len(train_batches))

    if not train_batches:
        logger.error(
            "No training batches created",
            validation_split=configuration["validation_split"],
            input_length=configuration["input_length"],
            output_length=configuration["output_length"],
            batch_size=configuration["batch_size"],
            training_data_rows=training_data.height,
        )
        message = (
            "No training batches created - check input data and configuration. "
            f"Training data has {training_data.height} rows, "
            f"input_length={configuration['input_length']}, "
            f"output_length={configuration['output_length']}, "
            f"batch_size={configuration['batch_size']}"
        )
        raise ValueError(message)

    sample_batch = train_batches[0]

    batch_size = sample_batch["past_continuous_features"].shape[0]
    logger.info("Batch size determined", batch_size=batch_size)

    past_continuous_size = (
        sample_batch["past_continuous_features"].reshape(batch_size, -1).shape[1]
    )
    past_categorical_size = (
        sample_batch["past_categorical_features"].reshape(batch_size, -1).shape[1]
    )
    future_categorical_size = (
        sample_batch["future_categorical_features"].reshape(batch_size, -1).shape[1]
    )
    static_categorical_size = (
        sample_batch["static_categorical_features"].reshape(batch_size, -1).shape[1]
    )

    input_size = cast(
        "int",
        past_continuous_size
        + past_categorical_size
        + future_categorical_size
        + static_categorical_size,
    )

    logger.info("Input size calculated", input_size=input_size)

    logger.info("Creating model")
    tide_model = Model(
        input_size=input_size,
        hidden_size=int(configuration["hidden_size"]),
        num_encoder_layers=int(configuration["num_encoder_layers"]),
        num_decoder_layers=int(configuration["num_decoder_layers"]),
        output_length=int(configuration["output_length"]),
        dropout_rate=float(configuration["dropout_rate"]),
        quantiles=[0.1, 0.5, 0.9],
    )

    logger.info("Training started", epochs=configuration["epoch_count"])

    losses = tide_model.train(
        train_batches=train_batches,
        epochs=int(configuration["epoch_count"]),
        learning_rate=float(configuration["learning_rate"]),
        checkpoint_directory=checkpoint_directory,
    )

    logger.info(
        "Training complete",
        final_loss=losses[-1] if losses else None,
        all_losses=losses,
    )

    return tide_model, tide_data


def get_training_date_range(lookback_days: int) -> tuple[datetime, datetime]:
    """Build a UTC date range used by prepare step."""
    end_date = datetime.now(tz=UTC).replace(hour=0, minute=0, second=0, microsecond=0)
    start_date = end_date - timedelta(days=lookback_days)
    return start_date, end_date


@task(name="prepare-training-data")
def prepare_data(
    start_date: datetime,
    end_date: datetime,
    output_key: str = "training/filtered_tide_training_data.parquet",
) -> str:
    """Read equity bars + categories from S3, filter, write consolidated parquet."""
    data_block = S3Bucket.load("data-bucket")
    artifact_block = S3Bucket.load("artifact-bucket")
    s3_client = data_block.credentials.get_boto3_session().client("s3")

    logger.info(
        "Preparing training data",
        data_bucket=data_block.bucket_name,
        artifacts_bucket=artifact_block.bucket_name,
        start_date=start_date.isoformat(),
        end_date=end_date.isoformat(),
    )

    training_data_uri = prepare_training_data(
        s3_client=s3_client,
        data_bucket_name=data_block.bucket_name,
        model_artifacts_bucket_name=artifact_block.bucket_name,
        start_date=start_date,
        end_date=end_date,
        output_key=output_key,
    )

    bucket_prefix = f"s3://{artifact_block.bucket_name}/"
    if training_data_uri.startswith(bucket_prefix):
        return training_data_uri.removeprefix(bucket_prefix)

    logger.warning(
        "Prepared training data URI did not match expected bucket",
        expected_bucket=artifact_block.bucket_name,
        training_data_uri=training_data_uri,
    )
    return output_key


@task(name="train-tide-model", timeout_seconds=3600)
def train_tide_model(
    training_data_key: str = "training/filtered_tide_training_data.parquet",
) -> str:
    """Download training data from S3, train model, upload artifact to S3."""
    artifact_block = S3Bucket.load("artifact-bucket")
    s3_client = artifact_block.credentials.get_boto3_session().client("s3")
    artifacts_bucket = artifact_block.bucket_name

    resolved_training_data_key = training_data_key
    bucket_prefix = f"s3://{artifacts_bucket}/"
    if training_data_key.startswith(bucket_prefix):
        resolved_training_data_key = training_data_key.removeprefix(bucket_prefix)

    logger.info(
        "Starting model training",
        artifacts_bucket=artifacts_bucket,
        training_data_key=resolved_training_data_key,
    )

    response = s3_client.get_object(
        Bucket=artifacts_bucket,
        Key=resolved_training_data_key,
    )
    training_data = pl.read_parquet(response["Body"].read())
    logger.info("Training data loaded", rows=training_data.height)

    with tempfile.TemporaryDirectory(prefix="checkpoints_") as checkpoint_directory:
        tide_model, tide_data = train_model(
            training_data,
            checkpoint_directory=checkpoint_directory,
        )

    timestamp = datetime.now(tz=UTC).strftime("%Y-%m-%d-%H-%M-%S-%f")[:-3]
    artifact_folder = f"artifacts/tide/{timestamp}"
    artifact_key = f"{artifact_folder}/output/model.tar.gz"

    with tempfile.TemporaryDirectory() as tmpdir:
        tide_model.save(directory_path=tmpdir)
        tide_data.save(directory_path=tmpdir)

        tar_buffer = io.BytesIO()
        with tarfile.open(fileobj=tar_buffer, mode="w:gz") as tar:
            for entry in Path(tmpdir).iterdir():
                tar.add(entry, arcname=entry.name)
        tar_bytes = tar_buffer.getvalue()

    logger.info(
        "Uploading model artifact",
        bucket=artifacts_bucket,
        key=artifact_key,
        size_bytes=len(tar_bytes),
    )

    s3_client.put_object(
        Bucket=artifacts_bucket,
        Key=artifact_key,
        Body=tar_bytes,
        ContentType="application/gzip",
    )

    logger.info(
        "Model artifact uploaded",
        artifact_path=f"s3://{artifacts_bucket}/{artifact_key}",
    )

    return f"s3://{artifacts_bucket}/{artifact_key}"


@flow(  # type: ignore[no-matching-overload]
    name="tide-training-pipeline",
    log_prints=True,
    on_completion=[send_training_notification],
    on_failure=[send_training_notification],
)
def training_pipeline(
    lookback_days: int = 365,
) -> str:
    """End-to-end training pipeline."""
    if lookback_days <= 0:
        message = "lookback_days must be positive"
        raise ValueError(message)

    training_data_key = "training/filtered_tide_training_data.parquet"
    start_date, end_date = get_training_date_range(lookback_days)

    prepared_key = prepare_data(
        start_date,
        end_date,
        training_data_key,
    )
    return train_tide_model(prepared_key)


if __name__ == "__main__":
    try:
        lookback_days = int(os.getenv("FUND_LOOKBACK_DAYS", "365"))
    except ValueError:
        logger.exception("FUND_LOOKBACK_DAYS must be a valid integer")
        sys.exit(1)

    if lookback_days <= 0:
        logger.error("FUND_LOOKBACK_DAYS must be positive", lookback_days=lookback_days)
        sys.exit(1)

    training_pipeline(
        lookback_days=lookback_days,
    )
