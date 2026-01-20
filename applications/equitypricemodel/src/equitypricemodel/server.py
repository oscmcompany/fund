import io
import json
import os
import tarfile
import tempfile
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path

import boto3
import polars as pl
import requests
import sentry_sdk
import structlog
from botocore.exceptions import ClientError
from fastapi import FastAPI, Request, Response, status
from internal.equity_bars_schema import equity_bars_schema
from sentry_sdk.integrations.logging import LoggingIntegration

from .equity_details_schema import equity_details_schema
from .predictions_schema import predictions_schema
from .preprocess import filter_equity_bars
from .tide_data import Data
from .tide_model import Model

sentry_sdk.init(
    dsn=os.environ.get("SENTRY_DSN"),
    environment=os.environ.get("ENVIRONMENT", "development"),
    traces_sample_rate=1.0,
    profiles_sample_rate=1.0,
    enable_tracing=True,
    propagate_traces=True,
    integrations=[
        LoggingIntegration(
            level=None,
            event_level="WARNING",
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

DATAMANAGER_BASE_URL = os.getenv("PSF_DATAMANAGER_BASE_URL", "http://datamanager:8080")


def find_latest_artifact_key(
    s3_client: boto3.client,
    bucket: str,
    prefix: str,
) -> str:
    """Find the latest model artifact under a prefix.

    Assumes folder names contain timestamps that sort alphabetically.
    E.g., artifacts/equitypricemodel-trainer-2026-01-14-15-00-26-204/
    Only considers folders that contain output/model.tar.gz.
    """
    logger.info("listing_artifact_folders", bucket=bucket, prefix=prefix)

    paginator = s3_client.get_paginator("list_objects_v2")
    folders: list[str] = []

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix, Delimiter="/"):
        folders.extend(
            common_prefix["Prefix"] for common_prefix in page.get("CommonPrefixes", [])
        )

    if not folders:
        message = f"No artifact folders found under s3://{bucket}/{prefix}"
        raise ValueError(message)

    folders.sort(reverse=True)

    for folder in folders:
        artifact_key = str(Path(folder) / "output" / "model.tar.gz")
        try:
            s3_client.head_object(Bucket=bucket, Key=artifact_key)
        except ClientError:
            logger.debug("artifact_not_found_in_folder", folder=folder)
            continue
        else:
            logger.info(
                "found_latest_artifact",
                folder_count=len(folders),
                latest_folder=folder,
                artifact_key=artifact_key,
            )
            return artifact_key

    message = f"No model.tar.gz found in any artifact folder under s3://{bucket}/{prefix}"
    raise ValueError(message)


def download_and_extract_artifacts(
    s3_client: boto3.client,
    bucket: str,
    artifact_key: str,
    extract_path: Path,
) -> None:
    """Download model artifacts from S3 and extract them."""
    logger.info(
        "downloading_model_artifacts",
        bucket=bucket,
        artifact_key=artifact_key,
    )

    with tempfile.NamedTemporaryFile(suffix=".tar.gz", delete=False) as temp_file:
        temp_path = Path(temp_file.name)

    try:
        s3_client.download_file(bucket, artifact_key, str(temp_path))
        logger.info("downloaded_artifact", size_bytes=temp_path.stat().st_size)

        with tarfile.open(temp_path, "r:gz") as tar:
            tar.extractall(path=extract_path, filter="data")

        logger.info("extracted_artifacts", extract_path=str(extract_path))

    finally:
        temp_path.unlink(missing_ok=True)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """Load model artifacts from S3 at startup."""
    import shutil  # noqa: PLC0415

    bucket = os.environ.get("AWS_S3_MODEL_ARTIFACTS_BUCKET")
    artifact_path = os.environ.get("AWS_S3_MODEL_ARTIFACT_PATH", "artifacts/")
    model_directory = "."

    if bucket:
        s3_client = boto3.client("s3")

        model_directory = tempfile.mkdtemp(prefix="model_artifacts_")
        extract_path = Path(model_directory)

        try:
            if artifact_path.endswith(".tar.gz"):
                artifact_key = artifact_path
            else:
                artifact_key = find_latest_artifact_key(
                    s3_client=s3_client,
                    bucket=bucket,
                    prefix=artifact_path,
                )

            download_and_extract_artifacts(
                s3_client=s3_client,
                bucket=bucket,
                artifact_key=artifact_key,
                extract_path=extract_path,
            )
        except Exception:
            logger.exception("failed_to_download_artifacts")
            raise

        logger.info("loading_model", directory=model_directory)
    else:
        logger.info("loading_model_from_local", directory=model_directory)

    app.state.model_directory = model_directory
    app.state.tide_model = Model.load(directory_path=model_directory)
    logger.info("model_loaded_successfully")

    yield

    if app.state.model_directory != "." and Path(app.state.model_directory).exists():
        shutil.rmtree(app.state.model_directory, ignore_errors=True)


application = FastAPI(lifespan=lifespan)


@application.get("/health")
def health_check() -> Response:
    return Response(status_code=status.HTTP_200_OK)


@application.post("/model/predictions")
@application.post("/predictions")
def create_predictions(request: Request) -> Response:
    logger.info("Starting prediction generation process")

    end_date = datetime.now(tz=UTC)
    start_date = end_date - timedelta(
        days=70
    )  # need >= 42 trading days (35 input + 7 output), ~60 calendar days + buffer

    try:
        equity_bars_response = requests.get(
            url=f"{DATAMANAGER_BASE_URL}/equity-bars",
            params={
                "start_timestamp": start_date.isoformat(),
                "end_timestamp": end_date.isoformat(),
            },
            timeout=60,
        )

        equity_bars_response.raise_for_status()

    except Exception as e:
        logger.exception(
            "Failed to fetch equity bars data",
            start_date=start_date.isoformat(),
            end_date=end_date.isoformat(),
            error=f"{e}",
        )
        return Response(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR)

    try:
        equity_details_response = requests.get(
            url=f"{DATAMANAGER_BASE_URL}/equity-details",
            timeout=60,
        )

        equity_details_response.raise_for_status()

    except Exception as e:
        logger.exception(
            "Failed to fetch equity details data",
            error=f"{e}",
        )
        return Response(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR)

    try:
        data = parse_responses(
            equity_bars_response=equity_bars_response,
            equity_details_response=equity_details_response,
        )
    except Exception as e:
        logger.exception(
            "Failed to parse and consolidate data responses",
            error=f"{e}",
        )
        return Response(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR)

    current_timestamp = datetime.now(tz=UTC)

    tide_data = Data.load(directory_path=request.app.state.model_directory)

    tide_data.preprocess_and_set_data(data=data)

    batches = tide_data.get_batches(data_type="predict")

    if not batches:
        logger.error("No data batches available for prediction")
        return Response(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR)

    logger.info(
        "Processing prediction batches",
        num_batches=len(batches),
    )

    all_predictions = []
    for batch in batches:
        raw_predictions = request.app.state.tide_model.predict(inputs=batch)
        batch_predictions = tide_data.postprocess_predictions(
            input_batch=batch,
            predictions=raw_predictions,
            current_datetime=current_timestamp,
        )
        all_predictions.append(batch_predictions)

    predictions = pl.concat(all_predictions)
    logger.info(
        "Combined predictions from all batches",
        total_predictions=predictions.height,
    )

    processed_prediction_timestamp = current_timestamp + timedelta(days=6)
    processed_predictions = predictions.filter(
        pl.col("timestamp")
        == int(
            processed_prediction_timestamp.replace(
                hour=0, minute=0, second=0, microsecond=0
            ).timestamp()
        )
    )

    processed_predictions = predictions_schema.validate(processed_predictions)

    try:
        save_predictions_response = requests.post(
            url=f"{DATAMANAGER_BASE_URL}/predictions",
            json={
                "timestamp": current_timestamp.isoformat(),
                "data": processed_predictions.to_dicts(),
            },
            timeout=60,
        )

        save_predictions_response.raise_for_status()

    except Exception as e:
        logger.exception(
            "Failed to save predictions data",
            timestamp=current_timestamp.isoformat(),
            error=f"{e}",
        )
        raise

    logger.info("Successfully generated and saved predictions")

    return Response(
        content=json.dumps({"data": processed_predictions.to_dicts()}).encode("utf-8"),
        status_code=status.HTTP_200_OK,
    )


def parse_responses(
    equity_bars_response: requests.Response,
    equity_details_response: requests.Response,
) -> pl.DataFrame:
    equity_bars_data = pl.read_parquet(io.BytesIO(equity_bars_response.content))

    equity_bars_data = equity_bars_data.unique(
        subset=["ticker", "timestamp"],
        keep="last",
    )

    equity_bars_data = equity_bars_data.filter(
        (pl.col("open_price") > 0)
        & (pl.col("high_price") > 0)
        & (pl.col("low_price") > 0)
        & (pl.col("close_price") > 0)
    )

    equity_bars_data = equity_bars_schema.validate(equity_bars_data)

    equity_bars_data = filter_equity_bars(equity_bars_data)

    equity_details_data = pl.read_csv(io.BytesIO(equity_details_response.content))

    equity_details_data = equity_details_schema.validate(equity_details_data)

    consolidated_data = equity_details_data.join(
        equity_bars_data, on="ticker", how="inner"
    )

    retained_columns = (
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
    )

    return consolidated_data.select(retained_columns)
