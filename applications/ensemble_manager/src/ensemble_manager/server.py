import asyncio
import contextlib
import io
import json
import logging
import os
import tarfile
import tempfile
import threading
import uuid
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast

import boto3
import polars as pl
import structlog
from botocore.exceptions import ClientError
from fastapi import FastAPI, Request, Response, status
from internal.database import (
    close_pool,
    emit_event,
    get_pool,
    listen_for_events,
    update_consumer_offset,
)
from internal.equity_bars_schema import equity_bars_schema
from internal.timestamps import to_timestamp_milliseconds

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client

from internal.equity_details_schema import equity_details_schema
from tide.data import Data
from tide.model import Model

from .metrics import (
    get_metrics,
    model_load_timestamp,
    observe_duration,
    prediction_batch_count,
    prediction_errors_total,
    prediction_requests_total,
    prediction_row_count,
    start_timer,
)
from .predictions_schema import predictions_schema
from .preprocess import filter_equity_bars, filter_to_trained_tickers

try:
    _error_log_path = Path("/var/log/fund/ensemble-manager-errors.log")
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

AWS_S3_DATA_BUCKET_NAME = os.getenv("AWS_S3_DATA_BUCKET_NAME", "")

_swap_lock = threading.Lock()
_CLEANUP_DELAY_SECONDS = 120
_background_tasks: set[asyncio.Task] = set()
_inference_lock: asyncio.Lock = asyncio.Lock()


def find_latest_artifact_key(
    s3_client: "S3Client",
    bucket: str,
    prefix: str,
) -> str:
    """Find the latest model artifact under a prefix.

    Assumes folder names contain timestamps that sort alphabetically.
    E.g., artifacts/tide/2026-03-19-21-28-12-557/
    Only considers folders that contain output/model.tar.gz.
    """
    if not prefix.endswith("/"):
        prefix = prefix + "/"

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
        except ClientError as error:
            error_code = error.response.get("Error", {}).get("Code", "")
            if error_code in ("404", "NoSuchKey"):
                logger.debug("artifact_not_found_in_folder", folder=folder)
                continue
            raise
        else:
            logger.info(
                "found_latest_artifact",
                folder_count=len(folders),
                latest_folder=folder,
                artifact_key=artifact_key,
            )
            return artifact_key

    message = (
        f"No model.tar.gz found in any artifact folder under s3://{bucket}/{prefix}"
    )
    raise ValueError(message)


def download_and_extract_artifacts(
    s3_client: "S3Client",
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

        def _safe_tar_filter(
            member: tarfile.TarInfo, dest_path: str, /
        ) -> tarfile.TarInfo | None:
            """Validate tar members to prevent path traversal outside extract_path."""
            base = Path(dest_path).resolve()
            name = member.name
            if not name:
                return None
            if Path(name).is_absolute():
                message = f"Refusing absolute path from tar archive: {name!r}"
                raise ValueError(message)
            member_path = (base / name).resolve()
            if not str(member_path).startswith(str(base)):
                message = f"Refusing path outside target directory: {name!r}"
                raise ValueError(message)
            return member

        with tarfile.open(temp_path, "r:gz") as tar:
            tar.extractall(path=extract_path, filter=_safe_tar_filter)  # noqa: S202

        logger.info("extracted_artifacts", extract_path=str(extract_path))

    finally:
        temp_path.unlink(missing_ok=True)


def _resolve_artifact_key(
    s3_client: "S3Client",
    bucket: str,
    artifact_path: str,
) -> str:
    """Resolve the model artifact S3 key."""
    model_version = os.environ.get("MODEL_VERSION", "latest")

    if model_version != "latest":
        logger.info("Using model version from environment", model_version=model_version)
        if model_version.endswith(".tar.gz"):
            return model_version
        return f"{artifact_path.rstrip('/')}/{model_version}/output/model.tar.gz"

    if artifact_path.endswith(".tar.gz"):
        return artifact_path

    return find_latest_artifact_key(
        s3_client=s3_client,
        bucket=bucket,
        prefix=artifact_path,
    )


def cleanup_model_directory(model_directory: str) -> None:
    if model_directory != "." and Path(model_directory).exists():
        import shutil  # noqa: PLC0415

        shutil.rmtree(model_directory, ignore_errors=True)


async def _fetch_equity_bars(
    start_date: datetime,
    end_date: datetime,
) -> pl.DataFrame:
    """Query equity_bars directly from PostgreSQL for the given date range."""
    pool = await get_pool()
    async with pool.connection() as connection:
        result = await connection.execute(
            """SELECT ticker,
                      EXTRACT(EPOCH FROM timestamp)::bigint * 1000 AS timestamp,
                      open_price, high_price, low_price, close_price,
                      volume, volume_weighted_average_price
               FROM equity_bars
               WHERE timestamp >= %s AND timestamp <= %s
               ORDER BY ticker, timestamp""",
            (start_date, end_date),
        )
        rows = await result.fetchall()

    if not rows:
        return pl.DataFrame(
            schema={
                "ticker": pl.String,
                "timestamp": pl.Int64,
                "open_price": pl.Float64,
                "high_price": pl.Float64,
                "low_price": pl.Float64,
                "close_price": pl.Float64,
                "volume": pl.Int64,
                "volume_weighted_average_price": pl.Float64,
            }
        )

    return pl.DataFrame(
        {
            "ticker": [row[0] for row in rows],
            "timestamp": [row[1] for row in rows],
            "open_price": [row[2] for row in rows],
            "high_price": [row[3] for row in rows],
            "low_price": [row[4] for row in rows],
            "close_price": [row[5] for row in rows],
            "volume": [row[6] for row in rows],
            "volume_weighted_average_price": [row[7] for row in rows],
        }
    )


async def _fetch_equity_details(s3_client: "S3Client", bucket: str) -> pl.DataFrame:
    """Read equity details CSV from S3 and validate."""
    response = await asyncio.to_thread(
        s3_client.get_object,
        Bucket=bucket,
        Key="equity/details/details.csv",
    )
    csv_bytes: bytes = response["Body"].read()
    equity_details_data = pl.read_csv(io.BytesIO(csv_bytes))
    equity_details_data = equity_details_data.with_columns(
        pl.col(col).str.strip_chars()
        for col in equity_details_data.columns
        if equity_details_data[col].dtype == pl.String
    )
    equity_details_validated = equity_details_schema.validate(equity_details_data)
    return cast(
        "pl.DataFrame",
        equity_details_validated.collect()
        if isinstance(equity_details_validated, pl.LazyFrame)
        else equity_details_validated,
    )


async def _insert_predictions(
    predictions: pl.DataFrame,
    correlation_id: str,
    model_run_id: str,
) -> None:
    """Insert validated predictions into the predictions table."""
    rows = [
        (
            correlation_id,
            model_run_id,
            row["ticker"],
            datetime.fromtimestamp(row["timestamp"] / 1000.0, tz=UTC),
            row["quantile_10"],
            row["quantile_50"],
            row["quantile_90"],
        )
        for row in predictions.iter_rows(named=True)
    ]
    pool = await get_pool()
    async with pool.connection() as connection, connection.cursor() as cursor:
        await cursor.executemany(
            """INSERT INTO predictions
                       (correlation_id, model_run_id, ticker, timestamp,
                        quantile_10, quantile_50, quantile_90)
                   VALUES (%s, %s, %s, %s, %s, %s, %s)
                   ON CONFLICT (ticker, timestamp) DO UPDATE SET
                       correlation_id = EXCLUDED.correlation_id,
                       model_run_id = EXCLUDED.model_run_id,
                       quantile_10 = EXCLUDED.quantile_10,
                       quantile_50 = EXCLUDED.quantile_50,
                       quantile_90 = EXCLUDED.quantile_90""",
            rows,
        )
    logger.info("Inserted predictions into database", row_count=len(rows))


def _prepare_inference_data(
    equity_bars_data: pl.DataFrame,
    equity_details_data: pl.DataFrame,
) -> pl.DataFrame:
    """Deduplicate, filter, validate, and join equity bars with equity details."""
    fetched_tickers = equity_bars_data["ticker"].n_unique()

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

    equity_bars_validated = equity_bars_schema.validate(equity_bars_data)
    equity_bars_data = cast(
        "pl.DataFrame",
        equity_bars_validated.collect()
        if isinstance(equity_bars_validated, pl.LazyFrame)
        else equity_bars_validated,
    )

    equity_bars_data = filter_equity_bars(equity_bars_data)
    filtered_tickers = equity_bars_data["ticker"].n_unique()

    consolidated_data = equity_details_data.join(
        equity_bars_data, on="ticker", how="inner"
    )
    consolidated_tickers = consolidated_data["ticker"].n_unique()

    logger.info(
        "Inference data consolidated",
        fetched_tickers=fetched_tickers,
        filtered_tickers=filtered_tickers,
        consolidated_tickers=consolidated_tickers,
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


def _compute_predictions(
    tide_model: "Model",
    model_directory: str,
    data: pl.DataFrame,
    current_timestamp: datetime,
) -> pl.DataFrame | None:
    """Run TiDE inference on prepared data. Returns validated predictions or None."""
    from tinygrad.tensor import Tensor  # noqa: PLC0415

    tide_data = Data.load(directory_path=model_directory)
    trained_tickers = cast("set[str]", set(tide_data.mappings["ticker"].keys()))
    input_ticker_count = data.select(pl.col("ticker").n_unique()).item()
    data = filter_to_trained_tickers(data=data, trained_tickers=trained_tickers)

    if data.is_empty():
        prediction_errors_total.labels(stage="ticker_filtering").inc()
        logger.error(
            "No input tickers matched trained set",
            input_ticker_count=input_ticker_count,
            trained_ticker_count=len(trained_tickers),
        )
        return None

    try:
        tide_data.apply_and_set_data(data=data)
    except ValueError:
        prediction_errors_total.labels(stage="apply_preprocessing").inc()
        logger.exception("Failed to apply preprocessing to inference data")
        return None

    model = tide_model
    dataset = tide_data.get_dataset(
        data_type="predict", output_length=model.output_length
    )

    if len(dataset) == 0:
        prediction_errors_total.labels(stage="batch_creation").inc()
        logger.error("No data samples available for prediction")
        return None

    logger.info("Processing prediction dataset", samples_count=len(dataset))

    batch = {
        "past_continuous_features": Tensor(dataset.past_continuous),
        "past_categorical_features": Tensor(dataset.past_categorical),
        "future_categorical_features": Tensor(dataset.future_categorical),
        "static_categorical_features": Tensor(dataset.static_categorical),
    }

    raw_predictions = model.predict(inputs=batch)
    predictions = tide_data.postprocess_predictions(
        input_batch=batch,
        predictions=raw_predictions,
        current_datetime=current_timestamp,
    )

    processed_prediction_timestamp = current_timestamp + timedelta(
        days=model.output_length - 1
    )
    processed_predictions = predictions.filter(
        pl.col("timestamp")
        == to_timestamp_milliseconds(
            processed_prediction_timestamp.replace(
                hour=0, minute=0, second=0, microsecond=0
            )
        )
    )

    try:
        validated_result = predictions_schema.validate(processed_predictions)
        return cast(
            "pl.DataFrame",
            validated_result.collect()
            if isinstance(validated_result, pl.LazyFrame)
            else validated_result,
        )
    except Exception:
        prediction_errors_total.labels(stage="schema_validation").inc()
        logger.exception("Predictions failed schema validation")
        return None


async def _run_predictions_from_event(app: FastAPI) -> None:
    """Fetch data, run inference, persist to PG, and emit completion event."""
    if _inference_lock.locked():
        logger.info("Inference already in progress, skipping predictions_requested")
        return
    async with _inference_lock:
        await _run_predictions_from_event_inner(app)


async def _run_predictions_from_event_inner(app: FastAPI) -> None:  # noqa: PLR0915
    correlation_id = str(uuid.uuid4())
    model_run_id = getattr(app.state, "current_run_id", "") or ""

    prediction_requests_total.inc()
    timer_start = start_timer()
    logger.info("Starting event-triggered prediction generation")

    with _swap_lock:
        local_model_directory = app.state.model_directory
        local_tide_model = app.state.tide_model

    end_date = datetime.now(tz=UTC)
    start_date = end_date - timedelta(days=70)

    try:
        equity_bars_data = await _fetch_equity_bars(start_date, end_date)
    except Exception:
        prediction_errors_total.labels(stage="fetch_equity_bars").inc()
        logger.exception("Failed to fetch equity bars from database")
        await emit_event(
            "predictions_failed",
            {"correlation_id": correlation_id, "reason": "fetch_equity_bars"},
        )
        observe_duration(timer_start)
        return

    s3_client = cast("S3Client", getattr(app.state, "s3_data_client", None))
    data_bucket = getattr(app.state, "data_bucket_name", "")

    if s3_client is None or not data_bucket:
        prediction_errors_total.labels(stage="fetch_equity_details").inc()
        logger.error("S3 data client or bucket not configured")
        await emit_event(
            "predictions_failed",
            {"correlation_id": correlation_id, "reason": "s3_not_configured"},
        )
        observe_duration(timer_start)
        return

    try:
        equity_details_data = await _fetch_equity_details(s3_client, data_bucket)
    except Exception:
        prediction_errors_total.labels(stage="fetch_equity_details").inc()
        logger.exception("Failed to fetch equity details from S3")
        await emit_event(
            "predictions_failed",
            {"correlation_id": correlation_id, "reason": "fetch_equity_details"},
        )
        observe_duration(timer_start)
        return

    try:
        data = _prepare_inference_data(equity_bars_data, equity_details_data)
    except Exception:
        prediction_errors_total.labels(stage="parse_responses").inc()
        logger.exception("Failed to prepare inference data")
        await emit_event(
            "predictions_failed",
            {"correlation_id": correlation_id, "reason": "prepare_data"},
        )
        observe_duration(timer_start)
        return

    current_timestamp = datetime.now(tz=UTC)

    validated_predictions = await asyncio.to_thread(
        _compute_predictions,
        local_tide_model,
        local_model_directory,
        data,
        current_timestamp,
    )

    if validated_predictions is None:
        await emit_event(
            "predictions_failed",
            {"correlation_id": correlation_id, "reason": "inference"},
        )
        observe_duration(timer_start)
        return

    try:
        await _insert_predictions(validated_predictions, correlation_id, model_run_id)
    except Exception:
        prediction_errors_total.labels(stage="save_predictions").inc()
        logger.exception("Failed to insert predictions into database")
        await emit_event(
            "predictions_failed",
            {"correlation_id": correlation_id, "reason": "insert_predictions"},
        )
        observe_duration(timer_start)
        return

    await emit_event("predictions_completed", {"correlation_id": correlation_id})

    prediction_batch_count.set(1)
    prediction_row_count.set(validated_predictions.height)
    observe_duration(timer_start)
    logger.info(
        "Successfully generated and saved predictions via event trigger",
        correlation_id=correlation_id,
    )


async def _event_listener_task(app: FastAPI) -> None:
    """Background task that listens for PG events and triggers inference."""
    if not os.environ.get("DATABASE_URL"):
        logger.info("Event listener disabled, no DATABASE_URL configured")
        return

    consumer_name = "ensemble-manager"

    while True:
        try:

            async def handler(
                event_type: str, event_id: int, _payload: dict[str, Any]
            ) -> None:
                if event_type == "predictions_requested":
                    logger.info("Received predictions_requested event")
                    task = asyncio.create_task(_run_predictions_from_event(app))
                    _background_tasks.add(task)
                    task.add_done_callback(_background_tasks.discard)
                    await update_consumer_offset(consumer_name, event_id)

            await listen_for_events("events", handler)
        except asyncio.CancelledError:
            return
        except Exception:
            logger.exception("Event listener error, reconnecting in 30s")
            await asyncio.sleep(30)


async def _sync_run_metadata(
    s3_client: "S3Client",
    bucket: str,
    artifact_key: str,
) -> str | None:
    """Fetch run_metadata.json and evaluation.json from S3.

    Inserts or updates the corresponding row in the model_runs table.
    Returns the run_id on success, or None if metadata is unavailable.
    """
    if not os.environ.get("DATABASE_URL"):
        return None

    artifact_folder = str(Path(artifact_key).parent.parent)
    metadata_key = f"{artifact_folder}/run_metadata.json"

    try:
        response = s3_client.get_object(Bucket=bucket, Key=metadata_key)
        metadata = json.loads(response["Body"].read())
    except ClientError as error:
        error_code = error.response.get("Error", {}).get("Code", "")
        if error_code in ("404", "NoSuchKey"):
            logger.debug("No run_metadata.json found", metadata_key=metadata_key)
            return None
        raise
    except (json.JSONDecodeError, KeyError):
        logger.warning("Invalid run_metadata.json", metadata_key=metadata_key)
        return None

    run_id = metadata.get("artifact_timestamp", "")
    if not run_id:
        return None

    evaluation_key = f"{artifact_folder}/evaluation.json"
    continuous_ranked_probability_score: float | None = None
    directional_accuracy: float | None = None
    quantile_coverage: float | None = None
    try:
        evaluation_response = s3_client.get_object(Bucket=bucket, Key=evaluation_key)
        evaluation = json.loads(evaluation_response["Body"].read())
        continuous_ranked_probability_score = evaluation.get("crps")
        directional_accuracy = evaluation.get("directional_accuracy")
        quantile_coverage = evaluation.get("quantile_coverage")
    except ClientError as error:
        error_code = error.response.get("Error", {}).get("Code", "")
        if error_code in ("404", "NoSuchKey"):
            logger.debug("No evaluation.json found", evaluation_key=evaluation_key)
        else:
            raise
    except (json.JSONDecodeError, KeyError):
        logger.warning("Invalid evaluation.json", evaluation_key=evaluation_key)

    try:
        pool = await get_pool()
        async with pool.connection() as connection:
            await connection.execute(
                """INSERT INTO model_runs (
                       run_id, artifact_key, training_data_key,
                       start_date, end_date, lookback_days,
                       status, stage_counts, completed_at,
                       continuous_ranked_probability_score,
                       directional_accuracy, quantile_coverage
                   ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, now(), %s, %s, %s)
                   ON CONFLICT (run_id) DO UPDATE SET
                       artifact_key = EXCLUDED.artifact_key,
                       training_data_key = EXCLUDED.training_data_key,
                       start_date = EXCLUDED.start_date,
                       end_date = EXCLUDED.end_date,
                       lookback_days = EXCLUDED.lookback_days,
                       status = EXCLUDED.status,
                       stage_counts = EXCLUDED.stage_counts,
                       completed_at = EXCLUDED.completed_at,
                       continuous_ranked_probability_score
                           = EXCLUDED.continuous_ranked_probability_score,
                       directional_accuracy = EXCLUDED.directional_accuracy,
                       quantile_coverage = EXCLUDED.quantile_coverage""",
                (
                    run_id,
                    artifact_key,
                    metadata.get("training_data_key"),
                    metadata.get("start_date"),
                    metadata.get("end_date"),
                    metadata.get("lookback_days"),
                    "completed",
                    json.dumps(metadata.get("stage_counts")),
                    continuous_ranked_probability_score,
                    directional_accuracy,
                    quantile_coverage,
                ),
            )
        logger.info("Synced model run metadata", run_id=run_id)
        return run_id  # noqa: TRY300
    except Exception:
        logger.exception("Failed to sync run metadata to PostgreSQL")
        return None


async def _artifact_polling_task(app: FastAPI) -> None:
    """Background task that polls S3 for new model artifacts."""
    bucket = os.environ.get("AWS_S3_MODEL_ARTIFACTS_BUCKET_NAME")
    artifact_path = os.environ.get("AWS_S3_MODEL_ARTIFACT_PATH", "artifacts/tide/")

    if not bucket:
        logger.info("Artifact polling disabled, no bucket configured")
        return

    s3_client = boto3.client("s3")
    poll_interval = 60

    while True:
        await asyncio.sleep(poll_interval)

        try:
            latest_key = await asyncio.to_thread(
                _resolve_artifact_key,
                s3_client=s3_client,
                bucket=bucket,
                artifact_path=artifact_path,
            )
        except ValueError:
            logger.debug("No artifacts found during polling")
            continue
        except Exception:
            logger.exception("Transient error during artifact polling")
            continue

        current_key = getattr(app.state, "current_artifact_key", None)
        if latest_key == current_key:
            continue

        logger.info(
            "New artifact detected",
            current_key=current_key,
            new_key=latest_key,
        )

        new_directory = tempfile.mkdtemp(prefix="model_artifacts_")
        try:
            await asyncio.to_thread(
                download_and_extract_artifacts,
                s3_client=s3_client,
                bucket=bucket,
                artifact_key=latest_key,
                extract_path=Path(new_directory),
            )

            new_model = await asyncio.to_thread(
                Model.load, directory_path=new_directory
            )
        except Exception:
            logger.exception("Failed to download or load model artifact")
            cleanup_model_directory(new_directory)
            continue

        with _swap_lock:
            old_directory = getattr(app.state, "model_directory", None)
            app.state.tide_model = new_model
            app.state.model_directory = new_directory
            app.state.current_artifact_key = latest_key

        model_load_timestamp.set(datetime.now(tz=UTC).timestamp())
        logger.info("Hot-swapped model", artifact_key=latest_key)

        if old_directory and old_directory != ".":
            await asyncio.sleep(_CLEANUP_DELAY_SECONDS)
            cleanup_model_directory(old_directory)

        try:
            run_id = await _sync_run_metadata(
                s3_client=s3_client,
                bucket=bucket,
                artifact_key=latest_key,
            )
            app.state.current_run_id = run_id or ""
        except Exception:
            logger.exception("Failed to sync run metadata after hot-swap")


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:  # noqa: PLR0915
    """Load model artifacts from S3 at startup."""

    bucket = os.environ.get("AWS_S3_MODEL_ARTIFACTS_BUCKET_NAME")
    artifact_path = os.environ.get("AWS_S3_MODEL_ARTIFACT_PATH", "artifacts/tide/")
    model_directory = "."
    s3_client: S3Client | None = None

    if bucket:
        s3_client = boto3.client("s3")

        model_directory = tempfile.mkdtemp(prefix="model_artifacts_")
        extract_path = Path(model_directory)

        try:
            artifact_key = _resolve_artifact_key(
                s3_client=s3_client,
                bucket=bucket,
                artifact_path=artifact_path,
            )

            download_and_extract_artifacts(
                s3_client=s3_client,
                bucket=bucket,
                artifact_key=artifact_key,
                extract_path=extract_path,
            )
        except Exception:
            logger.exception("Failed to download artifacts")
            raise

        app.state.current_artifact_key = artifact_key
        logger.info("Loading model", directory=model_directory)
    else:
        app.state.current_artifact_key = None
        logger.info("Loading model from local", directory=model_directory)

    data_bucket = AWS_S3_DATA_BUCKET_NAME
    if data_bucket:
        app.state.s3_data_client = boto3.client("s3")
        app.state.data_bucket_name = data_bucket
        logger.info("S3 data client initialized", bucket=data_bucket)
    else:
        app.state.s3_data_client = None
        app.state.data_bucket_name = ""

    app.state.model_directory = model_directory
    app.state.tide_model = Model.load(directory_path=model_directory)
    app.state.current_run_id = ""
    model_load_timestamp.set(datetime.now(tz=UTC).timestamp())
    logger.info("model_loaded_successfully")

    if app.state.current_artifact_key and bucket and s3_client is not None:
        try:
            run_id = await _sync_run_metadata(
                s3_client=s3_client,
                bucket=bucket,
                artifact_key=app.state.current_artifact_key,
            )
            app.state.current_run_id = run_id or ""
        except Exception:
            logger.exception("Failed to sync run metadata at startup")

    polling_task = asyncio.create_task(_artifact_polling_task(app))
    listener_task = asyncio.create_task(_event_listener_task(app))

    try:
        yield
    finally:
        polling_task.cancel()
        listener_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await polling_task
        with contextlib.suppress(asyncio.CancelledError):
            await listener_task
        await close_pool()
        cleanup_model_directory(app.state.model_directory)


application = FastAPI(lifespan=lifespan)
application.state.tide_model = None
application.state.model_directory = "."
application.state.current_artifact_key = None
application.state.current_run_id = ""
application.state.s3_data_client = None
application.state.data_bucket_name = ""


@application.get("/health")
def health_check(request: Request) -> Response:
    checks: dict[str, str] = {}
    healthy = True

    model = getattr(request.app.state, "tide_model", None)
    if model is not None:
        checks["model"] = "ok"
    else:
        checks["model"] = "error"
        healthy = False

    model_dir = getattr(request.app.state, "model_directory", None)
    if model_dir and Path(model_dir).exists():
        checks["model_directory"] = "ok"
    else:
        checks["model_directory"] = "error"
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


@application.post("/model/predictions")
@application.post("/predictions")
async def create_predictions(request: Request) -> Response:  # noqa: PLR0911
    prediction_requests_total.inc()
    timer_start = start_timer()
    logger.info("Starting prediction generation process")

    with _swap_lock:
        local_model_directory = request.app.state.model_directory
        local_tide_model = request.app.state.tide_model

    end_date = datetime.now(tz=UTC)
    # need >= 42 trading days (35 input + 7 output), ~60 calendar days + buffer
    start_date = end_date - timedelta(days=70)

    try:
        equity_bars_data = await _fetch_equity_bars(start_date, end_date)
    except Exception:
        prediction_errors_total.labels(stage="fetch_equity_bars").inc()
        logger.exception(
            "Failed to fetch equity bars data",
            start_date=start_date.isoformat(),
            end_date=end_date.isoformat(),
        )
        observe_duration(timer_start)
        return Response(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR)

    s3_client = cast("S3Client", getattr(request.app.state, "s3_data_client", None))
    data_bucket = getattr(request.app.state, "data_bucket_name", "")

    if s3_client is None or not data_bucket:
        prediction_errors_total.labels(stage="fetch_equity_details").inc()
        logger.error("S3 data client or bucket not configured")
        observe_duration(timer_start)
        return Response(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR)

    try:
        equity_details_data = await _fetch_equity_details(s3_client, data_bucket)
    except Exception:
        prediction_errors_total.labels(stage="fetch_equity_details").inc()
        logger.exception("Failed to fetch equity details data")
        observe_duration(timer_start)
        return Response(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR)

    try:
        data = _prepare_inference_data(equity_bars_data, equity_details_data)
    except Exception:
        prediction_errors_total.labels(stage="parse_responses").inc()
        logger.exception("Failed to prepare inference data")
        observe_duration(timer_start)
        return Response(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR)

    current_timestamp = datetime.now(tz=UTC)

    validated_predictions = await asyncio.to_thread(
        _compute_predictions,
        local_tide_model,
        local_model_directory,
        data,
        current_timestamp,
    )

    if validated_predictions is None:
        observe_duration(timer_start)
        return Response(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR)

    correlation_id = str(uuid.uuid4())
    model_run_id = getattr(request.app.state, "current_run_id", "") or ""

    try:
        await _insert_predictions(validated_predictions, correlation_id, model_run_id)
    except Exception:
        prediction_errors_total.labels(stage="save_predictions").inc()
        logger.exception("Failed to insert predictions into database")
        observe_duration(timer_start)
        return Response(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR)

    await emit_event("predictions_completed", {"correlation_id": correlation_id})

    prediction_batch_count.set(1)
    prediction_row_count.set(validated_predictions.height)
    observe_duration(timer_start)
    logger.info("Successfully generated predictions", correlation_id=correlation_id)

    return Response(
        content=json.dumps(
            {"correlation_id": correlation_id, "data": validated_predictions.to_dicts()}
        ).encode("utf-8"),
        status_code=status.HTTP_200_OK,
    )
