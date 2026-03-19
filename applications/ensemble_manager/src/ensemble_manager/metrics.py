import time

from fastapi import Response
from prometheus_client import Counter, Gauge, Histogram, generate_latest

prediction_requests_total = Counter(
    "ensemble_prediction_requests_total",
    "Total prediction requests received",
)
prediction_errors_total = Counter(
    "ensemble_prediction_errors_total",
    "Total prediction requests that failed",
    ["stage"],
)
prediction_duration_seconds = Histogram(
    "ensemble_prediction_duration_seconds",
    "Time to generate predictions end-to-end",
    buckets=[1, 5, 10, 30, 60, 120],
)
prediction_batch_count = Gauge(
    "ensemble_prediction_batch_count",
    "Number of batches in last prediction run",
)
prediction_row_count = Gauge(
    "ensemble_prediction_row_count",
    "Number of prediction rows in last run",
)
model_load_timestamp = Gauge(
    "ensemble_model_load_timestamp",
    "Unix timestamp of last successful model load",
)


def get_metrics() -> Response:
    return Response(
        content=generate_latest(),
        media_type="text/plain; version=0.0.4; charset=utf-8",
    )


def start_timer() -> float:
    return time.monotonic()


def observe_duration(start: float) -> None:
    prediction_duration_seconds.observe(time.monotonic() - start)
