import time

from fastapi import Response
from portfolio_manager.metrics import get_metrics, observe_duration, start_timer


def test_get_metrics_returns_response() -> None:
    result = get_metrics()

    assert isinstance(result, Response)
    assert result.media_type == "text/plain; version=0.0.4; charset=utf-8"
    assert result.body is not None


def test_start_timer_returns_float() -> None:
    result = start_timer()

    assert isinstance(result, float)
    assert result > 0


def test_observe_duration_accepts_start_timer_value() -> None:
    start = start_timer()
    time.sleep(0.01)

    observe_duration(start)


def test_observe_duration_accepts_zero_start() -> None:
    observe_duration(0.0)
