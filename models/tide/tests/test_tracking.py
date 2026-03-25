import os
from unittest.mock import patch

from tide.tracking import (
    get_environment_tag,
    get_host_tag,
    is_tracking_enabled,
)


def test_get_environment_tag_default() -> None:
    with patch.dict(os.environ, {}, clear=True):
        assert get_environment_tag() == "development"


def test_get_environment_tag_from_env() -> None:
    with patch.dict(os.environ, {"ENVIRONMENT": "production"}):
        assert get_environment_tag() == "production"


def test_get_host_tag_returns_string() -> None:
    assert isinstance(get_host_tag(), str)
    assert len(get_host_tag()) > 0


def test_is_tracking_enabled_false_by_default() -> None:
    with patch.dict(os.environ, {}, clear=True):
        assert is_tracking_enabled() is False


def test_is_tracking_enabled_true_when_uri_set() -> None:
    with patch.dict(os.environ, {"MLFLOW_TRACKING_URI": "http://localhost:5000"}):
        assert is_tracking_enabled() is True


def test_start_run_returns_none_when_not_configured() -> None:
    from tide.tracking import start_run  # noqa: PLC0415

    with patch.dict(os.environ, {}, clear=True):
        result = start_run(configuration={"lr": 0.001})
        assert result is None


def test_log_epoch_loss_noop_when_not_configured() -> None:
    from tide.tracking import log_epoch_loss  # noqa: PLC0415

    with patch.dict(os.environ, {}, clear=True):
        log_epoch_loss(epoch=0, loss=0.5)


def test_log_training_result_noop_when_not_configured() -> None:
    from tide.tracking import log_training_result  # noqa: PLC0415

    with patch.dict(os.environ, {}, clear=True):
        log_training_result(best_loss=0.1, all_losses=[0.5, 0.3, 0.1], total_epochs=3)


def test_end_run_noop_when_not_configured() -> None:
    from tide.tracking import end_run  # noqa: PLC0415

    with patch.dict(os.environ, {}, clear=True):
        end_run()
