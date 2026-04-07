from unittest.mock import MagicMock, patch

from prefect.schedules import Schedule
from tide.deploy import deploy_training_flow

LOOKBACK_DAYS = 30


@patch("tide.deploy.training_pipeline")
def test_deploy_training_flow_calls_deploy(mock_pipeline: MagicMock) -> None:
    mock_deploy = MagicMock()
    mock_pipeline.deploy = mock_deploy

    deploy_training_flow(lookback_days=LOOKBACK_DAYS)

    mock_deploy.assert_called_once()
    call_kwargs = mock_deploy.call_args.kwargs
    assert call_kwargs["name"] == "tide-trainer-remote"
    assert call_kwargs["work_pool_name"] == "fund-models-remote"
    schedule = call_kwargs["schedule"]
    assert isinstance(schedule, Schedule)
    assert schedule.cron == "0 22 * * 1-5"
    assert schedule.timezone == "America/New_York"
    assert call_kwargs["parameters"]["lookback_days"] == LOOKBACK_DAYS


@patch("tide.deploy.training_pipeline")
def test_deploy_training_flow_sets_build_options(
    mock_pipeline: MagicMock,
) -> None:
    mock_deploy = MagicMock()
    mock_pipeline.deploy = mock_deploy

    image = "123456789.dkr.ecr.us-east-1.amazonaws.com/fund/tide-model-runner:latest"
    deploy_training_flow(image=image)

    call_kwargs = mock_deploy.call_args.kwargs
    assert call_kwargs["image"] == image
    assert call_kwargs["build"] is False
    assert call_kwargs["push"] is False
