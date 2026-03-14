from unittest.mock import MagicMock, patch

from prefect.flows import EntrypointType
from tide.deploy import deploy_training_flow

LOOKBACK_DAYS = 30


@patch("tide.deploy.training_pipeline")
def test_deploy_training_flow_calls_deploy(mock_pipeline: MagicMock) -> None:
    mock_deploy = MagicMock()
    mock_pipeline.deploy = mock_deploy

    deploy_training_flow(
        base_url="http://example.com",
        data_bucket="data-bucket",
        artifacts_bucket="artifacts-bucket",
        lookback_days=LOOKBACK_DAYS,
    )

    mock_deploy.assert_called_once()
    call_kwargs = mock_deploy.call_args.kwargs
    assert call_kwargs["name"] == "daily-training"
    assert call_kwargs["work_pool_name"] == "training-pool"
    assert call_kwargs["cron"] == "0 22 * * *"
    assert call_kwargs["parameters"]["base_url"] == "http://example.com"
    assert call_kwargs["parameters"]["lookback_days"] == LOOKBACK_DAYS


@patch("tide.deploy.training_pipeline")
def test_deploy_training_flow_sets_module_path_entrypoint(
    mock_pipeline: MagicMock,
) -> None:
    mock_deploy = MagicMock()
    mock_pipeline.deploy = mock_deploy

    deploy_training_flow(
        base_url="http://example.com",
        data_bucket="data-bucket",
        artifacts_bucket="artifacts-bucket",
    )

    call_kwargs = mock_deploy.call_args.kwargs
    assert call_kwargs["entrypoint_type"] == EntrypointType.MODULE_PATH
    assert call_kwargs["build"] is False
    assert call_kwargs["push"] is False
