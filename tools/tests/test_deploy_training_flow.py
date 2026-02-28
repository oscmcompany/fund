from unittest.mock import MagicMock, patch

from tools.deploy_training_flow import deploy_training_flow


@patch("tools.deploy_training_flow.training_pipeline")
def test_deploy_training_flow_calls_deploy(mock_pipeline: MagicMock) -> None:
    mock_deploy = MagicMock()
    mock_pipeline.deploy = mock_deploy

    deploy_training_flow(
        base_url="http://example.com",
        data_bucket="data-bucket",
        artifacts_bucket="artifacts-bucket",
        lookback_days=30,
    )

    mock_deploy.assert_called_once()
    call_kwargs = mock_deploy.call_args.kwargs
    assert call_kwargs["name"] == "daily-training"
    assert call_kwargs["work_pool_name"] == "training-pool"
    assert call_kwargs["cron"] == "0 22 * * *"
    assert call_kwargs["parameters"]["base_url"] == "http://example.com"
    assert call_kwargs["parameters"]["lookback_days"] == 30


@patch("tools.deploy_training_flow.training_pipeline")
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
    from prefect.flows import EntrypointType

    assert call_kwargs["entrypoint_type"] == EntrypointType.MODULE_PATH
    assert call_kwargs["build"] is False
    assert call_kwargs["push"] is False
