import sys
from unittest.mock import MagicMock, patch

# sagemaker is a runtime dependency resolved from the workspace root; it is not
# available in isolated test environments, so mock its modules before importing
# the module under test to allow pytest to collect this file.
sys.modules.setdefault("sagemaker", MagicMock())
sys.modules.setdefault("sagemaker.estimator", MagicMock())
sys.modules.setdefault("sagemaker.inputs", MagicMock())
sys.modules.setdefault("sagemaker.session", MagicMock())

from tools.run_training_job import run_training_job  # noqa: E402


def test_run_training_job_calls_estimator_fit() -> None:
    mock_boto3_session = MagicMock()
    mock_sagemaker_session = MagicMock()
    mock_estimator_instance = MagicMock()

    with (
        patch(
            "tools.run_training_job.boto3.Session",
            return_value=mock_boto3_session,
        ),
        patch(
            "tools.run_training_job.Session",
            return_value=mock_sagemaker_session,
        ),
        patch(
            "tools.run_training_job.Estimator",
            return_value=mock_estimator_instance,
        ),
    ):
        run_training_job(
            application_name="equitypricemodel",
            trainer_image_uri="123456789.dkr.ecr.us-east-1.amazonaws.com/trainer:latest",
            s3_data_path="s3://test-bucket/training/data.parquet",
            iam_sagemaker_role_arn="arn:aws:iam::123456789:role/SageMakerRole",
            s3_artifact_path="s3://test-bucket/artifacts",
        )

    mock_estimator_instance.fit.assert_called_once()
    fit_call_args = mock_estimator_instance.fit.call_args.args[0]
    assert "train" in fit_call_args
