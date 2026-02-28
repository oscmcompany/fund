from unittest.mock import patch

import pytest
from tools.run_training_job import run_training_job


def test_run_training_job_calls_training_pipeline() -> None:
    with patch(
        "tools.run_training_job.training_pipeline",
        return_value="s3://bucket/artifacts/model.tar.gz",
    ) as mock_pipeline:
        result = run_training_job(
            base_url="http://datamanager:8080",
            data_bucket="fund-data-bucket",
            artifacts_bucket="fund-artifacts-bucket",
            start_date="2024-01-01",
            end_date="2024-12-31",
            lookback_days=365,
        )

    mock_pipeline.assert_called_once_with(
        base_url="http://datamanager:8080",
        data_bucket="fund-data-bucket",
        artifacts_bucket="fund-artifacts-bucket",
        start_date="2024-01-01",
        end_date="2024-12-31",
        lookback_days=365,
    )
    assert result == "s3://bucket/artifacts/model.tar.gz"


def test_run_training_job_returns_artifact_path() -> None:
    expected_path = "s3://my-bucket/artifacts/equitypricemodel-trainer-2024-01-01/output/model.tar.gz"
    with patch(
        "tools.run_training_job.training_pipeline",
        return_value=expected_path,
    ):
        result = run_training_job(
            base_url="http://datamanager:8080",
            data_bucket="fund-data-bucket",
            artifacts_bucket="fund-artifacts-bucket",
            start_date="2024-01-01",
            end_date="2024-12-31",
        )

    assert result == expected_path


def test_run_training_job_propagates_errors() -> None:
    with (
        patch(
            "tools.run_training_job.training_pipeline",
            side_effect=RuntimeError("Training failed"),
        ),
        pytest.raises(RuntimeError, match="Training failed"),
    ):
        run_training_job(
            base_url="http://datamanager:8080",
            data_bucket="fund-data-bucket",
            artifacts_bucket="fund-artifacts-bucket",
            start_date="2024-01-01",
            end_date="2024-12-31",
        )
