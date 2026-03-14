"""SSM Parameter Store configuration for fund services.

This module defines all SSM parameters used across the application.
Parameters can be updated in AWS without redeploying services.
"""

import pulumi
import pulumi_aws as aws
from config import stack_name

tags = {
    "project": "fund",
    "stack": pulumi.get_stack(),
    "manager": "pulumi",
}

# Portfolio Manager Configuration
uncertainty_threshold = aws.ssm.Parameter(
    "ssm_uncertainty_threshold",
    name=f"/fund/{stack_name}/portfolio-manager/uncertainty-threshold",
    type="String",
    value="1.5",
    description="Maximum inter-quartile range for predictions to be considered valid",
    tags=tags,
)

# Ensemble Manager Configuration
ensemble_manager_model_version = aws.ssm.Parameter(
    "ssm_ensemble_manager_model_version",
    name=f"/fund/{stack_name}/ensemble-manager/model-version",
    type="String",
    value="latest",
    description=(
        "Model artifact version to load (S3 key suffix or 'latest' for auto-discovery)"
    ),
    tags=tags,
)
