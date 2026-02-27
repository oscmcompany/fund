"""SSM Parameter Store configuration for fund services.

This module defines all SSM parameters used across the application.
Parameters can be updated in AWS without redeploying services.
"""

import pulumi
import pulumi_aws as aws

tags = {
    "project": "fund",
    "stack": pulumi.get_stack(),
    "manager": "pulumi",
}

# Portfolio Manager Configuration
uncertainty_threshold = aws.ssm.Parameter(
    "ssm_uncertainty_threshold",
    name="/fund/portfoliomanager/uncertainty_threshold",
    type="String",
    value="1.5",
    description="Maximum inter-quartile range for predictions to be considered valid",
    tags=tags,
)

# Equity Price Model Configuration
equitypricemodel_model_version = aws.ssm.Parameter(
    "ssm_equitypricemodel_model_version",
    name="/fund/equitypricemodel/model_version",
    type="String",
    value="",
    description=(
        "Artifact folder name for the equity price model. "
        "Empty string uses the latest available artifact."
    ),
    tags=tags,
    opts=pulumi.ResourceOptions(retain_on_delete=True),
)
