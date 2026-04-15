import pulumi
from compute import acm_certificate_arn, alb, cluster, service_discovery_namespace
from config import account_id
from iam import github_actions_infrastructure_role, github_actions_oidc_provider
from networking import ecs_security_group, private_subnet_1, private_subnet_2, vpc
from storage import (
    data_bucket,
    data_manager_image_uri,
    data_manager_repository,
    ensemble_manager_image_uri,
    ensemble_manager_repository,
    model_artifacts_bucket,
    portfolio_manager_image_uri,
    portfolio_manager_repository,
    tide_runner_image_uri,
    tide_runner_repository,
)
from training import models_cluster, tide_trainer_task_definition

protocol = "https://" if acm_certificate_arn else "http://"

fund_base_url = pulumi.Output.concat(protocol, alb.dns_name)

readme_content = """
# infrastructure

> Application infrastructure resources

## Outputs

- base URL: {0}
"""

pulumi.export("aws_account_id", account_id)
pulumi.export("aws_vpc_id", vpc.id)
pulumi.export("aws_ecs_private_subnet_1_id", private_subnet_1.id)
pulumi.export("aws_ecs_private_subnet_2_id", private_subnet_2.id)
pulumi.export("aws_ecs_security_group_id", ecs_security_group.id)
pulumi.export("aws_ecs_cluster_name", cluster.name)
pulumi.export("aws_ecs_models_cluster_name", models_cluster.name)
pulumi.export(
    "aws_ecs_tide_trainer_task_definition_arn", tide_trainer_task_definition.arn
)
pulumi.export("aws_alb_dns_name", alb.dns_name)
pulumi.export("aws_alb_url", pulumi.Output.concat(protocol, alb.dns_name))
pulumi.export("aws_service_discovery_namespace", service_discovery_namespace.name)
pulumi.export("aws_ecr_applications_data_manager_server_image", data_manager_image_uri)
pulumi.export(
    "aws_ecr_applications_portfolio_manager_server_image", portfolio_manager_image_uri
)
pulumi.export(
    "aws_ecr_applications_ensemble_manager_server_image", ensemble_manager_image_uri
)
pulumi.export(
    "aws_ecr_applications_data_manager_server_repository",
    data_manager_repository.repository_url,
)
pulumi.export(
    "aws_ecr_applications_portfolio_manager_server_repository",
    portfolio_manager_repository.repository_url,
)
pulumi.export(
    "aws_ecr_applications_ensemble_manager_server_repository",
    ensemble_manager_repository.repository_url,
)
pulumi.export("aws_s3_data_bucket_name", pulumi.Output.unsecret(data_bucket.bucket))
pulumi.export(
    "aws_s3_model_artifacts_bucket_name",
    pulumi.Output.unsecret(model_artifacts_bucket.bucket),
)
pulumi.export(
    "aws_ecr_models_tide_runner_repository",
    tide_runner_repository.repository_url,
)
pulumi.export("aws_ecr_models_tide_runner_image", tide_runner_image_uri)
pulumi.export(
    "aws_iam_github_actions_infrastructure_role_arn",
    github_actions_infrastructure_role.arn,
)
pulumi.export(
    "aws_iam_github_actions_oidc_provider_arn",
    github_actions_oidc_provider.arn,
)
pulumi.export("fund_base_url", fund_base_url)
pulumi.export("readme", pulumi.Output.format(readme_content, fund_base_url))
