import json
from typing import cast

import parameters
import pulumi
import pulumi_aws as aws


def require_secret_config_object(
    config: pulumi.Config,
    key: str,
) -> pulumi.Output[dict[str, str]]:
    config_full_key = config.full_key(key)
    if not pulumi.runtime.is_config_secret(config_full_key):
        message = f"Pulumi config '{key}' must be configured as a secret object."
        raise ValueError(message)

    return cast(
        "pulumi.Output[dict[str, str]]",
        config.require_secret_object(key),
    )


def serialize_secret_config_object(
    secret_values: dict[str, str],
    config_key: str,
    required_keys: set[str],
) -> str:
    missing_secret_keys = sorted(required_keys.difference(secret_values))
    if missing_secret_keys:
        message = (
            f"Pulumi config '{config_key}' is missing required keys: "
            f"{', '.join(missing_secret_keys)}."
        )
        raise ValueError(message)

    return json.dumps(secret_values, sort_keys=True)


stack_name = pulumi.get_stack()
if stack_name != "production":
    message = "Only the production Pulumi stack is supported."
    raise ValueError(message)

stack_config = pulumi.Config("fund")
aws_config = pulumi.Config("aws")

region = aws_config.require("region")

github_actions_role_name = stack_config.require("githubActionsRoleName")
github_repository = stack_config.require("githubRepository")
github_branch = stack_config.require("githubBranch")
github_workflow_files = cast(
    "list[str]",
    stack_config.require_object("githubWorkflowFiles"),
)
if not github_workflow_files:
    message = (
        "Pulumi config 'githubWorkflowFiles' must include at least one workflow file."
    )
    raise ValueError(message)

budget_alert_email_addresses_full_key = stack_config.full_key(
    "budgetAlertEmailAddresses"
)
if not pulumi.runtime.is_config_secret(budget_alert_email_addresses_full_key):
    message = (
        "Pulumi config 'budgetAlertEmailAddresses' must be configured as a secret list."
    )
    raise ValueError(message)
budget_alert_email_addresses = cast(
    "list[str]",
    stack_config.require_object("budgetAlertEmailAddresses"),
)
if not budget_alert_email_addresses:
    message = (
        "Pulumi config 'budgetAlertEmailAddresses' must include at least one email "
        "address."
    )
    raise ValueError(message)

monthly_budget_limit_usd = stack_config.require_float("monthlyBudgetLimitUsd")
sagemaker_execution_role_name = stack_config.require("sagemakerExecutionRoleName")

datamanager_secret_name = stack_config.require_secret("datamanagerSecretName")
portfoliomanager_secret_name = stack_config.require_secret("portfoliomanagerSecretName")
shared_secret_name = stack_config.require_secret("sharedSecretName")

datamanager_secret_values = require_secret_config_object(
    stack_config,
    "datamanagerSecretValue",
)
portfoliomanager_secret_values = require_secret_config_object(
    stack_config,
    "portfoliomanagerSecretValue",
)
shared_secret_values = require_secret_config_object(
    stack_config,
    "sharedSecretValue",
)

github_oidc_audience_claim = "token.actions.githubusercontent.com:aud"
github_oidc_repository_claim = "token.actions.githubusercontent.com:repository"
github_oidc_ref_claim = "token.actions.githubusercontent.com:ref"
github_oidc_sub_claim = "token.actions.githubusercontent.com:sub"
github_oidc_workflow_ref_claim = "token.actions.githubusercontent.com:job_workflow_ref"

github_workflow_refs = [
    (
        f"{github_repository}/.github/workflows/{github_workflow_file}"
        f"@refs/heads/{github_branch}"
    )
    for github_workflow_file in github_workflow_files
]

current_identity = aws.get_caller_identity()

account_id = current_identity.account_id

availability_zone_a = f"{region}a"
availability_zone_b = f"{region}b"

tags = {
    "project": "fund",
    "stack": stack_name,
    "manager": "pulumi",
}

github_oidc_provider_arn = pulumi.Output.concat(
    "arn:aws:iam::",
    account_id,
    ":oidc-provider/token.actions.githubusercontent.com",
)

datamanager_secret = aws.secretsmanager.Secret(
    "datamanager_secret",
    name=datamanager_secret_name,
    recovery_window_in_days=0,
    tags=tags,
)

portfoliomanager_secret = aws.secretsmanager.Secret(
    "portfoliomanager_secret",
    name=portfoliomanager_secret_name,
    recovery_window_in_days=0,
    tags=tags,
)

shared_secret = aws.secretsmanager.Secret(
    "shared_secret",
    name=shared_secret_name,
    recovery_window_in_days=0,
    tags=tags,
)

aws.secretsmanager.SecretVersion(
    "datamanager_secret_version",
    secret_id=datamanager_secret.id,
    secret_string=datamanager_secret_values.apply(
        lambda values: serialize_secret_config_object(
            values,
            "datamanagerSecretValue",
            {"MASSIVE_API_KEY"},
        )
    ),
)

aws.secretsmanager.SecretVersion(
    "portfoliomanager_secret_version",
    secret_id=portfoliomanager_secret.id,
    secret_string=portfoliomanager_secret_values.apply(
        lambda values: serialize_secret_config_object(
            values,
            "portfoliomanagerSecretValue",
            {"ALPACA_API_KEY_ID", "ALPACA_API_SECRET", "ALPACA_IS_PAPER"},
        )
    ),
)

aws.secretsmanager.SecretVersion(
    "shared_secret_version",
    secret_id=shared_secret.id,
    secret_string=shared_secret_values.apply(
        lambda values: serialize_secret_config_object(
            values,
            "sharedSecretValue",
            {"SENTRY_DSN"},
        )
    ),
)

infrastructure_alerts_topic = aws.sns.Topic(
    "infrastructure_alerts_topic",
    name="fund-infrastructure-alerts",
    tags=tags,
)

for notification_email_index, notification_email_address in enumerate(
    budget_alert_email_addresses,
    start=1,
):
    aws.sns.TopicSubscription(
        f"infrastructure_alert_email_subscription_{notification_email_index}",
        topic=infrastructure_alerts_topic.arn,
        protocol="email",
        endpoint=notification_email_address,
    )

aws.budgets.Budget(
    "production_cost_budget",
    account_id=account_id,
    name="fund-monthly-cost",
    budget_type="COST",
    time_unit="MONTHLY",
    limit_amount=f"{monthly_budget_limit_usd:.2f}",
    limit_unit="USD",
    notifications=[
        aws.budgets.BudgetNotificationArgs(
            comparison_operator="GREATER_THAN",
            notification_type="ACTUAL",
            threshold=monthly_budget_limit_usd,
            threshold_type="ABSOLUTE_VALUE",
            subscriber_email_addresses=budget_alert_email_addresses,
        ),
        aws.budgets.BudgetNotificationArgs(
            comparison_operator="GREATER_THAN",
            notification_type="FORECASTED",
            threshold=monthly_budget_limit_usd,
            threshold_type="ABSOLUTE_VALUE",
            subscriber_email_addresses=budget_alert_email_addresses,
        ),
    ],
)

# S3 Data Bucket for storing equity bars, predictions, portfolios
data_bucket = aws.s3.Bucket(
    "data_bucket",
    bucket_prefix="fund-data-",
    opts=pulumi.ResourceOptions(retain_on_delete=True),
    tags=tags,
)

aws.s3.BucketServerSideEncryptionConfiguration(
    "data_bucket_encryption",
    bucket=data_bucket.id,
    rules=[
        aws.s3.BucketServerSideEncryptionConfigurationRuleArgs(
            apply_server_side_encryption_by_default=aws.s3.BucketServerSideEncryptionConfigurationRuleApplyServerSideEncryptionByDefaultArgs(
                sse_algorithm="AES256",
            ),
        )
    ],
)

aws.s3.BucketPublicAccessBlock(
    "data_bucket_public_access_block",
    bucket=data_bucket.id,
    block_public_acls=True,
    block_public_policy=True,
    ignore_public_acls=True,
    restrict_public_buckets=True,
)

aws.s3.BucketVersioning(
    "data_bucket_versioning",
    bucket=data_bucket.id,
    versioning_configuration=aws.s3.BucketVersioningVersioningConfigurationArgs(
        status="Enabled",
    ),
)

# S3 Model Artifacts Bucket for storing trained model weights and checkpoints
model_artifacts_bucket = aws.s3.Bucket(
    "model_artifacts_bucket",
    bucket_prefix="fund-model-artifacts-",
    opts=pulumi.ResourceOptions(retain_on_delete=True),
    tags=tags,
)

aws.s3.BucketServerSideEncryptionConfiguration(
    "model_artifacts_bucket_encryption",
    bucket=model_artifacts_bucket.id,
    rules=[
        aws.s3.BucketServerSideEncryptionConfigurationRuleArgs(
            apply_server_side_encryption_by_default=aws.s3.BucketServerSideEncryptionConfigurationRuleApplyServerSideEncryptionByDefaultArgs(
                sse_algorithm="AES256",
            ),
        )
    ],
)

aws.s3.BucketPublicAccessBlock(
    "model_artifacts_bucket_public_access_block",
    bucket=model_artifacts_bucket.id,
    block_public_acls=True,
    block_public_policy=True,
    ignore_public_acls=True,
    restrict_public_buckets=True,
)

aws.s3.BucketVersioning(
    "model_artifacts_bucket_versioning",
    bucket=model_artifacts_bucket.id,
    versioning_configuration=aws.s3.BucketVersioningVersioningConfigurationArgs(
        status="Enabled",
    ),
)

# ECR Repositories - these must exist before images can be pushed
datamanager_repository = aws.ecr.Repository(
    "datamanager_repository",
    name="fund/datamanager-server",
    image_tag_mutability="MUTABLE",
    image_scanning_configuration=aws.ecr.RepositoryImageScanningConfigurationArgs(
        scan_on_push=True,
    ),
    tags=tags,
)

portfoliomanager_repository = aws.ecr.Repository(
    "portfoliomanager_repository",
    name="fund/portfoliomanager-server",
    image_tag_mutability="MUTABLE",
    image_scanning_configuration=aws.ecr.RepositoryImageScanningConfigurationArgs(
        scan_on_push=True,
    ),
    tags=tags,
)

equitypricemodel_repository = aws.ecr.Repository(
    "equitypricemodel_repository",
    name="fund/equitypricemodel-server",
    image_tag_mutability="MUTABLE",
    image_scanning_configuration=aws.ecr.RepositoryImageScanningConfigurationArgs(
        scan_on_push=True,
    ),
    tags=tags,
)

equitypricemodel_trainer_repository = aws.ecr.Repository(
    "equitypricemodel_trainer_repository",
    name="fund/equitypricemodel-trainer",
    image_tag_mutability="MUTABLE",
    image_scanning_configuration=aws.ecr.RepositoryImageScanningConfigurationArgs(
        scan_on_push=True,
    ),
    tags=tags,
)

# Generate image URIs - these will be used in task definitions
# For initial deployment, use a placeholder that will be updated when images are pushed
datamanager_image_uri = datamanager_repository.repository_url.apply(
    lambda url: f"{url}:latest"
)
portfoliomanager_image_uri = portfoliomanager_repository.repository_url.apply(
    lambda url: f"{url}:latest"
)
equitypricemodel_image_uri = equitypricemodel_repository.repository_url.apply(
    lambda url: f"{url}:latest"
)
equitypricemodel_trainer_image_uri = (
    equitypricemodel_trainer_repository.repository_url.apply(
        lambda url: f"{url}:latest"
    )
)

vpc = aws.ec2.Vpc(
    "vpc",
    cidr_block="10.0.0.0/16",
    enable_dns_hostnames=True,
    enable_dns_support=True,
    tags=tags,
)

# Internet Gateway for public subnets
igw = aws.ec2.InternetGateway(
    "igw",
    vpc_id=vpc.id,
    tags=tags,
)

# Public subnets for ALB
public_subnet_1 = aws.ec2.Subnet(
    "public_subnet_1",
    vpc_id=vpc.id,
    cidr_block="10.0.1.0/24",
    availability_zone=availability_zone_a,
    map_public_ip_on_launch=True,
    tags=tags,
)

public_subnet_2 = aws.ec2.Subnet(
    "public_subnet_2",
    vpc_id=vpc.id,
    cidr_block="10.0.2.0/24",
    availability_zone=availability_zone_b,
    map_public_ip_on_launch=True,
    tags=tags,
)

# Private subnets for ECS tasks
private_subnet_1 = aws.ec2.Subnet(
    "private_subnet_1",
    vpc_id=vpc.id,
    cidr_block="10.0.3.0/24",
    availability_zone=availability_zone_a,
    tags=tags,
)

private_subnet_2 = aws.ec2.Subnet(
    "private_subnet_2",
    vpc_id=vpc.id,
    cidr_block="10.0.4.0/24",
    availability_zone=availability_zone_b,
    tags=tags,
)

public_route_table = aws.ec2.RouteTable(
    "public_route_table",
    vpc_id=vpc.id,
    tags=tags,
)

aws.ec2.Route(
    "public_internet_route",
    route_table_id=public_route_table.id,
    destination_cidr_block="0.0.0.0/0",
    gateway_id=igw.id,
)

aws.ec2.RouteTableAssociation(
    "public_subnet_1_rta",
    subnet_id=public_subnet_1.id,
    route_table_id=public_route_table.id,
)

aws.ec2.RouteTableAssociation(
    "public_subnet_2_rta",
    subnet_id=public_subnet_2.id,
    route_table_id=public_route_table.id,
)

eip = aws.ec2.Eip(
    "nat_elastic_ip",
    domain="vpc",
    tags=tags,
)

# NAT Gateway in public subnet for private subnet outbound traffic
nat = aws.ec2.NatGateway(
    "nat_gateway",
    subnet_id=public_subnet_1.id,
    allocation_id=eip.id,
    tags=tags,
)

aws.cloudwatch.MetricAlarm(
    "nat_gateway_bytes_out_to_destination_alarm",
    name="fund-nat-gateway-bytes-out-to-destination",
    alarm_description=(
        "Triggers when NAT gateway outbound traffic exceeds 500 MB per hour for "
        "2 consecutive hours."
    ),
    namespace="AWS/NATGateway",
    metric_name="BytesOutToDestination",
    statistic="Sum",
    period=3600,
    evaluation_periods=2,
    threshold=500_000_000,
    comparison_operator="GreaterThanThreshold",
    treat_missing_data="notBreaching",
    dimensions={"NatGatewayId": nat.id},
    alarm_actions=[infrastructure_alerts_topic.arn],
    ok_actions=[infrastructure_alerts_topic.arn],
    tags=tags,
)

private_route_table = aws.ec2.RouteTable(
    "private_route_table",
    vpc_id=vpc.id,
    tags=tags,
)

aws.ec2.Route(
    "nat_route",
    route_table_id=private_route_table.id,
    destination_cidr_block="0.0.0.0/0",
    nat_gateway_id=nat.id,
)

aws.ec2.RouteTableAssociation(
    "private_subnet_1_rta",
    subnet_id=private_subnet_1.id,
    route_table_id=private_route_table.id,
)

aws.ec2.RouteTableAssociation(
    "private_subnet_2_rta",
    subnet_id=private_subnet_2.id,
    route_table_id=private_route_table.id,
)

alb_security_group = aws.ec2.SecurityGroup(
    "alb_sg",
    name="fund-alb",
    vpc_id=vpc.id,
    description="Security group for ALB",
    ingress=[
        aws.ec2.SecurityGroupIngressArgs(
            protocol="tcp",
            from_port=80,
            to_port=80,
            cidr_blocks=["0.0.0.0/0"],
            description="Allow HTTP",
        ),
        aws.ec2.SecurityGroupIngressArgs(
            protocol="tcp",
            from_port=443,
            to_port=443,
            cidr_blocks=["0.0.0.0/0"],
            description="Allow HTTPS",
        ),
    ],
    egress=[
        aws.ec2.SecurityGroupEgressArgs(
            protocol="-1",
            from_port=0,
            to_port=0,
            cidr_blocks=["0.0.0.0/0"],
            description="Allow all outbound",
        )
    ],
    tags=tags,
)

ecs_security_group = aws.ec2.SecurityGroup(
    "ecs_sg",
    name="fund-ecs-tasks",
    vpc_id=vpc.id,
    description="Security group for ECS tasks",
    tags=tags,
)

# Allow ALB to reach ECS tasks on port 8080
aws.ec2.SecurityGroupRule(
    "ecs_from_alb",
    type="ingress",
    security_group_id=ecs_security_group.id,
    source_security_group_id=alb_security_group.id,
    protocol="tcp",
    from_port=8080,
    to_port=8080,
    description="Allow ALB traffic",
)

# Allow ECS tasks to communicate with each other
aws.ec2.SecurityGroupRule(
    "ecs_self_ingress",
    type="ingress",
    security_group_id=ecs_security_group.id,
    source_security_group_id=ecs_security_group.id,
    protocol="tcp",
    from_port=8080,
    to_port=8080,
    description="Allow inter-service communication",
)

# Allow all outbound traffic from ECS tasks
aws.ec2.SecurityGroupRule(
    "ecs_egress",
    type="egress",
    security_group_id=ecs_security_group.id,
    protocol="-1",
    from_port=0,
    to_port=0,
    cidr_blocks=["0.0.0.0/0"],
    description="Allow all outbound",
)

# VPC Endpoints Security Group
vpc_endpoints_security_group = aws.ec2.SecurityGroup(
    "vpc_endpoints_sg",
    name="fund-vpc-endpoints",
    vpc_id=vpc.id,
    description="Security group for VPC endpoints",
    tags=tags,
)

aws.ec2.SecurityGroupRule(
    "vpc_endpoints_ingress",
    type="ingress",
    security_group_id=vpc_endpoints_security_group.id,
    source_security_group_id=ecs_security_group.id,
    protocol="tcp",
    from_port=443,
    to_port=443,
    description="Allow HTTPS from ECS tasks",
)

# S3 Gateway Endpoint
s3_gateway_endpoint = aws.ec2.VpcEndpoint(
    "s3_gateway_endpoint",
    vpc_id=vpc.id,
    service_name=pulumi.Output.concat("com.amazonaws.", region, ".s3"),
    vpc_endpoint_type="Gateway",
    route_table_ids=[private_route_table.id],
    tags=tags,
)

# ECR API Interface Endpoint
ecr_api_endpoint = aws.ec2.VpcEndpoint(
    "ecr_api_endpoint",
    vpc_id=vpc.id,
    service_name=pulumi.Output.concat("com.amazonaws.", region, ".ecr.api"),
    vpc_endpoint_type="Interface",
    subnet_ids=[private_subnet_1.id, private_subnet_2.id],
    security_group_ids=[vpc_endpoints_security_group.id],
    private_dns_enabled=True,
    tags=tags,
)

# ECR DKR Interface Endpoint
ecr_dkr_endpoint = aws.ec2.VpcEndpoint(
    "ecr_dkr_endpoint",
    vpc_id=vpc.id,
    service_name=pulumi.Output.concat("com.amazonaws.", region, ".ecr.dkr"),
    vpc_endpoint_type="Interface",
    subnet_ids=[private_subnet_1.id, private_subnet_2.id],
    security_group_ids=[vpc_endpoints_security_group.id],
    private_dns_enabled=True,
    tags=tags,
)

cluster = aws.ecs.Cluster(
    "ecs_cluster",
    name="fund-application",
    settings=[aws.ecs.ClusterSettingArgs(name="containerInsights", value="enabled")],
    tags=tags,
)

# Service Discovery Namespace for inter-service communication
service_discovery_namespace = aws.servicediscovery.PrivateDnsNamespace(
    "service_discovery",
    name="fund.local",
    vpc=vpc.id,
    description="Service discovery for fund services",
    tags=tags,
)

alb = aws.lb.LoadBalancer(
    "alb",
    name="fund-alb",
    subnets=[public_subnet_1.id, public_subnet_2.id],
    security_groups=[alb_security_group.id],
    internal=False,
    load_balancer_type="application",
    tags=tags,
)

datamanager_tg = aws.lb.TargetGroup(
    "datamanager_tg",
    name="fund-datamanager",
    port=8080,
    protocol="HTTP",
    vpc_id=vpc.id,
    target_type="ip",
    health_check=aws.lb.TargetGroupHealthCheckArgs(
        path="/health",
        healthy_threshold=2,
        unhealthy_threshold=3,
        timeout=5,
        interval=30,
    ),
    tags=tags,
)

portfoliomanager_tg = aws.lb.TargetGroup(
    "portfoliomanager_tg",
    name="fund-portfoliomanager",
    port=8080,
    protocol="HTTP",
    vpc_id=vpc.id,
    target_type="ip",
    health_check=aws.lb.TargetGroupHealthCheckArgs(
        path="/health",
        healthy_threshold=2,
        unhealthy_threshold=3,
        timeout=5,
        interval=30,
    ),
    tags=tags,
)

equitypricemodel_tg = aws.lb.TargetGroup(
    "equitypricemodel_tg",
    name="fund-equitypricemodel",
    port=8080,
    protocol="HTTP",
    vpc_id=vpc.id,
    target_type="ip",
    health_check=aws.lb.TargetGroupHealthCheckArgs(
        path="/health",
        healthy_threshold=2,
        unhealthy_threshold=3,
        timeout=5,
        interval=30,
    ),
    tags=tags,
)

acm_certificate_arn = None  # temporary disable HTTPS

if acm_certificate_arn:
    # HTTPS Listener (port 443)
    https_listener = aws.lb.Listener(
        "https_listener",
        load_balancer_arn=alb.arn,
        port=443,
        protocol="HTTPS",
        ssl_policy="ELBSecurityPolicy-TLS13-1-2-2021-06",
        certificate_arn=acm_certificate_arn,
        default_actions=[
            aws.lb.ListenerDefaultActionArgs(
                type="fixed-response",
                fixed_response=aws.lb.ListenerDefaultActionFixedResponseArgs(
                    content_type="text/plain",
                    message_body="Not Found",
                    status_code="404",
                ),
            )
        ],
        tags=tags,
    )

    # HTTP Listener (port 80) - Redirect to HTTPS
    http_listener = aws.lb.Listener(
        "http_listener",
        load_balancer_arn=alb.arn,
        port=80,
        protocol="HTTP",
        default_actions=[
            aws.lb.ListenerDefaultActionArgs(
                type="redirect",
                redirect=aws.lb.ListenerDefaultActionRedirectArgs(
                    protocol="HTTPS",
                    port="443",
                    status_code="HTTP_301",
                ),
            )
        ],
        tags=tags,
    )

    alb_listener = https_listener

else:
    # HTTP-only Listener (port 80)
    alb_listener = aws.lb.Listener(
        "http_listener",
        load_balancer_arn=alb.arn,
        port=80,
        protocol="HTTP",
        default_actions=[
            aws.lb.ListenerDefaultActionArgs(
                type="fixed-response",
                fixed_response=aws.lb.ListenerDefaultActionFixedResponseArgs(
                    content_type="text/plain",
                    message_body="Not Found",
                    status_code="404",
                ),
            )
        ],
        tags=tags,
    )

# Listener Rules for routing attached to primary listener
aws.lb.ListenerRule(
    "portfoliomanager_rule",
    listener_arn=alb_listener.arn,
    priority=200,  # Ensures that the more specific data manager paths take precedence
    actions=[
        aws.lb.ListenerRuleActionArgs(
            type="forward",
            target_group_arn=portfoliomanager_tg.arn,
        )
    ],
    conditions=[
        aws.lb.ListenerRuleConditionArgs(
            path_pattern=aws.lb.ListenerRuleConditionPathPatternArgs(
                values=["/portfolio*"]
            )
        )
    ],
    tags=tags,
)

aws.lb.ListenerRule(
    "datamanager_rule",
    listener_arn=alb_listener.arn,
    priority=100,
    actions=[
        aws.lb.ListenerRuleActionArgs(
            type="forward",
            target_group_arn=datamanager_tg.arn,
        )
    ],
    conditions=[
        aws.lb.ListenerRuleConditionArgs(
            path_pattern=aws.lb.ListenerRuleConditionPathPatternArgs(
                values=[
                    "/predictions*",
                    "/portfolios*",
                    "/equity-bars*",
                    "/equity-details*",
                ]
            )
        )
    ],
    tags=tags,
)

aws.lb.ListenerRule(
    "equitypricemodel_rule",
    listener_arn=alb_listener.arn,
    priority=150,
    actions=[
        aws.lb.ListenerRuleActionArgs(
            type="forward",
            target_group_arn=equitypricemodel_tg.arn,
        )
    ],
    conditions=[
        aws.lb.ListenerRuleConditionArgs(
            path_pattern=aws.lb.ListenerRuleConditionPathPatternArgs(
                values=["/model/*"]
            )
        )
    ],
    tags=tags,
)

github_actions_oidc_provider = aws.iam.OpenIdConnectProvider(
    "github_actions_oidc_provider",
    url="https://token.actions.githubusercontent.com",
    client_id_lists=["sts.amazonaws.com"],
    tags=tags,
)

github_actions_infrastructure_policy = aws.iam.Policy(
    "github_actions_infrastructure_policy",
    name="fund-github-actions-infrastructure-policy",
    description=(
        "Least-privilege policy for GitHub Actions infrastructure deployments."
    ),
    policy=pulumi.Output.all(
        datamanager_secret_name,
        portfoliomanager_secret_name,
        shared_secret_name,
        github_oidc_provider_arn,
    ).apply(
        lambda args: json.dumps(
            {
                "Version": "2012-10-17",
                "Statement": [
                    # These list/describe APIs are account-scoped and require wildcard
                    # resources.
                    {
                        "Sid": "ReadGlobalMetadata",
                        "Effect": "Allow",
                        "Action": [
                            "sts:GetCallerIdentity",
                            "tag:GetResources",
                            "tag:GetTagKeys",
                            "tag:GetTagValues",
                            "iam:Get*",
                            "iam:List*",
                            "ec2:Describe*",
                            "ecs:Describe*",
                            "ecs:List*",
                            "elasticloadbalancing:Describe*",
                            "ecr:Describe*",
                            "ecr:ListTagsForResource",
                            "s3:GetBucketLocation",
                            "s3:ListAllMyBuckets",
                            "ssm:DescribeParameters",
                            "secretsmanager:ListSecrets",
                            "logs:Describe*",
                            "cloudwatch:Describe*",
                            "cloudwatch:Get*",
                            "sns:Get*",
                            "sns:List*",
                            "budgets:Describe*",
                            "budgets:ViewBudget",
                            "servicediscovery:Get*",
                            "servicediscovery:List*",
                        ],
                        "Resource": "*",
                    },
                    # These control-plane APIs rely on generated identifiers and do not
                    # support practical resource-level scoping for stack create/update/
                    # delete operations.
                    {
                        "Sid": "ManageEC2ECSELBBudgetsAndServiceDiscovery",
                        "Effect": "Allow",
                        "Action": [
                            "ec2:*",
                            "ecs:*",
                            "elasticloadbalancing:*",
                            "budgets:*",
                            "servicediscovery:*",
                        ],
                        "Resource": "*",
                    },
                    # CreateRepository/GetAuthorizationToken require wildcard resources.
                    {
                        "Sid": "CreateAndAuthenticateECRRepositories",
                        "Effect": "Allow",
                        "Action": [
                            "ecr:CreateRepository",
                            "ecr:GetAuthorizationToken",
                        ],
                        "Resource": "*",
                    },
                    {
                        "Sid": "ManageECRRepositories",
                        "Effect": "Allow",
                        "Action": "ecr:*",
                        "Resource": (
                            f"arn:aws:ecr:{region}:{account_id}:repository/fund/*"
                        ),
                    },
                    # CreateBucket requires wildcard resources.
                    {
                        "Sid": "CreateBuckets",
                        "Effect": "Allow",
                        "Action": "s3:CreateBucket",
                        "Resource": "*",
                    },
                    {
                        "Sid": "ManageBuckets",
                        "Effect": "Allow",
                        "Action": "s3:*",
                        "Resource": [
                            "arn:aws:s3:::fund-data-*",
                            "arn:aws:s3:::fund-data-*/*",
                            "arn:aws:s3:::fund-model-artifacts-*",
                            "arn:aws:s3:::fund-model-artifacts-*/*",
                        ],
                    },
                    # CreateSecret requires wildcard resources before an ARN exists.
                    {
                        "Sid": "CreateSecrets",
                        "Effect": "Allow",
                        "Action": "secretsmanager:CreateSecret",
                        "Resource": "*",
                    },
                    {
                        "Sid": "ManageConfiguredSecrets",
                        "Effect": "Allow",
                        "Action": "secretsmanager:*",
                        "Resource": [
                            f"arn:aws:secretsmanager:{region}:{account_id}:secret:{args[0]}*",
                            f"arn:aws:secretsmanager:{region}:{account_id}:secret:{args[1]}*",
                            f"arn:aws:secretsmanager:{region}:{account_id}:secret:{args[2]}*",
                        ],
                    },
                    {
                        "Sid": "ManageParameters",
                        "Effect": "Allow",
                        "Action": "ssm:*",
                        "Resource": (
                            f"arn:aws:ssm:{region}:{account_id}:parameter/fund/*"
                        ),
                    },
                    {
                        "Sid": "ManageLogGroups",
                        "Effect": "Allow",
                        "Action": "logs:*",
                        "Resource": [
                            f"arn:aws:logs:{region}:{account_id}:log-group:/ecs/fund/*",
                            f"arn:aws:logs:{region}:{account_id}:log-group:/ecs/fund/*:*",
                        ],
                    },
                    # Alarm mutation APIs require wildcard resources.
                    {
                        "Sid": "ManageAlarms",
                        "Effect": "Allow",
                        "Action": [
                            "cloudwatch:DeleteAlarms",
                            "cloudwatch:ListTagsForResource",
                            "cloudwatch:PutMetricAlarm",
                            "cloudwatch:TagResource",
                            "cloudwatch:UntagResource",
                        ],
                        "Resource": "*",
                    },
                    # CreateTopic requires wildcard resources.
                    {
                        "Sid": "CreateInfrastructureAlertsTopic",
                        "Effect": "Allow",
                        "Action": "sns:CreateTopic",
                        "Resource": "*",
                    },
                    {
                        "Sid": "ManageInfrastructureAlertsTopic",
                        "Effect": "Allow",
                        "Action": "sns:*",
                        "Resource": [
                            f"arn:aws:sns:{region}:{account_id}:fund-infrastructure-alerts",
                            f"arn:aws:sns:{region}:{account_id}:fund-infrastructure-alerts:*",
                        ],
                    },
                    {
                        "Sid": "CreateGithubActionsOIDCProvider",
                        "Effect": "Allow",
                        "Action": "iam:CreateOpenIDConnectProvider",
                        "Resource": args[3],
                    },
                    # CreateRole uses wildcard resources by API design.
                    {
                        "Sid": "CreateRoles",
                        "Effect": "Allow",
                        "Action": "iam:CreateRole",
                        "Resource": "*",
                        "Condition": {
                            "StringEquals": {
                                "iam:RoleName": [
                                    "fund-ecs-execution-role",
                                    "fund-ecs-task-role",
                                    github_actions_role_name,
                                    sagemaker_execution_role_name,
                                ]
                            }
                        },
                    },
                    # CreatePolicy uses wildcard resources by API design.
                    {
                        "Sid": "CreatePolicies",
                        "Effect": "Allow",
                        "Action": "iam:CreatePolicy",
                        "Resource": "*",
                        "Condition": {
                            "StringLike": {
                                "iam:PolicyName": "fund-*",
                            }
                        },
                    },
                    # CreateServiceLinkedRole uses wildcard resources by API design.
                    {
                        "Sid": "CreateServiceLinkedRolesForStack",
                        "Effect": "Allow",
                        "Action": "iam:CreateServiceLinkedRole",
                        "Resource": "*",
                        "Condition": {
                            "StringEquals": {
                                "iam:AWSServiceName": [
                                    "ecs.amazonaws.com",
                                    "elasticloadbalancing.amazonaws.com",
                                ]
                            }
                        },
                    },
                    {
                        "Sid": "ManageRoles",
                        "Effect": "Allow",
                        "Action": [
                            "iam:AttachRolePolicy",
                            "iam:DeleteRole",
                            "iam:DetachRolePolicy",
                            "iam:PassRole",
                            "iam:TagRole",
                            "iam:UntagRole",
                            "iam:UpdateAssumeRolePolicy",
                        ],
                        "Resource": [
                            f"arn:aws:iam::{account_id}:role/fund-ecs-execution-role",
                            f"arn:aws:iam::{account_id}:role/fund-ecs-task-role",
                            f"arn:aws:iam::{account_id}:role/{github_actions_role_name}",
                            f"arn:aws:iam::{account_id}:role/{sagemaker_execution_role_name}",
                        ],
                        "Condition": {
                            "ArnLikeIfExists": {
                                "iam:PolicyARN": [
                                    "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy",
                                    f"arn:aws:iam::{account_id}:policy/fund-*",
                                ]
                            },
                            "StringLikeIfExists": {
                                "iam:PassedToService": [
                                    "ecs-tasks.amazonaws.com",
                                    "ecs.amazonaws.com",
                                    "sagemaker.amazonaws.com",
                                ]
                            },
                        },
                    },
                    {
                        "Sid": "ManageInlineRolePolicies",
                        "Effect": "Allow",
                        "Action": [
                            "iam:DeleteRolePolicy",
                            "iam:PutRolePolicy",
                        ],
                        "Resource": [
                            f"arn:aws:iam::{account_id}:role/fund-ecs-execution-role",
                            f"arn:aws:iam::{account_id}:role/fund-ecs-task-role",
                        ],
                        "Condition": {
                            "StringEquals": {
                                "iam:PolicyName": [
                                    "fund-ecs-execution-role-secrets-policy",
                                    "fund-ecs-task-role-s3-policy",
                                    "fund-ecs-task-role-ssm-policy",
                                ]
                            }
                        },
                    },
                    {
                        "Sid": "ManagePolicies",
                        "Effect": "Allow",
                        "Action": [
                            "iam:CreatePolicyVersion",
                            "iam:DeletePolicy",
                            "iam:DeletePolicyVersion",
                            "iam:SetDefaultPolicyVersion",
                            "iam:TagPolicy",
                            "iam:UntagPolicy",
                        ],
                        "Resource": f"arn:aws:iam::{account_id}:policy/fund-*",
                    },
                    {
                        "Sid": "ManageGithubActionsOIDCProvider",
                        "Effect": "Allow",
                        "Action": [
                            "iam:AddClientIDToOpenIDConnectProvider",
                            "iam:DeleteOpenIDConnectProvider",
                            "iam:RemoveClientIDFromOpenIDConnectProvider",
                            "iam:TagOpenIDConnectProvider",
                            "iam:UntagOpenIDConnectProvider",
                            "iam:UpdateOpenIDConnectProviderThumbprint",
                        ],
                        "Resource": args[3],
                    },
                    # Service-linked role teardown APIs are wildcard-resource only.
                    {
                        "Sid": "DeleteServiceLinkedRoles",
                        "Effect": "Allow",
                        "Action": [
                            "iam:DeleteServiceLinkedRole",
                            "iam:GetServiceLinkedRoleDeletionStatus",
                        ],
                        "Resource": "*",
                        "Condition": {
                            "StringLikeIfExists": {
                                "iam:AWSServiceName": [
                                    "ecs.amazonaws.com",
                                    "elasticloadbalancing.amazonaws.com",
                                ]
                            }
                        },
                    },
                ],
            },
            sort_keys=True,
        )
    ),
    opts=pulumi.ResourceOptions(retain_on_delete=True),
    tags=tags,
)

github_actions_infrastructure_role = aws.iam.Role(
    "github_actions_infrastructure_role",
    name=github_actions_role_name,
    assume_role_policy=github_actions_oidc_provider.arn.apply(
        lambda github_actions_oidc_provider_arn: json.dumps(
            {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Principal": {
                            "Federated": github_actions_oidc_provider_arn,
                        },
                        "Action": "sts:AssumeRoleWithWebIdentity",
                        "Condition": {
                            "StringEquals": {
                                github_oidc_audience_claim: "sts.amazonaws.com",
                                github_oidc_repository_claim: github_repository,
                                github_oidc_ref_claim: f"refs/heads/{github_branch}",
                                github_oidc_workflow_ref_claim: github_workflow_refs,
                            },
                            "StringLike": {
                                github_oidc_sub_claim: f"repo:{github_repository}:*",
                            },
                        },
                    }
                ],
            },
            sort_keys=True,
        )
    ),
    managed_policy_arns=[github_actions_infrastructure_policy.arn],
    opts=pulumi.ResourceOptions(retain_on_delete=True),
    tags=tags,
)

# IAM Role for ECS to perform infrastructure tasks
execution_role = aws.iam.Role(
    "execution_role",
    name="fund-ecs-execution-role",
    assume_role_policy=json.dumps(
        {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Action": "sts:AssumeRole",
                    "Effect": "Allow",
                    "Principal": {"Service": "ecs-tasks.amazonaws.com"},
                }
            ],
        },
        sort_keys=True,
    ),
    tags=tags,
)

aws.iam.RolePolicyAttachment(
    "execution_role_policy",
    role=execution_role.name,
    policy_arn="arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy",
)

# Allow ECS tasks to read secrets from Secrets Manager
aws.iam.RolePolicy(
    "execution_role_secrets_policy",
    name="fund-ecs-execution-role-secrets-policy",
    role=execution_role.id,
    policy=pulumi.Output.all(
        datamanager_secret.arn,
        portfoliomanager_secret.arn,
        shared_secret.arn,
    ).apply(
        lambda args: json.dumps(
            {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Action": ["secretsmanager:GetSecretValue"],
                        "Resource": [args[0], args[1], args[2]],
                    }
                ],
            },
            sort_keys=True,
        )
    ),
)


# IAM Role for ECS tasks to access AWS resources
task_role = aws.iam.Role(
    "task_role",
    name="fund-ecs-task-role",
    assume_role_policy=json.dumps(
        {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Action": "sts:AssumeRole",
                    "Effect": "Allow",
                    "Principal": {"Service": "ecs-tasks.amazonaws.com"},
                }
            ],
        },
        sort_keys=True,
    ),
    tags=tags,
)

aws.iam.RolePolicy(
    "task_role_s3_policy",
    name="fund-ecs-task-role-s3-policy",
    role=task_role.id,
    policy=pulumi.Output.all(data_bucket.arn, model_artifacts_bucket.arn).apply(
        lambda args: json.dumps(
            {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Action": ["s3:GetObject", "s3:PutObject", "s3:ListBucket"],
                        "Resource": [
                            args[0],
                            f"{args[0]}/*",
                            args[1],
                            f"{args[1]}/*",
                        ],
                    }
                ],
            },
            sort_keys=True,
        )
    ),
)

aws.iam.RolePolicy(
    "task_role_ssm_policy",
    name="fund-ecs-task-role-ssm-policy",
    role=task_role.id,
    policy=json.dumps(
        {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Action": ["ssm:GetParameter", "ssm:GetParameters"],
                    "Resource": f"arn:aws:ssm:{region}:{account_id}:parameter/fund/*",
                }
            ],
        },
        sort_keys=True,
    ),
)

# SageMaker Execution Policy
sagemaker_execution_policy = aws.iam.Policy(
    "sagemaker_execution_policy",
    name="fund-sagemaker-execution-policy",
    description="Least-privilege policy for SageMaker execution role.",
    policy=pulumi.Output.all(
        data_bucket.arn,
        model_artifacts_bucket.arn,
        equitypricemodel_trainer_repository.arn,
    ).apply(
        lambda args: json.dumps(
            {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Sid": "S3Access",
                        "Effect": "Allow",
                        "Action": [
                            "s3:GetObject",
                            "s3:PutObject",
                            "s3:DeleteObject",
                            "s3:ListBucket",
                        ],
                        "Resource": [
                            args[0],
                            f"{args[0]}/*",
                            args[1],
                            f"{args[1]}/*",
                        ],
                    },
                    {
                        "Sid": "ECRRepositoryAccess",
                        "Effect": "Allow",
                        "Action": [
                            "ecr:GetDownloadUrlForLayer",
                            "ecr:BatchGetImage",
                            "ecr:BatchCheckLayerAvailability",
                        ],
                        "Resource": args[2],
                    },
                    {
                        "Sid": "ECRAuthorizationToken",
                        "Effect": "Allow",
                        "Action": "ecr:GetAuthorizationToken",
                        "Resource": "*",
                    },
                    {
                        "Sid": "CloudWatchLogs",
                        "Effect": "Allow",
                        "Action": [
                            "logs:CreateLogGroup",
                            "logs:CreateLogStream",
                            "logs:PutLogEvents",
                            "logs:DescribeLogStreams",
                        ],
                        "Resource": "arn:aws:logs:*:*:log-group:/aws/sagemaker/*",
                    },
                ],
            },
            sort_keys=True,
        )
    ),
    opts=pulumi.ResourceOptions(retain_on_delete=True),
    tags=tags,
)

# SageMaker Execution Role for training jobs
sagemaker_execution_role = aws.iam.Role(
    "sagemaker_execution_role",
    name=sagemaker_execution_role_name,
    assume_role_policy=json.dumps(
        {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Action": "sts:AssumeRole",
                    "Effect": "Allow",
                    "Principal": {"Service": "sagemaker.amazonaws.com"},
                }
            ],
        },
        sort_keys=True,
    ),
    managed_policy_arns=[sagemaker_execution_policy.arn],
    opts=pulumi.ResourceOptions(retain_on_delete=True),
    tags=tags,
)

datamanager_log_group = aws.cloudwatch.LogGroup(
    "datamanager_logs",
    name="/ecs/fund/datamanager",
    retention_in_days=7,
    tags=tags,
)

portfoliomanager_log_group = aws.cloudwatch.LogGroup(
    "portfoliomanager_logs",
    name="/ecs/fund/portfoliomanager",
    retention_in_days=7,
    tags=tags,
)

equitypricemodel_log_group = aws.cloudwatch.LogGroup(
    "equitypricemodel_logs",
    name="/ecs/fund/equitypricemodel",
    retention_in_days=7,
    tags=tags,
)

datamanager_task_definition = aws.ecs.TaskDefinition(
    "datamanager_task",
    family="datamanager",
    cpu="256",
    memory="512",
    network_mode="awsvpc",
    requires_compatibilities=["FARGATE"],
    execution_role_arn=execution_role.arn,
    task_role_arn=task_role.arn,
    container_definitions=pulumi.Output.all(
        datamanager_log_group.name,
        datamanager_image_uri,
        datamanager_secret.arn,
        shared_secret.arn,
        data_bucket.bucket,
    ).apply(
        lambda args: json.dumps(
            [
                {
                    "name": "datamanager",
                    "image": args[1],
                    "portMappings": [{"containerPort": 8080, "protocol": "tcp"}],
                    "environment": [
                        {
                            "name": "MASSIVE_BASE_URL",
                            "value": "https://api.massive.io",
                        },
                        {
                            "name": "AWS_S3_DATA_BUCKET_NAME",
                            "value": args[4],
                        },
                        {
                            "name": "ENVIRONMENT",
                            "value": "production",
                        },
                    ],
                    "secrets": [
                        {
                            "name": "MASSIVE_API_KEY",
                            "valueFrom": f"{args[2]}:MASSIVE_API_KEY::",
                        },
                        {
                            "name": "SENTRY_DSN",
                            "valueFrom": f"{args[3]}:SENTRY_DSN::",
                        },
                    ],
                    "logConfiguration": {
                        "logDriver": "awslogs",
                        "options": {
                            "awslogs-group": args[0],
                            "awslogs-region": region,
                            "awslogs-stream-prefix": "datamanager",
                        },
                    },
                    "essential": True,
                }
            ],
            sort_keys=True,
        )
    ),
    tags=tags,
)

portfoliomanager_task_definition = aws.ecs.TaskDefinition(
    "portfoliomanager_task",
    family="portfoliomanager",
    cpu="256",
    memory="512",
    network_mode="awsvpc",
    requires_compatibilities=["FARGATE"],
    execution_role_arn=execution_role.arn,
    task_role_arn=task_role.arn,
    container_definitions=pulumi.Output.all(
        portfoliomanager_log_group.name,
        service_discovery_namespace.name,
        portfoliomanager_image_uri,
        portfoliomanager_secret.arn,
        shared_secret.arn,
        parameters.uncertainty_threshold.value,
    ).apply(
        lambda args: json.dumps(
            [
                {
                    "name": "portfoliomanager",
                    "image": args[2],
                    "portMappings": [{"containerPort": 8080, "protocol": "tcp"}],
                    "environment": [
                        {
                            "name": "FUND_DATAMANAGER_BASE_URL",
                            "value": f"http://datamanager.{args[1]}:8080",
                        },
                        {
                            "name": "FUND_EQUITYPRICEMODEL_BASE_URL",
                            "value": f"http://equitypricemodel.{args[1]}:8080",
                        },
                        {
                            "name": "ENVIRONMENT",
                            "value": "production",
                        },
                        {
                            "name": "FUND_UNCERTAINTY_THRESHOLD",
                            "value": args[5],
                        },
                    ],
                    "secrets": [
                        {
                            "name": "ALPACA_API_KEY_ID",
                            "valueFrom": f"{args[3]}:ALPACA_API_KEY_ID::",
                        },
                        {
                            "name": "ALPACA_API_SECRET",
                            "valueFrom": f"{args[3]}:ALPACA_API_SECRET::",
                        },
                        {
                            "name": "ALPACA_IS_PAPER",
                            "valueFrom": f"{args[3]}:ALPACA_IS_PAPER::",
                        },
                        {
                            "name": "SENTRY_DSN",
                            "valueFrom": f"{args[4]}:SENTRY_DSN::",
                        },
                    ],
                    "logConfiguration": {
                        "logDriver": "awslogs",
                        "options": {
                            "awslogs-group": args[0],
                            "awslogs-region": region,
                            "awslogs-stream-prefix": "portfoliomanager",
                        },
                    },
                    "essential": True,
                }
            ],
            sort_keys=True,
        )
    ),
    tags=tags,
)

equitypricemodel_task_definition = aws.ecs.TaskDefinition(
    "equitypricemodel_task",
    family="equitypricemodel",
    cpu="256",
    memory="512",
    network_mode="awsvpc",
    requires_compatibilities=["FARGATE"],
    execution_role_arn=execution_role.arn,
    task_role_arn=task_role.arn,
    container_definitions=pulumi.Output.all(
        equitypricemodel_log_group.name,
        service_discovery_namespace.name,
        equitypricemodel_image_uri,
        model_artifacts_bucket.bucket,
        shared_secret.arn,
    ).apply(
        lambda args: json.dumps(
            [
                {
                    "name": "equitypricemodel",
                    "image": args[2],
                    "portMappings": [{"containerPort": 8080, "protocol": "tcp"}],
                    "environment": [
                        {
                            "name": "FUND_DATAMANAGER_BASE_URL",
                            "value": f"http://datamanager.{args[1]}:8080",
                        },
                        {
                            "name": "AWS_S3_MODEL_ARTIFACTS_BUCKET_NAME",
                            "value": args[3],
                        },
                        {
                            "name": "ENVIRONMENT",
                            "value": "production",
                        },
                        {
                            "name": "DISABLE_DISK_CACHE",
                            "value": "1",
                        },
                    ],
                    "secrets": [
                        {
                            "name": "SENTRY_DSN",
                            "valueFrom": f"{args[4]}:SENTRY_DSN::",
                        },
                    ],
                    "logConfiguration": {
                        "logDriver": "awslogs",
                        "options": {
                            "awslogs-group": args[0],
                            "awslogs-region": region,
                            "awslogs-stream-prefix": "equitypricemodel",
                        },
                    },
                    "essential": True,
                }
            ],
            sort_keys=True,
        )
    ),
    tags=tags,
)

datamanager_sd_service = aws.servicediscovery.Service(
    "datamanager_sd",
    name="datamanager",
    dns_config=aws.servicediscovery.ServiceDnsConfigArgs(
        namespace_id=service_discovery_namespace.id,
        dns_records=[
            aws.servicediscovery.ServiceDnsConfigDnsRecordArgs(ttl=10, type="A")
        ],
    ),
    tags=tags,
)

portfoliomanager_sd_service = aws.servicediscovery.Service(
    "portfoliomanager_sd",
    name="portfoliomanager",
    dns_config=aws.servicediscovery.ServiceDnsConfigArgs(
        namespace_id=service_discovery_namespace.id,
        dns_records=[
            aws.servicediscovery.ServiceDnsConfigDnsRecordArgs(ttl=10, type="A")
        ],
    ),
    tags=tags,
)

equitypricemodel_sd_service = aws.servicediscovery.Service(
    "equitypricemodel_sd",
    name="equitypricemodel",
    dns_config=aws.servicediscovery.ServiceDnsConfigArgs(
        namespace_id=service_discovery_namespace.id,
        dns_records=[
            aws.servicediscovery.ServiceDnsConfigDnsRecordArgs(ttl=10, type="A")
        ],
    ),
    tags=tags,
)

datamanager_service = aws.ecs.Service(
    "datamanager_service",
    name="fund-datamanager",
    cluster=cluster.arn,
    task_definition=datamanager_task_definition.arn,
    desired_count=1,
    launch_type="FARGATE",
    network_configuration=aws.ecs.ServiceNetworkConfigurationArgs(
        subnets=[private_subnet_1.id, private_subnet_2.id],
        security_groups=[ecs_security_group.id],
        assign_public_ip=False,
    ),
    load_balancers=[
        aws.ecs.ServiceLoadBalancerArgs(
            target_group_arn=datamanager_tg.arn,
            container_name="datamanager",
            container_port=8080,
        )
    ],
    service_registries=aws.ecs.ServiceServiceRegistriesArgs(
        registry_arn=datamanager_sd_service.arn
    ),
    opts=pulumi.ResourceOptions(depends_on=[alb_listener]),
    tags=tags,
)

portfoliomanager_service = aws.ecs.Service(
    "portfoliomanager_service",
    name="fund-portfoliomanager",
    cluster=cluster.arn,
    task_definition=portfoliomanager_task_definition.arn,
    desired_count=1,
    launch_type="FARGATE",
    network_configuration=aws.ecs.ServiceNetworkConfigurationArgs(
        subnets=[private_subnet_1.id, private_subnet_2.id],
        security_groups=[ecs_security_group.id],
        assign_public_ip=False,
    ),
    load_balancers=[
        aws.ecs.ServiceLoadBalancerArgs(
            target_group_arn=portfoliomanager_tg.arn,
            container_name="portfoliomanager",
            container_port=8080,
        )
    ],
    service_registries=aws.ecs.ServiceServiceRegistriesArgs(
        registry_arn=portfoliomanager_sd_service.arn
    ),
    opts=pulumi.ResourceOptions(depends_on=[alb_listener, datamanager_service]),
    tags=tags,
)

equitypricemodel_service = aws.ecs.Service(
    "equitypricemodel_service",
    name="fund-equitypricemodel",
    cluster=cluster.arn,
    task_definition=equitypricemodel_task_definition.arn,
    desired_count=1,
    launch_type="FARGATE",
    network_configuration=aws.ecs.ServiceNetworkConfigurationArgs(
        subnets=[private_subnet_1.id, private_subnet_2.id],
        security_groups=[ecs_security_group.id],
        assign_public_ip=False,
    ),
    load_balancers=[
        aws.ecs.ServiceLoadBalancerArgs(
            target_group_arn=equitypricemodel_tg.arn,
            container_name="equitypricemodel",
            container_port=8080,
        )
    ],
    service_registries=aws.ecs.ServiceServiceRegistriesArgs(
        registry_arn=equitypricemodel_sd_service.arn
    ),
    opts=pulumi.ResourceOptions(depends_on=[alb_listener, datamanager_service]),
    tags=tags,
)

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
pulumi.export("aws_ecs_cluster_name", cluster.name)
pulumi.export("aws_alb_dns_name", alb.dns_name)
pulumi.export("aws_alb_url", pulumi.Output.concat(protocol, alb.dns_name))
pulumi.export("aws_service_discovery_namespace", service_discovery_namespace.name)
pulumi.export("aws_ecr_datamanager_image", datamanager_image_uri)
pulumi.export("aws_ecr_portfoliomanager_image", portfoliomanager_image_uri)
pulumi.export("aws_ecr_equitypricemodel_image", equitypricemodel_image_uri)
pulumi.export("aws_ecr_datamanager_repository", datamanager_repository.repository_url)
pulumi.export(
    "aws_ecr_portfoliomanager_repository", portfoliomanager_repository.repository_url
)
pulumi.export(
    "aws_ecr_equitypricemodel_repository", equitypricemodel_repository.repository_url
)
pulumi.export("aws_s3_data_bucket_name", data_bucket.bucket)
pulumi.export("aws_s3_model_artifacts_bucket_name", model_artifacts_bucket.bucket)
pulumi.export(
    "aws_ecr_equitypricemodel_trainer_repository",
    equitypricemodel_trainer_repository.repository_url,
)
pulumi.export(
    "aws_ecr_equitypricemodel_trainer_image", equitypricemodel_trainer_image_uri
)
pulumi.export("aws_iam_sagemaker_role_arn", sagemaker_execution_role.arn)
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
