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

random_suffix = stack_config.require_secret("randomSuffix")

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

prefect_allowed_cidrs = cast(
    "list[str]",
    stack_config.require_object("prefectAllowedCidrs"),
)
if not prefect_allowed_cidrs:
    message = (
        "Pulumi config 'prefectAllowedCidrs' must include at least one CIDR block."
    )
    raise ValueError(message)

prefect_allowed_ipv4_cidrs = [c for c in prefect_allowed_cidrs if ":" not in c]
prefect_allowed_ipv6_cidrs = [c for c in prefect_allowed_cidrs if ":" in c]

training_notification_sender_email = stack_config.require_secret(
    "trainingNotificationSenderEmail"
)
training_notification_recipient_emails = stack_config.require_secret(
    "trainingNotificationRecipientEmails"
)

data_manager_secret_name = stack_config.require_secret("datamanagerSecretName")
portfolio_manager_secret_name = stack_config.require_secret(
    "portfoliomanagerSecretName"
)
shared_secret_name = stack_config.require_secret("sharedSecretName")

data_manager_secret_values = require_secret_config_object(
    stack_config,
    "datamanagerSecretValue",
)
portfolio_manager_secret_values = require_secret_config_object(
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

data_manager_secret = aws.secretsmanager.Secret(
    "data_manager_secret",
    name=data_manager_secret_name,
    recovery_window_in_days=0,
    tags=tags,
)

portfolio_manager_secret = aws.secretsmanager.Secret(
    "portfolio_manager_secret",
    name=portfolio_manager_secret_name,
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
    "data_manager_secret_version",
    secret_id=data_manager_secret.id,
    secret_string=data_manager_secret_values.apply(
        lambda values: serialize_secret_config_object(
            values,
            "datamanagerSecretValue",
            {"MASSIVE_API_KEY"},
        )
    ),
)

aws.secretsmanager.SecretVersion(
    "portfolio_manager_secret_version",
    secret_id=portfolio_manager_secret.id,
    secret_string=portfolio_manager_secret_values.apply(
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
# alias: migrated from aws:s3/bucket:Bucket to aws:s3/bucketV2:BucketV2
data_bucket = aws.s3.BucketV2(
    "data_bucket",
    bucket=pulumi.Output.concat("fund-data-", random_suffix),
    opts=pulumi.ResourceOptions(
        retain_on_delete=True,
        aliases=[pulumi.Alias(type_="aws:s3/bucket:Bucket")],
    ),
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
    opts=pulumi.ResourceOptions(retain_on_delete=True),
)

aws.s3.BucketPublicAccessBlock(
    "data_bucket_public_access_block",
    bucket=data_bucket.id,
    block_public_acls=True,
    block_public_policy=True,
    ignore_public_acls=True,
    restrict_public_buckets=True,
    opts=pulumi.ResourceOptions(retain_on_delete=True),
)

aws.s3.BucketVersioning(
    "data_bucket_versioning",
    bucket=data_bucket.id,
    versioning_configuration=aws.s3.BucketVersioningVersioningConfigurationArgs(
        status="Enabled",
    ),
    opts=pulumi.ResourceOptions(retain_on_delete=True),
)

# S3 Model Artifacts Bucket for storing trained model weights and checkpoints
# alias: migrated from aws:s3/bucket:Bucket to aws:s3/bucketV2:BucketV2
model_artifacts_bucket = aws.s3.BucketV2(
    "model_artifacts_bucket",
    bucket=pulumi.Output.concat("fund-model-artifacts-", random_suffix),
    opts=pulumi.ResourceOptions(
        retain_on_delete=True,
        aliases=[pulumi.Alias(type_="aws:s3/bucket:Bucket")],
    ),
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
    opts=pulumi.ResourceOptions(retain_on_delete=True),
)

aws.s3.BucketPublicAccessBlock(
    "model_artifacts_bucket_public_access_block",
    bucket=model_artifacts_bucket.id,
    block_public_acls=True,
    block_public_policy=True,
    ignore_public_acls=True,
    restrict_public_buckets=True,
    opts=pulumi.ResourceOptions(retain_on_delete=True),
)

aws.s3.BucketVersioning(
    "model_artifacts_bucket_versioning",
    bucket=model_artifacts_bucket.id,
    versioning_configuration=aws.s3.BucketVersioningVersioningConfigurationArgs(
        status="Enabled",
    ),
    opts=pulumi.ResourceOptions(retain_on_delete=True),
)

# ECR Repositories - these must exist before images can be pushed
# force_delete allows repositories containing images to be deleted on stack teardown.
# If image rebuild and push times become prohibitive on daily down/up cycles, switch to
# retain_on_delete=True and add pulumi import statements to the maskfile up command.
data_manager_repository = aws.ecr.Repository(
    "data_manager_repository",
    name="fund/data-manager-server",
    image_tag_mutability="MUTABLE",
    force_delete=True,
    image_scanning_configuration=aws.ecr.RepositoryImageScanningConfigurationArgs(
        scan_on_push=True,
    ),
    tags=tags,
)

portfolio_manager_repository = aws.ecr.Repository(
    "portfolio_manager_repository",
    name="fund/portfolio-manager-server",
    image_tag_mutability="MUTABLE",
    force_delete=True,
    image_scanning_configuration=aws.ecr.RepositoryImageScanningConfigurationArgs(
        scan_on_push=True,
    ),
    tags=tags,
)

ensemble_manager_repository = aws.ecr.Repository(
    "ensemble_manager_repository",
    name="fund/ensemble-manager-server",
    image_tag_mutability="MUTABLE",
    force_delete=True,
    image_scanning_configuration=aws.ecr.RepositoryImageScanningConfigurationArgs(
        scan_on_push=True,
    ),
    tags=tags,
)

tide_trainer_repository = aws.ecr.Repository(
    "tide_trainer_repository",
    name="fund/tide-trainer",
    image_tag_mutability="MUTABLE",
    force_delete=True,
    image_scanning_configuration=aws.ecr.RepositoryImageScanningConfigurationArgs(
        scan_on_push=True,
    ),
    tags=tags,
)

training_server_repository = aws.ecr.Repository(
    "training_server_repository",
    name="fund/training-server",
    image_tag_mutability="MUTABLE",
    force_delete=True,
    image_scanning_configuration=aws.ecr.RepositoryImageScanningConfigurationArgs(
        scan_on_push=True,
    ),
    tags=tags,
)

training_worker_repository = aws.ecr.Repository(
    "training_worker_repository",
    name="fund/training-worker",
    image_tag_mutability="MUTABLE",
    force_delete=True,
    image_scanning_configuration=aws.ecr.RepositoryImageScanningConfigurationArgs(
        scan_on_push=True,
    ),
    tags=tags,
)

# Generate image URIs - these will be used in task definitions
# For initial deployment, use a placeholder that will be updated when images are pushed
data_manager_image_uri = data_manager_repository.repository_url.apply(
    lambda url: f"{url}:latest"
)
portfolio_manager_image_uri = portfolio_manager_repository.repository_url.apply(
    lambda url: f"{url}:latest"
)
ensemble_manager_image_uri = ensemble_manager_repository.repository_url.apply(
    lambda url: f"{url}:latest"
)
tide_trainer_image_uri = tide_trainer_repository.repository_url.apply(
    lambda url: f"{url}:latest"
)
training_server_image_uri = training_server_repository.repository_url.apply(
    lambda url: f"{url}:latest"
)
training_worker_image_uri = training_worker_repository.repository_url.apply(
    lambda url: f"{url}:latest"
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
        *(
            [
                aws.ec2.SecurityGroupIngressArgs(
                    protocol="tcp",
                    from_port=4200,
                    to_port=4200,
                    cidr_blocks=prefect_allowed_ipv4_cidrs,
                    description="Allow Prefect dashboard from team IPv4",
                ),
            ]
            if prefect_allowed_ipv4_cidrs
            else []
        ),
        *(
            [
                aws.ec2.SecurityGroupIngressArgs(
                    protocol="tcp",
                    from_port=4200,
                    to_port=4200,
                    ipv6_cidr_blocks=prefect_allowed_ipv6_cidrs,
                    description="Allow Prefect dashboard from team IPv6",
                ),
            ]
            if prefect_allowed_ipv6_cidrs
            else []
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

data_manager_tg = aws.lb.TargetGroup(
    "data_manager_tg",
    name="fund-data-manager",
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

portfolio_manager_tg = aws.lb.TargetGroup(
    "portfolio_manager_tg",
    name="fund-portfolio-manager",
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

ensemble_manager_tg = aws.lb.TargetGroup(
    "ensemble_manager_tg",
    name="fund-ensemble-manager",
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

training_tg = aws.lb.TargetGroup(
    "training_tg",
    name="fund-training",
    port=4200,
    protocol="HTTP",
    vpc_id=vpc.id,
    target_type="ip",
    health_check=aws.lb.TargetGroupHealthCheckArgs(
        path="/api/health",
        healthy_threshold=2,
        unhealthy_threshold=3,
        timeout=5,
        interval=30,
    ),
    tags=tags,
)

# Set acm_certificate_arn to enable HTTPS for the Prefect dashboard listener.
acm_certificate_arn = None

# Prefect dashboard listener on port 4200 (restricted by ALB security group)
if acm_certificate_arn:
    prefect_listener = aws.lb.Listener(
        "prefect_listener",
        load_balancer_arn=alb.arn,
        port=4200,
        protocol="HTTPS",
        ssl_policy="ELBSecurityPolicy-TLS13-1-2-2021-06",
        certificate_arn=acm_certificate_arn,
        default_actions=[
            aws.lb.ListenerDefaultActionArgs(
                type="forward",
                target_group_arn=training_tg.arn,
            )
        ],
        tags=tags,
    )
else:
    prefect_listener = aws.lb.Listener(
        "prefect_listener",
        load_balancer_arn=alb.arn,
        port=4200,
        protocol="HTTP",
        default_actions=[
            aws.lb.ListenerDefaultActionArgs(
                type="forward",
                target_group_arn=training_tg.arn,
            )
        ],
        tags=tags,
    )

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
    "portfolio_manager_rule",
    listener_arn=alb_listener.arn,
    priority=200,  # Ensures that the more specific data manager paths take precedence
    actions=[
        aws.lb.ListenerRuleActionArgs(
            type="forward",
            target_group_arn=portfolio_manager_tg.arn,
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
    "data_manager_rule",
    listener_arn=alb_listener.arn,
    priority=100,
    actions=[
        aws.lb.ListenerRuleActionArgs(
            type="forward",
            target_group_arn=data_manager_tg.arn,
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
    "ensemble_manager_rule",
    listener_arn=alb_listener.arn,
    priority=150,
    actions=[
        aws.lb.ListenerRuleActionArgs(
            type="forward",
            target_group_arn=ensemble_manager_tg.arn,
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
        data_manager_secret_name,
        portfolio_manager_secret_name,
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
                        "Sid": "ManageSESIdentities",
                        "Effect": "Allow",
                        "Action": [
                            "ses:CreateEmailIdentity",
                            "ses:DeleteEmailIdentity",
                            "ses:GetEmailIdentity",
                            "ses:TagResource",
                            "ses:UntagResource",
                            "ses:ListTagsForResource",
                        ],
                        "Resource": [
                            f"arn:aws:ses:{region}:{account_id}:identity/*",
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
                                    "fund-ecs-task-role-ses-policy",
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
        data_manager_secret.arn,
        portfolio_manager_secret.arn,
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

# SES Email Identity for training notifications
training_notification_email_identity = aws.ses.EmailIdentity(
    "training_notification_email_identity",
    email=training_notification_sender_email,
)

training_notification_sender_email_parameter = aws.ssm.Parameter(
    "training_notification_sender_email_parameter",
    name="/fund/training/training_notification_sender_email",
    type="SecureString",
    value=training_notification_sender_email,
    tags=tags,
)

training_notification_recipients_parameter = aws.ssm.Parameter(
    "training_notification_recipients_parameter",
    name="/fund/training/training_notification_recipients",
    type="SecureString",
    value=training_notification_recipient_emails,
    tags=tags,
)

# Allow ECS tasks to send emails via SES
aws.iam.RolePolicy(
    "task_role_ses_policy",
    name="fund-ecs-task-role-ses-policy",
    role=task_role.id,
    policy=training_notification_email_identity.arn.apply(
        lambda identity_arn: json.dumps(
            {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Action": ["ses:SendEmail", "ses:SendRawEmail"],
                        "Resource": identity_arn,
                    }
                ],
            },
            sort_keys=True,
        )
    ),
)

# Prefect Infrastructure

# RDS Security Group - allows inbound Postgres from ECS tasks
prefect_rds_security_group = aws.ec2.SecurityGroup(
    "prefect_rds_sg",
    name="fund-prefect-rds",
    vpc_id=vpc.id,
    description="Security group for Prefect RDS database",
    tags=tags,
)

aws.ec2.SecurityGroupRule(
    "prefect_rds_ingress",
    type="ingress",
    security_group_id=prefect_rds_security_group.id,
    source_security_group_id=ecs_security_group.id,
    protocol="tcp",
    from_port=5432,
    to_port=5432,
    description="Allow Postgres from ECS tasks",
)

aws.ec2.SecurityGroupRule(
    "prefect_rds_egress",
    type="egress",
    security_group_id=prefect_rds_security_group.id,
    protocol="-1",
    from_port=0,
    to_port=0,
    cidr_blocks=["0.0.0.0/0"],
    description="Allow all outbound",
)

# Redis Security Group - allows inbound Redis from ECS tasks
prefect_redis_security_group = aws.ec2.SecurityGroup(
    "prefect_redis_sg",
    name="fund-prefect-redis",
    vpc_id=vpc.id,
    description="Security group for Prefect Redis cache",
    tags=tags,
)

aws.ec2.SecurityGroupRule(
    "prefect_redis_ingress",
    type="ingress",
    security_group_id=prefect_redis_security_group.id,
    source_security_group_id=ecs_security_group.id,
    protocol="tcp",
    from_port=6379,
    to_port=6379,
    description="Allow Redis from ECS tasks",
)

aws.ec2.SecurityGroupRule(
    "prefect_redis_egress",
    type="egress",
    security_group_id=prefect_redis_security_group.id,
    protocol="-1",
    from_port=0,
    to_port=0,
    cidr_blocks=["0.0.0.0/0"],
    description="Allow all outbound",
)

# RDS Subnet Group
prefect_rds_subnet_group = aws.rds.SubnetGroup(
    "prefect_rds_subnet_group",
    name="fund-prefect-rds",
    subnet_ids=[private_subnet_1.id, private_subnet_2.id],
    tags=tags,
)

# RDS PostgreSQL for Prefect database
prefect_database = aws.rds.Instance(
    "prefect_database",
    identifier="fund-prefect",
    engine="postgres",
    engine_version="14",
    instance_class="db.t3.micro",
    allocated_storage=20,
    db_name="prefect",
    username="prefect",
    manage_master_user_password=True,
    db_subnet_group_name=prefect_rds_subnet_group.name,
    vpc_security_group_ids=[prefect_rds_security_group.id],
    skip_final_snapshot=False,
    final_snapshot_identifier=f"fund-prefect-final-{pulumi.get_stack()}",
    backup_retention_period=7,
    storage_encrypted=True,
    deletion_protection=True,
    tags=tags,
)

# Grant ECS execution role access to the RDS-managed master password secret
aws.iam.RolePolicy(
    "execution_role_prefect_db_secret_policy",
    name="fund-ecs-execution-role-prefect-db-secret",
    role=execution_role.id,
    policy=prefect_database.master_user_secrets[0]["secret_arn"].apply(
        lambda arn: json.dumps(
            {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Action": ["secretsmanager:GetSecretValue"],
                        "Resource": arn,
                    }
                ],
            },
            sort_keys=True,
        )
    ),
)

# ElastiCache Subnet Group
prefect_elasticache_subnet_group = aws.elasticache.SubnetGroup(
    "prefect_elasticache_subnet_group",
    name="fund-prefect-redis",
    subnet_ids=[private_subnet_1.id, private_subnet_2.id],
    tags=tags,
)

# ElastiCache Redis for Prefect messaging
prefect_redis = aws.elasticache.Cluster(
    "prefect_redis",
    cluster_id="fund-prefect-redis",
    engine="redis",
    engine_version="7.0",
    node_type="cache.t3.micro",
    num_cache_nodes=1,
    subnet_group_name=prefect_elasticache_subnet_group.name,
    security_group_ids=[prefect_redis_security_group.id],
    tags=tags,
)

# Allow ECS tasks to communicate with Prefect server on port 4200
aws.ec2.SecurityGroupRule(
    "ecs_prefect_ingress",
    type="ingress",
    security_group_id=ecs_security_group.id,
    source_security_group_id=ecs_security_group.id,
    protocol="tcp",
    from_port=4200,
    to_port=4200,
    description="Allow Prefect server communication",
)

# Allow ALB to reach Prefect server on port 4200
aws.ec2.SecurityGroupRule(
    "ecs_prefect_alb_ingress",
    type="ingress",
    security_group_id=ecs_security_group.id,
    source_security_group_id=alb_security_group.id,
    protocol="tcp",
    from_port=4200,
    to_port=4200,
    description="Allow ALB traffic to Prefect server",
)

# Prefect Server Log Group
training_server_log_group = aws.cloudwatch.LogGroup(
    "training_server_logs",
    name="/ecs/fund/training-server",
    retention_in_days=7,
    tags=tags,
)

# Prefect Worker Log Group
training_worker_log_group = aws.cloudwatch.LogGroup(
    "training_worker_logs",
    name="/ecs/fund/training-worker",
    retention_in_days=7,
    tags=tags,
)

# Prefect Server Task Definition
training_server_task_definition = aws.ecs.TaskDefinition(
    "training_server_task",
    family="training-server",
    cpu="512",
    memory="1024",
    network_mode="awsvpc",
    requires_compatibilities=["FARGATE"],
    execution_role_arn=execution_role.arn,
    task_role_arn=task_role.arn,
    container_definitions=pulumi.Output.all(
        training_server_log_group.name,
        prefect_database.endpoint,
        prefect_database.master_user_secrets[0]["secret_arn"],
        training_server_image_uri,
        alb.dns_name,
    ).apply(
        lambda args: json.dumps(
            [
                {
                    "name": "training-server",
                    "image": args[3],
                    # Inline bash/python constructs the database URL at runtime
                    # because the password comes from Secrets Manager and must be
                    # URL-encoded before embedding in the connection string.
                    # Extracting this to a separate script would require building
                    # and deploying another Docker image.
                    "command": [
                        "bash",
                        "-c",
                        (
                            "export PREFECT_API_DATABASE_CONNECTION_URL="
                            '$(python3 -c "'
                            "import os, urllib.parse;"
                            "p=urllib.parse.quote(os.environ['PREFECT_DB_PASSWORD'],safe='');"
                            f"print(f'postgresql+asyncpg://prefect:{{p}}@{args[1]}/prefect')"
                            '")'
                            " && prefect server start --host 0.0.0.0"
                        ),
                    ],
                    "portMappings": [{"containerPort": 4200, "protocol": "tcp"}],
                    "environment": [
                        {
                            "name": "PREFECT_UI_API_URL",
                            "value": (
                                f"{'https' if acm_certificate_arn else 'http'}://"
                                f"{args[4]}:4200/api"
                            ),
                        },
                    ],
                    "secrets": [
                        {
                            "name": "PREFECT_DB_PASSWORD",
                            "valueFrom": f"{args[2]}:password::",
                        },
                    ],
                    "logConfiguration": {
                        "logDriver": "awslogs",
                        "options": {
                            "awslogs-group": args[0],
                            "awslogs-region": region,
                            "awslogs-stream-prefix": "training-server",
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

# Prefect Server Service Discovery
training_server_sd_service = aws.servicediscovery.Service(
    "training_server_sd",
    name="training-server",
    dns_config=aws.servicediscovery.ServiceDnsConfigArgs(
        namespace_id=service_discovery_namespace.id,
        dns_records=[
            aws.servicediscovery.ServiceDnsConfigDnsRecordArgs(ttl=10, type="A")
        ],
    ),
    tags=tags,
)

# Prefect Server ECS Service
training_server_service = aws.ecs.Service(
    "training_server_service",
    name="fund-training-server",
    cluster=cluster.arn,
    task_definition=training_server_task_definition.arn,
    desired_count=1,
    launch_type="FARGATE",
    network_configuration=aws.ecs.ServiceNetworkConfigurationArgs(
        subnets=[private_subnet_1.id, private_subnet_2.id],
        security_groups=[ecs_security_group.id],
        assign_public_ip=False,
    ),
    load_balancers=[
        aws.ecs.ServiceLoadBalancerArgs(
            target_group_arn=training_tg.arn,
            container_name="training-server",
            container_port=4200,
        )
    ],
    service_registries=aws.ecs.ServiceServiceRegistriesArgs(
        registry_arn=training_server_sd_service.arn
    ),
    opts=pulumi.ResourceOptions(
        depends_on=[prefect_database, prefect_redis, prefect_listener],
    ),
    tags=tags,
)

# Prefect Worker Task Definition
training_worker_task_definition = aws.ecs.TaskDefinition(
    "training_worker_task",
    family="training-worker",
    cpu="4096",
    memory="8192",
    network_mode="awsvpc",
    requires_compatibilities=["FARGATE"],
    execution_role_arn=execution_role.arn,
    task_role_arn=task_role.arn,
    container_definitions=pulumi.Output.all(
        training_worker_log_group.name,
        service_discovery_namespace.name,
        data_bucket.bucket,
        model_artifacts_bucket.bucket,
        training_worker_image_uri,
        training_notification_sender_email_parameter.arn,
        training_notification_recipients_parameter.arn,
    ).apply(
        lambda args: json.dumps(
            [
                {
                    "name": "training-worker",
                    "image": args[4],
                    "environment": [
                        {
                            "name": "PREFECT_API_URL",
                            "value": f"http://training-server.{args[1]}:4200/api",
                        },
                        {
                            "name": "AWS_S3_DATA_BUCKET_NAME",
                            "value": args[2],
                        },
                        {
                            "name": "AWS_S3_MODEL_ARTIFACTS_BUCKET_NAME",
                            "value": args[3],
                        },
                        {
                            "name": "FUND_DATAMANAGER_BASE_URL",
                            "value": f"http://data-manager.{args[1]}:8080",
                        },
                        {
                            "name": "FUND_LOOKBACK_DAYS",
                            "value": "365",
                        },
                    ],
                    "secrets": [
                        {
                            "name": "FUND_TRAINING_NOTIFICATION_SENDER_EMAIL",
                            "valueFrom": args[5],
                        },
                        {
                            "name": "FUND_TRAINING_NOTIFICATION_RECIPIENT_EMAILS",
                            "valueFrom": args[6],
                        },
                    ],
                    "logConfiguration": {
                        "logDriver": "awslogs",
                        "options": {
                            "awslogs-group": args[0],
                            "awslogs-region": region,
                            "awslogs-stream-prefix": "training-worker",
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

# Prefect Worker ECS Service
training_worker_service = aws.ecs.Service(
    "training_worker_service",
    name="fund-training-worker",
    cluster=cluster.arn,
    task_definition=training_worker_task_definition.arn,
    desired_count=1,
    launch_type="FARGATE",
    network_configuration=aws.ecs.ServiceNetworkConfigurationArgs(
        subnets=[private_subnet_1.id, private_subnet_2.id],
        security_groups=[ecs_security_group.id],
        assign_public_ip=False,
    ),
    opts=pulumi.ResourceOptions(
        depends_on=[training_server_service],
    ),
    tags=tags,
)

data_manager_log_group = aws.cloudwatch.LogGroup(
    "data_manager_logs",
    name="/ecs/fund/data-manager",
    retention_in_days=7,
    tags=tags,
)

portfolio_manager_log_group = aws.cloudwatch.LogGroup(
    "portfolio_manager_logs",
    name="/ecs/fund/portfolio-manager",
    retention_in_days=7,
    tags=tags,
)

ensemble_manager_log_group = aws.cloudwatch.LogGroup(
    "ensemble_manager_logs",
    name="/ecs/fund/ensemble-manager",
    retention_in_days=7,
    tags=tags,
)

data_manager_task_definition = aws.ecs.TaskDefinition(
    "data_manager_task",
    family="data-manager",
    cpu="256",
    memory="512",
    network_mode="awsvpc",
    requires_compatibilities=["FARGATE"],
    execution_role_arn=execution_role.arn,
    task_role_arn=task_role.arn,
    container_definitions=pulumi.Output.all(
        data_manager_log_group.name,
        data_manager_image_uri,
        data_manager_secret.arn,
        shared_secret.arn,
        data_bucket.bucket,
    ).apply(
        lambda args: json.dumps(
            [
                {
                    "name": "data-manager",
                    "image": args[1],
                    "portMappings": [{"containerPort": 8080, "protocol": "tcp"}],
                    "environment": [
                        {
                            "name": "MASSIVE_BASE_URL",
                            "value": "https://api.massive.com",
                        },
                        {
                            "name": "AWS_S3_DATA_BUCKET_NAME",
                            "value": args[4],
                        },
                        {
                            "name": "FUND_ENVIRONMENT",
                            "value": "production",
                        },
                        {
                            "name": "RUST_LOG",
                            "value": "data_manager=info,tower_http=info",
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
                            "awslogs-stream-prefix": "data-manager",
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

portfolio_manager_task_definition = aws.ecs.TaskDefinition(
    "portfolio_manager_task",
    family="portfolio-manager",
    cpu="256",
    memory="512",
    network_mode="awsvpc",
    requires_compatibilities=["FARGATE"],
    execution_role_arn=execution_role.arn,
    task_role_arn=task_role.arn,
    container_definitions=pulumi.Output.all(
        portfolio_manager_log_group.name,
        service_discovery_namespace.name,
        portfolio_manager_image_uri,
        portfolio_manager_secret.arn,
        shared_secret.arn,
        parameters.uncertainty_threshold.value,
    ).apply(
        lambda args: json.dumps(
            [
                {
                    "name": "portfolio-manager",
                    "image": args[2],
                    "portMappings": [{"containerPort": 8080, "protocol": "tcp"}],
                    "environment": [
                        {
                            "name": "FUND_DATAMANAGER_BASE_URL",
                            "value": f"http://data-manager.{args[1]}:8080",
                        },
                        {
                            "name": "FUND_ENSEMBLE_MANAGER_BASE_URL",
                            "value": f"http://ensemble-manager.{args[1]}:8080",
                        },
                        {
                            "name": "FUND_ENVIRONMENT",
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
                            "awslogs-stream-prefix": "portfolio-manager",
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

ensemble_manager_task_definition = aws.ecs.TaskDefinition(
    "ensemble_manager_task",
    family="ensemble-manager",
    cpu="256",
    memory="512",
    network_mode="awsvpc",
    requires_compatibilities=["FARGATE"],
    execution_role_arn=execution_role.arn,
    task_role_arn=task_role.arn,
    container_definitions=pulumi.Output.all(
        ensemble_manager_log_group.name,
        service_discovery_namespace.name,
        ensemble_manager_image_uri,
        model_artifacts_bucket.bucket,
        shared_secret.arn,
    ).apply(
        lambda args: json.dumps(
            [
                {
                    "name": "ensemble-manager",
                    "image": args[2],
                    "portMappings": [{"containerPort": 8080, "protocol": "tcp"}],
                    "environment": [
                        {
                            "name": "FUND_DATAMANAGER_BASE_URL",
                            "value": f"http://data-manager.{args[1]}:8080",
                        },
                        {
                            "name": "AWS_S3_MODEL_ARTIFACTS_BUCKET_NAME",
                            "value": args[3],
                        },
                        {
                            "name": "FUND_ENVIRONMENT",
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
                            "awslogs-stream-prefix": "ensemble-manager",
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

data_manager_sd_service = aws.servicediscovery.Service(
    "data_manager_sd",
    name="data-manager",
    dns_config=aws.servicediscovery.ServiceDnsConfigArgs(
        namespace_id=service_discovery_namespace.id,
        dns_records=[
            aws.servicediscovery.ServiceDnsConfigDnsRecordArgs(ttl=10, type="A")
        ],
    ),
    tags=tags,
)

portfolio_manager_sd_service = aws.servicediscovery.Service(
    "portfolio_manager_sd",
    name="portfolio-manager",
    dns_config=aws.servicediscovery.ServiceDnsConfigArgs(
        namespace_id=service_discovery_namespace.id,
        dns_records=[
            aws.servicediscovery.ServiceDnsConfigDnsRecordArgs(ttl=10, type="A")
        ],
    ),
    tags=tags,
)

ensemble_manager_sd_service = aws.servicediscovery.Service(
    "ensemble_manager_sd",
    name="ensemble-manager",
    dns_config=aws.servicediscovery.ServiceDnsConfigArgs(
        namespace_id=service_discovery_namespace.id,
        dns_records=[
            aws.servicediscovery.ServiceDnsConfigDnsRecordArgs(ttl=10, type="A")
        ],
    ),
    tags=tags,
)

data_manager_service = aws.ecs.Service(
    "data_manager_service",
    name="fund-data-manager",
    cluster=cluster.arn,
    task_definition=data_manager_task_definition.arn,
    desired_count=1,
    launch_type="FARGATE",
    network_configuration=aws.ecs.ServiceNetworkConfigurationArgs(
        subnets=[private_subnet_1.id, private_subnet_2.id],
        security_groups=[ecs_security_group.id],
        assign_public_ip=False,
    ),
    load_balancers=[
        aws.ecs.ServiceLoadBalancerArgs(
            target_group_arn=data_manager_tg.arn,
            container_name="data-manager",
            container_port=8080,
        )
    ],
    service_registries=aws.ecs.ServiceServiceRegistriesArgs(
        registry_arn=data_manager_sd_service.arn
    ),
    opts=pulumi.ResourceOptions(depends_on=[alb_listener]),
    tags=tags,
)

portfolio_manager_service = aws.ecs.Service(
    "portfolio_manager_service",
    name="fund-portfolio-manager",
    cluster=cluster.arn,
    task_definition=portfolio_manager_task_definition.arn,
    desired_count=1,
    launch_type="FARGATE",
    network_configuration=aws.ecs.ServiceNetworkConfigurationArgs(
        subnets=[private_subnet_1.id, private_subnet_2.id],
        security_groups=[ecs_security_group.id],
        assign_public_ip=False,
    ),
    load_balancers=[
        aws.ecs.ServiceLoadBalancerArgs(
            target_group_arn=portfolio_manager_tg.arn,
            container_name="portfolio-manager",
            container_port=8080,
        )
    ],
    service_registries=aws.ecs.ServiceServiceRegistriesArgs(
        registry_arn=portfolio_manager_sd_service.arn
    ),
    opts=pulumi.ResourceOptions(depends_on=[alb_listener, data_manager_service]),
    tags=tags,
)

ensemble_manager_service = aws.ecs.Service(
    "ensemble_manager_service",
    name="fund-ensemble-manager",
    cluster=cluster.arn,
    task_definition=ensemble_manager_task_definition.arn,
    desired_count=1,
    launch_type="FARGATE",
    network_configuration=aws.ecs.ServiceNetworkConfigurationArgs(
        subnets=[private_subnet_1.id, private_subnet_2.id],
        security_groups=[ecs_security_group.id],
        assign_public_ip=False,
    ),
    load_balancers=[
        aws.ecs.ServiceLoadBalancerArgs(
            target_group_arn=ensemble_manager_tg.arn,
            container_name="ensemble-manager",
            container_port=8080,
        )
    ],
    service_registries=aws.ecs.ServiceServiceRegistriesArgs(
        registry_arn=ensemble_manager_sd_service.arn
    ),
    opts=pulumi.ResourceOptions(depends_on=[alb_listener, data_manager_service]),
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
pulumi.export("aws_ecr_data_manager_image", data_manager_image_uri)
pulumi.export("aws_ecr_portfolio_manager_image", portfolio_manager_image_uri)
pulumi.export("aws_ecr_ensemble_manager_image", ensemble_manager_image_uri)
pulumi.export("aws_ecr_data_manager_repository", data_manager_repository.repository_url)
pulumi.export(
    "aws_ecr_portfolio_manager_repository", portfolio_manager_repository.repository_url
)
pulumi.export(
    "aws_ecr_ensemble_manager_repository", ensemble_manager_repository.repository_url
)
pulumi.export("aws_s3_data_bucket_name", pulumi.Output.unsecret(data_bucket.bucket))
pulumi.export(
    "aws_s3_model_artifacts_bucket_name",
    pulumi.Output.unsecret(model_artifacts_bucket.bucket),
)
pulumi.export(
    "aws_ecr_tide_trainer_repository",
    tide_trainer_repository.repository_url,
)
pulumi.export("aws_ecr_tide_trainer_image", tide_trainer_image_uri)
pulumi.export(
    "aws_ecr_training_worker_repository", training_worker_repository.repository_url
)
pulumi.export("aws_ecr_training_worker_image", training_worker_image_uri)
pulumi.export(
    "prefect_api_url",
    pulumi.Output.concat(
        "http://training-server.", service_discovery_namespace.name, ":4200/api"
    ),
)
prefect_ui_url = (
    pulumi.Output.concat("https://", alb.dns_name, ":4200")
    if acm_certificate_arn
    else pulumi.Output.from_input("TLS certificate not configured")
)
pulumi.export("prefect_ui_url", prefect_ui_url)
pulumi.export("prefect_ui_tls_enabled", bool(acm_certificate_arn))
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
