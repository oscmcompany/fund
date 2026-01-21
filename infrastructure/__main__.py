import json

import parameters
import pulumi
import pulumi_aws as aws

current_identity = aws.get_caller_identity()

account_id = current_identity.account_id

region = aws.get_region().name

secret = aws.secretsmanager.get_secret(
    name="pocketsizefund/production/environment_variables",
)

availability_zone_a = f"{region}a"
availability_zone_b = f"{region}b"

tags = {
    "project": "pocketsizefund",
    "stack": pulumi.get_stack(),
    "manager": "pulumi",
}

# S3 Data Bucket for storing equity bars, predictions, portfolios
data_bucket = aws.s3.Bucket(
    "data_bucket",
    bucket_prefix="pocketsizefund-data-",
    tags=tags,
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
    bucket_prefix="pocketsizefund-model-artifacts-",
    tags=tags,
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
    name="pocketsizefund/datamanager-server",
    image_tag_mutability="MUTABLE",
    image_scanning_configuration=aws.ecr.RepositoryImageScanningConfigurationArgs(
        scan_on_push=True,
    ),
    tags=tags,
)

portfoliomanager_repository = aws.ecr.Repository(
    "portfoliomanager_repository",
    name="pocketsizefund/portfoliomanager-server",
    image_tag_mutability="MUTABLE",
    image_scanning_configuration=aws.ecr.RepositoryImageScanningConfigurationArgs(
        scan_on_push=True,
    ),
    tags=tags,
)

equitypricemodel_repository = aws.ecr.Repository(
    "equitypricemodel_repository",
    name="pocketsizefund/equitypricemodel-server",
    image_tag_mutability="MUTABLE",
    image_scanning_configuration=aws.ecr.RepositoryImageScanningConfigurationArgs(
        scan_on_push=True,
    ),
    tags=tags,
)

equitypricemodel_trainer_repository = aws.ecr.Repository(
    "equitypricemodel_trainer_repository",
    name="pocketsizefund/equitypricemodel-trainer",
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
    name="pocketsizefund-alb",
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
    name="pocketsizefund-ecs-tasks",
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
    name="pocketsizefund-vpc-endpoints",
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
    name="pocketsizefund-application",
    settings=[aws.ecs.ClusterSettingArgs(name="containerInsights", value="enabled")],
    tags=tags,
)

# Service Discovery Namespace for inter-service communication
service_discovery_namespace = aws.servicediscovery.PrivateDnsNamespace(
    "service_discovery",
    name="pocketsizefund.local",
    vpc=vpc.id,
    description="Service discovery for pocketsizefund services",
    tags=tags,
)

alb = aws.lb.LoadBalancer(
    "alb",
    name="pocketsizefund-alb",
    subnets=[public_subnet_1.id, public_subnet_2.id],
    security_groups=[alb_security_group.id],
    internal=False,
    load_balancer_type="application",
    tags=tags,
)

datamanager_tg = aws.lb.TargetGroup(
    "datamanager_tg",
    name="pocketsizefund-datamanager",
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
    name="pocketsizefund-portfoliomanager",
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
    name="pocketsizefund-equitypricemodel",
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

# IAM Role for ECS to perform infrastructure tasks
execution_role = aws.iam.Role(
    "execution_role",
    name="pocketsizefund-ecs-execution-role",
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
        }
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
    name="pocketsizefund-ecs-execution-role-secrets-policy",
    role=execution_role.id,
    policy=pulumi.Output.all(secret.arn).apply(
        lambda args: json.dumps(
            {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Action": ["secretsmanager:GetSecretValue"],
                        "Resource": args[0],
                    }
                ],
            }
        )
    ),
)


# IAM Role for ECS tasks to access AWS resources
task_role = aws.iam.Role(
    "task_role",
    name="pocketsizefund-ecs-task-role",
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
        }
    ),
    tags=tags,
)

aws.iam.RolePolicy(
    "task_role_s3_policy",
    name="pocketsizefund-ecs-task-role-s3-policy",
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
            }
        )
    ),
)

aws.iam.RolePolicy(
    "task_role_ssm_policy",
    name="pocketsizefund-ecs-task-role-ssm-policy",
    role=task_role.id,
    policy=json.dumps(
        {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Action": ["ssm:GetParameter", "ssm:GetParameters"],
                    "Resource": (
                        f"arn:aws:ssm:{region}:{account_id}:parameter/pocketsizefund/*"
                    ),
                }
            ],
        }
    ),
)

# SageMaker Execution Role for training jobs
sagemaker_execution_role = aws.iam.Role(
    "sagemaker_execution_role",
    name="pocketsizefund-sagemaker-execution-role",
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
        }
    ),
    tags=tags,
)

aws.iam.RolePolicy(
    "sagemaker_s3_policy",
    name="pocketsizefund-sagemaker-s3-policy",
    role=sagemaker_execution_role.id,
    policy=pulumi.Output.all(data_bucket.arn, model_artifacts_bucket.arn).apply(
        lambda args: json.dumps(
            {
                "Version": "2012-10-17",
                "Statement": [
                    {
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
                    }
                ],
            }
        )
    ),
)

aws.iam.RolePolicy(
    "sagemaker_ecr_policy",
    name="pocketsizefund-sagemaker-ecr-policy",
    role=sagemaker_execution_role.id,
    policy=pulumi.Output.all(equitypricemodel_trainer_repository.arn).apply(
        lambda args: json.dumps(
            {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Action": [
                            "ecr:GetDownloadUrlForLayer",
                            "ecr:BatchGetImage",
                            "ecr:BatchCheckLayerAvailability",
                        ],
                        "Resource": args[0],
                    },
                    {
                        "Effect": "Allow",
                        "Action": "ecr:GetAuthorizationToken",
                        "Resource": "*",
                    },
                ],
            }
        )
    ),
)

aws.iam.RolePolicy(
    "sagemaker_cloudwatch_policy",
    name="pocketsizefund-sagemaker-cloudwatch-policy",
    role=sagemaker_execution_role.id,
    policy=json.dumps(
        {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Action": [
                        "logs:CreateLogGroup",
                        "logs:CreateLogStream",
                        "logs:PutLogEvents",
                        "logs:DescribeLogStreams",
                    ],
                    "Resource": "arn:aws:logs:*:*:log-group:/aws/sagemaker/*",
                }
            ],
        }
    ),
)

datamanager_log_group = aws.cloudwatch.LogGroup(
    "datamanager_logs",
    name="/ecs/pocketsizefund/datamanager",
    retention_in_days=7,
    tags=tags,
)

portfoliomanager_log_group = aws.cloudwatch.LogGroup(
    "portfoliomanager_logs",
    name="/ecs/pocketsizefund/portfoliomanager",
    retention_in_days=7,
    tags=tags,
)

equitypricemodel_log_group = aws.cloudwatch.LogGroup(
    "equitypricemodel_logs",
    name="/ecs/pocketsizefund/equitypricemodel",
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
        secret.arn,
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
                            "value": "https://api.polygon.io",
                        },
                        {
                            "name": "AWS_S3_DATA_BUCKET_NAME",
                            "value": args[3],
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
                            "valueFrom": f"{args[2]}:SENTRY_DSN::",
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
            ]
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
        secret.arn,
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
                            "name": "PSF_DATAMANAGER_BASE_URL",
                            "value": f"http://datamanager.{args[1]}:8080",
                        },
                        {
                            "name": "PSF_EQUITYPRICEMODEL_BASE_URL",
                            "value": f"http://equitypricemodel.{args[1]}:8080",
                        },
                        {
                            "name": "ENVIRONMENT",
                            "value": "production",
                        },
                        {
                            "name": "PSF_UNCERTAINTY_THRESHOLD",
                            "value": args[4],
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
                            "valueFrom": f"{args[3]}:SENTRY_DSN::",
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
            ]
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
        secret.arn,
    ).apply(
        lambda args: json.dumps(
            [
                {
                    "name": "equitypricemodel",
                    "image": args[2],
                    "portMappings": [{"containerPort": 8080, "protocol": "tcp"}],
                    "environment": [
                        {
                            "name": "PSF_DATAMANAGER_BASE_URL",
                            "value": f"http://datamanager.{args[1]}:8080",
                        },
                        {
                            "name": "AWS_S3_MODEL_ARTIFACTS_BUCKET",
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
            ]
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
    name="pocketsizefund-datamanager",
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
    name="pocketsizefund-portfoliomanager",
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
    name="pocketsizefund-equitypricemodel",
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

psf_base_url = pulumi.Output.concat(protocol, alb.dns_name)

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
pulumi.export("aws_s3_data_bucket", data_bucket.bucket)
pulumi.export("aws_s3_model_artifacts_bucket", model_artifacts_bucket.bucket)
pulumi.export(
    "aws_ecr_equitypricemodel_trainer_repository",
    equitypricemodel_trainer_repository.repository_url,
)
pulumi.export(
    "aws_ecr_equitypricemodel_trainer_image", equitypricemodel_trainer_image_uri
)
pulumi.export("aws_iam_sagemaker_role_arn", sagemaker_execution_role.arn)
pulumi.export("psf_base_url", psf_base_url)
pulumi.export("readme", pulumi.Output.format(readme_content, psf_base_url))
