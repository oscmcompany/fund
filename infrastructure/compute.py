import json
from secrets import data_manager_secret, portfolio_manager_secret, shared_secret

import parameters
import pulumi
import pulumi_aws as aws
from config import region, tags
from iam import (
    execution_role,
    task_role,
    training_notification_recipients_parameter,
    training_notification_sender_email_parameter,
)
from networking import (
    alb_security_group,
    ecs_security_group,
    private_subnet_1,
    private_subnet_2,
    public_subnet_1,
    public_subnet_2,
    vpc,
)
from storage import (
    data_bucket,
    data_manager_image_uri,
    ensemble_manager_image_uri,
    model_artifacts_bucket,
    model_trainer_server_worker_image_uri,
    portfolio_manager_image_uri,
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
    name="fund-data-manager-server",
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
    name="fund-portfolio-manager-server",
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
    name="fund-ensemble-manager-server",
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

model_trainer_tg = aws.lb.TargetGroup(
    "model_trainer_tg",
    name="fund-model-trainer",
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
                target_group_arn=model_trainer_tg.arn,
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
                target_group_arn=model_trainer_tg.arn,
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

# Prefect Infrastructure

# RDS Security Group - allows inbound Postgres from ECS tasks
prefect_rds_security_group = aws.ec2.SecurityGroup(
    "prefect_rds_sg",
    name="fund-model-trainer-state",
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
    name="fund-model-trainer-broker",
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
    name="fund-model-trainer-state",
    subnet_ids=[private_subnet_1.id, private_subnet_2.id],
    tags=tags,
)

# RDS PostgreSQL for Prefect database
model_trainer_state = aws.rds.Instance(
    "model_trainer_state",
    identifier="fund-model-trainer-state",
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
    final_snapshot_identifier=f"fund-model-trainer-state-final-{pulumi.get_stack()}",
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
    policy=model_trainer_state.master_user_secrets[0]["secret_arn"].apply(
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
    name="fund-model-trainer-broker",
    subnet_ids=[private_subnet_1.id, private_subnet_2.id],
    tags=tags,
)

# ElastiCache Redis for Prefect messaging
model_trainer_broker = aws.elasticache.Cluster(
    "model_trainer_broker",
    cluster_id="fund-model-trainer-broker",
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
    name="/ecs/fund/model-trainer-server",
    retention_in_days=7,
    tags=tags,
)

# Prefect Worker Log Group
training_worker_log_group = aws.cloudwatch.LogGroup(
    "training_worker_logs",
    name="/ecs/fund/model-trainer-worker",
    retention_in_days=7,
    tags=tags,
)

# Prefect Server Task Definition
training_server_task_definition = aws.ecs.TaskDefinition(
    "training_server_task",
    family="model-trainer-server",
    cpu="512",
    memory="1024",
    network_mode="awsvpc",
    requires_compatibilities=["FARGATE"],
    execution_role_arn=execution_role.arn,
    task_role_arn=task_role.arn,
    container_definitions=pulumi.Output.all(
        training_server_log_group.name,
        model_trainer_state.endpoint,
        model_trainer_state.master_user_secrets[0]["secret_arn"],
        model_trainer_server_worker_image_uri,
        alb.dns_name,
    ).apply(
        lambda args: json.dumps(
            [
                {
                    "name": "model-trainer-server",
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
                            "awslogs-stream-prefix": "model-trainer-server",
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
    name="model-trainer-server",
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
    name="fund-model-trainer-server",
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
            target_group_arn=model_trainer_tg.arn,
            container_name="model-trainer-server",
            container_port=4200,
        )
    ],
    service_registries=aws.ecs.ServiceServiceRegistriesArgs(
        registry_arn=training_server_sd_service.arn
    ),
    opts=pulumi.ResourceOptions(
        depends_on=[model_trainer_state, model_trainer_broker, prefect_listener],
    ),
    tags=tags,
)

# Prefect Worker Task Definition
training_worker_task_definition = aws.ecs.TaskDefinition(
    "training_worker_task",
    family="model-trainer-worker",
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
        model_trainer_server_worker_image_uri,
        training_notification_sender_email_parameter.arn,
        training_notification_recipients_parameter.arn,
    ).apply(
        lambda args: json.dumps(
            [
                {
                    "name": "model-trainer-worker",
                    "image": args[4],
                    "environment": [
                        {
                            "name": "PREFECT_API_URL",
                            "value": f"http://model-trainer-server.{args[1]}:4200/api",
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
                            "value": f"http://data-manager-server.{args[1]}:8080",
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
                            "awslogs-stream-prefix": "model-trainer-worker",
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
    name="fund-model-trainer-worker",
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
    name="/ecs/fund/data-manager-server",
    retention_in_days=7,
    tags=tags,
)

portfolio_manager_log_group = aws.cloudwatch.LogGroup(
    "portfolio_manager_logs",
    name="/ecs/fund/portfolio-manager-server",
    retention_in_days=7,
    tags=tags,
)

ensemble_manager_log_group = aws.cloudwatch.LogGroup(
    "ensemble_manager_logs",
    name="/ecs/fund/ensemble-manager-server",
    retention_in_days=7,
    tags=tags,
)

data_manager_task_definition = aws.ecs.TaskDefinition(
    "data_manager_task",
    family="data-manager-server",
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
                    "name": "data-manager-server",
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
                            "awslogs-stream-prefix": "data-manager-server",
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
    family="portfolio-manager-server",
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
                    "name": "portfolio-manager-server",
                    "image": args[2],
                    "portMappings": [{"containerPort": 8080, "protocol": "tcp"}],
                    "environment": [
                        {
                            "name": "FUND_DATAMANAGER_BASE_URL",
                            "value": f"http://data-manager-server.{args[1]}:8080",
                        },
                        {
                            "name": "FUND_ENSEMBLE_MANAGER_BASE_URL",
                            "value": f"http://ensemble-manager-server.{args[1]}:8080",
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
                            "awslogs-stream-prefix": "portfolio-manager-server",
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
    family="ensemble-manager-server",
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
                    "name": "ensemble-manager-server",
                    "image": args[2],
                    "portMappings": [{"containerPort": 8080, "protocol": "tcp"}],
                    "environment": [
                        {
                            "name": "FUND_DATAMANAGER_BASE_URL",
                            "value": f"http://data-manager-server.{args[1]}:8080",
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
                            "awslogs-stream-prefix": "ensemble-manager-server",
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
    name="data-manager-server",
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
    name="portfolio-manager-server",
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
    name="ensemble-manager-server",
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
    name="fund-data-manager-server",
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
            container_name="data-manager-server",
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
    name="fund-portfolio-manager-server",
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
            container_name="portfolio-manager-server",
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
    name="fund-ensemble-manager-server",
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
            container_name="ensemble-manager-server",
            container_port=8080,
        )
    ],
    service_registries=aws.ecs.ServiceServiceRegistriesArgs(
        registry_arn=ensemble_manager_sd_service.arn
    ),
    opts=pulumi.ResourceOptions(depends_on=[alb_listener, data_manager_service]),
    tags=tags,
)
