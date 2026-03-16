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
    grafana_image_uri,
    mlflow_artifacts_bucket,
    mlflow_image_uri,
    model_artifacts_bucket,
    portfolio_manager_image_uri,
    training_server_image_uri,
    training_worker_image_uri,
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
    skip_final_snapshot=True,
    backup_retention_period=0,
    storage_encrypted=False,
    deletion_protection=False,
    apply_immediately=True,
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
                        {
                            "name": "MLFLOW_TRACKING_URI",
                            "value": f"http://mlflow.{args[1]}:8080",
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

# MLflow Infrastructure

# MLflow RDS Security Group - allows inbound Postgres from ECS tasks
mlflow_rds_security_group = aws.ec2.SecurityGroup(
    "mlflow_rds_sg",
    name="fund-mlflow-rds",
    vpc_id=vpc.id,
    description="Security group for MLflow RDS database",
    tags=tags,
)

aws.ec2.SecurityGroupRule(
    "mlflow_rds_ingress",
    type="ingress",
    security_group_id=mlflow_rds_security_group.id,
    source_security_group_id=ecs_security_group.id,
    protocol="tcp",
    from_port=5432,
    to_port=5432,
    description="Allow Postgres from ECS tasks",
)

aws.ec2.SecurityGroupRule(
    "mlflow_rds_egress",
    type="egress",
    security_group_id=mlflow_rds_security_group.id,
    protocol="-1",
    from_port=0,
    to_port=0,
    cidr_blocks=["0.0.0.0/0"],
    description="Allow all outbound",
)

# MLflow RDS Subnet Group
mlflow_rds_subnet_group = aws.rds.SubnetGroup(
    "mlflow_rds_subnet_group",
    name="fund-mlflow-rds",
    subnet_ids=[private_subnet_1.id, private_subnet_2.id],
    tags=tags,
)

# RDS PostgreSQL for MLflow database
mlflow_database = aws.rds.Instance(
    "mlflow_database",
    identifier="fund-mlflow",
    engine="postgres",
    engine_version="14",
    instance_class="db.t3.micro",
    allocated_storage=20,
    db_name="mlflow",
    username="mlflow",
    manage_master_user_password=True,
    db_subnet_group_name=mlflow_rds_subnet_group.name,
    vpc_security_group_ids=[mlflow_rds_security_group.id],
    skip_final_snapshot=False,
    final_snapshot_identifier=f"fund-mlflow-final-{pulumi.get_stack()}",
    backup_retention_period=7,
    storage_encrypted=True,
    deletion_protection=True,
    tags=tags,
)

# Grant ECS execution role access to the MLflow RDS-managed master password secret
aws.iam.RolePolicy(
    "execution_role_mlflow_db_secret_policy",
    name="fund-ecs-execution-role-mlflow-db-secret",
    role=execution_role.id,
    policy=mlflow_database.master_user_secrets[0]["secret_arn"].apply(
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

# Allow ECS tasks to communicate with MLflow on port 8080
aws.ec2.SecurityGroupRule(
    "ecs_mlflow_ingress",
    type="ingress",
    security_group_id=ecs_security_group.id,
    source_security_group_id=ecs_security_group.id,
    protocol="tcp",
    from_port=8080,
    to_port=8080,
    description="Allow MLflow server communication",
)

# Allow ALB to reach MLflow on port 8080
aws.ec2.SecurityGroupRule(
    "ecs_mlflow_alb_ingress",
    type="ingress",
    security_group_id=ecs_security_group.id,
    source_security_group_id=alb_security_group.id,
    protocol="tcp",
    from_port=8080,
    to_port=8080,
    description="Allow ALB traffic to MLflow server",
)

mlflow_tg = aws.lb.TargetGroup(
    "mlflow_tg",
    name="fund-mlflow",
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

mlflow_listener = aws.lb.Listener(
    "mlflow_listener",
    load_balancer_arn=alb.arn,
    port=5000,
    protocol="HTTP",
    default_actions=[
        aws.lb.ListenerDefaultActionArgs(
            type="forward",
            target_group_arn=mlflow_tg.arn,
        )
    ],
    tags=tags,
)

# MLflow Log Group
mlflow_log_group = aws.cloudwatch.LogGroup(
    "mlflow_logs",
    name="/ecs/fund/mlflow",
    retention_in_days=7,
    tags=tags,
)

# MLflow Task Definition
mlflow_task_definition = aws.ecs.TaskDefinition(
    "mlflow_task",
    family="mlflow",
    cpu="512",
    memory="2048",
    network_mode="awsvpc",
    requires_compatibilities=["FARGATE"],
    execution_role_arn=execution_role.arn,
    task_role_arn=task_role.arn,
    container_definitions=pulumi.Output.all(
        mlflow_log_group.name,
        mlflow_database.endpoint,
        mlflow_database.master_user_secrets[0]["secret_arn"],
        mlflow_image_uri,
        mlflow_artifacts_bucket.bucket,
    ).apply(
        lambda args: json.dumps(
            [
                {
                    "name": "mlflow",
                    "image": args[3],
                    "command": [
                        "bash",
                        "-c",
                        (
                            "export MLFLOW_BACKEND_STORE_URI="
                            '$(python3 -c "'
                            "import os, urllib.parse;"
                            "p=urllib.parse.quote(os.environ['MLFLOW_DB_PASSWORD'],safe='');"
                            f"print(f'postgresql://mlflow:{{p}}@{args[1]}/mlflow')"
                            '")'
                            " && exec mlflow server"
                            " --host 0.0.0.0"
                            " --port 8080"
                            " --workers 1"
                            " --backend-store-uri $MLFLOW_BACKEND_STORE_URI"
                            f" --default-artifact-root s3://{args[4]}"
                            " --serve-artifacts"
                        ),
                    ],
                    "portMappings": [{"containerPort": 8080, "protocol": "tcp"}],
                    "environment": [
                        {
                            "name": "MLFLOW_DEFAULT_ARTIFACT_ROOT",
                            "value": f"s3://{args[4]}",
                        },
                        {
                            "name": "MLFLOW_SERVER_DISABLE_SECURITY_MIDDLEWARE",
                            "value": "true",
                        },
                    ],
                    "secrets": [
                        {
                            "name": "MLFLOW_DB_PASSWORD",
                            "valueFrom": f"{args[2]}:password::",
                        },
                    ],
                    "logConfiguration": {
                        "logDriver": "awslogs",
                        "options": {
                            "awslogs-group": args[0],
                            "awslogs-region": region,
                            "awslogs-stream-prefix": "mlflow",
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

# MLflow Service Discovery
mlflow_sd_service = aws.servicediscovery.Service(
    "mlflow_sd",
    name="mlflow",
    dns_config=aws.servicediscovery.ServiceDnsConfigArgs(
        namespace_id=service_discovery_namespace.id,
        dns_records=[
            aws.servicediscovery.ServiceDnsConfigDnsRecordArgs(ttl=10, type="A")
        ],
    ),
    tags=tags,
)

# MLflow ECS Service
mlflow_service = aws.ecs.Service(
    "mlflow_service",
    name="fund-mlflow",
    cluster=cluster.arn,
    task_definition=mlflow_task_definition.arn,
    desired_count=1,
    launch_type="FARGATE",
    network_configuration=aws.ecs.ServiceNetworkConfigurationArgs(
        subnets=[private_subnet_1.id, private_subnet_2.id],
        security_groups=[ecs_security_group.id],
        assign_public_ip=False,
    ),
    load_balancers=[
        aws.ecs.ServiceLoadBalancerArgs(
            target_group_arn=mlflow_tg.arn,
            container_name="mlflow",
            container_port=8080,
        )
    ],
    service_registries=aws.ecs.ServiceServiceRegistriesArgs(
        registry_arn=mlflow_sd_service.arn
    ),
    opts=pulumi.ResourceOptions(
        depends_on=[mlflow_database, mlflow_listener],
    ),
    tags=tags,
)

# Grafana Infrastructure

grafana_tg = aws.lb.TargetGroup(
    "grafana_tg",
    name="fund-grafana",
    port=3000,
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

grafana_listener = aws.lb.Listener(
    "grafana_listener",
    load_balancer_arn=alb.arn,
    port=3000,
    protocol="HTTP",
    default_actions=[
        aws.lb.ListenerDefaultActionArgs(
            type="forward",
            target_group_arn=grafana_tg.arn,
        )
    ],
    tags=tags,
)

# Allow ECS tasks to communicate with Grafana on port 3000
aws.ec2.SecurityGroupRule(
    "ecs_grafana_ingress",
    type="ingress",
    security_group_id=ecs_security_group.id,
    source_security_group_id=ecs_security_group.id,
    protocol="tcp",
    from_port=3000,
    to_port=3000,
    description="Allow Grafana communication",
)

# Allow ALB to reach Grafana on port 3000
aws.ec2.SecurityGroupRule(
    "ecs_grafana_alb_ingress",
    type="ingress",
    security_group_id=ecs_security_group.id,
    source_security_group_id=alb_security_group.id,
    protocol="tcp",
    from_port=3000,
    to_port=3000,
    description="Allow ALB traffic to Grafana",
)

# Grafana Log Group
grafana_log_group = aws.cloudwatch.LogGroup(
    "grafana_logs",
    name="/ecs/fund/grafana",
    retention_in_days=7,
    tags=tags,
)

# Grafana Task Definition
grafana_task_definition = aws.ecs.TaskDefinition(
    "grafana_task",
    family="grafana",
    cpu="256",
    memory="512",
    network_mode="awsvpc",
    requires_compatibilities=["FARGATE"],
    execution_role_arn=execution_role.arn,
    task_role_arn=task_role.arn,
    container_definitions=pulumi.Output.all(
        grafana_log_group.name,
        grafana_image_uri,
        shared_secret.arn,
    ).apply(
        lambda args: json.dumps(
            [
                {
                    "name": "grafana",
                    "image": args[1],
                    "portMappings": [{"containerPort": 3000, "protocol": "tcp"}],
                    "environment": [
                        {
                            "name": "GF_SERVER_HTTP_PORT",
                            "value": "3000",
                        },
                        {
                            "name": "GF_AUTH_ANONYMOUS_ENABLED",
                            "value": "false",
                        },
                    ],
                    "secrets": [
                        {
                            "name": "GF_SECURITY_ADMIN_PASSWORD",
                            "valueFrom": f"{args[2]}:GF_SECURITY_ADMIN_PASSWORD::",
                        },
                    ],
                    "logConfiguration": {
                        "logDriver": "awslogs",
                        "options": {
                            "awslogs-group": args[0],
                            "awslogs-region": region,
                            "awslogs-stream-prefix": "grafana",
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

# Grafana ECS Service
grafana_service = aws.ecs.Service(
    "grafana_service",
    name="fund-grafana",
    cluster=cluster.arn,
    task_definition=grafana_task_definition.arn,
    desired_count=1,
    launch_type="FARGATE",
    network_configuration=aws.ecs.ServiceNetworkConfigurationArgs(
        subnets=[private_subnet_1.id, private_subnet_2.id],
        security_groups=[ecs_security_group.id],
        assign_public_ip=False,
    ),
    load_balancers=[
        aws.ecs.ServiceLoadBalancerArgs(
            target_group_arn=grafana_tg.arn,
            container_name="grafana",
            container_port=3000,
        )
    ],
    opts=pulumi.ResourceOptions(
        depends_on=[grafana_listener],
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
