import json
from secrets import data_manager_secret, portfolio_manager_secret, shared_secret

import parameters
import pulumi
import pulumi_aws as aws
from config import region, tags
from iam import (
    execution_role,
    task_role,
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
    portfolio_manager_image_uri,
)

cluster = aws.ecs.Cluster(
    "ecs_cluster",
    name="fund-applications",
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
    ip_address_type="ipv4",
    enable_cross_zone_load_balancing=True,
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

# Set acm_certificate_arn to enable HTTPS for the ALB.
acm_certificate_arn = None

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

data_manager_log_group = aws.cloudwatch.LogGroup(
    "data_manager_logs",
    name="/ecs/fund/applications-data-manager-server",
    retention_in_days=7,
    tags=tags,
)

portfolio_manager_log_group = aws.cloudwatch.LogGroup(
    "portfolio_manager_logs",
    name="/ecs/fund/applications-portfolio-manager-server",
    retention_in_days=7,
    tags=tags,
)

ensemble_manager_log_group = aws.cloudwatch.LogGroup(
    "ensemble_manager_logs",
    name="/ecs/fund/applications-ensemble-manager-server",
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
                            "name": "FUND_DATA_MANAGER_BASE_URL",
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
                            "name": "FUND_DATA_MANAGER_BASE_URL",
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
                        {
                            "name": "AWS_S3_MODEL_ARTIFACT_PATH",
                            "value": "artifacts/tide/",
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
    health_check_grace_period_seconds=180,
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
