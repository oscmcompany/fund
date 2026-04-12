import json
import os
import sys
from dataclasses import dataclass

import structlog

logger = structlog.get_logger()


@dataclass
class NetworkConfig:
    vpc_id: str
    private_subnet_1_id: str
    private_subnet_2_id: str
    ecs_security_group_id: str


def build_work_pool_template(
    template: dict,
    cluster: str,
    aws_credentials_block_id: str,
    task_definition_arn: str,
    network: NetworkConfig,
) -> dict:
    """Apply ECS GPU work pool configuration to a Prefect base job template."""
    aws_region = os.environ.get("AWS_REGION", "us-east-1")

    variables = template["variables"]["properties"]
    variables["cluster"]["default"] = cluster
    variables["aws_credentials"]["default"] = {
        "$ref": {"block_document_id": aws_credentials_block_id}
    }
    variables["capacity_provider_strategy"]["default"] = [
        {"capacityProvider": "fund-models-gpu", "weight": 1}
    ]
    variables["launch_type"]["default"] = None
    variables["task_definition_arn"]["default"] = task_definition_arn
    variables["vpc_id"]["default"] = network.vpc_id
    variables["network_configuration"]["default"] = {
        "subnets": [network.private_subnet_1_id, network.private_subnet_2_id],
        "securityGroups": [network.ecs_security_group_id],
        "assignPublicIp": "DISABLED",
    }

    task_def = template.setdefault("job_configuration", {}).setdefault(
        "task_definition", {}
    )
    containers = task_def.setdefault("containerDefinitions", [{}])
    if not containers:
        containers.append({})

    for container in containers:
        container["resourceRequirements"] = [{"type": "GPU", "value": "1"}]
        container["logConfiguration"] = {
            "logDriver": "awslogs",
            "options": {
                "awslogs-group": "/ecs/fund/models",
                "awslogs-region": aws_region,
                "awslogs-stream-prefix": "tide",
            },
        }

    return template


if __name__ == "__main__":
    expected_arg_count = 8
    if len(sys.argv) != expected_arg_count:
        usage = (
            "Usage: python build_work_pool_template.py"
            " <cluster> <aws_credentials_block_id> <task_definition_arn>"
            " <vpc_id> <private_subnet_1_id> <private_subnet_2_id>"
            " <ecs_security_group_id>"
        )
        logger.error(usage, args_received=len(sys.argv) - 1)
        sys.exit(1)

    (
        _,
        cluster,
        aws_credentials_block_id,
        task_definition_arn,
        vpc_id,
        private_subnet_1_id,
        private_subnet_2_id,
        ecs_security_group_id,
    ) = sys.argv

    try:
        template = json.load(sys.stdin)
    except json.JSONDecodeError as e:
        logger.exception("Failed to parse work pool template JSON", error=f"{e}")
        sys.exit(1)

    result = build_work_pool_template(
        template=template,
        cluster=cluster,
        aws_credentials_block_id=aws_credentials_block_id,
        task_definition_arn=task_definition_arn,
        network=NetworkConfig(
            vpc_id=vpc_id,
            private_subnet_1_id=private_subnet_1_id,
            private_subnet_2_id=private_subnet_2_id,
            ecs_security_group_id=ecs_security_group_id,
        ),
    )

    sys.stdout.write(json.dumps(result) + "\n")
