import base64
import json

import pulumi
import pulumi_aws as aws
from config import region, tags
from iam import execution_role, task_role
from networking import ecs_security_group, private_subnet_1, private_subnet_2
from storage import tide_runner_image_uri

models_cluster = aws.ecs.Cluster(
    "models_cluster",
    name="fund-models",
    settings=[aws.ecs.ClusterSettingArgs(name="containerInsights", value="enabled")],
    tags=tags,
)

models_instance_role = aws.iam.Role(
    "models_instance_role",
    name="fund-models-instance-role",
    assume_role_policy=json.dumps(
        {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Action": "sts:AssumeRole",
                    "Effect": "Allow",
                    "Principal": {"Service": "ec2.amazonaws.com"},
                }
            ],
        },
        sort_keys=True,
    ),
    tags=tags,
)

aws.iam.RolePolicyAttachment(
    "models_instance_role_ecs_policy",
    role=models_instance_role.name,
    policy_arn="arn:aws:iam::aws:policy/service-role/AmazonEC2ContainerServiceforEC2Role",
)

models_instance_profile = aws.iam.InstanceProfile(
    "models_instance_profile",
    name="fund-models-instance-profile",
    role=models_instance_role.name,
    tags=tags,
)

models_ami_parameter = aws.ssm.get_parameter_output(
    name="/aws/service/ecs/optimized-ami/amazon-linux-2023/gpu/recommended",
)

models_ami_id = models_ami_parameter.value.apply(
    lambda value: json.loads(value)["image_id"]
)

models_launch_template = aws.ec2.LaunchTemplate(
    "models_launch_template",
    name="fund-models-gpu",
    image_id=models_ami_id,
    instance_type="g4dn.xlarge",
    metadata_options=aws.ec2.LaunchTemplateMetadataOptionsArgs(
        http_endpoint="enabled",
        http_tokens="required",
    ),
    iam_instance_profile=aws.ec2.LaunchTemplateIamInstanceProfileArgs(
        arn=models_instance_profile.arn,
    ),
    vpc_security_group_ids=[ecs_security_group.id],
    user_data=models_cluster.name.apply(
        lambda cluster_name: base64.b64encode(
            "\n".join(
                [
                    "#!/bin/bash",
                    f"echo ECS_CLUSTER={cluster_name} >> /etc/ecs/ecs.config",
                    "echo ECS_ENABLE_GPU_SUPPORT=true >> /etc/ecs/ecs.config",
                ]
            ).encode()
        ).decode()
    ),
    tag_specifications=[
        aws.ec2.LaunchTemplateTagSpecificationArgs(
            resource_type="instance",
            tags={**tags, "Name": "fund-models-gpu"},
        ),
        aws.ec2.LaunchTemplateTagSpecificationArgs(
            resource_type="volume",
            tags={**tags, "Name": "fund-models-gpu"},
        ),
    ],
    tags=tags,
)

models_asg = aws.autoscaling.Group(
    "models_asg",
    name="fund-models-gpu",
    min_size=0,
    max_size=1,
    desired_capacity=0,
    vpc_zone_identifiers=[private_subnet_1.id, private_subnet_2.id],
    launch_template=aws.autoscaling.GroupLaunchTemplateArgs(
        id=models_launch_template.id,
        version="$Latest",
    ),
    protect_from_scale_in=True,
    tags=[
        aws.autoscaling.GroupTagArgs(
            key="Name",
            value="fund-models-gpu",
            propagate_at_launch=True,
        ),
    ]
    + [
        aws.autoscaling.GroupTagArgs(
            key=key,
            value=value,
            propagate_at_launch=True,
        )
        for key, value in tags.items()
    ],
)

models_capacity_provider = aws.ecs.CapacityProvider(
    "models_capacity_provider",
    name="fund-models-gpu",
    auto_scaling_group_provider=aws.ecs.CapacityProviderAutoScalingGroupProviderArgs(
        auto_scaling_group_arn=models_asg.arn,
        managed_scaling=aws.ecs.CapacityProviderAutoScalingGroupProviderManagedScalingArgs(
            status="ENABLED",
            target_capacity=100,
            minimum_scaling_step_size=1,
            maximum_scaling_step_size=1,
        ),
        managed_termination_protection="ENABLED",
    ),
    tags=tags,
)

aws.ecs.ClusterCapacityProviders(
    "models_cluster_capacity_providers",
    cluster_name=models_cluster.name,
    capacity_providers=[models_capacity_provider.name],
    default_capacity_provider_strategies=[
        aws.ecs.ClusterCapacityProvidersDefaultCapacityProviderStrategyArgs(
            capacity_provider=models_capacity_provider.name,
            weight=1,
        )
    ],
)

models_log_group = aws.cloudwatch.LogGroup(
    "models_logs",
    name="/ecs/fund/models",
    retention_in_days=7,
    tags=tags,
)

tide_trainer_task_definition = aws.ecs.TaskDefinition(
    "tide_trainer_task_definition",
    family="tide-runner",
    requires_compatibilities=["EC2"],
    network_mode="awsvpc",
    cpu="4096",
    memory="14336",
    execution_role_arn=execution_role.arn,
    task_role_arn=task_role.arn,
    container_definitions=pulumi.Output.all(
        models_log_group.name,
        tide_runner_image_uri,
    ).apply(
        lambda args: json.dumps(
            [
                {
                    "name": "prefect",
                    "image": args[1],
                    "essential": True,
                    "resourceRequirements": [{"type": "GPU", "value": "1"}],
                    "logConfiguration": {
                        "logDriver": "awslogs",
                        "options": {
                            "awslogs-group": args[0],
                            "awslogs-region": region,
                            "awslogs-stream-prefix": "tide",
                        },
                    },
                }
            ],
            sort_keys=True,
        )
    ),
    tags=tags,
)

__all__ = [
    "execution_role",
    "models_cluster",
    "models_log_group",
    "task_role",
    "tide_trainer_task_definition",
]
