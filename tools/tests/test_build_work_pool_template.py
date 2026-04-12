from tools.build_work_pool_template import NetworkConfig, build_work_pool_template


def _minimal_template() -> dict:
    return {
        "variables": {
            "properties": {
                "cluster": {},
                "aws_credentials": {},
                "capacity_provider_strategy": {},
                "launch_type": {},
                "task_definition_arn": {},
                "vpc_id": {},
                "network_configuration": {},
            }
        }
    }


def _default_network() -> NetworkConfig:
    return NetworkConfig(
        vpc_id="vpc-123",
        private_subnet_1_id="subnet-1",
        private_subnet_2_id="subnet-2",
        ecs_security_group_id="sg-123",
    )


def test_build_work_pool_template_sets_cluster() -> None:
    result = build_work_pool_template(
        template=_minimal_template(),
        cluster="test-cluster",
        aws_credentials_block_id="block-id",
        task_definition_arn="arn:aws:ecs:us-east-1:123:task-definition/tide:1",
        network=_default_network(),
    )

    assert result["variables"]["properties"]["cluster"]["default"] == "test-cluster"


def test_build_work_pool_template_sets_aws_credentials_ref() -> None:
    result = build_work_pool_template(
        template=_minimal_template(),
        cluster="test-cluster",
        aws_credentials_block_id="block-abc",
        task_definition_arn="arn:aws:ecs:us-east-1:123:task-definition/tide:1",
        network=_default_network(),
    )

    credentials = result["variables"]["properties"]["aws_credentials"]["default"]
    assert credentials == {"$ref": {"block_document_id": "block-abc"}}


def test_build_work_pool_template_sets_capacity_provider_and_clears_launch_type() -> (
    None
):
    result = build_work_pool_template(
        template=_minimal_template(),
        cluster="test-cluster",
        aws_credentials_block_id="block-id",
        task_definition_arn="arn:aws:ecs:us-east-1:123:task-definition/tide:1",
        network=_default_network(),
    )

    assert result["variables"]["properties"]["capacity_provider_strategy"][
        "default"
    ] == [{"capacityProvider": "fund-models-gpu", "weight": 1}]
    assert result["variables"]["properties"]["launch_type"]["default"] is None


def test_build_work_pool_template_sets_network_configuration() -> None:
    result = build_work_pool_template(
        template=_minimal_template(),
        cluster="test-cluster",
        aws_credentials_block_id="block-id",
        task_definition_arn="arn:aws:ecs:us-east-1:123:task-definition/tide:1",
        network=NetworkConfig(
            vpc_id="vpc-456",
            private_subnet_1_id="subnet-a",
            private_subnet_2_id="subnet-b",
            ecs_security_group_id="sg-789",
        ),
    )

    network = result["variables"]["properties"]["network_configuration"]["default"]
    assert network["subnets"] == ["subnet-a", "subnet-b"]
    assert network["securityGroups"] == ["sg-789"]
    assert network["assignPublicIp"] == "DISABLED"


def test_build_work_pool_template_configures_gpu_and_logging() -> None:
    result = build_work_pool_template(
        template=_minimal_template(),
        cluster="test-cluster",
        aws_credentials_block_id="block-id",
        task_definition_arn="arn:aws:ecs:us-east-1:123:task-definition/tide:1",
        network=_default_network(),
    )

    containers = result["job_configuration"]["task_definition"]["containerDefinitions"]
    assert len(containers) == 1
    assert containers[0]["resourceRequirements"] == [{"type": "GPU", "value": "1"}]
    log_opts = containers[0]["logConfiguration"]["options"]
    assert log_opts["awslogs-group"] == "/ecs/fund/models"
    assert log_opts["awslogs-stream-prefix"] == "tide"


def test_build_work_pool_template_populates_empty_containers_list() -> None:
    template = _minimal_template()
    template["job_configuration"] = {"task_definition": {"containerDefinitions": []}}

    result = build_work_pool_template(
        template=template,
        cluster="test-cluster",
        aws_credentials_block_id="block-id",
        task_definition_arn="arn:aws:ecs:us-east-1:123:task-definition/tide:1",
        network=_default_network(),
    )

    containers = result["job_configuration"]["task_definition"]["containerDefinitions"]
    assert len(containers) == 1
    assert "resourceRequirements" in containers[0]
