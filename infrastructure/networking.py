import pulumi
import pulumi_aws as aws
from config import (
    availability_zone_a,
    availability_zone_b,
    region,
    tags,
)
from notifications import infrastructure_alerts_topic

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
aws.ec2.VpcEndpoint(
    "s3_gateway_endpoint",
    vpc_id=vpc.id,
    service_name=pulumi.Output.concat("com.amazonaws.", region, ".s3"),
    vpc_endpoint_type="Gateway",
    route_table_ids=[private_route_table.id],
    tags=tags,
)

# ECR API Interface Endpoint
aws.ec2.VpcEndpoint(
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
aws.ec2.VpcEndpoint(
    "ecr_dkr_endpoint",
    vpc_id=vpc.id,
    service_name=pulumi.Output.concat("com.amazonaws.", region, ".ecr.dkr"),
    vpc_endpoint_type="Interface",
    subnet_ids=[private_subnet_1.id, private_subnet_2.id],
    security_group_ids=[vpc_endpoints_security_group.id],
    private_dns_enabled=True,
    tags=tags,
)
