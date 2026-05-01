import json
import textwrap

import pulumi
import pulumi_aws as aws
from compute import cluster, ensemble_manager_service
from config import account_id, region, tags
from storage import model_artifacts_bucket

# Enable EventBridge notifications on the model artifacts bucket so S3
# publishes "Object Created" events to the default event bus.
aws.s3.BucketNotification(
    "model_artifacts_eventbridge_notification",
    bucket=model_artifacts_bucket.id,
    eventbridge=True,
)

# IAM role for the redeployment Lambda function.
redeployment_lambda_role = aws.iam.Role(
    "redeployment_lambda_role",
    name="fund-redeployment-lambda-role",
    assume_role_policy=json.dumps(
        {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Action": "sts:AssumeRole",
                    "Effect": "Allow",
                    "Principal": {"Service": "lambda.amazonaws.com"},
                }
            ],
        },
        sort_keys=True,
    ),
    tags=tags,
)

# Inline policy: allow ecs:UpdateService on the ensemble-manager service and
# CloudWatch Logs write access for the Lambda log group.
aws.iam.RolePolicy(
    "redeployment_lambda_policy",
    name="fund-redeployment-lambda-policy",
    role=redeployment_lambda_role.id,
    policy=ensemble_manager_service.id.apply(
        lambda service_arn: json.dumps(
            {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Action": "ecs:UpdateService",
                        "Resource": service_arn,
                    },
                    {
                        "Effect": "Allow",
                        "Action": [
                            "logs:CreateLogGroup",
                            "logs:CreateLogStream",
                            "logs:PutLogEvents",
                        ],
                        "Resource": (
                            f"arn:aws:logs:{region}:{account_id}"
                            ":log-group:/aws/lambda/fund-redeploy-ensemble-manager*"
                        ),
                    },
                ],
            },
            sort_keys=True,
        )
    ),
)

# CloudWatch log group for the Lambda function.
aws.cloudwatch.LogGroup(
    "redeployment_lambda_logs",
    name="/aws/lambda/fund-redeploy-ensemble-manager",
    retention_in_days=7,
    tags=tags,
)

# Inline Lambda handler that validates the S3 key prefix and forces a new
# deployment on the ensemble-manager ECS service.
_handler_code = textwrap.dedent("""\
    import json
    import os

    import boto3

    ecs = boto3.client("ecs")

    CLUSTER = os.environ["ECS_CLUSTER_NAME"]
    SERVICE = os.environ["ECS_SERVICE_NAME"]
    REQUIRED_PREFIX = "artifacts/tide/"


    def handler(event, context):
        detail = event.get("detail", {})
        key = detail.get("object", {}).get("key", "")

        if not key.startswith(REQUIRED_PREFIX):
            print(f"Skipping key outside required prefix: {key}")
            return {"statusCode": 200, "body": "skipped"}

        print(f"New model artifact detected: {key}")
        response = ecs.update_service(
            cluster=CLUSTER,
            service=SERVICE,
            forceNewDeployment=True,
        )
        status = response["service"]["status"]
        print(f"Forced new deployment, service status: {status}")
        return {"statusCode": 200, "body": json.dumps({"status": status})}
""")

redeployment_lambda = aws.lambda_.Function(
    "redeployment_lambda",
    name="fund-redeploy-ensemble-manager",
    runtime="python3.12",
    handler="index.handler",
    memory_size=128,
    timeout=30,
    role=redeployment_lambda_role.arn,
    code=pulumi.AssetArchive({"index.py": pulumi.StringAsset(_handler_code)}),
    environment=aws.lambda_.FunctionEnvironmentArgs(
        variables={
            "ECS_CLUSTER_NAME": cluster.name,
            "ECS_SERVICE_NAME": ensemble_manager_service.name,
        },
    ),
    tags=tags,
)

# EventBridge rule: match S3 "Object Created" events for model.tar.gz files
# in the model artifacts bucket.
model_artifact_uploaded_rule = aws.cloudwatch.EventRule(
    "model_artifact_uploaded_rule",
    name="fund-model-artifact-uploaded",
    description="Triggers when a new model artifact is uploaded to S3",
    event_pattern=model_artifacts_bucket.bucket.apply(
        lambda bucket_name: json.dumps(
            {
                "source": ["aws.s3"],
                "detail-type": ["Object Created"],
                "detail": {
                    "bucket": {"name": [bucket_name]},
                    "object": {"key": [{"suffix": "model.tar.gz"}]},
                },
            },
            sort_keys=True,
        )
    ),
    tags=tags,
)

# EventBridge target: invoke the redeployment Lambda when the rule matches.
aws.cloudwatch.EventTarget(
    "model_artifact_uploaded_target",
    rule=model_artifact_uploaded_rule.name,
    arn=redeployment_lambda.arn,
)

# Allow EventBridge to invoke the Lambda function.
aws.lambda_.Permission(
    "redeployment_lambda_eventbridge_permission",
    action="lambda:InvokeFunction",
    function=redeployment_lambda.name,
    principal="events.amazonaws.com",
    source_arn=model_artifact_uploaded_rule.arn,
)
