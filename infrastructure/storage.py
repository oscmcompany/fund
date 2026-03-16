import json

import pulumi
import pulumi_aws as aws
from config import random_suffix, tags

_ecr_lifecycle_policy = json.dumps(
    {
        "rules": [
            {
                "rulePriority": 1,
                "description": "Expire untagged images immediately",
                "selection": {
                    "tagStatus": "untagged",
                    "countType": "sinceImagePushed",
                    "countUnit": "days",
                    "countNumber": 1,
                },
                "action": {"type": "expire"},
            }
        ]
    }
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
    name="fund/data_manager-server",
    image_tag_mutability="MUTABLE",
    force_delete=True,
    image_scanning_configuration=aws.ecr.RepositoryImageScanningConfigurationArgs(
        scan_on_push=True,
    ),
    tags=tags,
)

aws.ecr.LifecyclePolicy(
    "data_manager_repository_lifecycle",
    repository=data_manager_repository.name,
    policy=_ecr_lifecycle_policy,
)

portfolio_manager_repository = aws.ecr.Repository(
    "portfolio_manager_repository",
    name="fund/portfolio_manager-server",
    image_tag_mutability="MUTABLE",
    force_delete=True,
    image_scanning_configuration=aws.ecr.RepositoryImageScanningConfigurationArgs(
        scan_on_push=True,
    ),
    tags=tags,
)

aws.ecr.LifecyclePolicy(
    "portfolio_manager_repository_lifecycle",
    repository=portfolio_manager_repository.name,
    policy=_ecr_lifecycle_policy,
)

ensemble_manager_repository = aws.ecr.Repository(
    "ensemble_manager_repository",
    name="fund/ensemble_manager-server",
    image_tag_mutability="MUTABLE",
    force_delete=True,
    image_scanning_configuration=aws.ecr.RepositoryImageScanningConfigurationArgs(
        scan_on_push=True,
    ),
    tags=tags,
)

aws.ecr.LifecyclePolicy(
    "ensemble_manager_repository_lifecycle",
    repository=ensemble_manager_repository.name,
    policy=_ecr_lifecycle_policy,
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

aws.ecr.LifecyclePolicy(
    "tide_trainer_repository_lifecycle",
    repository=tide_trainer_repository.name,
    policy=_ecr_lifecycle_policy,
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

aws.ecr.LifecyclePolicy(
    "training_server_repository_lifecycle",
    repository=training_server_repository.name,
    policy=_ecr_lifecycle_policy,
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

aws.ecr.LifecyclePolicy(
    "training_worker_repository_lifecycle",
    repository=training_worker_repository.name,
    policy=_ecr_lifecycle_policy,
)

mlflow_repository = aws.ecr.Repository(
    "mlflow_repository",
    name="fund/mlflow-server",
    image_tag_mutability="MUTABLE",
    force_delete=True,
    image_scanning_configuration=aws.ecr.RepositoryImageScanningConfigurationArgs(
        scan_on_push=True,
    ),
    tags=tags,
)

aws.ecr.LifecyclePolicy(
    "mlflow_repository_lifecycle",
    repository=mlflow_repository.name,
    policy=_ecr_lifecycle_policy,
)

grafana_repository = aws.ecr.Repository(
    "grafana_repository",
    name="fund/grafana",
    image_tag_mutability="MUTABLE",
    force_delete=True,
    image_scanning_configuration=aws.ecr.RepositoryImageScanningConfigurationArgs(
        scan_on_push=True,
    ),
    tags=tags,
)

aws.ecr.LifecyclePolicy(
    "grafana_repository_lifecycle",
    repository=grafana_repository.name,
    policy=_ecr_lifecycle_policy,
)

# S3 MLflow Artifacts Bucket
mlflow_artifacts_bucket = aws.s3.BucketV2(
    "mlflow_artifacts_bucket",
    bucket=pulumi.Output.concat("fund-mlflow-artifacts-", random_suffix),
    tags=tags,
)

aws.s3.BucketServerSideEncryptionConfiguration(
    "mlflow_artifacts_bucket_encryption",
    bucket=mlflow_artifacts_bucket.id,
    rules=[
        aws.s3.BucketServerSideEncryptionConfigurationRuleArgs(
            apply_server_side_encryption_by_default=aws.s3.BucketServerSideEncryptionConfigurationRuleApplyServerSideEncryptionByDefaultArgs(
                sse_algorithm="AES256",
            ),
        )
    ],
)

aws.s3.BucketPublicAccessBlock(
    "mlflow_artifacts_bucket_public_access_block",
    bucket=mlflow_artifacts_bucket.id,
    block_public_acls=True,
    block_public_policy=True,
    ignore_public_acls=True,
    restrict_public_buckets=True,
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
mlflow_image_uri = mlflow_repository.repository_url.apply(
    lambda url: f"{url}:latest"
)
grafana_image_uri = grafana_repository.repository_url.apply(
    lambda url: f"{url}:latest"
)
