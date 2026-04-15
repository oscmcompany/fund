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
            },
            {
                "rulePriority": 2,
                "description": "Keep last 10 tagged images",
                "selection": {
                    "tagStatus": "tagged",
                    "tagPatternList": ["git-*"],
                    "countType": "imageCountMoreThan",
                    "countNumber": 10,
                },
                "action": {"type": "expire"},
            },
        ]
    }
)

# S3 Data Bucket for storing equity bars, predictions, portfolios
data_bucket = aws.s3.Bucket(
    "data_bucket",
    bucket=pulumi.Output.concat("fund-data-", random_suffix),
    opts=pulumi.ResourceOptions(retain_on_delete=True),
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
model_artifacts_bucket = aws.s3.Bucket(
    "model_artifacts_bucket",
    bucket=pulumi.Output.concat("fund-model-artifacts-", random_suffix),
    opts=pulumi.ResourceOptions(retain_on_delete=True),
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
    name="fund/applications-data-manager-server",
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
    name="fund/applications-portfolio-manager-server",
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
    name="fund/applications-ensemble-manager-server",
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

tide_runner_repository = aws.ecr.Repository(
    "tide_runner_repository",
    name="fund/models-tide-runner",
    image_tag_mutability="MUTABLE",
    force_delete=True,
    image_scanning_configuration=aws.ecr.RepositoryImageScanningConfigurationArgs(
        scan_on_push=True,
    ),
    tags=tags,
)

aws.ecr.LifecyclePolicy(
    "tide_runner_repository_lifecycle",
    repository=tide_runner_repository.name,
    policy=_ecr_lifecycle_policy,
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
tide_runner_image_uri = tide_runner_repository.repository_url.apply(
    lambda url: f"{url}:latest"
)
