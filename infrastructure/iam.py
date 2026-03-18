import json
from secrets import (
    data_manager_secret,
    data_manager_secret_name,
    portfolio_manager_secret,
    portfolio_manager_secret_name,
    shared_secret,
    shared_secret_name,
)

import pulumi
import pulumi_aws as aws
from config import (
    account_id,
    github_actions_role_name,
    github_branch,
    github_oidc_audience_claim,
    github_oidc_provider_arn,
    github_oidc_ref_claim,
    github_oidc_repository_claim,
    github_oidc_sub_claim,
    github_oidc_workflow_ref_claim,
    github_repository,
    github_workflow_refs,
    region,
    stack_name,
    tags,
    training_notification_recipient_emails,
    training_notification_sender_email,
)
from storage import data_bucket, model_artifacts_bucket

github_actions_oidc_provider = aws.iam.OpenIdConnectProvider(
    "github_actions_oidc_provider",
    url="https://token.actions.githubusercontent.com",
    client_id_lists=["sts.amazonaws.com"],
    tags=tags,
)

github_actions_infrastructure_policy = aws.iam.Policy(
    "github_actions_infrastructure_policy",
    name="fund-github-actions-infrastructure-policy",
    description=(
        "Least-privilege policy for GitHub Actions infrastructure deployments."
    ),
    policy=pulumi.Output.all(
        data_manager_secret_name,
        portfolio_manager_secret_name,
        shared_secret_name,
        github_oidc_provider_arn,
    ).apply(
        lambda args: json.dumps(
            {
                "Version": "2012-10-17",
                "Statement": [
                    # These list/describe APIs are account-scoped and require wildcard
                    # resources.
                    {
                        "Sid": "ReadGlobalMetadata",
                        "Effect": "Allow",
                        "Action": [
                            "sts:GetCallerIdentity",
                            "tag:GetResources",
                            "tag:GetTagKeys",
                            "tag:GetTagValues",
                            "iam:Get*",
                            "iam:List*",
                            "ec2:Describe*",
                            "ecs:Describe*",
                            "ecs:List*",
                            "elasticloadbalancing:Describe*",
                            "ecr:Describe*",
                            "ecr:ListTagsForResource",
                            "s3:GetBucketLocation",
                            "s3:ListAllMyBuckets",
                            "ssm:DescribeParameters",
                            "secretsmanager:ListSecrets",
                            "logs:Describe*",
                            "cloudwatch:Describe*",
                            "cloudwatch:Get*",
                            "sns:Get*",
                            "sns:List*",
                            "budgets:Describe*",
                            "budgets:ViewBudget",
                            "servicediscovery:Get*",
                            "servicediscovery:List*",
                        ],
                        "Resource": "*",
                    },
                    # These control-plane APIs rely on generated identifiers and do not
                    # support practical resource-level scoping for stack create/update/
                    # delete operations.
                    {
                        "Sid": "ManageEC2ECSELBBudgetsAndServiceDiscovery",
                        "Effect": "Allow",
                        "Action": [
                            "ec2:*",
                            "ecs:*",
                            "elasticloadbalancing:*",
                            "budgets:*",
                            "servicediscovery:*",
                        ],
                        "Resource": "*",
                    },
                    # CreateRepository/GetAuthorizationToken require wildcard resources.
                    {
                        "Sid": "CreateAndAuthenticateECRRepositories",
                        "Effect": "Allow",
                        "Action": [
                            "ecr:CreateRepository",
                            "ecr:GetAuthorizationToken",
                        ],
                        "Resource": "*",
                    },
                    {
                        "Sid": "ManageECRRepositories",
                        "Effect": "Allow",
                        "Action": "ecr:*",
                        "Resource": (
                            f"arn:aws:ecr:{region}:{account_id}:repository/fund/*"
                        ),
                    },
                    # CreateBucket requires wildcard resources.
                    {
                        "Sid": "CreateBuckets",
                        "Effect": "Allow",
                        "Action": "s3:CreateBucket",
                        "Resource": "*",
                    },
                    {
                        "Sid": "ManageBuckets",
                        "Effect": "Allow",
                        "Action": "s3:*",
                        "Resource": [
                            "arn:aws:s3:::fund-data-*",
                            "arn:aws:s3:::fund-data-*/*",
                            "arn:aws:s3:::fund-model-artifacts-*",
                            "arn:aws:s3:::fund-model-artifacts-*/*",
                        ],
                    },
                    # CreateSecret requires wildcard resources before an ARN exists.
                    {
                        "Sid": "CreateSecrets",
                        "Effect": "Allow",
                        "Action": "secretsmanager:CreateSecret",
                        "Resource": "*",
                    },
                    {
                        "Sid": "ManageConfiguredSecrets",
                        "Effect": "Allow",
                        "Action": "secretsmanager:*",
                        "Resource": [
                            f"arn:aws:secretsmanager:{region}:{account_id}:secret:{args[0]}*",
                            f"arn:aws:secretsmanager:{region}:{account_id}:secret:{args[1]}*",
                            f"arn:aws:secretsmanager:{region}:{account_id}:secret:{args[2]}*",
                        ],
                    },
                    {
                        "Sid": "ManageParameters",
                        "Effect": "Allow",
                        "Action": "ssm:*",
                        "Resource": (
                            f"arn:aws:ssm:{region}:{account_id}:parameter/fund/*"
                        ),
                    },
                    {
                        "Sid": "ManageLogGroups",
                        "Effect": "Allow",
                        "Action": "logs:*",
                        "Resource": [
                            f"arn:aws:logs:{region}:{account_id}:log-group:/ecs/fund/*",
                            f"arn:aws:logs:{region}:{account_id}:log-group:/ecs/fund/*:*",
                        ],
                    },
                    # Alarm mutation APIs require wildcard resources.
                    {
                        "Sid": "ManageAlarms",
                        "Effect": "Allow",
                        "Action": [
                            "cloudwatch:DeleteAlarms",
                            "cloudwatch:ListTagsForResource",
                            "cloudwatch:PutMetricAlarm",
                            "cloudwatch:TagResource",
                            "cloudwatch:UntagResource",
                        ],
                        "Resource": "*",
                    },
                    # CreateTopic requires wildcard resources.
                    {
                        "Sid": "CreateInfrastructureAlertsTopic",
                        "Effect": "Allow",
                        "Action": "sns:CreateTopic",
                        "Resource": "*",
                    },
                    {
                        "Sid": "ManageInfrastructureAlertsTopic",
                        "Effect": "Allow",
                        "Action": "sns:*",
                        "Resource": [
                            f"arn:aws:sns:{region}:{account_id}:fund-infrastructure-alerts",
                            f"arn:aws:sns:{region}:{account_id}:fund-infrastructure-alerts:*",
                        ],
                    },
                    {
                        "Sid": "ManageSESIdentities",
                        "Effect": "Allow",
                        "Action": [
                            "ses:CreateEmailIdentity",
                            "ses:DeleteEmailIdentity",
                            "ses:GetEmailIdentity",
                            "ses:TagResource",
                            "ses:UntagResource",
                            "ses:ListTagsForResource",
                        ],
                        "Resource": [
                            f"arn:aws:ses:{region}:{account_id}:identity/*",
                        ],
                    },
                    {
                        "Sid": "CreateGithubActionsOIDCProvider",
                        "Effect": "Allow",
                        "Action": "iam:CreateOpenIDConnectProvider",
                        "Resource": args[3],
                    },
                    # CreateRole uses wildcard resources by API design.
                    {
                        "Sid": "CreateRoles",
                        "Effect": "Allow",
                        "Action": "iam:CreateRole",
                        "Resource": "*",
                        "Condition": {
                            "StringEquals": {
                                "iam:RoleName": [
                                    "fund-ecs-execution-role",
                                    "fund-ecs-task-role",
                                    github_actions_role_name,
                                ]
                            }
                        },
                    },
                    # CreatePolicy uses wildcard resources by API design.
                    {
                        "Sid": "CreatePolicies",
                        "Effect": "Allow",
                        "Action": "iam:CreatePolicy",
                        "Resource": "*",
                        "Condition": {
                            "StringLike": {
                                "iam:PolicyName": "fund-*",
                            }
                        },
                    },
                    # CreateServiceLinkedRole uses wildcard resources by API design.
                    {
                        "Sid": "CreateServiceLinkedRolesForStack",
                        "Effect": "Allow",
                        "Action": "iam:CreateServiceLinkedRole",
                        "Resource": "*",
                        "Condition": {
                            "StringEquals": {
                                "iam:AWSServiceName": [
                                    "ecs.amazonaws.com",
                                    "elasticloadbalancing.amazonaws.com",
                                ]
                            }
                        },
                    },
                    {
                        "Sid": "ManageRoles",
                        "Effect": "Allow",
                        "Action": [
                            "iam:AttachRolePolicy",
                            "iam:DeleteRole",
                            "iam:DetachRolePolicy",
                            "iam:PassRole",
                            "iam:TagRole",
                            "iam:UntagRole",
                            "iam:UpdateAssumeRolePolicy",
                        ],
                        "Resource": [
                            f"arn:aws:iam::{account_id}:role/fund-ecs-execution-role",
                            f"arn:aws:iam::{account_id}:role/fund-ecs-task-role",
                            f"arn:aws:iam::{account_id}:role/{github_actions_role_name}",
                        ],
                        "Condition": {
                            "ArnLikeIfExists": {
                                "iam:PolicyARN": [
                                    "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy",
                                    f"arn:aws:iam::{account_id}:policy/fund-*",
                                ]
                            },
                            "StringLikeIfExists": {
                                "iam:PassedToService": [
                                    "ecs-tasks.amazonaws.com",
                                    "ecs.amazonaws.com",
                                ]
                            },
                        },
                    },
                    {
                        "Sid": "ManageInlineRolePolicies",
                        "Effect": "Allow",
                        "Action": [
                            "iam:DeleteRolePolicy",
                            "iam:PutRolePolicy",
                        ],
                        "Resource": [
                            f"arn:aws:iam::{account_id}:role/fund-ecs-execution-role",
                            f"arn:aws:iam::{account_id}:role/fund-ecs-task-role",
                        ],
                        "Condition": {
                            "StringEquals": {
                                "iam:PolicyName": [
                                    "fund-ecs-execution-role-secrets-policy",
                                    "fund-ecs-task-role-s3-policy",
                                    "fund-ecs-task-role-ssm-policy",
                                    "fund-ecs-task-role-ses-policy",
                                ]
                            }
                        },
                    },
                    {
                        "Sid": "ManagePolicies",
                        "Effect": "Allow",
                        "Action": [
                            "iam:CreatePolicyVersion",
                            "iam:DeletePolicy",
                            "iam:DeletePolicyVersion",
                            "iam:SetDefaultPolicyVersion",
                            "iam:TagPolicy",
                            "iam:UntagPolicy",
                        ],
                        "Resource": f"arn:aws:iam::{account_id}:policy/fund-*",
                    },
                    {
                        "Sid": "ManageGithubActionsOIDCProvider",
                        "Effect": "Allow",
                        "Action": [
                            "iam:AddClientIDToOpenIDConnectProvider",
                            "iam:DeleteOpenIDConnectProvider",
                            "iam:RemoveClientIDFromOpenIDConnectProvider",
                            "iam:TagOpenIDConnectProvider",
                            "iam:UntagOpenIDConnectProvider",
                            "iam:UpdateOpenIDConnectProviderThumbprint",
                        ],
                        "Resource": args[3],
                    },
                    # Service-linked role teardown APIs are wildcard-resource only.
                    {
                        "Sid": "DeleteServiceLinkedRoles",
                        "Effect": "Allow",
                        "Action": [
                            "iam:DeleteServiceLinkedRole",
                            "iam:GetServiceLinkedRoleDeletionStatus",
                        ],
                        "Resource": "*",
                        "Condition": {
                            "StringLikeIfExists": {
                                "iam:AWSServiceName": [
                                    "ecs.amazonaws.com",
                                    "elasticloadbalancing.amazonaws.com",
                                ]
                            }
                        },
                    },
                ],
            },
            sort_keys=True,
        )
    ),
    opts=pulumi.ResourceOptions(retain_on_delete=True),
    tags=tags,
)

github_actions_infrastructure_role = aws.iam.Role(
    "github_actions_infrastructure_role",
    name=github_actions_role_name,
    assume_role_policy=github_actions_oidc_provider.arn.apply(
        lambda github_actions_oidc_provider_arn: json.dumps(
            {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Principal": {
                            "Federated": github_actions_oidc_provider_arn,
                        },
                        "Action": "sts:AssumeRoleWithWebIdentity",
                        "Condition": {
                            "StringEquals": {
                                github_oidc_audience_claim: "sts.amazonaws.com",
                                github_oidc_repository_claim: github_repository,
                                github_oidc_ref_claim: f"refs/heads/{github_branch}",
                                github_oidc_workflow_ref_claim: github_workflow_refs,
                            },
                            "StringLike": {
                                github_oidc_sub_claim: f"repo:{github_repository}:*",
                            },
                        },
                    }
                ],
            },
            sort_keys=True,
        )
    ),
    managed_policy_arns=[github_actions_infrastructure_policy.arn],
    opts=pulumi.ResourceOptions(retain_on_delete=True),
    tags=tags,
)

# IAM Role for ECS to perform infrastructure tasks
execution_role = aws.iam.Role(
    "execution_role",
    name="fund-ecs-execution-role",
    assume_role_policy=json.dumps(
        {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Action": "sts:AssumeRole",
                    "Effect": "Allow",
                    "Principal": {"Service": "ecs-tasks.amazonaws.com"},
                }
            ],
        },
        sort_keys=True,
    ),
    tags=tags,
)

aws.iam.RolePolicyAttachment(
    "execution_role_policy",
    role=execution_role.name,
    policy_arn="arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy",
)

# Allow ECS tasks to read secrets from Secrets Manager
aws.iam.RolePolicy(
    "execution_role_secrets_policy",
    name="fund-ecs-execution-role-secrets-policy",
    role=execution_role.id,
    policy=pulumi.Output.all(
        data_manager_secret.arn,
        portfolio_manager_secret.arn,
        shared_secret.arn,
    ).apply(
        lambda args: json.dumps(
            {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Action": ["secretsmanager:GetSecretValue"],
                        "Resource": [args[0], args[1], args[2]],
                    },
                    {
                        "Effect": "Allow",
                        "Action": [
                            "ssm:GetParameters",
                            "ssm:GetParameter",
                        ],
                        "Resource": (
                            f"arn:aws:ssm:{region}:{account_id}"
                            ":parameter/fund/*"
                        ),
                    },
                ],
            },
            sort_keys=True,
        )
    ),
)


# IAM Role for ECS tasks to access AWS resources
task_role = aws.iam.Role(
    "task_role",
    name="fund-ecs-task-role",
    assume_role_policy=json.dumps(
        {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Action": "sts:AssumeRole",
                    "Effect": "Allow",
                    "Principal": {"Service": "ecs-tasks.amazonaws.com"},
                }
            ],
        },
        sort_keys=True,
    ),
    tags=tags,
)

aws.iam.RolePolicy(
    "task_role_s3_policy",
    name="fund-ecs-task-role-s3-policy",
    role=task_role.id,
    policy=pulumi.Output.all(data_bucket.arn, model_artifacts_bucket.arn).apply(
        lambda args: json.dumps(
            {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Action": ["s3:GetObject", "s3:PutObject", "s3:ListBucket"],
                        "Resource": [
                            args[0],
                            f"{args[0]}/*",
                            args[1],
                            f"{args[1]}/*",
                        ],
                    }
                ],
            },
            sort_keys=True,
        )
    ),
)

aws.iam.RolePolicy(
    "task_role_ssm_policy",
    name="fund-ecs-task-role-ssm-policy",
    role=task_role.id,
    policy=json.dumps(
        {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Action": ["ssm:GetParameter", "ssm:GetParameters"],
                    "Resource": f"arn:aws:ssm:{region}:{account_id}:parameter/fund/*",
                }
            ],
        },
        sort_keys=True,
    ),
)

# SES Email Identity for training notifications
training_notification_email_identity = aws.ses.EmailIdentity(
    "training_notification_email_identity",
    email=training_notification_sender_email,
)

training_notification_sender_email_parameter = aws.ssm.Parameter(
    "training_notification_sender_email_parameter",
    name=f"/fund/{stack_name}/training/notification-sender-email",
    type="SecureString",
    value=training_notification_sender_email,
    tags=tags,
)

training_notification_recipients_parameter = aws.ssm.Parameter(
    "training_notification_recipients_parameter",
    name=f"/fund/{stack_name}/training/notification-recipients",
    type="SecureString",
    value=training_notification_recipient_emails,
    tags=tags,
)

# Allow ECS tasks to send emails via SES
aws.iam.RolePolicy(
    "task_role_ses_policy",
    name="fund-ecs-task-role-ses-policy",
    role=task_role.id,
    policy=training_notification_email_identity.arn.apply(
        lambda identity_arn: json.dumps(
            {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Action": ["ses:SendEmail", "ses:SendRawEmail"],
                        "Resource": identity_arn,
                    }
                ],
            },
            sort_keys=True,
        )
    ),
)
