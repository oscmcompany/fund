import json
from typing import cast

import pulumi
import pulumi_aws as aws


def require_secret_config_object(
    config: pulumi.Config,
    key: str,
) -> pulumi.Output[dict[str, str]]:
    config_full_key = config.full_key(key)
    if not pulumi.runtime.is_config_secret(config_full_key):
        message = f"Pulumi config '{key}' must be configured as a secret object."
        raise ValueError(message)

    return cast(
        "pulumi.Output[dict[str, str]]",
        config.require_secret_object(key),
    )


def serialize_secret_config_object(
    secret_values: dict[str, str],
    config_key: str,
    required_keys: set[str],
) -> str:
    missing_secret_keys = sorted(required_keys.difference(secret_values))
    if missing_secret_keys:
        message = (
            f"Pulumi config '{config_key}' is missing required keys: "
            f"{', '.join(missing_secret_keys)}."
        )
        raise ValueError(message)

    return json.dumps(secret_values, sort_keys=True)


stack_name = pulumi.get_stack()
if stack_name != "production":
    message = "Only the production Pulumi stack is supported."
    raise ValueError(message)

stack_config = pulumi.Config("fund")
aws_config = pulumi.Config("aws")

region = aws_config.require("region")

random_suffix = stack_config.require_secret("randomSuffix")

github_actions_role_name = stack_config.require("githubActionsRoleName")
github_repository = stack_config.require("githubRepository")
github_branch = stack_config.require("githubBranch")
github_workflow_files = cast(
    "list[str]",
    stack_config.require_object("githubWorkflowFiles"),
)
if not github_workflow_files:
    message = (
        "Pulumi config 'githubWorkflowFiles' must include at least one workflow file."
    )
    raise ValueError(message)

budget_alert_email_addresses_full_key = stack_config.full_key(
    "budgetAlertEmailAddresses"
)
if not pulumi.runtime.is_config_secret(budget_alert_email_addresses_full_key):
    message = (
        "Pulumi config 'budgetAlertEmailAddresses' must be configured as a secret list."
    )
    raise ValueError(message)
budget_alert_email_addresses = cast(
    "list[str]",
    stack_config.require_object("budgetAlertEmailAddresses"),
)
if not budget_alert_email_addresses:
    message = (
        "Pulumi config 'budgetAlertEmailAddresses' must include at least one email "
        "address."
    )
    raise ValueError(message)

monthly_budget_limit_usd = stack_config.require_float("monthlyBudgetLimitUsd")

prefect_allowed_cidrs = cast(
    "list[str]",
    stack_config.require_object("prefectAllowedCidrs"),
)
if not prefect_allowed_cidrs:
    message = (
        "Pulumi config 'prefectAllowedCidrs' must include at least one CIDR block."
    )
    raise ValueError(message)

prefect_allowed_ipv4_cidrs = [c for c in prefect_allowed_cidrs if ":" not in c]
prefect_allowed_ipv6_cidrs = [c for c in prefect_allowed_cidrs if ":" in c]

training_notification_sender_email = stack_config.require_secret(
    "trainingNotificationSenderEmail"
)
training_notification_recipient_emails = stack_config.require_secret(
    "trainingNotificationRecipientEmails"
)

data_manager_secret_name = stack_config.require_secret("datamanagerSecretName")
portfolio_manager_secret_name = stack_config.require_secret(
    "portfoliomanagerSecretName"
)
shared_secret_name = stack_config.require_secret("sharedSecretName")

data_manager_secret_values = require_secret_config_object(
    stack_config,
    "datamanagerSecretValue",
)
portfolio_manager_secret_values = require_secret_config_object(
    stack_config,
    "portfoliomanagerSecretValue",
)
shared_secret_values = require_secret_config_object(
    stack_config,
    "sharedSecretValue",
)

github_oidc_audience_claim = "token.actions.githubusercontent.com:aud"
github_oidc_repository_claim = "token.actions.githubusercontent.com:repository"
github_oidc_ref_claim = "token.actions.githubusercontent.com:ref"
github_oidc_sub_claim = "token.actions.githubusercontent.com:sub"
github_oidc_workflow_ref_claim = "token.actions.githubusercontent.com:job_workflow_ref"

github_workflow_refs = [
    (
        f"{github_repository}/.github/workflows/{github_workflow_file}"
        f"@refs/heads/{github_branch}"
    )
    for github_workflow_file in github_workflow_files
]

current_identity = aws.get_caller_identity()

account_id = current_identity.account_id

availability_zone_a = f"{region}a"
availability_zone_b = f"{region}b"

tags = {
    "project": "fund",
    "stack": stack_name,
    "manager": "pulumi",
}

github_oidc_provider_arn = pulumi.Output.concat(
    "arn:aws:iam::",
    account_id,
    ":oidc-provider/token.actions.githubusercontent.com",
)
