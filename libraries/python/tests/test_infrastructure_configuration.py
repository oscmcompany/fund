import re
from pathlib import Path

REPOSITORY_ROOT = Path(__file__).resolve().parents[3]
INFRASTRUCTURE_ENTRYPOINT_PATH = REPOSITORY_ROOT / "infrastructure" / "__main__.py"
PRODUCTION_STACK_CONFIG_PATH = (
    REPOSITORY_ROOT / "infrastructure" / "Pulumi.production.yaml"
)


def load_infrastructure_entrypoint() -> str:
    return INFRASTRUCTURE_ENTRYPOINT_PATH.read_text(encoding="utf-8")


def load_production_stack_config() -> str:
    return PRODUCTION_STACK_CONFIG_PATH.read_text(encoding="utf-8")


def test_production_stack_config_stores_region_as_secret() -> None:
    production_stack_config = load_production_stack_config()

    assert "aws:region:" in production_stack_config
    assert re.search(r"aws:region:\s+secure:", production_stack_config)


def test_production_stack_config_stores_budget_alert_emails_as_secret() -> None:
    production_stack_config = load_production_stack_config()

    assert "fund:budgetAlertEmailAddresses:" in production_stack_config
    assert re.search(
        r"fund:budgetAlertEmailAddresses:\s+secure:", production_stack_config
    )


def test_infrastructure_entrypoint_contains_oidc_claim_constraints() -> None:
    infrastructure_entrypoint = load_infrastructure_entrypoint()

    assert (
        'github_oidc_repository_claim = "token.actions.githubusercontent.com:'
        'repository"' in infrastructure_entrypoint
    )
    assert (
        'github_oidc_ref_claim = "token.actions.githubusercontent.com:ref"'
        in infrastructure_entrypoint
    )
    assert (
        'github_oidc_workflow_ref_claim = "token.actions.githubusercontent.com:'
        'job_workflow_ref"' in infrastructure_entrypoint
    )
    assert (
        'github_oidc_sub_claim = "token.actions.githubusercontent.com:sub"'
        in infrastructure_entrypoint
    )


def test_infrastructure_entrypoint_does_not_use_administrator_access() -> None:
    infrastructure_entrypoint = load_infrastructure_entrypoint()

    assert "AdministratorAccess" not in infrastructure_entrypoint


def test_infrastructure_entrypoint_contains_nat_gateway_baseline_alarm() -> None:
    infrastructure_entrypoint = load_infrastructure_entrypoint()

    assert '"nat_gateway_bytes_out_to_destination_alarm"' in infrastructure_entrypoint
    assert 'metric_name="BytesOutToDestination"' in infrastructure_entrypoint
    assert "threshold=500_000_000" in infrastructure_entrypoint
    assert "period=3600" in infrastructure_entrypoint
    assert "evaluation_periods=2" in infrastructure_entrypoint


def test_infrastructure_entrypoint_contains_s3_bucket_encryption_resources() -> None:
    infrastructure_entrypoint = load_infrastructure_entrypoint()

    assert '"data_bucket_encryption"' in infrastructure_entrypoint
    assert '"model_artifacts_bucket_encryption"' in infrastructure_entrypoint


def test_infrastructure_entrypoint_attaches_custom_github_actions_policy() -> None:
    infrastructure_entrypoint = load_infrastructure_entrypoint()

    assert '"github_actions_infrastructure_policy"' in infrastructure_entrypoint
    assert (
        '"github_actions_infrastructure_role_custom_policy"'
        in infrastructure_entrypoint
    )


def test_infrastructure_entrypoint_contains_s3_public_access_block_resources() -> None:
    infrastructure_entrypoint = load_infrastructure_entrypoint()

    assert '"data_bucket_public_access_block"' in infrastructure_entrypoint
    assert '"model_artifacts_bucket_public_access_block"' in infrastructure_entrypoint


def test_infrastructure_entrypoint_scopes_oidc_provider_creation_statement() -> None:
    infrastructure_entrypoint = load_infrastructure_entrypoint()

    assert '"CreateGithubActionsOIDCProvider"' in infrastructure_entrypoint
    assert '"Resource": github_oidc_provider_arn' in infrastructure_entrypoint
    assert '"CreateIamResourcesForOscmStack"' not in infrastructure_entrypoint
