import re
from pathlib import Path

REPOSITORY_ROOT = Path(__file__).resolve().parents[3]
INFRASTRUCTURE_CONFIG_PATH = REPOSITORY_ROOT / "infrastructure" / "config.py"
PRODUCTION_STACK_CONFIG_PATH = (
    REPOSITORY_ROOT / "infrastructure" / "Pulumi.production.yaml"
)


def load_infrastructure_config() -> str:
    return INFRASTRUCTURE_CONFIG_PATH.read_text(encoding="utf-8")


def load_production_stack_config() -> str:
    return PRODUCTION_STACK_CONFIG_PATH.read_text(encoding="utf-8")


def test_production_stack_config_stores_region_as_plaintext() -> None:
    production_stack_config = load_production_stack_config()

    assert "aws:region:" in production_stack_config
    assert re.search(r"aws:region:\s+[a-z]+-[a-z]+-\d+", production_stack_config)
    assert not re.search(r"aws:region:\s+secure:", production_stack_config)


def test_production_stack_config_stores_budget_alert_emails_as_secret() -> None:
    production_stack_config = load_production_stack_config()

    assert "fund:budgetAlertEmailAddresses:" in production_stack_config
    assert re.search(
        r"fund:budgetAlertEmailAddresses:\s+secure:", production_stack_config
    )


def test_config_contains_oidc_claim_constants() -> None:
    infrastructure_config = load_infrastructure_config()

    assert (
        'github_oidc_repository_claim = "token.actions.githubusercontent.com:'
        'repository"' in infrastructure_config
    )
    assert (
        'github_oidc_ref_claim = "token.actions.githubusercontent.com:ref"'
        in infrastructure_config
    )
    assert (
        'github_oidc_workflow_ref_claim = "token.actions.githubusercontent.com:'
        'job_workflow_ref"' in infrastructure_config
    )
    assert (
        'github_oidc_sub_claim = "token.actions.githubusercontent.com:sub"'
        in infrastructure_config
    )
