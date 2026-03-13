from pathlib import Path

REPOSITORY_ROOT = Path(__file__).resolve().parents[3]
INFRASTRUCTURE_IAM_PATH = REPOSITORY_ROOT / "infrastructure" / "iam.py"


def load_infrastructure_iam() -> str:
    return INFRASTRUCTURE_IAM_PATH.read_text(encoding="utf-8")


def test_iam_does_not_use_administrator_access() -> None:
    infrastructure_iam = load_infrastructure_iam()

    assert "AdministratorAccess" not in infrastructure_iam


def test_iam_attaches_custom_github_actions_policy() -> None:
    infrastructure_iam = load_infrastructure_iam()

    assert '"github_actions_infrastructure_policy"' in infrastructure_iam
    assert (
        "managed_policy_arns=[github_actions_infrastructure_policy.arn]"
        in infrastructure_iam
    )


def test_iam_scopes_oidc_provider_creation_statement() -> None:
    infrastructure_iam = load_infrastructure_iam()

    assert '"CreateGithubActionsOIDCProvider"' in infrastructure_iam
    assert '"Resource": args[3]' in infrastructure_iam
    assert '"CreateIamResourcesForOscmStack"' not in infrastructure_iam
