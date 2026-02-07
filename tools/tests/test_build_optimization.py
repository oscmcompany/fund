"""Tests for build and deployment optimization logic."""
import re
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parent.parent.parent
MASKFILE = REPO_ROOT / "maskfile.md"


def _read_maskfile() -> str:
    """Read the maskfile.md content."""
    return MASKFILE.read_text()


def _extract_build_command() -> str:
    """Extract the build command bash block from maskfile."""
    content = _read_maskfile()
    match = re.search(
        r"#### build \(application_name\) \(stage_name\)\n.*?```bash\n(.*?)```",
        content,
        re.DOTALL,
    )
    assert match is not None, "Could not find build command"
    return match.group(1)


def _extract_push_command() -> str:
    """Extract the push command bash block from maskfile."""
    content = _read_maskfile()
    match = re.search(
        r"#### push \(application_name\) \(stage_name\)\n.*?```bash\n(.*?)```",
        content,
        re.DOTALL,
    )
    assert match is not None, "Could not find push command"
    return match.group(1)


# --- Build Cache Configuration ---


def test_build_cache_configuration_gha_cache_has_service_scope() -> None:
    build_cmd = _extract_build_command()
    assert "scope=" in build_cmd, "GHA cache must have scope parameter"
    assert "${application_name}" in build_cmd or "application_name" in build_cmd, \
        "Scope must include application name for per-service isolation"


def test_build_cache_configuration_uses_hybrid_cache_in_gha() -> None:
    build_cmd = _extract_build_command()
    assert "type=gha" in build_cmd, "Must use GHA cache in CI"
    assert "type=registry" in build_cmd, "Must use registry cache as fallback"


def test_build_cache_configuration_cache_to_uses_max_mode() -> None:
    build_cmd = _extract_build_command()
    cache_to_lines = [line for line in build_cmd.split('\n') if 'cache-to' in line.lower() or 'cache_to' in line.lower()]
    for line in cache_to_lines:
        if "type=gha" in line or "type=registry" in line:
            assert "mode=max" in line, f"Cache-to must use mode=max: {line}"


def test_build_cache_configuration_buildx_not_created_in_gha() -> None:
    build_cmd = _extract_build_command()
    # Should conditionally skip buildx creation in GHA
    assert "GITHUB_ACTIONS" in build_cmd, "Must check for GHA environment"


def test_build_cache_configuration_has_ecr_login_for_cache() -> None:
    build_cmd = _extract_build_command()
    assert "ecr get-login-password" in build_cmd, "Must log into ECR to pull cache"


def test_build_cache_configuration_uses_load_flag() -> None:
    build_cmd = _extract_build_command()
    assert "--load" in build_cmd, "Must use --load to make image available locally"


def test_build_cache_configuration_aws_region_validated() -> None:
    build_cmd = _extract_build_command()
    assert "AWS_REGION" in build_cmd, "Must use AWS_REGION"
    assert 'exit 1' in build_cmd, "Must exit if AWS_REGION is not set"


# --- Push Optimization ---


def test_push_optimization_checks_existing_image() -> None:
    push_cmd = _extract_push_command()
    has_check = any(term in push_cmd for term in [
        "describe-images",
        "batch-get-image",
        "content_hash",
        "rev-parse",
        "manifest inspect",
    ])
    assert has_check, "Push must check if image already exists in ECR"


def test_push_optimization_can_skip_push() -> None:
    push_cmd = _extract_push_command()
    assert "skip" in push_cmd.lower() or "already" in push_cmd.lower(), \
        "Push must have logic to skip when image is unchanged"


def test_push_optimization_has_ecr_login() -> None:
    push_cmd = _extract_push_command()
    assert "ecr get-login-password" in push_cmd, "Must log into ECR"


def test_push_optimization_pushes_latest_tag() -> None:
    push_cmd = _extract_push_command()
    assert ":latest" in push_cmd, "Must push with latest tag"


def test_push_optimization_has_error_handling() -> None:
    push_cmd = _extract_push_command()
    assert "set -euo pipefail" in push_cmd, "Must have strict error handling"


# --- Git Integration ---


def test_git_integration_push_tags_with_git_commit() -> None:
    push_cmd = _extract_push_command()
    has_git = "git" in push_cmd.lower() and ("rev-parse" in push_cmd or "commit" in push_cmd.lower())
    assert has_git, "Push should tag images with git commit hash for traceability"


# --- Rust CI Optimization ---


def test_rust_ci_optimization_ci_does_not_update_deps() -> None:
    content = _read_maskfile()
    ci_match = re.search(r"#### ci\n.*?```bash\n(.*?)```", content, re.DOTALL)
    assert ci_match is not None
    assert "cargo update" not in ci_match.group(1), "CI must not run cargo update"


def test_rust_ci_optimization_ci_checks_formatting_without_modifying() -> None:
    content = _read_maskfile()
    ci_match = re.search(r"#### ci\n.*?```bash\n(.*?)```", content, re.DOTALL)
    assert ci_match is not None
    ci_code = ci_match.group(1)
    assert "fmt" in ci_code and "--check" in ci_code, \
        "CI must check formatting without modifying files"


def test_rust_ci_optimization_ci_clippy_denies_warnings() -> None:
    content = _read_maskfile()
    ci_match = re.search(r"#### ci\n.*?```bash\n(.*?)```", content, re.DOTALL)
    assert ci_match is not None
    ci_code = ci_match.group(1)
    assert "clippy" in ci_code and "-D warnings" in ci_code, \
        "CI clippy must deny all warnings"


def test_rust_ci_optimization_ci_runs_tests() -> None:
    content = _read_maskfile()
    ci_match = re.search(r"#### ci\n.*?```bash\n(.*?)```", content, re.DOTALL)
    assert ci_match is not None
    assert "cargo test" in ci_match.group(1), "CI must run tests"


def test_rust_ci_optimization_ci_is_subset_of_all() -> None:
    """CI should cover the essential checks from 'all' without the extras."""
    content = _read_maskfile()
    ci_match = re.search(r"#### ci\n.*?```bash\n(.*?)```", content, re.DOTALL)
    assert ci_match is not None
    ci_code = ci_match.group(1)
    # CI should have: fmt, clippy (replaces check+lint), test
    assert "fmt" in ci_code, "CI must include formatting"
    assert "clippy" in ci_code, "CI must include clippy"
    assert "test" in ci_code, "CI must include tests"


# --- Dockerignore ---


def test_dockerignore_exists() -> None:
    path = REPO_ROOT / ".dockerignore"
    assert path.exists()


def test_dockerignore_excludes_venv() -> None:
    content = (REPO_ROOT / ".dockerignore").read_text()
    assert ".venv" in content, "Must exclude .venv from Docker context"


def test_dockerignore_excludes_pycache() -> None:
    content = (REPO_ROOT / ".dockerignore").read_text()
    assert "__pycache__" in content, "Must exclude __pycache__"
