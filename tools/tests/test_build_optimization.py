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


class TestBuildCacheConfiguration:
    """Validate Docker build caching is properly configured."""

    def test_gha_cache_has_service_scope(self) -> None:
        build_cmd = _extract_build_command()
        assert "scope=" in build_cmd, "GHA cache must have scope parameter"
        assert "${application_name}" in build_cmd or "application_name" in build_cmd, \
            "Scope must include application name for per-service isolation"

    def test_uses_hybrid_cache_in_gha(self) -> None:
        build_cmd = _extract_build_command()
        assert "type=gha" in build_cmd, "Must use GHA cache in CI"
        assert "type=registry" in build_cmd, "Must use registry cache as fallback"

    def test_cache_to_uses_max_mode(self) -> None:
        build_cmd = _extract_build_command()
        cache_to_lines = [line for line in build_cmd.split('\n') if 'cache-to' in line.lower() or 'cache_to' in line.lower()]
        for line in cache_to_lines:
            if "type=gha" in line or "type=registry" in line:
                assert "mode=max" in line, f"Cache-to must use mode=max: {line}"

    def test_buildx_not_created_in_gha(self) -> None:
        build_cmd = _extract_build_command()
        # Should conditionally skip buildx creation in GHA
        assert "GITHUB_ACTIONS" in build_cmd, "Must check for GHA environment"

    def test_has_ecr_login_for_cache(self) -> None:
        build_cmd = _extract_build_command()
        assert "ecr get-login-password" in build_cmd, "Must log into ECR to pull cache"

    def test_uses_load_flag(self) -> None:
        build_cmd = _extract_build_command()
        assert "--load" in build_cmd, "Must use --load to make image available locally"

    def test_aws_region_validated(self) -> None:
        build_cmd = _extract_build_command()
        assert "AWS_REGION" in build_cmd, "Must use AWS_REGION"
        assert 'exit 1' in build_cmd, "Must exit if AWS_REGION is not set"


class TestPushOptimization:
    """Validate push command has optimization to skip redundant pushes."""

    def test_checks_existing_image(self) -> None:
        push_cmd = _extract_push_command()
        has_check = any(term in push_cmd for term in [
            "describe-images",
            "batch-get-image",
            "content_hash",
            "rev-parse",
            "manifest inspect",
        ])
        assert has_check, "Push must check if image already exists in ECR"

    def test_can_skip_push(self) -> None:
        push_cmd = _extract_push_command()
        assert "skip" in push_cmd.lower() or "already" in push_cmd.lower(), \
            "Push must have logic to skip when image is unchanged"

    def test_has_ecr_login(self) -> None:
        push_cmd = _extract_push_command()
        assert "ecr get-login-password" in push_cmd, "Must log into ECR"

    def test_pushes_latest_tag(self) -> None:
        push_cmd = _extract_push_command()
        assert ":latest" in push_cmd, "Must push with latest tag"

    def test_has_error_handling(self) -> None:
        push_cmd = _extract_push_command()
        assert "set -euo pipefail" in push_cmd, "Must have strict error handling"


class TestGitIntegration:
    """Validate git integration for build traceability."""

    def test_push_tags_with_git_commit(self) -> None:
        push_cmd = _extract_push_command()
        has_git = "git" in push_cmd.lower() and ("rev-parse" in push_cmd or "commit" in push_cmd.lower())
        assert has_git, "Push should tag images with git commit hash for traceability"


class TestRustCiOptimization:
    """Validate Rust CI command is optimized."""

    def test_ci_does_not_update_deps(self) -> None:
        content = _read_maskfile()
        ci_match = re.search(r"#### ci\n.*?```bash\n(.*?)```", content, re.DOTALL)
        assert ci_match is not None
        assert "cargo update" not in ci_match.group(1), "CI must not run cargo update"

    def test_ci_checks_formatting_without_modifying(self) -> None:
        content = _read_maskfile()
        ci_match = re.search(r"#### ci\n.*?```bash\n(.*?)```", content, re.DOTALL)
        assert ci_match is not None
        ci_code = ci_match.group(1)
        assert "fmt" in ci_code and "--check" in ci_code, \
            "CI must check formatting without modifying files"

    def test_ci_clippy_denies_warnings(self) -> None:
        content = _read_maskfile()
        ci_match = re.search(r"#### ci\n.*?```bash\n(.*?)```", content, re.DOTALL)
        assert ci_match is not None
        ci_code = ci_match.group(1)
        assert "clippy" in ci_code and "-D warnings" in ci_code, \
            "CI clippy must deny all warnings"

    def test_ci_runs_tests(self) -> None:
        content = _read_maskfile()
        ci_match = re.search(r"#### ci\n.*?```bash\n(.*?)```", content, re.DOTALL)
        assert ci_match is not None
        assert "cargo test" in ci_match.group(1), "CI must run tests"

    def test_ci_is_subset_of_all(self) -> None:
        """CI should cover the essential checks from 'all' without the extras."""
        content = _read_maskfile()
        ci_match = re.search(r"#### ci\n.*?```bash\n(.*?)```", content, re.DOTALL)
        assert ci_match is not None
        ci_code = ci_match.group(1)
        # CI should have: fmt, clippy (replaces check+lint), test
        assert "fmt" in ci_code, "CI must include formatting"
        assert "clippy" in ci_code, "CI must include clippy"
        assert "test" in ci_code, "CI must include tests"


class TestDockerignore:
    """Validate .dockerignore is configured properly."""

    def test_dockerignore_exists(self) -> None:
        path = REPO_ROOT / ".dockerignore"
        assert path.exists()

    def test_excludes_venv(self) -> None:
        content = (REPO_ROOT / ".dockerignore").read_text()
        assert ".venv" in content, "Must exclude .venv from Docker context"

    def test_excludes_pycache(self) -> None:
        content = (REPO_ROOT / ".dockerignore").read_text()
        assert "__pycache__" in content, "Must exclude __pycache__"
