"""Tests for maskfile.md command configuration."""
import re
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parent.parent.parent
MASKFILE = REPO_ROOT / "maskfile.md"


def _read_maskfile() -> str:
    """Read the maskfile.md content."""
    return MASKFILE.read_text()


def _extract_code_blocks(content: str) -> list[str]:
    """Extract all bash code blocks from maskfile."""
    pattern = r"```bash\n(.*?)```"
    return re.findall(pattern, content, re.DOTALL)


class TestMaskfileStructure:
    """Validate maskfile.md structure."""

    def test_maskfile_exists(self) -> None:
        assert MASKFILE.exists(), "maskfile.md must exist"

    def test_has_infrastructure_section(self) -> None:
        content = _read_maskfile()
        assert "## infrastructure" in content

    def test_has_development_section(self) -> None:
        content = _read_maskfile()
        assert "## development" in content

    def test_has_images_subsection(self) -> None:
        content = _read_maskfile()
        assert "### images" in content

    def test_has_build_command(self) -> None:
        content = _read_maskfile()
        assert "#### build (application_name) (stage_name)" in content

    def test_has_push_command(self) -> None:
        content = _read_maskfile()
        assert "#### push (application_name) (stage_name)" in content


class TestDockerBuildCommand:
    """Validate Docker build command configuration."""

    def test_uses_buildx(self) -> None:
        content = _read_maskfile()
        assert "docker buildx build" in content, "Must use docker buildx for building"

    def test_has_cache_from(self) -> None:
        content = _read_maskfile()
        assert "--cache-from" in content, "Must configure cache-from"

    def test_has_cache_to(self) -> None:
        content = _read_maskfile()
        assert "--cache-to" in content, "Must configure cache-to"

    def test_gha_cache_has_scope(self) -> None:
        content = _read_maskfile()
        assert "scope=" in content, "GHA cache must use scope to prevent cross-service eviction"

    def test_has_gha_cache_in_ci(self) -> None:
        content = _read_maskfile()
        assert "type=gha" in content, "Must use GHA cache backend in CI"

    def test_has_registry_cache(self) -> None:
        content = _read_maskfile()
        assert "type=registry" in content, "Must use registry cache as fallback"

    def test_cache_mode_max(self) -> None:
        content = _read_maskfile()
        assert "mode=max" in content, "Must use mode=max for maximum layer caching"

    def test_targets_linux_amd64(self) -> None:
        content = _read_maskfile()
        assert "--platform linux/amd64" in content

    def test_uses_dockerfile_path(self) -> None:
        content = _read_maskfile()
        assert "--file applications/${application_name}/Dockerfile" in content

    def test_buildx_setup_skipped_in_gha(self) -> None:
        content = _read_maskfile()
        # When in GHA, should skip manual buildx creation since docker/setup-buildx-action handles it
        assert "GITHUB_ACTIONS" in content, "Must detect GitHub Actions environment"


class TestDockerPushCommand:
    """Validate Docker push command configuration."""

    def test_has_ecr_login(self) -> None:
        content = _read_maskfile()
        assert "aws ecr get-login-password" in content, "Must log into ECR"

    def test_has_digest_check(self) -> None:
        content = _read_maskfile()
        assert "describe-images" in content or "batch-get-image" in content or "content_hash" in content, \
            "Push command must check if image already exists before pushing"

    def test_has_skip_push_logic(self) -> None:
        content = _read_maskfile()
        assert "skipping push" in content.lower() or "skip" in content.lower(), \
            "Must have logic to skip push when image is unchanged"

    def test_pushes_with_commit_tag(self) -> None:
        content = _read_maskfile()
        assert "git" in content.lower() and ("tag" in content.lower() or "rev-parse" in content.lower()), \
            "Should tag images with git commit for traceability"


class TestRustDevelopmentCommands:
    """Validate Rust development commands."""

    def test_has_rust_section(self) -> None:
        content = _read_maskfile()
        assert "### rust" in content

    def test_has_update_command(self) -> None:
        content = _read_maskfile()
        assert "#### update" in content
        assert "cargo update" in content

    def test_has_check_command(self) -> None:
        content = _read_maskfile()
        assert "#### check" in content
        assert "cargo check" in content

    def test_has_format_command(self) -> None:
        content = _read_maskfile()
        assert "#### format" in content
        assert "cargo fmt" in content

    def test_has_lint_command(self) -> None:
        content = _read_maskfile()
        assert "#### lint" in content
        assert "cargo clippy" in content

    def test_has_test_command(self) -> None:
        content = _read_maskfile()
        assert "#### test" in content
        assert "cargo test" in content

    def test_has_all_command(self) -> None:
        content = _read_maskfile()
        assert "#### all" in content

    def test_has_ci_command(self) -> None:
        content = _read_maskfile()
        assert "#### ci" in content, "Must have a CI-optimized rust command"

    def test_ci_command_skips_cargo_update(self) -> None:
        content = _read_maskfile()
        # Find the ci command block
        ci_section_match = re.search(r"#### ci\n.*?```bash\n(.*?)```", content, re.DOTALL)
        assert ci_section_match is not None, "CI command must have a bash block"
        ci_code = ci_section_match.group(1)
        assert "cargo update" not in ci_code, "CI command must not run cargo update"

    def test_ci_command_skips_cargo_check(self) -> None:
        content = _read_maskfile()
        ci_section_match = re.search(r"#### ci\n.*?```bash\n(.*?)```", content, re.DOTALL)
        assert ci_section_match is not None
        ci_code = ci_section_match.group(1)
        assert "cargo check" not in ci_code, "CI command should skip cargo check (redundant with clippy)"

    def test_ci_command_has_format_check(self) -> None:
        content = _read_maskfile()
        ci_section_match = re.search(r"#### ci\n.*?```bash\n(.*?)```", content, re.DOTALL)
        assert ci_section_match is not None
        ci_code = ci_section_match.group(1)
        assert "cargo fmt" in ci_code, "CI must check formatting"
        assert "--check" in ci_code, "CI must use --check flag for fmt"

    def test_ci_command_has_clippy_with_warnings(self) -> None:
        content = _read_maskfile()
        ci_section_match = re.search(r"#### ci\n.*?```bash\n(.*?)```", content, re.DOTALL)
        assert ci_section_match is not None
        ci_code = ci_section_match.group(1)
        assert "cargo clippy" in ci_code
        assert "-D warnings" in ci_code, "Clippy must deny warnings in CI"

    def test_ci_command_has_tests(self) -> None:
        content = _read_maskfile()
        ci_section_match = re.search(r"#### ci\n.*?```bash\n(.*?)```", content, re.DOTALL)
        assert ci_section_match is not None
        ci_code = ci_section_match.group(1)
        assert "cargo test" in ci_code, "CI must run tests"

    def test_all_code_blocks_have_set_euo_pipefail(self) -> None:
        content = _read_maskfile()
        code_blocks = _extract_code_blocks(content)
        for block in code_blocks:
            assert "set -euo pipefail" in block, \
                f"All bash blocks must start with 'set -euo pipefail', found block without it"


class TestDockerfileConfiguration:
    """Validate Dockerfile best practices."""

    def test_datamanager_dockerfile_exists(self) -> None:
        path = REPO_ROOT / "applications" / "datamanager" / "Dockerfile"
        assert path.exists()

    def test_uses_multi_stage_build(self) -> None:
        content = (REPO_ROOT / "applications" / "datamanager" / "Dockerfile").read_text()
        from_count = content.count("FROM ")
        assert from_count >= 3, f"Must use multi-stage build, found {from_count} FROM statements"

    def test_uses_cargo_chef(self) -> None:
        content = (REPO_ROOT / "applications" / "datamanager" / "Dockerfile").read_text()
        assert "cargo-chef" in content, "Must use cargo-chef for dependency caching"

    def test_has_buildkit_cache_mounts(self) -> None:
        content = (REPO_ROOT / "applications" / "datamanager" / "Dockerfile").read_text()
        assert "--mount=type=cache" in content, "Must use BuildKit cache mounts"

    def test_caches_cargo_registry(self) -> None:
        content = (REPO_ROOT / "applications" / "datamanager" / "Dockerfile").read_text()
        assert "/usr/local/cargo/registry" in content, "Must cache cargo registry"

    def test_caches_build_target(self) -> None:
        content = (REPO_ROOT / "applications" / "datamanager" / "Dockerfile").read_text()
        assert "/app/target" in content, "Must cache build target directory"

    def test_final_stage_is_slim(self) -> None:
        content = (REPO_ROOT / "applications" / "datamanager" / "Dockerfile").read_text()
        assert "slim" in content, "Final stage should use slim base image"

    def test_builds_in_release_mode(self) -> None:
        content = (REPO_ROOT / "applications" / "datamanager" / "Dockerfile").read_text()
        assert "--release" in content, "Must build in release mode"
