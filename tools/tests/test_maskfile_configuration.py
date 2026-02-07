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


# --- Maskfile Structure ---


def test_maskfile_structure_maskfile_exists() -> None:
    assert MASKFILE.exists(), "maskfile.md must exist"


def test_maskfile_structure_has_infrastructure_section() -> None:
    content = _read_maskfile()
    assert "## infrastructure" in content


def test_maskfile_structure_has_development_section() -> None:
    content = _read_maskfile()
    assert "## development" in content


def test_maskfile_structure_has_images_subsection() -> None:
    content = _read_maskfile()
    assert "### images" in content


def test_maskfile_structure_has_build_command() -> None:
    content = _read_maskfile()
    assert "#### build (application_name) (stage_name)" in content


def test_maskfile_structure_has_push_command() -> None:
    content = _read_maskfile()
    assert "#### push (application_name) (stage_name)" in content


# --- Docker Build Command ---


def test_docker_build_command_uses_buildx() -> None:
    content = _read_maskfile()
    assert "docker buildx build" in content, "Must use docker buildx for building"


def test_docker_build_command_has_cache_from() -> None:
    content = _read_maskfile()
    assert "--cache-from" in content, "Must configure cache-from"


def test_docker_build_command_has_cache_to() -> None:
    content = _read_maskfile()
    assert "--cache-to" in content, "Must configure cache-to"


def test_docker_build_command_gha_cache_has_scope() -> None:
    content = _read_maskfile()
    assert "scope=" in content, "GHA cache must use scope to prevent cross-service eviction"


def test_docker_build_command_has_gha_cache_in_ci() -> None:
    content = _read_maskfile()
    assert "type=gha" in content, "Must use GHA cache backend in CI"


def test_docker_build_command_has_registry_cache() -> None:
    content = _read_maskfile()
    assert "type=registry" in content, "Must use registry cache as fallback"


def test_docker_build_command_cache_mode_max() -> None:
    content = _read_maskfile()
    assert "mode=max" in content, "Must use mode=max for maximum layer caching"


def test_docker_build_command_targets_linux_amd64() -> None:
    content = _read_maskfile()
    assert "--platform linux/amd64" in content


def test_docker_build_command_uses_dockerfile_path() -> None:
    content = _read_maskfile()
    assert "--file applications/${application_name}/Dockerfile" in content


def test_docker_build_command_buildx_setup_skipped_in_gha() -> None:
    content = _read_maskfile()
    # When in GHA, should skip manual buildx creation since docker/setup-buildx-action handles it
    assert "GITHUB_ACTIONS" in content, "Must detect GitHub Actions environment"


# --- Docker Push Command ---


def test_docker_push_command_has_ecr_login() -> None:
    content = _read_maskfile()
    assert "aws ecr get-login-password" in content, "Must log into ECR"


def test_docker_push_command_has_digest_check() -> None:
    content = _read_maskfile()
    assert "describe-images" in content or "batch-get-image" in content or "content_hash" in content, \
        "Push command must check if image already exists before pushing"


def test_docker_push_command_has_skip_push_logic() -> None:
    content = _read_maskfile()
    assert "skipping push" in content.lower() or "skip" in content.lower(), \
        "Must have logic to skip push when image is unchanged"


def test_docker_push_command_pushes_with_commit_tag() -> None:
    content = _read_maskfile()
    assert "git" in content.lower() and ("tag" in content.lower() or "rev-parse" in content.lower()), \
        "Should tag images with git commit for traceability"


# --- Rust Development Commands ---


def test_rust_development_commands_has_rust_section() -> None:
    content = _read_maskfile()
    assert "### rust" in content


def test_rust_development_commands_has_update_command() -> None:
    content = _read_maskfile()
    assert "#### update" in content
    assert "cargo update" in content


def test_rust_development_commands_has_check_command() -> None:
    content = _read_maskfile()
    assert "#### check" in content
    assert "cargo check" in content


def test_rust_development_commands_has_format_command() -> None:
    content = _read_maskfile()
    assert "#### format" in content
    assert "cargo fmt" in content


def test_rust_development_commands_has_lint_command() -> None:
    content = _read_maskfile()
    assert "#### lint" in content
    assert "cargo clippy" in content


def test_rust_development_commands_has_test_command() -> None:
    content = _read_maskfile()
    assert "#### test" in content
    assert "cargo test" in content


def test_rust_development_commands_has_all_command() -> None:
    content = _read_maskfile()
    assert "#### all" in content


def test_rust_development_commands_has_ci_command() -> None:
    content = _read_maskfile()
    assert "#### ci" in content, "Must have a CI-optimized rust command"


def test_rust_development_commands_ci_command_skips_cargo_update() -> None:
    content = _read_maskfile()
    # Find the ci command block
    ci_section_match = re.search(r"#### ci\n.*?```bash\n(.*?)```", content, re.DOTALL)
    assert ci_section_match is not None, "CI command must have a bash block"
    ci_code = ci_section_match.group(1)
    assert "cargo update" not in ci_code, "CI command must not run cargo update"


def test_rust_development_commands_ci_command_skips_cargo_check() -> None:
    content = _read_maskfile()
    ci_section_match = re.search(r"#### ci\n.*?```bash\n(.*?)```", content, re.DOTALL)
    assert ci_section_match is not None
    ci_code = ci_section_match.group(1)
    assert "cargo check" not in ci_code, "CI command should skip cargo check (redundant with clippy)"


def test_rust_development_commands_ci_command_has_format_check() -> None:
    content = _read_maskfile()
    ci_section_match = re.search(r"#### ci\n.*?```bash\n(.*?)```", content, re.DOTALL)
    assert ci_section_match is not None
    ci_code = ci_section_match.group(1)
    assert "cargo fmt" in ci_code, "CI must check formatting"
    assert "--check" in ci_code, "CI must use --check flag for fmt"


def test_rust_development_commands_ci_command_has_clippy_with_warnings() -> None:
    content = _read_maskfile()
    ci_section_match = re.search(r"#### ci\n.*?```bash\n(.*?)```", content, re.DOTALL)
    assert ci_section_match is not None
    ci_code = ci_section_match.group(1)
    assert "cargo clippy" in ci_code
    assert "-D warnings" in ci_code, "Clippy must deny warnings in CI"


def test_rust_development_commands_ci_command_has_tests() -> None:
    content = _read_maskfile()
    ci_section_match = re.search(r"#### ci\n.*?```bash\n(.*?)```", content, re.DOTALL)
    assert ci_section_match is not None
    ci_code = ci_section_match.group(1)
    assert "cargo test" in ci_code, "CI must run tests"


def test_rust_development_commands_all_code_blocks_have_set_euo_pipefail() -> None:
    content = _read_maskfile()
    code_blocks = _extract_code_blocks(content)
    for block in code_blocks:
        assert "set -euo pipefail" in block, \
            f"All bash blocks must start with 'set -euo pipefail', found block without it"


# --- Dockerfile Configuration ---


def test_dockerfile_configuration_datamanager_dockerfile_exists() -> None:
    path = REPO_ROOT / "applications" / "datamanager" / "Dockerfile"
    assert path.exists()


def test_dockerfile_configuration_uses_multi_stage_build() -> None:
    content = (REPO_ROOT / "applications" / "datamanager" / "Dockerfile").read_text()
    from_count = content.count("FROM ")
    assert from_count >= 3, f"Must use multi-stage build, found {from_count} FROM statements"


def test_dockerfile_configuration_uses_cargo_chef() -> None:
    content = (REPO_ROOT / "applications" / "datamanager" / "Dockerfile").read_text()
    assert "cargo-chef" in content, "Must use cargo-chef for dependency caching"


def test_dockerfile_configuration_has_buildkit_cache_mounts() -> None:
    content = (REPO_ROOT / "applications" / "datamanager" / "Dockerfile").read_text()
    assert "--mount=type=cache" in content, "Must use BuildKit cache mounts"


def test_dockerfile_configuration_caches_cargo_registry() -> None:
    content = (REPO_ROOT / "applications" / "datamanager" / "Dockerfile").read_text()
    assert "/usr/local/cargo/registry" in content, "Must cache cargo registry"


def test_dockerfile_configuration_caches_build_target() -> None:
    content = (REPO_ROOT / "applications" / "datamanager" / "Dockerfile").read_text()
    assert "/app/target" in content, "Must cache build target directory"


def test_dockerfile_configuration_final_stage_is_slim() -> None:
    content = (REPO_ROOT / "applications" / "datamanager" / "Dockerfile").read_text()
    assert "slim" in content, "Final stage should use slim base image"


def test_dockerfile_configuration_builds_in_release_mode() -> None:
    content = (REPO_ROOT / "applications" / "datamanager" / "Dockerfile").read_text()
    assert "--release" in content, "Must build in release mode"
