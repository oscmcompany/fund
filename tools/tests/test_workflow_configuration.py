"""Tests for GitHub Actions workflow configuration."""
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parent.parent.parent


def _read_workflow(name: str) -> str:
    """Read a workflow YAML file as text."""
    path = REPO_ROOT / ".github" / "workflows" / name
    return path.read_text()


class TestLaunchInfrastructureWorkflow:
    """Validate launch_infrastructure.yaml configuration."""

    def test_workflow_file_exists(self) -> None:
        path = REPO_ROOT / ".github" / "workflows" / "launch_infrastructure.yaml"
        assert path.exists(), "launch_infrastructure.yaml must exist"

    def test_has_concurrency_group(self) -> None:
        content = _read_workflow("launch_infrastructure.yaml")
        assert "concurrency:" in content, "Must have concurrency control"
        assert "cancel-in-progress: false" in content, "Must not cancel in-progress deployments"

    def test_has_docker_buildx_setup(self) -> None:
        content = _read_workflow("launch_infrastructure.yaml")
        assert "docker/setup-buildx-action" in content, "Must use docker/setup-buildx-action for proper cache support"

    def test_has_service_matrix(self) -> None:
        content = _read_workflow("launch_infrastructure.yaml")
        assert "datamanager" in content
        assert "portfoliomanager" in content
        assert "equitypricemodel" in content

    def test_has_change_detection(self) -> None:
        content = _read_workflow("launch_infrastructure.yaml")
        assert "dorny/paths-filter" in content, "Must use paths-filter for change detection"

    def test_has_aws_credentials_step(self) -> None:
        content = _read_workflow("launch_infrastructure.yaml")
        assert "aws-actions/configure-aws-credentials" in content

    def test_build_step_uses_mask_command(self) -> None:
        content = _read_workflow("launch_infrastructure.yaml")
        assert "mask infrastructure images build" in content

    def test_push_step_uses_mask_command(self) -> None:
        content = _read_workflow("launch_infrastructure.yaml")
        assert "mask infrastructure images push" in content

    def test_deploy_needs_build_and_push(self) -> None:
        content = _read_workflow("launch_infrastructure.yaml")
        assert "needs: build_and_push" in content, "Deploy must depend on build_and_push"

    def test_has_schedule_trigger(self) -> None:
        content = _read_workflow("launch_infrastructure.yaml")
        assert "schedule:" in content
        assert "cron:" in content

    def test_has_push_trigger_on_master(self) -> None:
        content = _read_workflow("launch_infrastructure.yaml")
        assert "push:" in content
        assert "master" in content

    def test_has_manual_trigger(self) -> None:
        content = _read_workflow("launch_infrastructure.yaml")
        assert "workflow_dispatch" in content


class TestRustCodeChecksWorkflow:
    """Validate run_rust_code_checks.yaml configuration."""

    def test_workflow_file_exists(self) -> None:
        path = REPO_ROOT / ".github" / "workflows" / "run_rust_code_checks.yaml"
        assert path.exists(), "run_rust_code_checks.yaml must exist"

    def test_has_change_detection(self) -> None:
        content = _read_workflow("run_rust_code_checks.yaml")
        assert "dorny/paths-filter" in content

    def test_detects_rust_file_changes(self) -> None:
        content = _read_workflow("run_rust_code_checks.yaml")
        assert "'**/*.rs'" in content, "Must detect .rs file changes"
        assert "'**/Cargo.toml'" in content, "Must detect Cargo.toml changes"
        assert "'**/Cargo.lock'" in content, "Must detect Cargo.lock changes"

    def test_has_rust_build_cache(self) -> None:
        content = _read_workflow("run_rust_code_checks.yaml")
        assert "Swatinem/rust-cache" in content, "Must use Swatinem/rust-cache for build caching"

    def test_cache_saves_only_on_master(self) -> None:
        content = _read_workflow("run_rust_code_checks.yaml")
        assert "refs/heads/master" in content, "Cache should only save on master branch"

    def test_uses_ci_optimized_command(self) -> None:
        content = _read_workflow("run_rust_code_checks.yaml")
        assert "mask development rust ci" in content, "Must use CI-optimized rust command"

    def test_does_not_use_cargo_update(self) -> None:
        content = _read_workflow("run_rust_code_checks.yaml")
        assert "cargo update" not in content, "CI should not run cargo update"

    def test_conditional_on_rust_changes(self) -> None:
        content = _read_workflow("run_rust_code_checks.yaml")
        assert "needs: detect_changes" in content
        assert "rust == 'true'" in content

    def test_runs_on_pull_request(self) -> None:
        content = _read_workflow("run_rust_code_checks.yaml")
        assert "pull_request" in content


class TestPythonCodeChecksWorkflow:
    """Validate run_python_code_checks.yaml configuration."""

    def test_workflow_file_exists(self) -> None:
        path = REPO_ROOT / ".github" / "workflows" / "run_python_code_checks.yaml"
        assert path.exists()

    def test_has_change_detection(self) -> None:
        content = _read_workflow("run_python_code_checks.yaml")
        assert "dorny/paths-filter" in content

    def test_detects_python_file_changes(self) -> None:
        content = _read_workflow("run_python_code_checks.yaml")
        assert "'**/*.py'" in content


class TestAllWorkflowsExist:
    """Validate that all expected workflow files exist."""

    def test_launch_infrastructure_exists(self) -> None:
        assert (REPO_ROOT / ".github" / "workflows" / "launch_infrastructure.yaml").exists()

    def test_rust_code_checks_exists(self) -> None:
        assert (REPO_ROOT / ".github" / "workflows" / "run_rust_code_checks.yaml").exists()

    def test_python_code_checks_exists(self) -> None:
        assert (REPO_ROOT / ".github" / "workflows" / "run_python_code_checks.yaml").exists()

    def test_markdown_code_checks_exists(self) -> None:
        assert (REPO_ROOT / ".github" / "workflows" / "run_markdown_code_checks.yaml").exists()
