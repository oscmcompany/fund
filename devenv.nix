{
  pkgs,
  lib,
  config,
  inputs,
  ...
}: let
  awsRegion = "us-east-1";

  isProd = builtins.getEnv "FUND_ENVIRONMENT" == "production";
in {
  dotenv.enable = true;

  languages = {
    rust.enable = true;
    python.enable = true;
    nix.enable = true;
  };

  git-hooks.hooks = {
    python-install = {
      enable = true;
      name = "Install Python dependencies";
      entry = "python-install";
      files = "(pyproject\\.toml|uv\\.lock)$";
      pass_filenames = false;
      language = "system";
    };
    python-checks = {
      enable = true;
      name = "Check all Python code";
      entry = "python-checks";
      files = "(\\.py|pyproject\\.toml|uv\\.lock)$";
      pass_filenames = false;
      language = "system";
      fail_fast = true;
    };
    rust-checks = {
      enable = true;
      name = "Check all Rust code";
      entry = "rust-checks";
      files = "(\\.rs|Cargo\\.(toml|lock))$";
      pass_filenames = false;
      language = "system";
      fail_fast = true;
    };
    markdown-checks = {
      enable = true;
      name = "Check all Markdown code";
      entry = "markdown-checks";
      files = "\\.md$";
      pass_filenames = false;
      language = "system";
      fail_fast = true;
    };
    yaml-checks = {
      enable = true;
      name = "Check all YAML code";
      entry = "yaml-checks";
      files = "\\.(yaml|yml)$";
      pass_filenames = false;
      language = "system";
      fail_fast = true;
    };
    nix-lint = {
      enable = true;
      name = "Lint all Nix code";
      entry = "nix-lint";
      files = "\\.nix$";
      pass_filenames = false;
      language = "system";
      fail_fast = true;
    };
  };

  env = {
    # DuckDB library path for Rust linker
    LIBRARY_PATH = "${pkgs.duckdb}/lib";

    # AWS region
    AWS_REGION = awsRegion;
    AWS_DEFAULT_REGION = awsRegion;

    # S3 bucket names (set in .envrc for prod)
    AWS_S3_DATA_BUCKET_NAME = "";
    AWS_S3_MODEL_ARTIFACTS_BUCKET_NAME = "";

    # tinygrad CPU JIT requires clang (gcc rejects --target flag)
    CC = "clang";

    # Development defaults
    ENVIRONMENT = "development";
  };

  packages = with pkgs; [
    alejandra
    awscli2
    clang
    bacon
    cargo-watch
    curl
    duckdb
    gh
    git
    jq
    markdownlint-cli
    ruff
    rustup
    uv
    xenon
  ];

  scripts.aws-buckets.exec = ''
    set -euo pipefail
    unset AWS_ENDPOINT_URL
    echo "=== Fund S3 Buckets ==="
    aws s3 ls | grep fund || echo "No fund buckets found"
  '';

  scripts.aws-secrets.exec = ''
    set -euo pipefail
    unset AWS_ENDPOINT_URL
    echo "=== Fund Secrets ==="
    aws secretsmanager list-secrets \
      --region ${awsRegion} \
      --query 'SecretList[?contains(Name, `fund`) || contains(Name, `secretspec`)].Name' \
      --output table
  '';

  # --- Development check scripts ---

  scripts.python-install.exec = ''
    set -euo pipefail
    echo "Installing Python dependencies"
    uv sync --all-packages --all-groups
    echo "Python dependencies installed successfully"
  '';

  scripts.python-format.exec = ''
    set -euo pipefail
    echo "Checking Python code formatting"
    ruff format --check
    echo "Python code formatting check passed"
  '';

  scripts.python-lint = {
    description = "Running Python lint checks";
    exec = ''
      set -euo pipefail
      ruff check --output-format=github .
      echo "Python linting completed successfully"
    '';
  };

  scripts.python-type-check.exec = ''
    set -euo pipefail
    echo "Running Python type checks"
    uvx ty check
    echo "Python type checks completed successfully"
  '';

  scripts.python-dead-code.exec = ''
    set -euo pipefail
    echo "Running dead code analysis"
    uvx vulture \
      --min-confidence 80 \
      --exclude '.venv,target' \
      . tools/src/tools/vulture_whitelist.py
    echo "Dead code check completed"
  '';

  scripts.python-complexity.exec = ''
    set -euo pipefail
    echo "Running Python complexity analysis"
    xenon --max-absolute D --max-modules D --max-average A \
      --ignore '.venv,target' .
    echo "Python complexity analysis completed successfully"
  '';

  scripts.python-test.exec = ''
    set -euo pipefail
    echo "Running Python tests"
    export CC=clang
    mkdir -p .coverage_output
    uv run coverage run -m pytest --tb=short -q
    uv run coverage combine 2>/dev/null || true
    uv run coverage xml -o .coverage_output/python.xml
    echo "Python tests completed successfully"
  '';

  scripts.python-checks.exec = ''
    devenv tasks run checks:python
  '';

  scripts.rust-format.exec = ''
    set -euo pipefail
    echo "Checking Rust code formatting"
    cargo fmt --all -- --check
    echo "Rust code formatting check passed"
  '';

  scripts.rust-check.exec = ''
    set -euo pipefail
    echo "Check Rust packages"
    cargo check --workspace
    echo "Rust packages checked successfully"
  '';

  scripts.rust-lint.exec = ''
    set -euo pipefail
    echo "Running Rust lint checks"
    cargo clippy
    echo "Rust linting completed successfully"
  '';

  scripts.rust-test.exec = ''
    set -euo pipefail
    echo "Running Rust tests"

    TEST_ARGS="--workspace --verbose --lib --bins"

    mkdir -p .coverage_output
    if ! command -v cargo-llvm-cov >/dev/null 2>&1; then
      echo "cargo-llvm-cov not available - running tests without coverage"
      cargo test $TEST_ARGS
    elif ! command -v llvm-cov >/dev/null 2>&1 || ! command -v llvm-profdata >/dev/null 2>&1; then
      echo "LLVM tools not available - running tests without coverage"
      cargo test $TEST_ARGS
    else
      export LLVM_COV=$(which llvm-cov)
      export LLVM_PROFDATA=$(which llvm-profdata)
      cargo llvm-cov $TEST_ARGS \
        --cobertura \
        --output-path .coverage_output/rust.xml
      echo "Rust tests with coverage completed successfully"
    fi
  '';

  scripts.rust-checks.exec = ''
    devenv tasks run checks:rust
  '';

  scripts.markdown-checks.exec = ''
    set -euo pipefail
    echo "Running Markdown lint checks"
    markdownlint "**/*.md" --ignore ".venv" \
      --ignore "target" --ignore ".scratchpad"
    echo "Markdown checks completed successfully"
  '';

  scripts.yaml-checks.exec = ''
    set -euo pipefail
    echo "Running YAML lint checks"
    uvx yamllint .
    echo "YAML checks completed successfully"
  '';

  scripts.nix-lint.exec = ''
    set -euo pipefail
    echo "Linting Nix files"
    alejandra --check --exclude ./.devenv --exclude ./.venv --exclude ./target .
    echo "Nix lint check passed"
  '';

  scripts.bump-deps.exec = ''
    set -euo pipefail
    echo "Bumping all dependencies..."
    echo "=== Rust ==="
    cargo update
    echo "=== Python ==="
    uv lock --upgrade
    echo ""
    echo "Dependencies bumped. Review changes:"
    echo "  git diff Cargo.lock uv.lock"
  '';

  tasks = {
    # --- Python checks (install first, then all others in parallel) ---

    "checks:python:install".exec = "python-install";

    "checks:python:format" = {
      exec = "python-format";
      after = ["checks:python:install"];
    };
    "checks:python:lint" = {
      exec = "python-lint";
      after = ["checks:python:install"];
    };
    "checks:python:type-check" = {
      exec = "python-type-check";
      after = ["checks:python:install"];
    };
    "checks:python:dead-code" = {
      exec = "python-dead-code";
      after = ["checks:python:install"];
    };
    "checks:python:complexity" = {
      exec = "python-complexity";
      after = ["checks:python:install"];
    };
    "checks:python:test" = {
      exec = "python-test";
      after = ["checks:python:install"];
    };

    # --- Rust checks (format parallel with check, lint+test after check) ---

    "checks:rust:format".exec = "rust-format";
    "checks:rust:check".exec = "rust-check";

    "checks:rust:lint" = {
      exec = "rust-lint";
      after = ["checks:rust:check"];
    };
    "checks:rust:test" = {
      exec = "rust-test";
      after = ["checks:rust:check"];
    };

    # --- Standalone checks ---

    "checks:markdown".exec = "markdown-checks";
    "checks:yaml".exec = "yaml-checks";
    "checks:nix".exec = "nix-lint";

    # --- Model training ---

    "models:tide:train".exec = ''
      set -euo pipefail
      echo "Running tide training pipeline"
      secretspec run -- uv run python -m models.tide.train
    '';

    "checks:ci" = {
      exec = ''
        echo "All CI checks passed"
      '';
      after = [
        "checks:nix"
        "checks:markdown"
        "checks:yaml"
        "checks:python:format"
        "checks:python:lint"
        "checks:python:type-check"
        "checks:python:dead-code"
        "checks:python:complexity"
        "checks:python:test"
        "checks:rust:format"
        "checks:rust:lint"
        "checks:rust:test"
      ];
    };
  };

  # --- Profiles ---

  profiles.apps.module = {
    env = {
      FUND_DATAMANAGER_BASE_URL = "http://localhost:8080";
      FUND_ENSEMBLEMANAGER_BASE_URL = "http://localhost:8082";
      MASSIVE_BASE_URL = "";
      DISABLE_DISK_CACHE = "1";
      BACKFILL_LOOKBACK_DAYS = "730";
    };

    scripts.cleanup-services.exec = ''
      for PORT in 8080 8081 8082; do
        PID=$(lsof -ti tcp:$PORT 2>/dev/null || true)
        if [ -n "$PID" ]; then
          echo "Killing stale process on port $PORT (PID $PID)"
          kill $PID 2>/dev/null || true
        fi
      done
      sleep 1
    '';

    processes = {
      data-manager.exec =
        if isProd
        then ''
          cd applications/data_manager
          exec secretspec run -- cargo run --release
        ''
        else ''
          cd applications/data_manager
          exec secretspec run -- cargo watch -x run
        '';

      ensemble-manager.exec = let
        waitForDataManager = ''
          while ! curl -sf http://localhost:8080/health > /dev/null 2>&1; do
            sleep 2
          done
        '';
        uvicornCmd = "uv run uvicorn ensemble_manager.server:application --host 0.0.0.0 --port 8082";
      in
        if isProd
        then ''
          ${waitForDataManager}
          cd applications/ensemble_manager
          exec secretspec run -- ${uvicornCmd}
        ''
        else ''
          ${waitForDataManager}
          cd applications/ensemble_manager
          exec secretspec run -- ${uvicornCmd} --reload
        '';

      portfolio-manager.exec = let
        waitForDeps = ''
          while ! curl -sf http://localhost:8080/health > /dev/null 2>&1; do
            sleep 2
          done
          while ! curl -sf http://localhost:8082/health > /dev/null 2>&1; do
            sleep 2
          done
        '';
        uvicornCmd = "uv run uvicorn portfolio_manager.server:application --host 0.0.0.0 --port 8081";
      in
        if isProd
        then ''
          ${waitForDeps}
          cd applications/portfolio_manager
          exec secretspec run -- ${uvicornCmd}
        ''
        else ''
          ${waitForDeps}
          cd applications/portfolio_manager
          exec secretspec run -- ${uvicornCmd} --reload
        '';

      artifact-watcher.exec = ''
        while ! curl -sf http://localhost:8082/health > /dev/null 2>&1; do
          sleep 2
        done
        exec secretspec run -- uv run --package tools python -m tools.artifact_watcher
      '';
    };
  };

  profiles.ml.module = {
    env = {
      FUND_LOOKBACK_DAYS = "";
      MLFLOW_TRACKING_URI = "";
      PREFECT_API_URL = "";
    };

    scripts.train-local.exec = ''
      set -euo pipefail
      echo "Running local training pipeline"
      uv run python -m models.tide.train
    '';

    scripts.deploy-training.exec = ''
      set -euo pipefail
      echo "Registering Prefect deployment"
      uv run python -m models.tide.deploy
    '';
  };

  enterShell = ''
    echo "Fund development environment"
    echo ""
    echo "  Profiles:"
    echo "    devenv --profile apps up      Start application services"
    echo "    devenv --profile ml shell     ML training environment"
    echo ""
    echo "  Services (apps profile):"
    echo "    Data Manager:     localhost:8080"
    echo "    Portfolio Manager: localhost:8081"
    echo "    Ensemble Manager: localhost:8082"
    echo ""
    echo "  Secrets (secretspec):"
    echo "    secretspec check          Validate production secrets"
    echo "    secretspec set <KEY>      Set a secret value"
    echo ""
    echo "  AWS:"
    echo "    aws-buckets       List fund S3 buckets"
    echo "    aws-secrets       List fund secrets"
    echo ""
    echo "  Tasks (devenv tasks run):"
    echo "    checks:python       All Python checks (parallel after install)"
    echo "    checks:rust         All Rust checks (parallel after cargo check)"
    echo "    checks:markdown     Markdown lint"
    echo "    checks:yaml         YAML lint"
    echo "    checks:nix          Nix lint (alejandra)"
    echo "    models:tide:train   Train tide model and upload artifacts"
    echo ""
    echo "  Utilities:"
    echo "    bump-deps           Update all dependency lockfiles"
  '';

  enterTest = ''
  '';
}
