{pkgs, ...}: let
  awsRegion = "us-east-1";

  rawFundProfile = builtins.getEnv "FUND_PROFILE";
  fundProfile =
    if rawFundProfile == ""
    then "development"
    else rawFundProfile;
  # Deployment is about WHERE the services run (the exe.dev VM), not which
  # profile/secrets are in use: a developer uses their `development/<name>`
  # profile both locally (hot-reload dev) and on a VM. Production is always
  # deployed; any other environment opts in explicitly by exporting
  # FUND_DEPLOYED=true on the VM, so local development defaults to dev mode.
  isDeployed = fundProfile == "production" || builtins.getEnv "FUND_DEPLOYED" == "true";

  bucketSlug = builtins.replaceStrings ["/" "."] ["-" "-"] fundProfile;

  # Log directory. Deployed profiles keep the root-owned /var/log/fund path
  # (provisioned on the VM, where logrotate and monitoring expect it). Local
  # development points FUND_LOG_DIR at an XDG state path so file logging works
  # without sudo.
  homeDirectory = builtins.getEnv "HOME";
  fundLogDir =
    if isDeployed
    then "/var/log/fund"
    else "${homeDirectory}/.local/state/fund/log";
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
    toml-checks = {
      enable = true;
      name = "Check all TOML code";
      entry = "toml-checks";
      files = "\\.toml$";
      pass_filenames = false;
      language = "system";
      fail_fast = true;
    };
    sql-checks = {
      enable = true;
      name = "Check all SQL code";
      entry = "sql-checks";
      files = "\\.sql$";
      pass_filenames = false;
      language = "system";
      fail_fast = true;
    };
    nix-lint = {
      enable = true;
      name = "Check all Nix code";
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

    # Active profile
    FUND_PROFILE = fundProfile;

    # Writable log directory for local file logging (see fundLogDir above)
    FUND_LOG_DIR = fundLogDir;

    # S3 bucket name derived from FUND_PROFILE
    AWS_S3_BUCKET_NAME = "oscm-fund-${bucketSlug}";

    # PostgreSQL
    DATABASE_URL = "postgresql://localhost:5432/fund";
    PGDATABASE = "fund";

    # tinygrad CPU JIT requires clang (gcc rejects --target flag)
    CC = "clang";

    # Secretspec CLI configuration
    SECRETSPEC_PROVIDER = "awssm";
    SECRETSPEC_PROFILE = fundProfile;

    # Disable AWS CLI pager so secrets output is not paged
    AWS_PAGER = "";
  };

  services.postgres = {
    enable = true;
    # allowUnfree: true in devenv.yaml enables the TSL-licensed timescaledb extension.
    package = pkgs.postgresql_16;
    extensions = extensions: [
      extensions.timescaledb
      extensions.pg_cron
      # pg_parquet: buildPgrxExtension marks all Darwin builds broken by default.
      # The flag is overcautious — pgrx supports macOS and pg_parquet works on Darwin.
      ((pkgs.callPackage ./nix/pg_parquet.nix {postgresql = pkgs.postgresql_16;}).overrideAttrs (old: {
        meta = old.meta // {broken = false;};
      }))
    ];
    port = 5432;
    listen_addresses = "127.0.0.1";
    initialDatabases = [
      {
        name = "fund";
        schema = ./schema.sql;
      }
    ];
    settings = {
      shared_preload_libraries = "timescaledb,pg_cron,pg_parquet";
      "cron.database_name" = "fund";
      "cron.timezone" = "UTC";
    };
  };

  packages = with pkgs; [
    alejandra
    awscli2
    clang
    bacon
    cargo-llvm-cov
    cargo-watch
    curl
    duckdb
    gh
    git
    rainfrog
    jq
    llvmPackages.llvm
    markdownlint-cli
    postgresql_16
    ruff
    rustup
    statix
    taplo
    uv
    xenon
  ];

  scripts.db-seed.exec = ''
    set -euo pipefail
    echo "Downloading latest database snapshot..."
    aws s3 cp s3://fund-backups/pg/fund-latest.dump.gz /tmp/fund-latest.dump.gz
    rm -f /tmp/fund-latest.dump
    gunzip /tmp/fund-latest.dump.gz
    pg_restore --host 127.0.0.1 --port 5432 \
      --no-owner --no-acl \
      --dbname fund --clean --if-exists /tmp/fund-latest.dump
    echo "Database seeded"
  '';

  scripts.db-migrate.exec = ''
    set -euo pipefail
    echo "Waiting for PostgreSQL..."
    while ! psql -h localhost -p 5432 -d fund -c 'SELECT 1' > /dev/null 2>&1; do sleep 1; done
    echo "Applying schema migrations..."
    psql -h localhost -p 5432 -d fund \
      -f ${./schema.sql} \
      --quiet --set ON_ERROR_STOP=on --set client_min_messages=warning
    echo "Schema migrations applied"
  '';

  scripts.aws-buckets.exec = ''
    set -euo pipefail
    unset AWS_ENDPOINT_URL
    echo "=== Fund S3 Buckets (profile: $FUND_PROFILE) ==="
    echo "  Bucket: $AWS_S3_BUCKET_NAME"
    echo ""
    buckets=$(aws s3 ls)
    printf '%s\n' "$buckets" | grep fund || echo "No fund buckets found"
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
    uv run coverage erase
    uv run coverage run -m pytest --tb=short -q
    uv run coverage combine 2>/dev/null || true
    uv run coverage xml -o .coverage_output/python.xml
    uv run coverage report --fail-under=80 | tee .coverage_output/python_report.txt
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

  scripts.rust-lint.exec = ''
    set -euo pipefail
    echo "Running Rust lint checks"
    cargo clippy --workspace
    echo "Rust linting completed successfully"
  '';

  scripts.rust-test.exec = ''
        set -euo pipefail
        echo "Running Rust tests"

        TEST_ARGS="--workspace --verbose --lib --bins"

        mkdir -p .coverage_output
        export LLVM_COV=$(which llvm-cov)
        export LLVM_PROFDATA=$(which llvm-profdata)
        cargo llvm-cov $TEST_ARGS \
          --cobertura \
          --output-path .coverage_output/rust.xml
        python3 -c "
    import xml.etree.ElementTree as ET, sys
    root = ET.parse('.coverage_output/rust.xml').getroot()
    rate = float(root.get('line-rate', 0)) * 100
    threshold = 10
    print(f'Rust line coverage: {rate:.1f}%')
    if rate < threshold:
        print(f'Coverage failure: {rate:.1f}% is below threshold of {threshold}%')
        sys.exit(1)
    "
        echo "Rust tests with coverage completed successfully"
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

  scripts.toml-checks.exec = ''
    set -euo pipefail
    echo "Running TOML checks"
    find . \
      \( -path "./.devenv" -o -path "./target" -o -path "./.venv" -o -path "./models/tide/.devenv" \) -prune \
      -o -name "*.toml" -print \
      | xargs taplo fmt --check --no-auto-config
    echo "TOML checks completed successfully"
  '';

  scripts.sql-checks.exec = ''
    set -euo pipefail
    echo "Running SQL checks"
    uvx sqlfluff lint .
    echo "SQL checks completed successfully"
  '';

  scripts.nix-lint.exec = ''
    set -euo pipefail
    echo "Checking Nix code formatting"
    alejandra --check --exclude ./.devenv --exclude ./.venv --exclude ./target --exclude ./models/tide/.devenv .
    echo "Nix formatting check passed"
    echo "Running Nix static analysis"
    statix check -c .statix.toml .
    echo "Nix checks completed successfully"
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

  scripts.trigger-rebalance.exec = ''
    psql -h localhost -p 5432 -d fund -c "SELECT emit_event('intraday_check',
    '{}')"
  '';

  scripts.backfill-bars.exec = ''
    set -euo pipefail

    if [ -z "''${BACKFILL_START_DATE:-}" ]; then
      echo "Usage: BACKFILL_START_DATE=YYYY-MM-DD devenv tasks run data:backfill-bars"
      echo "  Optional: BACKFILL_END_DATE=YYYY-MM-DD (defaults to today)"
      exit 1
    fi

    END_DATE="''${BACKFILL_END_DATE:-$(date -u +%Y-%m-%d)}"

    # Fetches grouped daily bars from Massive and writes them straight to S3 as
    # Hive-partitioned Parquet. No HTTP server or Postgres required: historical
    # bars are consumed by model training directly from S3.
    echo "Backfilling equity bars from $BACKFILL_START_DATE to $END_DATE"
    secretspec run -- cargo run --no-default-features --features data_manager --bin backfill -- \
      "$BACKFILL_START_DATE" "$END_DATE"
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

    # --- Rust checks (sequential to reuse compilation artifacts) ---

    "checks:rust:format".exec = "rust-format";

    "checks:rust:lint" = {
      exec = "rust-lint";
      after = ["checks:rust:format"];
    };
    "checks:rust:test" = {
      exec = "rust-test";
      after = ["checks:rust:lint"];
    };

    # --- Standalone checks ---

    "checks:markdown".exec = "markdown-checks";
    "checks:yaml".exec = "yaml-checks";
    "checks:toml".exec = "toml-checks";
    "checks:sql".exec = "sql-checks";
    "checks:nix".exec = "nix-lint";

    # --- Model training ---

    "models:tide:register-blocks".exec = ''
      set -euo pipefail
      echo "Registering Prefect S3 blocks"
      secretspec run -- uv run python -m tide.register_blocks
    '';

    "models:tide:train" = {
      exec = ''
        set -euo pipefail
        export CC=clang
        echo "Running tide training pipeline"
        secretspec run -- uv run python -m tide.workflow
      '';
      after = ["models:tide:register-blocks"];
    };

    # --- Data tasks ---

    "data:backfill-bars".exec = "backfill-bars";

    "checks:base" = {
      exec = ''
        echo "All base checks passed"
      '';
      after = [
        "checks:nix"
        "checks:markdown"
        "checks:yaml"
        "checks:toml"
        "checks:sql"
      ];
    };

    "checks:continuous-integration" = {
      exec = ''
        echo "All continuous integration checks passed"
      '';
      after = [
        "checks:base"
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

  profiles.applications.module = {
    env = {
      DISABLE_DISK_CACHE = "1";
      BACKFILL_LOOKBACK_DAYS = "730";
      DATABASE_URL = "postgresql://localhost:5432/fund";
      # Pin to last known-good tinygrad artifact (May 27 artifact is Burn binary, not safetensors)
      MODEL_VERSION = "2026-05-22-02-34-59-139";
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

    processes = let
      killPort = port: ''
        STALE_PID=$(lsof -ti tcp:${port} 2>/dev/null || true)
        if [ -n "$STALE_PID" ]; then
          echo "Killing stale process on port ${port} (PID $STALE_PID)"
          kill $STALE_PID 2>/dev/null || true
          sleep 1
        fi
      '';

      waitForPostgres = ''
        attempt=0
        max_attempts=90
        while ! psql -h localhost -p 5432 -d fund -c 'SELECT 1' > /dev/null 2>&1; do
          attempt=$((attempt + 1))
          if [ "$attempt" -ge "$max_attempts" ]; then
            echo "PostgreSQL (fund database) did not become ready after $((max_attempts * 2)) seconds"
            exit 1
          fi
          sleep 2
        done
      '';

      applySchema = ''
        echo "Applying schema migrations..."
        psql -h localhost -p 5432 -d fund \
          -f ${./schema.sql} \
          --quiet --set ON_ERROR_STOP=on --set client_min_messages=warning
        echo "Schema migrations applied"
      '';
    in {
      data-manager.exec =
        if isDeployed
        then ''
          set -euo pipefail
          ${waitForPostgres}
          ${applySchema}
          ${killPort "8080"}
          exec secretspec run -- cargo run --no-default-features --features data_manager --bin data_manager --release
        ''
        else ''
          set -euo pipefail
          ${waitForPostgres}
          ${applySchema}
          ${killPort "8080"}
          exec secretspec run -- cargo watch -x 'run --no-default-features --features data_manager --bin data_manager'
        '';

      ensemble-manager.exec = let
        uvicornCmd = "uv run uvicorn ensemble_manager.server:application --host 0.0.0.0 --port 8082";
      in
        if isDeployed
        then ''
          ${waitForPostgres}
          ${killPort "8082"}
          export CC=clang
          exec secretspec run -- ${uvicornCmd}
        ''
        else ''
          ${waitForPostgres}
          ${killPort "8082"}
          export CC=clang
          exec secretspec run -- ${uvicornCmd} --reload
        '';

      portfolio-manager.exec = let
        uvicornCmd = "uv run uvicorn portfolio_manager.server:application --host 0.0.0.0 --port 8081";
      in
        if isDeployed
        then ''
          ${waitForPostgres}
          ${killPort "8081"}
          exec secretspec run -- ${uvicornCmd}
        ''
        else ''
          ${waitForPostgres}
          ${killPort "8081"}
          exec secretspec run -- ${uvicornCmd} --reload
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
      export CC=clang
      echo "Running local training pipeline"
      uv run python -m tide.workflow
    '';

    scripts.deploy-training.exec = ''
      set -euo pipefail
      echo "Registering Prefect deployment"
      uv run python -m tide.deploy
    '';
  };

  enterShell = ''
    mkdir -p "$FUND_LOG_DIR" 2>/dev/null || true
    {
      echo "Fund development environment (profile: $FUND_PROFILE)"
      echo ""
      echo "  Bucket: $AWS_S3_BUCKET_NAME"
      echo ""
      echo "  Profiles:"
      echo "    devenv --profile applications up      Start application services"
      echo "    devenv --profile ml shell     ML training environment"
      echo ""
      echo "  Services (applications profile):"
      echo "    Data Manager:     localhost:8080"
      echo "    Portfolio Manager: localhost:8081"
      echo "    Ensemble Manager: localhost:8082"
      echo "    PostgreSQL:       localhost:5432/fund"
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
      echo "    checks:base         Non-language checks (nix, markdown, yaml, toml, sql)"
      echo "    checks:python       All Python checks (parallel after install)"
      echo "    checks:rust         All Rust checks (sequential: format, lint, test)"
      echo "    checks:markdown     Markdown lint"
      echo "    checks:yaml         YAML lint"
      echo "    checks:toml         TOML format check"
      echo "    checks:sql          SQL lint (PostgreSQL)"
      echo "    checks:nix          Nix checks (alejandra + statix)"
      echo "    data:backfill-bars  Backfill historical equity bar data"
      echo "    models:tide:train   Train tide model and upload artifacts"
      echo ""
      echo "  Utilities:"
      echo "    bump-deps           Update all dependency lockfiles"
    } >&2
  '';

  enterTest = ''
  '';
}
