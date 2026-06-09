{pkgs, ...}: let
  awsRegion = "us-east-1";

  rawFundProfile = builtins.getEnv "FUND_PROFILE";
  fundProfile =
    if rawFundProfile == ""
    then "development"
    else rawFundProfile;
  isProduction = fundProfile == "production";

  # Compute bucket name and secretspec profile at shell/process start time from
  # $FUND_PROFILE, which dotenv sets from .env. These cannot be baked in at Nix
  # evaluation time because dotenv runs after Nix evaluates devenv.nix. Model
  # artifacts live in the same per-profile bucket under models/tide/: the Rust
  # trainer (tide_train) writes there and the ensemble inference service
  # (AppState::from_env) reads there, so training and serving agree in both
  # dev and production.
  runtimeEnv = ''
    export AWS_S3_BUCKET_NAME="oscm-fund-$(echo ''${FUND_PROFILE} | tr '/.' '--')"
    export SECRETSPEC_PROFILE="''${FUND_PROFILE}"
    export AWS_S3_MODEL_ARTIFACTS_BUCKET_NAME="$AWS_S3_BUCKET_NAME"
    export AWS_S3_MODEL_ARTIFACT_PATH="models/tide/"
  '';

  applySchema = ''
    echo "Applying schema..."
    psql -h localhost -p 5432 -d fund \
      -f ${./schema.sql} \
      --quiet --set ON_ERROR_STOP=on --set client_min_messages=warning
    echo "Schema applied"
  '';

  # Training lookback window. Read from the environment so it can be overridden
  # per run (e.g. FUND_LOOKBACK_DAYS=1200 devenv --profile ml ...); a hardcoded
  # empty default would both shadow the override and break int parsing in the
  # tide trainer, which only falls back to its own default when the var is
  # unset, not when it is the empty string.
  rawLookbackDays = builtins.getEnv "FUND_LOOKBACK_DAYS";
  lookbackDays =
    if rawLookbackDays == ""
    then "365"
    else rawLookbackDays;

  # Log directory. Production keeps the root-owned /var/log/fund path
  # (provisioned on the VM, where logrotate and monitoring expect it). Local
  # development points FUND_LOG_DIR at an XDG state path so file logging works
  # without sudo.
  homeDirectory = builtins.getEnv "HOME";
  fundLogDir =
    if isProduction
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

    # Writable log directory for local file logging (see fundLogDir above)
    FUND_LOG_DIR = fundLogDir;

    # PostgreSQL
    DATABASE_URL = "postgresql://localhost:5432/fund";
    PGDATABASE = "fund";

    # tinygrad CPU JIT requires clang (gcc rejects --target flag)
    CC = "clang";

    # Secretspec CLI configuration
    SECRETSPEC_PROVIDER = "awssm";

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

  # database:restore — fast recovery from a nightly S3 dump when schema has not changed.
  # database:create — first-time setup: apply schema, seed equity details, backfill bars.
  # database:reset  — drop and recreate the empty fund database; run before database:create
  #                   after a breaking schema change.

  scripts.database-restore.exec = ''
    set -euo pipefail
    ${runtimeEnv}
    echo "Downloading database backup from S3..."
    aws s3 cp "s3://$AWS_S3_BUCKET_NAME/database/backups/fund-latest.dump.gz" /tmp/fund-latest.dump.gz
    rm -f /tmp/fund-latest.dump
    gunzip /tmp/fund-latest.dump.gz
    psql -h localhost -p 5432 -d fund -c "SELECT timescaledb_pre_restore();"
    pg_restore --host 127.0.0.1 --port 5432 \
      --no-owner --no-acl \
      --dbname fund --clean --if-exists /tmp/fund-latest.dump || true
    psql -h localhost -p 5432 -d fund -c "SELECT timescaledb_post_restore();"
    rm -f /tmp/fund-latest.dump
    echo "Database restored"
  '';

  scripts.database-backup.exec = ''
    set -euo pipefail
    ${runtimeEnv}
    echo "Creating database backup..."
    pg_dump -Fc -h localhost -p 5432 fund > /tmp/fund-latest.dump
    gzip -f /tmp/fund-latest.dump
    echo "Uploading backup to S3..."
    aws s3 cp /tmp/fund-latest.dump.gz "s3://$AWS_S3_BUCKET_NAME/database/backups/fund-latest.dump.gz"
    rm -f /tmp/fund-latest.dump.gz
    echo "Database backup complete"
  '';

  scripts.database-fetch-equity-details.exec = ''
    set -euo pipefail
    ${runtimeEnv}
    echo "Downloading equity details from S3..."
    aws s3 cp "s3://$AWS_S3_BUCKET_NAME/data/equity/details/details.csv" /tmp/equity_details.csv
    echo "Loading equity details into database..."
    psql -h localhost -p 5432 -d fund <<'SQL'
      CREATE TEMP TABLE tmp_equity_details (LIKE equity_details INCLUDING ALL);
      \COPY tmp_equity_details (ticker, sector, industry) FROM '/tmp/equity_details.csv' CSV HEADER
      INSERT INTO equity_details SELECT * FROM tmp_equity_details
        ON CONFLICT (ticker) DO UPDATE SET sector = EXCLUDED.sector, industry = EXCLUDED.industry;
      DROP TABLE tmp_equity_details;
    SQL
    rm -f /tmp/equity_details.csv
    echo "Equity details loaded"
  '';

  scripts.database-fetch-equity-bars.exec = ''
    set -euo pipefail

    if [ -z "''${BACKFILL_START_DATE:-}" ]; then
      echo "Usage: BACKFILL_START_DATE=YYYY-MM-DD devenv tasks run database:fetch-equity-bars"
      echo "  Optional: BACKFILL_END_DATE=YYYY-MM-DD (defaults to today)"
      exit 1
    fi

    END_DATE="''${BACKFILL_END_DATE:-$(date -u +%Y-%m-%d)}"

    echo "Waiting for data-manager to be healthy..."
    attempt=0
    max_attempts=30
    while ! curl -sf http://localhost:8080/health > /dev/null 2>&1; do
      attempt=$((attempt + 1))
      if [ "$attempt" -ge "$max_attempts" ]; then
        echo "data-manager did not become healthy after $((max_attempts * 2)) seconds"
        exit 1
      fi
      sleep 2
    done
    echo "Data-manager is healthy"

    echo "Fetching equity bars from $BACKFILL_START_DATE to $END_DATE"
    secretspec run -- uv run --package tools python -m tools.sync_equity_bars_data \
      http://localhost:8080 \
      "{\"start_date\": \"$BACKFILL_START_DATE\", \"end_date\": \"$END_DATE\"}"
  '';

  scripts.database-reset.exec = ''
    set -euo pipefail
    echo "Resetting fund database..."
    psql -h localhost -p 5432 -d postgres -c "DROP DATABASE IF EXISTS fund WITH (FORCE)"
    psql -h localhost -p 5432 -d postgres -c "CREATE DATABASE fund"
    echo "Fund database reset"
  '';

  scripts.aws-buckets.exec = ''
    set -euo pipefail
    ${runtimeEnv}
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
    cargo clippy --workspace --all-features --all-targets
    echo "Rust linting completed successfully"
  '';

  scripts.rust-test.exec = ''
        set -euo pipefail
        echo "Running Rust tests"

        TEST_ARGS="--workspace --verbose --lib --bins --all-features"

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
    ${runtimeEnv}
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

    # Rust-native TiDE training (burn). Reads bars + details from S3, trains, and
    # uploads a model.tar.gz the ensemble inference service loads directly. The
    # former Python/tinygrad workflow and its Prefect block registration are
    # retired.
    "models:tide:train".exec = ''
      set -euo pipefail
      echo "Running tide training pipeline (Rust + burn)"
      ${runtimeEnv}
      secretspec run -- cargo run --release --no-default-features --features train --bin tide_train
    '';

    # --- Data tasks ---

    # Historical S3 backfill: writes Hive-partitioned parquet that model
    # training reads directly. Distinct from database:fetch-equity-bars, which
    # populates the PostgreSQL rolling buffer through the data-manager API.
    "data:backfill-bars".exec = "backfill-bars";

    # --- Database lifecycle tasks ---
    # Create: first-time setup or post-reset recovery after a breaking schema change.
    #   Applies schema, then seeds equity details, then backfills equity bars.
    #   Requires BACKFILL_START_DATE=YYYY-MM-DD (and optionally BACKFILL_END_DATE).
    # Update: automatic on each process start via applySchema in data-manager.exec.
    #   Safe for additive changes; loud failure for breaking changes.
    # Restore: fast recovery from a nightly S3 dump (schema must match the dump).

    "database:reset".exec = "database-reset";
    "database:restore".exec = "database-restore";
    "database:backup".exec = "database-backup";
    "database:fetch-equity-details".exec = "database-fetch-equity-details";
    "database:fetch-equity-bars".exec = "database-fetch-equity-bars";

    "database:create".exec = ''
      set -euo pipefail
      if [ -z "''${BACKFILL_START_DATE:-}" ]; then
        echo "Usage: BACKFILL_START_DATE=YYYY-MM-DD devenv tasks run database:create"
        echo "  Optional: BACKFILL_END_DATE=YYYY-MM-DD (defaults to today)"
        exit 1
      fi
      ${applySchema}
      database-fetch-equity-details
      database-fetch-equity-bars
    '';

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
      # The Rust ensemble service reads Burn-native artifacts; track the most
      # recent training run rather than pinning (the old pin protected the
      # retired tinygrad loader from Burn artifacts).
      MODEL_VERSION = "latest";
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
    in {
      data-manager.exec =
        if isProduction
        then ''
          set -euo pipefail
          ${runtimeEnv}
          ${waitForPostgres}
          ${applySchema}
          ${killPort "8080"}
          exec secretspec run -- cargo run --no-default-features --features data_manager --bin data_manager --release
        ''
        else ''
          set -euo pipefail
          ${runtimeEnv}
          ${waitForPostgres}
          ${applySchema}
          ${killPort "8080"}
          exec secretspec run -- cargo watch -x 'run --no-default-features --features data_manager --bin data_manager'
        '';

      # Rust ensemble_model (Burn): serves predictions over HTTP and consumes
      # predictions_requested from the Postgres event bus. Replaces the former
      # Python ensemble_manager (uvicorn/tinygrad).
      ensemble-manager.exec =
        if isProduction
        then ''
          set -euo pipefail
          ${runtimeEnv}
          ${waitForPostgres}
          ${killPort "8082"}
          exec secretspec run -- cargo run --no-default-features --features ensemble_model --bin ensemble_model --release
        ''
        else ''
          set -euo pipefail
          ${runtimeEnv}
          ${waitForPostgres}
          ${killPort "8082"}
          exec secretspec run -- cargo watch -x 'run --no-default-features --features ensemble_model --bin ensemble_model'
        '';

      portfolio-manager.exec = let
        uvicornCmd = "uv run uvicorn portfolio_manager.server:application --host 0.0.0.0 --port 8081";
      in
        if isProduction
        then ''
          ${runtimeEnv}
          ${waitForPostgres}
          ${killPort "8081"}
          exec secretspec run -- ${uvicornCmd}
        ''
        else ''
          ${runtimeEnv}
          ${waitForPostgres}
          ${killPort "8081"}
          exec secretspec run -- ${uvicornCmd} --reload
        '';
    };
  };

  profiles.ml.module = {
    env = {
      FUND_LOOKBACK_DAYS = lookbackDays;
      MLFLOW_TRACKING_URI = "";
      PREFECT_API_URL = "";
    };

    scripts.train-local.exec = ''
      set -euo pipefail
      echo "Running local training pipeline (Rust + burn)"
      cargo run --release --no-default-features --features train --bin tide_train
    '';
  };

  enterShell = ''
    ${runtimeEnv}
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
      echo "    checks:base                    Non-language checks (nix, markdown, yaml, toml, sql)"
      echo "    checks:python                  All Python checks (parallel after install)"
      echo "    checks:rust                    All Rust checks (sequential: format, lint, test)"
      echo "    checks:markdown                Markdown lint"
      echo "    checks:yaml                    YAML lint"
      echo "    checks:toml                    TOML format check"
      echo "    checks:sql                     SQL lint (PostgreSQL)"
      echo "    checks:nix                     Nix checks (alejandra + statix)"
      echo "    database:create                First-time setup: apply schema + seed details + fetch bars"
      echo "    database:reset                 Drop and recreate empty fund database"
      echo "    database:restore               Restore from nightly S3 dump"
      echo "    database:backup                Dump fund database and upload to S3"
      echo "    database:fetch-equity-details  Seed equity_details from S3 CSV"
      echo "    database:fetch-equity-bars     Backfill equity bars (requires BACKFILL_START_DATE)"
      echo "    models:tide:train              Train tide model and upload artifacts"
      echo ""
      echo "  Utilities:"
      echo "    bump-deps           Update all dependency lockfiles"
    } >&2
  '';

  enterTest = ''
  '';
}
