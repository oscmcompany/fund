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
  # tide trainer (tide_model_trainer) writes there and the Rust ensemble service
  # reads there, so training and serving agree in both dev and production.
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
    nix.enable = true;
  };

  git-hooks.hooks = {
    check-rust = {
      enable = true;
      name = "Check all Rust code";
      entry = "check-rust";
      files = "(\\.rs|Cargo\\.(toml|lock))$";
      pass_filenames = false;
      language = "system";
      fail_fast = true;
    };
    check-markdown = {
      enable = true;
      name = "Check all Markdown code";
      entry = "check-markdown";
      files = "\\.md$";
      pass_filenames = false;
      language = "system";
      fail_fast = true;
    };
    check-yaml = {
      enable = true;
      name = "Check all YAML code";
      entry = "check-yaml";
      files = "\\.(yaml|yml)$";
      pass_filenames = false;
      language = "system";
      fail_fast = true;
    };
    check-toml = {
      enable = true;
      name = "Check all TOML code";
      entry = "check-toml";
      files = "\\.toml$";
      pass_filenames = false;
      language = "system";
      fail_fast = true;
    };
    check-sql = {
      enable = true;
      name = "Check all SQL code";
      entry = "check-sql";
      files = "\\.sql$";
      pass_filenames = false;
      language = "system";
      fail_fast = true;
    };
    check-nix = {
      enable = true;
      name = "Check all Nix code";
      entry = "check-nix";
      files = "\\.nix$";
      pass_filenames = false;
      language = "system";
      fail_fast = true;
    };
    check-sqlx = {
      enable = true;
      name = "Check sqlx query metadata cache";
      entry = "check-sqlx";
      files = "\\.rs$|schema\\.sql$";
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

    # sqlx compile-time query checking uses the committed .sqlx/ cache rather
    # than a live database connection; run `cargo sqlx prepare -- --all-features`
    # to regenerate the cache after changing queries.
    SQLX_OFFLINE = "true";

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
      shared_preload_libraries = "timescaledb,pg_cron";
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
    duckdb # retained for local data exploration and experimentation
    gh
    git
    rainfrog
    jq
    llvmPackages.llvm
    markdownlint-cli
    postgresql_16
    rustup
    (sqlfluff.overridePythonAttrs (_: {
      # The aarch64-darwin binary is not cached on cache.nixos.org for this
      # nixpkgs revision; building from source runs the full pytest suite which
      # exceeds available memory (OOM kill). Tests are validated by Hydra when
      # producing the Linux binary cache entry.
      doCheck = false;
    }))
    sqlx-cli
    statix
    taplo
    uv # retained for local Python experimentation; use `uv venv` + `uv pip install` for project-scoped package installs
    yamllint
  ];

  # database:restore — fast recovery from a nightly S3 dump when schema has not changed.
  # database:create — first-time setup: apply schema, seed equity details, fetch equity bars.
  # database:reset  — drop and recreate the empty fund database; run before database:create
  #                   after a breaking schema change.

  scripts.restore-database.exec = ''
    set -euo pipefail
    ${runtimeEnv}
    BACKUP_KEY="''${AWS_S3_DATABASE_BACKUP_KEY:-database/backups/fund-latest.dump.gz}"
    echo "Downloading database backup from S3..."
    aws s3 cp "s3://$AWS_S3_BUCKET_NAME/$BACKUP_KEY" /tmp/fund-latest.dump.gz
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

  scripts.backup-database.exec = ''
    set -euo pipefail
    ${runtimeEnv}
    BACKUP_KEY="''${AWS_S3_DATABASE_BACKUP_KEY:-database/backups/fund-latest.dump.gz}"
    echo "Creating database backup..."
    pg_dump -Fc -h localhost -p 5432 fund > /tmp/fund-latest.dump
    gzip -f /tmp/fund-latest.dump
    echo "Uploading backup to S3..."
    aws s3 cp /tmp/fund-latest.dump.gz "s3://$AWS_S3_BUCKET_NAME/$BACKUP_KEY"
    rm -f /tmp/fund-latest.dump.gz
    echo "Database backup complete"
  '';

  scripts.fetch-database-equity-details.exec = ''
    set -euo pipefail
    ${runtimeEnv}
    echo "Downloading equity details from S3..."
    aws s3 cp "s3://$AWS_S3_BUCKET_NAME/data/equity/details/details.csv" /tmp/equity_details.csv
    echo "Loading equity details into database..."
    # The source CSV is the full listing export (11 columns); stage it verbatim
    # and project only the columns equity_details keeps. ON_ERROR_STOP makes a
    # malformed file fail the task instead of silently loading nothing.
    psql -h localhost -p 5432 -d fund --set ON_ERROR_STOP=on <<'SQL'
      CREATE TEMP TABLE tmp_equity_details (
        ticker TEXT,
        name TEXT,
        last_sale TEXT,
        net_change TEXT,
        percent_change TEXT,
        market_capitalization TEXT,
        country TEXT,
        ipo_year TEXT,
        volume TEXT,
        sector TEXT,
        industry TEXT
      );
      \COPY tmp_equity_details FROM '/tmp/equity_details.csv' CSV HEADER
      INSERT INTO equity_details (ticker, sector, industry)
        SELECT
          ticker,
          COALESCE(NULLIF(sector, '''), 'NOT AVAILABLE'),
          COALESCE(NULLIF(industry, '''), 'NOT AVAILABLE')
        FROM tmp_equity_details
        WHERE ticker IS NOT NULL
        ON CONFLICT (ticker) DO UPDATE SET sector = EXCLUDED.sector, industry = EXCLUDED.industry;
      DROP TABLE tmp_equity_details;
    SQL
    rm -f /tmp/equity_details.csv
    echo "Equity details loaded"
  '';

  scripts.fetch-database-equity-bars.exec = ''
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
    current_date="$BACKFILL_START_DATE"
    request_count=0
    while [[ "$current_date" <= "$END_DATE" ]]; do
      request_count=$((request_count + 1))
      date_string="''${current_date}T12:00:00Z"
      echo "Syncing equity bars for $current_date (request $request_count)"
      status_code=$(curl -s -o /dev/null -w "%{http_code}" \
        -X POST http://localhost:8080/equity-bars \
        -H "Content-Type: application/json" \
        -d "{\"date\": \"''${date_string}\"}" \
        --max-time 60)
      echo "Status: $status_code"
      if [ "$status_code" -ge 400 ]; then
        echo "Warning: request for $current_date returned HTTP $status_code"
      fi
      current_date=$(date -d "$current_date +1 day" +%Y-%m-%d 2>/dev/null \
        || date -jf "%Y-%m-%d" "$current_date" -v+1d +%Y-%m-%d)
    done
    echo "All dates processed ($request_count requests)"
  '';

  scripts.reset-database.exec = ''
    set -euo pipefail
    echo "Resetting fund database..."
    psql -h localhost -p 5432 -d postgres -c "DROP DATABASE IF EXISTS fund WITH (FORCE)"
    psql -h localhost -p 5432 -d postgres -c "CREATE DATABASE fund"
    echo "Fund database reset"
  '';

  scripts.list-aws-buckets.exec = ''
    set -euo pipefail
    ${runtimeEnv}
    unset AWS_ENDPOINT_URL
    echo "=== Fund S3 Buckets (profile: $FUND_PROFILE) ==="
    echo "  Bucket: $AWS_S3_BUCKET_NAME"
    echo ""
    buckets=$(aws s3 ls)
    printf '%s\n' "$buckets" | grep fund || echo "No fund buckets found"
  '';

  scripts.list-aws-secrets.exec = ''
    set -euo pipefail
    unset AWS_ENDPOINT_URL
    echo "=== Fund Secrets ==="
    aws secretsmanager list-secrets \
      --region ${awsRegion} \
      --query 'SecretList[?contains(Name, `fund`) || contains(Name, `secretspec`)].Name' \
      --output table
  '';

  # --- Development check scripts ---

  scripts.format-rust.exec = ''
    set -euo pipefail
    echo "Checking Rust code formatting"
    cargo fmt --all -- --check
    echo "Rust code formatting check passed"
  '';

  scripts.lint-rust.exec = ''
    set -euo pipefail
    echo "Running Rust lint checks"
    cargo clippy --workspace --all-features --all-targets
    echo "Rust linting completed successfully"
  '';

  scripts.test-rust.exec = ''
    set -euo pipefail
    echo "Running Rust tests"

    TEST_ARGS="--lib --bins --all-features"

    mkdir -p .coverage_output
    export LLVM_COV=$(which llvm-cov)
    export LLVM_PROFDATA=$(which llvm-profdata)
    cargo llvm-cov $TEST_ARGS \
      --cobertura \
      --output-path .coverage_output/rust.xml

    rate=$(awk 'match($0, /line-rate="([^"]*)"/, a) {print a[1]; exit}' .coverage_output/rust.xml)
    rate_pct=$(awk "BEGIN {printf \"%.1f\", ''${rate:-0} * 100}")
    threshold=75
    echo "Rust line coverage: ''${rate_pct}%"
    if awk "BEGIN {exit !(''${rate_pct} + 0 < ''${threshold})}"; then
      echo "Coverage failure: ''${rate_pct}% is below threshold of ''${threshold}%"
      exit 1
    fi

    echo "Rust tests with coverage completed successfully"
  '';

  scripts.check-rust.exec = ''
    devenv tasks run checks:rust
  '';

  scripts.check-sqlx.exec = ''
    set -euo pipefail
    if ! pg_isready -q 2>/dev/null; then
      echo "sqlx prepare check skipped: database not available"
      echo "Run 'devenv --profile application up' then 'cargo sqlx prepare -- --all-features' to verify the cache"
      exit 0
    fi
    echo "Checking sqlx query metadata cache is up to date"
    cargo sqlx prepare --check -- --all-features
    echo "sqlx prepare check passed"
  '';

  scripts.check-markdown.exec = ''
    set -euo pipefail
    echo "Running Markdown lint checks"
    markdownlint "**/*.md" --ignore ".venv" \
      --ignore "target" --ignore ".scratchpad"
    echo "Markdown checks completed successfully"
  '';

  scripts.check-yaml.exec = ''
    set -euo pipefail
    echo "Running YAML lint checks"
    yamllint .
    echo "YAML checks completed successfully"
  '';

  scripts.check-toml.exec = ''
    set -euo pipefail
    echo "Running TOML checks"
    find . \
      \( -path "./.devenv" -o -path "./target" -o -path "./.venv" \) -prune \
      -o -name "*.toml" -print \
      | xargs taplo fmt --check --no-auto-config
    echo "TOML checks completed successfully"
  '';

  scripts.check-sql.exec = ''
    set -euo pipefail
    echo "Running SQL checks"
    sqlfluff lint .
    echo "SQL checks completed successfully"
  '';

  scripts.check-nix.exec = ''
    set -euo pipefail
    echo "Checking Nix code formatting"
    alejandra --check --exclude ./.devenv --exclude ./.venv --exclude ./target .
    echo "Nix formatting check passed"
    echo "Running Nix static analysis"
    statix check -c .statix.toml .
    echo "Nix checks completed successfully"
  '';

  scripts.bump-rust-dependencies.exec = ''
    set -euo pipefail
    echo "Bumping all dependencies..."
    echo "=== Rust ==="
    cargo update
    echo ""
    echo "Dependencies bumped. Review changes:"
    echo "  git diff Cargo.lock"
  '';

  scripts.trigger-rebalance.exec = ''
    psql -h localhost -p 5432 -d fund -c "SELECT emit_event('intraday_check',
    '{}')"
  '';

  scripts.provision-development-application-vm.exec = "bash tools/provision-application-vm";
  scripts.provision-production-application-vm.exec = "bash tools/provision-application-vm --production";
  scripts.provision-development-trainer-vm.exec = "bash tools/provision-trainer-vm";
  scripts.provision-production-trainer-vm.exec = "bash tools/provision-trainer-vm --production";

  scripts.backfill-s3-equity-bars.exec = ''
    set -euo pipefail

    if [ -z "''${BACKFILL_START_DATE:-}" ]; then
      echo "Usage: BACKFILL_START_DATE=YYYY-MM-DD devenv tasks run data:backfill-s3-equity-bars"
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
    # --- Rust checks (lint and test run in parallel after format) ---

    "checks:rust:format".exec = "format-rust";

    "checks:rust:lint" = {
      exec = "lint-rust";
      after = ["checks:rust:format"];
    };
    "checks:rust:test" = {
      exec = "test-rust";
      after = ["checks:rust:format"];
    };

    # --- Standalone checks ---

    "checks:markdown".exec = "check-markdown";
    "checks:yaml".exec = "check-yaml";
    "checks:toml".exec = "check-toml";
    "checks:sql".exec = "check-sql";
    "checks:nix".exec = "check-nix";

    # --- Model training ---

    # Rust-native TiDE training (burn). Reads bars + details from S3, trains, and
    # uploads a model.tar.gz the ensemble inference service loads directly. The
    # former Python/tinygrad workflow and its Prefect block registration are
    # retired.
    "models:tide:train".exec = ''
      set -euo pipefail
      echo "Running tide training pipeline (Rust + burn)"
      ${runtimeEnv}
      secretspec run -- cargo run --release --no-default-features --features train --bin tide_model_trainer
    '';

    # --- Data tasks ---

    # Historical S3 backfill: writes Hive-partitioned parquet that model
    # training reads directly. Distinct from database:fetch-equity-bars, which
    # populates the PostgreSQL rolling buffer through the data-manager API.
    "data:backfill-s3-equity-bars".exec = "backfill-s3-equity-bars";

    # --- Database lifecycle tasks ---
    # Three lifecycle modes:
    #   Create  — build a working database from scratch (schema change or fresh VM).
    #   Update  — automatic on each data-manager start; safe for additive schema changes.
    #   Restore — fast recovery from the nightly S3 dump when schema has not changed.

    # Drops and recreates the empty fund database. Run before database:create when
    # recovering from a breaking schema change.
    "database:reset".exec = "reset-database";

    # Downloads the nightly pg_dump from S3 and restores the full database: equity
    # bars, details, predictions, model runs, and all trading history. Fast, but
    # requires the schema to match the dump exactly.
    "database:restore".exec = "restore-database";

    # Dumps the live database and uploads it to S3. Also runs automatically via
    # pg_cron at 22:00 UTC on weekdays after all nightly exports complete.
    "database:backup".exec = "backup-database";

    "database:fetch-equity-details".exec = "fetch-database-equity-details";
    "database:fetch-equity-bars".exec = "fetch-database-equity-bars";

    # Builds an inference-ready database from scratch: applies the schema, seeds
    # equity details, and backfills equity bars from the live API. Use after
    # database:reset when the schema has changed or on a fresh VM. No trading
    # history is restored. Requires BACKFILL_START_DATE=YYYY-MM-DD.
    "database:create".exec = ''
      set -euo pipefail
      if [ -z "''${BACKFILL_START_DATE:-}" ]; then
        echo "Usage: BACKFILL_START_DATE=YYYY-MM-DD devenv tasks run database:create"
        echo "  Optional: BACKFILL_END_DATE=YYYY-MM-DD (defaults to today)"
        exit 1
      fi
      ${applySchema}
      fetch-database-equity-details
      fetch-database-equity-bars
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
        "checks:rust:format"
        "checks:rust:lint"
        "checks:rust:test"
      ];
    };
  };

  # --- Profiles ---

  profiles.application.module = {
    env = {
      DISABLE_DISK_CACHE = "1";
      BACKFILL_LOOKBACK_DAYS = "730";
      DATABASE_URL = "postgresql://localhost:5432/fund";
      # The Rust ensemble service reads Burn-native artifacts; track the most
      # recent training run rather than pinning (the old pin protected the
      # retired tinygrad loader from Burn artifacts).
      MODEL_VERSION = "latest";
    };

    scripts.cleanup-application-services.exec = ''
      for PORT in 8080 8082 8083; do
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

      # ensemble_manager: serves predictions over HTTP and consumes
      # predictions_requested from the Postgres event bus.
      ensemble-manager.exec =
        if isProduction
        then ''
          set -euo pipefail
          ${runtimeEnv}
          ${waitForPostgres}
          ${killPort "8082"}
          exec secretspec run -- cargo run --no-default-features --features ensemble_manager --bin ensemble_manager --release
        ''
        else ''
          set -euo pipefail
          ${runtimeEnv}
          ${waitForPostgres}
          ${killPort "8082"}
          exec secretspec run -- cargo watch -x 'run --no-default-features --features ensemble_manager --bin ensemble_manager'
        '';

      portfolio-manager.exec =
        if isProduction
        then ''
          set -euo pipefail
          ${runtimeEnv}
          ${waitForPostgres}
          ${killPort "8083"}
          exec secretspec run -- cargo run --no-default-features --features portfolio_manager --bin portfolio_manager --release
        ''
        else ''
          set -euo pipefail
          ${runtimeEnv}
          ${waitForPostgres}
          ${killPort "8083"}
          exec secretspec run -- cargo watch -x 'run --no-default-features --features portfolio_manager --bin portfolio_manager'
        '';
    };
  };

  profiles.ml.module = {
    env = {
      FUND_LOOKBACK_DAYS = lookbackDays;
      MLFLOW_TRACKING_URI = "";
      PREFECT_API_URL = "";
    };
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
      echo "    devenv --profile application up      Start application services"
      echo "    devenv --profile ml shell     ML training environment"
      echo ""
      echo "  Services (application profile):"
      echo "    Data Manager:      localhost:8080"
      echo "    Ensemble Manager:  localhost:8082"
      echo "    Portfolio Manager: localhost:8083"
      echo "    PostgreSQL:        localhost:5432/fund"
      echo ""
      echo "  Secrets (secretspec):"
      echo "    secretspec check          Validate production secrets"
      echo "    secretspec set <KEY>      Set a secret value"
      echo ""
      echo "  AWS:"
      echo "    list-aws-buckets       List fund S3 buckets"
      echo "    list-aws-secrets       List fund secrets"
      echo ""
      echo "  Tasks (devenv tasks run):"
      echo "    checks:base                    Non-language checks (nix, markdown, yaml, toml, sql)"
      echo "    checks:rust                    All Rust checks (sequential: format, lint, test)"
      echo "    checks:markdown                Markdown lint"
      echo "    checks:yaml                    YAML lint"
      echo "    checks:toml                    TOML format check"
      echo "    checks:sql                     SQL lint (PostgreSQL)"
      echo "    checks:nix                     Nix checks (alejandra + statix)"
      echo "    database:create                First-time setup: apply schema + seed details + fetch bars"
      echo "                                   (use when schema changed or starting fresh)"
      echo "    database:reset                 Drop and recreate the empty fund database"
      echo "                                   (run before database:create after a breaking schema change)"
      echo "    database:restore               Restore from nightly S3 dump (fast recovery, schema must match)"
      echo "    database:backup                Dump fund database and upload to S3 (also runs nightly at 22:00 UTC)"
      echo "    database:fetch-equity-details  Seed equity_details from S3 CSV"
      echo "    database:fetch-equity-bars     Backfill equity bars into PostgreSQL (requires BACKFILL_START_DATE)"
      echo "    data:backfill-s3-equity-bars         Backfill equity bars to S3 Parquet for training (requires BACKFILL_START_DATE)"
      echo "    models:tide:train                    Train tide model and upload artifacts"
      echo ""
      echo "  VM provisioning:"
      echo "    provision-development-application-vm  Provision a development application VM"
      echo "    provision-production-application-vm   Provision a production application VM"
      echo "    provision-development-trainer-vm      Provision a development trainer VM"
      echo "    provision-production-trainer-vm       Provision a production trainer VM"
      echo ""
      echo "  Utilities:"
      echo "    bump-rust-dependencies           Update all dependency lockfiles"
    } >&2
  '';

  enterTest = ''
  '';
}
