{pkgs, ...}: let
  awsRegion = "us-east-1";

  # Compute bucket name and secretspec profile at shell/process start time from
  # $FUND_PROFILE, which dotenv sets from .env. These cannot be baked in at Nix
  # evaluation time because dotenv runs after Nix evaluates devenv.nix. Model
  # artifacts live in the same per-profile bucket under models/tide/: the Rust
  # tide trainer (tide_model_trainer) writes there and the inference service
  # reads there, so training and serving agree in both dev and production.
  runtimeEnv = ''
    export AWS_S3_BUCKET_NAME="oscm-fund-$(echo ''${FUND_PROFILE} | tr '/.' '--')"
    export SECRETSPEC_PROFILE="''${FUND_PROFILE}"
    export AWS_S3_MODEL_ARTIFACT_PATH="models/tide/"
    if [[ ! -w "''${FUND_LOG_DIR:-/var/log/fund}" ]]; then
      export FUND_LOG_DIR="$HOME/.local/state/fund/log"
    else
      export FUND_LOG_DIR="''${FUND_LOG_DIR:-/var/log/fund}"
    fi
    mkdir -p "$FUND_LOG_DIR" 2>/dev/null || true
  '';

  applySchema = ''
    echo "Applying schema..."
    psql -h localhost -p 5432 -d fund \
      -f ${./schema.sql} \
      --quiet --set ON_ERROR_STOP=on --set client_min_messages=warning
    echo "Schema applied"
  '';

  # Training lookback window. Read from the environment so it can be overridden
  # per run (e.g. FUND_LOOKBACK_DAYS=1200 devenv --profile trainer ...); a hardcoded
  # empty default would both shadow the override and break int parsing in the
  # tide trainer, which only falls back to its own default when the var is
  # unset, not when it is the empty string.
  rawLookbackDays = builtins.getEnv "FUND_LOOKBACK_DAYS";
  lookbackDays =
    if rawLookbackDays == ""
    then "365"
    else rawLookbackDays;

  # Log directory. VMs use /var/log/fund (provisioned by bootstrap-machine).
  # The runtimeEnv block above detects when that path is not writable (e.g.
  # local laptop without bootstrap) and falls back to an XDG state path.
  fundLogDir = "/var/log/fund";
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

  # database:create  — apply the schema (idempotent DDL).
  # database:reset   — drop and recreate the empty fund database; run before database:create
  #                    after a breaking schema change.

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

  scripts.seed-equity-bars.exec = ''
    set -euo pipefail

    if [ -z "''${SEED_SOURCE:-}" ] || [ -z "''${SEED_TARGET:-}" ] || [ -z "''${SEED_START_DATE:-}" ]; then
      echo "Usage: SEED_SOURCE=<massive|s3> SEED_TARGET=<s3|postgresql|all> SEED_START_DATE=YYYY-MM-DD devenv tasks run data:equity-bars"
      echo "  Optional: SEED_END_DATE=YYYY-MM-DD (defaults to today)"
      echo ""
      echo "  Sources: massive (Massive API), s3 (existing S3 Parquet)"
      echo "  Targets: s3, postgresql, all"
      echo "  Note: --source s3 with --target s3 or --target all is not supported"
      exit 1
    fi

    END_DATE_ARGS=""
    if [ -n "''${SEED_END_DATE:-}" ]; then
      END_DATE_ARGS="$SEED_END_DATE"
    fi

    echo "Seeding equity bars: source=$SEED_SOURCE target=$SEED_TARGET from $SEED_START_DATE"
    ${runtimeEnv}
    secretspec run -- cargo run --no-default-features --features data --bin seed_equity_bars -- \
      --source "$SEED_SOURCE" --target "$SEED_TARGET" "$SEED_START_DATE" $END_DATE_ARGS
  '';

  scripts.seed-equity-details.exec = ''
    set -euo pipefail

    if [ -z "''${SEED_TARGET:-}" ]; then
      echo "Usage: SEED_TARGET=<s3|postgresql|all> devenv tasks run data:equity-details"
      echo ""
      echo "  Targets: s3, postgresql, all"
      exit 1
    fi

    echo "Seeding equity details: target=$SEED_TARGET"
    ${runtimeEnv}
    secretspec run -- cargo run --no-default-features --features data --bin seed_equity_details -- \
      --target "$SEED_TARGET"
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
    # uploads a model.tar.gz the inference service loads directly. The
    # former Python/tinygrad workflow and its Prefect block registration are
    # retired.
    "models:tide:train".exec = ''
      set -euo pipefail
      echo "Running tide training pipeline (Rust + burn)"
      ${runtimeEnv}
      secretspec run -- cargo run --release --no-default-features --features train --bin tide_model_trainer
    '';

    # --- Data tasks ---

    # Seed equity bars from Massive API or S3 into S3 and/or PostgreSQL.
    "data:equity-bars".exec = "seed-equity-bars";

    # Seed equity details from the embedded CSV into S3 and/or PostgreSQL.
    "data:equity-details".exec = "seed-equity-details";

    # Full bootstrap: seed equity details and equity bars into all targets.
    # Runs equity-details first (fast, no date range), then equity-bars.
    "data:seed" = {
      exec = ''
        set -euo pipefail

        if [ -z "''${SEED_START_DATE:-}" ]; then
          echo "Usage: SEED_SOURCE=<massive|s3> SEED_START_DATE=YYYY-MM-DD devenv tasks run data:seed"
          echo "  Optional: SEED_END_DATE=YYYY-MM-DD (defaults to today)"
          echo ""
          echo "  Seeds equity details and equity bars into both S3 and PostgreSQL."
          echo "  Source controls where equity bars are read from (massive or s3)."
          exit 1
        fi

        if [ -z "''${SEED_SOURCE:-}" ]; then
          echo "Error: SEED_SOURCE is required (massive or s3)"
          exit 1
        fi

        echo "=== Seeding equity details (target=all) ==="
        SEED_TARGET=all seed-equity-details

        # When source is s3, target=all is rejected (s3-to-s3 is a no-op).
        # Route to postgresql instead.
        if [ "$SEED_SOURCE" = "s3" ]; then
          BARS_TARGET="postgresql"
        else
          BARS_TARGET="all"
        fi

        echo ""
        echo "=== Seeding equity bars (source=$SEED_SOURCE target=$BARS_TARGET) ==="
        SEED_TARGET="$BARS_TARGET" seed-equity-bars
      '';
    };

    # --- Database lifecycle tasks ---
    # Two lifecycle modes:
    #   Create — apply the schema (idempotent DDL). Use on a fresh VM or after schema changes.
    #   Reset  — drop and recreate the empty database. Run before create after breaking changes.

    # Opens an interactive psql session against the local fund database.
    "database:connect".exec = "psql -h localhost -p 5432 -d fund";

    # Drops and recreates the empty fund database. Run before database:create when
    # recovering from a breaking schema change.
    "database:reset".exec = "reset-database";

    # Dumps the live database and uploads it to S3. Also runs automatically via
    # pg_cron at 22:00 UTC on weekdays after all nightly exports complete.
    "database:backup".exec = "backup-database";

    # Applies the schema to the fund database. Safe to re-run (all DDL is
    # idempotent). Use after database:reset or on a fresh VM.
    "database:create".exec = ''
      set -euo pipefail
      ${applySchema}
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
      # The inference service reads Burn-native artifacts; track the most
      # recent training run rather than pinning (the old pin protected the
      # retired tinygrad loader from Burn artifacts).
      MODEL_VERSION = "latest";
    };

    processes.fund.exec = ''
      set -euo pipefail
      ${runtimeEnv}
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
      ${applySchema}
      exec secretspec run -- cargo run --release --bin fund
    '';
  };

  profiles.trainer.module = {
    env = {
      FUND_LOOKBACK_DAYS = lookbackDays;
      MLFLOW_TRACKING_URI = "";
      PREFECT_API_URL = "";
    };
  };

  enterShell = ''
    ${runtimeEnv}
    {
      echo "Fund development environment (profile: $FUND_PROFILE)"
      echo ""
      echo "  Bucket: $AWS_S3_BUCKET_NAME"
      echo ""
      echo "  Profiles:"
      echo "    devenv --profile application up      Start application services"
      echo "    devenv --profile trainer shell     Model training environment"
      echo ""
      echo "  Services (application profile):"
      echo "    Fund:         consolidated event-driven binary"
      echo "    PostgreSQL:   localhost:5432/fund"
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
      echo "    database:connect               Open interactive psql session"
      echo "    database:create                Apply schema (idempotent, safe to re-run)"
      echo "    database:reset                 Drop and recreate the empty fund database"
      echo "                                   (run before database:create after a breaking schema change)"
      echo "    database:backup                Dump fund database and upload to S3 (also runs nightly at 22:00 UTC)"
      echo "    data:equity-bars               Seed equity bars (requires SEED_SOURCE, SEED_TARGET, SEED_START_DATE)"
      echo "    data:equity-details            Seed equity details (requires SEED_TARGET)"
      echo "    data:seed                      Full bootstrap: seed details + bars (requires SEED_SOURCE, SEED_START_DATE)"
      echo "    models:tide:train              Train tide model and upload artifacts"
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
