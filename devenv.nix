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
    echo "Applying dashboard reader role..."
    psql -h localhost -p 5432 -d fund \
      -f ${./tools/dashboard_reader_setup.sql} \
      --quiet --set ON_ERROR_STOP=on --set client_min_messages=warning
    echo "Dashboard reader role applied"
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

  # S3-compatible object store for the integration test suite. Defined here so
  # the process definition and the environment handed to the tests cannot drift
  # apart; `tests/common` reads these values from the environment.
  #
  # SeaweedFS rather than MinIO: MinIO is marked insecure in this nixpkgs
  # (CVE-2026-40344 and CVE-2026-41145 are unauthenticated object-write
  # bypasses), and using it would mean adding it to permittedInsecurePackages —
  # a project-wide statement that a known-vulnerable package is acceptable, to
  # gain nothing SeaweedFS does not already provide here.
  #
  # The credentials are arbitrary. SeaweedFS with no S3 configuration file
  # accepts any signature, but the AWS SDK still requires non-empty values to
  # sign a request, so the tests must send something.
  objectStorePort = 8333;
  objectStoreAccessKey = "fundtest";
  objectStoreSecretKey = "fundtestsecret";
  objectStoreEndpoint = "http://127.0.0.1:${toString objectStorePort}";
in {
  dotenv.enable = true;

  languages = {
    rust.enable = true;
    nix.enable = true;
  };

  git-hooks.hooks = {
    # Ordered before check-rust deliberately. The sqlx check takes about four
    # seconds against a warm build; check-rust runs fmt, clippy over all targets,
    # and the test suite with coverage. Schema drift is the failure most likely
    # to be caught here, so surfacing it in seconds rather than after minutes of
    # unrelated work is worth the ordering.
    check-sqlx = {
      enable = true;
      name = "Check sqlx query metadata cache";
      entry = "check-sqlx";
      files = "\\.rs$|schema\\.sql$";
      pass_filenames = false;
      language = "system";
      fail_fast = true;
    };
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

    # S3-compatible endpoint and credentials for the integration test suite.
    # Set unconditionally rather than inside the test profile so `cargo test`
    # works from a plain devenv shell; only the MinIO process itself is
    # profile-scoped.
    TEST_S3_ENDPOINT = objectStoreEndpoint;
    TEST_S3_ACCESS_KEY = objectStoreAccessKey;
    TEST_S3_SECRET_KEY = objectStoreSecretKey;
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
      "cron.log_run" = "on";
    };
  };

  packages = with pkgs; [
    alejandra
    awscli2
    clang
    bacon
    cargo-llvm-cov
    cargo-machete
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
    seaweedfs
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

  # No `-U exedev`. That is the VM's OS user, so hardcoding it made the two tasks that matter most
  # during a cutover -- drop, recreate -- fail everywhere else with `role "exedev" does not exist`.
  # Letting libpq default to the invoking OS user is what every other psql call in this file does,
  # and on the VM the invoking user *is* exedev, so the behaviour there is unchanged.
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

  scripts.check-unused-dependencies.exec = ''
    set -euo pipefail
    echo "Checking for unused Rust dependencies"
    cargo machete
    echo "No unused dependencies found"
  '';

  # Brings up the services the integration targets need, if they are not
  # already listening. Mirrors what check-sqlx does for the database: a check
  # that cannot run is worse than no check, and the alternative is every
  # developer remembering to start two processes before every commit.
  scripts.ensure-test-services.exec = ''
    set -euo pipefail
    # Any HTTP response means the object store is listening. `curl -f` would
    # treat a 403 as down, and an unsigned GET / returns 403 on MinIO and on
    # real S3 — TEST_S3_ENDPOINT is overridable, so the probe must not depend
    # on one server answering 200 there.
    object_store_up() {
      [ -n "$(curl -s -o /dev/null -w '%{http_code}' --max-time 2 "${objectStoreEndpoint}" 2>/dev/null | grep -v '^000$')" ]
    }

    needed=""
    pg_isready -q 2>/dev/null || needed="postgres"
    object_store_up || needed="$needed object-store"

    if [ -z "$needed" ]; then
      exit 0
    fi

    echo "Starting test services:$needed"
    devenv --profile test up $needed --detach >/dev/null 2>&1 || true

    for _ in $(seq 1 60); do
      pg_ok=false
      store_ok=false
      pg_isready -q 2>/dev/null && pg_ok=true
      object_store_up && store_ok=true
      if [ "$pg_ok" = true ] && [ "$store_ok" = true ]; then
        exit 0
      fi
      sleep 1
    done

    echo "Test services did not start within 60s"
    echo "  postgres:     $(pg_isready -q 2>/dev/null && echo ready || echo down)"
    echo "  object-store: ${objectStoreEndpoint}"
    echo "Start them with 'devenv --profile test up postgres object-store --detach'."
    exit 1
  '';

  scripts.test-rust.exec = ''
    set -euo pipefail
    ensure-test-services
    echo "Running Rust tests"

    # Integration test targets are named individually rather than passing
    # --tests, so adding a target is a deliberate act and a new file cannot
    # join the gate unnoticed. Every target here runs against the devenv-managed
    # PostgreSQL and MinIO; none needs a container runtime.
    #
    # Before this, target selection was --lib --bins, which silently excluded
    # every file under tests/ — around 1,300 lines that compiled under clippy
    # but whose assertions had never been executed.
    #
    # Each integration target owns its own database (fund_test_database,
    # fund_test_handlers, fund_test_dashboard) recreated on first use —
    # #[serial] only serializes within a process, so a shared database would have
    # two binaries deleting from the same tables concurrently.
    #
    # test_dashboard is not optional cover. The dashboard is the only part of the
    # tree using raw sqlx::query rather than the checked macros, so nothing else
    # catches a mistyped column there until the page is loaded.
    TEST_ARGS="--lib --bins --all-features --test test_database --test test_handlers --test test_dashboard"

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
    # The committed .sqlx/ cache can disagree with schema.sql, and every offline
    # build believes the cache. Only a live connection catches that, so a
    # missing database is a failure rather than a pass: exiting 0 here reported
    # success for a check that had not run, which is worse than not having it.
    if ! pg_isready -q 2>/dev/null; then
      echo "Starting PostgreSQL to verify the sqlx query metadata cache"
      devenv up postgres --detach >/dev/null 2>&1 || true
      for attempt in $(seq 1 30); do
        if pg_isready -q 2>/dev/null; then
          break
        fi
        sleep 1
      done
    fi
    if ! pg_isready -q 2>/dev/null; then
      echo "sqlx prepare check FAILED: no database after 30s"
      echo "Start it with 'devenv up postgres --detach', then re-run the commit."
      echo "The .sqlx/ cache cannot be validated against schema.sql without one."
      exit 1
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

  scripts.start-duckdb.exec = ''
    set -euo pipefail

    if [ -z "''${1:-}" ]; then
      echo "Usage: start-duckdb <bucket-name>" >&2
      echo "" >&2
      echo "Examples:" >&2
      echo "  start-duckdb oscm-fund-production" >&2
      echo "  start-duckdb oscm-fund-development-john-forstmeier" >&2
      exit 1
    fi

    export AWS_S3_BUCKET_NAME="$1"
    echo "Opening DuckDB lab (bucket: $AWS_S3_BUCKET_NAME)"

    exec duckdb ~/lab.duckdb -init "$DEVENV_ROOT/tools/duckdb_initialization.sql"
  '';

  # Emits the same event pg_cron raises every five minutes during a session, so a manual trigger
  # exercises the identical handler path. Named for the command it emits; there is no "rebalance"
  # in the vocabulary any more.
  scripts.trigger-evaluation.exec = ''
    psql -h localhost -p 5432 -d fund -c "SELECT emit_event('portfolio_evaluation_requested', jsonb_build_object('reason', 'manual'))"
  '';

  scripts.trigger-liquidation.exec = ''
    psql -h localhost -p 5432 -d fund -c "SELECT emit_event('portfolio_liquidation_requested', jsonb_build_object('reason', 'manual'))"
  '';

  scripts.provision-development-application-vm.exec = "bash tools/provision-application-vm --environment development";
  scripts.provision-production-application-vm.exec = "bash tools/provision-application-vm --environment production";
  scripts.provision-development-trainer-vm.exec = "bash tools/provision-trainer-vm --environment development";
  scripts.provision-production-trainer-vm.exec = "bash tools/provision-trainer-vm --environment production";

  scripts.start-application.exec = ''
    set -euo pipefail

    # Idempotent: skip if tmux session already exists
    if tmux has-session -t fund 2>/dev/null; then
      echo "Application is already running (tmux session 'fund' exists)"
      echo "  tmux attach -t fund    # attach to session"
      exit 0
    fi

    # Idempotent: install cron entry only if not already present
    if ! crontab -l 2>/dev/null | grep -qF 'sync-application'; then
      (crontab -l 2>/dev/null || true; echo '* * * * * bash ~/fund-cron.sh tools/sync-application >> /var/log/fund/sync-application.log 2>&1') | crontab -
      echo "Installed sync-application cron entry"
    else
      echo "Sync cron entry already installed"
    fi

    # Start devenv in a tmux session with a restart loop
    tmux new-session -d -s fund 'cd ~/fund && while true; do devenv --profile application up; sleep 5; done'
    echo "Application started in tmux session 'fund'"
    echo "  tmux attach -t fund    # attach to session"
  '';

  scripts.stop-application.exec = ''
    set -euo pipefail

    # Remove cron entry
    if crontab -l 2>/dev/null | grep -qF 'sync-application'; then
      crontab -l 2>/dev/null | grep -vF 'sync-application' | crontab - || true
      echo "Removed sync-application cron entry"
    else
      echo "No sync cron entry to remove"
    fi

    # Stop devenv processes
    pkill -TERM -u "$USER" -f "process-compose" 2>/dev/null && echo "Sent SIGTERM to process-compose" || true
    pkill -TERM -u "$USER" -f "devenv.*--profile application" 2>/dev/null && echo "Sent SIGTERM to devenv" || true

    # Kill tmux session (breaks the restart loop)
    if tmux has-session -t fund 2>/dev/null; then
      tmux kill-session -t fund
      echo "Killed tmux session 'fund'"
    else
      echo "No tmux session to kill"
    fi

    echo "Application stopped"
  '';

  scripts.start-trainer.exec = ''
    set -euo pipefail

    # Two entries, each checked independently. A single early exit on the first one would mean a
    # machine provisioned before the sync entry existed never gets it, which is the drift the sync
    # entry is there to prevent.

    # 23:00 UTC on weekdays: 19:00 Eastern in summer, 18:00 in winter, so it is post-close in both
    # halves of the year without a daylight-saving rule in the crontab. The artifact is then ready
    # the evening before the session that uses it rather than three hours before, which is more
    # margin for a failed run, not less.
    #
    # A fixed UTC hour is what makes that true year-round. 06:00 UTC, the previous schedule, is
    # 01:00 or 02:00 Eastern -- after the close it was named for, but on the wrong side of
    # midnight, so the run and the session it served never shared a date.
    if crontab -l 2>/dev/null | grep -qF 'train-tide-model'; then
      echo "Training cron entry already installed"
    else
      (crontab -l 2>/dev/null || true; echo '0 23 * * 1-5 bash ~/fund-cron.sh tools/train-tide-model >> /var/log/fund/train-tide-model.log 2>&1') | crontab -
      echo "Installed training cron entry (weekdays 23:00 UTC, post-close Eastern)"
    fi

    if crontab -l 2>/dev/null | grep -qF 'sync-trainer'; then
      echo "Sync cron entry already installed"
    else
      (crontab -l 2>/dev/null || true; echo '* * * * * bash ~/fund-cron.sh tools/sync-trainer >> /var/log/fund/sync-trainer.log 2>&1') | crontab -
      echo "Installed sync-trainer cron entry"
    fi
  '';

  scripts.stop-trainer.exec = ''
    set -euo pipefail

    if crontab -l 2>/dev/null | grep -qF 'train-tide-model'; then
      crontab -l 2>/dev/null | grep -vF 'train-tide-model' | crontab - || true
      echo "Removed training cron entry"
    else
      echo "No training cron entry to remove"
    fi

    if crontab -l 2>/dev/null | grep -qF 'sync-trainer'; then
      crontab -l 2>/dev/null | grep -vF 'sync-trainer' | crontab - || true
      echo "Removed sync-trainer cron entry"
    else
      echo "No sync cron entry to remove"
    fi
  '';

  # Bars come from Alpaca and go into PostgreSQL. There is no source or target to choose any more:
  # Massive is no longer a provider, and the trainer fetches and archives its own S3 parquet rather
  # than reading what a seed run left behind.
  scripts.seed-equity-bars.exec = ''
    set -euo pipefail

    if [ -z "''${SEED_START_DATE:-}" ]; then
      echo "Usage: SEED_START_DATE=YYYY-MM-DD devenv tasks run data:equity-bars"
      echo "  Optional: SEED_END_DATE=YYYY-MM-DD (defaults to today, US/Eastern)"
      echo ""
      echo "  Fetches daily bars from Alpaca for the tradable universe and upserts them into"
      echo "  equity_bars. Safe to re-run over a range already seeded."
      exit 1
    fi

    echo "Seeding equity bars from $SEED_START_DATE to ''${SEED_END_DATE:-today}"
    ${runtimeEnv}
    secretspec run -- cargo run --release --bin seed_equity_bars -- \
      "$SEED_START_DATE" ''${SEED_END_DATE:-}
  '';

  scripts.seed-equity-details.exec = ''
    set -euo pipefail

    echo "Seeding equity details from the embedded CSV"
    ${runtimeEnv}
    secretspec run -- cargo run --release --bin seed_equity_details
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
    "checks:rust:unused-dependencies" = {
      exec = "check-unused-dependencies";
      after = ["checks:rust:format"];
    };

    # --- Standalone checks ---

    "checks:markdown".exec = "check-markdown";
    "checks:yaml".exec = "check-yaml";
    "checks:toml".exec = "check-toml";
    "checks:sql".exec = "check-sql";
    "checks:nix".exec = "check-nix";

    # --- Model training ---

    # Rust-native TiDE training (burn). Fetches its own bars from Alpaca, archives them to S3,
    # trains against the accumulated window, and uploads a model.tar.gz the service loads directly.
    # The former Python/tinygrad workflow and its Prefect block registration are retired.
    "models:tide:train".exec = ''
      set -euo pipefail
      echo "Running tide training pipeline (Rust + burn)"
      ${runtimeEnv}
      secretspec run -- cargo run --release --bin tide_model_trainer
    '';

    # --- Data tasks ---

    # Seed equity bars from Alpaca into PostgreSQL over a date range.
    "data:equity-bars".exec = "seed-equity-bars";

    # Seed ticker metadata from the embedded CSV into PostgreSQL.
    "data:equity-details".exec = "seed-equity-details";

    # Full bootstrap for an empty database. Details first, because they are fast and carry no date
    # range, and because the pair screen's sector rule silently admits nothing without them.
    "data:seed" = {
      exec = ''
        set -euo pipefail

        if [ -z "''${SEED_START_DATE:-}" ]; then
          echo "Usage: SEED_START_DATE=YYYY-MM-DD devenv tasks run data:seed"
          echo "  Optional: SEED_END_DATE=YYYY-MM-DD (defaults to today, US/Eastern)"
          echo ""
          echo "  Seeds ticker metadata and daily bars into PostgreSQL. The screen needs 60"
          echo "  sessions of aligned closes and the model 70, so allow at least six months."
          exit 1
        fi

        echo "=== Seeding equity details ==="
        seed-equity-details

        echo ""
        echo "=== Seeding equity bars ==="
        seed-equity-bars
      '';
    };

    # --- Database lifecycle tasks ---
    # Two lifecycle modes:
    #   Create — apply the schema (idempotent DDL). Use on a fresh VM or after schema changes.
    #   Reset  — drop and recreate the empty database. Run before create after breaking changes.

    # Opens an interactive psql session against the local fund database.
    "database:connect".exec = "exec psql -h localhost -p 5432 -d fund";

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

    # --- Lab tasks ---

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

    "checks:all" = {
      exec = ''
        echo "All checks passed"
      '';
      after = [
        "checks:base"
        "checks:rust:format"
        "checks:rust:lint"
        "checks:rust:test"
        "checks:rust:unused-dependencies"
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

    # Shared setup: wait for PostgreSQL and apply schema before any module starts.
    # process-compose `depends_on` ensures this completes first.
    processes.schema.exec = ''
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
    '';

    # One service process, not three.
    #
    # `data`, `inference`, and `portfolio` were separate processes distinguished by a `--module`
    # flag, each running its own scheduler and its own consumer. The rebuild replaced all of that
    # with one binary woken by pg_cron through LISTEN/NOTIFY: there is no `--module` flag left to
    # pass, and splitting one event loop across three processes would mean three listeners racing
    # for the same events.
    #
    # The shutdown timeout is the handler drain bound plus headroom. `bin/fund` waits for running
    # handlers before returning, and killing it mid-drain is exactly the outcome the drain exists
    # to prevent -- a liquidation stopped between two broker orders.
    #
    # Launched through `run-with-secrets`, not `secretspec run` directly. secretspec does not
    # forward SIGTERM: it exits on the signal and orphans the binary underneath it, so the drain
    # above never ran and every restart left a service behind. See that script for the measurement.
    processes.fund = {
      exec = ''
        set -euo pipefail
        ${runtimeEnv}
        exec bash "$DEVENV_ROOT/tools/run-with-secrets" cargo run --release --bin fund
      '';
      process-compose.depends_on.schema.condition = "process_completed_successfully";
      process-compose.shutdown.signal = 15;
      process-compose.shutdown.timeout_seconds = 120;
    };

    processes.dashboard = {
      exec = ''
        set -euo pipefail
        ${runtimeEnv}
        export DATABASE_URL="postgresql://dashboard_reader@localhost:5432/fund"
        exec cargo run --release --bin dashboard
      '';
      process-compose.depends_on.schema.condition = "process_completed_successfully";
      process-compose.shutdown.signal = 15;
      process-compose.shutdown.timeout_seconds = 30;
    };
  };

  profiles.trainer.module = {
    env = {
      FUND_LOOKBACK_DAYS = lookbackDays;
      MLFLOW_TRACKING_URI = "";
      PREFECT_API_URL = "";
    };
  };

  # Test profile: an S3-compatible object store for the integration suite.
  #
  # Scoped to its own profile rather than declared top level, because profile
  # modules merge with the top level and a top-level process would therefore
  # start in production, which has no use for it. PostgreSQL is top level by
  # contrast because production genuinely needs it.
  #
  # This replaces the LocalStack container the suite used to start through
  # testcontainers. The tests never needed Docker as such — they needed an HTTP
  # endpoint speaking S3, and `create_test_s3_client` already builds its client
  # with an explicit endpoint and path-style addressing, which is exactly the
  # configuration an S3-compatible server wants.
  profiles.test.module = {
    processes.object-store.exec = ''
      set -euo pipefail
      OBJECT_STORE_DIR="''${DEVENV_STATE:-.devenv/state}/object-store"
      mkdir -p "$OBJECT_STORE_DIR"
      exec weed server \
        -dir="$OBJECT_STORE_DIR" \
        -ip=127.0.0.1 \
        -s3 \
        -s3.port=${toString objectStorePort}
    '';
  };

  enterShell = ''
    ${runtimeEnv}
    {
      echo "Fund development environment (profile: $FUND_PROFILE)"
      echo ""
      echo "  Bucket: $AWS_S3_BUCKET_NAME"
      echo ""
      echo "  Profiles:"
      echo "    devenv --profile application up      Start application processes"
      echo "    devenv --profile trainer shell       Model training environment"
      echo ""
      echo "  Processes (application profile):"
      echo "    postgresql                  PostgreSQL 16 with TimescaleDB"
      echo "                                and pg_cron (localhost:5432)"
      echo "    schema                      Apply database schema"
      echo "                                (runs first, then exits)"
      echo "    fund                        The service: one event loop woken"
      echo "                                by pg_cron. Predictions, the"
      echo "                                evaluation pass, liquidation, the"
      echo "                                account and market data syncs, and"
      echo "                                the nightly export"
      echo "    dashboard                   Monitoring UI (localhost:8084)"
      echo ""
      echo "  Scripts:"
      echo "    provision-{production|development}-{application|trainer}-vm"
      echo "                                Provision a VM on exe.dev for the"
      echo "                                given environment and role"
      echo "    start-application           Start application processes and"
      echo "                                install sync cron (run on VM)"
      echo "    stop-application            Stop application processes and"
      echo "                                remove sync cron (run on VM)"
      echo "    start-trainer               Install training cron job"
      echo "                                (run on VM)"
      echo "    stop-trainer                Remove training cron job"
      echo "                                (run on VM)"
      echo "    list-aws-buckets            List fund S3 buckets"
      echo "    list-aws-secrets            List fund secrets in AWS"
      echo "    trigger-evaluation          Emit a portfolio evaluation"
      echo "                                request manually"
      echo "    trigger-liquidation         Emit a portfolio liquidation"
      echo "                                request manually"
      echo "    start-duckdb                Open DuckDB with S3 data lake"
      echo "                                views pre-loaded (pass bucket name)"
      echo "    bump-rust-dependencies      Update all dependency lockfiles"
      echo ""
      echo "  Tasks (devenv tasks run <name>):"
      echo "    checks:rust                 All Rust checks (format, lint,"
      echo "                                test, unused-deps)"
      echo "    checks:base                 Non-language checks (nix, markdown,"
      echo "                                yaml, toml, sql)"
      echo "    checks:all                  All checks combined"
      echo "    database:connect            Open interactive psql session"
      echo "    database:create             Apply schema (idempotent)"
      echo "    database:reset              Drop and recreate empty fund"
      echo "                                database"
      echo "    database:backup             Dump database and upload to S3"
      echo "    data:seed                   Full data bootstrap (run without"
      echo "                                arguments for usage)"
      echo "    data:equity-bars            Seed equity bars (run without"
      echo "                                arguments for usage)"
      echo "    data:equity-details         Seed equity details (run without"
      echo "                                arguments for usage)"
      echo "    models:tide:train           Train TiDE model and upload"
      echo "                                artifacts"
    } >&2
  '';

  enterTest = ''
  '';
}
