{pkgs, ...}: let
  awsRegion = "us-east-1";

  # Compute bucket name and secretspec profile at shell/process start time from
  # $FUND_PROFILE, which dotenv sets from .env. These cannot be baked in at Nix
  # evaluation time because dotenv runs after Nix evaluates devenv.nix. Model
  # artifacts live in the same per-profile bucket under models/tide/: the Rust
  # tide trainer (tide_model_trainer) writes there and the inference service
  # reads there, so training and serving agree in both dev and production.
  #
  # Two buckets, split by whether the bytes are a provider-derived fact or a
  # record of what one instance did. The archive holds data/** and is shared:
  # production writes it, every instance reads it, and it is not per-profile
  # because the corpus is expensive and irreproducible once a subscription
  # lapses. Everything else -- exports, models, database backups -- stays in the
  # per-profile bucket, so a stray key is a bug rather than a judgement call.
  archiveBucket = "oscm-fund-archive";
  runtimeEnv = ''
    export AWS_S3_BUCKET_NAME="oscm-fund-$(echo ''${FUND_PROFILE} | tr '/.' '--')"
    export AWS_S3_ARCHIVE_BUCKET_NAME="${archiveBucket}"
    export SECRETSPEC_PROFILE="''${FUND_PROFILE}"
    export AWS_S3_MODEL_ARTIFACT_PATH="models/tide/"
    if [[ ! -w "''${FUND_LOG_DIRECTORY:-/var/log/fund}" ]]; then
      export FUND_LOG_DIRECTORY="$HOME/.local/state/fund/log"
    else
      export FUND_LOG_DIRECTORY="''${FUND_LOG_DIRECTORY:-/var/log/fund}"
    fi
    mkdir -p "$FUND_LOG_DIRECTORY" 2>/dev/null || true
    # Its own tree, not a subdirectory of the logs. The journal is the only original this
    # application owns, and the retention and rotation a log directory gets are the wrong ones for
    # it. Falls back beside the logs on a machine bootstrap has not reached.
    if [[ ! -w "''${FUND_JOURNAL_DIRECTORY:-/var/journal/fund}" ]]; then
      export FUND_JOURNAL_DIRECTORY="$HOME/.local/state/fund/journal"
    else
      export FUND_JOURNAL_DIRECTORY="''${FUND_JOURNAL_DIRECTORY:-/var/journal/fund}"
    fi
    mkdir -p "$FUND_JOURNAL_DIRECTORY" 2>/dev/null || true
  '';

  applySchema = ''
    ${postgresEnv}
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

  # PostgreSQL role. DATABASE_URL below names no user; psql falls back to the OS
  # user but sqlx does not, failing with `role "anonymous" does not exist`.
  #
  # Resolved in the shell rather than as an `env` attribute, because the obvious
  # spelling does not work: `builtins.getEnv "USER"` reads empty under the pure
  # evaluation devenv performs, so the attribute silently vanished and every
  # sqlx caller fell back to "anonymous". That is not a hypothetical -- it made
  # the check-sqlx pre-commit hook reject every Rust commit, with an error
  # naming a role nobody configured. `id -un` is answerable at run time, which
  # is the only time the OS user is actually knowable.
  #
  # Still not folded into DATABASE_URL, which would bake a machine-specific name
  # into a Nix store path.
  postgresEnv = ''
    export PGUSER="''${PGUSER:-$(id -un)}"
  '';

  # Log and journal directories. VMs use /var/log/fund and /var/journal/fund, both provisioned by
  # bootstrap-machine. The runtimeEnv block above detects when either path is not writable (e.g. a
  # local laptop without bootstrap) and falls back to an XDG state path.
  fundLogDirectory = "/var/log/fund";
  fundJournalDirectory = "/var/journal/fund";

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

    # Writable directories for the service logs and the journal (see above)
    FUND_LOG_DIRECTORY = fundLogDirectory;
    FUND_JOURNAL_DIRECTORY = fundJournalDirectory;

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
    echo "  Records: $AWS_S3_BUCKET_NAME"
    echo "  Archive: $AWS_S3_ARCHIVE_BUCKET_NAME (shared, holds data/**)"
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

    # Probe the endpoint the suite actually connects to, not pg_isready's default.
    # tests/common/mod.rs reads TEST_DATABASE_URL_BASE and falls back to
    # localhost:5432, so the probe derives host and port from the same value. A
    # bare `pg_isready` is a proxy for that, and the two can disagree — a data
    # directory carries its port in postgresql.conf, so a profile whose directory
    # was initialised while 5432 was taken keeps the shifted port afterwards. The
    # proxy then loops for a minute against a port nothing is listening on and
    # reports "did not start", which is the one thing that had not happened.
    # Stripped in this order because each step can otherwise hide the next: a query string can
    # contain a slash, userinfo can contain a colon, and an IPv6 host contains several. Splitting
    # the authority on its first colon without removing userinfo first reads
    # `user:password@host:5433` as host `user`, and pg_isready then waits out its timeout against a
    # host that does not exist.
    test_base="''${TEST_DATABASE_URL_BASE:-postgresql://localhost:5432}"
    test_authority="''${test_base#*//}"
    test_authority="''${test_authority%%\?*}"
    test_authority="''${test_authority%%/*}"
    test_authority="''${test_authority##*@}"
    case "$test_authority" in
      \[*\]*)
        # Bracketed IPv6. libpq wants the bare address, so the brackets come off.
        test_host="''${test_authority#\[}"
        test_host="''${test_host%%\]*}"
        test_port="''${test_authority##*\]}"
        test_port="''${test_port#:}"
        ;;
      *:*)
        test_host="''${test_authority%%:*}"
        test_port="''${test_authority##*:}"
        ;;
      *)
        test_host="$test_authority"
        test_port=""
        ;;
    esac
    [ -z "$test_port" ] && test_port=5432

    postgres_up() {
      pg_isready -q -h "$test_host" -p "$test_port" 2>/dev/null
    }

    needed=""
    postgres_up || needed="postgres"
    object_store_up || needed="$needed object-store"

    if [ -z "$needed" ]; then
      exit 0
    fi

    echo "Starting test services:$needed"
    devenv --profile test up $needed --detach >/dev/null 2>&1 || true

    for _ in $(seq 1 60); do
      pg_ok=false
      store_ok=false
      postgres_up && pg_ok=true
      object_store_up && store_ok=true
      if [ "$pg_ok" = true ] && [ "$store_ok" = true ]; then
        exit 0
      fi
      sleep 1
    done

    echo "Test services did not start within 60s"
    echo "  postgres:     $test_host:$test_port ($(postgres_up && echo ready || echo down))"
    echo "  object-store: ${objectStoreEndpoint}"
    echo "Start them with 'devenv --profile test up postgres object-store --detach'."
    echo "If postgres is listening on another port, that port is recorded in the"
    echo "profile's postgresql.conf; remove the state directory to reinitialise it."
    exit 1
  '';

  scripts.test-rust.exec = ''
    set -euo pipefail
    ${postgresEnv}
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
    #
    # test_schedules is the exception to the paragraph above: it needs neither
    # PostgreSQL nor MinIO, because it reads schema.sql as text. It is here
    # because the pg_cron blocks it checks are stripped out by
    # tests/common/mod.rs before the schema is applied, so the trading schedules
    # have no other executable cover at all.
    #
    # test_model_artifact is the second such exception, and needs no network
    # either: it packages what the trainer's publish stage writes and loads it
    # back through the service's own loader. The two sides are built from the
    # same constants but by different code, and nothing else executes the join
    # between them.
    TEST_ARGS="--lib --bins --all-features --test test_database --test test_handlers --test test_dashboard --test test_schedules --test test_model_artifact"

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
    ${postgresEnv}
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

  # Reconciles secretspec.toml against what the awssm provider actually holds.
  #
  # The two drift in both directions and neither direction announces itself. A profile retired from
  # the file leaves its secrets live in AWS -- three such profiles were found holding valid Alpaca
  # credentials nobody could name -- and a key added to the file has no value until someone sets it,
  # which surfaces as a runtime failure in whatever reads it first.
  scripts.audit-secrets.exec = ''
    set -euo pipefail

    declared="$(mktemp)"
    actual="$(mktemp)"
    trap 'rm -f "$declared" "$actual"' EXIT

    project="$(awk -F'"' '/^name[[:space:]]*=/ {print $2; exit}' "$DEVENV_ROOT/secretspec.toml")"
    [ -z "$project" ] && { echo "Could not read the project name from secretspec.toml"; exit 1; }

    # Emits "path<TAB>required" for every key the file declares.
    awk -v project="$project" '
      /^\[profiles\./ {
        line = $0
        sub(/^\[profiles\./, "", line); sub(/\]$/, "", line); gsub(/"/, "", line)
        profile = line; in_profile = 1; next
      }
      /^\[/ { in_profile = 0; next }
      in_profile && /^[A-Za-z_][A-Za-z0-9_]*[[:space:]]*=/ {
        required = ($0 ~ /required[[:space:]]*=[[:space:]]*true/) ? "required" : "optional"
        printf "secretspec/%s/%s/%s\t%s\n", project, profile, $1, required
      }
    ' "$DEVENV_ROOT/secretspec.toml" | LC_ALL=C sort > "$declared"

    # `|| true` on the grep: with pipefail active it returns 1 when the store holds nothing under
    # the prefix, which aborted the script before it printed anything -- and an empty store is the
    # case the audit most needs to report, since every required key is then missing.
    # LC_ALL=C on both sorts, because `comm` below compares bytes while `sort` follows the locale,
    # and every key contains '/' and '_', which en_US.UTF-8 and C order differently.
    aws secretsmanager list-secrets --max-results 100 \
      --query "SecretList[?starts_with(Name, 'secretspec/$project/')].Name" --output text \
      | tr '\t' '\n' | { grep -v '^$' || true; } | LC_ALL=C sort > "$actual"

    echo "secretspec audit for project '$project'"
    echo "  declared in secretspec.toml: $(wc -l < "$declared" | tr -d ' ')"
    echo "  stored in AWS:               $(wc -l < "$actual" | tr -d ' ')"
    echo ""

    status=0

    orphans="$(comm -13 <(cut -f1 "$declared") "$actual" || true)"
    if [ -n "$orphans" ]; then
      status=1
      echo "Stored in AWS but not declared -- live credentials nothing reads:"
      echo "$orphans" | sed 's/^/  /'
      echo ""
    fi

    # Optional keys carry defaults in the file, so their absence is a choice rather than a fault.
    missing_required="$(comm -23 <(awk -F'\t' '$2=="required" {print $1}' "$declared") "$actual" || true)"
    if [ -n "$missing_required" ]; then
      status=1
      echo "Declared required but absent from AWS -- will fail at run time:"
      echo "$missing_required" | sed 's/^/  /'
      echo ""
    fi

    if [ "$status" -eq 0 ]; then
      echo "No drift: every declared key is stored, and nothing is stored that is not declared."
    fi
    exit "$status"
  '';

  # Reports every PostgreSQL cluster currently listening, and which one owns 5432.
  #
  # Two clusters can run at once and only one can hold 5432; devenv writes the port into each data
  # directory at initialisation, so a directory initialised while 5432 was taken keeps the shifted
  # port forever afterwards. Everything here hardcodes localhost:5432, so the loser becomes
  # invisible while still running -- including its pg_cron, which goes on emitting trading events
  # into whichever database it can reach. That state ran for a week undetected.
  scripts.doctor-database.exec = ''
    set -euo pipefail
    ${postgresEnv}

    expected="$DEVENV_ROOT/.devenv/state/postgres"
    running=0
    owner_of_5432=""

    echo "PostgreSQL clusters listening on 5432-5436"
    for port in 5432 5433 5434 5435 5436; do
      pg_isready -q -h localhost -p "$port" 2>/dev/null || continue
      running=$((running + 1))
      directory="$(psql -h localhost -p "$port" -d postgres -tAc \
        "SELECT setting FROM pg_settings WHERE name='data_directory'" 2>/dev/null || echo '<unreadable>')"
      identifier="$(psql -h localhost -p "$port" -d postgres -tAc \
        "SELECT system_identifier FROM pg_control_system()" 2>/dev/null || echo '?')"
      echo "  port $port  system_id=$identifier"
      echo "            $directory"
      [ "$port" = "5432" ] && owner_of_5432="$directory"
    done

    echo ""
    status=0

    if [ "$running" -eq 0 ]; then
      echo "Nothing is listening. Start one with 'devenv up postgres --detach'."
      exit 1
    fi

    if [ "$running" -gt 1 ]; then
      status=1
      echo "$running clusters are running. Each carries its own pg_cron, and every scheduler that"
      echo "  can reach the database on 5432 emits into it -- so scheduled events are duplicated."
      echo "  Stop the extra one, or remove its data directory to force re-initialisation."
    fi

    if [ -z "$owner_of_5432" ]; then
      status=1
      echo "Nothing owns 5432, which is the port DATABASE_URL and every psql call in this file use."
    elif [ "$owner_of_5432" != "$expected" ]; then
      status=1
      echo "5432 is owned by a data directory other than the development one."
      echo "  expected: $expected"
      echo "  actual:   $owner_of_5432"
      echo "  Everything that connects to localhost:5432 is talking to that other cluster."
    fi

    [ "$status" -eq 0 ] && echo "One cluster, on 5432, from the development data directory."
    exit "$status"
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
      echo "Usage: start-duckdb <archive-bucket-name>" >&2
      echo "" >&2
      echo "Every view reads data/**, so this names the shared archive." >&2
      echo "" >&2
      echo "Example:" >&2
      echo "  start-duckdb ${archiveBucket}" >&2
      exit 1
    fi

    export AWS_S3_ARCHIVE_BUCKET_NAME="$1"
    echo "Opening DuckDB lab (archive: $AWS_S3_ARCHIVE_BUCKET_NAME)"

    exec duckdb ~/lab.duckdb -init "$DEVENV_ROOT/tools/duckdb_initialization.sql"
  '';

  # Emits the same event pg_cron raises every five minutes during a session, so a manual trigger
  # exercises the identical handler path. Named for the command it emits; there is no "rebalance"
  # in the vocabulary any more.
  scripts.trigger-evaluation.exec = ''
    psql -h localhost -p 5432 -d fund -c "SELECT emit_event('portfolio_evaluation_requested', jsonb_build_object('reason', 'manual'))"
  '';

  # The counterpart the other two assume has already run. Its schedule is a twenty-minute window at
  # the open, so a service started at any other hour finds no predictions for the session and every
  # later evaluation screens nothing -- a healthy-looking idle loop that reports `screened: 0` and
  # never says why. This is the only way to reach the inference path outside that window.
  scripts.trigger-predictions.exec = ''
    psql -h localhost -p 5432 -d fund -c "SELECT emit_event('predictions_requested', jsonb_build_object('reason', 'manual'))"
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
    #
    # CRON_TZ pins the hour to UTC in the crontab itself. Every other schedule in this system says
    # which timezone it is in -- pg_cron has `cron.timezone = UTC` in the settings above -- and this
    # one was reading it from host state nobody sets, so "23:00 UTC" was a hope rather than a fact.
    #
    # Checked independently of the entry below, for the reason in the comment at the top of this
    # script: a machine provisioned before the pin existed has the training entry already, so a
    # check that rode along with it would never run there.
    #
    # A crontab variable applies only to the entries *below* it, so presence is not proof. A second
    # assignment above the trainer entry would leave that schedule in the host timezone while a
    # grep for CRON_TZ=UTC anywhere still passed. Rather than test for that, this normalizes: every
    # CRON_TZ line is stripped and exactly one is prepended, so the managed crontab carries a single
    # timezone declaration at the top and every entry inherits it regardless of the order they were
    # installed in. Idempotent, and it makes the invariant true instead of merely checked.
    if [ "$(crontab -l 2>/dev/null | grep -c '^CRON_TZ=' || true)" = "1" ] \
       && crontab -l 2>/dev/null | head -n 1 | grep -qE '^CRON_TZ=UTC$'; then
      echo "Cron timezone already pinned to UTC"
    else
      (echo 'CRON_TZ=UTC'; crontab -l 2>/dev/null | grep -v '^CRON_TZ=' || true) | crontab -
      echo "Pinned cron timezone to UTC (host is $(date +%Z), offset $(date +%z))"
    fi

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

  # Seeding, by data type and target. Bars come from Massive either way -- the grouped endpoint
  # answers by date rather than by symbol list -- but the two targets serve different readers and
  # have different requirements, which is why they are separate scripts and separate subcommands of
  # the `seed` binary rather than one invocation with a flag.
  #
  #   *-postgres  what the application trades from. Needs a database, no AWS.
  #   *-s3        what the trainer trains from. Needs AWS and Massive, no database.
  #
  # Neither one can stand in for the other: the application never reads the archive and the trainer
  # has no database to read.

  scripts.seed-equity-bars-postgres.exec = ''
    set -euo pipefail

    if [ -z "''${SEED_START_DATE:-}" ]; then
      echo "Usage: SEED_START_DATE=YYYY-MM-DD devenv tasks run data:seed:postgres"
      echo "  Optional: SEED_END_DATE=YYYY-MM-DD (defaults to today, US/Eastern)"
      echo ""
      echo "  Fetches whole-market daily bars from Massive, one request per session, and upserts"
      echo "  them into equity_bars. Needs MASSIVE_BASE_URL and MASSIVE_API_KEY, not Alpaca"
      echo "  credentials. Safe to re-run over a range already seeded."
      exit 1
    fi

    END_FLAG=""
    if [ -n "''${SEED_END_DATE:-}" ]; then
      END_FLAG="--end $SEED_END_DATE"
    fi

    echo "Seeding equity bars into PostgreSQL from $SEED_START_DATE to ''${SEED_END_DATE:-today}"
    ${runtimeEnv}
    secretspec run -- cargo run --release --bin seed -- \
      equity-bars daily postgres --start "$SEED_START_DATE" $END_FLAG
  '';

  scripts.seed-equity-details-postgres.exec = ''
    set -euo pipefail

    echo "Seeding equity details into PostgreSQL from the embedded CSV"
    ${runtimeEnv}
    secretspec run -- cargo run --release --bin seed -- equity-details postgres
  '';

  # Repairs account_snapshots from Alpaca's portfolio history. Not a seed: it fills only the
  # sessions that are missing, and never overwrites one the post-close sync already wrote.
  scripts.backfill-account-snapshots.exec = ''
    set -euo pipefail

    if [ -z "''${BACKFILL_START_DATE:-}" ]; then
      echo "Usage: BACKFILL_START_DATE=YYYY-MM-DD devenv tasks run database:backfill"
      echo "  Optional: BACKFILL_END_DATE=YYYY-MM-DD (defaults to today, US/Eastern)"
      echo "  Optional: BACKFILL_DRY_RUN=1 to report the gaps without writing anything"
      echo ""
      echo "  Rebuilds missing account_snapshots rows from /v2/account/portfolio/history."
      echo "  Reconstructed rows carry equity only -- portfolio history reports no balances."
      echo "  Needs Alpaca credentials and a database. Safe to re-run: existing rows are left"
      echo "  alone, so this never downgrades a full snapshot to an equity-only one."
      exit 1
    fi

    # Deliberately not defaulted to a dry run. The flag has to be asked for, so that a scripted
    # invocation that forgets it fails loudly at the usage block above rather than reporting a
    # tidy plan and writing nothing.
    DRY_RUN_FLAG=""
    if [ -n "''${BACKFILL_DRY_RUN:-}" ]; then
      DRY_RUN_FLAG="--dry-run"
      echo "Dry run: reporting gaps without writing"
    fi

    echo "Backfilling account snapshots from $BACKFILL_START_DATE to ''${BACKFILL_END_DATE:-today}"
    ${runtimeEnv}
    secretspec run -- cargo run --release --bin backfill_account_snapshots -- \
      $DRY_RUN_FLAG "$BACKFILL_START_DATE" ''${BACKFILL_END_DATE:-}
  '';

  # No required start date, unlike the PostgreSQL side. The archive is repaired by set difference
  # against what the bucket already holds, so "no arguments" is the useful default -- it means make
  # the last two years right, whatever is currently missing from them.
  scripts.seed-equity-bars-s3.exec = ''
    set -euo pipefail

    # Either date alone is a window now: an end with no start repairs the two years before it. The
    # guard that used to sit here refused exactly that, because a lone positional would have been
    # read as the *start* date and the archive repaired over the wrong window.
    START_FLAG=""
    if [ -n "''${SEED_START_DATE:-}" ]; then
      START_FLAG="--start $SEED_START_DATE"
    fi
    END_FLAG=""
    if [ -n "''${SEED_END_DATE:-}" ]; then
      END_FLAG="--end $SEED_END_DATE"
    fi

    echo "Seeding the S3 equity bar archive from ''${SEED_START_DATE:-two years back} to ''${SEED_END_DATE:-the last closed session}"
    ${runtimeEnv}
    secretspec run -- cargo run --release --bin seed -- \
      equity-bars daily s3 $START_FLAG $END_FLAG
  '';

  scripts.seed-equity-details-s3.exec = ''
    set -euo pipefail

    echo "Archiving equity details to S3 from the embedded CSV"
    ${runtimeEnv}
    secretspec run -- cargo run --release --bin seed -- equity-details s3
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

    # Rust-native TiDE training (burn). Repairs its own S3 bar archive over the training window --
    # every weekday in it with no partition is fetched from Massive, so a missed night and a week of
    # downtime cost the same nothing -- then trains against that window and uploads a model.tar.gz
    # the service loads directly. A gap older than the window is `data:seed:s3`, which floors at two
    # years.
    # The bars themselves need no Alpaca credentials: the grouped endpoint answers by date, so there
    # is no symbol list to build and therefore no broker to ask for one. The trading calendar does,
    # and without it the scan requests holidays, which answer empty forever and cannot be told apart
    # from a session Massive is missing. Both the calendar and the series-boundary table warn and
    # skip when the credentials are absent rather than failing the run -- a repair that requests a
    # few holidays is a better trade than no repair at all. The seed binaries, which are run
    # deliberately rather than nightly, require the calendar instead of degrading.
    # The former Python/tinygrad workflow and its Prefect block registration are retired.
    "models:tide:train".exec = ''
      set -euo pipefail
      echo "Repairing the bar archive and running the tide training pipeline (Rust + burn)"
      ${runtimeEnv}
      secretspec run -- cargo run --release --bin tide_model_trainer
    '';

    # The same pipeline, run to rehearse it rather than to publish a model. It differs from
    # `models:tide:train` in exactly two ways, and both are deliberate.
    #
    # The artifact prefix is `models/tide-smoke/`. `resolve_artifact_key` serves the
    # lexicographically greatest folder under whatever prefix it is given, and a rehearsal artifact
    # is always the newest one -- so publishing it beside the real runs would hand the service a
    # one-epoch model, silently and until the next nightly run.
    #
    # `FUND_EPOCHS` defaults to 1, because what is under test is that the four stages connect, not
    # that the model converges. Everything else is left alone: the lookback stays the trainer's own
    # default, so the rehearsal reads the same window the nightly run does.
    #
    # `FUND_LOOKBACK_DAYS` has a floor near 250 -- the split reserves the last fifth of the window
    # for validation and windowing needs 36 sessions of it -- and the trainer now says so before it
    # touches the network rather than after it has loaded a year of bars.
    "models:tide:train:smoke".exec = ''
      set -euo pipefail
      ${runtimeEnv}
      export AWS_S3_MODEL_ARTIFACT_PATH="models/tide-smoke/"
      export FUND_EPOCHS="''${FUND_EPOCHS:-1}"
      echo "Rehearsing the tide training pipeline ($FUND_EPOCHS epoch(s), lookback ''${FUND_LOOKBACK_DAYS:-trainer default})"
      echo "  Publishing to s3://$AWS_S3_BUCKET_NAME/$AWS_S3_MODEL_ARTIFACT_PATH, which nothing serves from."
      secretspec run -- cargo run --release --bin tide_model_trainer
    '';

    # The rehearsal above, pointed at the production archive. It exists because the two buckets do
    # not hold the same history: the development archive was seeded in one pass from a clean
    # upstream, so a loader bug that only appears across a schema or provider change is reproducible
    # in production and nowhere else.
    #
    # The one place in this file that overrides FUND_PROFILE, and it is set *before* runtimeEnv
    # because both the bucket name and the secretspec profile are derived from it. Setting it after
    # would read production credentials against the development bucket.
    #
    # Publishes to `models/tide-smoke/`, exactly as the task above does. Nothing resolves artifacts
    # from that prefix, so a one-epoch rehearsal cannot become the model the production service
    # loads -- which is why this is a separate task rather than a flag on `models:tide:train`.
    #
    # Not read-only, and that is worth knowing before running it: stage one repairs the production
    # bar archive over the lookback window, the same write the nightly job makes. That is a gap
    # being filled rather than a side effect, but it is a write to production data.
    "models:tide:train:smoke:production".exec = ''
      set -euo pipefail
      export FUND_PROFILE="production"
      ${runtimeEnv}
      export AWS_S3_MODEL_ARTIFACT_PATH="models/tide-smoke/"
      export FUND_EPOCHS="''${FUND_EPOCHS:-1}"
      echo "Rehearsing against PRODUCTION ($FUND_EPOCHS epoch(s), lookback ''${FUND_LOOKBACK_DAYS:-trainer default})"
      echo "  Reading and repairing s3://$AWS_S3_ARCHIVE_BUCKET_NAME/data/equity/bars/interval=one_day/"
      echo "  Publishing to s3://$AWS_S3_BUCKET_NAME/$AWS_S3_MODEL_ARTIFACT_PATH, which nothing serves from."
      secretspec run -- cargo run --release --bin tide_model_trainer
    '';

    # --- Data tasks ---
    #
    # Two targets with disjoint requirements, so each half preflights its own and says which task to
    # run instead. `data:seed` runs both, which works on a laptop with `devenv up` and on the
    # application VM; on the trainer VM, which has no database, only `data:seed:s3` can run. A
    # silent skip would be the wrong answer there -- a seed that quietly did half its work is how a
    # deployment ends up with an archive and no trading data, or the reverse.

    # Bootstrap for an empty database. Details first, because they are fast and carry no date range,
    # and because the pair screen's sector rule silently admits nothing without them.
    "data:seed:postgres" = {
      exec = ''
        set -euo pipefail

        if ! psql -h localhost -p 5432 -d fund -c 'SELECT 1' > /dev/null 2>&1; then
          echo "No PostgreSQL at localhost:5432 (database 'fund')."
          echo "  This half of the seed writes what the application trades from and needs a"
          echo "  database. Start one with 'devenv up', or if you are on the trainer VM run"
          echo "  'devenv tasks run data:seed:s3' instead -- the trainer has no database."
          exit 1
        fi

        if [ -z "''${SEED_START_DATE:-}" ]; then
          echo "Usage: SEED_START_DATE=YYYY-MM-DD devenv tasks run data:seed:postgres"
          echo "  Optional: SEED_END_DATE=YYYY-MM-DD (defaults to today, US/Eastern)"
          echo ""
          echo "  Seeds ticker metadata and daily bars into PostgreSQL. The screen needs 60"
          echo "  sessions of aligned closes and the model 40, so allow at least six months."
          exit 1
        fi

        echo "=== Seeding equity details into PostgreSQL ==="
        seed-equity-details-postgres

        echo ""
        echo "=== Seeding equity bars into PostgreSQL ==="
        seed-equity-bars-postgres
      '';
    };

    # Bootstrap and repair for the S3 archive the trainer trains from. Needs no start date: the
    # range is repaired by set difference against what the bucket already holds, so re-running is
    # both cheap and the way to close a gap.
    # Deliberately declares no `after` on the PostgreSQL half. An `after` would order the two when
    # both run, but it would also drag the database half into a bare `data:seed:s3` -- which is the
    # one command the trainer VM, having no database, must be able to run on its own. The cost is
    # that `data:seed` fetches each session from Massive twice, once per target; that is the price
    # of each half being independently runnable, and the ranges usually differ anyway.
    "data:seed:s3" = {
      exec = ''
        set -euo pipefail

        if [ -z "''${AWS_S3_ARCHIVE_BUCKET_NAME:-}" ]; then
          echo "AWS_S3_ARCHIVE_BUCKET_NAME is not set."
          echo "  This half of the seed writes what the trainer trains from and needs AWS and"
          echo "  Massive credentials. If you only wanted the database, run"
          echo "  'devenv tasks run data:seed:postgres' instead."
          exit 1
        fi

        echo "=== Archiving equity details to S3 ==="
        seed-equity-details-s3

        echo ""
        echo "=== Seeding the S3 equity bar archive ==="
        seed-equity-bars-s3
      '';
    };

    # `devenv tasks run data:seed` runs both halves through prefix group execution, the way
    # `checks:rust` runs its subtasks -- so there is deliberately no `data:seed` task to define.

    # --- Database lifecycle tasks ---
    # Two lifecycle modes:
    #   Create — apply the schema (idempotent DDL). Use on a fresh VM or after schema changes.
    #   Reset  — drop and recreate the empty database. Run before create after breaking changes.

    # Opens an interactive psql session against the local fund database.
    "database:connect".exec = "exec psql -h localhost -p 5432 -d fund";

    # Drops and recreates the empty fund database. Run before database:create when
    # recovering from a breaking schema change.
    "database:reset".exec = "reset-database";

    # Dumps the live database with pg_dump and uploads it to S3. Manual only:
    # nothing schedules it. pg_cron runs SQL inside the database and cannot shell
    # out to pg_dump, so this cannot become a cron job as written.
    #
    # Not the same thing as the nightly export, which the market data sync does
    # chain automatically. That writes per-table parquet under exports/ for
    # querying; this writes a pg_restore-able dump under database/backups/. Only
    # this one restores equity_pairs, which nothing else can reconstruct.
    "database:backup".exec = "backup-database";

    # Applies the schema to the fund database. Safe to re-run (all DDL is
    # idempotent). Use after database:reset or on a fresh VM.
    "database:create".exec = ''
      set -euo pipefail
      ${applySchema}
    '';

    # Refills account_snapshots from Alpaca after a reset or a failed post-close
    # sync. The counterpart to database:backup, and the reason a reset is now
    # survivable: backup restores equity_pairs, this restores the equity series.
    "database:backfill".exec = "backfill-account-snapshots";

    # Reports which cluster owns 5432, and fails when more than one is running.
    # Run this first when the database behaves as though it holds someone else's data.
    "database:doctor".exec = "doctor-database";

    # --- Secrets ---

    # Reconciles secretspec.toml against the awssm provider in both directions.
    "secrets:audit".exec = "audit-secrets";

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
      echo "  Records: $AWS_S3_BUCKET_NAME"
      echo "  Archive: $AWS_S3_ARCHIVE_BUCKET_NAME (shared, holds data/**)"
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
      echo "    data:seed                   Both targets below (needs a"
      echo "                                database and AWS)"
      echo "    data:seed:postgres          Bars and details into PostgreSQL"
      echo "                                (run without arguments for usage)"
      echo "    data:seed:s3                Bars and details into the S3 archive"
      echo "                                the trainer reads; repairs whatever"
      echo "                                is missing, two years by default"
      echo "    models:tide:train           Repair the S3 bar archive over the"
      echo "                                training window, train TiDE, and"
      echo "                                upload artifacts"
    } >&2
  '';

  enterTest = ''
  '';
}
