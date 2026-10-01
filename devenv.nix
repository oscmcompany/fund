{pkgs, ...}: let
  awsRegion = "us-east-1";
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
      excludes = ["^src_old/"];
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
    AWS_REGION = awsRegion;
    AWS_DEFAULT_REGION = awsRegion;

    # Secretspec CLI configuration
    SECRETSPEC_PROVIDER = "awssm";

    # Disable AWS CLI pager so secrets output is not paged
    AWS_PAGER = "";
  };

  packages = with pkgs; [
    alejandra
    awscli2
    cargo-llvm-cov
    cargo-machete
    curl
    duckdb # retained for local data exploration and experimentation
    gh
    git
    jq
    llvmPackages.llvm
    markdownlint-cli
    rustup
    statix
    taplo
    yamllint
  ];

  scripts.format-rust.exec = ''
    set -euo pipefail
    echo "Checking Rust code formatting"
    cargo fmt --all -- --check
    echo "Rust code formatting check passed"
  '';

  scripts.lint-rust.exec = ''
    set -euo pipefail
    echo "Running Rust lint checks"
    cargo clippy --workspace --all-features --all-targets -- -D warnings
    echo "Rust linting completed successfully"
  '';

  scripts.check-unused-dependencies.exec = ''
    set -euo pipefail
    echo "Checking for unused Rust dependencies"
    cargo machete
    echo "No unused dependencies found"
  '';

  scripts.test-rust.exec = ''
    set -euo pipefail
    echo "Running Rust tests"

    mkdir -p .coverage_output
    export LLVM_COV=$(which llvm-cov)
    export LLVM_PROFDATA=$(which llvm-profdata)
    cargo llvm-cov --lib --bins --tests --all-features \
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

  scripts.check-nix.exec = ''
    set -euo pipefail
    echo "Checking Nix code formatting"
    alejandra --check --exclude ./.devenv --exclude ./.venv --exclude ./target .
    echo "Nix formatting check passed"
    echo "Running Nix static analysis"
    statix check -c .statix.toml .
    echo "Nix checks completed successfully"
  '';

  scripts.start-duckdb.exec = ''
    set -euo pipefail
    cd "$DEVENV_ROOT"
    duckdb -init views.sql "$@"
  '';

  # Creates each view of views.sql alone against the real buckets and counts its rows. An empty view and one that does
  # not create both answer nothing, so both fail; a view whose producer does not run in this profile must be empty.
  # Exits 0 when every view is as expected, 3 when any is not, and 1 when the check could not be made.
  scripts.check-views.exec = ''
    set -euo pipefail
    cd "$DEVENV_ROOT"
    for variable in AWS_S3_ARCHIVE_BUCKET_NAME AWS_S3_RECORDS_BUCKET_NAME; do
      if [[ -z "''${!variable:-}" ]]; then
        echo "Error: $variable is not set" >&2
        exit 1
      fi
    done
    mapfile -t views < <(sed -nE 's/^CREATE OR REPLACE VIEW ([a-z_]+) AS$/\1/p' views.sql)
    if [[ ''${#views[@]} -eq 0 ]]; then
      echo "Error: found no views in views.sql" >&2
      exit 1
    fi
    # Each is a claim about the world: a dormant view that starts reading rows fails, so its line is removed on purpose.
    case "''${FUND_PROFILE:-}" in
      # Until the new archiver ships records from its host (pivot task 25).
      production) dormant=" journal logs " ;;
      development/*) dormant=" journal logs " ;;
      *) dormant=" " ;;
    esac
    preamble="$(awk '/^CREATE OR REPLACE VIEW /{exit} {print}' views.sql)"
    broken=0 live=0 unread=0
    for view in "''${views[@]}"; do
      statement="$(awk -v start="CREATE OR REPLACE VIEW $view AS" '$0 == start {inside = 1} inside {print} inside && /^\);$/ {exit}' views.sql)"
      # Bailing on the first error keeps a failed creation from adding a second error of its own.
      if output="$(printf '%s\n.bail on\n%s\nSELECT count(*) FROM %s;\n' "$preamble" "$statement" "$view" | duckdb -csv -noheader 2>&1)"; then
        rows="$(grep -E '^[0-9]+$' <<<"$output" | tail -n 1)"
      else
        rows=""
      fi
      if [[ "$dormant" == *" $view "* ]]; then
        if [[ -n "$rows" && "$rows" -gt 0 ]]; then
          echo "$view: dormant but reads $rows rows; remove it from the dormant list"
          broken=$((broken + 1))
        elif [[ -n "$rows" ]] || { grep -q 'No files found that match the pattern' <<<"$output" && [[ "$(grep -c 'Error' <<<"$output")" -eq 1 ]]; }; then
          echo "$view: dormant"
        else
          echo "$view: dormant, and failed for another reason than nothing being written: $(grep -m 1 'Error' <<<"$output")"
          broken=$((broken + 1))
        fi
        continue
      fi
      live=$((live + 1))
      if [[ -z "$rows" ]]; then
        echo "$view: did not create: $(grep -m 1 'Error' <<<"$output" || echo "$output")"
        broken=$((broken + 1))
        unread=$((unread + 1))
      elif [[ "$rows" -eq 0 ]]; then
        echo "$view: reads no rows"
        broken=$((broken + 1))
      else
        echo "$view: $rows rows"
      fi
    done
    # Every live view unreadable is the check failing to reach S3, not every view breaking at once.
    if [[ "$live" -gt 0 && "$unread" -eq "$live" ]]; then
      echo "Error: no live view could be read; the check was not made" >&2
      exit 1
    fi
    if [[ "$broken" -gt 0 ]]; then
      echo "$broken of ''${#views[@]} views are not as this profile expects"
      exit 3
    fi
    echo "All $live live views read rows"
  '';

  scripts.bump-rust-dependencies.exec = ''
    set -euo pipefail
    cargo update
    echo "Dependencies bumped. Review changes: git diff Cargo.lock"
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
    "checks:nix".exec = "check-nix";

    "checks:base" = {
      exec = ''
        echo "All base checks passed"
      '';
      after = [
        "checks:nix"
        "checks:markdown"
        "checks:yaml"
        "checks:toml"
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

  # dotenv runs after Nix evaluates this file, so the profile is read at shell start.
  enterShell = ''
    export SECRETSPEC_PROFILE="''${FUND_PROFILE:-development}"
    {
      echo "Fund development environment (secretspec profile: $SECRETSPEC_PROFILE)"
      echo ""
      echo "  Tasks (devenv tasks run <name>):"
      echo "    checks:rust                 All Rust checks (format, lint,"
      echo "                                test with coverage, unused-deps)"
      echo "    checks:base                 Non-language checks (nix, markdown,"
      echo "                                yaml, toml)"
      echo "    checks:all                  All checks combined"
      echo ""
      echo "  Scripts:"
      echo "    bump-rust-dependencies      Update the Cargo lockfile"
      echo "    start-duckdb                DuckDB with the archive views"
      echo "    check-views                 Fail on any archive view that is empty"
    } >&2
  '';
}
