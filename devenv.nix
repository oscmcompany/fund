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
      files = "(\\.rs|Cargo\\.(toml|lock)|(clippy|secretspec)\\.toml|views\\.sql|check-views)$";
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
    cargo-mutants
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

  scripts.mutate-rust.exec = ''
    set -euo pipefail
    echo "Running mutation tests on the lines changed in ''${1:?a unified diff file}"
    cargo mutants --in-diff "$1"
    echo "Every mutant in the diff was caught"
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

  scripts.check-views.exec = ''
    "$DEVENV_ROOT/check-views" "$@"
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
      echo "    mutate-rust <diff>          Mutation-test the lines a diff changes"
    } >&2
  '';
}
