{pkgs, ...}: let
  awsRegion = "us-east-1";
in {
  dotenv.enable = true;

  languages = {
    # The one pinned toolchain the laptop, CI and a host's rustup all read.
    rust = {
      enable = true;
      toolchainFile = ./rust-toolchain.toml;
    };
    nix.enable = true;
  };

  # Each hook runs the task CI runs, so a commit and a pull request check the same files the same way.
  git-hooks.hooks = let
    hook = name: check: files: {
      enable = true;
      inherit name files;
      entry = "devenv tasks run checks:${check}";
      pass_filenames = false;
      language = "system";
      fail_fast = true;
    };
  in {
    check-private-files =
      hook "Check no private file is tracked" "private-files" ""
      // {always_run = true;};
    check-rust =
      hook "Check all Rust code" "rust"
      "(\\.rs|Cargo\\.(toml|lock)|(clippy|secretspec|rust-toolchain)\\.toml|views\\.sql|check-views|check-private-files|devenv\\.(nix|lock|yaml))$"
      // {excludes = ["^src_old/"];};
    check-markdown = hook "Check all Markdown code" "markdown" "\\.md$";
    check-yaml = hook "Check all YAML code" "yaml" "\\.(yaml|yml)$";
    check-toml = hook "Check all TOML code" "toml" "\\.toml$";
    check-nix = hook "Check all Nix code" "nix" "\\.nix$";
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
    duckdb # start-duckdb and check-views
    gh
    git
    jq
    markdownlint-cli
    statix
    taplo
    yamllint
  ];

  scripts.mutate-rust.exec = ''
    set -euo pipefail
    echo "Running mutation tests on the lines changed in ''${1:?a unified diff file}"
    cargo mutants --in-diff "$1"
    echo "Every mutant in the diff was caught"
  '';

  scripts.start-duckdb.exec = ''
    set -euo pipefail
    cd "$DEVENV_ROOT"
    duckdb -init views.sql "$@"
  '';

  scripts.check-views.exec = ''
    "$DEVENV_ROOT/check-views" "$@"
  '';

  scripts.update-rust-dependencies.exec = ''
    set -euo pipefail
    cargo update
    echo "Dependencies updated. Review changes: git diff Cargo.lock"
  '';

  # Lints read the files git tracks, so the laptop and CI's clean checkout lint the same set.
  tasks = {
    # --- Rust checks (lint and test run in parallel after format) ---

    "checks:rust:format".exec = ''
      set -euo pipefail
      cargo fmt --all -- --check
    '';

    "checks:rust:lint" = {
      exec = ''
        set -euo pipefail
        cargo clippy --workspace --all-features --all-targets -- -D warnings
      '';
      after = ["checks:rust:format"];
    };
    "checks:rust:test" = {
      exec = ''
        set -euo pipefail
        mkdir -p .coverage_output
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
      '';
      after = ["checks:rust:format"];
    };
    "checks:rust:unused-dependencies" = {
      exec = ''
        set -euo pipefail
        cargo machete
      '';
      after = ["checks:rust:format"];
    };

    # --- Standalone checks ---

    "checks:markdown".exec = ''
      set -euo pipefail
      git ls-files -z '*.md' | xargs -0 -r markdownlint
    '';
    "checks:yaml".exec = ''
      set -euo pipefail
      git ls-files -z '*.yaml' '*.yml' | xargs -0 -r yamllint
    '';
    "checks:toml".exec = ''
      set -euo pipefail
      git ls-files -z '*.toml' | xargs -0 -r taplo fmt --check --no-auto-config
    '';
    "checks:nix".exec = ''
      set -euo pipefail
      git ls-files -z '*.nix' | xargs -0 -r alejandra --check
      git ls-files -z '*.nix' | xargs -0 -r -n 1 statix check -c .statix.toml
    '';
    "checks:private-files".exec = ''
      "$DEVENV_ROOT/check-private-files"
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
      ];
    };

    "checks:all" = {
      exec = ''
        echo "All checks passed"
      '';
      after = [
        "checks:base"
        "checks:private-files"
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
      echo "    checks:private-files        Fail on any tracked private file"
      echo "    checks:all                  All checks combined"
      echo ""
      echo "  Scripts:"
      echo "    update-rust-dependencies    Update the Cargo lockfile"
      echo "    start-duckdb                DuckDB with the archive views"
      echo "    check-views                 Fail on any archive view that is empty"
      echo "    mutate-rust <diff>          Mutation-test the lines a diff changes"
    } >&2
  '';
}
