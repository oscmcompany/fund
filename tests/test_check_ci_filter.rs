//! The CI `rust` path filter covers every tracked file the local Rust hook does, so a change the hook checks on a
//! commit is checked again by CI; CI may cover more.

use std::io::Write;
use std::path::PathBuf;
use std::process::{Command, Stdio};

fn root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

/// The Rust hook's file pattern from `devenv.nix`, with Nix's string escaping undone.
fn hook_pattern() -> String {
    let devenv = std::fs::read_to_string(root().join("devenv.nix")).unwrap();
    let mut lines = devenv.lines();
    lines
        .find(|line| line.contains(r#"hook "Check all Rust code" "rust""#))
        .expect("the Rust hook in devenv.nix");
    let pattern = lines.next().unwrap().trim().trim_end_matches(';');
    pattern
        .strip_prefix('"')
        .and_then(|pattern| pattern.strip_suffix('"'))
        .expect("the Rust hook's pattern on the line after its name")
        .replace(r"\\", r"\")
}

/// The `rust:` entries of the CI workflow's path filter.
fn ci_patterns() -> Vec<String> {
    let workflow =
        std::fs::read_to_string(root().join(".github/workflows/continuous_integration.yaml"))
            .unwrap();
    let mut lines = workflow.lines().skip_while(|line| line.trim() != "rust:");
    lines.next().expect("the rust filter in the CI workflow");
    lines
        .map_while(|line| line.trim().strip_prefix("- "))
        .map(|entry| entry.trim_matches('\'').to_string())
        .collect()
}

/// Whether a CI filter entry matches `path`; only `dir/**` and exact paths are read, so any other form fails loudly
/// rather than being matched differently from the workflow.
fn ci_matches(pattern: &str, path: &str) -> bool {
    match pattern.strip_suffix("/**") {
        Some(directory) => {
            assert!(
                !directory.contains(['*', '?', '[', '{', '!']),
                "unsupported filter {pattern}"
            );
            path.starts_with(&format!("{directory}/"))
        }
        None => {
            assert!(
                !pattern.contains(['*', '?', '[', '{', '!']),
                "unsupported filter {pattern}"
            );
            path == pattern
        }
    }
}

/// The `files` the hook pattern selects, matched by `grep -E` as the hook's own matcher would.
fn hook_selects(pattern: &str, files: &[&str]) -> Vec<String> {
    let mut grep = Command::new("grep")
        .args(["-E", pattern])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    grep.stdin
        .take()
        .unwrap()
        .write_all(files.join("\n").as_bytes())
        .unwrap();
    let output = grep.wait_with_output().unwrap();
    // grep exits 1 when nothing matches and 2 when the pattern is invalid, which must not read as an empty selection.
    assert!(
        matches!(output.status.code(), Some(0 | 1)),
        "grep rejected {pattern}"
    );
    String::from_utf8(output.stdout)
        .unwrap()
        .lines()
        .map(str::to_string)
        .collect()
}

/// The files the hook checks that no CI filter entry covers.
fn uncovered(pattern: &str, ci: &[String], files: &[&str]) -> Vec<String> {
    hook_selects(pattern, files)
        .into_iter()
        .filter(|path| !ci.iter().any(|entry| ci_matches(entry, path)))
        .collect()
}

#[test]
fn reports_a_hook_file_the_ci_filter_misses() {
    let ci = vec!["src/**".to_string(), "Cargo.toml".to_string()];
    let files = [
        "src/lib.rs",
        "Cargo.toml",
        "host/run",
        "README.md",
        "nested/Cargo.toml",
    ];
    assert_eq!(
        uncovered(r"(\.rs|Cargo\.toml|^host/.+)$", &ci, &files),
        vec!["host/run", "nested/Cargo.toml"]
    );
}

#[test]
#[should_panic(expected = "unsupported filter !src/**")]
fn refuses_a_negated_ci_filter() {
    ci_matches("!src/**", "src/lib.rs");
}

#[test]
#[should_panic(expected = "grep rejected")]
fn refuses_a_pattern_grep_cannot_compile() {
    hook_selects("(", &["src/lib.rs"]);
}

#[test]
fn reads_the_hook_pattern_and_the_ci_filter() {
    let pattern = hook_pattern();
    assert!(pattern.starts_with(r"(\.rs|"), "{pattern}");
    let ci = ci_patterns();
    assert!(ci.contains(&"src/**".to_string()), "{ci:?}");
    assert!(ci.contains(&"Cargo.lock".to_string()), "{ci:?}");
}

#[test]
fn ci_covers_every_tracked_file_the_rust_hook_checks() {
    let mut git = Command::new("git");
    // A hook exports `GIT_*` variables that would point the listing at the commit's index rather than the tree.
    for (name, _) in std::env::vars_os() {
        if name.to_string_lossy().starts_with("GIT_") {
            git.env_remove(name);
        }
    }
    // `-z` lists paths raw, where the default quotes an unusual path and hides it from an anchored pattern.
    let listing = git
        .arg("-C")
        .arg(root())
        .args(["ls-files", "-z"])
        .output()
        .unwrap();
    assert!(listing.status.success());
    let listing = String::from_utf8(listing.stdout).unwrap();
    let files: Vec<&str> = listing.split_terminator('\0').collect();
    assert!(
        files.iter().all(|path| !path.contains('\n')),
        "a tracked path holds a newline"
    );
    let pattern = hook_pattern();
    assert!(
        hook_selects(&pattern, &files).len() > 50,
        "the hook pattern selected almost nothing: {pattern}"
    );
    assert_eq!(
        uncovered(&pattern, &ci_patterns(), &files),
        Vec::<String>::new()
    );
}
