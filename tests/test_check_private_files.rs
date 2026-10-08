//! What `check-private-files` reports against a scratch repository: each kind of private file fails it once tracked,
//! and the public files whose names come closest pass.

use std::path::{Path, PathBuf};
use std::process::Command;

fn script() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("check-private-files")
}

fn git(repository: &Path, arguments: &[&str]) {
    let status = Command::new("git")
        .arg("-C")
        .arg(repository)
        .args(arguments)
        .status()
        .unwrap();
    assert!(status.success(), "git {arguments:?}");
}

/// A repository tracking `paths`, each forced past any ignore file, and the script's exit code and output over it.
fn check(paths: &[&str]) -> (i32, String) {
    let repository = std::env::temp_dir().join(format!("check-private-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&repository).unwrap();
    git(&repository, &["init", "--quiet"]);
    for path in paths {
        let file = repository.join(path);
        std::fs::create_dir_all(file.parent().unwrap()).unwrap();
        std::fs::write(&file, "x").unwrap();
        git(&repository, &["add", "--force", path]);
    }
    let output = Command::new(script()).arg(&repository).output().unwrap();
    std::fs::remove_dir_all(&repository).unwrap();
    (
        output.status.code().unwrap(),
        format!(
            "{}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        ),
    )
}

const PUBLIC: [&str; 7] = [
    "src/journal.rs",
    "src/common/journal.rs",
    "src/common/journal/record.rs",
    "studies/src/lib.rs",
    "data/equity_details.csv",
    "secretspec.toml",
    "views.sql",
];

#[test]
fn test_public_files_pass() {
    let (code, output) = check(&PUBLIC);
    assert_eq!(code, 0, "{output}");
}

#[test]
fn test_each_private_file_fails_and_is_named() {
    for private in [
        "playbook.toml",
        "config/playbook-development.toml",
        "data/equity/bars/data.parquet",
        "data/raw/trades.csv.gz",
        "records/2026-10-08.jsonl",
        "models/weights.safetensors",
        "data/capital_flows.toml",
        "journal/README.md",
        "studies/src/bin/null_check.rs",
        ".scratchpad/plan_pivot.md",
        "journal/échange.md",
        "data/équité.parquet",
    ] {
        let mut paths = PUBLIC.to_vec();
        paths.push(private);
        let (code, output) = check(&paths);
        assert_eq!(code, 3, "{private}: {output}");
        let named: Vec<&str> = output.lines().skip(1).collect();
        assert_eq!(named, [private], "{output}");
    }
}

/// A path that is not valid UTF-8, tracked straight into the index since the filesystem may refuse to hold it.
#[test]
fn test_a_private_path_that_is_not_utf8_fails() {
    use std::os::unix::ffi::OsStrExt;
    let repository = std::env::temp_dir().join(format!("check-private-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&repository).unwrap();
    git(&repository, &["init", "--quiet"]);
    git(&repository, &["hash-object", "-w", "/dev/null"]);
    let entry = std::ffi::OsStr::from_bytes(
        b"100644,e69de29bb2d1d6434b8b29ae775ad8c2e48c5391,config/playbook\xff.toml",
    );
    let status = Command::new("git")
        .arg("-C")
        .arg(&repository)
        .args(["update-index", "--add", "--cacheinfo"])
        .arg(entry)
        .status()
        .unwrap();
    assert!(status.success());
    let output = Command::new(script())
        .arg(&repository)
        .env("LC_ALL", "en_US.UTF-8")
        .output()
        .unwrap();
    std::fs::remove_dir_all(&repository).unwrap();
    assert_eq!(output.status.code(), Some(3), "{output:?}");
}

#[test]
fn test_a_directory_that_is_no_repository_cannot_be_checked() {
    let directory = std::env::temp_dir().join(format!("check-private-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&directory).unwrap();
    let output = Command::new(script())
        .arg(&directory)
        .env("GIT_CEILING_DIRECTORIES", std::env::temp_dir())
        .output()
        .unwrap();
    std::fs::remove_dir_all(&directory).unwrap();
    assert_eq!(output.status.code(), Some(1));
}
