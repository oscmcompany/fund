//! What `snapshot` refuses against a scratch repository: anything not public on `origin/master` exits 3, names its
//! reason and writes nothing, while a public target passes the check.

use std::path::{Path, PathBuf};
use std::process::Command;

fn script() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tools/snapshot")
}

/// A command that ignores the `GIT_*` variables a hook exports, so it acts on the scratch repository it runs in.
fn isolated(program: impl AsRef<std::ffi::OsStr>) -> Command {
    let mut command = Command::new(program);
    for (name, _) in std::env::vars_os() {
        if name.to_string_lossy().starts_with("GIT_") {
            command.env_remove(name);
        }
    }
    command
}

fn git(repository: &Path, arguments: &[&str]) -> String {
    let output = isolated("git")
        .arg("-C")
        .arg(repository)
        .args(["-c", "user.name=test", "-c", "user.email=test@example.com"])
        .args(arguments)
        .output()
        .unwrap();
    assert!(output.status.success(), "git {arguments:?}");
    String::from_utf8(output.stdout).unwrap().trim().to_string()
}

fn write(repository: &Path, path: &str, contents: &str) {
    let file = repository.join(path);
    std::fs::create_dir_all(file.parent().unwrap()).unwrap();
    std::fs::write(file, contents).unwrap();
}

/// A repository whose `origin/master` holds a source file and a CSV, with an ignored plan and an untracked study
/// beside them, and one local commit that `origin/master` does not reach.
struct Scratch {
    repository: PathBuf,
    published: String,
    unpublished: String,
}

impl Scratch {
    fn new() -> Self {
        let repository = std::env::temp_dir().join(format!("snapshot-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&repository).unwrap();
        git(&repository, &["init", "--quiet"]);
        write(
            &repository,
            ".gitignore",
            ".scratchpad/\n/studies/src/bin/\n",
        );
        write(&repository, "src/public.rs", "fn main() {}\n");
        write(
            &repository,
            "tests/fixtures/sample.csv",
            "name,value\na,1\n",
        );
        git(&repository, &["add", "."]);
        git(&repository, &["commit", "--quiet", "-m", "Public"]);
        let published = git(&repository, &["rev-parse", "HEAD"]);
        git(
            &repository,
            &["update-ref", "refs/remotes/origin/master", &published],
        );
        write(&repository, "src/later.rs", "fn later() {}\n");
        git(&repository, &["add", "."]);
        git(&repository, &["commit", "--quiet", "-m", "Local"]);
        let unpublished = git(&repository, &["rev-parse", "HEAD"]);
        write(&repository, ".scratchpad/plan.md", "private\n");
        write(&repository, "studies/src/bin/study.rs", "fn main() {}\n");
        Scratch {
            repository,
            published,
            unpublished,
        }
    }

    /// The script's exit code and standard error for `arguments`, run from inside the repository.
    fn run(&self, arguments: &[&str]) -> (i32, String) {
        let output = isolated(script())
            .current_dir(&self.repository)
            .args(arguments)
            .output()
            .unwrap();
        (
            output.status.code().unwrap(),
            String::from_utf8_lossy(&output.stderr).to_string(),
        )
    }

    fn assert_refused(&self, arguments: &[&str], reason: &str) {
        let (code, stderr) = self.run(arguments);
        assert_eq!(code, 3, "{arguments:?}: {stderr}");
        assert!(stderr.contains(reason), "{arguments:?}: {stderr}");
        assert!(
            !self.repository.join(".scratchpad/snapshots").exists(),
            "{arguments:?} wrote a snapshot"
        );
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.repository);
    }
}

#[test]
fn refuses_every_target_not_public_on_origin_master() {
    let scratch = Scratch::new();
    scratch.assert_refused(
        &["code", ".scratchpad/plan.md:1-1"],
        "not tracked on origin/master",
    );
    scratch.assert_refused(
        &["code", "studies/src/bin/study.rs:1-1"],
        "not tracked on origin/master",
    );
    scratch.assert_refused(
        &["code", "src/later.rs:1-1"],
        "not tracked on origin/master",
    );
    scratch.assert_refused(&["code", "src:1-1"], "not a regular file");
    scratch.assert_refused(&["code", "src/:1-1"], "under that exact path");
    scratch.assert_refused(&["code", "./src/public.rs:1-1"], "under that exact path");
    scratch.assert_refused(&["code", ":/src/public.rs:1-1"], "under that exact path");
    scratch.assert_refused(&["code", "src/public.rs:2-1"], "the line range is empty");
    scratch.assert_refused(&["data", ".scratchpad/plan.md"], "not a CSV file");
    scratch.assert_refused(&["commit", &scratch.unpublished], "not on origin/master");
    scratch.assert_refused(&["commit", "--all"], "not a commit hash");
    scratch.assert_refused(&["commit", "abcdef12"], "no such commit");
}

#[test]
fn refuses_a_public_file_changed_in_the_working_copy() {
    let scratch = Scratch::new();
    write(
        &scratch.repository,
        "src/public.rs",
        "fn main() { secret(); }\n",
    );
    scratch.assert_refused(&["code", "src/public.rs:1-1"], "differs from origin/master");
    write(
        &scratch.repository,
        "tests/fixtures/sample.csv",
        "name,value\na,2\n",
    );
    scratch.assert_refused(
        &["data", "tests/fixtures/sample.csv"],
        "differs from origin/master",
    );
}

#[test]
fn refuses_a_symlink_to_identical_contents() {
    let scratch = Scratch::new();
    write(&scratch.repository, ".scratchpad/copy.rs", "fn main() {}\n");
    std::fs::remove_file(scratch.repository.join("src/public.rs")).unwrap();
    std::os::unix::fs::symlink(
        scratch.repository.join(".scratchpad/copy.rs"),
        scratch.repository.join("src/public.rs"),
    )
    .unwrap();
    scratch.assert_refused(&["code", "src/public.rs:1-1"], "differs from origin/master");
}

#[test]
fn ignores_a_local_branch_named_origin_master() {
    let scratch = Scratch::new();
    git(
        &scratch.repository,
        &["branch", "origin/master", &scratch.unpublished],
    );
    scratch.assert_refused(
        &["code", "src/later.rs:1-1"],
        "not tracked on origin/master",
    );
    scratch.assert_refused(&["commit", &scratch.unpublished], "not on origin/master");
}

#[test]
fn passes_public_targets_through_the_check() {
    let scratch = Scratch::new();
    for arguments in [
        vec!["code", "src/public.rs:1-1"],
        vec!["data", "tests/fixtures/sample.csv", "5"],
        vec!["commit", scratch.published.as_str()],
    ] {
        let (code, stderr) = scratch.run(&arguments);
        assert_ne!(code, 3, "{arguments:?}: {stderr}");
        assert!(!stderr.contains("Refused"), "{arguments:?}: {stderr}");
    }
}
