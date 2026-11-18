//! What `snapshot` refuses against a scratch repository: anything not public on `origin/master` exits 3, names its
//! reason and writes nothing, while a public target renders from its published bytes.

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
            "name,note\na,\"one, two\"\nb,\"line\r\nbreak\"\nc,beyond\n",
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

/// Stand-ins for `freeze` and `resvg`: the first requires the theme, edits the working copy as a race would, then
/// copies its source to the output, so the PNG holds exactly the bytes the script chose to render.
const FREEZE: &str = r#"#!/usr/bin/env bash
set -euo pipefail
while (($#)); do
  case "$1" in
  -c) theme="$2"; shift 2 ;;
  -o) output="$2"; shift 2 ;;
  --lines | --language) shift 2 ;;
  *) source="$1"; shift ;;
  esac
done
[[ -f "${theme}" ]]
printf 'unpublished\n' >src/public.rs
cp "${source}" "${output}"
"#;
const RESVG: &str = "#!/usr/bin/env bash\ncp \"${@: -2:1}\" \"${@: -1}\"\n";

#[test]
fn renders_the_published_bytes_of_each_public_target() {
    let scratch = Scratch::new();
    let stubs = scratch.repository.join(".stubs");
    std::fs::create_dir_all(&stubs).unwrap();
    for (name, body) in [("freeze", FREEZE), ("resvg", RESVG)] {
        let stub = stubs.join(name);
        std::fs::write(&stub, body).unwrap();
        std::fs::set_permissions(&stub, std::os::unix::fs::PermissionsExt::from_mode(0o755))
            .unwrap();
    }
    // A user's DuckDB settings must not reach the rendered table.
    std::fs::write(stubs.join(".duckdbrc"), ".timer on\n").unwrap();
    let path = format!(
        "{}:{}",
        stubs.display(),
        std::env::var("PATH").unwrap_or_default()
    );
    let snapshots = scratch
        .repository
        .canonicalize()
        .unwrap()
        .join(".scratchpad/snapshots");
    let render = |arguments: &[&str], name: &str| -> String {
        let output = isolated(script())
            .current_dir(&scratch.repository)
            .env("PATH", &path)
            .env("SNAPSHOT_FONTS", &stubs)
            .env("HOME", &stubs)
            .args(arguments)
            .output()
            .unwrap();
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert_eq!(output.status.code(), Some(0), "{arguments:?}: {stderr}");
        let printed = String::from_utf8(output.stdout).unwrap();
        assert_eq!(printed.trim(), snapshots.join(name).to_str().unwrap());
        std::fs::read_to_string(snapshots.join(name)).unwrap()
    };

    assert_eq!(
        render(&["code", "src/public.rs:1-1"], "public.rs-1-1.png"),
        "fn main() {}\n"
    );
    let table = render(&["data", "tests/fixtures/sample.csv", "2"], "sample.png");
    let lines: Vec<&str> = table.lines().map(str::trim_end).collect();
    assert_eq!(lines.len(), 4, "{table}");
    assert_eq!(
        lines[0].split_whitespace().collect::<Vec<_>>(),
        ["name", "note"]
    );
    assert!(
        lines[2].starts_with('a') && lines[2].ends_with(" one, two"),
        "{table}"
    );
    assert!(
        lines[3].starts_with('b') && lines[3].ends_with(" line break"),
        "{table}"
    );
    let commit = render(
        &["commit", &scratch.published],
        &format!("commit-{}.png", &scratch.published[..8]),
    );
    assert!(commit.contains("Public"), "{commit}");
}
