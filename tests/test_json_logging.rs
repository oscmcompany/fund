//! What `tools/json-log.sh` guarantees to the two run scripts that source it.
//!
//! The shell is where this lives, so these run it rather than restate it.

use std::path::{Path, PathBuf};
use std::process::Command;

fn helper() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tools")
        .join("json-log.sh")
}

/// A scratch log directory of this test's own, so one test's lines cannot reach another's file.
fn temporary_directory(label: &str) -> PathBuf {
    let directory =
        std::env::temp_dir().join(format!("fund-json-log-{label}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&directory);
    std::fs::create_dir_all(&directory).expect("the directory must be creatable");
    directory
}

/// Sources the helper with `directory` as the log directory and runs `body` under it.
fn run_under_the_helper(directory: &Path, body: &str) -> std::process::Output {
    Command::new("bash")
        .arg("-c")
        .arg(format!(
            "set -uo pipefail\n\
             JSON_LOG_TARGET=test\n\
             FUND_LOG_DIRECTORY={directory}\n\
             source {helper}\n\
             {body}\n",
            directory = directory.display(),
            helper = helper().display(),
        ))
        .output()
        .expect("bash must run")
}

fn only_log_file(directory: &Path) -> (String, String) {
    let mut entries: Vec<_> = std::fs::read_dir(directory)
        .expect("the log directory must be readable")
        .map(|entry| entry.expect("the entry must be readable").path())
        .filter(|path| path.extension().is_some_and(|extension| extension == "log"))
        .collect();
    assert_eq!(entries.len(), 1, "exactly one log file: {entries:?}");
    let path = entries.remove(0);
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .expect("the name must be text")
        .to_string();
    let contents = std::fs::read_to_string(&path).expect("the file must be readable");
    (name, contents)
}

/// `export_logs` collects `<date>.<service>.log` and nothing else, so a file named any other way
/// is written, never shipped, and never noticed.
#[test]
fn test_the_log_is_named_the_only_shape_the_export_collects() {
    let directory = temporary_directory("naming");

    run_under_the_helper(&directory, "open_the_json_log researcher\nlog hello");

    let (name, _) = only_log_file(&directory);
    let (date, service) = name
        .strip_suffix(".log")
        .and_then(|stem| stem.split_once('.'))
        .unwrap_or_else(|| panic!("a dated, service-named file: {name}"));
    assert_eq!(service, "researcher");
    chrono::NaiveDate::parse_from_str(date, "%Y-%m-%d")
        .unwrap_or_else(|_| panic!("the first field must be a date: {name}"));
    let _ = std::fs::remove_dir_all(&directory);
}

/// One unparsable line keeps its whole file from ever being deleted, so a message carrying a quote
/// or a control character has to survive the escaping rather than break the line it is in.
#[test]
fn test_every_line_survives_as_json_including_an_awkward_message() {
    let directory = temporary_directory("escaping");

    run_under_the_helper(
        &directory,
        "open_the_json_log researcher\n\
         log 'a \"quoted\" path C:\\\\fund and a tab\tinside'\n\
         run_wrapped leg printf 'plain %s\\n' 'line'",
    );

    let (_, contents) = only_log_file(&directory);
    let lines: Vec<&str> = contents.lines().collect();
    assert_eq!(lines.len(), 2, "both lines must be written: {contents}");
    for line in lines {
        let parsed: serde_json::Value =
            serde_json::from_str(line).unwrap_or_else(|error| panic!("{error}: {line}"));
        assert!(parsed.get("timestamp").is_some(), "no timestamp: {line}");
        assert!(parsed.get("level").is_some(), "no level: {line}");
        assert!(
            parsed.pointer("/fields/message").is_some(),
            "the reader keeps only objects carrying a message: {line}"
        );
    }
    let _ = std::fs::remove_dir_all(&directory);
}

/// The wrapper is a pipeline, and a pipeline reports its *last* element's status by default. Losing
/// the leg's would make a failed fold or a failed training run read as a clean night.
#[test]
fn test_a_failed_leg_keeps_its_status_through_the_wrapper() {
    let directory = temporary_directory("status");

    let output = run_under_the_helper(
        &directory,
        "open_the_json_log researcher\n\
         run_wrapped leg bash -c 'echo working; exit 3'\n\
         printf 'STATUS=%s' \"$?\" > \"$FUND_LOG_DIRECTORY/status\"\n",
    );

    assert!(output.status.success(), "the script itself must not crash");
    let status = std::fs::read_to_string(directory.join("status")).unwrap_or_default();
    assert_eq!(status, "STATUS=3", "the leg's own status, not the pipe's");
    let _ = std::fs::remove_dir_all(&directory);
}

/// A second leg must not inherit the first one's failure, or one bad night would report every
/// later leg as failed too.
#[test]
fn test_a_clean_leg_after_a_failed_one_reports_clean() {
    let directory = temporary_directory("reset");

    run_under_the_helper(
        &directory,
        "open_the_json_log researcher\n\
         run_wrapped first bash -c 'exit 3' || true\n\
         run_wrapped second bash -c 'exit 0'\n\
         printf 'STATUS=%s' \"$?\" > \"$FUND_LOG_DIRECTORY/status\"\n",
    );

    let status = std::fs::read_to_string(directory.join("status")).unwrap_or_default();
    assert_eq!(status, "STATUS=0");
    let _ = std::fs::remove_dir_all(&directory);
}

/// A child's own output must not be able to impersonate the wrapper's bookkeeping. The status once
/// travelled as a `__EXIT__` line through the same stream the child writes on.
#[test]
fn test_a_child_cannot_fake_its_own_exit_status() {
    let directory = temporary_directory("impersonation");

    run_under_the_helper(
        &directory,
        "open_the_json_log researcher\n\
         run_wrapped leg bash -c 'echo __EXIT__5'\n\
         printf 'STATUS=%s' \"$?\" > \"$FUND_LOG_DIRECTORY/status\"\n",
    );

    let status = std::fs::read_to_string(directory.join("status")).unwrap_or_default();
    assert_eq!(
        status, "STATUS=0",
        "the child succeeded; only its output said otherwise"
    );
    let (_, contents) = only_log_file(&directory);
    assert!(
        contents.contains("__EXIT__5"),
        "and the line itself must still be logged: {contents}"
    );
    let _ = std::fs::remove_dir_all(&directory);
}

/// `read` returns false on a final line with no newline, so without the second half of the loop
/// condition the last thing a crashing child said is the one line that never reaches the log.
#[test]
fn test_a_final_line_without_a_newline_is_still_logged() {
    let directory = temporary_directory("unterminated");

    run_under_the_helper(
        &directory,
        "open_the_json_log researcher\n\
         run_wrapped leg bash -c \"printf 'last diagnostic'\"\n",
    );

    let (_, contents) = only_log_file(&directory);
    assert!(
        contents.contains("last diagnostic"),
        "the unterminated line must be logged: {contents}"
    );
    let _ = std::fs::remove_dir_all(&directory);
}

/// A failed export after a clean fold is still a failed run: the records never left a box that is
/// about to power off, which is the one failure nothing later recovers from. Both run scripts end
/// on this, which is why it lives in the helper rather than twice in shell.
#[test]
fn test_the_reported_status_is_the_first_leg_that_failed() {
    let directory = temporary_directory("first-failure");

    run_under_the_helper(
        &directory,
        "for pair in '0 0' '0 3' '2 0' '2 3'; do \
           printf '%s=%s\\n' \"$pair\" \"$(first_failure $pair)\"; \
         done > \"$FUND_LOG_DIRECTORY/answers\"",
    );

    let answers = std::fs::read_to_string(directory.join("answers")).expect("the file must exist");
    assert_eq!(
        answers.lines().collect::<Vec<_>>(),
        vec!["0 0=0", "0 3=3", "2 0=2", "2 3=2"],
        "a clean run reports zero, either failing leg reports itself, and the first one wins"
    );
    let _ = std::fs::remove_dir_all(&directory);
}
