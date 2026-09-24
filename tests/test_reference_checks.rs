//! What the two `--check` tools tell the nightly, whose record reads their exit status.
//!
//! The shell is where the contract lives, so these run it rather than restate it.

use std::path::{Path, PathBuf};
use std::process::Command;

fn tool(name: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tools")
        .join(name)
}

/// Runs a tool's own `leave` trap around a script that exits with `status`.
///
/// Lifted out of the tool with `sed` and run unchanged, so what is under test is the function the
/// tool actually installs rather than a copy of it.
fn exit_through_leave(tool_name: &str, declared: bool, status: i32) -> i32 {
    let program = format!(
        "set -uo pipefail\n\
         WORK=\"$(mktemp -d)\"\n\
         DRIFT_STATUS=3\n\
         DRIFT_DECLARED={declared}\n\
         eval \"$(sed -n '/^leave() {{/,/^}}/p' \"$1\")\"\n\
         trap leave EXIT\n\
         exit {status}\n",
    );
    // The path travels as `$1` rather than inside the program, so a checkout under a directory
    // with a space in its name still reaches `sed` as one argument.
    Command::new("bash")
        .arg("-c")
        .arg(program)
        .arg("bash")
        .arg(tool(tool_name))
        .status()
        .expect("bash must run")
        .code()
        .expect("the script must exit rather than be killed")
}

/// curl exits 3 on a malformed URL, and `set -e` would carry that out of the tool. Read as drift,
/// a check that never reached the provider would be recorded as the provider having moved.
#[test]
fn test_an_undeclared_drift_status_leaves_as_a_failed_check() {
    for name in ["fetch-trade-conditions", "fetch-industry-classifications"] {
        assert_eq!(exit_through_leave(name, false, 3), 1, "{name}");
    }
}

/// The control: the line that decides drift must still be able to say so.
#[test]
fn test_a_declared_drift_leaves_as_drift_and_everything_else_is_untouched() {
    for name in ["fetch-trade-conditions", "fetch-industry-classifications"] {
        assert_eq!(exit_through_leave(name, true, 3), 3, "{name}");
        assert_eq!(exit_through_leave(name, false, 0), 0, "{name}");
        assert_eq!(exit_through_leave(name, false, 1), 1, "{name}");
    }
}

/// Every drift exit in each tool must declare itself first, or `leave` rewrites it to a failure
/// and real drift is never recorded.
#[test]
fn test_every_drift_exit_is_declared() {
    for name in ["fetch-trade-conditions", "fetch-industry-classifications"] {
        let script = std::fs::read_to_string(tool(name)).expect("the tool must be readable");
        let lines: Vec<&str> = script.lines().collect();
        let exits: Vec<usize> = lines
            .iter()
            .enumerate()
            .filter(|(_, line)| line.trim() == "exit \"${DRIFT_STATUS}\"")
            .map(|(index, _)| index)
            .collect();
        assert_eq!(exits.len(), 1, "{name} has exactly one drift exit");
        assert_eq!(
            lines[exits[0] - 1].trim(),
            "DRIFT_DECLARED=true",
            "{name} declares drift on the line before it exits with it"
        );
    }
}
