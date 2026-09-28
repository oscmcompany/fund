//! What the archiver does when it is started, and what provisioning does to a box already running.
//!
//! The shell is where both live, so these run it rather than restate it.

use std::path::{Path, PathBuf};
use std::process::Command;

/// Compiled in for the same reason `test_schedules.rs` does it: a copy that drifted from the
/// working tree would pass.
const PROVISION_ARCHIVER: &str = include_str!("../tools/provision-archiver");

fn tool(name: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tools")
        .join(name)
}

/// A scratch directory of this test's own, so a stub on `PATH` cannot reach another test's.
fn temporary_directory(label: &str) -> PathBuf {
    let directory =
        std::env::temp_dir().join(format!("fund-archiver-boot-{label}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&directory);
    std::fs::create_dir_all(&directory).expect("the directory must be creatable");
    directory
}

fn run(program: &str) -> std::process::Output {
    Command::new("bash")
        .arg("-c")
        .arg(program)
        .output()
        .expect("bash must run")
}

/// The line that took the production box down on 2026-09-23.
///
/// `run-archiver` arms an EXIT trap that calls `StopInstances`, so SIGTERM powers the instance off
/// mid-reconcile. This is a literal check because the reconcile is a JSON array of literals, and
/// that array is what regressed.
#[test]
fn test_the_reconcile_kills_the_unit_rather_than_stopping_it() {
    assert!(
        PROVISION_ARCHIVER.contains("systemctl kill -s KILL fund-archiver.service"),
        "the reconcile must end a running fold with SIGKILL, which runs no trap"
    );
    assert!(
        !PROVISION_ARCHIVER.contains("systemctl stop fund-archiver.service"),
        "a graceful stop runs run-archiver's trap, which powers the box off underneath the reconcile"
    );
}

/// The inhibit a reconcile leaves behind must be the one the next boot can clear itself.
///
/// The operator's `/etc/fund-archiver.inhibit` keeps the box *up*, which is right for someone
/// sitting at it and ruinous for a scheduled start that then never stops.
#[test]
fn test_the_reconcile_writes_the_one_shot_inhibit_and_not_the_operators() {
    assert!(
        PROVISION_ARCHIVER.contains("touch /var/lib/fund/inhibit-once"),
        "the reconcile must write the one-shot inhibit"
    );
    assert!(
        !PROVISION_ARCHIVER.contains("touch /etc/fund-archiver.inhibit"),
        "the operator's inhibit keeps the box up, so a reconcile must never leave it armed"
    );
}

/// `Delayed` means SSM could not reach the target and will retry, which is what a box that has just
/// booted looks like. Treating it as terminal fails a reconcile that is still going to succeed.
#[test]
fn test_a_delayed_command_keeps_polling_and_reports_the_status_it_reaches() {
    let directory = temporary_directory("delayed");
    let statuses = directory.join("statuses");
    std::fs::write(&statuses, "Delayed\nDelayed\nSuccess\n").expect("the file must be writable");

    // A stub `aws` that reads one status per call, so the sequence rather than a single value is
    // what the wait sees.
    let stub = directory.join("aws");
    std::fs::write(
        &stub,
        format!(
            "#!/usr/bin/env bash\nCOUNT_FILE={directory}/count\nCOUNT=$(cat \"$COUNT_FILE\" 2>/dev/null || echo 1)\n\
             echo $((COUNT + 1)) > \"$COUNT_FILE\"\nsed -n \"${{COUNT}}p\" {statuses}\n",
            directory = directory.display(),
            statuses = statuses.display()
        ),
    )
    .expect("the stub must be writable");
    permit_execution(&stub);

    let output = run(&format!(
        "set -euo pipefail\nPATH={directory}:$PATH\nREGION=us-east-1\n\
         eval \"$(sed -n '/^command_status() {{/,/^}}/p;/^wait_for_the_command() {{/,/^}}/p' {script})\"\n\
         wait_for_the_command command-id i-0 5 0\n",
        directory = directory.display(),
        script = tool("provision-archiver").display()
    ));
    assert!(
        output.status.success(),
        "the fragment must run: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        String::from_utf8_lossy(&output.stdout).trim(),
        "Success",
        "a Delayed invocation must be polled again, not reported as the outcome"
    );
    let _ = std::fs::remove_dir_all(&directory);
}

/// The budget expires inside a pause, so the last polled value is a sample from before it.
///
/// Here the command turns `Success` on the call *after* the final polling attempt, which is exactly
/// the window a loop that returns its last sample reports as a failure.
#[test]
fn test_the_status_is_read_once_more_after_the_budget_expires() {
    let directory = temporary_directory("stale");
    let statuses = directory.join("statuses");
    std::fs::write(&statuses, "InProgress\nInProgress\nSuccess\n")
        .expect("the file must be writable");

    let stub = directory.join("aws");
    std::fs::write(
        &stub,
        format!(
            "#!/usr/bin/env bash\nCOUNT_FILE={directory}/count\nCOUNT=$(cat \"$COUNT_FILE\" 2>/dev/null || echo 1)\n\
             echo $((COUNT + 1)) > \"$COUNT_FILE\"\nsed -n \"${{COUNT}}p\" {statuses}\n",
            directory = directory.display(),
            statuses = statuses.display()
        ),
    )
    .expect("the stub must be writable");
    permit_execution(&stub);

    // Two attempts, so the loop reads InProgress twice and the third read is the one after it.
    let output = run(&format!(
        "set -euo pipefail\nPATH={directory}:$PATH\nREGION=us-east-1\n\
         eval \"$(sed -n '/^command_status() {{/,/^}}/p;/^wait_for_the_command() {{/,/^}}/p' {script})\"\n\
         wait_for_the_command command-id i-0 2 0\n",
        directory = directory.display(),
        script = tool("provision-archiver").display()
    ));
    assert!(
        output.status.success(),
        "the fragment must run: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        String::from_utf8_lossy(&output.stdout).trim(),
        "Success",
        "a command that finishes during the final pause must not be reported by its stale status"
    );
    let _ = std::fs::remove_dir_all(&directory);
}

/// Consumed once and gone, which is what makes an unfinished reconcile cost a fold rather than a
/// box that stays up until the stall alarm.
#[test]
fn test_the_one_shot_inhibit_is_reported_once_and_removed() {
    let directory = temporary_directory("one-shot");
    let inhibit = directory.join("inhibit-once");
    std::fs::write(&inhibit, "").expect("the file must be writable");

    let output = run(&format!(
        "set -uo pipefail\n\
         eval \"$(sed -n '/^consume_a_one_shot_inhibit() {{/,/^}}/p' {script})\"\n\
         consume_a_one_shot_inhibit {inhibit} && echo FIRST=present || echo FIRST=absent\n\
         consume_a_one_shot_inhibit {inhibit} && echo SECOND=present || echo SECOND=absent\n",
        script = tool("run-archiver").display(),
        inhibit = inhibit.display()
    ));
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("FIRST=present"),
        "a present one-shot inhibit must be reported: {stdout}"
    );
    assert!(
        stdout.contains("SECOND=absent"),
        "it must not survive being consumed, or every boot would skip its fold: {stdout}"
    );
    assert!(
        !inhibit.exists(),
        "the file itself must be gone, not merely reported"
    );
    let _ = std::fs::remove_dir_all(&directory);
}

/// The one-shot branch must sit under the trap, or consuming it skips the fold *and* leaves the box
/// running -- the failure it exists to prevent.
#[test]
fn test_the_one_shot_inhibit_is_consumed_after_the_stop_trap_is_armed() {
    let script =
        std::fs::read_to_string(tool("run-archiver")).expect("the script must be readable");
    let trap = script
        .find("trap stop_self EXIT")
        .expect("run-archiver must arm the stop trap");
    let consume = script
        .find("if consume_a_one_shot_inhibit")
        .expect("run-archiver must consume the one-shot inhibit");
    assert!(
        trap < consume,
        "the box must already be set to stop before a one-shot inhibit can exit the run"
    );
}

fn permit_execution(path: &Path) {
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o755))
        .expect("the stub must be executable");
}
