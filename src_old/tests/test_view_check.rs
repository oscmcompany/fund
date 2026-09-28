//! What `tools/check-views` tells the nightly, run against a scripted `duckdb` in place of S3.
//!
//! The script is the contract, so these run it whole and replace only the binary it shells out to.

use std::path::{Path, PathBuf};
use std::process::Command;

fn root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

/// The views the real initialization script creates, read the way the tool reads them.
fn views() -> Vec<String> {
    let script = std::fs::read_to_string(root().join("tools/duckdb_initialization.sql"))
        .expect("the initialization script must be readable");
    script
        .lines()
        .filter_map(|line| line.strip_prefix("CREATE OR REPLACE VIEW "))
        .filter_map(|rest| rest.strip_suffix(" AS"))
        .map(str::to_string)
        .collect()
}

/// Runs the tool with a `duckdb` that prints `counts` as `view,rows` lines and nothing else.
///
/// A view absent from `counts` is one that failed to create, which is what the real binary shows
/// for it on stdout: nothing. Given one view's block alone, as the tool does to learn why a dormant
/// view failed, the fake prints that view's entry in `errors` to stderr, or the no-files error a
/// producer that has written nothing gets.
fn check(counts: &[(&str, u64)], errors: &[(&str, &str)], profile: &str) -> (i32, String) {
    let directory = std::env::temp_dir().join(format!("check-views-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&directory).expect("a scratch directory");
    let fake = directory.join("duckdb");
    let lines: String = counts
        .iter()
        .map(|(view, rows)| format!("{view},{rows}\n"))
        .collect();
    let cases: String = errors
        .iter()
        .map(|(view, error)| format!("  {view}) echo '{error}' >&2 ;;\n"))
        .collect();
    std::fs::write(
        &fake,
        format!(
            "#!/usr/bin/env bash\n\
             input=\"$(cat)\"\n\
             if grep -q 'DuckDB initialized' <<<\"$input\"; then printf '%s' '{lines}'; exit 0; fi\n\
             view=\"$(sed -nE 's/^CREATE OR REPLACE VIEW ([a-z_]+) AS$/\\1/p' <<<\"$input\")\"\n\
             case \"$view\" in\n{cases}  \
             *) echo 'IO Error: No files found that match the pattern \"s3://records/x\"' >&2 ;;\n\
             esac\n"
        ),
    )
    .expect("the fake binary must write");
    make_executable(&fake);

    let output = Command::new(root().join("tools/check-views"))
        .env("DUCKDB", &fake)
        .env("AWS_S3_ARCHIVE_BUCKET_NAME", "archive")
        .env("AWS_S3_RECORDS_BUCKET_NAME", "records")
        .env("FUND_PROFILE", profile)
        .output()
        .expect("the tool must run");
    std::fs::remove_dir_all(&directory).ok();
    (
        output.status.code().expect("the tool must exit"),
        String::from_utf8_lossy(&output.stdout).into_owned(),
    )
}

fn make_executable(path: &Path) {
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o755))
        .expect("the fake binary must be executable");
}

/// The tool must find every view, or the rest of these would pass over an empty list.
#[test]
fn test_the_views_are_read_off_the_initialization_script() {
    let found = views();
    assert_eq!(found.len(), 14, "found {found:?}");
    for expected in ["training_bars", "journal", "archive_runs", "experiments"] {
        assert!(found.iter().any(|view| view == expected), "{expected}");
    }
}

#[test]
fn test_every_view_reading_rows_passes() {
    let names = views();
    let counts: Vec<(&str, u64)> = names.iter().map(|view| (view.as_str(), 7)).collect();

    let (status, stdout) = check(&counts, &[], "development/someone");

    assert_eq!(status, 0, "{stdout}");
}

/// Both ways a view answers nothing: one that failed to create and one that reads no rows.
#[test]
fn test_an_empty_view_and_a_missing_one_both_fail_the_check() {
    let names = views();
    for (broken, rows) in [("journal", Some(0)), ("journal", None)] {
        let counts: Vec<(&str, u64)> = names
            .iter()
            .filter_map(|view| match (view.as_str() == broken, rows) {
                (true, Some(rows)) => Some((view.as_str(), rows)),
                (true, None) => None,
                (false, _) => Some((view.as_str(), 7)),
            })
            .collect();

        let (status, stdout) = check(&counts, &[], "development/someone");

        assert_eq!(status, 3, "{rows:?}: {stdout}");
        assert!(stdout.contains("journal:"), "{stdout}");
    }
}

/// Production's dormant views may be empty, and one that starts reading rows fails, because its
/// producer has come back and the list must be edited on purpose.
#[test]
fn test_a_dormant_view_may_be_empty_and_may_not_read_rows() {
    let names = views();
    let dormant = [
        "equity_pairs",
        "account_snapshots",
        "account_activities",
        "events",
        "journal",
        "experiments",
    ];
    let live: Vec<(&str, u64)> = names
        .iter()
        .filter(|view| !dormant.contains(&view.as_str()))
        .map(|view| (view.as_str(), 7))
        .collect();

    assert_eq!(check(&live, &[], "production").0, 0);

    let mut returned = live.clone();
    returned.push(("journal", 12));
    let (status, stdout) = check(&returned, &[], "production");
    assert_eq!(status, 3, "{stdout}");
    assert!(
        stdout.contains("remove it from the dormant list"),
        "{stdout}"
    );
}

/// A dormant view is empty only when its producer has written nothing. One that failed for any other
/// reason -- here a denied read -- is broken, which is what a dormant list must not be able to hide.
#[test]
fn test_a_dormant_view_failing_for_another_reason_is_broken() {
    let names = views();
    let dormant = [
        "equity_pairs",
        "account_snapshots",
        "account_activities",
        "events",
        "journal",
        "experiments",
    ];
    let live: Vec<(&str, u64)> = names
        .iter()
        .filter(|view| !dormant.contains(&view.as_str()))
        .map(|view| (view.as_str(), 7))
        .collect();

    let (status, stdout) = check(
        &live,
        &[(
            "journal",
            "HTTP Error: Unable to connect to URL: 403 (Forbidden)",
        )],
        "production",
    );

    assert_eq!(status, 3, "{stdout}");
    assert!(
        stdout.contains("journal: dormant, and failed for a reason other than"),
        "{stdout}"
    );
    assert!(stdout.contains("events: dormant\n"), "{stdout}");
}

/// Nothing readable at all is the check failing to reach S3, and must not be recorded as every view
/// breaking at once.
#[test]
fn test_no_view_readable_is_a_check_not_made() {
    let (status, stdout) = check(&[], &[], "development/someone");

    assert_eq!(status, 1, "{stdout}");
}
