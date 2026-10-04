//! What `check-views` reports, run whole against a scripted `duckdb` in place of S3: the script is the contract, so
//! only the binary it shells out to is replaced.

use std::path::{Path, PathBuf};
use std::process::Command;

/// How the fake `duckdb` answers one view.
enum Answer {
    Rows(u64),
    Error(&'static str),
}

const NOTHING_WRITTEN: &str = "IO Error: No files found that match the pattern \"s3://bucket/x\"";
const OUT_OF_REACH: &str =
    "HTTP Error: HTTP GET error reading 's3://bucket/x' (HTTP 403 Forbidden)";

fn root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

fn scratch() -> PathBuf {
    let directory = std::env::temp_dir().join(format!("check-views-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&directory).unwrap();
    directory
}

/// Runs `script` with a `duckdb` that answers each view as `answers` says, and any other view as a producer that has
/// written nothing. Nothing this process writes is executed, so no sibling test's fork can leave it busy.
fn check(script: &Path, answers: &[(&str, Answer)], profile: &str) -> (i32, String) {
    let directory = scratch();
    let cases: String = answers
        .iter()
        .map(|(view, answer)| match answer {
            Answer::Rows(rows) => format!("  {view}) echo {rows} ;;\n"),
            Answer::Error(error) => format!("  {view}) echo '{error}' >&2; exit 1 ;;\n"),
        })
        .collect();
    let answers_file = directory.join("answers.sh");
    std::fs::write(
        &answers_file,
        format!(
            "view=\"$(sed -nE 's/^CREATE OR REPLACE VIEW ([a-z0-9_]+) AS$/\\1/p')\"\n\
             case \"$view\" in\n{cases}  *) echo '{NOTHING_WRITTEN}' >&2; exit 1 ;;\nesac\n"
        ),
    )
    .unwrap();
    let output = Command::new("bash")
        .arg(script)
        .env("DUCKDB", root().join("tests/fixtures/fake-duckdb"))
        .env("FAKE_DUCKDB_ANSWERS", &answers_file)
        .env("AWS_S3_ARCHIVE_BUCKET_NAME", "archive")
        .env("AWS_S3_RECORDS_BUCKET_NAME", "records")
        .env("FUND_PROFILE", profile)
        .output()
        .unwrap();
    std::fs::remove_dir_all(&directory).unwrap();
    (
        output.status.code().unwrap(),
        String::from_utf8_lossy(&output.stdout).into_owned(),
    )
}

fn real(answers: &[(&str, Answer)], profile: &str) -> (i32, String) {
    check(&root().join("check-views"), answers, profile)
}

fn bars(daily: Answer, minute: Answer) -> Vec<(&'static str, Answer)> {
    vec![
        ("massive_daily_bars", daily),
        ("alpaca_minute_bars", minute),
    ]
}

/// The view each output line names, so a test first knows every view was reported.
fn reported(output: &str) -> Vec<&str> {
    output
        .lines()
        .filter_map(|line| line.split_once(": ").map(|(view, _)| view))
        .collect()
}

#[test]
fn test_every_view_in_views_sql_is_checked() {
    let mut answers = bars(Answer::Rows(5), Answer::Rows(7));
    answers.extend([
        ("journal", Answer::Rows(2)),
        ("logs", Answer::Rows(3)),
        ("experiments", Answer::Rows(1)),
    ]);
    let (status, output) = real(&answers, "");
    assert_eq!(
        reported(&output),
        [
            "massive_daily_bars",
            "alpaca_minute_bars",
            "journal",
            "logs",
            "experiments"
        ]
    );
    assert_eq!(status, 0, "{output}");
}

#[test]
fn test_an_empty_view_fails() {
    let (status, output) = real(&bars(Answer::Rows(0), Answer::Rows(7)), "production");
    assert_eq!(status, 3, "{output}");
    assert!(
        output.contains("massive_daily_bars: reads no rows"),
        "{output}"
    );
}

#[test]
fn test_a_view_that_does_not_create_fails() {
    let (status, output) = real(
        &bars(Answer::Rows(5), Answer::Error("Binder Error: no column")),
        "production",
    );
    assert_eq!(status, 3, "{output}");
    assert!(
        output.contains("alpaca_minute_bars: did not create: Binder Error: no column"),
        "{output}"
    );
}

#[test]
fn test_a_dormant_view_is_empty_in_every_development_profile() {
    for profile in ["development", "development/john.forstmeier"] {
        let (status, output) = real(&bars(Answer::Rows(5), Answer::Rows(7)), profile);
        assert_eq!(status, 0, "{profile}: {output}");
        assert!(output.contains("logs: dormant\n"), "{profile}: {output}");
    }
}

#[test]
fn test_a_dormant_view_that_reads_rows_fails() {
    let mut answers = bars(Answer::Rows(5), Answer::Rows(7));
    answers.push(("logs", Answer::Rows(4)));
    let (status, output) = real(&answers, "development");
    assert_eq!(status, 3, "{output}");
    assert!(
        output.contains("logs: dormant but reads 4 rows"),
        "{output}"
    );
}

/// A developer's studies may or may not have shipped, so their journal and experiments pass either way.
#[test]
fn test_an_optional_view_passes_empty_or_read_but_fails_when_broken() {
    let (status, output) = real(&bars(Answer::Rows(5), Answer::Rows(7)), "development");
    assert_eq!(status, 0, "{output}");
    assert!(
        output.contains("journal: optional, nothing written\n"),
        "{output}"
    );
    assert!(
        output.contains("experiments: optional, nothing written\n"),
        "{output}"
    );
    let mut answers = bars(Answer::Rows(5), Answer::Rows(7));
    answers.extend([
        ("journal", Answer::Rows(4)),
        ("experiments", Answer::Rows(0)),
    ]);
    let (status, output) = real(&answers, "development/john.forstmeier");
    assert_eq!(status, 0, "{output}");
    assert!(output.contains("journal: optional, 4 rows\n"), "{output}");
    assert!(
        output.contains("experiments: optional, 0 rows\n"),
        "{output}"
    );
    let mut answers = bars(Answer::Rows(5), Answer::Rows(7));
    answers.extend([
        ("journal", Answer::Rows(2)),
        ("logs", Answer::Rows(3)),
        ("experiments", Answer::Error("Binder Error: no column")),
    ]);
    let (status, output) = real(&answers, "production");
    assert_eq!(status, 3, "{output}");
    assert!(
        output.contains("experiments: optional, and did not create: Binder Error: no column"),
        "{output}"
    );
}

/// The archiver host ships production's records nightly, so there they are live and must read rows.
#[test]
fn test_production_records_are_live() {
    let mut answers = bars(Answer::Rows(5), Answer::Rows(7));
    answers.extend([("journal", Answer::Rows(2)), ("logs", Answer::Rows(3))]);
    let (status, output) = real(&answers, "production");
    assert_eq!(status, 0, "{output}");
    assert!(output.contains("journal: 2 rows\n"), "{output}");
    let (status, output) = real(&bars(Answer::Rows(5), Answer::Rows(7)), "production");
    assert_eq!(status, 3, "{output}");
    assert!(output.contains("journal: did not create"), "{output}");
    // Each records view on its own: logs missing while the journal reads.
    let mut answers = bars(Answer::Rows(5), Answer::Rows(7));
    answers.push(("journal", Answer::Rows(2)));
    let (status, output) = real(&answers, "production");
    assert_eq!(status, 3, "{output}");
    assert!(output.contains("logs: did not create"), "{output}");
}

#[test]
fn test_every_live_view_out_of_reach_is_a_check_not_made() {
    let mut answers = bars(Answer::Error(OUT_OF_REACH), Answer::Error(OUT_OF_REACH));
    answers.extend([
        ("journal", Answer::Error(OUT_OF_REACH)),
        ("logs", Answer::Error(OUT_OF_REACH)),
        ("experiments", Answer::Error(OUT_OF_REACH)),
    ]);
    let (status, output) = real(&answers, "production");
    assert_eq!(status, 1, "{output}");
}

/// A glob that matches nothing reached S3, so every live view breaking that way is a regression, not an outage.
#[test]
fn test_every_live_glob_matching_nothing_is_a_broken_view() {
    let (status, output) = real(
        &bars(
            Answer::Error(NOTHING_WRITTEN),
            Answer::Error(NOTHING_WRITTEN),
        ),
        "production",
    );
    assert_eq!(status, 3, "{output}");
}

/// A declaration the parser cannot read must fail the check rather than leave its view unchecked.
#[test]
fn test_a_declaration_the_parser_misses_fails_the_check() {
    let directory = scratch();
    std::fs::copy(root().join("check-views"), directory.join("check-views")).unwrap();
    let read_one = "CREATE OR REPLACE VIEW bars_1m AS\nSELECT 1\n);\n";
    std::fs::write(directory.join("views.sql"), read_one).unwrap();
    let (status, output) = check(
        &directory.join("check-views"),
        &[("bars_1m", Answer::Rows(9))],
        "",
    );
    assert_eq!(
        (status, output.as_str()),
        (0, "bars_1m: 9 rows\nAll 1 live views read rows\n")
    );
    let missed = format!("{read_one}CREATE VIEW unparsed AS\nSELECT 1\n);\n");
    std::fs::write(directory.join("views.sql"), missed).unwrap();
    let (status, _) = check(&directory.join("check-views"), &[], "");
    assert_eq!(status, 1);
    std::fs::remove_dir_all(&directory).unwrap();
}
