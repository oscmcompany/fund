//! The `verdicts` view of `views.sql` run by a real `duckdb` over a local journal laid out as the profile bucket is, so
//! its join to declarations is read rather than stubbed.

use std::path::{Path, PathBuf};
use std::process::Command;

fn root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

/// The statement opening on `opening`, through its first line ending in `;`, as `check-views` reads a view.
fn statement(views: &str, opening: &str) -> String {
    let start = views
        .find(opening)
        .unwrap_or_else(|| panic!("views.sql holds {opening}"));
    let mut statement = String::new();
    for line in views[start..].lines() {
        statement.push_str(line);
        statement.push('\n');
        if line.trim_end().ends_with(';') {
            return statement;
        }
    }
    panic!("{opening} never ends a line in `;`")
}

/// The macros `verdicts` calls and the view itself, reading `records` in place of the profile bucket.
fn pointed(records: &Path) -> String {
    let views = std::fs::read_to_string(root().join("views.sql")).unwrap();
    let script = format!(
        "SET VARIABLE profile_bucket = '{}';\n{}\n{}\n{}\n",
        records.display(),
        statement(&views, "CREATE OR REPLACE MACRO partitions("),
        statement(&views, "CREATE OR REPLACE MACRO sessions("),
        statement(&views, "CREATE OR REPLACE VIEW verdicts AS\n"),
    )
    .replace("'s3://' || ", "");
    assert!(!script.contains("s3://"), "{script}");
    script
}

/// A researcher journal line at `time` on 2026-11-02 UTC.
fn line(time: &str, event_type: &str, payload: &str) -> String {
    format!(
        "SELECT 'fixture' AS run_id, TIMESTAMPTZ '2026-11-02 {time}+00' AS timestamp, 'abc' AS \"commit\", \
         '{event_type}' AS event_type, '{payload}' AS payload"
    )
}

fn declared(time: &str, label: &str, question: &str, bar: &str) -> String {
    line(
        time,
        "question_declared",
        &format!(
            r#"{{"label":"{label}","question":"{question}","baseline":"null_strategy","metric":"net_bp","bar":{{"above":{bar}}},"as_of":"2026-10-30","parameters":{{}}}}"#
        ),
    )
}

fn reached(time: &str, question: &str, outcome: &str) -> String {
    line(
        time,
        "verdict_reached",
        &format!(r#"{{"label":"study","question":"{question}","outcome":{outcome},"runs":[]}}"#),
    )
}

/// Each verdict reads the latest declaration of its question made at or before it, so a re-declaration after a
/// verdict leaves that verdict's bar alone, another study's declaration of the same question is ignored, and a verdict
/// nothing declared keeps its row.
#[test]
fn test_each_verdict_reads_the_declaration_standing_when_it_was_reached() {
    let directory = std::env::temp_dir().join(format!("verdicts-{}", uuid::Uuid::new_v4()));
    let records = directory.join("records");
    let journal = records.join("records/journal/producer=researcher/year=2026/month=11/day=02");
    std::fs::create_dir_all(&journal).unwrap();
    let refuted = r#"{"kind":"refuted","scope":"every name","reopen":"a lower cost"}"#;
    let lines = [
        declared("14:00:00", "study", "first", "1.0"),
        reached("14:30:00", "first", refuted),
        declared("15:00:00", "study", "first", "0.1"),
        declared("15:15:00", "other", "first", "9.0"),
        reached("15:30:00", "first", r#"{"kind":"live","reading":0.5}"#),
        reached(
            "16:00:00",
            "undeclared",
            r#"{"kind":"inconclusive","cause":"too few sessions"}"#,
        ),
    ];
    let script = format!(
        "SET TimeZone = 'UTC';\n\
         COPY ({}) TO '{journal}/data.parquet';\n\
         {}\n\
         SELECT strftime(timestamp, '%H:%M'), question, outcome, coalesce(bar ->> '$.above', 'none'), \
         coalesce(strftime(declared_at, '%H:%M'), 'none'), len(runs) FROM verdicts ORDER BY timestamp;\n",
        lines.join(" UNION ALL "),
        pointed(&records),
        journal = journal.display(),
    );
    let output = Command::new("duckdb")
        .args(["-csv", "-noheader", "-c", &script])
        .output();
    std::fs::remove_dir_all(&directory).unwrap();
    let output = output.expect("duckdb is on the path, as devenv provides it");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        output.status.success(),
        "{stdout}{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        stdout.lines().collect::<Vec<_>>(),
        [
            "14:30,first,refuted,1.0,14:00,0",
            "15:30,first,live,0.1,15:00,0",
            "16:00,undeclared,inconclusive,none,none,0",
        ]
    );
}
