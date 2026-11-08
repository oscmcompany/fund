//! What `check-timezones` reports over a scratch directory: each way of deriving a date from an instant fails it
//! until the zone is pinned, and the lines that come closest without deriving one pass.

use std::path::PathBuf;
use std::process::Command;

fn script() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("checks/timezones")
}

/// The script's exit code and output over a directory holding each `(name, contents)` file.
fn check(files: &[(&str, &str)]) -> (i32, String) {
    let directory = std::env::temp_dir().join(format!("check-timezones-{}", uuid::Uuid::new_v4()));
    for (name, contents) in files {
        let file = directory.join(name);
        std::fs::create_dir_all(file.parent().unwrap()).unwrap();
        std::fs::write(&file, contents).unwrap();
    }
    let output = Command::new(script()).arg(&directory).output().unwrap();
    std::fs::remove_dir_all(&directory).unwrap();
    let stderr = String::from_utf8_lossy(&output.stderr);
    (
        output.status.code().unwrap(),
        format!(
            "{}{}",
            String::from_utf8_lossy(&output.stdout),
            stderr.replace(&format!("{}/", directory.display()), "")
        ),
    )
}

#[test]
fn test_each_unpinned_date_fails_and_is_named() {
    for (name, contents, line) in [
        (
            "cast.sql",
            "SELECT 1;\nSELECT CAST(timestamp AS DATE) FROM quotes;\n",
            2,
        ),
        ("shorthand.sql", "SELECT timestamp::date FROM quotes;\n", 1),
        (
            "truncate.sql",
            "SELECT date_trunc('day', timestamp) FROM quotes;\n",
            1,
        ),
        (
            "format.sql",
            "SELECT strftime(timestamp, '%Y-%m-%d') FROM quotes;\n",
            1,
        ),
        ("part.sql", "SELECT day(timestamp) FROM quotes;\n", 1),
        (
            "wrapped.sql",
            "SELECT CAST(\n    timestamp\n    AS DATE\n) FROM quotes;\n",
            3,
        ),
        (
            "late.sql",
            "SELECT timestamp::DATE FROM quotes;\nSET TimeZone = 'UTC';\n",
            1,
        ),
        (
            "study.rs",
            "const SQL: &str = \"\n    SELECT timestamp::DATE AS session FROM quotes\";\n",
            2,
        ),
        (
            "job.sh",
            "duckdb -c \"SELECT date_part('dow', timestamp) FROM quotes\"\n",
            1,
        ),
        (
            "bin/job",
            "#!/usr/bin/env bash\nduckdb -c \"SELECT extract(day FROM timestamp) FROM quotes\"\n",
            2,
        ),
        (
            "inline.sql",
            "SELECT timestamp::DATE FROM quotes; SET TimeZone = 'UTC';\n",
            1,
        ),
        (
            "upper.rs",
            "const SQL: &str = \"SELECT CAST(timestamp AS DATE) FROM quotes\";\n",
            1,
        ),
        (
            "upper.sh",
            "duckdb -c \"SELECT CAST(timestamp AS DATE) FROM quotes\"\n",
            1,
        ),
        (
            "lower.sh",
            "duckdb -c \"SELECT timestamp::date FROM quotes\"\n",
            1,
        ),
        (
            "part.sh",
            "duckdb -c \"SELECT day(timestamp) FROM quotes\"\n",
            1,
        ),
        (
            "connections.sh",
            "duckdb -c \"SET TimeZone = 'UTC'; SELECT 1\"\nduckdb -c \"SELECT timestamp::DATE FROM quotes\"\n",
            2,
        ),
    ] {
        let (code, output) = check(&[(name, contents)]);
        assert_eq!(code, 3, "{name}: {output}");
        let named: Vec<&str> = output
            .lines()
            .filter_map(|hit| hit.split_once(": ").map(|(location, _)| location))
            .filter(|location| !location.starts_with("Error"))
            .collect();
        assert_eq!(named, [format!("{name}:{line}")], "{output}");
    }
}

#[test]
fn test_a_pinned_or_converted_date_passes() {
    let (code, output) = check(&[
        (
            "pinned.sql",
            "-- Instants print in UTC.\nSET TimeZone = 'UTC';\nSELECT timestamp::DATE FROM quotes;\n",
        ),
        (
            "global.sql",
            "set global timezone='America/New_York';\nSELECT date_trunc('day', timestamp);\n",
        ),
        (
            "converted.sql",
            "SELECT (timestamp AT TIME ZONE 'America/New_York')::DATE FROM quotes;\n",
        ),
        (
            "function.sql",
            "SELECT CAST(timezone('UTC', timestamp) AS DATE) FROM quotes;\n",
        ),
        ("comment.sql", "SELECT 1; -- never timestamp::DATE\n"),
        (
            "pinned.rs",
            "const SQL: &str = \"SET TimeZone = 'UTC'; SELECT timestamp::DATE FROM quotes\";\n",
        ),
        (
            "rust.rs",
            "// SELECT timestamp::DATE\nfn session() { calendar.map(TradingSession::date); bar.day(); }\n\
             const SQL: &str = \"SELECT regexp_extract(file, 'day=(\\\\d+)') FROM files\";\n",
        ),
        ("unread.rs", "fn main() { let _ = timestamp::DATE; }\n"),
        (
            "connection.sh",
            "duckdb -c \"SET TimeZone = 'UTC'; SELECT timestamp::DATE FROM quotes\"\n",
        ),
        (
            "inline.sql",
            "SET TimeZone = 'UTC'; SELECT timestamp AT TIME ZONE 'UTC' FROM quotes;\n\
             SELECT timestamp::DATE FROM quotes;\n",
        ),
        (
            "heredoc.sh",
            "duckdb <<'SQL'\nSET TimeZone = 'UTC';\nSELECT timestamp::DATE FROM quotes;\nSQL\n",
        ),
        (
            "commented.sh",
            "# SELECT timestamp::DATE\nduckdb -c \"SELECT 1\"\n",
        ),
        (
            "bin/python",
            "#!/usr/bin/env python3\nprint(\"SELECT timestamp::DATE FROM quotes\")\n",
        ),
        ("notes.txt", "SELECT timestamp::DATE FROM quotes;\n"),
    ]);
    assert_eq!(code, 0, "{output}");
    assert!(output.contains("in 11 files"), "{output}");
}

#[test]
fn test_the_tracked_tree_passes() {
    let output = Command::new(script()).output().unwrap();
    assert_eq!(output.status.code(), Some(0), "{output:?}");
}

#[test]
fn test_a_missing_path_cannot_be_checked() {
    let output = Command::new(script())
        .arg(std::env::temp_dir().join(format!("check-timezones-{}", uuid::Uuid::new_v4())))
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(1), "{output:?}");
}

#[test]
fn test_an_unreadable_file_cannot_be_checked() {
    use std::os::unix::fs::PermissionsExt;
    let directory = std::env::temp_dir().join(format!("check-timezones-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&directory).unwrap();
    let file = directory.join("job.sh");
    std::fs::write(&file, "duckdb -c \"SELECT 1\"\n").unwrap();
    std::fs::set_permissions(&file, std::fs::Permissions::from_mode(0o000)).unwrap();
    let output = Command::new(script()).arg(&directory).output().unwrap();
    std::fs::remove_dir_all(&directory).unwrap();
    assert_eq!(output.status.code(), Some(1), "{output:?}");
}
