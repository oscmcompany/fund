//! The `bar_seam` view of `views.sql` run by a real `duckdb` over local files laid out as the buckets are, so its join,
//! units, window and labels are read rather than stubbed.

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

/// The macros `bar_seam` calls and the view itself, reading `records` and `archive` in place of the two buckets.
fn pointed(records: &Path, archive: &Path) -> String {
    let views = std::fs::read_to_string(root().join("views.sql")).unwrap();
    let script = format!(
        "SET VARIABLE records_bucket = '{}';\nSET VARIABLE market_data_bucket = '{}';\n{}\n{}\n{}\n{}\n",
        records.display(),
        archive.display(),
        statement(&views, "CREATE OR REPLACE MACRO sessions("),
        statement(&views, "CREATE OR REPLACE MACRO millionths("),
        statement(&views, "CREATE OR REPLACE MACRO trillionths("),
        statement(&views, "CREATE OR REPLACE VIEW bar_seam AS\n"),
    )
    .replace("'s3://' || ", "");
    assert!(!script.contains("s3://"), "{script}");
    script
}

/// A journal line of the trader's run `fixture`, journaled at 20:00 so its own `timestamp` matches no bar's.
fn line(event_type: &str, payload: &str) -> String {
    format!(
        "SELECT 'fixture' AS run_id, TIMESTAMPTZ '2026-10-08 20:00:00+00' AS timestamp, '{event_type}' AS event_type, \
         '{payload}' AS payload"
    )
}

/// A trader bar in the journal's units: whole-cent prices as millionths, two trades of one share, and the high and
/// low bounding the open and close.
fn built(symbol: &str, interval: &str, minute: &str, open_cents: u64, close_cents: u64) -> String {
    let [open, close] = [open_cents, close_cents].map(|cents| cents * 10_000);
    line(
        "bar_built",
        &format!(
            r#"{{"symbol":"{symbol}","interval":"{interval}","timestamp":"2026-10-08T{minute}:00Z","trade_count":2,"volume":2000000,"dollar_volume":"{dollar_volume}","opened_at":"2026-10-08T{minute}:10Z","open":{open},"closed_at":"2026-10-08T{minute}:50Z","close":{close},"high":{high},"low":{low}}}"#,
            dollar_volume = u128::from(open + close) * 1_000_000,
            high = open.max(close),
            low = open.min(close),
        ),
    )
}

/// The same bar as `built` in the archive's decimal units.
fn derived(symbol: &str, minute: &str, open_cents: u64, close_cents: u64) -> String {
    let dollars = |cents: u64| format!("{}.{:02}", cents / 100, cents % 100);
    let (open, close) = (dollars(open_cents), dollars(close_cents));
    let (high, low) = (
        dollars(open_cents.max(close_cents)),
        dollars(open_cents.min(close_cents)),
    );
    format!(
        "SELECT '{symbol}' AS symbol, TIMESTAMPTZ '2026-10-08 {minute}:00+00' AS timestamp, 2::UBIGINT AS trade_count, \
         2::DECIMAL(20, 6) AS volume, ({open} + {close})::DECIMAL(38, 12) AS dollar_volume, \
         TIMESTAMPTZ '2026-10-08 {minute}:10+00' AS opened_at, {open}::DECIMAL(18, 6) AS open, \
         TIMESTAMPTZ '2026-10-08 {minute}:50+00' AS closed_at, {close}::DECIMAL(18, 6) AS close, \
         {high}::DECIMAL(18, 6) AS high, {low}::DECIMAL(18, 6) AS low"
    )
}

#[test]
fn test_the_seam_matches_minutes_in_the_run_and_differs_only_where_the_bars_do() {
    let directory = std::env::temp_dir().join(format!("bar-seam-{}", uuid::Uuid::new_v4()));
    let (records, archive) = (directory.join("records"), directory.join("archive"));
    let journal = records.join("records/journal/producer=trader/year=2026/month=10/day=08");
    let bars = archive.join(
        "data/equity/stage=parsed/trades/provider=alpaca/origin=derived/interval=one_minute/year=2026/month=10/day=08",
    );
    std::fs::create_dir_all(&journal).unwrap();
    std::fs::create_dir_all(&bars).unwrap();
    let journal_lines = [
        line(
            "configuration_resolved",
            r#"{"parameters":{"universe":{"source":"environment","value":"QQQ,SPY"}}}"#,
        ),
        built("SPY", "one_minute", "14:00", 78_010, 78_015),
        built("SPY", "one_minute", "14:01", 78_015, 78_016),
        built("QQQ", "one_minute", "14:01", 76_100, 76_100),
        // A five-minute bar is no minute of the seam and widens no run.
        built("SPY", "five_minute", "14:05", 78_020, 78_020),
    ];
    let archive_rows = [
        derived("SPY", "13:59", 78_000, 78_000),
        derived("SPY", "14:00", 78_010, 78_015),
        derived("SPY", "14:01", 78_015, 78_015),
        derived("SPY", "14:02", 78_020, 78_020),
        derived("QQQ", "14:00", 76_090, 76_090),
        derived("IWM", "14:00", 25_000, 25_000),
    ];
    let script = format!(
        "SET TimeZone = 'UTC';\n\
         COPY ({}) TO '{}/data.parquet';\n\
         COPY ({}) TO '{}/data.parquet';\n\
         {}\n\
         SELECT run_id, session, symbol, strftime(timestamp, '%H:%M'), presence, trade_count_difference, \
         volume_difference, dollar_volume_difference, opened_at_difference, open_difference, closed_at_difference, \
         close_difference, high_difference, low_difference FROM bar_seam ORDER BY symbol, timestamp;\n",
        journal_lines.join(" UNION ALL "),
        journal.display(),
        archive_rows.join(" UNION ALL "),
        bars.display(),
        pointed(&records, &archive),
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
            "fixture,2026-10-08,QQQ,14:00,archive_only,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL",
            "fixture,2026-10-08,QQQ,14:01,trader_only,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL",
            "fixture,2026-10-08,SPY,14:00,both,0,0.000000,0.000000000000,00:00:00,0.000000,00:00:00,0.000000,0.000000,0.000000",
            "fixture,2026-10-08,SPY,14:01,both,0,0.000000,0.010000000000,00:00:00,0.000000,00:00:00,0.010000,0.010000,0.000000",
        ]
    );
}
