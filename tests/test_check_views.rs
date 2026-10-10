//! What `check-views` reports, run whole against a scripted `duckdb` in place of S3: the script is the contract, so
//! only the binary it shells out to is replaced. Also that every series `views.sql` reads is one `Key` writes.

use std::path::{Path, PathBuf};
use std::process::Command;

use chrono::NaiveDate;
use fund::common::heal::Leg;
use fund::common::market::record::BarInterval;
use fund::common::storage::{
    BarsKey, Host, JournalKey, Key, LogsKey, Origin, Provider, QuotesKey, ReferenceKey,
    ReferenceTable, Service, TradesKey,
};
use fund::common::time::SessionDate;
use strum::IntoEnumIterator;

/// How the fake `duckdb` answers one view.
#[derive(Clone, Copy)]
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

/// The archive's market data views in `views.sql` order, each answering as the daily or the minute bars do.
fn bars(daily: Answer, minute: Answer) -> Vec<(&'static str, Answer)> {
    vec![
        ("alpaca_minute_bars", minute),
        ("alpaca_five_minute_bars", minute),
        ("alpaca_minute_quote_bars", minute),
        ("alpaca_five_minute_quote_bars", minute),
        ("alpaca_daily_quote_bars", daily),
        ("alpaca_minute_trade_bars", minute),
        ("alpaca_five_minute_trade_bars", minute),
        ("alpaca_daily_trade_bars", daily),
        ("massive_minute_bars", minute),
        ("massive_five_minute_bars", minute),
        ("massive_daily_bars", daily),
        ("massive_minute_quote_bars", minute),
        ("massive_five_minute_quote_bars", minute),
        ("massive_daily_quote_bars", daily),
        ("massive_minute_trade_bars", minute),
        ("massive_five_minute_trade_bars", minute),
        ("massive_daily_trade_bars", daily),
        ("trade_conditions", daily),
        ("security_details", daily),
        ("splits", daily),
        ("series_boundaries", daily),
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
        ("bar_seam", Answer::Rows(0)),
    ]);
    let (status, output) = real(&answers, "");
    assert_eq!(
        reported(&output),
        [
            "alpaca_minute_bars",
            "alpaca_five_minute_bars",
            "alpaca_minute_quote_bars",
            "alpaca_five_minute_quote_bars",
            "alpaca_daily_quote_bars",
            "alpaca_minute_trade_bars",
            "alpaca_five_minute_trade_bars",
            "alpaca_daily_trade_bars",
            "massive_minute_bars",
            "massive_five_minute_bars",
            "massive_daily_bars",
            "massive_minute_quote_bars",
            "massive_five_minute_quote_bars",
            "massive_daily_quote_bars",
            "massive_minute_trade_bars",
            "massive_five_minute_trade_bars",
            "massive_daily_trade_bars",
            "trade_conditions",
            "security_details",
            "splits",
            "series_boundaries",
            "journal",
            "logs",
            "experiments",
            "bar_seam"
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

/// A developer's paper trader may or may not have shipped, so its logs and the seam pass either way.
#[test]
fn test_trader_records_are_optional_in_every_development_profile() {
    for profile in ["development", "development/john.forstmeier"] {
        let (status, output) = real(&bars(Answer::Rows(5), Answer::Rows(7)), profile);
        assert_eq!(status, 0, "{profile}: {output}");
        assert!(
            output.contains("logs: optional, nothing written\n"),
            "{profile}: {output}"
        );
        assert!(
            output.contains("bar_seam: optional, nothing written\n"),
            "{profile}: {output}"
        );
        let mut answers = bars(Answer::Rows(5), Answer::Rows(7));
        answers.extend([("logs", Answer::Rows(4)), ("bar_seam", Answer::Rows(9))]);
        let (status, output) = real(&answers, profile);
        assert_eq!(status, 0, "{profile}: {output}");
        assert!(
            output.contains("logs: optional, 4 rows\n"),
            "{profile}: {output}"
        );
        assert!(
            output.contains("bar_seam: optional, 9 rows\n"),
            "{profile}: {output}"
        );
    }
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
        ("bar_seam", Answer::Error(OUT_OF_REACH)),
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

/// `check-views` ends a view at its first line ending in `;`, so that line must be the last before a blank line.
#[test]
fn test_every_view_ends_on_its_first_line_ending_in_a_semicolon() {
    let views = std::fs::read_to_string(root().join("views.sql")).unwrap();
    let lines: Vec<&str> = views.lines().collect();
    let mut ended = Vec::new();
    for (start, line) in lines.iter().enumerate() {
        let Some(view) = line
            .strip_prefix("CREATE OR REPLACE VIEW ")
            .and_then(|rest| rest.strip_suffix(" AS"))
        else {
            continue;
        };
        let end = start
            + lines[start..]
                .iter()
                .position(|line| line.trim_end().ends_with(';'))
                .unwrap_or_else(|| panic!("{view} has no line ending in ;"));
        assert!(
            lines[start..end].iter().all(|line| !line.trim().is_empty()),
            "{view} runs past a blank line before its ;"
        );
        assert!(
            lines.get(end + 1).is_none_or(|line| line.is_empty()),
            "{view} ends on line {} with more of it to come",
            end + 1
        );
        ended.push(view);
    }
    assert_eq!(ended.len(), 25, "{ended:?}");
}

/// Each `(bucket, series)` passed to the `sessions` and `snapshots` macros in `views.sql`, in file order.
fn macro_series() -> Vec<(String, String)> {
    let views = std::fs::read_to_string(root().join("views.sql")).unwrap();
    let mut calls = Vec::new();
    for (name, bucket) in [
        ("sessions('", None),
        ("snapshots('", Some("market_data_bucket")),
    ] {
        for (start, _) in views.match_indices(name) {
            let arguments = &views[start + name.len() - 1..];
            let arguments = &arguments[..arguments.find(')').unwrap()];
            let quoted: Vec<&str> = arguments.split('\'').skip(1).step_by(2).collect();
            let bucket = bucket.unwrap_or(quoted[0]).to_string();
            calls.push((start, bucket, quoted[quoted.len() - 1].to_string()));
        }
    }
    calls.sort();
    calls
        .into_iter()
        .map(|(_, bucket, series)| (bucket, series))
        .collect()
}

/// Every parsed archive series the views cover, built from its parts.
fn archive_keys() -> Vec<Key> {
    let session = SessionDate::from_date(NaiveDate::from_ymd_opt(2026, 10, 8).unwrap());
    let bars =
        |provider, origin, interval| Key::from(BarsKey::new(provider, origin, interval, session));
    let mut keys = vec![
        bars(Provider::Alpaca, Origin::Vendor, BarInterval::OneMinute),
        bars(Provider::Alpaca, Origin::Derived, BarInterval::FiveMinute),
        bars(Provider::Massive, Origin::Vendor, BarInterval::OneMinute),
        bars(Provider::Massive, Origin::Derived, BarInterval::FiveMinute),
        bars(Provider::Massive, Origin::Vendor, BarInterval::OneDay),
    ];
    for provider in [Provider::Alpaca, Provider::Massive] {
        for interval in BarInterval::iter() {
            keys.push(QuotesKey::new(provider, Origin::Derived, interval, session).into());
            keys.push(TradesKey::new(provider, Origin::Derived, interval, session).into());
        }
    }
    for (provider, table) in [
        (Provider::Massive, ReferenceTable::Conditions),
        (Provider::Massive, ReferenceTable::SecurityDetails),
        (Provider::Massive, ReferenceTable::Splits),
        (Provider::Alpaca, ReferenceTable::SeriesBoundaries),
    ] {
        keys.push(ReferenceKey::new(provider, table, session).into());
    }
    keys
}

/// Whether a views series names a key's series, a `name=*` segment standing for any value of that partition.
fn names(view_series: &str, key_series: &str) -> bool {
    let (view, key): (Vec<_>, Vec<_>) = (
        view_series.split('/').collect(),
        key_series.split('/').collect(),
    );
    view.len() == key.len()
        && view
            .iter()
            .zip(&key)
            .all(|(view, key)| match view.strip_suffix('*') {
                Some(partition) => partition.ends_with('=') && key.starts_with(partition),
                None => view == key,
            })
}

#[test]
fn test_every_series_views_sql_reads_is_a_key_series() {
    let session = SessionDate::from_date(NaiveDate::from_ymd_opt(2026, 10, 8).unwrap());
    let mut keys = archive_keys();
    for host in Host::iter() {
        keys.push(JournalKey::new(host, session).into());
        keys.push(LogsKey::new(host, Service::new("archive_nightly").unwrap(), session).into());
    }
    let called = macro_series();
    // 21 archive views, `journal`, `logs`, `experiments`, and `bar_seam`'s two reads.
    assert_eq!(called.len(), 26, "{called:?}");
    for (bucket, series) in &called {
        let expected_bucket = if series.starts_with("records/") {
            "records_bucket"
        } else {
            "market_data_bucket"
        };
        assert_eq!(bucket, expected_bucket, "{series}");
        assert!(
            keys.iter().any(|key| names(series, key.series().as_str())),
            "no key writes {series}"
        );
    }
}

#[test]
fn test_every_archive_series_the_nightly_writes_has_a_view() {
    let session = SessionDate::from_date(NaiveDate::from_ymd_opt(2026, 10, 8).unwrap());
    let archive = archive_keys();
    let read: Vec<String> = macro_series()
        .into_iter()
        .map(|(_, series)| series)
        .collect();
    for key in &archive {
        assert!(
            read.contains(&key.series().to_string()),
            "no view reads {}",
            key.series()
        );
    }
    // A tick leg's key is its daily file; the leg writes every interval of that series.
    for leg in Leg::iter() {
        let key = leg.key(session);
        let written: Vec<Key> = match key {
            Key::Quotes(daily) => BarInterval::iter()
                .map(|interval| daily.at_interval(interval).into())
                .collect(),
            Key::Trades(daily) => BarInterval::iter()
                .map(|interval| daily.at_interval(interval).into())
                .collect(),
            Key::Bars(..)
            | Key::Reference(..)
            | Key::RawBars { .. }
            | Key::RawQuotes { .. }
            | Key::RawTrades { .. }
            | Key::Journal(..)
            | Key::Logs(..) => vec![key],
        };
        for key in written {
            assert!(archive.contains(&key), "{leg} writes {}", key.series());
        }
    }
}
