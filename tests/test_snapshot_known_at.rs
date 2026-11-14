//! Every reference view of `views.sql` run by a real `duckdb` over local snapshots laid out as the archive is, so each
//! row's `known_at` is read rather than stubbed, and a snapshot without its fetch instant is refused.

use std::path::{Path, PathBuf};
use std::process::Command;

use chrono::{DateTime, NaiveDate, Utc};
use fund::common::journal::RunId;
use fund::common::market::Symbol;
use fund::common::market::borrow::BorrowFlags;
use fund::common::market::industry::{AccessionNumber, FiledIndustryCode};
use fund::common::market::security_details::{CentralIndexKey, IndustryCode};
use fund::common::storage::{Key, KnownAt, Provider, ReferenceKey, ReferenceTable};
use fund::common::time::SessionDate;
use fund::storage::provenance::{Provenance, Subscription};
use fund::storage::reference::{encode_borrow_flags, encode_filed_industry_codes};
use strum::IntoEnumIterator;

const FETCHED_AT: &str = "2026-11-10T22:01:02.123456789+00:00";
/// `FETCHED_AT` rounded up to the microsecond, as DuckDB prints it.
const FETCHED_KNOWN_AT: &str = "2026-11-10 22:01:02.123457+00";
const ACCEPTED_AT: &str = "2026-03-02T21:15:00+00:00";
const ACCEPTED_KNOWN_AT: &str = "2026-03-02 21:15:00+00";

fn root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

fn views() -> String {
    std::fs::read_to_string(root().join("views.sql")).unwrap()
}

/// The statement opening on `opening`, through its first line ending in `;`.
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

/// Each reference view with the table and provider its snapshots series names.
fn reference_views() -> Vec<(String, ReferenceTable, Provider)> {
    let views = views();
    let mut found = Vec::new();
    for (start, _) in views.match_indices("CREATE OR REPLACE VIEW ") {
        let name = views[start + "CREATE OR REPLACE VIEW ".len()..]
            .split_whitespace()
            .next()
            .unwrap()
            .to_string();
        let body = statement(&views[start..], "CREATE OR REPLACE VIEW ");
        let Some(series) = body.split("snapshots('").nth(1) else {
            continue;
        };
        let part = |label: &str| {
            series
                .split(&format!("{label}="))
                .nth(1)
                .and_then(|rest| rest.split('/').next())
                .unwrap()
                .to_string()
        };
        found.push((
            name,
            part("table").parse().unwrap(),
            part("provider").parse().unwrap(),
        ));
    }
    found
}

/// The macros a reference view calls and the view itself, reading `archive` in place of the bucket.
fn pointed(archive: &Path, view: &str) -> String {
    let views = views();
    format!(
        "SET TimeZone = 'UTC';\nSET VARIABLE archive_bucket = '{}';\n{}\n{}\n{}\n",
        archive.display(),
        statement(&views, "CREATE OR REPLACE MACRO microsecond_ceiling("),
        statement(&views, "CREATE OR REPLACE MACRO snapshots("),
        statement(&views, &format!("CREATE OR REPLACE VIEW {view} AS\n")),
    )
    .replace("'s3://' || ", "")
}

/// Runs `script` and returns its CSV output, or its error when it failed.
fn duckdb(script: &str) -> Result<String, String> {
    let output = Command::new("duckdb")
        .args(["-csv", "-noheader", "-c", script])
        .output()
        .expect("duckdb is on the path, as devenv provides it");
    match output.status.success() {
        true => Ok(String::from_utf8_lossy(&output.stdout).into_owned()),
        false => Err(String::from_utf8_lossy(&output.stderr).into_owned()),
    }
}

fn scratch() -> PathBuf {
    let directory = std::env::temp_dir().join(format!("known-at-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&directory).unwrap();
    directory
}

fn as_of() -> SessionDate {
    SessionDate::from_date(NaiveDate::from_ymd_opt(2026, 11, 10).unwrap())
}

/// Where the archive holds `key`'s file under `archive`, its directory made.
fn located(archive: &Path, key: ReferenceKey) -> PathBuf {
    let path = archive.join(Key::from(key).path());
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    path
}

/// Each `known_at` the view reads, sorted.
fn known_at(archive: &Path, view: &str) -> Result<Vec<String>, String> {
    let script = format!(
        "{}SELECT known_at::VARCHAR FROM {view} ORDER BY known_at;",
        pointed(archive, view)
    );
    duckdb(&script).map(|output| output.lines().map(str::to_string).collect())
}

/// Two rows of `table`'s `as_of` snapshot written by DuckDB, with `fetched` as the fetch instant in the metadata when
/// given; a published table's rows carry its column.
fn snapshot(
    archive: &Path,
    table: ReferenceTable,
    provider: Provider,
    as_of: SessionDate,
    fetched: Option<&str>,
) {
    let path = located(archive, ReferenceKey::new(provider, table, as_of));
    let published = match table.known_at() {
        KnownAt::Fetched => String::new(),
        KnownAt::Published { column } => format!(", TIMESTAMPTZ '{ACCEPTED_AT}' AS {column}"),
    };
    let metadata = match fetched {
        Some(fetched) => format!(", KV_METADATA {{'fund.fetched_at': '{fetched}'}}"),
        None => String::new(),
    };
    duckdb(&format!(
        "COPY (SELECT * FROM range(2) AS rows(number), (SELECT 1{published})) TO '{}' (FORMAT parquet{metadata});",
        path.display()
    ))
    .unwrap();
}

#[test]
fn test_every_reference_view_reads_known_at_from_the_fetch_or_the_vendors_publication() {
    let views = reference_views();
    let mut viewed: Vec<String> = views
        .iter()
        .map(|(_, table, _)| table.to_string())
        .collect();
    viewed.sort();
    let unviewed: Vec<String> = ReferenceTable::iter()
        .filter(|table| !viewed.contains(&table.to_string()))
        .map(|table| table.to_string())
        .collect();
    assert_eq!(unviewed, vec!["traded_symbols".to_string()]);
    assert_eq!(viewed.len(), 6, "{viewed:?}");
    for (view, table, provider) in views {
        let archive = scratch();
        snapshot(&archive, table, provider, as_of(), Some(FETCHED_AT));
        let expected = match table.known_at() {
            KnownAt::Fetched => FETCHED_KNOWN_AT,
            KnownAt::Published { .. } => ACCEPTED_KNOWN_AT,
        };
        assert_eq!(
            known_at(&archive, &view),
            Ok(vec![expected.to_string(), expected.to_string()]),
            "{view}"
        );
        std::fs::remove_dir_all(&archive).unwrap();
    }
}

#[test]
fn test_a_snapshot_without_its_fetch_instant_is_refused() {
    let archive = scratch();
    snapshot(
        &archive,
        ReferenceTable::Splits,
        Provider::Massive,
        as_of(),
        None,
    );
    let refusal = known_at(&archive, "splits").unwrap_err();
    assert!(refusal.contains("has no fund.fetched_at"), "{refusal}");
    std::fs::remove_dir_all(&archive).unwrap();
}

#[test]
fn test_a_snapshot_without_its_fetch_instant_is_refused_whatever_the_query_selects() {
    let views = reference_views();
    assert_eq!(views.len(), 6, "{views:?}");
    for (view, table, provider) in views {
        let archive = scratch();
        snapshot(&archive, table, provider, as_of(), None);
        let script = format!("{}SELECT count(*) FROM {view};", pointed(&archive, &view));
        let refusal = duckdb(&script).unwrap_err();
        assert!(
            refusal.contains("has no fund.fetched_at"),
            "{view}: {refusal}"
        );
        std::fs::remove_dir_all(&archive).unwrap();
    }
}

#[test]
fn test_each_snapshot_reads_its_own_fetch_instant() {
    let archive = scratch();
    let earlier = SessionDate::from_date(NaiveDate::from_ymd_opt(2026, 11, 9).unwrap());
    for (as_of, fetched) in [
        (earlier, "2026-11-09T21:30:00+00:00"),
        (as_of(), FETCHED_AT),
    ] {
        snapshot(
            &archive,
            ReferenceTable::Splits,
            Provider::Massive,
            as_of,
            Some(fetched),
        );
    }
    let script = format!(
        "{}SELECT as_of::VARCHAR || ' ' || known_at::VARCHAR FROM splits ORDER BY as_of, known_at;",
        pointed(&archive, "splits")
    );
    let read: Vec<String> = duckdb(&script)
        .unwrap()
        .lines()
        .map(str::to_string)
        .collect();
    assert_eq!(
        read,
        vec![
            "2026-11-09 2026-11-09 21:30:00+00".to_string(),
            "2026-11-09 2026-11-09 21:30:00+00".to_string(),
            format!("2026-11-10 {FETCHED_KNOWN_AT}"),
            format!("2026-11-10 {FETCHED_KNOWN_AT}"),
        ]
    );
    std::fs::remove_dir_all(&archive).unwrap();
}

fn provenance(subscription: Subscription) -> Provenance {
    Provenance::new(
        subscription,
        FETCHED_AT.parse::<DateTime<Utc>>().unwrap(),
        RunId::new(uuid::Uuid::nil()),
        None,
    )
}

#[test]
fn test_the_written_fetch_instant_is_the_known_at_duckdb_reads() {
    let archive = scratch();
    let key = ReferenceKey::new(Provider::Alpaca, ReferenceTable::BorrowFlags, as_of());
    let flags = [
        BorrowFlags::new(Symbol::new("AAPL").unwrap(), true, true),
        BorrowFlags::new(Symbol::new("ZZZ").unwrap(), false, false),
    ];
    let bytes =
        encode_borrow_flags(&key, &flags, &provenance(Subscription::BrokerageAccount)).unwrap();
    std::fs::write(located(&archive, key), bytes).unwrap();
    assert_eq!(
        known_at(&archive, "borrow_flags"),
        Ok(vec![
            FETCHED_KNOWN_AT.to_string(),
            FETCHED_KNOWN_AT.to_string()
        ])
    );
    std::fs::remove_dir_all(&archive).unwrap();
}

#[test]
fn test_a_written_acceptance_is_the_known_at_duckdb_reads() {
    let archive = scratch();
    let key = ReferenceKey::new(Provider::Sec, ReferenceTable::FiledIndustryCodes, as_of());
    let filed = |filer: u64, accepted_at: &str| {
        FiledIndustryCode::new(
            CentralIndexKey::new(filer).unwrap(),
            AccessionNumber::new("0000320193-26-000001").unwrap(),
            Some(IndustryCode::new("3571").unwrap()),
            accepted_at.parse().unwrap(),
        )
    };
    let rows = [
        filed(320193, ACCEPTED_AT),
        filed(789019, "2026-03-02T21:16:40.250Z"),
    ];
    let bytes =
        encode_filed_industry_codes(&key, &rows, &provenance(Subscription::EdgarPublic)).unwrap();
    std::fs::write(located(&archive, key), bytes).unwrap();
    assert_eq!(
        known_at(&archive, "filed_industry_codes"),
        Ok(vec![
            ACCEPTED_KNOWN_AT.to_string(),
            "2026-03-02 21:16:40.25+00".to_string(),
        ])
    );
    std::fs::remove_dir_all(&archive).unwrap();
}
