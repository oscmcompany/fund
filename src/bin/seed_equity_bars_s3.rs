//! Seeds the S3 bar archive over a date range, or repairs whatever of it is missing.
//!
//! Bootstrap and repair tooling for the prefix the trainer trains from. The trainer repairs its own
//! window every night, but only the window it reads — so this exists for the two cases that window
//! cannot cover: a bucket with nothing in it, and an outage older than the training lookback.
//!
//! Usage: `seed_equity_bars_s3 [start YYYY-MM-DD] [end YYYY-MM-DD]`
//! With no arguments the window is the last [`DEFAULT_ARCHIVE_LOOKBACK_DAYS`] days ending today
//! (US/Eastern), which is the answer to "just make the archive right".
//!
//! Needs Massive and AWS credentials and no database, so it runs from either VM or a laptop. The
//! work itself is [`fund::data::archive::archive_missing_sessions`] — the same call the trainer
//! makes, because a second implementation of the gap scan would drift from this one and the
//! symptom would be a model quietly trained across a hole.

use chrono::{NaiveDate, Utc};
use tracing::{error, info};

use fund::common::massive::MassiveClient;
use fund::common::observability::init_tracing;
use fund::data::archive;
use fund::data::calendar::SessionDate;

const USAGE: &str = "Usage: seed_equity_bars_s3 [start YYYY-MM-DD] [end YYYY-MM-DD]";

/// Calendar days the archive covers when no start date is given.
///
/// Two years rather than the trainer's one: this is the floor the archive is built to, and the
/// training window is what gets read out of it. Widening `FUND_LOOKBACK_DAYS` later should not also
/// require a backfill, so the seed deliberately reaches further back than any run needs today.
const DEFAULT_ARCHIVE_LOOKBACK_DAYS: i64 = 730;

/// Inclusive date range, validated on construction.
#[derive(Debug, PartialEq, Eq)]
struct ArchiveRange {
    start: SessionDate,
    end: SessionDate,
}

impl ArchiveRange {
    /// Rejects an inverted range, so an `ArchiveRange` in scope is proof the window is orderable.
    fn new(start: SessionDate, end: SessionDate) -> Result<Self, String> {
        if start > end {
            return Err(format!(
                "Invalid range: start date {start} must be on or before end date {end}"
            ));
        }
        Ok(Self { start, end })
    }
}

fn parse_date(value: &str) -> Result<SessionDate, String> {
    NaiveDate::parse_from_str(value, "%Y-%m-%d")
        .map(SessionDate::from_date)
        .map_err(|error| format!("Invalid date '{value}': expected YYYY-MM-DD ({error})"))
}

/// Parses the optional positional date arguments.
///
/// `today` is a parameter rather than read from the clock here, because a function that reads the
/// wall clock cannot be tested across the hours where the Eastern date and the UTC date disagree.
fn parse_arguments(arguments: &[String], today: SessionDate) -> Result<ArchiveRange, String> {
    match arguments {
        [] => ArchiveRange::new(
            today.plus_calendar_days(-DEFAULT_ARCHIVE_LOOKBACK_DAYS),
            today,
        ),
        [start] => ArchiveRange::new(parse_date(start)?, today),
        [start, end] => ArchiveRange::new(parse_date(start)?, parse_date(end)?),
        _ => Err(format!("Too many arguments\n{USAGE}")),
    }
}

#[tokio::main]
async fn main() {
    fund::common::crypto::install_default_crypto_provider();
    let tracing_guard = init_tracing(
        "seed-equity-bars-s3.log",
        Some("info"),
        "seed-equity-bars-s3",
    );

    let arguments: Vec<String> = std::env::args().skip(1).collect();
    let range = match parse_arguments(&arguments, SessionDate::at(Utc::now())) {
        Ok(range) => range,
        Err(message) => {
            eprintln!("{message}");
            drop(tracing_guard);
            std::process::exit(2);
        }
    };

    let code = match run(&range).await {
        Ok(summary) => {
            info!(
                sessions_requested = summary.sessions_requested,
                sessions_written = summary.sessions_written,
                sessions_without_data = summary.sessions_without_data,
                sessions_failed = summary.sessions_failed.len(),
                bars_written = summary.bars_written,
                "Equity bar archive seeded"
            );
            // A run that stepped over a session leaves exactly the hole this tool exists to close,
            // and the exit code is the operator's only signal that the range needs re-running. A
            // session with no data is not counted: holidays land there and always will.
            if summary.sessions_failed.is_empty() {
                0
            } else {
                1
            }
        }
        Err(error) => {
            error!(%error, "Seeding the equity bar archive failed");
            eprintln!("Seeding the equity bar archive failed: {error}");
            1
        }
    };

    // `std::process::exit` runs no destructors, so the non-blocking appender's guard would never
    // drop and its buffered lines would be lost — exactly when the failure log matters.
    drop(tracing_guard);
    std::process::exit(code);
}

async fn run(range: &ArchiveRange) -> Result<archive::ArchiveSummary, Box<dyn std::error::Error>> {
    let bucket = std::env::var("AWS_S3_BUCKET_NAME")
        .map_err(|_| "AWS_S3_BUCKET_NAME must be set (the equity-bar data bucket)")?;
    let massive = MassiveClient::from_env()?;
    let s3_client = fund::common::aws::s3_client().await;

    info!(
        bucket,
        start = %range.start,
        end = %range.end,
        "Seeding the equity bar archive from Massive"
    );

    Ok(
        archive::archive_missing_sessions(&s3_client, &massive, &bucket, range.start, range.end)
            .await?,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn session(year: i32, month: u32, day: u32) -> SessionDate {
        SessionDate::from_date(
            NaiveDate::from_ymd_opt(year, month, day).expect("test date must be valid"),
        )
    }

    #[test]
    fn test_no_arguments_covers_the_default_lookback_ending_today() {
        let today = session(2026, 8, 6);
        let range = parse_arguments(&[], today).unwrap();
        assert_eq!(range.end, today);
        assert_eq!(
            range.start,
            today.plus_calendar_days(-DEFAULT_ARCHIVE_LOOKBACK_DAYS)
        );
    }

    #[test]
    fn test_one_argument_runs_from_that_date_to_today() {
        let today = session(2026, 8, 6);
        let range = parse_arguments(&["2026-01-02".to_string()], today).unwrap();
        assert_eq!(range.start, session(2026, 1, 2));
        assert_eq!(range.end, today);
    }

    #[test]
    fn test_two_arguments_bound_both_ends() {
        let range = parse_arguments(
            &["2026-01-02".to_string(), "2026-02-03".to_string()],
            session(2026, 8, 6),
        )
        .unwrap();
        assert_eq!(range.start, session(2026, 1, 2));
        assert_eq!(range.end, session(2026, 2, 3));
    }

    #[test]
    fn test_an_inverted_range_is_rejected() {
        let error = parse_arguments(
            &["2026-02-03".to_string(), "2026-01-02".to_string()],
            session(2026, 8, 6),
        )
        .unwrap_err();
        assert!(error.contains("must be on or before"), "{error}");
    }

    #[test]
    fn test_a_malformed_date_is_rejected() {
        let error = parse_arguments(&["2026-13-02".to_string()], session(2026, 8, 6)).unwrap_err();
        assert!(error.contains("expected YYYY-MM-DD"), "{error}");
    }

    #[test]
    fn test_too_many_arguments_are_rejected() {
        let arguments = vec![
            "2026-01-02".to_string(),
            "2026-02-03".to_string(),
            "2026-03-04".to_string(),
        ];
        let error = parse_arguments(&arguments, session(2026, 8, 6)).unwrap_err();
        assert!(error.contains("Too many arguments"), "{error}");
    }
}
