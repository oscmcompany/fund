//! Backfills the intraday bar archive from Massive, one cadence at a time.
//!
//! Reads and repairs `data/equity/bars/interval=<cadence>/`. No database is touched.

use chrono::NaiveDate;
use tracing::{error, info};

use fund::common::log::init_tracing;
use fund::common::massive::MassiveClient;
use fund::common::types::{BarInterval, SessionDate};
use fund::data::archive;

const USAGE: &str = "Usage: seed_intraday_bars_s3 START_DATE END_DATE [CADENCE]\n\
                     Dates are Eastern calendar dates, inclusive: YYYY-MM-DD.\n\
                     CADENCE is five_minute (default) or one_minute.";

/// What the archive is filled with unless told otherwise.
///
/// Five minutes, because coverage across the whole universe puts the cliff between one minute and
/// five: 95.8% of names clear 99% of regular-session buckets at five, against 33.2% at one.
const DEFAULT_CADENCE: BarInterval = BarInterval::FiveMinute;

struct Parameters {
    start: SessionDate,
    end: SessionDate,
    interval: BarInterval,
}

impl Parameters {
    fn parse(arguments: &[String]) -> Result<Self, String> {
        let (start, end, cadence) = match arguments {
            [start, end] => (start, end, None),
            [start, end, cadence] => (start, end, Some(cadence)),
            _ => {
                return Err(format!(
                    "Expected two dates and an optional cadence\n{USAGE}"
                ))
            }
        };

        let start = session(start, "START_DATE")?;
        let end = session(end, "END_DATE")?;
        if start > end {
            return Err(format!("START_DATE must not be after END_DATE\n{USAGE}"));
        }

        // Refused rather than redirected: the aggregates route stamps a daily bar sixteen hours
        // from where the grouped route does, so it would not line up with the archive beside it.
        let interval = match cadence.map(String::as_str) {
            None => DEFAULT_CADENCE,
            Some("five_minute") => BarInterval::FiveMinute,
            Some("one_minute") => BarInterval::OneMinute,
            Some(other) => {
                return Err(format!(
                    "CADENCE must be five_minute or one_minute, got {other:?}\n{USAGE}"
                ))
            }
        };

        Ok(Self {
            start,
            end,
            interval,
        })
    }
}

/// Parses an Eastern calendar date, which is what a session is.
fn session(raw: &str, name: &str) -> Result<SessionDate, String> {
    NaiveDate::parse_from_str(raw.trim(), "%Y-%m-%d")
        .map(SessionDate::from_date)
        .map_err(|_| format!("{name} must be YYYY-MM-DD, got {raw:?}\n{USAGE}"))
}

#[tokio::main]
async fn main() {
    fund::common::crypto::install_default_crypto_provider();
    let tracing_guard = init_tracing(
        "seed-intraday-bars-s3.log",
        Some("info"),
        "seed-intraday-bars-s3",
    );

    let arguments: Vec<String> = std::env::args().skip(1).collect();
    let parameters = match Parameters::parse(&arguments) {
        Ok(parameters) => parameters,
        Err(message) => {
            eprintln!("{message}");
            drop(tracing_guard);
            std::process::exit(2);
        }
    };

    let code = match run(&parameters).await {
        Ok(summary) => {
            println!(
                "requested {} sessions, wrote {}, {} without data, {} failed, {} bars, {} symbols missing",
                summary.sessions_requested,
                summary.sessions_written,
                summary.sessions_without_data,
                summary.sessions_failed.len(),
                summary.bars_written,
                summary.symbols_failed
            );
            0
        }
        Err(error) => {
            error!(%error, "Seeding the intraday bar archive failed");
            eprintln!("Seeding the intraday bar archive failed: {error}");
            1
        }
    };

    drop(tracing_guard);
    std::process::exit(code);
}

/// Repairs the intraday archive over the requested window.
///
/// Only the sessions the bucket is missing are fetched, so re-running over a repaired range costs
/// one listing and nothing else.
async fn run(
    parameters: &Parameters,
) -> Result<archive::ArchiveSummary, Box<dyn std::error::Error>> {
    let bucket = std::env::var("AWS_S3_BUCKET_NAME")
        .map_err(|_| "AWS_S3_BUCKET_NAME must be set (the equity-bar data bucket)")?;
    let massive = MassiveClient::from_env()?;
    let s3_client = fund::common::aws::s3_client().await;

    info!(
        bucket,
        start = %parameters.start,
        end = %parameters.end,
        interval = %parameters.interval,
        "Seeding the intraday bar archive from Massive"
    );

    Ok(archive::archive_intraday_sessions(
        &s3_client,
        &massive,
        &bucket,
        parameters.interval,
        parameters.start,
        parameters.end,
    )
    .await?)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn arguments(values: &[&str]) -> Vec<String> {
        values.iter().map(|value| value.to_string()).collect()
    }

    #[test]
    fn test_the_cadence_defaults_to_five_minutes() {
        let parameters = Parameters::parse(&arguments(&["2026-08-01", "2026-08-20"])).unwrap();
        assert_eq!(parameters.interval, BarInterval::FiveMinute);
        assert_eq!(parameters.start, SessionDate::from_date(date(2026, 8, 1)));
        assert_eq!(parameters.end, SessionDate::from_date(date(2026, 8, 20)));

        let parameters =
            Parameters::parse(&arguments(&["2026-08-01", "2026-08-20", "one_minute"])).unwrap();
        assert_eq!(parameters.interval, BarInterval::OneMinute);

        // The documented spelling, passed explicitly. Without this a typo in that arm sends a user
        // who followed the usage text to the "CADENCE must be" error and the default test still passes.
        let parameters =
            Parameters::parse(&arguments(&["2026-08-01", "2026-08-20", "five_minute"])).unwrap();
        assert_eq!(parameters.interval, BarInterval::FiveMinute);
    }

    fn date(year: i32, month: u32, day: u32) -> NaiveDate {
        NaiveDate::from_ymd_opt(year, month, day).expect("a valid test date")
    }

    /// A daily cadence would come from the aggregates route, which stamps a daily bar sixteen hours
    /// from where the grouped route stamps it — so a backfill taken here would not line up with the
    /// archive it landed beside.
    #[test]
    fn test_a_daily_cadence_is_refused() {
        assert!(Parameters::parse(&arguments(&["2026-08-01", "2026-08-20", "one_day"])).is_err());
        assert!(Parameters::parse(&arguments(&["2026-08-01", "2026-08-20", "1Day"])).is_err());
    }

    #[test]
    fn test_an_unusable_window_is_refused() {
        assert!(Parameters::parse(&arguments(&["2026-08-20", "2026-08-01"])).is_err());
        assert!(Parameters::parse(&arguments(&["2026-13-01", "2026-08-20"])).is_err());
        assert!(Parameters::parse(&arguments(&["not-a-date", "2026-08-20"])).is_err());
        assert!(Parameters::parse(&arguments(&["2026-08-01"])).is_err());
        assert!(Parameters::parse(&[]).is_err());
    }

    /// A one-day window is a window, not an error: it is how a single missing session is repaired.
    #[test]
    fn test_a_single_session_window_is_allowed() {
        let parameters = Parameters::parse(&arguments(&["2026-08-20", "2026-08-20"])).unwrap();
        assert_eq!(parameters.start, parameters.end);
    }
}
