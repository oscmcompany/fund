//! Standalone equity-bar backfill command.
//!
//! Fetches grouped daily bars from the Massive API for an inclusive date range
//! and writes them directly to S3 as Hive-partitioned Parquet. Postgres and the
//! HTTP server are intentionally bypassed: historical bars are consumed by model
//! training, which reads from S3, while the `equity_bars` table is only a 90-day
//! rolling buffer.
//!
//! Usage: `backfill <start YYYY-MM-DD> [end YYYY-MM-DD]`
//! The end date defaults to today (US/Eastern) when omitted.

use chrono::{NaiveDate, Utc};
use chrono_tz::US::Eastern;
use fund::common::observability::init_tracing;
use fund::data::equity_bars::{backfill, BackfillSummary};
use fund::data::state::State;

const USAGE: &str = "Usage: backfill <start YYYY-MM-DD> [end YYYY-MM-DD]";

/// A backfill that recorded any per-day failures is incomplete, so it must not
/// look successful: return a non-zero exit code in that case. Days with no
/// market data (holidays) are `Ok` and do not count as failures.
fn exit_code_for(summary: &BackfillSummary) -> i32 {
    if summary.days_failed > 0 {
        1
    } else {
        0
    }
}

fn parse_date(value: &str) -> Result<NaiveDate, String> {
    NaiveDate::parse_from_str(value, "%Y-%m-%d")
        .map_err(|error| format!("Invalid date '{}': expected YYYY-MM-DD ({})", value, error))
}

fn parse_arguments(arguments: &[String]) -> Result<(NaiveDate, NaiveDate), String> {
    if arguments.len() > 2 {
        return Err(USAGE.to_string());
    }

    let start = match arguments.first() {
        Some(value) => parse_date(value)?,
        None => return Err(USAGE.to_string()),
    };

    let end = match arguments.get(1) {
        Some(value) => parse_date(value)?,
        None => Utc::now().with_timezone(&Eastern).date_naive(),
    };

    if start > end {
        let message =
            format!("Invalid range: start date {start} must be on or before end date {end}");
        return Err(message);
    }

    Ok((start, end))
}

#[tokio::main]
async fn main() {
    fund::common::crypto::install_default_crypto_provider();

    let _tracing_guard = init_tracing("backfill-errors.log", Some("warn"));

    let arguments: Vec<String> = std::env::args().skip(1).collect();
    let (start, end) = match parse_arguments(&arguments) {
        Ok(range) => range,
        Err(message) => {
            eprintln!("{}", message);
            std::process::exit(2);
        }
    };

    let state = State::from_env().await;

    match backfill(&state, start, end).await {
        Ok(summary) => {
            // Print a human-readable summary to stdout so the outcome is visible
            // regardless of how structured logging is routed.
            println!(
                "Backfill {}: {} day(s) written, {} weekend day(s) skipped, {} day(s) failed, {} total bars.",
                if summary.days_failed == 0 {
                    "complete"
                } else {
                    "INCOMPLETE"
                },
                summary.days_processed,
                summary.days_skipped_weekend,
                summary.days_failed,
                summary.total_bars,
            );
            tracing::info!(
                days_processed = summary.days_processed,
                days_skipped_weekend = summary.days_skipped_weekend,
                days_failed = summary.days_failed,
                total_bars = summary.total_bars,
                "Backfill finished"
            );
            let exit_code = exit_code_for(&summary);
            if exit_code != 0 {
                let log_directory =
                    std::env::var("FUND_LOG_DIR").unwrap_or_else(|_| "/var/log/fund".to_string());
                eprintln!(
                    "{} day(s) failed to backfill; see {}/backfill-errors.log* for details.",
                    summary.days_failed, log_directory
                );
                std::process::exit(exit_code);
            }
        }
        Err(error) => {
            tracing::error!("Backfill failed: {}", error);
            eprintln!("Backfill failed: {}", error);
            std::process::exit(1);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{exit_code_for, parse_arguments, parse_date};
    use chrono::{NaiveDate, Utc};
    use chrono_tz::US::Eastern;
    use fund::data::equity_bars::BackfillSummary;

    #[test]
    fn test_exit_code_for_reflects_failures() {
        let mut summary = BackfillSummary::default();
        assert_eq!(exit_code_for(&summary), 0);

        // Days with no data (holidays) are not failures.
        summary.days_skipped_weekend = 3;
        summary.days_processed = 5;
        assert_eq!(exit_code_for(&summary), 0);

        summary.days_failed = 1;
        assert_eq!(exit_code_for(&summary), 1);
    }

    #[test]
    fn test_parse_date_valid() {
        assert_eq!(
            parse_date("2026-05-20").unwrap(),
            NaiveDate::from_ymd_opt(2026, 5, 20).unwrap()
        );
    }

    #[test]
    fn test_parse_date_invalid() {
        assert!(parse_date("2026/05/20").is_err());
        assert!(parse_date("not-a-date").is_err());
    }

    #[test]
    fn test_parse_arguments_start_and_end() {
        let arguments = vec!["2026-05-20".to_string(), "2026-05-23".to_string()];
        let (start, end) = parse_arguments(&arguments).unwrap();
        assert_eq!(start, NaiveDate::from_ymd_opt(2026, 5, 20).unwrap());
        assert_eq!(end, NaiveDate::from_ymd_opt(2026, 5, 23).unwrap());
    }

    #[test]
    fn test_parse_arguments_defaults_end_to_today() {
        let arguments = vec!["2020-01-01".to_string()];
        let (start, end) = parse_arguments(&arguments).unwrap();
        assert_eq!(start, NaiveDate::from_ymd_opt(2020, 1, 1).unwrap());
        // The default end date is "today" in US/Eastern, the exchange calendar
        // the backfill operates on.
        assert_eq!(end, Utc::now().with_timezone(&Eastern).date_naive());
    }

    #[test]
    fn test_parse_arguments_missing_start() {
        let arguments: Vec<String> = Vec::new();
        assert!(parse_arguments(&arguments).is_err());
    }

    #[test]
    fn test_parse_arguments_rejects_extra_arguments() {
        let arguments = vec![
            "2026-05-20".to_string(),
            "2026-05-23".to_string(),
            "unexpected".to_string(),
        ];
        assert!(parse_arguments(&arguments).is_err());
    }

    #[test]
    fn test_parse_arguments_rejects_inverted_range() {
        let arguments = vec!["2026-05-23".to_string(), "2026-05-20".to_string()];
        let error = parse_arguments(&arguments).unwrap_err();
        assert!(error.contains("Invalid range"));
    }
}
