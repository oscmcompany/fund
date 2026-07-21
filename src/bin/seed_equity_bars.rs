//! Seed equity bars from a specified source to a specified target.
//!
//! Fetches daily bars for an inclusive date range and writes them to the
//! selected target(s). Weekends are skipped; holidays produce zero bars.
//!
//! Usage: `seed_equity_bars --source <massive|s3> --target <s3|postgresql|all> <start YYYY-MM-DD> [end YYYY-MM-DD]`
//! The end date defaults to today (US/Eastern) when omitted.

use chrono::{NaiveDate, Utc};
use chrono_tz::US::Eastern;
use fund::common::observability::init_tracing;
use fund::data::equity_bars::{seed, SeedSource, SeedSummary, SeedTarget};
use fund::data::state::State;

const USAGE: &str = "Usage: seed_equity_bars --source <massive|s3> --target <s3|postgresql|all> <start YYYY-MM-DD> [end YYYY-MM-DD]";

/// A seed run that recorded any per-day failures is incomplete, so it must not
/// look successful: return a non-zero exit code in that case.
fn exit_code_for(summary: &SeedSummary) -> i32 {
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

fn parse_source(value: &str) -> Result<SeedSource, String> {
    match value {
        "massive" => Ok(SeedSource::Massive),
        "s3" => Ok(SeedSource::S3),
        _ => Err(format!(
            "Invalid source '{}': expected 'massive' or 's3'",
            value
        )),
    }
}

fn parse_target(value: &str) -> Result<SeedTarget, String> {
    match value {
        "s3" => Ok(SeedTarget::S3),
        "postgresql" => Ok(SeedTarget::PostgreSQL),
        "all" => Ok(SeedTarget::All),
        _ => Err(format!(
            "Invalid target '{}': expected 's3', 'postgresql', or 'all'",
            value
        )),
    }
}

#[derive(Debug)]
struct Arguments {
    source: SeedSource,
    target: SeedTarget,
    start: NaiveDate,
    end: NaiveDate,
}

fn parse_arguments(arguments: &[String]) -> Result<Arguments, String> {
    let mut source: Option<SeedSource> = None;
    let mut target: Option<SeedTarget> = None;
    let mut positional: Vec<String> = Vec::new();
    let mut index = 0;

    while index < arguments.len() {
        match arguments[index].as_str() {
            "--source" => {
                index += 1;
                let value = arguments
                    .get(index)
                    .ok_or_else(|| "--source requires a value".to_string())?;
                source = Some(parse_source(value)?);
            }
            "--target" => {
                index += 1;
                let value = arguments
                    .get(index)
                    .ok_or_else(|| "--target requires a value".to_string())?;
                target = Some(parse_target(value)?);
            }
            other => {
                positional.push(other.to_string());
            }
        }
        index += 1;
    }

    let source = source.ok_or_else(|| format!("--source is required\n{}", USAGE))?;
    let target = target.ok_or_else(|| format!("--target is required\n{}", USAGE))?;

    // Validate source/target combinations.
    if matches!(source, SeedSource::S3) && matches!(target, SeedTarget::S3 | SeedTarget::All) {
        return Err(
            "Invalid combination: --source s3 with --target s3 or --target all is not supported"
                .to_string(),
        );
    }

    if positional.is_empty() {
        return Err(format!("Start date is required\n{}", USAGE));
    }
    if positional.len() > 2 {
        return Err(format!("Too many positional arguments\n{}", USAGE));
    }

    let start = parse_date(&positional[0])?;
    let end = match positional.get(1) {
        Some(value) => parse_date(value)?,
        None => Utc::now().with_timezone(&Eastern).date_naive(),
    };

    if start > end {
        let message =
            format!("Invalid range: start date {start} must be on or before end date {end}");
        return Err(message);
    }

    Ok(Arguments {
        source,
        target,
        start,
        end,
    })
}

#[tokio::main]
async fn main() {
    fund::common::crypto::install_default_crypto_provider();

    let _tracing_guard = init_tracing(
        "seed-equity-bars-errors.log",
        Some("warn"),
        "seed-equity-bars",
    );

    let raw_arguments: Vec<String> = std::env::args().skip(1).collect();
    let arguments = match parse_arguments(&raw_arguments) {
        Ok(arguments) => arguments,
        Err(message) => {
            eprintln!("{}", message);
            std::process::exit(2);
        }
    };

    let state = State::from_env().await;

    match seed(
        &state,
        arguments.start,
        arguments.end,
        arguments.source,
        arguments.target,
    )
    .await
    {
        Ok(summary) => {
            println!(
                "Seed {}: {} day(s) written, {} non-trading day(s) skipped, {} day(s) failed, {} total bars.",
                if summary.days_failed == 0 {
                    "complete"
                } else {
                    "INCOMPLETE"
                },
                summary.days_processed,
                summary.days_skipped_non_trading,
                summary.days_failed,
                summary.total_bars,
            );
            tracing::info!(
                days_processed = summary.days_processed,
                days_skipped_non_trading = summary.days_skipped_non_trading,
                days_failed = summary.days_failed,
                total_bars = summary.total_bars,
                "Seed finished"
            );
            let exit_code = exit_code_for(&summary);
            if exit_code != 0 {
                let log_directory =
                    std::env::var("FUND_LOG_DIR").unwrap_or_else(|_| "/var/log/fund".to_string());
                eprintln!(
                    "{} day(s) failed to seed; see {}/seed-equity-bars-errors.log* for details.",
                    summary.days_failed, log_directory
                );
                std::process::exit(exit_code);
            }
        }
        Err(error) => {
            tracing::error!("Seed failed: {}", error);
            eprintln!("Seed failed: {}", error);
            std::process::exit(1);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{exit_code_for, parse_arguments, parse_date, parse_source, parse_target};
    use chrono::{NaiveDate, Utc};
    use chrono_tz::US::Eastern;
    use fund::data::equity_bars::SeedSummary;

    #[test]
    fn test_exit_code_for_reflects_failures() {
        let mut summary = SeedSummary::default();
        assert_eq!(exit_code_for(&summary), 0);

        summary.days_skipped_non_trading = 3;
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
    fn test_parse_source_massive() {
        assert!(matches!(
            parse_source("massive").unwrap(),
            super::SeedSource::Massive
        ));
    }

    #[test]
    fn test_parse_source_s3() {
        assert!(matches!(parse_source("s3").unwrap(), super::SeedSource::S3));
    }

    #[test]
    fn test_parse_source_invalid() {
        assert!(parse_source("invalid").is_err());
    }

    #[test]
    fn test_parse_target_s3() {
        assert!(matches!(parse_target("s3").unwrap(), super::SeedTarget::S3));
    }

    #[test]
    fn test_parse_target_postgresql() {
        assert!(matches!(
            parse_target("postgresql").unwrap(),
            super::SeedTarget::PostgreSQL
        ));
    }

    #[test]
    fn test_parse_target_all() {
        assert!(matches!(
            parse_target("all").unwrap(),
            super::SeedTarget::All
        ));
    }

    #[test]
    fn test_parse_target_invalid() {
        assert!(parse_target("both").is_err());
    }

    #[test]
    fn test_parse_arguments_full() {
        let arguments = vec![
            "--source".to_string(),
            "massive".to_string(),
            "--target".to_string(),
            "s3".to_string(),
            "2026-05-20".to_string(),
            "2026-05-23".to_string(),
        ];
        let parsed = parse_arguments(&arguments).unwrap();
        assert_eq!(parsed.start, NaiveDate::from_ymd_opt(2026, 5, 20).unwrap());
        assert_eq!(parsed.end, NaiveDate::from_ymd_opt(2026, 5, 23).unwrap());
    }

    #[test]
    fn test_parse_arguments_defaults_end_to_today() {
        let arguments = vec![
            "--source".to_string(),
            "massive".to_string(),
            "--target".to_string(),
            "s3".to_string(),
            "2020-01-01".to_string(),
        ];
        let parsed = parse_arguments(&arguments).unwrap();
        assert_eq!(parsed.start, NaiveDate::from_ymd_opt(2020, 1, 1).unwrap());
        assert_eq!(parsed.end, Utc::now().with_timezone(&Eastern).date_naive());
    }

    #[test]
    fn test_parse_arguments_missing_source() {
        let arguments = vec![
            "--target".to_string(),
            "s3".to_string(),
            "2026-05-20".to_string(),
        ];
        let error = parse_arguments(&arguments).unwrap_err();
        assert!(error.contains("--source is required"));
    }

    #[test]
    fn test_parse_arguments_missing_target() {
        let arguments = vec![
            "--source".to_string(),
            "massive".to_string(),
            "2026-05-20".to_string(),
        ];
        let error = parse_arguments(&arguments).unwrap_err();
        assert!(error.contains("--target is required"));
    }

    #[test]
    fn test_parse_arguments_missing_start_date() {
        let arguments = vec![
            "--source".to_string(),
            "massive".to_string(),
            "--target".to_string(),
            "s3".to_string(),
        ];
        let error = parse_arguments(&arguments).unwrap_err();
        assert!(error.contains("Start date is required"));
    }

    #[test]
    fn test_parse_arguments_rejects_extra_arguments() {
        let arguments = vec![
            "--source".to_string(),
            "massive".to_string(),
            "--target".to_string(),
            "s3".to_string(),
            "2026-05-20".to_string(),
            "2026-05-23".to_string(),
            "unexpected".to_string(),
        ];
        assert!(parse_arguments(&arguments).is_err());
    }

    #[test]
    fn test_parse_arguments_rejects_inverted_range() {
        let arguments = vec![
            "--source".to_string(),
            "massive".to_string(),
            "--target".to_string(),
            "s3".to_string(),
            "2026-05-23".to_string(),
            "2026-05-20".to_string(),
        ];
        let error = parse_arguments(&arguments).unwrap_err();
        assert!(error.contains("Invalid range"));
    }

    #[test]
    fn test_parse_arguments_rejects_s3_to_s3() {
        let arguments = vec![
            "--source".to_string(),
            "s3".to_string(),
            "--target".to_string(),
            "s3".to_string(),
            "2026-05-20".to_string(),
        ];
        let error = parse_arguments(&arguments).unwrap_err();
        assert!(error.contains("Invalid combination"));
    }

    #[test]
    fn test_parse_arguments_rejects_s3_to_all() {
        let arguments = vec![
            "--source".to_string(),
            "s3".to_string(),
            "--target".to_string(),
            "all".to_string(),
            "2026-05-20".to_string(),
        ];
        let error = parse_arguments(&arguments).unwrap_err();
        assert!(error.contains("Invalid combination"));
    }

    #[test]
    fn test_parse_arguments_allows_s3_to_postgresql() {
        let arguments = vec![
            "--source".to_string(),
            "s3".to_string(),
            "--target".to_string(),
            "postgresql".to_string(),
            "2026-05-20".to_string(),
        ];
        assert!(parse_arguments(&arguments).is_ok());
    }

    #[test]
    fn test_parse_arguments_flags_after_positional() {
        let arguments = vec![
            "2026-05-20".to_string(),
            "--source".to_string(),
            "massive".to_string(),
            "--target".to_string(),
            "s3".to_string(),
        ];
        let parsed = parse_arguments(&arguments).unwrap();
        assert_eq!(parsed.start, NaiveDate::from_ymd_opt(2026, 5, 20).unwrap());
    }
}
