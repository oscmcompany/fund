//! Seed equity details from the compile-time-embedded CSV to PostgreSQL
//! and/or S3.
//!
//! The CSV lives at `data/equity_details.csv` in the repository root and is
//! embedded into the binary at compile time. This binary parses it and writes
//! the validated rows to the requested target(s).
//!
//! Usage: `seed_equity_details --target <s3|postgresql|all>`

use fund::common::observability::init_tracing;
use fund::data::database::seed_equity_details;
use fund::data::equity_details::parse_embedded_equity_details;
use fund::data::state::State;

const USAGE: &str = "Usage: seed_equity_details --target <s3|postgresql|all>";

const S3_KEY: &str = "data/equity/details/details.csv";

/// The same CSV content embedded at compile time. Both the parser (which
/// produces validated `EquityDetail` rows for PostgreSQL) and the S3 upload
/// (which stores the raw CSV) read from this single source.
const EQUITY_DETAILS_CSV: &str = include_str!("../../data/equity_details.csv");

#[derive(Debug)]
enum Target {
    S3,
    PostgreSQL,
    All,
}

fn parse_target(value: &str) -> Result<Target, String> {
    match value {
        "s3" => Ok(Target::S3),
        "postgresql" => Ok(Target::PostgreSQL),
        "all" => Ok(Target::All),
        _ => Err(format!(
            "Invalid target '{}': expected 's3', 'postgresql', or 'all'",
            value
        )),
    }
}

fn parse_arguments(arguments: &[String]) -> Result<Target, String> {
    let mut target: Option<Target> = None;
    let mut index = 0;

    while index < arguments.len() {
        match arguments[index].as_str() {
            "--target" => {
                index += 1;
                let value = arguments
                    .get(index)
                    .ok_or_else(|| "--target requires a value".to_string())?;
                target = Some(parse_target(value)?);
            }
            other => {
                return Err(format!("Unknown argument '{}'\n{}", other, USAGE));
            }
        }
        index += 1;
    }

    target.ok_or_else(|| format!("--target is required\n{}", USAGE))
}

async fn upload_to_s3(state: &State) -> Result<(), String> {
    use aws_sdk_s3::primitives::ByteStream;

    state
        .s3_client
        .put_object()
        .bucket(&state.bucket_name)
        .key(S3_KEY)
        .body(ByteStream::from(EQUITY_DETAILS_CSV.as_bytes().to_vec()))
        .send()
        .await
        .map_err(|error| format!("Failed to upload equity details to S3: {}", error))?;

    tracing::info!("Uploaded equity details CSV to S3: {}", S3_KEY);
    Ok(())
}

async fn insert_into_postgresql(state: &State) -> Result<u64, String> {
    let pool = state
        .database
        .pool()
        .ok_or("PostgreSQL not configured but target is postgresql")?;

    let details = parse_embedded_equity_details()
        .map_err(|error| format!("Failed to parse equity details CSV: {}", error))?;

    let rows_affected = seed_equity_details(pool, &details)
        .await
        .map_err(|error| format!("Failed to seed equity details: {}", error))?;

    Ok(rows_affected)
}

#[tokio::main]
async fn main() {
    fund::common::crypto::install_default_crypto_provider();

    let _tracing_guard = init_tracing("seed-equity-details-errors.log", Some("warn"));

    let raw_arguments: Vec<String> = std::env::args().skip(1).collect();
    let target = match parse_arguments(&raw_arguments) {
        Ok(target) => target,
        Err(message) => {
            eprintln!("{}", message);
            std::process::exit(2);
        }
    };

    let state = State::from_env().await;

    let result: Result<(), String> = match target {
        Target::S3 => upload_to_s3(&state).await.map(|()| {
            println!("Equity details uploaded to S3: {}", S3_KEY);
        }),
        Target::PostgreSQL => insert_into_postgresql(&state).await.map(|rows| {
            println!("Equity details seeded to PostgreSQL: {} rows", rows);
        }),
        Target::All => match insert_into_postgresql(&state).await {
            Ok(rows) => match upload_to_s3(&state).await {
                Ok(()) => {
                    println!(
                        "Equity details seeded to PostgreSQL ({} rows) and S3 ({})",
                        rows, S3_KEY
                    );
                    Ok(())
                }
                Err(error) => Err(error),
            },
            Err(error) => Err(error),
        },
    };

    if let Err(error) = result {
        tracing::error!("Seed equity details failed: {}", error);
        eprintln!("Seed equity details failed: {}", error);
        std::process::exit(1);
    }
}

#[cfg(test)]
mod tests {
    use super::{parse_arguments, parse_target};

    #[test]
    fn test_parse_target_s3() {
        assert!(matches!(parse_target("s3").unwrap(), super::Target::S3));
    }

    #[test]
    fn test_parse_target_postgresql() {
        assert!(matches!(
            parse_target("postgresql").unwrap(),
            super::Target::PostgreSQL
        ));
    }

    #[test]
    fn test_parse_target_all() {
        assert!(matches!(parse_target("all").unwrap(), super::Target::All));
    }

    #[test]
    fn test_parse_target_invalid() {
        assert!(parse_target("pg").is_err());
        assert!(parse_target("both").is_err());
    }

    #[test]
    fn test_parse_arguments_valid() {
        let arguments = vec!["--target".to_string(), "s3".to_string()];
        assert!(parse_arguments(&arguments).is_ok());
    }

    #[test]
    fn test_parse_arguments_missing_target() {
        let arguments: Vec<String> = Vec::new();
        let error = parse_arguments(&arguments).unwrap_err();
        assert!(error.contains("--target is required"));
    }

    #[test]
    fn test_parse_arguments_missing_target_value() {
        let arguments = vec!["--target".to_string()];
        let error = parse_arguments(&arguments).unwrap_err();
        assert!(error.contains("--target requires a value"));
    }

    #[test]
    fn test_parse_arguments_unknown_argument() {
        let arguments = vec!["--unknown".to_string(), "value".to_string()];
        let error = parse_arguments(&arguments).unwrap_err();
        assert!(error.contains("Unknown argument"));
    }
}
