//! Writes the ticker metadata that accompanies the S3 bar archive.
//!
//! Usage: `seed_equity_details_s3`
//!
//! There are no arguments and no database. The source is the CSV compiled into the binary, the same
//! one `seed_equity_details_postgres` loads into `equity_details`.
//!
//! Training does **not** read this object — the trainer parses the embedded CSV directly, so a
//! model run cannot be broken by its absence. It exists for readers outside the process: DuckDB's
//! `training_details` view resolves here, and an archive that describes its own tickers is one an
//! analyst can use without the binary that wrote it.

use tracing::{error, info};

use fund::common::observability::init_tracing;
use fund::data::archive;
use fund::data::details;

#[tokio::main]
async fn main() {
    fund::common::crypto::install_default_crypto_provider();
    let tracing_guard = init_tracing(
        "seed-equity-details-s3.log",
        Some("info"),
        "seed-equity-details-s3",
    );

    let code = match run().await {
        Ok(bytes) => {
            info!(bytes, "Equity details archived");
            0
        }
        Err(error) => {
            error!(%error, "Archiving equity details failed");
            eprintln!("Archiving equity details failed: {error}");
            1
        }
    };

    // `std::process::exit` runs no destructors, so the non-blocking appender's guard would never
    // drop and its buffered lines would be lost — exactly when the failure log matters.
    drop(tracing_guard);
    std::process::exit(code);
}

/// Uploads the embedded ticker metadata and returns the byte length written.
///
/// Reads `AWS_S3_BUCKET_NAME` from the environment and writes one object at
/// `data/equity/details/details.csv`, overwriting whatever is there. No database and no Massive
/// credential are involved: the source is compiled into this binary, so the only way this produces
/// a wrong object is if the embedded CSV itself is wrong — which the parse below catches first.
async fn run() -> Result<usize, Box<dyn std::error::Error>> {
    let bucket = std::env::var("AWS_S3_BUCKET_NAME")
        .map_err(|_| "AWS_S3_BUCKET_NAME must be set (the equity-bar data bucket)")?;
    let s3_client = fund::common::aws::s3_client().await;

    // Parsed before it is uploaded, so a malformed embedded CSV fails here rather than becoming an
    // object every downstream reader has to discover is unusable.
    let parsed = details::parse_embedded_details()?;
    let csv = details::embedded_csv();
    info!(bucket, tickers = parsed.len(), "Archiving equity details");

    archive::archive_details(&s3_client, &bucket, csv).await?;
    Ok(csv.len())
}
