//! Seeds ticker metadata into an empty database from the embedded CSV.
//!
//! Bootstrap tooling, not a scheduled job. Nothing in the running service writes `equity_details`:
//! Alpaca does not publish sector or industry, so the metadata has one source and it is compiled
//! into the binary. A fresh database therefore has no sector information until this runs, and the
//! pair screen's different-sector rule silently admits nothing without it.
//!
//! Usage: `seed_equity_details`
//!
//! There are no arguments. The previous version took `--target <s3|postgresql|all>`, from a data
//! topology that no longer exists — the trainer reads the same embedded CSV directly rather than a
//! copy of it in a bucket, so there is no second target left to choose between.

use fund::common::database::connect_pool;
use fund::common::observability::init_tracing;
use fund::data::details;
use tracing::{error, info};

#[tokio::main]
async fn main() {
    fund::common::crypto::install_default_crypto_provider();
    let tracing_guard = init_tracing(
        "seed-equity-details.log",
        Some("info"),
        "seed-equity-details",
    );

    let code = match run().await {
        Ok(stored) => {
            info!(rows = stored, "Equity details seeded");
            0
        }
        Err(error) => {
            error!(%error, "Seeding equity details failed");
            eprintln!("Seeding equity details failed: {error}");
            1
        }
    };

    // `std::process::exit` runs no destructors, so the non-blocking appender's guard would never
    // drop and its buffered lines would be lost — exactly when the failure log matters.
    drop(tracing_guard);
    std::process::exit(code);
}

async fn run() -> Result<u64, Box<dyn std::error::Error>> {
    let details = details::parse_embedded_details()?;
    info!(tickers = details.len(), "Parsed embedded ticker metadata");

    let pool = connect_pool().await?;
    Ok(details::store_details(&pool, &details).await?)
}
