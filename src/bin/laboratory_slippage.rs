//! Laboratory: what the trader's entries paid against their decision prices.
//!
//! Reads the exported trader journal, so it measures what shipped rather than a box's local files.

use chrono::Utc;
use tracing::{error, info, warn};

use fund::common::aws::Producer;
use fund::common::journal::ReadLine;
use fund::common::log::init_tracing;
use fund::data::export::read_exported_journal;
use fund::laboratory::journal as laboratory;
use fund::laboratory::slippage::{legs, summarize_legs};

const LOG_FILE: &str = "laboratory-slippage.log";

#[tokio::main]
async fn main() {
    fund::common::crypto::install_default_crypto_provider();
    let tracing_guard = init_tracing(LOG_FILE, Some("info"), "laboratory-slippage");

    let code = match run().await {
        Ok(()) => 0,
        Err(error) => {
            error!(%error, "Measuring slippage failed");
            eprintln!("Measuring slippage failed: {error}");
            1
        }
    };

    drop(tracing_guard);
    std::process::exit(code);
}

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let bucket = std::env::var("AWS_S3_RECORDS_BUCKET_NAME")
        .map_err(|_| "AWS_S3_RECORDS_BUCKET_NAME must be set")?;
    let s3_client = fund::common::aws::s3_client().await;

    let records = read_exported_journal(&s3_client, &bucket, Producer::Trader).await?;
    let unreadable = records
        .iter()
        .filter(|record| matches!(record, ReadLine::Unreadable { .. }))
        .count();
    let legs = legs(&records);
    let summary = summarize_legs(&legs);

    // The population first, so an estimate over three legs cannot read like one over three thousand.
    println!(
        "{} journal records read from s3://{bucket} ({unreadable} unreadable), {} legs, {} undefined, {} sessions",
        records.len(),
        summary.legs,
        summary.undefined,
        summary.sessions
    );
    match &summary.per_session {
        Some(cost) => println!(
            "cost {:+.2}bp per leg, {:.2} standard error across {} sessions",
            cost.mean, cost.standard_error, cost.sessions
        ),
        None => println!("cost unmeasurable: fewer than two sessions carry a defined leg"),
    }
    println!("{:<12}{:>10}{:>6}", "session", "bp", "legs");
    for (session, (mean, count)) in &summary.by_session {
        println!("{:<12}{mean:>+10.2}{count:>6}", session.to_string());
    }
    println!("{:<8}{:>10}{:>6}", "ticker", "bp", "legs");
    for (ticker, (mean, count)) in &summary.by_ticker {
        println!("{ticker:<8}{mean:>+10.2}{count:>6}");
    }

    match laboratory::Journal::from_env() {
        Ok(journal) => {
            journal
                .record(
                    uuid::Uuid::new_v4(),
                    Utc::now(),
                    laboratory::Observation::SlippageMeasured(laboratory::SlippageMeasured {
                        legs: summary.legs,
                        undefined: summary.undefined,
                        cost_basis_points: summary.per_session,
                        first_session: legs.iter().map(|leg| leg.session).min(),
                        last_session: legs.iter().map(|leg| leg.session).max(),
                    }),
                )
                .await
        }
        Err(error) => warn!(%error, "No laboratory journal; this run is not recorded"),
    }
    info!(legs = summary.legs, "Measured slippage");
    Ok(())
}
