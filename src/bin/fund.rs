//! Consolidated fund binary.
//!
//! Runs the data, inference, and portfolio services in a single process,
//! sharing a single `PgPool` and `S3Client`. All work is driven by PostgreSQL
//! LISTEN/NOTIFY events — no HTTP servers are started.

use tracing::{error, info};

#[tokio::main]
async fn main() {
    fund::common::crypto::install_default_crypto_provider();

    let _tracing_guard = fund::common::observability::init_tracing("fund.log", None);

    if let Err(error) = run().await {
        error!("{}", error);
        eprintln!("{}", error);
        std::process::exit(1);
    }
}

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    info!("Starting consolidated fund service");

    // Shared infrastructure: one pool, one S3 client for all modules.
    let database_url = std::env::var("DATABASE_URL")
        .map_err(|_| "DATABASE_URL environment variable must be set")?;
    let pool = sqlx::PgPool::connect(&database_url).await?;
    info!("Connected to PostgreSQL");

    let s3_client = fund::common::aws::s3_client().await;

    // --- data ---
    {
        let _span = tracing::info_span!("data").entered();
        let state = fund::data::state::State::with_pool(pool.clone(), s3_client.clone());
        fund::data::startup::migrate_equity_details(&state).await;
        fund::data::scheduler::spawn_sync_scheduler(state.clone());
        fund::data::equity_quotes::spawn_quote_stream(state.clone());
        info!("Data service started");
    }

    // --- inference ---
    {
        let _span = tracing::info_span!("inference").entered();
        let state = fund::inference::state::AppState::with_pool(pool.clone(), s3_client.clone());
        fund::inference::pipeline::poll_artifact_once(&state).await;
        tokio::spawn(fund::inference::pipeline::start_artifact_polling(
            state.clone(),
        ));
        fund::inference::consumer::spawn_event_consumer(state);
        info!("Inference service started");
    }

    // --- portfolio ---
    {
        let _span = tracing::info_span!("portfolio").entered();
        let state = fund::portfolio::state::AppState::with_pool(pool)?;
        fund::portfolio::consumer::spawn_event_consumer(state);
        info!("Portfolio service started");
    }

    info!("All modules running, waiting for events");
    tokio::signal::ctrl_c().await?;
    info!("Shutting down");
    Ok(())
}
