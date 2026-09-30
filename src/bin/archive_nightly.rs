//! Heals the market-data archive over the last trading days and journals how each owed session ended. Exits 0 when
//! every owed session was written, 1 when any was not, and 2 when the run could not start.

use std::process::ExitCode;

use chrono::Utc;
use tracing::Instrument;
use tracing_subscriber::EnvFilter;

use fund::archive::Archive;
use fund::common::heal::is_complete;
use fund::common::journal::Observation;
use fund::common::time::SessionDate;
use fund::heal::{Clients, Parameters, lock, run};
use fund::ingest::alpaca::Alpaca;
use fund::ingest::massive::Massive;
use fund::journal::Journal;

const REFUSED_TO_START: u8 = 2;

#[tokio::main]
async fn main() -> ExitCode {
    tracing_subscriber::fmt()
        .json()
        .with_current_span(true)
        .with_span_list(false)
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();
    let (parameters, configuration) = match Parameters::from_environment() {
        Ok(resolved) => resolved,
        Err(refusal) => {
            tracing::error!(%refusal, "Parameters refused");
            return ExitCode::from(REFUSED_TO_START);
        }
    };
    let mut journal = match Journal::open(parameters.journal_directory()) {
        Ok(journal) => journal,
        Err(error) => {
            tracing::error!(%error, "Journal did not open");
            return ExitCode::from(REFUSED_TO_START);
        }
    };
    let span = tracing::info_span!(
        "run",
        run_id = %journal.run_id(),
        commit = journal.commit().map_or("unknown", |commit| commit.as_str()),
    );
    async move {
        // Held until the process exits.
        let _lock = match lock(parameters.journal_directory()) {
            Ok(file) => file,
            Err(refusal) => {
                tracing::error!(%refusal, "Another run is under way or the lock is unavailable");
                return ExitCode::from(REFUSED_TO_START);
            }
        };
        if let Err(error) = journal.append(Utc::now(), Observation::ConfigurationResolved(configuration)) {
            tracing::error!(%error, "Configuration was not journaled");
            return ExitCode::from(REFUSED_TO_START);
        }
        let http_client = reqwest::Client::new();
        let sdk_configuration = aws_config::load_from_env().await;
        let clients = match (
            Archive::from_environment(&sdk_configuration),
            Massive::from_environment(http_client.clone()),
            Alpaca::from_environment(http_client),
        ) {
            (Ok(archive), Ok(massive), Ok(alpaca)) => Clients::new(archive, massive, alpaca),
            (Err(refusal), _, _) | (_, Err(refusal), _) | (_, _, Err(refusal)) => {
                tracing::error!(%refusal, "Client configuration refused");
                return ExitCode::from(REFUSED_TO_START);
            }
        };
        tracing::info!("Starting the archive heal");
        let finished = match run(&parameters, &clients, &mut journal, SessionDate::at(Utc::now())).await {
            Ok(finished) => finished,
            Err(error) => {
                tracing::error!(%error, "Heal stopped");
                return ExitCode::FAILURE;
            }
        };
        let complete = is_complete(finished.outcomes());
        tracing::info!(window = ?finished.window(), outcomes = ?finished.outcomes(), complete, "Heal finished");
        if let Err(error) = journal.append(Utc::now(), Observation::HealFinished(finished)) {
            tracing::error!(%error, "Heal outcome was not journaled");
            return ExitCode::FAILURE;
        }
        if complete { ExitCode::SUCCESS } else { ExitCode::FAILURE }
    }
    .instrument(span)
    .await
}
