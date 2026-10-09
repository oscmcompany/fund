//! Heals the market-data archive over the last trading days, journals how each owed session ended, and ships the
//! journal and logs to the records bucket. Exits 0 when every owed session was written and every file shipped, 1 when
//! anything was not, and 2 when the run could not start.

use std::process::ExitCode;

use chrono::Utc;
use tracing::Instrument;

use fund::archive::Archive;
use fund::common::heal::is_complete;
use fund::common::journal::Observation;
use fund::common::storage::{Host, Service};
use fund::common::time::SessionDate;
use fund::heal;
use fund::ingest::alpaca::Alpaca;
use fund::ingest::massive::Massive;
use fund::records::{Exclusion, RefusedToStart, resolved, ship_logged, start};

const SERVICE: &str = "archive_nightly";

#[tokio::main]
async fn main() -> ExitCode {
    let today = SessionDate::at(Utc::now());
    let service = Service::new(SERVICE).expect("the service name is one path segment");
    let parameters = heal::Parameters::from_environment();
    let started = start(service.clone(), today);
    let span = started.span();
    async move {
        let (parameters, configuration) = match resolved(parameters) {
            Ok(resolved) => resolved,
            Err(refused) => return refused.into(),
        };
        // The lock is held until the process exits.
        let (mut journal, _lock) = match started.open(
            parameters.journal_directory(),
            configuration,
            Exclusion::Exclusive,
        ) {
            Ok(opened) => opened,
            Err(refused) => return refused.into(),
        };
        let http_client = reqwest::Client::new();
        let sdk_configuration = aws_config::load_from_env().await;
        let (clients, records) = match (
            Archive::market_data(&sdk_configuration),
            Archive::records(&sdk_configuration),
            Massive::from_environment(http_client.clone()),
            Alpaca::from_environment(http_client),
        ) {
            (Ok(archive), Ok(records), Ok(massive), Ok(alpaca)) => {
                (heal::Clients::new(archive, massive, alpaca), records)
            }
            (Err(refusal), _, _, _)
            | (_, Err(refusal), _, _)
            | (_, _, Err(refusal), _)
            | (_, _, _, Err(refusal)) => {
                tracing::error!(%refusal, "Client configuration refused");
                return RefusedToStart.into();
            }
        };
        tracing::info!("Starting the archive heal");
        let complete = match heal::run(&parameters, &clients, &mut journal, today).await {
            Ok(finished) => {
                let complete = is_complete(finished.outcomes());
                tracing::info!(window = ?finished.window(), outcomes = ?finished.outcomes(), complete, "Heal finished");
                match journal.append(Utc::now(), Observation::HealFinished(finished)) {
                    Ok(()) => complete,
                    Err(error) => {
                        tracing::error!(%error, "Heal outcome was not journaled");
                        false
                    }
                }
            }
            Err(error) => {
                tracing::error!(%error, "Heal stopped");
                false
            }
        };
        let all_shipped = ship_logged(
            &records,
            Host::Archiver,
            &service,
            parameters.journal_directory(),
            parameters.log_directory(),
        )
        .await;
        if complete && all_shipped { ExitCode::SUCCESS } else { ExitCode::FAILURE }
    }
    .instrument(span)
    .await
}
