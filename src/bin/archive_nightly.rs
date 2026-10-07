//! Heals the market-data archive over the last trading days, journals how each owed session ended, and ships the
//! journal and logs to the records bucket. Exits 0 when every owed session was written and every file shipped, 1 when
//! anything was not, and 2 when the run could not start.

use std::fs::File;
use std::process::ExitCode;
use std::sync::Mutex;

use chrono::Utc;
use tracing::Instrument;
use tracing_subscriber::Layer;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, fmt};

use fund::archive::Archive;
use fund::common::heal::is_complete;
use fund::common::journal::{Commit, Observation, RunId};
use fund::common::storage::{Host, Service};
use fund::common::time::SessionDate;
use fund::heal::{Clients, Parameters, run};
use fund::ingest::alpaca::Alpaca;
use fund::ingest::massive::Massive;
use fund::journal::{Journal, built_commit, lock};
use fund::parameter::log_directory_from_environment;
use fund::records::{log_file_name, ship, shipped_filter};
use uuid::Uuid;

const SERVICE: &str = "archive_nightly";
const REFUSED_TO_START: u8 = 2;

#[tokio::main]
async fn main() -> ExitCode {
    let today = SessionDate::at(Utc::now());
    let service = Service::new(SERVICE).expect("the service name is one path segment");
    let resolved = Parameters::from_environment();
    let log_file = log_directory_from_environment().map(|directory| {
        std::fs::create_dir_all(&directory).and_then(|()| {
            File::options()
                .create(true)
                .append(true)
                .open(directory.join(log_file_name(&service, today)))
        })
    });
    // A refused log directory is reported with the other parameters; only a directory that resolved can fail to open.
    let (log_writer, log_file_error) = match log_file {
        Ok(Ok(file)) => (Some(Mutex::new(file)), None),
        Ok(Err(error)) => (None, Some(error)),
        Err(_) => (None, None),
    };
    tracing_subscriber::registry()
        .with(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .with(
            fmt::layer()
                .json()
                .with_current_span(true)
                .with_span_list(false),
        )
        .with(log_writer.map(|writer| {
            fmt::layer()
                .json()
                .with_current_span(true)
                .with_span_list(false)
                .with_writer(writer)
                .with_filter(shipped_filter())
        }))
        .init();
    let run_id = RunId::new(Uuid::new_v4());
    let commit = built_commit();
    let span = tracing::info_span!(
        "run",
        %run_id,
        commit = commit.as_ref().map_or("unknown", Commit::as_str),
    );
    async move {
        let (parameters, configuration) = match resolved {
            Ok(resolved) => resolved,
            Err(refusal) => {
                tracing::error!(%refusal, "Parameters refused");
                return ExitCode::from(REFUSED_TO_START);
            }
        };
        if let Some(error) = log_file_error {
            tracing::error!(%error, "Log file did not open");
            return ExitCode::from(REFUSED_TO_START);
        }
        let mut journal = match Journal::open(parameters.journal_directory(), run_id) {
            Ok(journal) => journal,
            Err(error) => {
                tracing::error!(%error, "Journal did not open");
                return ExitCode::from(REFUSED_TO_START);
            }
        };
        // Held until the process exits.
        let _lock = match lock(parameters.journal_directory(), &service) {
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
        let (clients, records) = match (
            Archive::market_data(&sdk_configuration),
            Archive::records(&sdk_configuration),
            Massive::from_environment(http_client.clone()),
            Alpaca::from_environment(http_client),
        ) {
            (Ok(archive), Ok(records), Ok(massive), Ok(alpaca)) => {
                (Clients::new(archive, massive, alpaca), records)
            }
            (Err(refusal), _, _, _)
            | (_, Err(refusal), _, _)
            | (_, _, Err(refusal), _)
            | (_, _, _, Err(refusal)) => {
                tracing::error!(%refusal, "Client configuration refused");
                return ExitCode::from(REFUSED_TO_START);
            }
        };
        tracing::info!("Starting the archive heal");
        let complete = match run(&parameters, &clients, &mut journal, today).await {
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
        // Shipped whatever the heal did, since a failed night's records are the ones most worth reading.
        let shipped = ship(
            &records,
            Host::Archiver,
            &service,
            parameters.journal_directory(),
            parameters.log_directory(),
            // Taken now, so a run that crossed midnight ships the journal file its last records went to.
            SessionDate::at(Utc::now()),
        )
        .await;
        let mut all_shipped = true;
        for (key, outcome) in &shipped {
            match outcome {
                Ok(()) => tracing::info!(path = key.path(), "Records shipped"),
                Err(cause) => {
                    all_shipped = false;
                    tracing::error!(path = key.path(), %cause, "Records not shipped");
                }
            }
        }
        if complete && all_shipped { ExitCode::SUCCESS } else { ExitCode::FAILURE }
    }
    .instrument(span)
    .await
}
