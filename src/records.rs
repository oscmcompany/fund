//! Starts a run's log file, journal and lock, and ships a host's local journal and log files to its profile bucket.
//! Recent sessions ship again on every run, so a file that grew since its last shipment replaces its object whole.

use std::fs::File;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::sync::Mutex;

use chrono::Utc;
use tracing::{Level, Span};
use tracing_subscriber::filter::{EnvFilter, LevelFilter, Targets};
use tracing_subscriber::fmt;
use tracing_subscriber::layer::{Layer, SubscriberExt};
use tracing_subscriber::util::SubscriberInitExt;
use uuid::Uuid;

use crate::common::journal::{ConfigurationResolved, Observation, RunId, merge, read};
use crate::common::storage::{Host, JournalKey, Key, LogsKey, Service};
use crate::common::time::SessionDate;
use crate::journal::{Journal, UNKNOWN_COMMIT, built_commit, lock};
use crate::parameter::log_directory_from_environment;
use crate::storage::{self, Bucket, BucketError, logs};

/// The exit code of a run that could not start.
const REFUSED_TO_START: u8 = 2;

/// A run that could not start, its cause already logged.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RefusedToStart;

impl From<RefusedToStart> for ExitCode {
    fn from(_: RefusedToStart) -> Self {
        ExitCode::from(REFUSED_TO_START)
    }
}

/// A run whose subscriber is installed: its service, id and span, and why its log file did not open, if it did not.
pub struct Started {
    service: Service,
    run_id: RunId,
    span: Span,
    log_file_error: Option<std::io::Error>,
}

/// Installs the subscriber, logging to stdout and to `service`'s file for `today` in the resolved log directory, and
/// draws the run's id. A log file that did not open is reported by `Started::open`, inside the run's span.
pub fn start(service: Service, today: SessionDate) -> Started {
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
    let span = tracing::info_span!(
        "run",
        %run_id,
        commit = built_commit().map_or_else(|| UNKNOWN_COMMIT.to_string(), |commit| commit.to_string()),
    );
    Started {
        service,
        run_id,
        span,
        log_file_error,
    }
}

/// The parameters a binary resolved, or, once the refusal is logged, a refused start.
pub fn resolved<Parameters, Refusal: std::fmt::Display>(
    resolved: Result<(Parameters, ConfigurationResolved), Refusal>,
) -> Result<(Parameters, ConfigurationResolved), RefusedToStart> {
    resolved.map_err(|refusal| {
        tracing::error!(%refusal, "Parameters refused");
        RefusedToStart
    })
}

impl Started {
    /// The span every record of the run is logged under.
    pub fn span(&self) -> Span {
        self.span.clone()
    }

    /// Opens the run's journal in `journal_directory`, takes the service's lock, and journals `configuration` first;
    /// the lock is held while the returned file lives.
    pub fn open(
        self,
        journal_directory: &Path,
        configuration: ConfigurationResolved,
    ) -> Result<(Journal, File), RefusedToStart> {
        if let Some(error) = self.log_file_error {
            tracing::error!(%error, "Log file did not open");
            return Err(RefusedToStart);
        }
        let mut journal = Journal::open(journal_directory, self.run_id).map_err(|error| {
            tracing::error!(%error, "Journal did not open");
            RefusedToStart
        })?;
        let held = lock(journal_directory, &self.service).map_err(|refusal| {
            tracing::error!(%refusal, "Another run is under way or the lock is unavailable");
            RefusedToStart
        })?;
        journal
            .append(
                Utc::now(),
                Observation::ConfigurationResolved(configuration),
            )
            .map_err(|error| {
                tracing::error!(%error, "Configuration was not journaled");
                RefusedToStart
            })?;
        Ok((journal, held))
    }
}

/// Calendar days of local files shipped each run, today included, so a week of failed shipments heals by itself.
pub const RESHIPPED_DAYS: i64 = 7;

/// What the shipped log file keeps: everything the process logs, except the SDK's credential chain below a warning,
/// which does not belong in a bucket whatever `RUST_LOG` asks stdout for.
pub fn shipped_filter() -> Targets {
    Targets::new()
        .with_default(LevelFilter::TRACE)
        .with_target("aws_config", Level::WARN)
}

/// The file one service's runs log to in a session, named for the session the run started in.
pub fn log_file_name(service: &Service, session: SessionDate) -> String {
    format!("{}-{session}.log", service.as_str())
}

/// Why one records file did not land in the bucket.
#[derive(Debug, thiserror::Error)]
pub enum ShipFailure {
    #[error("reading {} failed: {error}", .path.display())]
    Read {
        path: PathBuf,
        error: std::io::Error,
    },
    #[error("{0}")]
    Encode(storage::EncodeRefusal),
    #[error("{0}")]
    Decode(storage::DecodeRefusal),
    #[error("{0}")]
    Bucket(BucketError),
    /// Another writer changed the object on each of this many attempts.
    #[error("another writer changed the object on each of {attempts} attempts")]
    Contended { attempts: u32 },
    /// A create found the key taken, and reading it back returned other bytes or none.
    #[error("{path} was taken, and reading it back did not return these bytes")]
    Unmatched { path: String },
    /// A file in a run's notes directory whose name cannot be a note's key.
    #[error(transparent)]
    Unnamed(crate::common::storage::NoteNameRefusal),
    /// An entry under the notes directory that is not a directory or regular file where one belongs, or whose
    /// directory name reads as no digest or run.
    #[error("{} names no note", .path.display())]
    Unkeyed { path: PathBuf },
}

/// Each recent journal and log file that exists, encoded under its key, or why it could not be.
fn shipments(
    host: Host,
    service: &Service,
    journal_directory: &Path,
    log_directory: &Path,
    today: SessionDate,
) -> Vec<(Key, Result<Vec<u8>, ShipFailure>)> {
    let mut shipments = Vec::new();
    for days in 0..RESHIPPED_DAYS {
        let session = today.plus_calendar_days(-days);
        let journal_key = JournalKey::new(host, session);
        let journal_path = journal_directory.join(crate::journal::file_name(session));
        if let Some(contents) = contents(&journal_path) {
            let encoded = contents.and_then(|text| {
                storage::journal::encode(&journal_key, &read(&text))
                    .map_err(|refusal| ShipFailure::Encode(refusal.into()))
            });
            shipments.push((journal_key.into(), encoded));
        }
        let logs_key = LogsKey::new(host, service.clone(), session);
        if let Some(contents) = contents(&log_directory.join(log_file_name(service, session))) {
            let encoded = contents.and_then(|text| {
                logs::encode(&logs::parse(&text))
                    .map_err(|refusal| ShipFailure::Encode(refusal.into()))
            });
            shipments.push((logs_key.into(), encoded));
        }
    }
    shipments
}

/// The file's text, or `None` when there is no file, which is nothing to ship rather than a failure.
pub(crate) fn contents(path: &Path) -> Option<Result<String, ShipFailure>> {
    match std::fs::read_to_string(path) {
        Ok(text) => Some(Ok(text)),
        Err(error) if error.kind() == ErrorKind::NotFound => None,
        Err(error) => Some(Err(ShipFailure::Read {
            path: path.to_path_buf(),
            error,
        })),
    }
}

/// Ships every recent file, returning each key with whether it landed.
async fn ship(
    profile: &Bucket,
    host: Host,
    service: &Service,
    journal_directory: &Path,
    log_directory: &Path,
    today: SessionDate,
) -> Vec<(Key, Result<(), ShipFailure>)> {
    let mut shipped = Vec::new();
    for (key, encoded) in shipments(host, service, journal_directory, log_directory, today) {
        let outcome = match encoded {
            Ok(body) => profile.put(&key, body).await.map_err(ShipFailure::Bucket),
            Err(failure) => Err(failure),
        };
        shipped.push((key, outcome));
    }
    shipped
}

/// Ships every recent file whatever the run did, since a failed run's records are the ones most worth reading, and
/// logs each key's outcome; true when every file landed.
pub async fn ship_logged(
    profile: &Bucket,
    host: Host,
    service: &Service,
    journal_directory: &Path,
    log_directory: &Path,
) -> bool {
    // Taken now, so a run that crossed midnight ships the journal file its last records went to.
    let today = SessionDate::at(Utc::now());
    let shipped = ship(
        profile,
        host,
        service,
        journal_directory,
        log_directory,
        today,
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
    all_shipped
}

/// Attempts at merging into a session's object before giving up to whoever keeps rewriting it.
const MERGE_ATTEMPTS: u32 = 5;

/// Reads `key`'s object, adds the lines of `text` it lacks, and writes it back only over the version read.
pub async fn merge_into(profile: &Bucket, key: JournalKey, text: &str) -> Result<(), ShipFailure> {
    let object = Key::from(key);
    for _ in 0..MERGE_ATTEMPTS {
        let (held, tag) = match profile
            .get_tagged(&object)
            .await
            .map_err(ShipFailure::Bucket)?
        {
            Some((body, tag)) => (
                storage::journal::decode(&key, body).map_err(|refusal| {
                    ShipFailure::Decode(storage::DecodeRefusal::from(refusal))
                })?,
                Some(tag),
            ),
            None => (Vec::new(), None),
        };
        let body = storage::journal::encode(&key, &merge(held, read(text)))
            .map_err(|refusal| ShipFailure::Encode(storage::EncodeRefusal::from(refusal)))?;
        let written = match tag {
            Some(tag) => profile.replace(&object, body, &tag).await,
            None => profile.create(&object, body).await,
        };
        match written {
            Ok(()) => return Ok(()),
            Err(BucketError::Contended { .. }) => continue,
            Err(error) => return Err(ShipFailure::Bucket(error)),
        }
    }
    Err(ShipFailure::Contended {
        attempts: MERGE_ATTEMPTS,
    })
}

#[cfg(test)]
mod tests {
    use chrono::NaiveDate;

    use super::*;

    fn date(text: &str) -> SessionDate {
        SessionDate::from_date(text.parse::<NaiveDate>().unwrap())
    }

    fn started(log_file_error: Option<std::io::Error>) -> Started {
        Started {
            service: Service::new("archive_nightly").unwrap(),
            run_id: RunId::new(Uuid::new_v4()),
            span: Span::none(),
            log_file_error,
        }
    }

    fn event_types(directory: &Path) -> Vec<&'static str> {
        Journal::open(directory, RunId::new(Uuid::new_v4()))
            .unwrap()
            .history()
            .iter()
            .map(|record| record.observation().event_type())
            .collect()
    }

    /// A run refuses while another holds the lock, and so does a run whose log file did not open; neither journals.
    #[test]
    fn test_a_run_journals_its_configuration_only_once_it_may_start() {
        let directory = std::env::temp_dir().join(format!("fund-records-{}", Uuid::new_v4()));
        let configuration = || ConfigurationResolved::new(std::collections::BTreeMap::new());
        let (_journal, _held) = started(None).open(&directory, configuration()).unwrap();
        assert_eq!(event_types(&directory), ["configuration_resolved"]);
        let refused = started(None).open(&directory, configuration());
        assert_eq!(refused.err(), Some(RefusedToStart));
        let unlogged = started(Some(std::io::Error::other("read-only")));
        assert_eq!(
            unlogged.open(&directory, configuration()).err(),
            Some(RefusedToStart)
        );
        assert_eq!(event_types(&directory), ["configuration_resolved"]);
        std::fs::remove_dir_all(&directory).unwrap();
    }

    #[test]
    fn test_the_shipped_log_leaves_out_the_credential_chain() {
        let filter = shipped_filter();
        assert!(!filter.would_enable("aws_config::profile::credentials", &Level::INFO));
        assert!(filter.would_enable("aws_config::profile::credentials", &Level::WARN));
        assert!(filter.would_enable("fund::heal", &Level::INFO));
        assert!(filter.would_enable("archive_nightly", &Level::DEBUG));
    }

    #[test]
    fn test_only_the_last_week_of_files_that_exist_ships() {
        let directory = std::env::temp_dir().join(format!("fund-records-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&directory).unwrap();
        let service = Service::new("archive_nightly").unwrap();
        let today = date("2026-09-30");
        for session in ["2026-09-30", "2026-09-24", "2026-09-23"] {
            std::fs::write(
                directory.join(crate::journal::file_name(date(session))),
                "torn",
            )
            .unwrap();
        }
        std::fs::write(
            directory.join(log_file_name(&service, date("2026-09-29"))),
            "torn",
        )
        .unwrap();
        let shipped: Vec<(String, bool)> =
            shipments(Host::Archiver, &service, &directory, &directory, today)
                .into_iter()
                .map(|(key, encoded)| (key.path(), encoded.is_ok()))
                .collect();
        assert_eq!(
            shipped,
            [
                (
                    "records/journal/producer=archiver/year=2026/month=09/day=30/data.parquet".to_string(),
                    true
                ),
                (
                    "records/logs/producer=archiver/service=archive_nightly/year=2026/month=09/day=29/data.parquet"
                        .to_string(),
                    true
                ),
                (
                    "records/journal/producer=archiver/year=2026/month=09/day=24/data.parquet".to_string(),
                    true
                ),
            ]
        );
        std::fs::remove_dir_all(&directory).unwrap();
    }

    #[test]
    fn test_a_file_that_cannot_be_read_is_a_failed_shipment() {
        let directory = std::env::temp_dir().join(format!("fund-records-{}", uuid::Uuid::new_v4()));
        let service = Service::new("archive_nightly").unwrap();
        let today = date("2026-09-30");
        // A directory where the journal file should be reads as an error, not as no file.
        std::fs::create_dir_all(directory.join(crate::journal::file_name(today))).unwrap();
        let shipped = shipments(Host::Archiver, &service, &directory, &directory, today);
        assert_eq!(shipped.len(), 1);
        assert!(shipped[0].1.is_err());
        std::fs::remove_dir_all(&directory).unwrap();
    }
}
