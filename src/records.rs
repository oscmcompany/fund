//! Ships a host's local journal and log files to its records bucket. Recent sessions ship again on every run, so a
//! file that grew since its last shipment replaces its object whole.

use std::io::ErrorKind;
use std::path::{Path, PathBuf};

use tracing::Level;
use tracing_subscriber::filter::{LevelFilter, Targets};

use crate::archive::{Archive, ArchiveError, DecodeRefusal, EncodeRefusal, journal, logs};
use crate::common::journal::read;
use crate::common::storage::{Host, JournalKey, Key, LogsKey, Service};
use crate::common::time::SessionDate;

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
#[derive(Debug)]
pub enum ShipFailure {
    Read {
        path: PathBuf,
        error: std::io::Error,
    },
    Encode(EncodeRefusal),
    Decode(DecodeRefusal),
    Archive(ArchiveError),
    /// Another writer changed the object on each of this many attempts.
    Contended {
        attempts: u32,
    },
}

impl std::fmt::Display for ShipFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Read { path, error } => {
                write!(formatter, "reading {} failed: {error}", path.display())
            }
            Self::Encode(refusal) => write!(formatter, "{refusal}"),
            Self::Decode(refusal) => write!(formatter, "{refusal}"),
            Self::Archive(error) => write!(formatter, "{error}"),
            Self::Contended { attempts } => {
                write!(
                    formatter,
                    "another writer changed the object on each of {attempts} attempts"
                )
            }
        }
    }
}

impl std::error::Error for ShipFailure {}

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
                journal::encode(&journal_key, &read(&text))
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
pub async fn ship(
    archive: &Archive,
    host: Host,
    service: &Service,
    journal_directory: &Path,
    log_directory: &Path,
    today: SessionDate,
) -> Vec<(Key, Result<(), ShipFailure>)> {
    let mut shipped = Vec::new();
    for (key, encoded) in shipments(host, service, journal_directory, log_directory, today) {
        let outcome = match encoded {
            Ok(body) => archive.put(&key, body).await.map_err(ShipFailure::Archive),
            Err(failure) => Err(failure),
        };
        shipped.push((key, outcome));
    }
    shipped
}

#[cfg(test)]
mod tests {
    use chrono::NaiveDate;

    use super::*;

    fn date(text: &str) -> SessionDate {
        SessionDate::from_date(text.parse::<NaiveDate>().unwrap())
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
