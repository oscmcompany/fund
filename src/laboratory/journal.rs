//! The laboratory's own append-only record of what it ran.
//!
//! Keyed by run and rolled on the UTC date: an experiment happens at an instant, not on a session.

use std::path::{Path, PathBuf};

use chrono::{DateTime, NaiveDate, Utc};
use serde::Serialize;
use tokio::io::AsyncWriteExt;
use tracing::{debug, error};
use uuid::Uuid;

use crate::laboratory::dataset::DatasetFingerprint;
use crate::laboratory::metrics::Distribution;
use crate::laboratory::predictor::Evaluation;

/// The shape of a laboratory record, versioned independently of the application journal.
pub const SCHEMA_VERSION: u32 = 1;

/// Where the laboratory writes when `FUND_LABORATORY_JOURNAL_DIRECTORY` says nothing.
const DEFAULT_JOURNAL_DIRECTORY: &str = "/var/journal/fund/laboratory";

/// Errors writing the laboratory journal.
#[derive(Debug, thiserror::Error)]
pub enum JournalError {
    #[error("failed to create the journal directory {directory}: {source}")]
    Directory {
        directory: String,
        source: std::io::Error,
    },
    #[error("failed to write the journal: {0}")]
    Io(#[from] std::io::Error),
    #[error("failed to serialize a record: {0}")]
    Serialize(#[from] serde_json::Error),
}

/// One thing the laboratory did.
///
/// Named `<subject>_<past participle>`, the convention the application journal already uses.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(
    tag = "experiment_type",
    content = "payload",
    rename_all = "snake_case"
)]
pub enum Observation {
    DatasetBuilt(DatasetBuilt),
    ForecastScored(ForecastScored),
    FeatureTriaged(FeatureTriaged),
}

impl Observation {
    /// The stable name this observation serializes under, and the partition it exports into.
    pub fn experiment_type(&self) -> &'static str {
        match self {
            Observation::DatasetBuilt(_) => "dataset_built",
            Observation::ForecastScored(_) => "forecast_scored",
            Observation::FeatureTriaged(_) => "feature_triaged",
        }
    }
}

/// How much one feature says about the session it precedes.
///
/// `excess_bits` is the answer and the other two are why it should be believed: the raw estimate is
/// biased upward at this sample size, and `null_bits` is that bias measured on the same rows.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct FeatureTriaged {
    pub feature: String,
    pub sessions: usize,
    pub bits: Option<Distribution>,
    pub null_bits: Option<Distribution>,
    pub excess_bits: Option<Distribution>,
}

/// One frame prepared for an experiment to read.
///
/// The fingerprint is what makes a later result comparable, so it is recorded once here and
/// referenced by `run_id` rather than repeated on every result the run goes on to produce.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct DatasetBuilt {
    pub fingerprint: DatasetFingerprint,
    /// The commit this ran from, so a number can be traced to the code that produced it.
    pub revision: Option<String>,
}

/// What one forecast was worth over one dataset.
///
/// Summarized rather than per session: `sessions` is what the panel held and each distribution
/// counts what it could measure, so a forecast that never ranked reads as absent and not as zero.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ForecastScored {
    pub predictor: String,
    pub sessions: usize,
    pub information_coefficient: Option<Distribution>,
    pub decile_spread: Option<Distribution>,
    pub directional_accuracy: Option<Distribution>,
}

impl From<&Evaluation> for ForecastScored {
    fn from(evaluation: &Evaluation) -> Self {
        Self {
            predictor: evaluation.predictor.clone(),
            sessions: evaluation.sessions.len(),
            information_coefficient: evaluation.information_coefficient,
            decile_spread: evaluation.decile_spread,
            directional_accuracy: evaluation.directional_accuracy,
        }
    }
}

/// One line of the laboratory journal: an observation with the envelope that addresses it.
///
/// `run_id` sits where the application's record carries `session_date`, and threads every record
/// one run emits. An experiment spans hundreds of sessions and belongs to none of them.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Record {
    pub schema_version: u32,
    pub event_id: Uuid,
    pub run_id: Uuid,
    pub timestamp: DateTime<Utc>,
    #[serde(flatten)]
    pub observation: Observation,
}

impl Record {
    /// Stamps an observation with a fresh identity at `timestamp`.
    pub fn new(run_id: Uuid, timestamp: DateTime<Utc>, observation: Observation) -> Self {
        Self {
            schema_version: SCHEMA_VERSION,
            event_id: Uuid::new_v4(),
            run_id,
            timestamp,
            observation,
        }
    }
}

/// The file the writer currently holds open, and the UTC date it belongs to.
struct OpenDate {
    date: NaiveDate,
    file: tokio::fs::File,
}

/// Appends records to the current UTC date's file.
///
/// The date comes from the record rather than from construction, so the file rolls at UTC midnight
/// whatever the process was doing at the time.
pub struct Journal {
    directory: PathBuf,
    open_date: tokio::sync::Mutex<Option<OpenDate>>,
}

impl Journal {
    /// Opens a journal against `directory`, creating it if needed.
    pub fn new(directory: impl Into<PathBuf>) -> Result<Self, JournalError> {
        let directory = directory.into();
        std::fs::create_dir_all(&directory).map_err(|source| JournalError::Directory {
            directory: directory.display().to_string(),
            source,
        })?;
        Ok(Self {
            directory,
            open_date: tokio::sync::Mutex::new(None),
        })
    }

    /// Opens a journal at `FUND_LABORATORY_JOURNAL_DIRECTORY`, or the default beneath it.
    pub fn from_env() -> Result<Self, JournalError> {
        let directory = std::env::var("FUND_LABORATORY_JOURNAL_DIRECTORY")
            .unwrap_or_else(|_| DEFAULT_JOURNAL_DIRECTORY.to_string());
        Self::new(directory)
    }

    pub fn directory(&self) -> &Path {
        &self.directory
    }

    /// Appends one record and returns once it is fsynced to the disk.
    pub async fn append(&self, record: &Record) -> Result<(), JournalError> {
        let mut line = serde_json::to_vec(record)?;
        line.push(b'\n');
        let date = record.timestamp.date_naive();

        let mut open_date = self.open_date.lock().await;
        let open = match open_date.as_mut() {
            Some(open) if open.date == date => open,
            _ => {
                let file = tokio::fs::OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(self.directory.join(file_name(date)))
                    .await?;
                open_date.insert(OpenDate { date, file })
            }
        };

        open.file.write_all(&line).await?;
        open.file.flush().await?;
        open.file.sync_all().await?;
        Ok(())
    }

    /// Appends a record, reporting a failure without propagating it.
    ///
    /// A journal write must not become a new way for an experiment to fail, so running unobserved
    /// beats refusing to run.
    pub async fn record(&self, run_id: Uuid, timestamp: DateTime<Utc>, observation: Observation) {
        let record = Record::new(run_id, timestamp, observation);
        let experiment_type = record.observation.experiment_type();
        match self.append(&record).await {
            Ok(()) => debug!(experiment_type, %run_id, "Laboratory record written"),
            Err(error) => error!(
                experiment_type,
                %run_id,
                %error,
                "Laboratory record could not be written; the run proceeds unobserved"
            ),
        }
    }

    /// Blocks appends for as long as the returned guard is held.
    pub async fn seal(&self) -> JournalGuard<'_> {
        let mut open_date = self.open_date.lock().await;
        *open_date = None;
        JournalGuard {
            _appends_blocked: open_date,
        }
    }
}

/// Proof that no append can run while it is alive.
pub struct JournalGuard<'a> {
    _appends_blocked: tokio::sync::MutexGuard<'a, Option<OpenDate>>,
}

/// The file one UTC date's records are written to.
pub fn file_name(date: NaiveDate) -> String {
    format!("laboratory-{date}.jsonl")
}

/// Recovers the UTC date from a name built by [`file_name`], or `None` if it does not have that
/// shape.
pub fn date_from_file_name(name: &str) -> Option<NaiveDate> {
    name.strip_prefix("laboratory-")
        .and_then(|rest| rest.strip_suffix(".jsonl"))
        .and_then(|date| NaiveDate::parse_from_str(date, "%Y-%m-%d").ok())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::types::SessionDate;
    use crate::laboratory::metrics::SessionMetrics;

    fn fingerprint() -> DatasetFingerprint {
        DatasetFingerprint {
            session: SessionDate::from_date(NaiveDate::from_ymd_opt(2026, 8, 17).unwrap()),
            lookback_days: 365,
            rows: 10,
            tickers: 2,
            first_timestamp: DateTime::from_timestamp_millis(0),
            last_timestamp: DateTime::from_timestamp_millis(86_400_000),
            splits_digest: 0xAB,
            boundaries_digest: 0xCD,
        }
    }

    fn observation() -> Observation {
        Observation::DatasetBuilt(DatasetBuilt {
            fingerprint: fingerprint(),
            revision: Some("abc1234".to_string()),
        })
    }

    /// The export partitions on this name, so no two variants may collide and none may drift from
    /// the tag `rename_all` generates for it.
    #[test]
    fn test_each_observation_exports_under_its_own_partition() {
        let forecast = Observation::ForecastScored(ForecastScored {
            predictor: "persistence".to_string(),
            sessions: 4,
            information_coefficient: None,
            decile_spread: None,
            directional_accuracy: None,
        });
        let value: serde_json::Value = serde_json::to_value(&forecast).unwrap();

        assert_eq!(
            value["experiment_type"],
            serde_json::json!("forecast_scored")
        );
        assert_eq!(forecast.experiment_type(), "forecast_scored");
        assert_ne!(forecast.experiment_type(), observation().experiment_type());

        let triaged = Observation::FeatureTriaged(FeatureTriaged {
            feature: "daily_return".to_string(),
            sessions: 499,
            bits: None,
            null_bits: None,
            excess_bits: None,
        });
        let value: serde_json::Value = serde_json::to_value(&triaged).unwrap();

        assert_eq!(
            value["experiment_type"],
            serde_json::json!("feature_triaged")
        );
        assert_eq!(triaged.experiment_type(), "feature_triaged");
        assert_ne!(triaged.experiment_type(), forecast.experiment_type());
        assert_ne!(triaged.experiment_type(), observation().experiment_type());
    }

    /// Two different counts, and conflating them would read a forecast that ranked twice out of
    /// five hundred sessions as one that ranked throughout.
    #[test]
    fn test_a_scored_forecast_separates_the_panel_from_what_it_could_measure() {
        let measurable = SessionMetrics {
            information_coefficient: Some(0.02),
            ..SessionMetrics::default()
        };
        let evaluation = Evaluation {
            predictor: "persistence".to_string(),
            sessions: vec![
                SessionMetrics::default(),
                SessionMetrics::default(),
                measurable,
                measurable,
            ],
            information_coefficient: Some(Distribution {
                mean: 0.02,
                standard_error: 0.0,
                sessions: 2,
            }),
            decile_spread: None,
            directional_accuracy: None,
        };

        let record = ForecastScored::from(&evaluation);

        assert_eq!(record.sessions, 4, "every session the panel held");
        assert_eq!(
            record.information_coefficient.unwrap().sessions,
            2,
            "and only the ones that yielded a reading"
        );
    }

    #[test]
    fn test_file_names_round_trip_through_their_date() {
        let date = NaiveDate::from_ymd_opt(2026, 8, 17).unwrap();
        assert_eq!(file_name(date), "laboratory-2026-08-17.jsonl");
        assert_eq!(date_from_file_name(&file_name(date)), Some(date));
    }

    /// `experiment_type()` restates what `rename_all` generates for the variant. Left unpinned, a
    /// rename would move the export partition while the logged name kept the old spelling.
    #[test]
    fn test_the_reported_experiment_type_is_the_tag_that_serializes() {
        let value: serde_json::Value = serde_json::to_value(observation()).unwrap();
        assert_eq!(value["experiment_type"], serde_json::json!("dataset_built"));
        assert_eq!(observation().experiment_type(), "dataset_built");
        assert_eq!(
            value["experiment_type"].as_str(),
            Some(observation().experiment_type())
        );
    }

    /// The application's journal names its files by session date. Reading one of those as a
    /// laboratory file would file an instant under a trading day.
    #[test]
    fn test_an_application_journal_file_name_is_not_a_laboratory_one() {
        assert_eq!(date_from_file_name("fund-2026-08-17.jsonl"), None);
        assert_eq!(date_from_file_name("laboratory-2026-08-17.txt"), None);
    }

    /// The envelope is what the export partitions and joins on, so every field of it must survive
    /// serialization even though the payload is flattened alongside.
    #[test]
    fn test_a_record_serializes_its_envelope_and_its_payload() {
        let run_id = Uuid::new_v4();
        let timestamp = DateTime::from_timestamp_millis(1_755_000_000_000).unwrap();
        let record = Record::new(run_id, timestamp, observation());

        let value: serde_json::Value = serde_json::to_value(&record).unwrap();

        assert_eq!(value["schema_version"], serde_json::json!(1));
        assert_eq!(value["run_id"], serde_json::json!(run_id.to_string()));
        assert_eq!(value["experiment_type"], serde_json::json!("dataset_built"));
        assert_eq!(
            value["payload"]["fingerprint"]["rows"],
            serde_json::json!(10)
        );
        assert_eq!(
            value["payload"]["fingerprint"]["splits_digest"],
            serde_json::json!(0xAB)
        );
        assert!(
            value.get("session_date").is_none(),
            "an experiment belongs to no trading day"
        );
    }

    #[tokio::test]
    async fn test_records_roll_into_the_file_for_their_utc_date() {
        let directory = tempfile::tempdir().unwrap();
        let journal = Journal::new(directory.path()).unwrap();
        let run_id = Uuid::new_v4();

        let first = NaiveDate::from_ymd_opt(2026, 8, 17).unwrap();
        let second = NaiveDate::from_ymd_opt(2026, 8, 18).unwrap();

        // 23:30 on the 17th and 00:30 on the 18th: an hour apart, either side of UTC midnight.
        for (date, hour, minute) in [(first, 23, 30), (second, 0, 30)] {
            let timestamp = date.and_hms_opt(hour, minute, 0).unwrap().and_utc();
            journal
                .append(&Record::new(run_id, timestamp, observation()))
                .await
                .unwrap();
        }

        assert!(directory.path().join(file_name(first)).exists());
        assert!(directory.path().join(file_name(second)).exists());
    }
}
