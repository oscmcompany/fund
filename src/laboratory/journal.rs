//! The laboratory's own append-only record of what it ran.
//!
//! Keyed by run and rolled on the Eastern session the run's instant falls in.

use std::path::{Path, PathBuf};

use chrono::{DateTime, NaiveDate, Utc};
use serde::Serialize;
use tokio::io::AsyncWriteExt;
use tracing::{debug, error};
use uuid::Uuid;

use crate::common::types::SessionDate;
use crate::laboratory::convergence::Curve;
use crate::laboratory::dataset::DatasetFingerprint;
use crate::laboratory::metrics::Distribution;
use crate::laboratory::predictor::Evaluation;
use crate::laboratory::stability::{Association, SignAgreement};

/// The shape of a laboratory record, versioned independently of the application journal.
///
/// Readers map old versions forward rather than rewriting files, so this only ever goes up. v2 added
/// `liquidity_floor` to the `dataset_built` fingerprint and v3 added `reference_digest` beside it,
/// naming the point-in-time universe the rows were classified against.
pub const SCHEMA_VERSION: u32 = 3;

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
    StabilityMeasured(StabilityMeasured),
    RegimeMeasured(RegimeMeasured),
    ConvergenceMeasured(ConvergenceMeasured),
}

impl Observation {
    /// The stable name this observation serializes under, and the partition it exports into.
    pub fn experiment_type(&self) -> &'static str {
        match self {
            Observation::DatasetBuilt(_) => "dataset_built",
            Observation::ForecastScored(_) => "forecast_scored",
            Observation::FeatureTriaged(_) => "feature_triaged",
            Observation::StabilityMeasured(_) => "stability_measured",
            Observation::RegimeMeasured(_) => "regime_measured",
            Observation::ConvergenceMeasured(_) => "convergence_measured",
        }
    }
}

/// Whether a dislocated spread closes, which is the premise the pair book rests on.
///
/// `selection` carries the control and `segment` the replication, so an arm that only converges over
/// one half of the window says so in the record rather than in a follow-up nobody runs.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ConvergenceMeasured {
    /// How the pair was admitted: the screen's correlation band, or the population without it.
    pub selection: String,
    /// Which stretch of the window the entries were opened in.
    pub segment: String,
    pub sessions: usize,
    pub universe: usize,
    pub entries: usize,
    /// Over the entries that converged, so no convergence is absent rather than zero.
    pub median_sessions_to_convergence: Option<f64>,
    /// What the shares are worth: a convergence earns roughly this many deviations and a stop loses
    /// [`crate::portfolio::screen::STOP_LOSS_WIDENING`], so the curves alone cannot say which wins.
    pub mean_entry_z_score: Option<f64>,
    pub curves: Vec<Curve>,
}

/// Whether one forecast's per-session readings follow from the state of the market.
///
/// `lag` separates the two answers this can give: at zero the state describes the session being
/// read, which explains without anticipating, and only a positive lag is something a book could act
/// on before the session it speaks about.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct RegimeMeasured {
    pub predictor: String,
    /// Which per-session statistic was explained.
    pub statistic: String,
    /// Which description of the market it was explained by.
    pub state: String,
    /// Which stretch of the window it was measured over, so a figure can be asked to appear twice.
    pub segment: String,
    pub sessions: usize,
    pub associations: Vec<Association>,
    /// How far apart the two halves landed, recorded only on the whole-window record.
    ///
    /// Empty on a half's own record: the comparison qualifies the figure it is a split of, and
    /// repeating it on each half would journal one comparison three times.
    pub half_differences: Vec<HalfDifference>,
}

/// The gap between the two halves of the window, at one lag.
///
/// The split-sample check is this number against its error, not two figures that happen to point
/// the same way.
#[derive(Debug, Clone, Copy, PartialEq, Serialize)]
pub struct HalfDifference {
    pub lag: usize,
    /// The second half's association minus the first's.
    pub difference: f64,
    /// The halves are disjoint stretches, so their errors add in quadrature.
    pub standard_error: f64,
}

impl HalfDifference {
    /// The gap between two associations at the same lag, or `None` where either is missing.
    pub fn between(
        first_half: Option<&Association>,
        second_half: Option<&Association>,
    ) -> Option<Self> {
        let (first_half, second_half) = (first_half?, second_half?);
        if first_half.lag != second_half.lag {
            return None;
        }
        Some(Self {
            lag: first_half.lag,
            difference: second_half.correlation - first_half.correlation,
            standard_error: (first_half.standard_error.powi(2)
                + second_half.standard_error.powi(2))
            .sqrt(),
        })
    }
}

/// Whether one forecast's per-session readings carry into the sessions after them.
///
/// Both statistics are recorded because they answer the same question at different bluntness: a
/// series can co-move in magnitude while its sign is a coin, and only the sign is tradeable.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct StabilityMeasured {
    pub predictor: String,
    /// Which per-session statistic was followed through time.
    pub statistic: String,
    pub sessions: usize,
    pub autocorrelations: Vec<Association>,
    pub sign_agreements: Vec<SignAgreement>,
}

/// How much one feature says about the session it precedes.
///
/// `excess_share` is the answer and the rest are why it should be believed: the raw estimate is
/// biased upward at this sample size, `null_bits` is that bias measured on the same rows, and
/// `target_entropy_bits` is the ceiling that makes two targets comparable at all.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct FeatureTriaged {
    pub feature: String,
    pub sessions: usize,
    pub bits: Option<Distribution>,
    pub null_bits: Option<Distribution>,
    pub excess_bits: Option<Distribution>,
    /// What the target itself carries, which caps anything a feature can say about it.
    pub target_entropy_bits: Option<Distribution>,
    /// `excess_bits` as a share of that ceiling, which is the only figure comparable across targets.
    pub excess_share: Option<Distribution>,
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

/// The file the writer currently holds open, and the session it belongs to.
struct OpenSession {
    session: SessionDate,
    file: tokio::fs::File,
}

/// Appends records to the current session's file.
///
/// The session comes from the record's own instant rather than from construction, so the file rolls
/// at Eastern midnight whatever the process was doing at the time.
pub struct Journal {
    directory: PathBuf,
    open_session: tokio::sync::Mutex<Option<OpenSession>>,
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
            open_session: tokio::sync::Mutex::new(None),
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
        let session = SessionDate::at(record.timestamp);

        let mut open_session = self.open_session.lock().await;
        let open = match open_session.as_mut() {
            Some(open) if open.session == session => open,
            _ => {
                let file = tokio::fs::OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(self.directory.join(file_name(session)))
                    .await?;
                open_session.insert(OpenSession { session, file })
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
        let mut open_session = self.open_session.lock().await;
        *open_session = None;
        JournalGuard {
            _appends_blocked: open_session,
        }
    }
}

/// Proof that no append can run while it is alive.
pub struct JournalGuard<'a> {
    _appends_blocked: tokio::sync::MutexGuard<'a, Option<OpenSession>>,
}

/// Prefix naming files whose date is an Eastern session.
///
/// Distinct from the `laboratory-` files an earlier build wrote, whose date was the UTC day. The two
/// disagree either side of 20:00 Eastern, and nothing in a file says which rule named it, so the
/// generations are told apart by name rather than by inspection. Legacy files are inert: they are
/// never appended to, exported, or deleted, and can be removed by hand.
const SESSION_FILE_PREFIX: &str = "laboratory-session-";

/// The file one session's records are written to.
pub fn file_name(session_date: SessionDate) -> String {
    format!("{SESSION_FILE_PREFIX}{}.jsonl", session_date.date())
}

/// Recovers the session from a name built by [`file_name`], or `None` for anything else.
///
/// Accepted only if it is exactly what the writer would have produced: `%Y-%m-%d` also parses
/// `2026-8-11`, and admitting both spellings would let one session reach the export twice.
pub fn session_from_file_name(name: &str) -> Option<SessionDate> {
    let date = name
        .strip_prefix(SESSION_FILE_PREFIX)?
        .strip_suffix(".jsonl")?;
    let session_date = SessionDate::from_date(NaiveDate::parse_from_str(date, "%Y-%m-%d").ok()?);
    (file_name(session_date) == name).then_some(session_date)
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
            liquidity_floor: None,
            rows: 10,
            tickers: 2,
            first_timestamp: DateTime::from_timestamp_millis(0),
            last_timestamp: DateTime::from_timestamp_millis(86_400_000),
            splits_digest: 0xAB,
            boundaries_digest: 0xCD,
            reference_digest: Some(0xEF),
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
            target_entropy_bits: None,
            excess_share: None,
        });
        let value: serde_json::Value = serde_json::to_value(&triaged).unwrap();

        assert_eq!(
            value["experiment_type"],
            serde_json::json!("feature_triaged")
        );
        assert_eq!(triaged.experiment_type(), "feature_triaged");
        assert_ne!(triaged.experiment_type(), forecast.experiment_type());
        assert_ne!(triaged.experiment_type(), observation().experiment_type());

        let stability = Observation::StabilityMeasured(StabilityMeasured {
            predictor: "persistence".to_string(),
            statistic: "information_coefficient".to_string(),
            sessions: 498,
            autocorrelations: Vec::new(),
            sign_agreements: Vec::new(),
        });
        let value: serde_json::Value = serde_json::to_value(&stability).unwrap();

        assert_eq!(
            value["experiment_type"],
            serde_json::json!("stability_measured")
        );
        assert_eq!(stability.experiment_type(), "stability_measured");
        for other in [&forecast, &triaged, &observation()] {
            assert_ne!(stability.experiment_type(), other.experiment_type());
        }

        let regime = Observation::RegimeMeasured(RegimeMeasured {
            predictor: "persistence".to_string(),
            statistic: "information_coefficient".to_string(),
            state: "breadth".to_string(),
            segment: "whole".to_string(),
            sessions: 499,
            associations: Vec::new(),
            half_differences: Vec::new(),
        });
        let value: serde_json::Value = serde_json::to_value(&regime).unwrap();

        assert_eq!(
            value["experiment_type"],
            serde_json::json!("regime_measured")
        );
        assert_eq!(regime.experiment_type(), "regime_measured");
        for other in [&forecast, &triaged, &stability, &observation()] {
            assert_ne!(regime.experiment_type(), other.experiment_type());
        }

        let convergence = Observation::ConvergenceMeasured(ConvergenceMeasured {
            selection: "screened".to_string(),
            segment: "whole".to_string(),
            sessions: 499,
            universe: 200,
            entries: 1_284,
            median_sessions_to_convergence: None,
            mean_entry_z_score: None,
            curves: Vec::new(),
        });
        let value: serde_json::Value = serde_json::to_value(&convergence).unwrap();

        assert_eq!(
            value["experiment_type"],
            serde_json::json!("convergence_measured")
        );
        assert_eq!(convergence.experiment_type(), "convergence_measured");
        for other in [&forecast, &triaged, &stability, &regime, &observation()] {
            assert_ne!(convergence.experiment_type(), other.experiment_type());
        }
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
    fn test_file_names_round_trip_through_their_session() {
        let session = SessionDate::from_date(NaiveDate::from_ymd_opt(2026, 8, 17).unwrap());
        assert_eq!(file_name(session), "laboratory-session-2026-08-17.jsonl");
        assert_eq!(session_from_file_name(&file_name(session)), Some(session));
        assert_eq!(
            session_from_file_name("laboratory-session-2026-8-17.jsonl"),
            None,
            "one session must not reach the export under two spellings"
        );
    }

    /// A file the previous build wrote names a UTC day, and nothing inside it says so.
    ///
    /// Read as a session it would ship records either side of 20:00 Eastern to the wrong partition
    /// and then delete the only local copy, so it must not be recognised at all.
    #[test]
    fn test_a_legacy_utc_dated_file_is_not_read_as_a_session() {
        assert_eq!(session_from_file_name("laboratory-2026-08-17.jsonl"), None);
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
        assert_eq!(session_from_file_name("fund-2026-08-17.jsonl"), None);
        assert_eq!(session_from_file_name("laboratory-2026-08-17.txt"), None);
    }

    /// Two halves pointing the same way is not agreement, and only the gap and its own error can
    /// say whether they differ. The halves are disjoint, so the errors add in quadrature.
    #[test]
    fn test_the_gap_between_two_halves_carries_its_own_error() {
        let half = |correlation: f64, standard_error: f64| Association {
            lag: 1,
            correlation,
            standard_error,
            pairs: 250,
        };
        let first = half(0.20, 0.03);
        let second = half(0.50, 0.04);

        let gap = HalfDifference::between(Some(&first), Some(&second)).unwrap();

        assert_eq!(gap.lag, 1);
        assert!((gap.difference - 0.30).abs() < 1e-12, "{gap:?}");
        assert!((gap.standard_error - 0.05).abs() < 1e-12, "{gap:?}");
        assert_eq!(
            HalfDifference::between(Some(&first), None),
            None,
            "a half that could not be measured leaves no gap to report"
        );
        let other_lag = half(0.50, 0.04);
        assert_eq!(
            HalfDifference::between(Some(&Association { lag: 0, ..first }), Some(&other_lag)),
            None,
            "two lags are two questions"
        );
    }

    /// The envelope is what the export partitions and joins on, so every field of it must survive
    /// serialization even though the payload is flattened alongside.
    #[test]
    fn test_a_record_serializes_its_envelope_and_its_payload() {
        let run_id = Uuid::new_v4();
        let timestamp = DateTime::from_timestamp_millis(1_755_000_000_000).unwrap();
        let record = Record::new(run_id, timestamp, observation());

        let value: serde_json::Value = serde_json::to_value(&record).unwrap();

        assert_eq!(value["schema_version"], serde_json::json!(3));
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

    /// An evening run straddles UTC midnight and no session boundary, and the export partitions on
    /// this name into a bucket whose every other prefix is Eastern-session-partitioned.
    #[tokio::test]
    async fn test_records_roll_on_the_eastern_session_and_not_the_utc_date() {
        let directory = tempfile::tempdir().unwrap();
        let journal = Journal::new(directory.path()).unwrap();
        let run_id = Uuid::new_v4();

        let seventeenth = NaiveDate::from_ymd_opt(2026, 8, 17).unwrap();
        let eighteenth = NaiveDate::from_ymd_opt(2026, 8, 18).unwrap();

        // 23:30 and 00:30 UTC are an hour apart and both fall on the evening of the 17th Eastern;
        // 05:00 UTC on the 18th is the small hours of the 18th.
        for (date, hour) in [(seventeenth, 23), (eighteenth, 0), (eighteenth, 5)] {
            let timestamp = date.and_hms_opt(hour, 30, 0).unwrap().and_utc();
            journal
                .append(&Record::new(run_id, timestamp, observation()))
                .await
                .unwrap();
        }

        let session_file = |date: NaiveDate| {
            directory
                .path()
                .join(file_name(SessionDate::from_date(date)))
        };
        let lines = |date: NaiveDate| std::fs::read_to_string(session_file(date)).unwrap();

        assert_eq!(
            lines(seventeenth).lines().count(),
            2,
            "the evening pair belongs to one session"
        );
        assert_eq!(lines(eighteenth).lines().count(), 1);
    }
}
