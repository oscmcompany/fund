//! The laboratory's own append-only record of what it ran.
//!
//! Keyed by run and rolled on the Eastern session the run's instant falls in.

use std::path::{Path, PathBuf};

use chrono::{DateTime, NaiveDate, Utc};
use serde::{Deserialize, Serialize};
use tokio::io::AsyncWriteExt;
use tracing::{debug, error};
use uuid::Uuid;

use crate::common::types::SessionDate;
use crate::laboratory::convergence::Curve;
use crate::laboratory::dataset::DatasetFingerprint;
use crate::laboratory::harness::{
    DeclaredUniverse, Horizon, NetOfCost, Pairing, Quantity, StudyResult,
};
use crate::laboratory::metrics::Distribution;
use crate::laboratory::predictor::Evaluation;
use crate::laboratory::stability::{Association, SignAgreement};

/// The shape of a laboratory record, versioned independently of the application journal.
///
/// Readers map old versions forward rather than rewriting files, so this only ever goes up. v2 added
/// `liquidity_floor` to the `dataset_built` fingerprint and v3 added `reference_digest` beside it,
/// naming the point-in-time universe the rows were classified against. v4 added
/// `factor_specification`, naming the factor set a residual panel was fitted against. v5 adds the
/// `study_measured` observation — the first record carrying a declaration alongside a reading — and
/// `screen_window` beside the fingerprint's floor, because a screen is a floor *and* the stretch of
/// history it was applied over. v6 adds `slippage_measured`. v7 writes an unmeasurable
/// `net_of_cost` as null where v5 and v6 wrote `{"outcome": "unmeasured"}`; both read as `None`.
pub const SCHEMA_VERSION: u32 = 7;

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
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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
    StudyMeasured(StudyMeasured),
    SlippageMeasured(SlippageMeasured),
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
            Observation::StudyMeasured(_) => "study_measured",
            Observation::SlippageMeasured(_) => "slippage_measured",
        }
    }
}

/// One declared comparison and what it read.
///
/// The declaration travels with the reading rather than being recoverable from the binary that
/// produced it, because a threshold is only a threshold relative to the family it was spent
/// against: a record carrying the statistic without `family_tests` is a number nobody can judge.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StudyMeasured {
    pub question: String,
    pub family: String,
    /// Tests sharing the family, which is the denominator `required_standard_errors` came from.
    pub family_tests: usize,
    pub universe: DeclaredUniverse,
    pub horizon: Horizon,
    pub quantity: Quantity,
    pub pairing: Pairing,
    pub treatment: String,
    pub control: String,
    /// Rows both arms were folded from, beside the per-arm session counts inside each distribution.
    pub observations: usize,
    pub treatment_reading: Option<Distribution>,
    pub control_reading: Option<Distribution>,
    pub difference: Option<Distribution>,
    pub standard_errors: Option<f64>,
    pub required_standard_errors: f64,
    pub family_wise_error_rate: f64,
    /// `None` where nothing was measurable, which is not the same as a reading that failed the bar.
    pub clears_haircut: Option<bool>,
    /// `None` exactly where `difference` is.
    #[serde(deserialize_with = "net_of_cost_through_every_version")]
    pub net_of_cost: Option<NetOfCost>,
}

/// Reads a `net_of_cost` from any version, mapping the retired `unmeasured` outcome to `None`.
fn net_of_cost_through_every_version<'de, D>(deserializer: D) -> Result<Option<NetOfCost>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::de::Error;
    let value = Option::<serde_json::Value>::deserialize(deserializer)?;
    match value {
        None => Ok(None),
        Some(value) if value["outcome"] == "unmeasured" => Ok(None),
        Some(value) => serde_json::from_value(value)
            .map(Some)
            .map_err(D::Error::custom),
    }
}

impl From<&StudyResult> for StudyMeasured {
    fn from(result: &StudyResult) -> Self {
        let declaration = result.declaration();
        Self {
            question: declaration.question().to_string(),
            family: declaration.family().name().to_string(),
            family_tests: declaration.family().tests().get(),
            universe: declaration.universe().clone(),
            horizon: declaration.horizon(),
            quantity: declaration.quantity(),
            pairing: result.pairing(),
            treatment: result.treatment_name().to_string(),
            control: result.control_name().to_string(),
            observations: result.observations(),
            treatment_reading: result.treatment(),
            control_reading: result.control(),
            difference: result.difference(),
            standard_errors: result.standard_errors(),
            required_standard_errors: result.haircut().required_standard_errors(),
            family_wise_error_rate: result.haircut().family_wise_error_rate(),
            clears_haircut: result.clears_haircut(),
            net_of_cost: result.net_of_cost(),
        }
    }
}

/// Whether a dislocated spread closes, which is the premise the pair book rests on.
///
/// `selection` carries the control and `segment` the replication, so an arm that only converges over
/// one half of the window says so in the record rather than in a follow-up nobody runs.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
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
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StabilityMeasured {
    pub predictor: String,
    /// Which per-session statistic was followed through time.
    pub statistic: String,
    pub sessions: usize,
    pub autocorrelations: Vec<Association>,
    pub sign_agreements: Vec<SignAgreement>,
}

/// What the trader's completed-pair entries paid against the prices they were decided at.
///
/// A measurement of the record rather than a test of a hypothesis, so it carries no family: read
/// from `pair_opened`, whose fill and decision prices have sat side by side and never subtracted.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SlippageMeasured {
    pub legs: usize,
    /// Legs whose decision price could not be divided by.
    pub undefined: usize,
    /// The mean of each session's mean leg cost in basis points, with its error across sessions.
    pub cost_basis_points: Option<Distribution>,
    /// The first and last session the legs came from.
    pub first_session: Option<SessionDate>,
    pub last_session: Option<SessionDate>,
}

/// How much one feature says about the session it precedes.
///
/// `excess_share` is the answer and the rest are why it should be believed: the raw estimate is
/// biased upward at this sample size, `null_bits` is that bias measured on the same rows, and
/// `target_entropy_bits` is the ceiling that makes two targets comparable at all.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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
    /// Rows the feature was actually defined on.
    ///
    /// Not every column covers the panel: the quote and trade summaries are short of a handful of
    /// names a session, and a ranking that folded that away would print the same number for a
    /// feature measured on all of the panel and one measured on two thirds of it.
    pub defined_rows: usize,
}

/// One frame prepared for an experiment to read.
///
/// The fingerprint is what makes a later result comparable, so it is recorded once here and
/// referenced by `run_id` rather than repeated on every result the run goes on to produce.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DatasetBuilt {
    pub fingerprint: DatasetFingerprint,
    /// The commit this ran from, so a number can be traced to the code that produced it.
    ///
    /// Private because it is not a caller's choice. It was read from `FUND_REVISION` at eight sites
    /// and set by none of them, so every record ever written carried null.
    revision: Option<String>,
}

impl DatasetBuilt {
    /// Records a prepared dataset, stamping the commit this binary was built from.
    ///
    /// `None` when the build had no git to ask, which is the honest answer; `build.rs` appends
    /// `-dirty` rather than naming a commit that is not what ran.
    pub fn new(fingerprint: DatasetFingerprint) -> Self {
        Self {
            fingerprint,
            revision: option_env!("FUND_REVISION").map(str::to_string),
        }
    }
}

/// What one forecast was worth over one dataset.
///
/// Summarized rather than per session: `sessions` is what the panel held and each distribution
/// counts what it could measure, so a forecast that never ranked reads as absent and not as zero.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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

    /// Opens a journal in the same directory the application's uses, from the same variable.
    ///
    /// The two never share a host, and `laboratory-session-` against `session-` already tells the
    /// files apart, so a second variable bought nothing and was declared in no script.
    pub fn from_env() -> Result<Self, JournalError> {
        Self::new(crate::common::journal::journal_directory_from_env())
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

/// A line of the laboratory journal, read back.
pub type ReadRecord = crate::common::journal::ReadLine<Record>;

/// Reads every line of one laboratory session's file, naming an unreadable line by its experiment.
pub fn read_records(contents: &str) -> Vec<ReadRecord> {
    crate::common::journal::read_lines(contents, "experiment_type")
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
            screen_window: None,
            rows: 10,
            tickers: 2,
            first_timestamp: DateTime::from_timestamp_millis(0),
            last_timestamp: DateTime::from_timestamp_millis(86_400_000),
            splits_digest: 0xAB,
            boundaries_digest: 0xCD,
            reference_digest: Some(0xEF),
            factor_specification: Some(
                crate::laboratory::residual::FactorSpecification::new(45, 0.25)
                    .expect("the fixture must be a usable specification"),
            ),
            microstructure: crate::laboratory::dataset::Microstructure::Omitted,
            quote_summary_digest: None,
            trade_summary_digest: None,
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
            defined_rows: 1_000,
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

    /// A dataset record reads back as written, fingerprint and floor included.
    #[test]
    fn test_a_dataset_record_reads_back_as_written() {
        let timestamp = DateTime::from_timestamp_millis(1_755_000_000_000).unwrap();
        let record = Record::new(Uuid::nil(), timestamp, observation());
        let line = serde_json::to_string(&record).unwrap();
        match read_records(&line).as_slice() {
            [crate::common::journal::ReadLine::Read(read)] => assert_eq!(**read, record),
            other => panic!("{line} read as {other:?}"),
        }
    }

    /// The denominator has to reach the file, not only the terminal.
    ///
    /// A record carrying the statistic without the family it was spent against is a number nobody
    /// can judge later — which is exactly the state every prior laboratory record is in, and the
    /// reason this one exists.
    #[test]
    fn test_a_study_record_carries_the_family_that_set_its_threshold() {
        use std::num::NonZeroUsize;

        use crate::laboratory::harness::{
            Arm, Declaration, DeclaredUniverse, Family, Horizon, Pairing, Quantity, Study,
        };

        let one = NonZeroUsize::new(1).expect("a positive count");
        // One session a day, the same four for both arms, which is what `Pairing::Matched` requires.
        let arm = |name: &str, readings: [f64; 4]| {
            let keyed: Vec<(i64, Option<f64>)> = readings
                .into_iter()
                .enumerate()
                .map(|(index, reading)| (index as i64 * 86_400_000, Some(reading)))
                .collect();
            Arm::new(name, keyed, 4).expect("a usable arm")
        };
        let result = Study::new(
            Declaration::new(
                "does the sector factor explain anything",
                Family::new(
                    "residual-panel",
                    NonZeroUsize::new(7).expect("a positive count"),
                ),
                Horizon::Sessions(one),
                DeclaredUniverse::Unscreened,
                Quantity::Unpriced {
                    units: crate::laboratory::harness::Units::Share,
                },
            ),
            Pairing::Matched,
            arm("real sectors", [0.30, 0.25, 0.28, 0.26]),
            arm("permuted sectors", [0.22, 0.20, 0.21, 0.19]),
            &fingerprint(),
        )
        .expect("the fixture must assemble")
        .measure();

        let observation = Observation::StudyMeasured(StudyMeasured::from(&result));
        assert_eq!(observation.experiment_type(), "study_measured");
        let timestamp = DateTime::from_timestamp_millis(1_755_000_000_000).unwrap();
        let record = Record::new(Uuid::nil(), timestamp, observation.clone());
        let line = serde_json::to_string(&record).unwrap();
        match read_records(&line).as_slice() {
            [crate::common::journal::ReadLine::Read(read)] => assert_eq!(**read, record),
            other => panic!("{line} read as {other:?}"),
        }

        let value: serde_json::Value = serde_json::to_value(&observation).unwrap();
        assert_eq!(
            value["experiment_type"],
            serde_json::json!("study_measured")
        );
        assert_eq!(
            value["experiment_type"].as_str(),
            Some(observation.experiment_type())
        );
        assert_eq!(value["payload"]["family_tests"], serde_json::json!(7));
        assert_eq!(
            value["payload"]["family"],
            serde_json::json!("residual-panel")
        );
        assert_eq!(
            value["payload"]["universe"],
            serde_json::json!("Unscreened")
        );
        assert_eq!(value["payload"]["pairing"], serde_json::json!("Matched"));
        assert_eq!(
            value["payload"]["treatment"],
            serde_json::json!("real sectors")
        );
        // The threshold itself, so a reader does not have to rebuild it from the count.
        assert!(
            (value["payload"]["required_standard_errors"]
                .as_f64()
                .expect("a number")
                - 2.690_109_527_158_866)
                .abs()
                < 1e-9,
            "{value}"
        );
        assert_eq!(value["payload"]["clears_haircut"], serde_json::json!(true));
        assert_eq!(
            value["payload"]["net_of_cost"]["outcome"],
            serde_json::json!("not_a_return")
        );
    }

    /// A v6 record wrote an unmeasurable cost as an outcome that no longer exists; it reads as
    /// absent rather than failing the whole record.
    #[test]
    fn test_a_retired_unmeasured_outcome_reads_as_absent() {
        use crate::laboratory::harness::{DeclaredUniverse, Horizon, Pairing, Quantity, Units};
        let unmeasured = StudyMeasured {
            question: "does anything differ".to_string(),
            family: "residual-panel".to_string(),
            family_tests: 2,
            universe: DeclaredUniverse::Unscreened,
            horizon: Horizon::Sessions(std::num::NonZeroUsize::new(1).unwrap()),
            quantity: Quantity::Unpriced {
                units: Units::Share,
            },
            pairing: Pairing::Matched,
            treatment: "treatment".to_string(),
            control: "control".to_string(),
            observations: 0,
            treatment_reading: None,
            control_reading: None,
            difference: None,
            standard_errors: None,
            required_standard_errors: 2.24,
            family_wise_error_rate: 0.05,
            clears_haircut: None,
            net_of_cost: None,
        };
        let mut value = serde_json::to_value(&unmeasured).unwrap();
        assert_eq!(value["net_of_cost"], serde_json::Value::Null);
        value["net_of_cost"] = serde_json::json!({"outcome": "unmeasured"});
        let read: StudyMeasured = serde_json::from_value(value.clone()).unwrap();
        assert_eq!(read.net_of_cost, None);
        value["net_of_cost"] = serde_json::Value::Null;
        let read: StudyMeasured = serde_json::from_value(value).unwrap();
        assert_eq!(read.net_of_cost, None);
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

        assert_eq!(value["schema_version"], serde_json::json!(7));
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
        // The universe is part of the result, so it has to reach the file rather than only the
        // struct: two runs over the same window and different universes must not read as one.
        assert_eq!(
            value["payload"]["fingerprint"]["reference_digest"],
            serde_json::json!(0xEF)
        );
        // The window is half the screen, and a floor without it does not name a population: the
        // same bounds over a trailing month and over two years admit different sets of names.
        assert!(
            value["payload"]["fingerprint"]
                .as_object()
                .expect("the fingerprint is an object")
                .contains_key("screen_window"),
            "{value}"
        );
        assert_eq!(
            value["payload"]["fingerprint"]["screen_window"],
            serde_json::json!(null)
        );
        // Both fields, not just the lookback: a panel fitted at a different variance share measures
        // a different set of names, and a record carrying only one of them cannot say which.
        assert_eq!(
            value["payload"]["fingerprint"]["factor_specification"]["volatility_sessions"],
            serde_json::json!(45)
        );
        assert_eq!(
            value["payload"]["fingerprint"]["factor_specification"]
                ["minimum_residual_variance_share"],
            serde_json::json!(0.25)
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

    /// One variable configures a box and the file names keep the two records apart, so a script
    /// that provisions `/var/journal/fund` provisions both.
    #[test]
    #[serial_test::serial]
    fn test_both_journals_open_the_same_directory() {
        let laboratory = Journal::from_env().expect("the laboratory journal must resolve");
        let application = crate::common::journal::Journal::from_env()
            .expect("the application journal must resolve");

        assert_eq!(laboratory.directory(), application.directory());
    }

    /// Without this the `revision` on every record is null, which is the state this field was in
    /// from the day it was added until the build began stamping it.
    #[test]
    fn test_the_build_stamps_a_revision() {
        let stamp = DatasetBuilt::new(fingerprint()).revision;

        let stamp = stamp.expect("the build must stamp a revision; this tree is a git checkout");
        let commit = stamp.strip_suffix("-dirty").unwrap_or(&stamp);
        assert_eq!(commit.len(), 40, "a full sha, not an abbreviation: {stamp}");
        assert!(
            commit
                .chars()
                .all(|character| character.is_ascii_hexdigit()),
            "a sha and nothing else: {stamp}"
        );
    }
}
