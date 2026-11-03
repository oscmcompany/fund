//! Loads what studies read and catalogs what they do: a `Study` journals every dataset its loaders read and every
//! experiment it reports, then merges the journal into the profile bucket.

pub mod compare;
pub mod dataset;
pub mod replay;
pub mod sensitivity;
pub mod source;

use std::io;
use std::num::NonZeroU32;
use std::path::PathBuf;

use chrono::Utc;
use tokio::time::Instant;
use uuid::Uuid;

use crate::common::journal::{Observation, RunId};
use crate::common::laboratory::dataset::Fingerprint;
use crate::common::laboratory::experiment::{
    DatasetRead, Elapsed, ExperimentRan, Label, Machine, Name, Outputs, Parameters, SourceDigest,
};
use crate::common::laboratory::verdict::{
    Bar, Outcome, Question, QuestionDeclared, VerdictReached,
};
use crate::common::storage::{Host, JournalKey, Key, ObjectKey, SourceKey};
use crate::common::time::SessionDate;
use crate::journal::{Journal, file_name};
use crate::laboratory::dataset::Lineage;
use crate::laboratory::source::{Source, digest_of};
use crate::records::{RESHIPPED_DAYS, ShipFailure, contents, merge_into};
use crate::storage::{Bucket, BucketError};

/// One run of study code: its journal, what it is about, the code it runs, the machine it runs on, and when it opened.
pub struct Study {
    journal: Journal,
    label: Label,
    source_digest: SourceDigest,
    machine: Machine,
    opened: Instant,
}

#[derive(Debug, thiserror::Error)]
pub enum StudyError {
    #[error("the journal failed: {0}")]
    Journal(io::Error),
    /// The dataset was read by another run, whose journal holds its `dataset_read`.
    #[error("the dataset was read by run {run}, not this one")]
    ReadByAnotherRun { run: RunId },
}

impl Study {
    /// Opens a run journaling into `directory`, named for this machine's hostname, and writes `source` into its `sources`
    /// subdirectory for `finish` to ship; a study binary opens through `study!`, which captures `source`.
    pub fn open(label: Label, directory: impl Into<PathBuf>, source: Source) -> io::Result<Self> {
        let output = std::process::Command::new("hostname").output()?;
        if !output.status.success() {
            return Err(io::Error::other(format!(
                "hostname failed: {}",
                String::from_utf8_lossy(&output.stderr).trim()
            )));
        }
        let cores = std::thread::available_parallelism()?;
        let machine = Machine::new(
            String::from_utf8_lossy(&output.stdout),
            std::env::consts::ARCH,
            std::env::consts::OS,
            NonZeroU32::try_from(cores).unwrap_or(NonZeroU32::MAX),
        )
        .map_err(|refusal| io::Error::new(io::ErrorKind::InvalidData, refusal.to_string()))?;
        let journal = Journal::open(directory, RunId::new(Uuid::new_v4()))?;
        let sources = journal.directory().join(SOURCES);
        std::fs::create_dir_all(&sources)?;
        let source_digest = source.digest();
        // Written aside and renamed, so a concurrent `finish` never reads a partial source.
        let partial = sources.join(format!(".{}.{}", source_digest.as_str(), journal.run_id()));
        std::fs::write(&partial, source.encode())?;
        std::fs::rename(
            &partial,
            sources.join(format!("{}.{SOURCE_EXTENSION}", source_digest.as_str())),
        )?;
        Ok(Self {
            journal,
            label,
            source_digest,
            machine,
            opened: Instant::now(),
        })
    }

    pub fn run_id(&self) -> RunId {
        self.journal.run_id()
    }

    /// Journals a read; only loaders call it, so every dataset a study holds was cataloged.
    pub(crate) fn read(&mut self, fingerprint: &Fingerprint) -> io::Result<()> {
        let read = DatasetRead::new(
            self.label.clone(),
            self.machine.clone(),
            fingerprint.clone(),
        );
        self.journal
            .append(Utc::now(), Observation::DatasetRead(Box::new(read)))
    }

    /// Journals one experiment over the datasets `lineages` came from, which only the loader builds, so the record
    /// names exactly what was read; refused for a dataset another run read.
    pub fn experiment(
        &mut self,
        parameters: Parameters,
        lineages: &[&Lineage],
        outputs: Outputs,
    ) -> Result<(), StudyError> {
        if let Some(lineage) = lineages
            .iter()
            .find(|lineage| lineage.run() != self.run_id())
        {
            return Err(StudyError::ReadByAnotherRun { run: lineage.run() });
        }
        let ran = ExperimentRan::new(
            self.label.clone(),
            self.machine.clone(),
            parameters,
            lineages
                .iter()
                .map(|lineage| lineage.fingerprint().clone())
                .collect(),
            outputs,
            Elapsed::from_milliseconds(
                u64::try_from(self.opened.elapsed().as_millis()).unwrap_or(u64::MAX),
            ),
            self.source_digest.clone(),
        );
        self.journal
            .append(Utc::now(), Observation::ExperimentRan(Box::new(ran)))
            .map_err(StudyError::Journal)
    }

    /// Journals a question: the baseline skill is quoted against, the metric and bar that decide it, and the session
    /// after which data is the "as of" confirm read. Meant to be called before reading; nothing enforces the order.
    pub fn declare(
        &mut self,
        question: Question,
        baseline: Name,
        metric: Name,
        bar: Bar,
        as_of: SessionDate,
        parameters: Parameters,
    ) -> io::Result<()> {
        let declared = QuestionDeclared::new(
            self.label.clone(),
            question,
            baseline,
            metric,
            bar,
            as_of,
            parameters,
        );
        self.journal.append(
            Utc::now(),
            Observation::QuestionDeclared(Box::new(declared)),
        )
    }

    /// Journals how `question` closed and the runs the outcome rests on, which may include other runs than this one.
    pub fn verdict(
        &mut self,
        question: Question,
        outcome: Outcome,
        runs: Vec<RunId>,
    ) -> io::Result<()> {
        let reached = VerdictReached::new(self.label.clone(), question, outcome, runs);
        self.journal
            .append(Utc::now(), Observation::VerdictReached(Box::new(reached)))
    }

    /// Stores each source kept in the journal directory under its digest, then merges each recent session file there
    /// into its researcher object in `profile`, so studies from other directories or machines that shipped the same
    /// session keep their records.
    pub async fn finish(self, profile: &Bucket) -> Vec<(Shipment, Result<(), ShipFailure>)> {
        let mut shipped = Vec::new();
        let sources = self.journal.directory().join(SOURCES);
        match std::fs::read_dir(&sources) {
            Ok(entries) => {
                for entry in entries {
                    let path = match entry {
                        Ok(entry)
                            if entry
                                .path()
                                .extension()
                                .is_some_and(|extension| extension == SOURCE_EXTENSION) =>
                        {
                            entry.path()
                        }
                        Ok(..) => continue,
                        Err(error) => {
                            let failure = ShipFailure::Read {
                                path: sources.clone(),
                                error,
                            };
                            shipped.push((Shipment::Local(sources.clone()), Err(failure)));
                            continue;
                        }
                    };
                    match std::fs::read(&path) {
                        Ok(encoded) => {
                            let key = SourceKey::new(digest_of(&encoded));
                            let outcome = store_source(profile, &key, encoded).await;
                            if outcome.is_ok()
                                && let Err(error) = std::fs::remove_file(&path)
                            {
                                tracing::warn!(path = %path.display(), %error, "Shipped source not removed");
                            }
                            shipped.push((Shipment::Source(key), outcome));
                        }
                        Err(error) => {
                            shipped.push((
                                Shipment::Local(path.clone()),
                                Err(ShipFailure::Read { path, error }),
                            ));
                        }
                    }
                }
            }
            Err(error) => shipped.push((
                Shipment::Local(sources.clone()),
                Err(ShipFailure::Read {
                    path: sources,
                    error,
                }),
            )),
        }
        let today = SessionDate::at(Utc::now());
        for days in 0..RESHIPPED_DAYS {
            let session = today.plus_calendar_days(-days);
            let key = JournalKey::new(Host::Researcher, session);
            let outcome = match contents(&self.journal.directory().join(file_name(session))) {
                Some(Ok(text)) => merge_into(profile, key, &text).await,
                Some(Err(failure)) => Err(failure),
                None => continue,
            };
            shipped.push((Shipment::Journal(key), outcome));
        }
        shipped
    }
}

/// What one outcome of `finish` is about: a source stored under its key, a journal session merged into its key, or a
/// local path in the sources directory that could not be read, so no key was derived for it.
#[derive(Debug, Clone, PartialEq)]
pub enum Shipment {
    Source(SourceKey),
    Journal(JournalKey),
    Local(PathBuf),
}

impl std::fmt::Display for Shipment {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Source(key) => formatter.write_str(&key.path()),
            Self::Journal(key) => formatter.write_str(&Key::from(*key).path()),
            Self::Local(path) => write!(formatter, "{}", path.display()),
        }
    }
}

/// The subdirectory of a journal directory holding each source a study there opened with and no `finish` has stored
/// yet; one that failed to store stays for the next `finish`.
const SOURCES: &str = "sources";

const SOURCE_EXTENSION: &str = "json";

/// Creates `encoded` under `key`; a taken key counts as stored only when it reads back as these bytes, since a create
/// can also be contended by a concurrent delete.
async fn store_source(
    profile: &Bucket,
    key: &SourceKey,
    encoded: Vec<u8>,
) -> Result<(), ShipFailure> {
    match profile.create(key, encoded.clone()).await {
        Ok(()) => Ok(()),
        Err(BucketError::Contended { path }) => match profile.get(key).await {
            Ok(Some(held)) if held == encoded => Ok(()),
            Ok(Some(..) | None) => Err(ShipFailure::Unmatched { path }),
            Err(error) => Err(ShipFailure::Bucket(error)),
        },
        Err(error) => Err(ShipFailure::Bucket(error)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_a_shipment_displays_the_path_it_names() {
        let session =
            SessionDate::from_date(chrono::NaiveDate::from_ymd_opt(2026, 10, 21).unwrap());
        let digest = SourceDigest::new(&"0a".repeat(32)).unwrap();
        assert_eq!(
            Shipment::Source(SourceKey::new(digest)).to_string(),
            "records/sources/producer=researcher/\
             sha256=0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a/source.json"
        );
        assert_eq!(
            Shipment::Journal(JournalKey::new(Host::Researcher, session)).to_string(),
            "records/journal/producer=researcher/year=2026/month=10/day=21/data.parquet"
        );
        assert_eq!(
            Shipment::Local(PathBuf::from("journal/sources")).to_string(),
            "journal/sources"
        );
    }

    #[test]
    fn test_a_study_keeps_its_source_beside_its_journal_until_it_ships() {
        let directory = std::env::temp_dir().join(format!("fund-study-{}", Uuid::new_v4()));
        let source = Source::new("kept.rs", "fn main() {}");
        Study::open(Label::new("kept").unwrap(), &directory, source.clone()).unwrap();
        let kept: Vec<_> = std::fs::read_dir(directory.join("sources"))
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .collect();
        assert_eq!(
            kept,
            [directory.join(
                "sources/5c44ba654f4225378bdd19b74298a74f21cd7f31077f66d91f1a7f10e3119c51.json"
            )]
        );
        assert_eq!(std::fs::read(&kept[0]).unwrap(), source.encode());
        std::fs::remove_dir_all(&directory).unwrap();
    }

    /// A declaration, an experiment carrying its checks and the verdict that closes the question read back from the
    /// journal in the order the study wrote them.
    #[test]
    fn test_a_study_journals_its_question_checks_and_verdict() {
        use crate::common::journal::{ReadLine, read};
        use crate::common::laboratory::experiment::Metric;
        use crate::common::laboratory::verdict::{Checks, NullReading, Reopen, Scope};

        let directory = std::env::temp_dir().join(format!("fund-study-{}", Uuid::new_v4()));
        let mut study = Study::open(
            Label::new("records").unwrap(),
            &directory,
            Source::new("records.rs", "fn main() {}"),
        )
        .unwrap();
        let question = Question::new("does the arm beat its baseline").unwrap();
        let as_of = SessionDate::from_date(chrono::NaiveDate::from_ymd_opt(2026, 10, 30).unwrap());
        study
            .declare(
                question.clone(),
                Name::new("null_strategy").unwrap(),
                Name::new("net_bp").unwrap(),
                Bar::Above(Metric::new(1.0).unwrap()),
                as_of,
                Parameters::default(),
            )
            .unwrap();
        let checks =
            Checks::new(1254, 3, NullReading::Measured(Metric::new(0.1).unwrap())).unwrap();
        study
            .experiment(
                Parameters::default(),
                &[],
                Outputs::default().checks(checks.clone()).unwrap(),
            )
            .unwrap();
        let outcome = Outcome::Refuted {
            scope: Scope::new("every name, 15 minutes").unwrap(),
            reopen: Reopen::new("a cost under 1bp").unwrap(),
        };
        study
            .verdict(question.clone(), outcome.clone(), vec![study.run_id()])
            .unwrap();
        let file = std::fs::read_dir(&directory)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .find(|path| {
                path.extension()
                    .is_some_and(|extension| extension == "jsonl")
            })
            .unwrap();
        let observations: Vec<Observation> = read(&std::fs::read_to_string(&file).unwrap())
            .into_iter()
            .map(|line| match line {
                ReadLine::Read(record) => record.observation().clone(),
                other @ (ReadLine::Untyped(_) | ReadLine::Unreadable { .. }) => panic!("{other:?}"),
            })
            .collect();
        assert_eq!(
            observations
                .iter()
                .map(Observation::event_type)
                .collect::<Vec<_>>(),
            ["question_declared", "experiment_ran", "verdict_reached"]
        );
        match (&observations[0], &observations[1], &observations[2]) {
            (
                Observation::QuestionDeclared(declared),
                Observation::ExperimentRan(ran),
                Observation::VerdictReached(reached),
            ) => {
                assert_eq!((declared.question(), declared.as_of()), (&question, as_of));
                assert_eq!(ran.checks(), Some(&checks));
                assert_eq!(
                    (reached.question(), reached.outcome(), reached.runs()),
                    (&question, &outcome, &[study.run_id()][..])
                );
            }
            other => panic!("{other:?}"),
        }
        std::fs::remove_dir_all(&directory).unwrap();
    }

    /// Two studies in different directories ship the same session and source; the object keeps both runs' records,
    /// the second finds its source already stored, a study abandoned before `finish` has its source shipped by the next
    /// one in its directory, and each stored source leaves the directory.
    #[tokio::test]
    #[ignore = "writes researcher journal objects to the bucket AWS_S3_PROFILE_BUCKET_NAME names, which must be a \
                development bucket"]
    async fn live_two_studies_shipping_one_session_keep_both() {
        let bucket = std::env::var("AWS_S3_PROFILE_BUCKET_NAME").unwrap();
        assert!(
            bucket.contains("development"),
            "refusing to write test journals into {bucket}"
        );
        let configuration = aws_config::load_from_env().await;
        let profile = Bucket::profile(&configuration).unwrap();
        let source = Source::new("live merge check.rs", &Uuid::new_v4().to_string());
        let abandoned = Source::new("live merge check.rs", &Uuid::new_v4().to_string());
        let mut runs = Vec::new();
        for iteration in 0..2 {
            let directory = std::env::temp_dir().join(format!("fund-study-{}", Uuid::new_v4()));
            if iteration == 0 {
                Study::open(
                    Label::new("live merge check").unwrap(),
                    &directory,
                    abandoned.clone(),
                )
                .unwrap();
            }
            let mut study = Study::open(
                Label::new("live merge check").unwrap(),
                &directory,
                source.clone(),
            )
            .unwrap();
            study
                .experiment(Parameters::default(), &[], Outputs::default())
                .unwrap();
            runs.push(study.run_id());
            let shipped = study.finish(&profile).await;
            assert!(shipped.iter().any(
                |(shipment, _)| *shipment == Shipment::Source(SourceKey::new(source.digest()))
            ));
            for (shipment, outcome) in shipped {
                assert!(outcome.is_ok(), "{shipment}: {outcome:?}");
            }
            assert_eq!(
                std::fs::read_dir(directory.join("sources"))
                    .unwrap()
                    .count(),
                0
            );
            std::fs::remove_dir_all(&directory).unwrap();
        }
        assert_eq!(
            profile.get(&SourceKey::new(source.digest())).await.unwrap(),
            Some(source.encode())
        );
        assert_eq!(
            profile
                .get(&SourceKey::new(abandoned.digest()))
                .await
                .unwrap(),
            Some(abandoned.encode())
        );
        let key = JournalKey::new(Host::Researcher, SessionDate::at(Utc::now()));
        let held = crate::storage::journal::decode(
            &key,
            profile.get(&Key::from(key)).await.unwrap().unwrap(),
        )
        .unwrap();
        for run in runs {
            assert!(
                held.iter().any(|line| matches!(
                    line,
                    crate::common::journal::ReadLine::Read(record) if record.run_id() == run
                )),
                "{run} is missing"
            );
        }
    }
}
