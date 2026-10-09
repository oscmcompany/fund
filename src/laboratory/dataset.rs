//! Loads archive series for studies, each with the fingerprint of exactly what was read.

use std::collections::BTreeMap;

use crate::archive::bars::{DecodeRefusal, decode};
use crate::archive::{Archive, ArchiveError};
use crate::common::heal::massive_daily_bars;
use crate::common::journal::RunId;
use crate::common::laboratory::dataset::{
    Contamination, DatasetLeg, Fingerprint, FingerprintRefusal,
};
use crate::common::laboratory::series::{Series, SeriesRefusal};
use crate::common::market::record::Bar;
use crate::common::storage::EntityTag;
use crate::common::time::calendar::TradingCalendar;
use crate::common::time::{SessionDate, SessionRange};
use crate::laboratory::Study;

/// Bars by session and the fingerprint of the partitions they came from.
#[derive(Debug)]
pub struct Dataset {
    bars: BTreeMap<SessionDate, Vec<Bar>>,
    fingerprint: Fingerprint,
    /// The run whose journal holds this read.
    run: RunId,
}

impl Dataset {
    pub fn bars(&self) -> &BTreeMap<SessionDate, Vec<Bar>> {
        &self.bars
    }

    pub fn fingerprint(&self) -> &Fingerprint {
        &self.fingerprint
    }

    pub fn run(&self) -> RunId {
        self.run
    }

    /// One reading per session read, `read` folding that session's bars; a missing session stays out of the series.
    pub fn series(&self, read: impl Fn(&[Bar]) -> Option<f64>) -> Result<Series, SeriesRefusal> {
        Series::new(
            self.bars
                .iter()
                .map(|(session, bars)| (*session, read(bars))),
        )
    }
}

#[derive(Debug)]
pub enum DatasetError {
    Window(FingerprintRefusal),
    /// The read could not be journaled, so it is not returned: a study holds only cataloged data.
    Journal(std::io::Error),
    Archive(ArchiveError),
    Decode {
        session: SessionDate,
        refusal: DecodeRefusal,
    },
    /// A leg retired with its data, which only journals still name.
    Retired {
        leg: DatasetLeg,
    },
    /// A partition that holds no bars is a defect in the archive, not a gap, so it is refused rather than read.
    EmptyPartition {
        session: SessionDate,
    },
}

impl std::fmt::Display for DatasetError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Window(refusal) => write!(formatter, "{refusal}"),
            Self::Journal(error) => write!(formatter, "the read could not be journaled: {error}"),
            Self::Archive(error) => write!(formatter, "{error}"),
            Self::EmptyPartition { session } => {
                write!(formatter, "the partition for {session} holds no bars")
            }
            Self::Retired { leg } => write!(formatter, "{leg} was retired with its data"),
            Self::Decode { session, refusal } => {
                write!(
                    formatter,
                    "the partition for {session} did not decode: {refusal:?}"
                )
            }
        }
    }
}

impl std::error::Error for DatasetError {}

/// Massive daily bars for every trading session in `range`, journaled to `study` before it is returned; a session with
/// no partition is recorded missing in the fingerprint rather than refused.
pub async fn daily_bars(
    archive: &Archive,
    calendar: &TradingCalendar,
    range: SessionRange,
    study: &mut Study,
) -> Result<Dataset, DatasetError> {
    load(
        DatasetLeg::MassiveDailyBars,
        archive,
        calendar,
        range,
        study,
    )
    .await
}

/// `leg`'s bars for every trading session in `range`, journaled to `study` before it is returned.
async fn load(
    leg: DatasetLeg,
    archive: &Archive,
    calendar: &TradingCalendar,
    range: SessionRange,
    study: &mut Study,
) -> Result<Dataset, DatasetError> {
    // Taken empty first, so the window is checked before any read and its missing sessions are the ones to read.
    let owed =
        Fingerprint::new(leg, range, calendar, BTreeMap::new()).map_err(DatasetError::Window)?;
    let (mut bars, mut tags) = (BTreeMap::new(), BTreeMap::new());
    for session in owed.missing() {
        let Some((read, tag)) = partition(archive, leg, *session).await? else {
            continue;
        };
        bars.insert(*session, admit(*session, read)?);
        tags.insert(*session, tag);
    }
    let fingerprint = Fingerprint::new(leg, range, calendar, tags).map_err(DatasetError::Window)?;
    study.read(&fingerprint).map_err(DatasetError::Journal)?;
    Ok(Dataset {
        bars,
        fingerprint,
        run: study.run_id(),
    })
}

/// `leg`'s bars for `session` with the tag of the version read, or `None` when it has no partition.
async fn partition(
    archive: &Archive,
    leg: DatasetLeg,
    session: SessionDate,
) -> Result<Option<(Vec<Bar>, EntityTag)>, DatasetError> {
    match leg {
        DatasetLeg::MassiveDailyBars => {
            let key = massive_daily_bars(session);
            let Some((body, tag)) = archive
                .get_tagged(&key.into())
                .await
                .map_err(DatasetError::Archive)?
            else {
                return Ok(None);
            };
            let (bars, _) =
                decode(&key, body).map_err(|refusal| DatasetError::Decode { session, refusal })?;
            Ok(Some((bars, tag)))
        }
        DatasetLeg::LegacyDailyBars => Err(DatasetError::Retired { leg }),
    }
}

/// Every partition `fingerprint` read that has since been rewritten or removed, each tag looked up now.
pub async fn lineage(
    archive: &Archive,
    fingerprint: &Fingerprint,
) -> Result<Vec<Contamination>, ArchiveError> {
    let mut current = BTreeMap::new();
    for session in fingerprint.partitions().keys() {
        let tag = match fingerprint.leg() {
            DatasetLeg::MassiveDailyBars => {
                archive.tag(&massive_daily_bars(*session).into()).await?
            }
            // Never looked up, so every partition such a read held is reported gone.
            DatasetLeg::LegacyDailyBars => None,
        };
        if let Some(tag) = tag {
            current.insert(*session, tag);
        }
    }
    Ok(fingerprint.contaminated(&current))
}

fn admit(session: SessionDate, bars: Vec<Bar>) -> Result<Vec<Bar>, DatasetError> {
    match bars.is_empty() {
        true => Err(DatasetError::EmptyPartition { session }),
        false => Ok(bars),
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use chrono::NaiveDate;

    use super::*;
    use crate::common::journal::{Observation, ReadLine, read};
    use crate::common::laboratory::estimate::{Estimate, summarize};
    use crate::common::laboratory::experiment::{Label, Outputs, Parameters};
    use crate::common::market::record::{BarInterval, BarPrices};
    use crate::common::market::{Price, Shares, Symbol};
    use crate::common::storage::{Host, JournalKey};
    use crate::common::time::calendar::TradingSession;
    use crate::ingest::alpaca::Alpaca;

    fn session(day: i64) -> SessionDate {
        SessionDate::from_date(NaiveDate::from_ymd_opt(2026, 3, 2).unwrap()).plus_calendar_days(day)
    }

    /// Sessions 0, 1 and 3 read by `run` with one, two and three bars, and session 2 missing.
    pub(crate) fn dataset(run: RunId) -> Dataset {
        let (open, close) = (
            chrono::NaiveTime::from_hms_opt(9, 30, 0).unwrap(),
            chrono::NaiveTime::from_hms_opt(16, 0, 0).unwrap(),
        );
        let calendar = TradingCalendar::new(
            (0..4)
                .map(|day| TradingSession::new(session(day), open, close).unwrap())
                .collect(),
            SessionRange::new(session(0), session(3)).unwrap(),
        )
        .unwrap();
        let price = |dollars: f64| Price::from_dollars(dollars).unwrap();
        let bars = |day: i64, count: usize| {
            let bar = Bar::new(
                Symbol::new("AAPL").unwrap(),
                BarInterval::OneDay,
                session(day).regular_close(),
                BarPrices::new(price(10.0), price(12.0), price(9.5), price(11.0)).unwrap(),
                Shares::from_float(100.0).unwrap(),
                None,
                None,
            )
            .unwrap();
            (session(day), vec![bar; count])
        };
        let read = [0, 1, 3]
            .map(|day| (session(day), EntityTag::new(&format!("\"tag-{day}\""))))
            .into();
        Dataset {
            bars: [bars(0, 1), bars(1, 2), bars(3, 3)].into(),
            fingerprint: Fingerprint::new(
                DatasetLeg::MassiveDailyBars,
                SessionRange::new(session(0), session(3)).unwrap(),
                &calendar,
                read,
            )
            .unwrap(),
            run,
        }
    }

    /// The series keeps exactly the sessions read, an unmeasured one as `None`.
    #[test]
    fn test_a_series_holds_one_reading_per_session_read() {
        let dataset = dataset(RunId::new(uuid::Uuid::new_v4()));
        let series = dataset
            .series(|bars| (bars.len() != 2).then_some(bars.len() as f64))
            .unwrap();
        assert_eq!(
            series.readings().iter().collect::<Vec<_>>(),
            [
                (&session(0), &Some(1.0)),
                (&session(1), &None),
                (&session(3), &Some(3.0))
            ]
        );
        assert_eq!(dataset.fingerprint().missing(), [session(2)]);
    }

    /// A study's reads and experiments land in its journal in order, name exactly the data read, and survive the
    /// parquet encoding the records bucket stores.
    #[test]
    fn test_a_study_catalogs_what_it_read_and_ran() {
        let directory = std::env::temp_dir().join(format!("fund-study-{}", uuid::Uuid::new_v4()));
        let mut study = Study::open(Label::new("bar counts").unwrap(), &directory).unwrap();
        let dataset = dataset(study.run_id());
        study.read(dataset.fingerprint()).unwrap();
        let series = dataset.series(|bars| Some(bars.len() as f64)).unwrap();
        let estimate = Estimate::try_from(summarize(&series)).unwrap();
        for variant in ["all", "none"] {
            study
                .experiment(
                    Parameters::new([("variant", variant)]).unwrap(),
                    &[&dataset],
                    Outputs::default()
                        .estimate("bars per session", estimate)
                        .unwrap()
                        .metric("sessions", 3.0)
                        .unwrap(),
                )
                .unwrap();
        }
        let file = std::fs::read_dir(&directory)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .find(|path| {
                path.extension()
                    .is_some_and(|extension| extension == "jsonl")
            })
            .unwrap();
        let lines = read(&std::fs::read_to_string(&file).unwrap());
        let observations: Vec<Observation> = lines
            .iter()
            .map(|line| match line {
                ReadLine::Read(record) => record.observation().clone(),
                ReadLine::Unreadable { line, cause, .. } => panic!("line {line}: {cause:?}"),
            })
            .collect();
        assert_eq!(
            observations
                .iter()
                .map(Observation::event_type)
                .collect::<Vec<_>>(),
            ["dataset_read", "experiment_ran", "experiment_ran"]
        );
        let (first, second) = match (&observations[1], &observations[2]) {
            (Observation::ExperimentRan(first), Observation::ExperimentRan(second)) => {
                (first, second)
            }
            other => panic!("{other:?}"),
        };
        assert_eq!(first.fingerprints(), [dataset.fingerprint().clone()]);
        assert_eq!(first.parameters().settings()["variant"], "all");
        assert_eq!(first.estimates()["bars per session"], estimate);
        assert!(!first.machine().hostname().is_empty());
        assert_eq!(
            (
                first.machine().architecture(),
                first.machine().operating_system()
            ),
            (std::env::consts::ARCH, std::env::consts::OS)
        );
        assert!(first.since_opened() <= second.since_opened());
        let session = match &lines[0] {
            ReadLine::Read(record) => record.session(),
            ReadLine::Unreadable { .. } => unreachable!("every line read above"),
        };
        let key = JournalKey::new(Host::Researcher, session);
        let encoded = crate::archive::journal::encode(&key, &lines).unwrap();
        assert_eq!(
            crate::archive::journal::decode(&key, encoded).unwrap(),
            lines
        );
        std::fs::remove_dir_all(&directory).unwrap();
    }

    /// A dataset another run read carries no `dataset_read` in this run's journal, so it is refused rather than
    /// recorded as this run's input.
    #[test]
    fn test_an_experiment_refuses_a_dataset_another_run_read() {
        let directory = std::env::temp_dir().join(format!("fund-study-{}", uuid::Uuid::new_v4()));
        let mut study = Study::open(Label::new("bar counts").unwrap(), &directory).unwrap();
        let elsewhere = RunId::new(uuid::Uuid::new_v4());
        assert!(matches!(
            study.experiment(Parameters::default(), &[&dataset(elsewhere)], Outputs::default()),
            Err(crate::laboratory::StudyError::ReadByAnotherRun { run }) if run == elsewhere
        ));
        assert!(!directory.read_dir().unwrap().any(|entry| {
            entry
                .unwrap()
                .path()
                .extension()
                .is_some_and(|extension| extension == "jsonl")
        }));
        std::fs::remove_dir_all(&directory).unwrap();
    }

    #[test]
    fn test_an_empty_partition_is_refused_rather_than_read() {
        let session = SessionDate::from_date(NaiveDate::from_ymd_opt(2026, 9, 28).unwrap());
        assert!(matches!(
            admit(session, Vec::new()),
            Err(DatasetError::EmptyPartition { session: refused }) if refused == session
        ));
    }

    /// Read-only: one week of the production archive, read twice, under secretspec. The week holds the layout's first
    /// sessions, 2026-09-28 and 09-29, so it always reads something.
    #[tokio::test]
    #[ignore = "reads the live archive and the Alpaca calendar; run deliberately under secretspec"]
    async fn live_a_week_of_daily_bars_is_read_whole_and_reproducibly() {
        let configuration = aws_config::load_from_env().await;
        let archive = Archive::market_data(&configuration).unwrap();
        let alpaca = Alpaca::from_environment(reqwest::Client::new()).unwrap();
        let session =
            |month, day| SessionDate::from_date(NaiveDate::from_ymd_opt(2026, month, day).unwrap());
        let range = SessionRange::new(session(9, 28), session(10, 2)).unwrap();
        let calendar = alpaca.calendar(range).await.unwrap();
        let directory = std::env::temp_dir().join(format!("fund-study-{}", uuid::Uuid::new_v4()));
        let mut study = Study::open(Label::new("live loader check").unwrap(), &directory).unwrap();
        let dataset = daily_bars(&archive, &calendar, range, &mut study)
            .await
            .unwrap();
        let fingerprint = dataset.fingerprint();
        assert!(
            fingerprint.partitions().contains_key(&session(9, 28)),
            "{fingerprint:?}"
        );
        let mut sessions: Vec<SessionDate> = fingerprint.partitions().keys().copied().collect();
        sessions.extend(fingerprint.missing());
        sessions.sort();
        assert_eq!(sessions, calendar.trading_days_in_range(range));
        assert_eq!(
            dataset.bars().keys().collect::<Vec<_>>(),
            fingerprint.partitions().keys().collect::<Vec<_>>()
        );
        for (session, bars) in dataset.bars() {
            assert!(bars.len() > 1000, "{session}: {} bars", bars.len());
        }
        let counts = dataset.series(|bars| Some(bars.len() as f64)).unwrap();
        assert_eq!(
            counts.readings().keys().collect::<Vec<_>>(),
            fingerprint.partitions().keys().collect::<Vec<_>>()
        );
        let again = daily_bars(&archive, &calendar, range, &mut study)
            .await
            .unwrap();
        let journaled: Vec<String> = std::fs::read_dir(&directory)
            .unwrap()
            .map(|entry| std::fs::read_to_string(entry.unwrap().path()).unwrap())
            .collect();
        assert_eq!(journaled.concat().matches("\"dataset_read\"").count(), 2);
        std::fs::remove_dir_all(&directory).unwrap();
        assert_eq!(again.fingerprint(), fingerprint);
        assert_eq!(lineage(&archive, fingerprint).await.unwrap(), []);
        println!(
            "{} sessions read, {} missing, {} bars; {:?}",
            fingerprint.partitions().len(),
            fingerprint.missing().len(),
            dataset.bars().values().map(Vec::len).sum::<usize>(),
            fingerprint.partitions()
        );
    }
}
