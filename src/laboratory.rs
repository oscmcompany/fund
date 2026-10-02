//! Runs studies. A study is measured only here, and only after its run is journaled, so no reading exists that the
//! journal does not hold.

pub mod dataset;

use std::io;

use chrono::Utc;

use crate::common::journal::{Observation, RunId};
use crate::common::laboratory::{HoldoutRefusal, Study, StudyRan};
use crate::journal::Journal;

#[derive(Debug)]
pub enum RunError {
    Journal(io::Error),
    Holdout(HoldoutRefusal),
}

impl std::fmt::Display for RunError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Journal(error) => write!(formatter, "the journal failed: {error}"),
            Self::Holdout(refusal) => write!(formatter, "{refusal}"),
        }
    }
}

impl std::error::Error for RunError {}

/// Every study the journal's directory holds, with the run that measured it.
pub fn studies_ran(journal: &Journal) -> io::Result<Vec<(RunId, StudyRan)>> {
    Ok(journal
        .history()?
        .into_iter()
        .filter_map(|record| match record.observation() {
            Observation::StudyRan(ran) => Some((record.run_id(), (**ran).clone())),
            Observation::ConfigurationResolved(_)
            | Observation::PartitionWritten(_)
            | Observation::HealFinished(_) => None,
        })
        .collect())
}

/// Measures `study` and returns the reading once its `study_ran` record is durable. A registered study is first held
/// against the exploratory runs this journal holds, read here so no caller can hand in a shorter history.
pub fn run(study: Study, journal: &mut Journal) -> Result<StudyRan, RunError> {
    let history = studies_ran(journal).map_err(RunError::Journal)?;
    study
        .holdout(history.iter().map(|(run, ran)| (*run, ran)))
        .map_err(RunError::Holdout)?;
    let ran = study.measure();
    journal
        .append(Utc::now(), Observation::StudyRan(Box::new(ran.clone())))
        .map_err(RunError::Journal)?;
    Ok(ran)
}

#[cfg(test)]
mod tests {
    use chrono::NaiveDate;
    use uuid::Uuid;

    use super::*;
    use crate::common::heal::Leg;
    use crate::common::journal::{ReadLine, RunId, read};
    use crate::common::laboratory::dataset::Fingerprint;
    use crate::common::laboratory::{
        Arm, Direction, Exploration, KillLine, Lane, Null, Overlap, Pairing, Quantity, Source,
    };
    use crate::common::register::{Accession, AccessionNumber, Bid, Family, Opening};
    use crate::common::time::SessionDate;
    use crate::common::time::calendar::{TradingCalendar, TradingSession};

    #[test]
    fn test_a_run_returns_only_what_the_journal_holds() {
        let session = |day| {
            SessionDate::from_date(NaiveDate::from_ymd_opt(2026, 3, 2).unwrap())
                .plus_calendar_days(day)
        };
        let arm = |name, values: [f64; 3]| {
            Arm::new(
                name,
                (0..3).map(|day| (session(day), Some(values[day as usize]))),
                30,
            )
            .unwrap()
        };
        let lane = Lane::Exploratory(
            Exploration::new(
                Family::Overnight,
                "liquid-common@1".parse().unwrap(),
                "1 sessions".parse().unwrap(),
                "does the gap persist",
                KillLine::new(0.5, Direction::Higher).unwrap(),
            )
            .unwrap(),
        );
        let quantity = Quantity::Unpriced {
            units: "net-bp".parse().unwrap(),
        };
        let study = Study::new(
            lane,
            Source::Synthetic {
                description: "three sessions written in the test".to_string(),
            },
            quantity,
            Pairing::Matched,
            arm("gap-top-decile", [1.0, 2.0, 4.0]),
            arm("gap-bottom-decile", [0.0, 1.0, 1.0]),
        )
        .unwrap();
        let directory = std::env::temp_dir().join(format!("fund-laboratory-{}", Uuid::new_v4()));
        let mut journal = Journal::open(&directory, RunId::new(Uuid::new_v4())).unwrap();
        let ran = run(study, &mut journal).unwrap();
        let file = std::fs::read_dir(&directory)
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .path();
        let held: Vec<Observation> = read(&std::fs::read_to_string(file).unwrap())
            .into_iter()
            .map(|line| match line {
                ReadLine::Read(record) => record.observation().clone(),
                ReadLine::Unreadable { line, cause, .. } => panic!("line {line}: {cause:?}"),
            })
            .collect();
        assert_eq!(held, [Observation::StudyRan(Box::new(ran.clone()))]);
        assert_eq!(held[0].event_type(), "study_ran");
        assert_eq!(ran.survives_kill_line(), Some(true));
        std::fs::remove_dir_all(&directory).unwrap();
    }

    /// A second run in the same journal directory is refused over sessions the first explored, and admitted past them;
    /// a torn line and a foreign file in the directory do not stop the history being read.
    #[test]
    fn test_a_registered_run_is_refused_over_data_this_journal_explored() {
        let session = |day| {
            SessionDate::from_date(NaiveDate::from_ymd_opt(2026, 3, 2).unwrap())
                .plus_calendar_days(day)
        };
        let (open, close) = (
            chrono::NaiveTime::from_hms_opt(9, 30, 0).unwrap(),
            chrono::NaiveTime::from_hms_opt(16, 0, 0).unwrap(),
        );
        let calendar = TradingCalendar::new(
            (0..10)
                .map(|day| TradingSession::new(session(day), open, close).unwrap())
                .collect(),
            session(0),
            session(9),
        )
        .unwrap();
        let study = |lane, days: std::ops::Range<i64>| {
            let read = days
                .clone()
                .map(|day| (session(day), format!("\"tag-{day}\"")))
                .collect();
            let fingerprint = Fingerprint::new(
                Leg::MassiveDailyBars,
                session(0),
                session(9),
                &calendar,
                read,
            )
            .unwrap();
            let arm = |name| {
                Arm::new(
                    name,
                    days.clone().map(|day| (session(day), Some(day as f64))),
                    10,
                )
                .unwrap()
            };
            Study::new(
                lane,
                Source::Archive { fingerprint },
                Quantity::Unpriced {
                    units: "net-bp".parse().unwrap(),
                },
                Pairing::Matched,
                arm("treatment"),
                arm("control"),
            )
            .unwrap()
        };
        let exploration = Lane::Exploratory(
            Exploration::new(
                Family::Overnight,
                "liquid-common@1".parse().unwrap(),
                "1 sessions".parse().unwrap(),
                "does the gap persist",
                KillLine::new(0.0, Direction::Higher).unwrap(),
            )
            .unwrap(),
        );
        let registered = || {
            let opening = Opening::new(
                Family::Overnight,
                "liquid-common@1".parse().unwrap(),
                "1 sessions".parse().unwrap(),
                "the gap persists".to_string(),
                Bid::Unrecorded,
                session(0),
                None,
                None,
            )
            .unwrap();
            Lane::Registered {
                accession: Accession::open(AccessionNumber::new(1).unwrap(), opening)
                    .study()
                    .unwrap(),
                null: Null::Omitted {
                    reason: "a journal seam test".to_string(),
                },
            }
        };
        let directory = std::env::temp_dir().join(format!("fund-laboratory-{}", Uuid::new_v4()));
        let explorer = RunId::new(Uuid::new_v4());
        run(
            study(exploration, 0..4),
            &mut Journal::open(&directory, explorer).unwrap(),
        )
        .unwrap();
        std::fs::write(directory.join("notes.txt"), "not a journal").unwrap();
        let mut torn = std::fs::OpenOptions::new()
            .append(true)
            .open(
                std::fs::read_dir(&directory)
                    .unwrap()
                    .map(|entry| entry.unwrap().path())
                    .find(|path| {
                        path.extension()
                            .is_some_and(|extension| extension == "jsonl")
                    })
                    .unwrap(),
            )
            .unwrap();
        std::io::Write::write_all(&mut torn, b"{\"torn").unwrap();
        let mut journal = Journal::open(&directory, RunId::new(Uuid::new_v4())).unwrap();
        match run(study(registered(), 2..6), &mut journal) {
            Err(RunError::Holdout(refusal)) => assert_eq!(
                refusal.overlaps,
                [Overlap {
                    run: explorer,
                    sessions: vec![session(2), session(3)]
                }]
            ),
            other => panic!("{other:?}"),
        }
        assert!(run(study(registered(), 4..8), &mut journal).is_ok());
        assert_eq!(studies_ran(&journal).unwrap().len(), 2);
        std::fs::remove_dir_all(&directory).unwrap();
    }
}
