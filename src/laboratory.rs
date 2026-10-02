//! Runs studies. A study is measured only here, and only after its run is journaled, so no reading exists that the
//! journal does not hold.

use std::io;

use chrono::Utc;

use crate::common::journal::Observation;
use crate::common::laboratory::{Study, StudyRan};
use crate::journal::Journal;

/// Measures `study` and returns the reading once its `study_ran` record is durable.
pub fn run(study: Study, journal: &mut Journal) -> io::Result<StudyRan> {
    let ran = study.measure();
    journal.append(Utc::now(), Observation::StudyRan(Box::new(ran.clone())))?;
    Ok(ran)
}

#[cfg(test)]
mod tests {
    use chrono::NaiveDate;
    use uuid::Uuid;

    use super::*;
    use crate::common::journal::{ReadLine, RunId, read};
    use crate::common::laboratory::{Arm, Exploration, Lane, Pairing, Quantity};
    use crate::common::register::Family;
    use crate::common::time::SessionDate;

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
                0.5,
            )
            .unwrap(),
        );
        let quantity = Quantity::Unpriced {
            units: "net-bp".parse().unwrap(),
        };
        let study = Study::new(
            lane,
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
}
