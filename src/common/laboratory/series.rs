//! One reading per session, the object every study reduces its data to before anything is estimated.

use std::collections::BTreeMap;

use crate::common::laboratory::READING_BOUND;
use crate::common::time::SessionDate;

/// Readings by session; `None` is a session that was read and could not be measured.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Series {
    readings: BTreeMap<SessionDate, Option<f64>>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum SeriesRefusal {
    /// NaN, or beyond `READING_BOUND`, where sums and squares would stop being finite.
    OutOfRange {
        session: SessionDate,
        value: f64,
    },
    ReadTwice {
        session: SessionDate,
    },
}

impl std::fmt::Display for SeriesRefusal {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::OutOfRange { session, value } => {
                write!(
                    formatter,
                    "the reading for {session} is out of range: {value}"
                )
            }
            Self::ReadTwice { session } => write!(formatter, "{session} was read twice"),
        }
    }
}

impl std::error::Error for SeriesRefusal {}

impl Series {
    pub fn new(
        readings: impl IntoIterator<Item = (SessionDate, Option<f64>)>,
    ) -> Result<Self, SeriesRefusal> {
        let mut held = BTreeMap::new();
        for (session, reading) in readings {
            if held.insert(session, admit(session, reading)?).is_some() {
                return Err(SeriesRefusal::ReadTwice { session });
            }
        }
        Ok(Self { readings: held })
    }

    pub fn readings(&self) -> &BTreeMap<SessionDate, Option<f64>> {
        &self.readings
    }

    /// Applies `function` to every measured reading; an unmeasured session stays unmeasured.
    pub fn map(&self, function: impl Fn(f64) -> f64) -> Result<Self, SeriesRefusal> {
        Self::new(
            self.readings
                .iter()
                .map(|(session, reading)| (*session, reading.map(&function))),
        )
    }

    /// Combines the readings of the sessions both series read; a session either could not measure stays unmeasured.
    pub fn zip_with(
        &self,
        other: &Self,
        function: impl Fn(f64, f64) -> f64,
    ) -> Result<Self, SeriesRefusal> {
        Self::new(self.readings.iter().filter_map(|(session, left)| {
            let right = other.readings.get(session)?;
            Some((
                *session,
                left.zip(*right).map(|(left, right)| function(left, right)),
            ))
        }))
    }

    /// Both series' sessions together, refused where they share one, so partitions read apart can be joined.
    pub fn concatenate(&self, other: &Self) -> Result<Self, SeriesRefusal> {
        Self::new(
            self.readings
                .iter()
                .chain(&other.readings)
                .map(|(session, reading)| (*session, *reading)),
        )
    }
}

fn admit(session: SessionDate, reading: Option<f64>) -> Result<Option<f64>, SeriesRefusal> {
    match reading {
        Some(value) if value.is_nan() || value.abs() > READING_BOUND => {
            Err(SeriesRefusal::OutOfRange { session, value })
        }
        Some(_) | None => Ok(reading),
    }
}

#[cfg(test)]
mod tests {
    use chrono::NaiveDate;
    use proptest::prelude::*;

    use super::*;

    fn session(day: i64) -> SessionDate {
        SessionDate::from_date(NaiveDate::from_ymd_opt(2026, 3, 2).unwrap()).plus_calendar_days(day)
    }

    fn series(first_day: i64, readings: &[Option<f64>]) -> Series {
        Series::new(
            readings
                .iter()
                .enumerate()
                .map(|(day, reading)| (session(first_day + day as i64), *reading)),
        )
        .unwrap()
    }

    #[test]
    fn test_a_series_refuses_what_it_could_not_have_read() {
        assert_eq!(
            Series::new([(session(0), Some(f64::NAN))]).map_err(|refusal| refusal.to_string()),
            Err("the reading for 2026-03-02 is out of range: NaN".to_string())
        );
        assert_eq!(
            Series::new([(session(1), Some(-1.1e50))]),
            Err(SeriesRefusal::OutOfRange {
                session: session(1),
                value: -1.1e50
            })
        );
        assert_eq!(
            Series::new([(session(2), Some(1.0)), (session(2), None)]),
            Err(SeriesRefusal::ReadTwice {
                session: session(2)
            })
        );
        assert!(Series::new([(session(0), Some(1e50)), (session(1), Some(-1e50))]).is_ok());
        assert_eq!(
            series(0, &[Some(1e50)]).map(|value| value * 2.0),
            Err(SeriesRefusal::OutOfRange {
                session: session(0),
                value: 2e50
            })
        );
    }

    #[test]
    fn test_zip_keeps_only_shared_sessions_and_their_gaps() {
        let left = series(0, &[Some(5.0), None, Some(7.0), Some(8.0)]);
        let right = series(1, &[Some(1.0), Some(2.0), None, Some(9.0)]);
        assert_eq!(
            left.zip_with(&right, |left, right| left - right).unwrap(),
            series(1, &[None, Some(5.0), None])
        );
    }

    #[test]
    fn test_concatenation_refuses_a_shared_session() {
        assert_eq!(
            series(0, &[Some(1.0), None]).concatenate(&series(1, &[Some(2.0)])),
            Err(SeriesRefusal::ReadTwice {
                session: session(1)
            })
        );
        assert_eq!(
            series(0, &[Some(1.0)])
                .concatenate(&series(1, &[None]))
                .unwrap(),
            series(0, &[Some(1.0), None])
        );
    }

    fn readings() -> impl Strategy<Value = Vec<Option<f64>>> {
        prop::collection::vec(prop::option::of(-1000.0..1000.0f64), 0..30)
    }

    proptest! {
        #[test]
        fn test_map_preserves_identity(readings in readings()) {
            let original = series(0, &readings);
            prop_assert_eq!(original.map(|value| value).unwrap(), original);
        }

        #[test]
        fn test_map_preserves_composition(readings in readings()) {
            let (first, second) = (|value: f64| value * 3.0 - 1.0, |value: f64| value.abs().sqrt());
            let original = series(0, &readings);
            prop_assert_eq!(
                original.map(first).unwrap().map(second).unwrap(),
                original.map(|value| second(first(value))).unwrap()
            );
        }

        /// The empty series is concatenation's identity, and concatenating disjoint pieces does not depend on grouping.
        #[test]
        fn test_concatenation_is_a_monoid(first in readings(), second in readings(), third in readings()) {
            let (first, second, third) = (
                series(0, &first),
                series(100, &second),
                series(200, &third),
            );
            prop_assert_eq!(first.concatenate(&Series::default()).unwrap(), first.clone());
            prop_assert_eq!(Series::default().concatenate(&first).unwrap(), first.clone());
            prop_assert_eq!(
                first.concatenate(&second).unwrap().concatenate(&third).unwrap(),
                first.concatenate(&second.concatenate(&third).unwrap()).unwrap()
            );
        }
    }
}
