//! The daily risk-free rate a Sharpe takes off: the 3-month Treasury yield, an annual percentage, spread evenly over a
//! trading year, and dated so each session reads only a yield published before it opened.

use std::collections::BTreeMap;

use chrono::NaiveDate;

use crate::common::laboratory::series::{Series, SeriesRefusal};
use crate::common::laboratory::sharpe::SESSIONS_PER_YEAR;
use crate::common::time::SessionDate;

/// A yield as an annual percentage, `4.17` being 4.17% a year; finite and at most 100 in size, which no Treasury
/// yield has come near.
#[derive(Debug, Clone, Copy, PartialEq, PartialOrd)]
pub struct AnnualYield(f64);

/// A yield that is not finite or past 100% either way, with the value read.
#[derive(Debug, Clone, Copy, PartialEq, thiserror::Error)]
#[error("{value} is not an annual yield in percent")]
pub struct YieldRefusal {
    pub value: f64,
}

impl AnnualYield {
    pub fn new(percent: f64) -> Result<Self, YieldRefusal> {
        match (-100.0..=100.0).contains(&percent) {
            true => Ok(Self(percent)),
            false => Err(YieldRefusal { value: percent }),
        }
    }

    pub fn percent(self) -> f64 {
        self.0
    }

    /// The rate a session earns at this yield: the annual fraction over the sessions in a year, uncompounded, since
    /// the difference is far below the noise of any Sharpe it enters.
    pub fn daily(self) -> f64 {
        self.0 / 100.0 / SESSIONS_PER_YEAR
    }
}

/// The oldest a yield may be and still stand for a session: a long weekend and a holiday, with room to spare.
pub const MAXIMUM_YIELD_AGE_DAYS: i64 = 7;

/// Each session's daily rate from the latest yield dated before it, since a day's yield is published after it ends;
/// a session with no earlier yield, or only one older than `MAXIMUM_YIELD_AGE_DAYS`, is unmeasured.
pub fn daily_rates(
    yields: &BTreeMap<NaiveDate, AnnualYield>,
    sessions: impl IntoIterator<Item = SessionDate>,
) -> Result<Series, SeriesRefusal> {
    Series::new(sessions.into_iter().map(|session| {
        let rate = yields
            .range(..session.date())
            .next_back()
            .filter(|(published, _)| {
                (session.date() - **published).num_days() <= MAXIMUM_YIELD_AGE_DAYS
            })
            .map(|(_, annual)| annual.daily());
        (session, rate)
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn date(day: u32) -> NaiveDate {
        NaiveDate::from_ymd_opt(2026, 11, day).unwrap()
    }

    #[test]
    fn test_a_yield_is_spread_over_the_trading_year() {
        // 5.04% a year over 252 sessions is 2bp a session.
        assert!((AnnualYield::new(5.04).unwrap().daily() - 0.0002).abs() < 1e-15);
        assert!(AnnualYield::new(f64::NAN).unwrap_err().value.is_nan());
        assert_eq!(AnnualYield::new(150.0), Err(YieldRefusal { value: 150.0 }));
    }

    #[test]
    fn test_a_yield_older_than_a_week_stands_for_no_session() {
        let yields = BTreeMap::from([(date(2), AnnualYield::new(5.04).unwrap())]);
        let rates = daily_rates(
            &yields,
            [9, 10].map(|day| SessionDate::from_date(date(day))),
        )
        .unwrap();
        let read: Vec<(String, Option<f64>)> = rates
            .readings()
            .iter()
            .map(|(session, rate)| {
                (
                    session.date().to_string(),
                    rate.map(|rate| (rate * 1e6).round() / 1e6),
                )
            })
            .collect();
        // Seven days after the 2nd still reads it; eight days does not.
        assert_eq!(
            read,
            [
                ("2026-11-09".to_string(), Some(0.0002)),
                ("2026-11-10".to_string(), None)
            ]
        );
        assert_eq!(MAXIMUM_YIELD_AGE_DAYS, 7);
    }

    #[test]
    fn test_a_session_reads_the_latest_yield_published_before_it() {
        // Yields for Thursday the 5th and Friday the 6th; Monday the 9th reads Friday's, Friday reads Thursday's.
        let yields = BTreeMap::from([
            (date(5), AnnualYield::new(2.52).unwrap()),
            (date(6), AnnualYield::new(5.04).unwrap()),
        ]);
        let sessions = [5, 6, 9].map(|day| SessionDate::from_date(date(day)));
        let rates = daily_rates(&yields, sessions).unwrap();
        let sessions: Vec<String> = rates
            .readings()
            .keys()
            .map(|session| session.date().to_string())
            .collect();
        assert_eq!(sessions, ["2026-11-05", "2026-11-06", "2026-11-09"]);
        let read: Vec<Option<f64>> = rates.readings().values().copied().collect();
        assert_eq!(read[0], None);
        assert!((read[1].unwrap() - 0.0001).abs() < 1e-15);
        assert!((read[2].unwrap() - 0.0002).abs() < 1e-15);
    }
}
