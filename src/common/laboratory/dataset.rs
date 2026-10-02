//! What a study read: one archive series over a window of sessions, each partition with the version read, so a later
//! rewrite of any of them is visible against the record.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::common::time::SessionDate;
use crate::common::time::calendar::TradingCalendar;

/// The partitions a study read, by session with the entity tag of the version read, and every trading session in the
/// window that had none.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Fingerprint {
    series: String,
    first: SessionDate,
    last: SessionDate,
    partitions: BTreeMap<SessionDate, String>,
    /// Trading sessions in the window with no partition, so an absence is read as one rather than as a short window.
    missing: Vec<SessionDate>,
}

/// Why a fingerprint could not be taken.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FingerprintRefusal {
    Inverted {
        first: SessionDate,
        last: SessionDate,
    },
    CalendarShort {
        first: SessionDate,
        last: SessionDate,
    },
    /// A partition for a day the calendar does not trade, or outside the window.
    NotATradingSession { session: SessionDate },
}

impl std::fmt::Display for FingerprintRefusal {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Inverted { first, last } => write!(
                formatter,
                "the window {first} to {last} ends before it starts"
            ),
            Self::CalendarShort { first, last } => {
                write!(formatter, "the calendar does not cover {first} to {last}")
            }
            Self::NotATradingSession { session } => {
                write!(
                    formatter,
                    "a partition for {session}, which is not a trading session in the window"
                )
            }
        }
    }
}

impl Fingerprint {
    /// `partitions` holds what was found; every other trading session from `first` to `last` is recorded missing.
    pub fn new(
        series: impl Into<String>,
        first: SessionDate,
        last: SessionDate,
        calendar: &TradingCalendar,
        partitions: BTreeMap<SessionDate, String>,
    ) -> Result<Self, FingerprintRefusal> {
        if last < first {
            return Err(FingerprintRefusal::Inverted { first, last });
        }
        if !calendar.covers(first, last) {
            return Err(FingerprintRefusal::CalendarShort { first, last });
        }
        let sessions = calendar.trading_days_in_range(first, last);
        if let Some(session) = partitions
            .keys()
            .find(|session| !sessions.contains(session))
        {
            return Err(FingerprintRefusal::NotATradingSession { session: *session });
        }
        let missing = sessions
            .into_iter()
            .filter(|session| !partitions.contains_key(session))
            .collect();
        Ok(Self {
            series: series.into(),
            first,
            last,
            partitions,
            missing,
        })
    }

    pub fn series(&self) -> &str {
        &self.series
    }

    pub fn partitions(&self) -> &BTreeMap<SessionDate, String> {
        &self.partitions
    }

    pub fn missing(&self) -> &[SessionDate] {
        &self.missing
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use chrono::{NaiveDate, NaiveTime};

    use crate::common::time::calendar::TradingSession;

    fn session(day: u32) -> SessionDate {
        SessionDate::from_date(NaiveDate::from_ymd_opt(2026, 9, day).unwrap())
    }

    /// 2026-09-21 to 2026-09-27: Monday to Friday trade.
    fn calendar() -> TradingCalendar {
        let open = NaiveTime::from_hms_opt(9, 30, 0).unwrap();
        let close = NaiveTime::from_hms_opt(16, 0, 0).unwrap();
        TradingCalendar::new(
            (21..=25)
                .map(|day| TradingSession::new(session(day), open, close).unwrap())
                .collect(),
            session(21),
            session(27),
        )
        .unwrap()
    }

    fn tags(days: &[u32]) -> BTreeMap<SessionDate, String> {
        days.iter()
            .map(|day| (session(*day), format!("\"tag-{day}\"")))
            .collect()
    }

    #[test]
    fn test_every_trading_session_is_read_or_named_missing() {
        let fingerprint = Fingerprint::new(
            "data/bars/",
            session(21),
            session(27),
            &calendar(),
            tags(&[21, 22, 25]),
        )
        .unwrap();
        assert_eq!(
            fingerprint.partitions().keys().copied().collect::<Vec<_>>(),
            [session(21), session(22), session(25)]
        );
        assert_eq!(fingerprint.missing(), [session(23), session(24)]);
        assert_eq!(fingerprint.series(), "data/bars/");
    }

    #[test]
    fn test_a_fingerprint_refuses_what_it_could_not_have_read() {
        assert_eq!(
            Fingerprint::new("s", session(25), session(21), &calendar(), tags(&[])),
            Err(FingerprintRefusal::Inverted {
                first: session(25),
                last: session(21)
            })
        );
        assert_eq!(
            Fingerprint::new("s", session(21), session(28), &calendar(), tags(&[])),
            Err(FingerprintRefusal::CalendarShort {
                first: session(21),
                last: session(28)
            })
        );
        // Saturday, and a session outside the window.
        for day in [26, 25] {
            assert_eq!(
                Fingerprint::new("s", session(21), session(24), &calendar(), tags(&[21, day])),
                Err(FingerprintRefusal::NotATradingSession {
                    session: session(day)
                })
            );
        }
    }

    #[test]
    fn test_a_fingerprint_reads_back_as_written() {
        let fingerprint = Fingerprint::new(
            "data/bars/",
            session(21),
            session(25),
            &calendar(),
            tags(&[21, 24]),
        )
        .unwrap();
        let encoded = serde_json::to_string(&fingerprint).unwrap();
        assert_eq!(
            serde_json::from_str::<Fingerprint>(&encoded).unwrap(),
            fingerprint
        );
    }
}
