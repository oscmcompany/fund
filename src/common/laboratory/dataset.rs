//! What a study read: one archive series over a window of sessions, each partition with the version read, so a later
//! rewrite of any of them is visible against the record.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::common::time::SessionDate;
use crate::common::time::calendar::TradingCalendar;

/// One archive series a study can read, under the name the journal stores.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    Serialize,
    Deserialize,
    strum::Display,
    strum::EnumString,
    strum::IntoStaticStr,
    strum::EnumIter,
)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum DatasetLeg {
    MassiveDailyBars,
}

/// The partitions a study read, by session with the entity tag of the version read, and every trading session in the
/// window that had none.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(try_from = "FingerprintFields")]
pub struct Fingerprint {
    leg: DatasetLeg,
    first: SessionDate,
    last: SessionDate,
    partitions: BTreeMap<SessionDate, String>,
    /// Trading sessions in the window with no partition, so an absence is read as one rather than as a short window.
    missing: Vec<SessionDate>,
}

#[derive(Deserialize)]
struct FingerprintFields {
    leg: DatasetLeg,
    first: SessionDate,
    last: SessionDate,
    partitions: BTreeMap<SessionDate, String>,
    missing: Vec<SessionDate>,
}

impl TryFrom<FingerprintFields> for Fingerprint {
    type Error = FingerprintRefusal;

    /// Checks what holds without a calendar; that every session trades was checked when the fingerprint was taken.
    fn try_from(fields: FingerprintFields) -> Result<Self, Self::Error> {
        let (first, last) = (fields.first, fields.last);
        if last < first {
            return Err(FingerprintRefusal::Inverted { first, last });
        }
        let outside = |session: &&SessionDate| **session < first || last < **session;
        if let Some(session) = fields
            .partitions
            .keys()
            .chain(&fields.missing)
            .find(outside)
        {
            return Err(FingerprintRefusal::NotATradingSession { session: *session });
        }
        if let Some(session) = fields
            .missing
            .iter()
            .find(|session| fields.partitions.contains_key(session))
        {
            return Err(FingerprintRefusal::ReadAndMissing { session: *session });
        }
        if !fields.missing.is_sorted() {
            return Err(FingerprintRefusal::MissingUnordered);
        }
        Ok(Self {
            leg: fields.leg,
            first,
            last,
            partitions: fields.partitions,
            missing: fields.missing,
        })
    }
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
    NotATradingSession {
        session: SessionDate,
    },
    ReadAndMissing {
        session: SessionDate,
    },
    MissingUnordered,
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
            Self::ReadAndMissing { session } => {
                write!(formatter, "{session} is both read and missing")
            }
            Self::MissingUnordered => write!(formatter, "the missing sessions are out of order"),
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
    /// `partitions` holds what was found; every other trading session from `first` to `last` is recorded missing. Taken
    /// only by the crate's loaders, so a study cannot vouch for partitions nothing read.
    pub(crate) fn new(
        leg: DatasetLeg,
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
            leg,
            first,
            last,
            partitions,
            missing,
        })
    }

    pub fn leg(&self) -> DatasetLeg {
        self.leg
    }

    pub fn partitions(&self) -> &BTreeMap<SessionDate, String> {
        &self.partitions
    }

    pub fn missing(&self) -> &[SessionDate] {
        &self.missing
    }

    /// Every partition read whose version is no longer the one read; `current` holds each partition's tag now, and a
    /// partition absent from it is gone. A study reading one of these rests on data that has since been rewritten.
    pub fn contaminated(&self, current: &BTreeMap<SessionDate, String>) -> Vec<Contamination> {
        self.partitions
            .iter()
            .filter(|(session, read)| current.get(session) != Some(read))
            .map(|(session, read)| Contamination {
                session: *session,
                read: read.clone(),
                now: current.get(session).cloned(),
            })
            .collect()
    }
}

/// A partition read under one version that now holds another, or is gone.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Contamination {
    pub session: SessionDate,
    pub read: String,
    /// `None` when the partition is gone.
    pub now: Option<String>,
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
            DatasetLeg::MassiveDailyBars,
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
        assert_eq!(fingerprint.leg(), DatasetLeg::MassiveDailyBars);
    }

    #[test]
    fn test_a_fingerprint_refuses_what_it_could_not_have_read() {
        assert_eq!(
            Fingerprint::new(
                DatasetLeg::MassiveDailyBars,
                session(25),
                session(21),
                &calendar(),
                tags(&[])
            ),
            Err(FingerprintRefusal::Inverted {
                first: session(25),
                last: session(21)
            })
        );
        assert_eq!(
            Fingerprint::new(
                DatasetLeg::MassiveDailyBars,
                session(21),
                session(28),
                &calendar(),
                tags(&[])
            ),
            Err(FingerprintRefusal::CalendarShort {
                first: session(21),
                last: session(28)
            })
        );
        // Saturday, and a session outside the window.
        for day in [26, 25] {
            assert_eq!(
                Fingerprint::new(
                    DatasetLeg::MassiveDailyBars,
                    session(21),
                    session(24),
                    &calendar(),
                    tags(&[21, day])
                ),
                Err(FingerprintRefusal::NotATradingSession {
                    session: session(day)
                })
            );
        }
    }

    #[test]
    fn test_a_partition_rewritten_or_gone_since_it_was_read_is_contaminated() {
        let fingerprint = Fingerprint::new(
            DatasetLeg::MassiveDailyBars,
            session(21),
            session(25),
            &calendar(),
            tags(&[21, 22, 24]),
        )
        .unwrap();
        let mut current = tags(&[21, 22, 24, 25]);
        assert_eq!(fingerprint.contaminated(&current), []);
        current.insert(session(22), "\"rewritten\"".to_string());
        current.remove(&session(24));
        assert_eq!(
            fingerprint.contaminated(&current),
            [
                Contamination {
                    session: session(22),
                    read: "\"tag-22\"".to_string(),
                    now: Some("\"rewritten\"".to_string())
                },
                Contamination {
                    session: session(24),
                    read: "\"tag-24\"".to_string(),
                    now: None
                }
            ]
        );
    }

    #[test]
    fn test_a_stored_fingerprint_must_agree_with_its_own_window() {
        let stored = serde_json::to_value(
            Fingerprint::new(
                DatasetLeg::MassiveDailyBars,
                session(21),
                session(25),
                &calendar(),
                tags(&[21, 24]),
            )
            .unwrap(),
        )
        .unwrap();
        let mut outside = stored.clone();
        outside["partitions"]["2026-09-28"] = serde_json::json!("\"tag\"");
        let mut both = stored.clone();
        both["missing"] = serde_json::json!(["2026-09-21", "2026-09-22"]);
        let mut unordered = stored.clone();
        unordered["missing"] = serde_json::json!(["2026-09-23", "2026-09-22"]);
        let mut inverted = stored;
        inverted["last"] = serde_json::json!("2026-09-20");
        for refused in [outside, both, unordered, inverted] {
            assert!(
                serde_json::from_value::<Fingerprint>(refused.clone()).is_err(),
                "{refused}"
            );
        }
    }

    #[test]
    fn test_a_fingerprint_reads_back_as_written() {
        let fingerprint = Fingerprint::new(
            DatasetLeg::MassiveDailyBars,
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

    /// Each leg keeps the name journals already hold, and serde and strum agree on it.
    #[test]
    fn test_a_dataset_leg_round_trips_under_its_stored_name() {
        use strum::IntoEnumIterator;
        assert_eq!(
            DatasetLeg::iter().map(<&str>::from).collect::<Vec<_>>(),
            ["massive_daily_bars"]
        );
        for leg in DatasetLeg::iter() {
            let stored = serde_json::to_value(leg).unwrap();
            assert_eq!(stored, serde_json::json!(leg.to_string()));
            assert_eq!(serde_json::from_value::<DatasetLeg>(stored).unwrap(), leg);
            assert_eq!(leg.to_string().parse::<DatasetLeg>().unwrap(), leg);
        }
    }
}
