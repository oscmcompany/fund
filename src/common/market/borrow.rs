//! What the broker reports of each name's short sale, and the rule a backtest reads it by: a snapshot is fetched after
//! the close of the session it is dated by, so a session reads the latest snapshot dated before it.

use std::collections::BTreeMap;

use crate::common::market::Symbol;
use crate::common::time::SessionDate;

/// Whether a name can be sold short on a session.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    strum::Display,
    strum::EnumString,
    strum::IntoStaticStr,
    strum::EnumIter,
)]
#[strum(serialize_all = "snake_case")]
pub enum Borrow {
    Available,
    Unavailable,
}

/// One name's short-sale flags as Alpaca's asset listing reports them, both kept since the broker can report either
/// without the other.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BorrowFlags {
    symbol: Symbol,
    shortable: bool,
    easy_to_borrow: bool,
}

impl BorrowFlags {
    pub fn new(symbol: Symbol, shortable: bool, easy_to_borrow: bool) -> Self {
        Self {
            symbol,
            shortable,
            easy_to_borrow,
        }
    }

    pub fn symbol(&self) -> &Symbol {
        &self.symbol
    }

    pub fn shortable(&self) -> bool {
        self.shortable
    }

    pub fn easy_to_borrow(&self) -> bool {
        self.easy_to_borrow
    }

    /// Available only when the name is both shortable and easy to borrow, since Alpaca shorts no other kind.
    pub fn borrow(&self) -> Borrow {
        match (self.shortable, self.easy_to_borrow) {
            (true, true) => Borrow::Available,
            (true, false) | (false, true) | (false, false) => Borrow::Unavailable,
        }
    }
}

/// Every name's flags in one snapshot, by symbol.
pub type BorrowSnapshot = BTreeMap<Symbol, BorrowFlags>;

/// What `symbol` could be borrowed as on `session`, read from the latest snapshot dated before it; a name that
/// snapshot does not list is not an active asset and cannot be shorted. `None` when no snapshot predates the session,
/// since nothing was recorded then.
pub fn borrow_on(
    snapshots: &BTreeMap<SessionDate, BorrowSnapshot>,
    session: SessionDate,
    symbol: &Symbol,
) -> Option<Borrow> {
    let (_, snapshot) = snapshots.range(..session).next_back()?;
    Some(
        snapshot
            .get(symbol)
            .map_or(Borrow::Unavailable, BorrowFlags::borrow),
    )
}

#[cfg(test)]
mod tests {
    use chrono::NaiveDate;
    use strum::IntoEnumIterator;

    use super::*;

    fn symbol(raw: &str) -> Symbol {
        Symbol::new(raw).unwrap()
    }

    fn session(day: u32) -> SessionDate {
        SessionDate::from_date(NaiveDate::from_ymd_opt(2026, 11, day).unwrap())
    }

    #[test]
    fn test_a_name_is_borrowable_only_when_shortable_and_easy_to_borrow() {
        let read = |shortable, easy| BorrowFlags::new(symbol("AAA"), shortable, easy).borrow();
        assert_eq!(
            [
                read(true, true),
                read(true, false),
                read(false, true),
                read(false, false)
            ],
            [
                Borrow::Available,
                Borrow::Unavailable,
                Borrow::Unavailable,
                Borrow::Unavailable
            ]
        );
        let names: Vec<&str> = Borrow::iter().map(Into::into).collect();
        assert_eq!(names, ["available", "unavailable"]);
        for borrow in Borrow::iter() {
            assert_eq!(borrow.to_string().parse(), Ok(borrow));
        }
    }

    #[test]
    fn test_a_session_reads_the_latest_snapshot_dated_before_it() {
        let snapshot = |easy| {
            BorrowSnapshot::from([(symbol("AAA"), BorrowFlags::new(symbol("AAA"), true, easy))])
        };
        // Fetched after the closes of the 5th and the 6th: the 6th trades on the 5th's, the 9th on the 6th's.
        let snapshots =
            BTreeMap::from([(session(5), snapshot(true)), (session(6), snapshot(false))]);
        assert_eq!(borrow_on(&snapshots, session(5), &symbol("AAA")), None);
        assert_eq!(
            borrow_on(&snapshots, session(6), &symbol("AAA")),
            Some(Borrow::Available)
        );
        assert_eq!(
            borrow_on(&snapshots, session(9), &symbol("AAA")),
            Some(Borrow::Unavailable)
        );
        assert_eq!(
            borrow_on(&snapshots, session(9), &symbol("ZZZ")),
            Some(Borrow::Unavailable)
        );
    }
}
