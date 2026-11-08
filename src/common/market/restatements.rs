//! Tapes A and B republish an unchanged quote on every half minute; the republication begins no quote, so it is
//! dropped before a fold counts it.

use std::collections::BTreeMap;

use chrono::{DateTime, TimeDelta, Utc};

use super::Symbol;
use super::trade_bars::Tape;

/// How often tapes A and B restate an unchanged quote, on the wall clock's whole and half minute.
pub const RESTATEMENT_CADENCE: TimeDelta = TimeDelta::seconds(30);

/// How far either side of a cadence boundary a restatement may be stamped.
pub const RESTATEMENT_WINDOW: TimeDelta = TimeDelta::milliseconds(50);

/// Each symbol's previous row, against which its next row is read as a restatement or a quote.
#[derive(Debug, Clone)]
pub struct Restatements<Row> {
    previous: BTreeMap<Symbol, Row>,
}

impl<Row> Default for Restatements<Row> {
    fn default() -> Self {
        Self {
            previous: BTreeMap::new(),
        }
    }
}

impl<Row: PartialEq> Restatements<Row> {
    /// Whether `row`, everything a vendor served but its timestamp, repeats `symbol`'s previous row on tapes A or B
    /// within the window of a cadence boundary; `row` becomes the previous row either way.
    pub fn restates(
        &mut self,
        symbol: &Symbol,
        timestamp: DateTime<Utc>,
        tape: Option<Tape>,
        row: Row,
    ) -> bool {
        let repeated = match self.previous.get_mut(symbol) {
            Some(previous) => std::mem::replace(previous, row) == *previous,
            None => {
                self.previous.insert(symbol.clone(), row);
                false
            }
        };
        let restating_tape = match tape {
            Some(Tape::ConsolidatedTape) => true,
            Some(Tape::UnlistedTrading) | None => false,
        };
        repeated && restating_tape && near_boundary(timestamp)
    }

    /// Forgets `symbol`'s previous row, for a row that is no quote, so the row after it is never a restatement.
    pub fn interrupt(&mut self, symbol: &Symbol) {
        self.previous.remove(symbol);
    }
}

/// Whether `timestamp` lies within the window of a cadence boundary, on either side.
fn near_boundary(timestamp: DateTime<Utc>) -> bool {
    let offset = TimeDelta::seconds(
        timestamp
            .timestamp()
            .rem_euclid(RESTATEMENT_CADENCE.num_seconds()),
    ) + TimeDelta::nanoseconds(timestamp.timestamp_subsec_nanos().into());
    offset <= RESTATEMENT_WINDOW || RESTATEMENT_CADENCE - offset <= RESTATEMENT_WINDOW
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;
    use crate::common::market::quote_bars::QuoteFold;
    use crate::common::market::record::Quote;
    use crate::common::market::{Price, Shares};

    /// What the tests compare of a row: its quote's prices and sizes and its tape's letter.
    type Row = (Price, Price, Shares, Shares, char);

    fn instant(text: &str) -> DateTime<Utc> {
        text.parse().unwrap()
    }

    fn tape(letter: char) -> Option<Tape> {
        match letter {
            'A' | 'B' => Some(Tape::ConsolidatedTape),
            'C' => Some(Tape::UnlistedTrading),
            _ => None,
        }
    }

    /// The 14:00 minute's quote count and closing quote's start after folding `rows` through the filter, with how
    /// many it dropped.
    fn fold(rows: &[(&str, f64, char)]) -> (u64, DateTime<Utc>, usize) {
        let mut fold = QuoteFold::new(
            instant("2026-10-02T13:30:00Z"),
            instant("2026-10-02T20:00:00Z"),
        )
        .unwrap();
        let mut restatements = Restatements::<Row>::default();
        let mut dropped = 0;
        for &(at, ask, letter) in rows {
            let quote = Quote::new(
                Symbol::new("ABC").unwrap(),
                instant(at),
                Price::from_dollars(50.25).unwrap(),
                Price::from_dollars(ask).unwrap(),
                Shares::whole(100).unwrap(),
                Shares::whole(200).unwrap(),
            )
            .unwrap();
            let row = (
                quote.bid(),
                quote.ask(),
                quote.bid_size(),
                quote.ask_size(),
                letter,
            );
            match restatements.restates(quote.symbol(), quote.timestamp(), tape(letter), row) {
                true => dropped += 1,
                false => fold.push(&quote),
            }
        }
        let (bars, _) = fold.finish();
        let minute = bars
            .iter()
            .find(|bar| bar.timestamp() == instant("2026-10-02T14:00:00Z"))
            .unwrap();
        (
            minute.sums().quote_count().count(),
            minute.sums().closing().since(),
            dropped,
        )
    }

    #[test]
    fn test_a_tape_a_restatement_on_the_half_minute_is_dropped() {
        assert_eq!(
            fold(&[
                ("2026-10-02T14:00:10Z", 50.27, 'A'),
                ("2026-10-02T14:00:29.995Z", 50.27, 'A'),
            ]),
            (1, instant("2026-10-02T14:00:10Z"), 1)
        );
    }

    #[test]
    fn test_a_quote_that_moves_the_ask_a_tick_is_kept() {
        assert_eq!(
            fold(&[
                ("2026-10-02T14:00:10Z", 50.27, 'A'),
                ("2026-10-02T14:00:29.995Z", 50.28, 'A'),
            ]),
            (2, instant("2026-10-02T14:00:29.995Z"), 0)
        );
    }

    #[test]
    fn test_a_tape_c_repeat_on_the_half_minute_is_kept() {
        assert_eq!(
            fold(&[
                ("2026-10-02T14:00:10Z", 50.27, 'C'),
                ("2026-10-02T14:00:29.995Z", 50.27, 'C'),
            ]),
            (2, instant("2026-10-02T14:00:29.995Z"), 0)
        );
    }

    #[test]
    fn test_a_tape_a_repeat_off_the_boundary_is_kept() {
        assert_eq!(
            fold(&[
                ("2026-10-02T14:00:10Z", 50.27, 'A'),
                ("2026-10-02T14:00:20Z", 50.27, 'A'),
            ]),
            (2, instant("2026-10-02T14:00:20Z"), 0)
        );
    }

    /// The phase drifts either side of the boundary; 51 ms out on either side is a quote.
    #[test]
    fn test_the_window_holds_either_side_of_the_boundary() {
        for (at, dropped) in [
            ("2026-10-02T14:00:29.992Z", 1),
            ("2026-10-02T14:00:30.012Z", 1),
            ("2026-10-02T14:00:29.950Z", 1),
            ("2026-10-02T14:00:30.050Z", 1),
            ("2026-10-02T14:00:29.949Z", 0),
            ("2026-10-02T14:00:30.051Z", 0),
            ("2026-10-02T14:00:59.992Z", 1),
            ("2026-10-02T14:01:00.012Z", 1),
        ] {
            let (_, _, counted) = fold(&[("2026-10-02T14:00:10Z", 50.27, 'B'), (at, 50.27, 'B')]);
            assert_eq!(counted, dropped, "{at}");
        }
    }

    /// A row that is no quote between two identical ones leaves the second a quote.
    #[test]
    fn test_an_interruption_forgets_the_previous_row() {
        let symbol = Symbol::new("ABC").unwrap();
        let mut restatements = Restatements::<u8>::default();
        let boundary = instant("2026-10-02T14:00:30Z");
        assert!(!restatements.restates(&symbol, boundary, tape('A'), 1));
        assert!(restatements.restates(&symbol, boundary, tape('A'), 1));
        restatements.interrupt(&symbol);
        assert!(!restatements.restates(&symbol, boundary, tape('A'), 1));
        assert!(!restatements.restates(&symbol, boundary, None, 1));
    }

    /// Two symbols' rows interleave without reading as each other's predecessor.
    #[test]
    fn test_each_symbol_keeps_its_own_previous_row() {
        let (first, second) = (Symbol::new("ABC").unwrap(), Symbol::new("XYZ").unwrap());
        let mut restatements = Restatements::<u8>::default();
        let boundary = instant("2026-10-02T14:00:30Z");
        assert!(!restatements.restates(&first, boundary, tape('A'), 1));
        assert!(!restatements.restates(&second, boundary, tape('A'), 2));
        assert!(restatements.restates(&first, boundary, tape('A'), 1));
        assert!(!restatements.restates(&second, boundary, tape('A'), 1));
    }

    proptest! {
        /// Every dropped row equals its symbol's previous row, and every repeat on tape A at a boundary is dropped.
        #[test]
        fn test_only_and_every_boundary_repeat_is_dropped(
            rows in prop::collection::vec(
                (0_usize..2, 0_u8..2, prop::sample::select(vec!['A', 'B', 'C', ' ']), prop_oneof![0_i64..60_000, -60_i64..60, 29_940_i64..30_060]),
                1..64,
            ),
        ) {
            let symbols = [Symbol::new("ABC").unwrap(), Symbol::new("XYZ").unwrap()];
            let mut restatements = Restatements::<(u8, char)>::default();
            let mut previous: BTreeMap<usize, (u8, char)> = BTreeMap::new();
            for (symbol, value, letter, millisecond) in rows {
                let at = instant("2026-10-02T14:00:00Z") + TimeDelta::milliseconds(millisecond);
                let dropped = restatements.restates(&symbols[symbol], at, tape(letter), (value, letter));
                let offset = millisecond.rem_euclid(30_000);
                let expected = previous.insert(symbol, (value, letter)) == Some((value, letter))
                    && matches!(letter, 'A' | 'B')
                    && (offset <= 50 || offset >= 29_950);
                prop_assert_eq!(dropped, expected);
            }
        }
    }
}
