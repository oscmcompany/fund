//! The book the journal's fills fold into, read against the book the broker reports, which is authoritative: positions
//! must agree exactly, and cash within what rounding the broker's average prices can explain.

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

use crate::common::book::{Book, Cash, Fill, Position};
use crate::common::market::Symbol;

/// Where the expected and reported books differ, or that they agree; journaled either way as `book_reconciled`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BookReconciled {
    expected_cash: Cash,
    reported_cash: Cash,
    allowance: Allowance,
    gaps: Vec<PositionGap>,
}

/// How far cash may differ and still agree, in cash units; unsigned, so it is never negative.
// Written as a decimal string, since serde's buffer for a tagged journal record cannot hold a u128.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct Allowance(u128);

impl Allowance {
    /// Cash must agree exactly.
    pub const NONE: Self = Self(0);
}

impl TryFrom<String> for Allowance {
    type Error = std::num::ParseIntError;

    fn try_from(raw: String) -> Result<Self, Self::Error> {
        raw.parse().map(Self)
    }
}

impl From<Allowance> for String {
    fn from(allowance: Allowance) -> Self {
        allowance.0.to_string()
    }
}

/// A symbol whose reported position differs from the expected one.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PositionGap {
    symbol: Symbol,
    expected: Position,
    reported: Position,
}

impl PositionGap {
    pub fn symbol(&self) -> &Symbol {
        &self.symbol
    }
}

impl BookReconciled {
    /// Positions agree exactly and cash within the allowance.
    pub fn agrees(&self) -> bool {
        self.gaps.is_empty()
            && self
                .reported_cash
                .units()
                .abs_diff(self.expected_cash.units())
                <= self.allowance.0
    }

    pub fn gaps(&self) -> &[PositionGap] {
        &self.gaps
    }
}

/// `expected` read against `reported`, with cash allowed to differ by `allowance`.
pub fn reconcile(expected: &Book, reported: &Book, allowance: Allowance) -> BookReconciled {
    let symbols = expected
        .positions()
        .keys()
        .chain(reported.positions().keys())
        .collect::<BTreeSet<_>>();
    let gaps = symbols
        .into_iter()
        .filter_map(|symbol| {
            let (expected, reported) = (expected.position(symbol), reported.position(symbol));
            (expected != reported).then(|| PositionGap {
                symbol: symbol.clone(),
                expected,
                reported,
            })
        })
        .collect();
    BookReconciled {
        expected_cash: expected.cash(),
        reported_cash: reported.cash(),
        allowance,
        gaps,
    }
}

/// Half a cent in cash units, the most rounding a fill's cash to the cent can move it.
const HALF_CENT: u128 = 5_000_000_000;

/// How far the journal's cash can stray from the broker's when each fill is priced at the broker's average rounded
/// to the nearest tick, half a tick a share unit, and the broker books each fill's cash to the cent, half a cent more.
pub fn rounding_allowance<'a>(fills: impl IntoIterator<Item = &'a Fill>) -> Allowance {
    Allowance(
        fills
            .into_iter()
            .map(|fill| u128::from(fill.shares().units().div_ceil(2)) + HALF_CENT)
            .sum(),
    )
}

#[cfg(test)]
mod tests {
    use chrono::{DateTime, Utc};
    use proptest::prelude::*;

    use super::*;
    use crate::common::book::Side;
    use crate::common::market::{DollarVolume, Price, Shares};

    fn symbol(raw: &str) -> Symbol {
        Symbol::new(raw).unwrap()
    }

    fn holding(cash: i128, positions: &[(&str, i128)]) -> Book {
        Book::reported(
            Cash::from_units(cash),
            positions
                .iter()
                .map(|(raw, units)| (symbol(raw), Position::from_units(*units))),
        )
    }

    /// Cash may differ by exactly the allowance and still agree; one unit more diverges, as does any position gap.
    #[test]
    fn test_books_agree_within_the_allowance_and_on_every_position() {
        let expected = holding(1_000, &[("SPY", 1_000_000)]);
        let allowance = Allowance(10);
        assert!(reconcile(&expected, &holding(1_010, &[("SPY", 1_000_000)]), allowance).agrees());
        assert!(reconcile(&expected, &holding(990, &[("SPY", 1_000_000)]), allowance).agrees());
        assert!(!reconcile(&expected, &holding(1_011, &[("SPY", 1_000_000)]), allowance).agrees());
        assert!(!reconcile(&expected, &holding(989, &[("SPY", 1_000_000)]), allowance).agrees());
        let reported = holding(1_000, &[("AAPL", -500_000), ("SPY", 2_000_000)]);
        let reading = reconcile(&expected, &reported, allowance);
        assert!(!reading.agrees());
        assert_eq!(
            reading.gaps(),
            [
                PositionGap {
                    symbol: symbol("AAPL"),
                    expected: Position::from_units(0),
                    reported: Position::from_units(-500_000),
                },
                PositionGap {
                    symbol: symbol("SPY"),
                    expected: Position::from_units(1_000_000),
                    reported: Position::from_units(2_000_000),
                },
            ]
        );
    }

    /// Cash at the two ends of the integer's range differs by more than any i128 holds, and still reads as diverged
    /// rather than overflowing.
    #[test]
    fn test_cash_at_the_range_ends_diverges_without_overflow() {
        let reading = reconcile(
            &holding(i128::MIN, &[]),
            &holding(i128::MAX, &[]),
            Allowance(u128::MAX - 1),
        );
        assert!(!reading.agrees());
        assert!(
            reconcile(
                &holding(i128::MIN, &[]),
                &holding(i128::MAX, &[]),
                Allowance(u128::MAX)
            )
            .agrees()
        );
    }

    /// Three shares and one and a half: half a unit a share unit, rounded up, is 1,500,000 and 750,001, and each fill
    /// adds half a cent, 5,000,000,000 units.
    #[test]
    fn test_the_allowance_is_half_a_tick_a_share_unit_and_half_a_cent_a_fill() {
        let fill = |units| {
            Fill::new(
                "2026-10-06T16:00:00Z".parse::<DateTime<Utc>>().unwrap(),
                symbol("SPY"),
                Side::Buy,
                Shares::from_units(units),
                Price::from_ticks(780_680_000).unwrap(),
                DollarVolume::default(),
            )
            .unwrap()
        };
        assert_eq!(
            rounding_allowance(&[fill(3_000_000), fill(1_500_001)]),
            Allowance(10_002_250_001)
        );
        assert_eq!(rounding_allowance(&[]), Allowance::NONE);
    }

    proptest! {
        #[test]
        fn property_allowance_round_trips_through_its_decimal_string(units in any::<u128>()) {
            let written = serde_json::to_string(&Allowance(units)).unwrap();
            prop_assert_eq!(&written, &format!("\"{units}\""));
            prop_assert_eq!(serde_json::from_str::<Allowance>(&written).unwrap(), Allowance(units));
        }

        /// A book agrees with itself at no allowance, and the gaps between two books name exactly the symbols whose
        /// positions differ.
        #[test]
        fn property_gaps_name_exactly_the_differing_positions(
            cash in any::<i64>(),
            left in prop::collection::vec(-5..5i128, 3),
            right in prop::collection::vec(-5..5i128, 3),
        ) {
            let symbols = ["AAPL", "SPY", "QQQ"];
            let book = |units: &[i128]| holding(
                i128::from(cash),
                &symbols.iter().zip(units).map(|(raw, units)| (*raw, *units)).collect::<Vec<_>>(),
            );
            let (left_book, right_book) = (book(&left), book(&right));
            prop_assert!(reconcile(&left_book, &left_book, Allowance::NONE).agrees());
            let reading = reconcile(&left_book, &right_book, Allowance::NONE);
            let named: BTreeSet<&str> = reading.gaps().iter().map(|gap| gap.symbol().as_str()).collect();
            let differing: BTreeSet<&str> = symbols
                .iter()
                .zip(left.iter().zip(&right))
                .filter(|(_, (left, right))| left != right)
                .map(|(raw, _)| *raw)
                .collect();
            prop_assert_eq!(named, differing);
        }
    }
}
