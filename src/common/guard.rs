//! The hard pre-trade check between orders and the broker: an order goes out only in a symbol the broker reports
//! tradable, and in whole shares where it trades no fraction; anything it cannot vouch for is refused with its cause.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::common::book::Side;
use crate::common::market::{SHARE_SCALE, Shares, Symbol};
use crate::common::strategy::Order;

/// What the broker reports of a symbol's trading, read before orders go out.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Tradability {
    /// Trades in any amount, fractions included.
    Fractionable,
    /// Trades in whole shares only.
    WholeSharesOnly,
    /// Listed, but inactive or reported not open to orders.
    Untradable,
    /// Not listed at the broker at all.
    Unlisted,
}

/// Why the guard held an order back.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    strum::Display,
    strum::EnumString,
    strum::IntoStaticStr,
    strum::EnumIter,
)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum GuardCause {
    Untradable,
    Unlisted,
    /// The symbol trades whole shares only and the order holds a fraction.
    Fractional,
    /// No reading of the symbol was taken, so nothing vouches for it.
    Unread,
}

/// An order the guard held back, journaled so the gap between a target and the book it reached is explained.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrderGuarded {
    symbol: Symbol,
    side: Side,
    shares: Shares,
    cause: GuardCause,
}

impl OrderGuarded {
    pub fn cause(&self) -> GuardCause {
        self.cause
    }
}

/// A tradability read that failed, journaled with its cause once before every order it leaves unvouched is held.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TradabilityUnread {
    cause: String,
}

impl TradabilityUnread {
    pub fn new(cause: String) -> Self {
        Self { cause }
    }
}

/// `orders` split into those free to go out, in their order, and those held back.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Guarded {
    passed: Vec<Order>,
    held: Vec<OrderGuarded>,
}

impl Guarded {
    pub fn passed(&self) -> &[Order] {
        &self.passed
    }

    pub fn held(&self) -> &[OrderGuarded] {
        &self.held
    }
}

/// Lets each order through only when `tradability` vouches for its symbol and size.
pub fn guard(orders: Vec<Order>, tradability: &BTreeMap<Symbol, Tradability>) -> Guarded {
    let mut guarded = Guarded {
        passed: Vec::new(),
        held: Vec::new(),
    };
    for order in orders {
        let whole = order.shares().units() % SHARE_SCALE == 0;
        let cause = match (tradability.get(order.symbol()), whole) {
            (Some(Tradability::Fractionable), true | false)
            | (Some(Tradability::WholeSharesOnly), true) => {
                guarded.passed.push(order);
                continue;
            }
            (Some(Tradability::WholeSharesOnly), false) => GuardCause::Fractional,
            (Some(Tradability::Untradable), true | false) => GuardCause::Untradable,
            (Some(Tradability::Unlisted), true | false) => GuardCause::Unlisted,
            (None, true | false) => GuardCause::Unread,
        };
        guarded.held.push(OrderGuarded {
            symbol: order.symbol().clone(),
            side: order.side(),
            shares: order.shares(),
            cause,
        });
    }
    guarded
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;
    use crate::common::book::Book;
    use crate::common::strategy::{Target, orders};

    fn symbol(raw: &str) -> Symbol {
        Symbol::new(raw).unwrap()
    }

    /// Buys from an empty book, one per symbol, in the symbol order `orders` gives.
    fn buys(wanted: &[(&str, u64)]) -> Vec<Order> {
        orders(
            &Book::default(),
            &Target::new(
                wanted
                    .iter()
                    .map(|(raw, units)| (symbol(raw), Shares::from_units(*units)))
                    .collect(),
            ),
        )
    }

    /// Each reading against a whole and a fractional order, as the paper account reported SPY, VWDRY and SSUNF on
    /// 2026-10-06, with an unlisted and an unread symbol.
    #[test]
    fn test_each_reading_lets_through_only_what_it_vouches_for() {
        let tradability = BTreeMap::from([
            (symbol("SPY"), Tradability::Fractionable),
            (symbol("VWDRY"), Tradability::WholeSharesOnly),
            (symbol("SSUNF"), Tradability::Untradable),
            (symbol("ZZZZ"), Tradability::Unlisted),
        ]);
        for (units, passed, held) in [
            (
                2_000_000,
                vec!["SPY", "VWDRY"],
                vec![
                    ("QQQ", GuardCause::Unread),
                    ("SSUNF", GuardCause::Untradable),
                    ("ZZZZ", GuardCause::Unlisted),
                ],
            ),
            (
                1_500_000,
                vec!["SPY"],
                vec![
                    ("QQQ", GuardCause::Unread),
                    ("SSUNF", GuardCause::Untradable),
                    ("VWDRY", GuardCause::Fractional),
                    ("ZZZZ", GuardCause::Unlisted),
                ],
            ),
        ] {
            let orders = buys(&[
                ("SPY", units),
                ("VWDRY", units),
                ("SSUNF", units),
                ("ZZZZ", units),
                ("QQQ", units),
            ]);
            let guarded = guard(orders, &tradability);
            let symbols: Vec<&str> = guarded
                .passed()
                .iter()
                .map(|order| order.symbol().as_str())
                .collect();
            assert_eq!(symbols, passed, "{units}");
            let causes: Vec<(&str, GuardCause)> = guarded
                .held()
                .iter()
                .map(|held| (held.symbol.as_str(), held.cause()))
                .collect();
            assert_eq!(causes, held, "{units}");
        }
    }

    /// Names agree between strum and serde for every cause.
    #[test]
    fn test_guard_causes_read_back_as_written() {
        use strum::IntoEnumIterator;

        let causes: Vec<&str> = GuardCause::iter().map(Into::into).collect();
        assert_eq!(causes, ["untradable", "unlisted", "fractional", "unread"]);
        for cause in GuardCause::iter() {
            assert_eq!(
                serde_json::to_string(&cause).unwrap(),
                format!("\"{cause}\"")
            );
            assert_eq!(cause.to_string().parse(), Ok(cause));
        }
    }

    proptest! {
        /// Every order is either passed or held, never both or neither, and the passed keep their order.
        #[test]
        fn property_the_guard_partitions_the_orders(
            wanted in prop::collection::btree_map(
                prop::sample::select(vec!["AAPL", "MSFT", "SPY", "QQQ", "VWDRY"]),
                1..5_000_000u64,
                0..5,
            ),
            readings in prop::collection::vec(
                prop::sample::select(vec![
                    Tradability::Fractionable,
                    Tradability::WholeSharesOnly,
                    Tradability::Untradable,
                    Tradability::Unlisted,
                ]),
                5,
            ),
        ) {
            let wanted: Vec<(&str, u64)> = wanted.into_iter().collect();
            let tradability: BTreeMap<Symbol, Tradability> = ["AAPL", "MSFT", "SPY", "QQQ"]
                .into_iter()
                .zip(readings)
                .map(|(raw, reading)| (symbol(raw), reading))
                .collect();
            let orders = buys(&wanted);
            let guarded = guard(orders.clone(), &tradability);
            let mut sorted: Vec<&Symbol> = guarded
                .passed()
                .iter()
                .map(Order::symbol)
                .chain(guarded.held().iter().map(|held| &held.symbol))
                .collect();
            sorted.sort();
            prop_assert_eq!(sorted, orders.iter().map(Order::symbol).collect::<Vec<_>>());
            let mut passed = guarded.passed().iter();
            let mut next = passed.next();
            for order in &orders {
                if next == Some(order) {
                    next = passed.next();
                }
            }
            prop_assert_eq!(next, None);
        }
    }
}
