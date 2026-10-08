//! The hard pre-trade check between orders and the broker: an order goes out only in a symbol the broker reports
//! tradable, in whole shares where it trades no fraction, and as a fractional buy only when it meets the broker's
//! minimum; anything it cannot vouch for is refused with its cause.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::common::book::Side;
use crate::common::market::{DollarVolume, PRICE_SCALE, Price, SHARE_SCALE, Shares, Symbol};
use crate::common::order::BrokerFailure;
use crate::common::strategy::Order;

/// The least a fractional buy may be worth, one dollar, under which Alpaca refuses it; a fractional sell has no floor.
const LEAST_FRACTIONAL_BUY: DollarVolume =
    DollarVolume::from_units(PRICE_SCALE.unsigned_abs() as u128 * SHARE_SCALE as u128);

/// What the broker reports of a symbol's trading, read before orders go out.
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
    Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, strum::Display, strum::IntoStaticStr,
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
    /// A fractional buy worth less than the broker's minimum at `price`, the price it was valued at.
    BelowMinimum {
        price: Price,
    },
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

/// The tradability the broker reported for every symbol an execution would trade, journaled when there was one.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TradabilityRead {
    readings: BTreeMap<Symbol, Tradability>,
}

impl TradabilityRead {
    pub fn new(readings: BTreeMap<Symbol, Tradability>) -> Self {
        Self { readings }
    }
}

/// A tradability read that failed, journaled with its cause once before every order it leaves unvouched is held.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TradabilityUnread {
    cause: BrokerFailure,
}

impl TradabilityUnread {
    pub fn new(cause: BrokerFailure) -> Self {
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

/// Lets each order through only when `tradability` vouches for its symbol and size. A fractional buy is held under the
/// minimum at `price`; one without a price goes out for the broker to judge.
pub fn guard(
    orders: Vec<Order>,
    tradability: &BTreeMap<Symbol, Tradability>,
    price: impl Fn(&Symbol) -> Option<Price>,
) -> Guarded {
    let mut guarded = Guarded {
        passed: Vec::new(),
        held: Vec::new(),
    };
    for order in orders {
        let whole = order.shares().is_whole();
        let cause = match (
            tradability.get(order.symbol()),
            whole,
            below_minimum(&order, &price),
        ) {
            (Some(Tradability::Fractionable), false, Some(price)) => {
                GuardCause::BelowMinimum { price }
            }
            (Some(Tradability::Fractionable), true | false, _)
            | (Some(Tradability::WholeSharesOnly), true, _) => {
                guarded.passed.push(order);
                continue;
            }
            (Some(Tradability::WholeSharesOnly), false, _) => GuardCause::Fractional,
            (Some(Tradability::Untradable), true | false, _) => GuardCause::Untradable,
            (Some(Tradability::Unlisted), true | false, _) => GuardCause::Unlisted,
            (None, true | false, _) => GuardCause::Unread,
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

/// The symbol's known price when `order` is a buy worth less than the broker's minimum at it.
fn below_minimum(order: &Order, price: impl Fn(&Symbol) -> Option<Price>) -> Option<Price> {
    match order.side() {
        Side::Buy => price(order.symbol())
            .filter(|price| DollarVolume::of(*price, order.shares()) < LEAST_FRACTIONAL_BUY),
        Side::Sell => None,
    }
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

    #[test]
    fn test_tradability_names_agree_between_strum_and_serde() {
        use strum::IntoEnumIterator;

        let names: Vec<&str> = Tradability::iter().map(Into::into).collect();
        assert_eq!(
            names,
            [
                "fractionable",
                "whole_shares_only",
                "untradable",
                "unlisted"
            ]
        );
        for tradability in Tradability::iter() {
            assert_eq!(tradability.to_string().parse(), Ok(tradability));
            assert_eq!(
                serde_json::to_string(&tradability).unwrap(),
                format!("\"{tradability}\"")
            );
        }
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
            let guarded = guard(orders, &tradability, |_| None);
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
        // Without `EnumIter` the list below is by hand, so a new cause must first break this match.
        let _listed = |cause: GuardCause| match cause {
            GuardCause::Untradable
            | GuardCause::Unlisted
            | GuardCause::Fractional
            | GuardCause::Unread
            | GuardCause::BelowMinimum { .. } => (),
        };
        let causes = [
            GuardCause::Untradable,
            GuardCause::Unlisted,
            GuardCause::Fractional,
            GuardCause::Unread,
            GuardCause::BelowMinimum {
                price: Price::from_ticks(12_500_000).unwrap(),
            },
        ];
        let names: Vec<&str> = causes.iter().map(Into::into).collect();
        assert_eq!(
            names,
            [
                "untradable",
                "unlisted",
                "fractional",
                "unread",
                "below_minimum"
            ]
        );
        let written: Vec<String> = causes
            .iter()
            .map(|cause| serde_json::to_string(cause).unwrap())
            .collect();
        assert_eq!(
            written,
            [
                r#""untradable""#,
                r#""unlisted""#,
                r#""fractional""#,
                r#""unread""#,
                r#"{"below_minimum":{"price":12500000}}"#,
            ]
        );
        for (cause, json) in causes.iter().zip(&written) {
            assert_eq!(&serde_json::from_str::<GuardCause>(json).unwrap(), cause);
        }
    }

    /// A fractional buy is held below a dollar at its price and passes at one; a sell, a whole share and an unpriced
    /// buy pass whatever they are worth.
    #[test]
    fn test_a_fractional_buy_under_a_dollar_is_held() {
        let fractionable = BTreeMap::from([(symbol("F"), Tradability::Fractionable)]);
        let price = |_: &Symbol| Some(Price::from_ticks(10_000_000).unwrap());
        let held = |order: Order, price: &dyn Fn(&Symbol) -> Option<Price>| {
            guard(vec![order], &fractionable, price)
                .held()
                .iter()
                .map(OrderGuarded::cause)
                .collect::<Vec<_>>()
        };
        let buy = |units| buys(&[("F", units)]).remove(0);
        assert_eq!(
            held(buy(99_999), &price),
            [GuardCause::BelowMinimum {
                price: Price::from_ticks(10_000_000).unwrap()
            }]
        );
        assert_eq!(held(buy(100_000), &price), []);
        assert_eq!(held(buy(99_999), &|_| None), []);
        let cheap = |_: &Symbol| Some(Price::from_ticks(500_000).unwrap());
        assert_eq!(held(buy(1_000_000), &cheap), []);
        let book = Book::of(
            &crate::common::book::Fill::new(
                "2026-10-07T18:00:00Z".parse().unwrap(),
                symbol("F"),
                Side::Buy,
                Shares::from_units(150_000),
                Price::from_ticks(10_000_000).unwrap(),
                DollarVolume::from_units(0),
            )
            .unwrap(),
        );
        let sell = orders(
            &book,
            &Target::new(BTreeMap::from([(symbol("F"), Shares::from_units(100_000))])),
        )
        .remove(0);
        assert_eq!(sell.side(), Side::Sell);
        assert_eq!(held(sell, &price), []);
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
            let guarded = guard(orders.clone(), &tradability, |_| None);
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
