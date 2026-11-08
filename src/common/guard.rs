//! The hard pre-trade check between orders and the broker: an order goes out only in a symbol the broker reports
//! tradable, in whole shares where it trades no fraction, as a short only where it lends whole shares easily, and as a
//! fractional buy only when it meets the broker's minimum; anything it cannot vouch for is refused with its cause.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use crate::common::book::{Book, Side};
use crate::common::market::{DollarVolume, PRICE_SCALE, Price, SHARE_SCALE, Shares, Symbol};
use crate::common::order::BrokerFailure;
use crate::common::risk::DollarLimit;
use crate::common::strategy::{Leg, Order, Target, orders};

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
    WholeSharesOnly,
    /// Listed, but inactive or reported not open to orders.
    Untradable,
    Unlisted,
}

/// What the broker reports of a symbol's short sale, read beside its tradability.
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
pub enum Shortability {
    EasyToBorrow,
    /// Shortable only through a locate, which Alpaca does not take.
    HardToBorrow,
    Unshortable,
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
    /// The order opens or adds to a short in a symbol the broker reports not shortable.
    Unshortable,
    /// The order opens or adds to a short in a symbol the broker reports hard to borrow.
    HardToBorrow,
    /// The order opens beyond zero and the close to flat before it was held or did not fill whole.
    Unclosed,
    /// The order opens or adds to a holding worth `notional` at `price`, past the per-name limit.
    BeyondPerName {
        price: Price,
        notional: DollarVolume,
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
    pub fn new(order: &Order, cause: GuardCause) -> Self {
        Self {
            symbol: order.symbol().clone(),
            side: order.side(),
            shares: order.shares(),
            cause,
        }
    }

    pub fn cause(&self) -> GuardCause {
        self.cause
    }
}

/// The tradability the broker reported for every symbol an execution would trade, and the shortability of each it
/// lists, journaled when there was one; the record's time is when the borrow was read.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TradabilityRead {
    readings: BTreeMap<Symbol, Tradability>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    shortability: BTreeMap<Symbol, Shortability>,
}

impl TradabilityRead {
    pub fn new(
        readings: BTreeMap<Symbol, Tradability>,
        shortability: BTreeMap<Symbol, Shortability>,
    ) -> Self {
        Self {
            readings,
            shortability,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.readings.is_empty()
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

/// The symbols whose reducing leg fell short of flat, where an order opening beyond zero would cross it.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Unclosed {
    symbols: BTreeSet<Symbol>,
}

impl Unclosed {
    /// Whether `order` opens beyond zero in a symbol whose close fell short.
    pub fn holds(&self, order: &Order) -> bool {
        match order.leg() {
            Leg::Increasing => self.symbols.contains(order.symbol()),
            Leg::Reducing => false,
        }
    }

    /// Records `order`'s symbol as unclosed when it reduced and did not go out or fill whole.
    pub fn record(&mut self, order: &Order, whole: bool) {
        match (order.leg(), whole) {
            (Leg::Reducing, false) => {
                self.symbols.insert(order.symbol().clone());
            }
            (Leg::Reducing, true) | (Leg::Increasing, true | false) => {}
        }
    }
}

/// Lets each of the orders taking `book` to `target` through only when `read` vouches for its symbol and size, and for
/// its borrow when it opens or adds to a short; a cover is never held for borrow. A fractional buy is held under the
/// minimum at `price`; one without a price goes out for the broker to judge. An order opening beyond zero is held as
/// unclosed after its symbol's close was held, and an order adding to a holding is held when the holding after it, read
/// from `book` and the orders passed before it, would pass `per_name` at `price`.
pub fn guard(
    book: &Book,
    target: &Target,
    read: &TradabilityRead,
    per_name: DollarLimit,
    price: impl Fn(&Symbol) -> Option<Price>,
) -> Guarded {
    let mut guarded = Guarded {
        passed: Vec::new(),
        held: Vec::new(),
    };
    let mut unclosed = Unclosed::default();
    // Each symbol's position once the orders passed before this one fill.
    let mut reached: BTreeMap<Symbol, i128> = BTreeMap::new();
    for order in orders(book, target) {
        let before = *reached
            .entry(order.symbol().clone())
            .or_insert_with(|| book.position(order.symbol()).units());
        let shares = i128::from(order.shares().units());
        let resulting = match order.side() {
            Side::Buy => before.saturating_add(shares),
            Side::Sell => before.saturating_sub(shares),
        };
        let cause = match unclosed.holds(&order) {
            true => Some(GuardCause::Unclosed),
            false => cause(&order, resulting, read, per_name, &price),
        };
        unclosed.record(&order, cause.is_none());
        match cause {
            None => {
                reached.insert(order.symbol().clone(), resulting);
                guarded.passed.push(order);
            }
            Some(cause) => guarded.held.push(OrderGuarded::new(&order, cause)),
        }
    }
    guarded
}

/// Why `order` is held, or `None` when `read` vouches for it.
fn cause(
    order: &Order,
    resulting: i128,
    read: &TradabilityRead,
    per_name: DollarLimit,
    price: impl Fn(&Symbol) -> Option<Price>,
) -> Option<GuardCause> {
    let whole = order.shares().is_whole();
    match (read.readings.get(order.symbol()), whole) {
        (Some(Tradability::Fractionable), true | false)
        | (Some(Tradability::WholeSharesOnly), true) => {}
        (Some(Tradability::WholeSharesOnly), false) => return Some(GuardCause::Fractional),
        (Some(Tradability::Untradable), true | false) => return Some(GuardCause::Untradable),
        (Some(Tradability::Unlisted), true | false) => return Some(GuardCause::Unlisted),
        (None, true | false) => return Some(GuardCause::Unread),
    }
    if let Some(cause) = beyond_per_name(order, resulting, per_name, &price) {
        return Some(cause);
    }
    if order.borrows() {
        // A broker lends no fraction of a share, whatever it trades.
        match (read.shortability.get(order.symbol()), whole) {
            (Some(Shortability::EasyToBorrow), true) => {}
            (Some(Shortability::EasyToBorrow), false) => return Some(GuardCause::Fractional),
            (Some(Shortability::HardToBorrow), true | false) => {
                return Some(GuardCause::HardToBorrow);
            }
            (Some(Shortability::Unshortable), true | false) => {
                return Some(GuardCause::Unshortable);
            }
            (None, true | false) => return Some(GuardCause::Unread),
        }
    }
    match whole {
        true => None,
        false => below_minimum(order, price).map(|price| GuardCause::BelowMinimum { price }),
    }
}

/// The cause holding an order that opens or adds to a holding worth more than `per_name` at its known price once the
/// position is `resulting` millionths; a reducing order is never held for its size, so a holding past the limit can
/// always be cut.
fn beyond_per_name(
    order: &Order,
    resulting: i128,
    per_name: DollarLimit,
    price: impl Fn(&Symbol) -> Option<Price>,
) -> Option<GuardCause> {
    match order.leg() {
        Leg::Increasing => {
            let price = price(order.symbol())?;
            // A holding past one `u64` of millionths is past any limit, so saturating there holds it all the same.
            let shares =
                Shares::from_units(u64::try_from(resulting.unsigned_abs()).unwrap_or(u64::MAX));
            let notional = DollarVolume::of(price, shares);
            (notional.units() > per_name.units())
                .then_some(GuardCause::BeyondPerName { price, notional })
        }
        Leg::Reducing => None,
    }
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

    use chrono::TimeDelta;

    use super::*;
    use crate::common::book::{Book, Cash, Fill, Position};
    use crate::common::monoid::{Monoid, concatenate};
    use crate::common::problem::formulate;
    use crate::common::risk::{Limits, NameLosses};
    use crate::common::solver::Solver;
    use crate::common::strategy::Holding;
    use crate::common::time::calendar::SessionPhase;

    fn symbol(raw: &str) -> Symbol {
        Symbol::new(raw).unwrap()
    }

    /// A per-name limit of a billion dollars, which no order here reaches.
    fn wide() -> DollarLimit {
        DollarLimit::new(Cash::from_units(1_000_000_000 * 1_000_000_000_000)).unwrap()
    }

    /// A read of `readings` with no shortability, so any order that borrows is held as unread.
    fn unshorted(readings: &BTreeMap<Symbol, Tradability>) -> TradabilityRead {
        TradabilityRead::new(readings.clone(), BTreeMap::new())
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

    /// A target of `units` millionths long of each named symbol.
    fn longs(wanted: &[(&str, u64)]) -> Target {
        Target::new(
            wanted
                .iter()
                .map(|(raw, units)| (symbol(raw), Shares::from_units(*units)))
                .collect(),
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
            let target = longs(&[
                ("SPY", units),
                ("VWDRY", units),
                ("SSUNF", units),
                ("ZZZZ", units),
                ("QQQ", units),
            ]);
            let guarded = guard(
                &Book::default(),
                &target,
                &unshorted(&tradability),
                wide(),
                |_| None,
            );
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
            | GuardCause::BelowMinimum { .. }
            | GuardCause::Unshortable
            | GuardCause::HardToBorrow
            | GuardCause::Unclosed
            | GuardCause::BeyondPerName { .. } => (),
        };
        let causes = [
            GuardCause::Untradable,
            GuardCause::Unlisted,
            GuardCause::Fractional,
            GuardCause::Unread,
            GuardCause::BelowMinimum {
                price: Price::from_ticks(12_500_000).unwrap(),
            },
            GuardCause::Unshortable,
            GuardCause::HardToBorrow,
            GuardCause::Unclosed,
            GuardCause::BeyondPerName {
                price: Price::from_ticks(12_500_000).unwrap(),
                notional: DollarVolume::from_units(25_000_000_000_000),
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
                "below_minimum",
                "unshortable",
                "hard_to_borrow",
                "unclosed",
                "beyond_per_name"
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
                r#""unshortable""#,
                r#""hard_to_borrow""#,
                r#""unclosed""#,
                r#"{"beyond_per_name":{"price":12500000,"notional":"25000000000000"}}"#,
            ]
        );
        for (cause, json) in causes.iter().zip(&written) {
            assert_eq!(&serde_json::from_str::<GuardCause>(json).unwrap(), cause);
        }
    }

    #[test]
    fn test_shortability_names_agree_between_strum_and_serde() {
        use strum::IntoEnumIterator;

        let names: Vec<&str> = Shortability::iter().map(Into::into).collect();
        assert_eq!(names, ["easy_to_borrow", "hard_to_borrow", "unshortable"]);
        for shortability in Shortability::iter() {
            assert_eq!(shortability.to_string().parse(), Ok(shortability));
            assert_eq!(
                serde_json::to_string(&shortability).unwrap(),
                format!("\"{shortability}\"")
            );
        }
    }

    /// A short opens or grows only where the broker lends whole shares easily, and a cover goes out whatever the borrow
    /// reads: from flat, 2 short each of EASY, HARD, NONE and MUTE; from 2 short, flat in HARD, NONE and
    /// MUTE; and from half a share short, 2 short in HALF.
    #[test]
    fn test_a_short_goes_out_only_where_easy_to_borrow_and_a_cover_always_does() {
        let names = ["EASY", "HARD", "NONE", "MUTE", "HALF"];
        let tradability = names
            .iter()
            .map(|raw| (symbol(raw), Tradability::Fractionable))
            .collect();
        let shortability = BTreeMap::from([
            (symbol("EASY"), Shortability::EasyToBorrow),
            (symbol("HARD"), Shortability::HardToBorrow),
            (symbol("NONE"), Shortability::Unshortable),
            (symbol("HALF"), Shortability::EasyToBorrow),
        ]);
        let read = TradabilityRead::new(tradability, shortability);
        let short = || Holding::short(Shares::whole(2).unwrap()).unwrap();
        let causes = |book: &Book, target: Target| {
            let guarded = guard(book, &target, &read, wide(), |_| None);
            let passed: Vec<(String, Side)> = guarded
                .passed()
                .iter()
                .map(|order| (order.symbol().to_string(), order.side()))
                .collect();
            let held: Vec<(String, GuardCause)> = guarded
                .held()
                .iter()
                .map(|held| (held.symbol.to_string(), held.cause()))
                .collect();
            (passed, held)
        };
        let owned = |pairs: &[(&str, Side)]| -> Vec<(String, Side)> {
            pairs
                .iter()
                .map(|(raw, side)| (raw.to_string(), *side))
                .collect()
        };
        let opening = Target::new(
            ["EASY", "HARD", "NONE", "MUTE"]
                .into_iter()
                .map(|raw| (symbol(raw), short()))
                .collect(),
        );
        assert_eq!(
            causes(&Book::default(), opening),
            (
                owned(&[("EASY", Side::Sell)]),
                vec![
                    ("HARD".to_string(), GuardCause::HardToBorrow),
                    ("MUTE".to_string(), GuardCause::Unread),
                    ("NONE".to_string(), GuardCause::Unshortable),
                ]
            )
        );
        let shorted = Book::reported(
            Cash::default(),
            ["HARD", "NONE", "MUTE"]
                .into_iter()
                .map(|raw| (symbol(raw), Position::from_units(-2_000_000))),
        );
        assert_eq!(
            causes(&shorted, Target::default()),
            (
                owned(&[
                    ("HARD", Side::Buy),
                    ("MUTE", Side::Buy),
                    ("NONE", Side::Buy)
                ]),
                vec![]
            )
        );
        let half = Book::reported(
            Cash::default(),
            [(symbol("HALF"), Position::from_units(-500_000))],
        );
        assert_eq!(
            causes(
                &half,
                Target::new(BTreeMap::from([(symbol("HALF"), short())]))
            ),
            (
                owned(&[]),
                vec![("HALF".to_string(), GuardCause::Fractional)]
            )
        );
    }

    /// From half a share long to 2 short in a name that trades no fraction, the close of the half is held as fractional
    /// and the open of 2 behind it as unclosed, so nothing crosses zero; a second name with nothing to close opens.
    #[test]
    fn test_an_open_behind_a_held_close_is_held_as_unclosed() {
        let names = ["PART", "OPEN"];
        let read = TradabilityRead::new(
            names
                .iter()
                .map(|raw| (symbol(raw), Tradability::WholeSharesOnly))
                .collect(),
            names
                .iter()
                .map(|raw| (symbol(raw), Shortability::EasyToBorrow))
                .collect(),
        );
        let book = Book::reported(
            Cash::default(),
            [(symbol("PART"), Position::from_units(500_000))],
        );
        let target = Target::new(
            names
                .iter()
                .map(|raw| {
                    (
                        symbol(raw),
                        Holding::short(Shares::whole(2).unwrap()).unwrap(),
                    )
                })
                .collect(),
        );
        let guarded = guard(&book, &target, &read, wide(), |_| None);
        let passed: Vec<(&str, u64)> = guarded
            .passed()
            .iter()
            .map(|order| (order.symbol().as_str(), order.shares().units()))
            .collect();
        assert_eq!(passed, [("OPEN", 2_000_000)]);
        let held: Vec<(&str, u64, GuardCause)> = guarded
            .held()
            .iter()
            .map(|held| (held.symbol.as_str(), held.shares.units(), held.cause()))
            .collect();
        assert_eq!(
            held,
            [
                ("PART", 500_000, GuardCause::Fractional),
                ("PART", 2_000_000, GuardCause::Unclosed),
            ]
        );
    }

    /// An order opening or adding to a holding is held past the per-name limit and passes at it, and an order cutting a
    /// holding passes whatever it is worth: under a $100 limit at $10, 10 shares bought or shorted pass, 10.000001 are
    /// held with their $100.00001, an unpriced 11 pass for the broker to judge, and 11 sold from 20 held pass; the limit
    /// reads the holding after the order, so 2 more on 9 long or 9 short are held where 1 more on 9, or 10 long past 2
    /// short, pass where 11 long past 2 short is held; past one order's units, a close of u64::MAX millionths passes
    /// where the open of 11 short beyond it does not.
    #[test]
    fn test_an_order_past_the_per_name_limit_is_held_unless_it_cuts() {
        let limit = DollarLimit::new(Cash::from_units(100 * 1_000_000_000_000)).unwrap();
        let ten = |_: &Symbol| Some(Price::from_ticks(10_000_000).unwrap());
        let read = TradabilityRead::new(
            BTreeMap::from([(symbol("A"), Tradability::Fractionable)]),
            BTreeMap::from([(symbol("A"), Shortability::EasyToBorrow)]),
        );
        let causes = |book: &Book, wanted: Holding, price: &dyn Fn(&Symbol) -> Option<Price>| {
            let target = Target::new(BTreeMap::from([(symbol("A"), wanted)]));
            guard(book, &target, &read, limit, price)
                .held()
                .iter()
                .map(OrderGuarded::cause)
                .collect::<Vec<_>>()
        };
        let long = |units| Holding::from(Shares::from_units(units));
        let short = |count| Holding::short(Shares::whole(count).unwrap()).unwrap();
        let flat = Book::default();
        assert_eq!(causes(&flat, long(10_000_000), &ten), []);
        assert_eq!(causes(&flat, short(10), &ten), []);
        assert_eq!(
            causes(&flat, long(10_000_001), &ten),
            [GuardCause::BeyondPerName {
                price: Price::from_ticks(10_000_000).unwrap(),
                notional: DollarVolume::from_units(100_000_010_000_000),
            }]
        );
        assert_eq!(causes(&flat, long(11_000_000), &|_| None), []);
        let held = |units| {
            Book::reported(
                Cash::default(),
                [(symbol("A"), Position::from_units(units))],
            )
        };
        assert_eq!(causes(&held(20_000_000), long(9_000_000), &ten), []);
        let eleven = [GuardCause::BeyondPerName {
            price: Price::from_ticks(10_000_000).unwrap(),
            notional: DollarVolume::from_units(110_000_000_000_000),
        }];
        assert_eq!(causes(&held(9_000_000), long(11_000_000), &ten), eleven);
        assert_eq!(causes(&held(-9_000_000), short(11), &ten), eleven);
        assert_eq!(causes(&held(9_000_000), long(10_000_000), &ten), []);
        assert_eq!(causes(&held(-2_000_000), long(10_000_000), &ten), []);
        assert_eq!(causes(&held(-2_000_000), long(11_000_000), &ten), eleven);
        assert_eq!(
            causes(&held(i128::from(u64::MAX)), short(11), &ten),
            [GuardCause::BeyondPerName {
                price: Price::from_ticks(10_000_000).unwrap(),
                notional: DollarVolume::from_units(110_000_000_000_000),
            }]
        );
    }

    /// A fractional buy is held below a dollar at its price and passes at one; a sell, a whole share and an unpriced
    /// buy pass whatever they are worth.
    #[test]
    fn test_a_fractional_buy_under_a_dollar_is_held() {
        let fractionable = BTreeMap::from([(symbol("F"), Tradability::Fractionable)]);
        let price = |_: &Symbol| Some(Price::from_ticks(10_000_000).unwrap());
        let held = |book: &Book, target: Target, price: &dyn Fn(&Symbol) -> Option<Price>| {
            guard(book, &target, &unshorted(&fractionable), wide(), price)
                .held()
                .iter()
                .map(OrderGuarded::cause)
                .collect::<Vec<_>>()
        };
        let flat = Book::default();
        let buy = |units| longs(&[("F", units)]);
        assert_eq!(
            held(&flat, buy(99_999), &price),
            [GuardCause::BelowMinimum {
                price: Price::from_ticks(10_000_000).unwrap()
            }]
        );
        assert_eq!(held(&flat, buy(100_000), &price), []);
        assert_eq!(held(&flat, buy(99_999), &|_| None), []);
        let cheap = |_: &Symbol| Some(Price::from_ticks(500_000).unwrap());
        assert_eq!(held(&flat, buy(1_000_000), &cheap), []);
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
        let sell = longs(&[("F", 100_000)]);
        assert_eq!(orders(&book, &sell)[0].side(), Side::Sell);
        assert_eq!(held(&book, sell, &price), []);
    }

    const SYMBOLS: [&str; 4] = ["AAPL", "MSFT", "SPY", "QQQ"];

    /// What the live chain is given at one decision: a book, a target, a price for every symbol, risk's inputs and
    /// limits, and a reading per symbol, absent where none was taken.
    #[derive(Debug, Clone)]
    struct Decision {
        book: Book,
        wanted: Target,
        prices: BTreeMap<Symbol, Price>,
        gross: Cash,
        per_name: Cash,
        limits: Limits,
        phase: SessionPhase,
        opening: Cash,
        readings: BTreeMap<Symbol, Tradability>,
    }

    /// Prices from ten cents to $500, half under a dollar, so a fractional buy can fall under the minimum.
    fn arbitrary_decision(
        readings: impl prop::strategy::Strategy<Value = Option<Tradability>>,
    ) -> impl prop::strategy::Strategy<Value = Decision> {
        let units = || {
            prop::collection::btree_map(
                prop::sample::select(SYMBOLS.to_vec()).prop_map(symbol),
                0..20_000_000u64,
                0..4,
            )
        };
        let dollars = |count: i128| Cash::from_units(count * 1_000_000_000_000);
        (
            (0..20_000i128, units()),
            units(),
            prop::collection::vec(
                prop_oneof![100_000..1_000_000i64, 1_000_000..500_000_000i64],
                4,
            ),
            (1..20_000i128, 1..20_000i128, 1..20_000i128, 0..20_000i128),
            prop::option::of(0..390i64),
            prop::collection::vec(readings, 4),
        )
            .prop_map(
                move |(
                    (cash, held),
                    wanted,
                    ticks,
                    (gross, per_name, loss, opening),
                    until_close,
                    readings,
                )| {
                    Decision {
                        book: Book::reported(
                            dollars(cash),
                            held.into_iter().map(|(symbol, units)| {
                                (symbol, Position::from_units(i128::from(units)))
                            }),
                        ),
                        wanted: Target::new(
                            wanted
                                .into_iter()
                                .map(|(symbol, units)| (symbol, Shares::from_units(units)))
                                .collect(),
                        ),
                        prices: SYMBOLS
                            .iter()
                            .zip(ticks)
                            .map(|(raw, ticks)| (symbol(raw), Price::from_ticks(ticks).unwrap()))
                            .collect(),
                        gross: dollars(gross),
                        per_name: dollars(per_name),
                        limits: Limits::new(
                            dollars(gross),
                            dollars(per_name),
                            dollars(loss),
                            dollars(loss),
                            TimeDelta::minutes(15),
                        )
                        .unwrap(),
                        phase: until_close.map_or(SessionPhase::AfterClose, |minutes| {
                            SessionPhase::Open {
                                until_close: TimeDelta::minutes(minutes),
                            }
                        }),
                        opening: dollars(opening),
                        readings: SYMBOLS
                            .iter()
                            .zip(readings)
                            .filter_map(|(raw, reading)| Some((symbol(raw), reading?)))
                            .collect(),
                    }
                },
            )
    }

    /// The live chain's pure stages: the solution's target, the guard over the orders that reach it, and the book after
    /// every passed order fills at its price.
    fn chain(decision: &Decision) -> (Target, Guarded, Book) {
        let price = |symbol: &Symbol| decision.prices.get(symbol).copied();
        let solution = Solver::Clip.solve(
            &formulate(
                &decision.limits,
                decision.phase,
                decision.opening,
                &decision.book,
                Ok(&NameLosses::default()),
                price,
                decision.wanted.clone(),
            )
            .unwrap(),
        );
        let guarded = guard(
            &decision.book,
            solution.target(),
            &unshorted(&decision.readings),
            decision.limits.per_name(),
            price,
        );
        let after = decision
            .book
            .clone()
            .combine(concatenate(guarded.passed().iter().map(|order| {
                Book::of(
                    &Fill::new(
                        "2026-10-08T15:00:00Z".parse().unwrap(),
                        order.symbol().clone(),
                        order.side(),
                        order.shares(),
                        decision.prices[order.symbol()],
                        DollarVolume::default(),
                    )
                    .unwrap(),
                )
            })));
        (solution.target().clone(), guarded, after)
    }

    proptest! {
        /// Whatever the readings, the orders still owed from the book the passed fills reach to the solution's target
        /// are exactly the held ones, and with none held that book is the target.
        #[test]
        fn property_what_the_guard_holds_is_what_the_chain_leaves_owed(
            decision in arbitrary_decision(prop::sample::select(vec![
                None,
                Some(Tradability::Fractionable),
                Some(Tradability::WholeSharesOnly),
                Some(Tradability::Untradable),
                Some(Tradability::Unlisted),
            ])),
        ) {
            let (target, guarded, after) = chain(&decision);
            let owed: Vec<(Symbol, Side, Shares)> = orders(&after, &target)
                .into_iter()
                .map(|order| (order.symbol().clone(), order.side(), order.shares()))
                .collect();
            let held: Vec<(Symbol, Side, Shares)> = guarded
                .held()
                .iter()
                .map(|held| (held.symbol.clone(), held.side, held.shares))
                .collect();
            prop_assert_eq!(owed, held);
            if guarded.held().is_empty() {
                let wanted: BTreeMap<Symbol, Position> = target
                    .holdings()
                    .iter()
                    .map(|(symbol, holding)| (symbol.clone(), holding.position()))
                    .collect();
                prop_assert_eq!(after.positions().keys().collect::<Vec<_>>(), wanted.keys().collect::<Vec<_>>());
                prop_assert_eq!(after.positions(), &wanted);
            }
        }

        /// Where every symbol trades fractions, only buys under the minimum are held, so the book the chain reaches
        /// holds no symbol above the solution's target and stays within the per-name and gross limits at the
        /// decision's prices.
        #[test]
        fn property_a_fractionable_chain_reaches_no_further_than_risk_allows(
            decision in arbitrary_decision(Just(Some(Tradability::Fractionable))),
        ) {
            let (target, guarded, after) = chain(&decision);
            for held in guarded.held() {
                prop_assert_eq!(held.side, Side::Buy);
                let below_minimum = matches!(held.cause, GuardCause::BelowMinimum { .. });
                prop_assert!(below_minimum, "{:?}", held);
            }
            let mut worth = 0_i128;
            for (symbol, position) in after.positions() {
                let wanted = target.holdings().get(symbol).map_or(0, |holding| holding.position().units());
                prop_assert!(0 <= position.units() && position.units() <= wanted);
                let value = position.units() * i128::from(decision.prices[symbol].ticks());
                prop_assert!(value <= decision.per_name.units());
                worth += value;
            }
            prop_assert!(worth <= decision.gross.units());
        }

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
            let target = longs(&wanted);
            let built = orders(&Book::default(), &target);
            let guarded = guard(&Book::default(), &target, &unshorted(&tradability), wide(), |_| None);
            let mut sorted: Vec<&Symbol> = guarded
                .passed()
                .iter()
                .map(Order::symbol)
                .chain(guarded.held().iter().map(|held| &held.symbol))
                .collect();
            sorted.sort();
            prop_assert_eq!(sorted, built.iter().map(Order::symbol).collect::<Vec<_>>());
            let mut passed = guarded.passed().iter();
            let mut next = passed.next();
            for order in &built {
                if next == Some(order) {
                    next = passed.next();
                }
            }
            prop_assert_eq!(next, None);
        }
    }
}
