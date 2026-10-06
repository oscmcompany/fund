//! A strategy as an arrow from what is known, the market state and the book, to the holdings it wants; the roll that
//! blends one such target into another; and the orders that close the gap between a book and a target.

pub mod noise;

use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};

use chrono::TimeDelta;

use crate::common::book::{Book, Side};
use crate::common::market::state::MarketState;
use crate::common::market::{Shares, Symbol};

/// Decides the holdings wanted after each decision bar; pure, so replay and the live loop share one `decide`.
pub trait Strategy {
    fn decide(&self, state: &MarketState, book: &Book) -> Target;
}

/// The holdings a strategy wants, long-only; a symbol absent from it is wanted at zero.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Target(BTreeMap<Symbol, Shares>);

impl Target {
    /// Zero holdings are dropped, so equal wants are equal targets.
    pub fn new(holdings: BTreeMap<Symbol, Shares>) -> Self {
        Self(
            holdings
                .into_iter()
                .filter(|(_, shares)| !shares.is_zero())
                .collect(),
        )
    }

    pub fn holdings(&self) -> &BTreeMap<Symbol, Shares> {
        &self.0
    }
}

/// The parts of `PROGRESS_SCALE` a switch has rolled from its outgoing target to its incoming one.
pub const PROGRESS_SCALE: u32 = 1_000_000;

/// How far a roll-off has gone, from none to all of the way.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Progress(u32);

impl Progress {
    pub const NONE: Self = Self(0);
    pub const WHOLE: Self = Self(PROGRESS_SCALE);

    /// The share of `window` that `elapsed` covers, held to `[NONE, WHOLE]`; a zero window has already rolled.
    pub fn of(elapsed: TimeDelta, window: TimeDelta) -> Self {
        if elapsed >= window {
            return Self::WHOLE;
        }
        if elapsed <= TimeDelta::zero() {
            return Self::NONE;
        }
        let parts = nanoseconds(elapsed) * i128::from(PROGRESS_SCALE) / nanoseconds(window);
        Self(u32::try_from(parts).expect("a partial roll is under the scale"))
    }
}

fn nanoseconds(span: TimeDelta) -> i128 {
    i128::from(span.num_seconds()) * 1_000_000_000 + i128::from(span.subsec_nanos())
}

/// The target `progress` of the way from `from` to `to`, each holding moved toward `to` and truncated toward `from`,
/// so a switch blends from the outgoing strategy's holdings into the incoming one's over its window.
pub fn roll(from: &Target, to: &Target, progress: Progress) -> Target {
    let symbols: BTreeSet<&Symbol> = from.0.keys().chain(to.0.keys()).collect();
    Target::new(
        symbols
            .into_iter()
            .map(|symbol| {
                let start = i128::from(from.0.get(symbol).copied().unwrap_or_default().units());
                let end = i128::from(to.0.get(symbol).copied().unwrap_or_default().units());
                let moved = (end - start) * i128::from(progress.0) / i128::from(PROGRESS_SCALE);
                let units = u64::try_from(start + moved)
                    .expect("a rolled holding lies between two holdings");
                (symbol.clone(), Shares::from_units(units))
            })
            .collect(),
    )
}

/// An instruction to trade `shares` of `symbol`, never zero.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Order {
    symbol: Symbol,
    side: Side,
    shares: Shares,
}

impl Order {
    pub fn symbol(&self) -> &Symbol {
        &self.symbol
    }

    pub fn side(&self) -> Side {
        self.side
    }

    pub fn shares(&self) -> Shares {
        self.shares
    }
}

/// The orders that take `book` to `target`, sells before buys so a rebalance frees cash before spending it.
pub fn orders(book: &Book, target: &Target) -> Vec<Order> {
    // Straight to the target, which holds while our orders are small against volume; a schedule stage goes here if not.
    let symbols = book.positions().keys().chain(target.0.keys());
    let mut orders: Vec<Order> = symbols
        .collect::<BTreeSet<_>>()
        .into_iter()
        .filter_map(|symbol| {
            let wanted = i128::from(target.0.get(symbol).copied().unwrap_or_default().units());
            let gap = wanted - book.position(symbol).units();
            let side = match gap.cmp(&0) {
                Ordering::Equal => return None,
                Ordering::Greater => Side::Buy,
                Ordering::Less => Side::Sell,
            };
            let units = u64::try_from(gap.unsigned_abs()).expect("an order fits u64 share units");
            Some(Order {
                symbol: symbol.clone(),
                side,
                shares: Shares::from_units(units),
            })
        })
        .collect();
    orders.sort_by_key(|order| order.side == Side::Buy);
    orders
}

#[cfg(test)]
mod tests {
    use chrono::{DateTime, Utc};
    use proptest::prelude::*;

    use super::{
        BTreeMap, Book, Order, Progress, Shares, Side, Symbol, Target, TimeDelta, orders, roll,
    };
    use crate::common::book::Fill;
    use crate::common::market::{DollarVolume, Price};
    use crate::common::monoid::{Monoid, concatenate};

    fn symbol(raw: &str) -> Symbol {
        Symbol::new(raw).unwrap()
    }

    fn filled(order: &Order, ticks: i64) -> Fill {
        Fill::new(
            "2026-09-25T13:30:00Z".parse::<DateTime<Utc>>().unwrap(),
            order.symbol.clone(),
            order.side,
            order.shares,
            Price::from_ticks(ticks).unwrap(),
            DollarVolume::default(),
        )
        .unwrap()
    }

    fn arbitrary_holdings() -> impl prop::strategy::Strategy<Value = BTreeMap<Symbol, Shares>> {
        prop::collection::btree_map(
            prop::sample::select(vec!["AAPL", "MSFT", "SPY", "QQQ"]).prop_map(symbol),
            (0..1_000_000_000u64).prop_map(Shares::from_units),
            0..4,
        )
    }

    fn holding(holdings: &BTreeMap<Symbol, Shares>) -> Book {
        concatenate(holdings.iter().map(|(symbol, shares)| {
            Book::of(
                &Fill::new(
                    "2026-09-25T13:30:00Z".parse::<DateTime<Utc>>().unwrap(),
                    symbol.clone(),
                    Side::Buy,
                    *shares,
                    Price::from_ticks(1).unwrap(),
                    DollarVolume::default(),
                )
                .unwrap(),
            )
        }))
    }

    #[test]
    fn test_a_rebalance_sells_before_it_buys() {
        let book = holding(&BTreeMap::from([(
            symbol("MSFT"),
            Shares::whole(5).unwrap(),
        )]));
        let target = Target::new(BTreeMap::from([
            (symbol("AAPL"), Shares::whole(2).unwrap()),
            (symbol("SPY"), Shares::whole(0).unwrap()),
        ]));
        let orders = orders(&book, &target);
        let sides: Vec<_> = orders
            .iter()
            .map(|order| (order.symbol().as_str(), order.side(), order.shares()))
            .collect();
        assert_eq!(
            sides,
            [
                ("MSFT", Side::Sell, Shares::whole(5).unwrap()),
                ("AAPL", Side::Buy, Shares::whole(2).unwrap()),
            ]
        );
    }

    /// Thirty seconds of a two-minute window is a quarter; before it is none, at or past it all, and a zero window
    /// has already rolled.
    #[test]
    fn test_progress_is_the_share_of_the_window_elapsed() {
        let window = TimeDelta::minutes(2);
        assert_eq!(
            Progress::of(TimeDelta::seconds(30), window),
            Progress(250_000)
        );
        assert_eq!(Progress::of(TimeDelta::seconds(-1), window), Progress::NONE);
        assert_eq!(Progress::of(TimeDelta::zero(), window), Progress::NONE);
        assert_eq!(Progress::of(window, window), Progress(1_000_000));
        assert_eq!(Progress::of(TimeDelta::minutes(5), window), Progress::WHOLE);
        assert_eq!(
            Progress::of(TimeDelta::zero(), TimeDelta::zero()),
            Progress::WHOLE
        );
        assert_eq!(
            Progress::of(TimeDelta::microseconds(100), TimeDelta::microseconds(400)),
            Progress(250_000)
        );
    }

    /// A quarter of the way from 4 SPY to 0 SPY and 8 AAPL holds 3 SPY and 2 AAPL.
    #[test]
    fn test_a_roll_moves_each_holding_its_share_of_the_way() {
        let from = Target::new(BTreeMap::from([(symbol("SPY"), Shares::whole(4).unwrap())]));
        let to = Target::new(BTreeMap::from([(
            symbol("AAPL"),
            Shares::whole(8).unwrap(),
        )]));
        assert_eq!(
            roll(&from, &to, Progress(250_000)),
            Target::new(BTreeMap::from([
                (symbol("SPY"), Shares::whole(3).unwrap()),
                (symbol("AAPL"), Shares::whole(2).unwrap()),
            ]))
        );
    }

    proptest! {
        /// A roll starts at its outgoing target, ends at its incoming one, keeps every holding between the two, and
        /// rolling a target into itself changes nothing.
        #[test]
        fn property_a_roll_runs_between_its_targets(
            from in arbitrary_holdings(),
            to in arbitrary_holdings(),
            parts in 0..=1_000_000u32,
        ) {
            let (from, to) = (Target::new(from), Target::new(to));
            prop_assert_eq!(roll(&from, &to, Progress::NONE), from.clone());
            prop_assert_eq!(roll(&from, &to, Progress::WHOLE), to.clone());
            prop_assert_eq!(roll(&to, &to, Progress(parts)), to.clone());
            let rolled = roll(&from, &to, Progress(parts));
            for symbol in from.holdings().keys().chain(to.holdings().keys()) {
                let held = |target: &Target| target.holdings().get(symbol).copied().unwrap_or_default();
                let (start, end, now) = (held(&from), held(&to), held(&rolled));
                prop_assert!(start.min(end) <= now && now <= start.max(end));
            }
        }

        /// Filling every order at any price leaves the book holding exactly the target.
        #[test]
        fn property_filled_orders_reach_the_target(
            held in arbitrary_holdings(),
            wanted in arbitrary_holdings(),
            ticks in 1..1_000_000_000i64,
        ) {
            let book = holding(&held);
            let target = Target::new(wanted);
            let after = book.clone().combine(concatenate(
                orders(&book, &target).iter().map(|order| Book::of(&filled(order, ticks))),
            ));
            let reached: BTreeMap<Symbol, Shares> = after
                .positions()
                .iter()
                .map(|(symbol, position)| {
                    (symbol.clone(), Shares::from_units(u64::try_from(position.units()).unwrap()))
                })
                .collect();
            prop_assert_eq!(reached.keys().collect::<Vec<_>>(), target.holdings().keys().collect::<Vec<_>>());
            prop_assert_eq!(&reached, target.holdings());
        }

        /// A target equal to the book's holdings orders nothing: reconciling is the identity there.
        #[test]
        fn property_a_book_at_its_target_orders_nothing(held in arbitrary_holdings()) {
            prop_assert!(orders(&holding(&held), &Target::new(held)).is_empty());
        }
    }
}
