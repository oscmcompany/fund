//! A strategy as an arrow from what is known, the market state and the book, to the holdings it wants, and the
//! orders that close the gap between a book and a target.

use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};

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

    use super::{BTreeMap, Book, Order, Shares, Side, Symbol, Target, orders};
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

    proptest! {
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
