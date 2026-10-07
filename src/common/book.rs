//! What a strategy holds, as cash and positions folded from fills. A commutative monoid, so a book is its opening
//! combined with every fill since, in any grouping, and marking it to prices is a homomorphism into cash.

use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::common::market::{
    DollarVolume, Dollars, PRICE_SCALE, Price, SHARE_SCALE, Shares, Symbol,
};
use crate::common::monoid::Monoid;

/// A signed amount of money in `DollarVolume` units, ticks × millionths of a share, so a fill's cash is exact.
// Written as a decimal string, since serde's buffer for a tagged journal record cannot hold an i128.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct Cash(i128);

impl TryFrom<String> for Cash {
    type Error = std::num::ParseIntError;

    fn try_from(raw: String) -> Result<Self, Self::Error> {
        raw.parse().map(Self)
    }
}

impl From<Cash> for String {
    fn from(value: Cash) -> Self {
        value.0.to_string()
    }
}

impl Cash {
    pub fn from_units(units: i128) -> Self {
        Self(units)
    }

    pub fn units(self) -> i128 {
        self.0
    }

    /// The amount in dollars, for presentation only: sums belong on `units`.
    pub fn dollars(self) -> f64 {
        self.0 as f64 / (PRICE_SCALE as f64 * SHARE_SCALE as f64)
    }

    /// Only a fill's notional or cost, which is at most the notional: under 10^13 ticks × `u64::MAX` units.
    fn of(amount: DollarVolume) -> Self {
        Self(i128::try_from(amount.units()).expect("a fill's amount fits i128"))
    }

    fn negated(self) -> Self {
        Self(-self.0)
    }
}

impl From<Dollars> for Cash {
    fn from(dollars: Dollars) -> Self {
        Self(i128::from(dollars.millionths()) * i128::from(SHARE_SCALE))
    }
}

impl Monoid for Cash {
    fn empty() -> Self {
        Self::default()
    }

    fn combine(self, other: Self) -> Self {
        Self(self.0.checked_add(other.0).expect("cash fits i128"))
    }
}

/// A signed holding in millionths of a share; signed so fills form a group, though targets are long-only today.
// Written as a decimal string, since serde's buffer for a tagged journal record cannot hold an i128.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct Position(i128);

impl TryFrom<String> for Position {
    type Error = std::num::ParseIntError;

    fn try_from(raw: String) -> Result<Self, Self::Error> {
        raw.parse().map(Self)
    }
}

impl From<Position> for String {
    fn from(value: Position) -> Self {
        value.0.to_string()
    }
}

impl Position {
    pub fn from_units(units: i128) -> Self {
        Self(units)
    }

    pub fn units(self) -> i128 {
        self.0
    }

    fn of(shares: Shares) -> Self {
        Self(i128::from(shares.units()))
    }
}

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    serde::Serialize,
    serde::Deserialize,
    strum::Display,
    strum::EnumString,
    strum::IntoStaticStr,
    strum::EnumIter,
)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum Side {
    Buy,
    Sell,
}

/// One execution: `shares` of `symbol` at `price`, charged `cost`, stamped `filled_against`: the bar it filled at in a
/// replay, or the broker's closing report of the order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Fill {
    filled_against: DateTime<Utc>,
    symbol: Symbol,
    side: Side,
    shares: Shares,
    price: Price,
    cost: DollarVolume,
}

/// Why a fill was refused: its cost exceeds what it trades, the bound that keeps a fill's cash exact.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FillRefusal {
    CostBeyondNotional {
        cost: DollarVolume,
        notional: DollarVolume,
    },
}

impl Fill {
    pub fn new(
        filled_against: DateTime<Utc>,
        symbol: Symbol,
        side: Side,
        shares: Shares,
        price: Price,
        cost: DollarVolume,
    ) -> Result<Self, FillRefusal> {
        let notional = DollarVolume::of(price, shares);
        if cost > notional {
            return Err(FillRefusal::CostBeyondNotional { cost, notional });
        }
        Ok(Self {
            filled_against,
            symbol,
            side,
            shares,
            price,
            cost,
        })
    }

    pub fn filled_against(&self) -> DateTime<Utc> {
        self.filled_against
    }

    pub fn symbol(&self) -> &Symbol {
        &self.symbol
    }

    pub fn side(&self) -> Side {
        self.side
    }

    pub fn shares(&self) -> Shares {
        self.shares
    }

    pub fn price(&self) -> Price {
        self.price
    }

    pub fn cost(&self) -> DollarVolume {
        self.cost
    }

    /// The price times the shares, before the cost.
    pub fn notional(&self) -> DollarVolume {
        DollarVolume::of(self.price, self.shares)
    }
}

/// Why a book could not be marked: it holds a symbol with no price.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ValuationRefusal {
    Unpriced { symbol: Symbol },
}

/// Cash and every non-zero position; a zero position is dropped, so equal holdings are equal books.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Book {
    cash: Cash,
    positions: BTreeMap<Symbol, Position>,
}

impl Book {
    /// A book holding only `cash`, the opening a replay starts from.
    pub fn funded(cash: Cash) -> Self {
        Self {
            cash,
            positions: BTreeMap::new(),
        }
    }

    /// The book a broker reports holding, combined from one book per position, so a symbol reported twice is summed
    /// and a zero position is dropped like any other book's.
    pub fn reported(cash: Cash, positions: impl IntoIterator<Item = (Symbol, Position)>) -> Self {
        positions.into_iter().fold(
            Self {
                cash,
                positions: BTreeMap::new(),
            },
            |book, (symbol, position)| {
                book.combine(Self {
                    cash: Cash::empty(),
                    positions: BTreeMap::from([(symbol, position)]),
                })
            },
        )
    }

    /// The change one fill makes, so a book after fills is `funded(cash)` combined with each fill's `of`.
    pub fn of(fill: &Fill) -> Self {
        let (notional, cost) = (Cash::of(fill.notional()), Cash::of(fill.cost));
        let shares = Position::of(fill.shares);
        let (cash, position) = match fill.side {
            Side::Buy => (notional.negated(), shares),
            Side::Sell => (notional, Position(-shares.0)),
        };
        Self {
            cash: cash.combine(cost.negated()),
            positions: BTreeMap::from([(fill.symbol.clone(), position)]),
        }
        .canonical()
    }

    pub fn cash(&self) -> Cash {
        self.cash
    }

    /// The holding in `symbol`, zero when none is held.
    pub fn position(&self, symbol: &Symbol) -> Position {
        self.positions.get(symbol).copied().unwrap_or_default()
    }

    pub fn positions(&self) -> &BTreeMap<Symbol, Position> {
        &self.positions
    }

    /// Cash plus each position at `price`, refused for the first position `price` cannot answer.
    pub fn value(
        &self,
        price: impl Fn(&Symbol) -> Option<Price>,
    ) -> Result<Cash, ValuationRefusal> {
        self.positions
            .iter()
            .try_fold(self.cash, |total, (symbol, position)| {
                let price = price(symbol).ok_or_else(|| ValuationRefusal::Unpriced {
                    symbol: symbol.clone(),
                })?;
                let worth = position
                    .0
                    .checked_mul(i128::from(price.ticks()))
                    .expect("a position's worth fits i128");
                Ok(total.combine(Cash(worth)))
            })
    }

    fn canonical(mut self) -> Self {
        self.positions.retain(|_, position| position.0 != 0);
        self
    }
}

impl Monoid for Book {
    fn empty() -> Self {
        Self::default()
    }

    fn combine(mut self, other: Self) -> Self {
        self.cash = self.cash.combine(other.cash);
        for (symbol, position) in other.positions {
            let held = self.positions.entry(symbol).or_default();
            held.0 = held
                .0
                .checked_add(position.0)
                .expect("a position fits i128");
        }
        self.canonical()
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;
    use crate::common::monoid::{concatenate, laws};

    fn symbol(raw: &str) -> Symbol {
        Symbol::new(raw).unwrap()
    }

    fn fill(raw: &str, side: Side, shares: u64, ticks: i64, cost: u128) -> Fill {
        Fill::new(
            "2026-09-25T13:30:00Z".parse().unwrap(),
            symbol(raw),
            side,
            Shares::whole(shares).unwrap(),
            Price::from_ticks(ticks).unwrap(),
            DollarVolume::from_units(cost),
        )
        .unwrap()
    }

    fn arbitrary_fill() -> impl Strategy<Value = Fill> {
        (
            prop::sample::select(vec!["AAPL", "MSFT", "SPY"]),
            prop::bool::ANY,
            1..10_000u64,
            1..1_000_000_000i64,
            0..=100u128,
        )
            .prop_map(|(raw, buy, shares, ticks, percent)| {
                let side = if buy { Side::Buy } else { Side::Sell };
                let notional = DollarVolume::of(
                    Price::from_ticks(ticks).unwrap(),
                    Shares::whole(shares).unwrap(),
                );
                fill(raw, side, shares, ticks, notional.units() * percent / 100)
            })
    }

    fn arbitrary_book() -> impl Strategy<Value = Book> {
        (
            0..1_000_000_000_000_000i128,
            prop::collection::vec(arbitrary_fill(), 0..6),
        )
            .prop_map(|(cash, fills)| {
                Book::funded(Cash(cash)).combine(concatenate(fills.iter().map(Book::of)))
            })
    }

    /// A buy spends its notional and cost and a sell receives its notional less its cost, in exact units.
    #[test]
    fn test_a_fill_moves_cash_by_its_notional_and_cost() {
        // 10 shares at $2: 10 × 10^6 share units × 2 × 10^6 ticks.
        let buy = Book::of(&fill("AAPL", Side::Buy, 10, 2_000_000, 7));
        assert_eq!(buy.cash(), Cash::from_units(-20_000_000_000_007));
        assert_eq!(buy.position(&symbol("AAPL")).units(), 10_000_000);
        let sell = Book::of(&fill("AAPL", Side::Sell, 10, 2_000_000, 7));
        assert_eq!(sell.cash(), Cash::from_units(19_999_999_999_993));
        assert_eq!(sell.position(&symbol("AAPL")).units(), -10_000_000);
    }

    /// A round trip at one price drops the position and leaves exactly its two costs spent.
    #[test]
    fn test_a_round_trip_at_one_price_costs_exactly_its_charges() {
        let book = Book::funded(Cash(1_000))
            .combine(Book::of(&fill("AAPL", Side::Buy, 3, 5_000_000, 11)))
            .combine(Book::of(&fill("AAPL", Side::Sell, 3, 5_000_000, 13)));
        assert_eq!(book.positions().keys().count(), 0);
        assert_eq!(book.cash(), Cash::from_units(1_000 - 24));
        assert_eq!(book, Book::funded(Cash(976)));
    }

    /// A cost past the notional is refused, so a fill's cash stays within what a `Cash` holds.
    #[test]
    fn test_a_fill_costing_more_than_it_trades_is_refused() {
        let price = Price::from_ticks(2).unwrap();
        let shares = Shares::from_units(3);
        let at = "2026-09-25T13:30:00Z".parse().unwrap();
        let fill = |cost| {
            Fill::new(
                at,
                symbol("AAPL"),
                Side::Buy,
                shares,
                price,
                DollarVolume::from_units(cost),
            )
        };
        assert!(fill(6).is_ok());
        assert_eq!(
            fill(7),
            Err(FillRefusal::CostBeyondNotional {
                cost: DollarVolume::from_units(7),
                notional: DollarVolume::from_units(6),
            })
        );
    }

    #[test]
    fn test_a_reported_book_sums_repeats_and_drops_zeros() {
        let book = Book::reported(
            Cash::from_units(5),
            [
                (symbol("AAPL"), Position::from_units(2)),
                (symbol("AAPL"), Position::from_units(3)),
                (symbol("MSFT"), Position::from_units(0)),
            ],
        );
        let positions: Vec<_> = book
            .positions()
            .iter()
            .map(|(symbol, position)| (symbol.as_str(), position.units()))
            .collect();
        assert_eq!(positions, [("AAPL", 5)]);
        assert_eq!(book.cash(), Cash::from_units(5));
    }

    #[test]
    fn test_cash_presents_in_dollars() {
        assert_eq!(Cash::from_units(-1_500_000_000_000).dollars(), -1.5);
    }

    #[test]
    fn test_dollars_become_cash_in_its_units() {
        let dollars: Dollars = "1.5".parse().unwrap();
        assert_eq!(Cash::from(dollars), Cash::from_units(1_500_000_000_000));
    }

    #[test]
    fn test_an_unpriced_position_refuses_the_mark() {
        let book = Book::of(&fill("AAPL", Side::Buy, 1, 1_000_000, 0));
        assert_eq!(
            book.value(|_| None),
            Err(ValuationRefusal::Unpriced {
                symbol: symbol("AAPL")
            })
        );
        assert_eq!(Book::empty().value(|_| None), Ok(Cash::empty()));
    }

    proptest! {
        #[test]
        fn property_books_are_a_commutative_monoid(
            first in arbitrary_book(),
            second in arbitrary_book(),
            third in arbitrary_book(),
        ) {
            laws::check(first, second, third)?;
        }

        #[test]
        fn property_cash_is_a_commutative_monoid(
            first in -(1i128 << 100)..(1i128 << 100),
            second in -(1i128 << 100)..(1i128 << 100),
            third in -(1i128 << 100)..(1i128 << 100),
        ) {
            laws::check(Cash(first), Cash(second), Cash(third))?;
        }

        /// Marking at fixed prices sends a combined book to the sum of the marks.
        #[test]
        fn property_marking_is_a_homomorphism(
            first in arbitrary_book(),
            second in arbitrary_book(),
            ticks in 1..1_000_000_000i64,
        ) {
            let price = |_: &Symbol| Some(Price::from_ticks(ticks).unwrap());
            prop_assert_eq!(
                first.clone().combine(second.clone()).value(price),
                Ok(first.value(price).unwrap().combine(second.value(price).unwrap()))
            );
            prop_assert_eq!(Book::empty().value(price), Ok(Cash::empty()));
        }

        /// A buy and a sell of the same fill cancel to the two costs, the group inverse up to charges.
        #[test]
        fn property_a_sell_undoes_a_buy_but_its_costs(fill in arbitrary_fill()) {
            let opposite = Fill::new(
                fill.filled_against(),
                fill.symbol().clone(),
                match fill.side() {
                    Side::Buy => Side::Sell,
                    Side::Sell => Side::Buy,
                },
                fill.shares(),
                fill.price(),
                fill.cost(),
            )
            .unwrap();
            let both = Book::of(&fill).combine(Book::of(&opposite));
            prop_assert!(both.positions().is_empty());
            prop_assert_eq!(both.cash(), Cash::of(fill.cost()).combine(Cash::of(fill.cost())).negated());
        }
    }
}
