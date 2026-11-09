//! A strategy as an arrow from what is known, the market state and the book, to the holdings it wants; the roll that
//! blends one such target into another; and the orders that close the gap between a book and a target.

pub mod noise;

use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};

use chrono::TimeDelta;

use crate::common::book::{Book, Position, Side};
use crate::common::market::state::MarketState;
use crate::common::market::{SHARE_SCALE, Shares, Symbol};

/// Decides the holdings wanted after each decision bar; pure, so replay and the live loop share one `decide`.
pub trait Strategy {
    fn decide(&self, state: &MarketState, book: &Book) -> Target;
}

/// The holdings a strategy wants; a symbol absent from it is wanted flat.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(from = "BTreeMap<Symbol, Holding>", into = "BTreeMap<Symbol, Holding>")]
pub struct Target(BTreeMap<Symbol, Holding>);

impl From<BTreeMap<Symbol, Holding>> for Target {
    fn from(holdings: BTreeMap<Symbol, Holding>) -> Self {
        Self::new(holdings)
    }
}

impl From<Target> for BTreeMap<Symbol, Holding> {
    fn from(target: Target) -> Self {
        target.0
    }
}

impl Target {
    /// Zero holdings are dropped, so equal wants are equal targets.
    pub fn new<Wanted: Into<Holding>>(holdings: BTreeMap<Symbol, Wanted>) -> Self {
        Self(
            holdings
                .into_iter()
                .map(|(symbol, wanted)| (symbol, wanted.into()))
                .filter(|(_, holding)| !holding.shares().is_zero())
                .collect(),
        )
    }

    pub fn holdings(&self) -> &BTreeMap<Symbol, Holding> {
        &self.0
    }

    /// The signed millionths wanted in `symbol`, zero when absent.
    fn wanted(&self, symbol: &Symbol) -> i128 {
        self.0
            .get(symbol)
            .map_or(0, |holding| holding.position().units())
    }
}

/// One name's wanted holding: long any count of millionths, or short whole shares, since a broker lends no fraction.
// Written as signed millionths, so a long reads back from journals written when targets were long-only.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(try_from = "HoldingUnits", into = "HoldingUnits")]
pub enum Holding {
    Long(Shares),
    Short(WholeShares),
}

/// A short's size: at least one whole share, at most `i64::MAX` millionths so its signed count is written exactly.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct WholeShares(Shares);

impl WholeShares {
    pub fn shares(self) -> Shares {
        self.0
    }
}

/// Why a signed count cannot be a holding, naming the count.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum HoldingRefusal {
    #[error("a short of {} millionths is not a whole number of shares", .position.units())]
    FractionalShort { position: Position },
    #[error("{} millionths is past what a holding holds", .position.units())]
    BeyondRange { position: Position },
}

impl From<Shares> for Holding {
    fn from(shares: Shares) -> Self {
        Self::Long(shares)
    }
}

impl TryFrom<Position> for Holding {
    type Error = HoldingRefusal;

    fn try_from(position: Position) -> Result<Self, Self::Error> {
        let units = position.units();
        if let Ok(long) = u64::try_from(units) {
            return Ok(Self::Long(Shares::from_units(long)));
        }
        let short = u64::try_from(units.unsigned_abs())
            .ok()
            .filter(|short| i64::try_from(*short).is_ok())
            .ok_or(HoldingRefusal::BeyondRange { position })?;
        if !short.is_multiple_of(SHARE_SCALE) {
            return Err(HoldingRefusal::FractionalShort { position });
        }
        Ok(Self::Short(WholeShares(Shares::from_units(short))))
    }
}

impl Holding {
    /// A short of `shares`, refused unless whole.
    pub fn short(shares: Shares) -> Result<Self, HoldingRefusal> {
        Self::try_from(Position::from_units(-i128::from(shares.units())))
    }

    /// The signed count, negative when short.
    pub fn position(self) -> Position {
        match self {
            Self::Long(shares) => Position::from_units(i128::from(shares.units())),
            Self::Short(short) => Position::from_units(-i128::from(short.0.units())),
        }
    }

    /// The count on either side, which exposure is measured on.
    pub fn shares(self) -> Shares {
        match self {
            Self::Long(shares) => shares,
            Self::Short(short) => short.0,
        }
    }

    /// The holding on the same side with at most `units` millionths, a short truncated to whole shares.
    pub fn capped(self, units: u128) -> Self {
        let kept = u64::try_from(u128::from(self.shares().units()).min(units))
            .expect("a capped holding is at most the holding");
        match self {
            Self::Long(_) => Self::Long(Shares::from_units(kept)),
            Self::Short(_) => Self::short_at_most(kept),
        }
    }

    /// The most whole shares short within `units` millionths, flat when that is none, so zero has one form.
    fn short_at_most(units: u64) -> Self {
        let whole = units - units % SHARE_SCALE;
        match whole {
            0 => Self::Long(Shares::default()),
            1.. => Self::Short(WholeShares(Shares::from_units(whole))),
        }
    }

    /// The holding at `position`, a short truncated toward zero to whole shares; only for a count between two holdings.
    fn truncated(position: Position) -> Self {
        let units = u64::try_from(position.units().unsigned_abs())
            .expect("a count between two holdings fits u64");
        match position.units() >= 0 {
            true => Self::Long(Shares::from_units(units)),
            false => Self::short_at_most(units),
        }
    }
}

/// A holding as journaled: a long's millionths, or a short's negated.
#[derive(serde::Serialize, serde::Deserialize)]
#[serde(untagged)]
enum HoldingUnits {
    Long(u64),
    Short(i64),
}

impl TryFrom<HoldingUnits> for Holding {
    type Error = HoldingRefusal;

    fn try_from(units: HoldingUnits) -> Result<Self, Self::Error> {
        let signed = match units {
            HoldingUnits::Long(units) => i128::from(units),
            HoldingUnits::Short(units) => i128::from(units),
        };
        Self::try_from(Position::from_units(signed))
    }
}

impl From<Holding> for HoldingUnits {
    fn from(holding: Holding) -> Self {
        match holding {
            Holding::Long(shares) => Self::Long(shares.units()),
            Holding::Short(short) => Self::Short(
                -i64::try_from(short.0.units()).expect("a short is at most i64::MAX millionths"),
            ),
        }
    }
}

/// The parts of `PROGRESS_SCALE` a switch has rolled from its outgoing target to its incoming one.
pub const PROGRESS_SCALE: u32 = 1_000_000;

/// How far a roll-off has gone, from none to all of the way, journaled as its parts of `PROGRESS_SCALE`.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
#[serde(try_from = "u32")]
pub struct Progress(u32);

/// Parts past `PROGRESS_SCALE`, which no roll-off reaches.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("{parts} parts is past {PROGRESS_SCALE}")]
pub struct ProgressRefusal {
    parts: u32,
}

impl TryFrom<u32> for Progress {
    type Error = ProgressRefusal;

    fn try_from(parts: u32) -> Result<Self, Self::Error> {
        Self::new(parts)
    }
}

impl Progress {
    pub const NONE: Self = Self(0);
    pub const WHOLE: Self = Self(PROGRESS_SCALE);

    /// `parts` of `PROGRESS_SCALE` through a roll-off, refused past the whole.
    pub fn new(parts: u32) -> Result<Self, ProgressRefusal> {
        if parts > PROGRESS_SCALE {
            return Err(ProgressRefusal { parts });
        }
        Ok(Self(parts))
    }

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
/// and a short then toward zero to whole shares, so a switch blends from the outgoing strategy's holdings into the
/// incoming one's over its window.
pub fn roll(from: &Target, to: &Target, progress: Progress) -> Target {
    let symbols: BTreeSet<&Symbol> = from.0.keys().chain(to.0.keys()).collect();
    Target::new(
        symbols
            .into_iter()
            .map(|symbol| {
                let (start, end) = (from.wanted(symbol), to.wanted(symbol));
                let moved = (end - start) * i128::from(progress.0) / i128::from(PROGRESS_SCALE);
                let rolled = Holding::truncated(Position::from_units(start + moved));
                (symbol.clone(), rolled)
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
    leg: Leg,
}

/// Whether an order takes its holding toward flat or away from it; no order does both, since a broker refuses one
/// across zero.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Leg {
    Reducing,
    Increasing,
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

    pub fn leg(&self) -> Leg {
        self.leg
    }

    /// Whether the order opens or adds to a short, the only kind that borrows.
    pub fn borrows(&self) -> bool {
        match (self.side, self.leg) {
            (Side::Sell, Leg::Increasing) => true,
            (Side::Sell, Leg::Reducing) | (Side::Buy, Leg::Reducing | Leg::Increasing) => false,
        }
    }

    /// The orders taking `symbol` from `from` to `to` millionths, which lie on one side of zero: none when equal, and
    /// a gap past one order's `u64` units as several.
    fn between(symbol: &Symbol, from: i128, to: i128) -> Vec<Self> {
        let side = match to.cmp(&from) {
            Ordering::Equal => return Vec::new(),
            Ordering::Greater => Side::Buy,
            Ordering::Less => Side::Sell,
        };
        let leg = match to.unsigned_abs() < from.unsigned_abs() {
            true => Leg::Reducing,
            false => Leg::Increasing,
        };
        let mut remaining = to.abs_diff(from);
        let mut split = Vec::new();
        while remaining > 0 {
            let units = u64::try_from(remaining).unwrap_or(u64::MAX);
            remaining -= u128::from(units);
            split.push(Self {
                symbol: symbol.clone(),
                side,
                shares: Shares::from_units(units),
                leg,
            });
        }
        split
    }
}

/// The orders that take `book` to `target`, sells before buys so a rebalance frees cash before spending it. A gap
/// across zero is split there, the close to flat going before the open beyond it.
pub fn orders(book: &Book, target: &Target) -> Vec<Order> {
    // Straight to the target, which holds while our orders are small against volume; a schedule stage goes here if not.
    let symbols = book.positions().keys().chain(target.0.keys());
    let mut orders: Vec<Order> = symbols
        .collect::<BTreeSet<_>>()
        .into_iter()
        .flat_map(|symbol| {
            let (held, wanted) = (book.position(symbol).units(), target.wanted(symbol));
            let waypoint = match held.signum() * wanted.signum() < 0 {
                true => 0,
                false => wanted,
            };
            let mut split = Order::between(symbol, held, waypoint);
            split.extend(Order::between(symbol, waypoint, wanted));
            split
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
        BTreeMap, Book, Holding, HoldingRefusal, Leg, Order, Position, Progress, ProgressRefusal,
        Shares, Side, Symbol, Target, TimeDelta, orders, roll,
    };
    use crate::common::book::{Cash, Fill};
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

    fn arbitrary_long_holdings() -> impl prop::strategy::Strategy<Value = BTreeMap<Symbol, Shares>>
    {
        prop::collection::btree_map(
            prop::sample::select(vec!["AAPL", "MSFT", "SPY", "QQQ"]).prop_map(symbol),
            (0..1_000_000_000u64).prop_map(Shares::from_units),
            0..4,
        )
    }

    /// Longs of any millionths and shorts of whole shares, so a target can cross the book through zero.
    fn arbitrary_holdings() -> impl prop::strategy::Strategy<Value = BTreeMap<Symbol, Holding>> {
        prop::collection::btree_map(
            prop::sample::select(vec!["AAPL", "MSFT", "SPY", "QQQ"]).prop_map(symbol),
            prop_oneof![
                (0..1_000_000_000u64).prop_map(|units| Holding::from(Shares::from_units(units))),
                (0..1_000u64)
                    .prop_map(|count| Holding::short(Shares::whole(count).unwrap()).unwrap()),
            ],
            0..4,
        )
    }

    fn positions(target: &Target) -> BTreeMap<Symbol, Position> {
        target
            .holdings()
            .iter()
            .map(|(symbol, holding)| (symbol.clone(), holding.position()))
            .collect()
    }

    #[test]
    fn test_a_gap_past_one_order_is_split_into_orders_that_each_fit() {
        let symbol = Symbol::new("ABC").unwrap();
        let book = holding(&BTreeMap::from([(
            symbol.clone(),
            Holding::from(Shares::from_units(u64::MAX)),
        )]));
        let target = Target::new(BTreeMap::from([(
            symbol,
            Holding::short(Shares::whole(1).unwrap()).unwrap(),
        )]));
        let sold: Vec<(Side, u64)> = orders(&book, &target)
            .iter()
            .map(|order| (order.side(), order.shares().units()))
            .collect();
        assert_eq!(
            sold,
            [
                (Side::Sell, 18_446_744_073_709_551_615),
                (Side::Sell, 1_000_000)
            ]
        );
    }

    fn holding(holdings: &BTreeMap<Symbol, Holding>) -> Book {
        Book::reported(
            Cash::default(),
            holdings
                .iter()
                .map(|(symbol, holding)| (symbol.clone(), holding.position())),
        )
    }

    #[test]
    fn test_a_rebalance_sells_before_it_buys() {
        let book = holding(&BTreeMap::from([(
            symbol("MSFT"),
            Holding::from(Shares::whole(5).unwrap()),
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

    #[test]
    fn test_progress_reads_back_and_refuses_parts_past_the_scale() {
        let half = Progress::of(TimeDelta::seconds(30), TimeDelta::minutes(1));
        assert_eq!(serde_json::to_string(&half).unwrap(), "500000");
        assert_eq!(
            serde_json::from_str::<Progress>("500000").unwrap(),
            Progress(500_000)
        );
        assert_eq!(
            serde_json::from_str::<Progress>("1000000").unwrap(),
            Progress::WHOLE
        );
        assert_eq!(
            Progress::new(1_000_001),
            Err(ProgressRefusal { parts: 1_000_001 })
        );
        assert!(serde_json::from_str::<Progress>("1000001").is_err());
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

    /// A long reads back as the bare count journals wrote before targets were signed, a short as its negation, and a
    /// fractional short is refused naming its count.
    #[test]
    fn test_a_holding_is_journaled_as_signed_millionths() {
        let long = Holding::from(Shares::from_units(1_500_000));
        let short = Holding::short(Shares::whole(2).unwrap()).unwrap();
        assert_eq!(serde_json::to_string(&long).unwrap(), "1500000");
        assert_eq!(serde_json::to_string(&short).unwrap(), "-2000000");
        assert_eq!(serde_json::from_str::<Holding>("1500000").unwrap(), long);
        assert_eq!(serde_json::from_str::<Holding>("-2000000").unwrap(), short);
        assert_eq!(short.position(), Position::from_units(-2_000_000));
        assert_eq!(short.shares(), Shares::from_units(2_000_000));
        assert_eq!(
            Holding::short(Shares::from_units(1_500_000)),
            Err(HoldingRefusal::FractionalShort {
                position: Position::from_units(-1_500_000)
            })
        );
        assert!(serde_json::from_str::<Holding>("-1500000").is_err());
        assert_eq!(
            Holding::try_from(Position::from_units(-(1_i128 << 64))),
            Err(HoldingRefusal::BeyondRange {
                position: Position::from_units(-(1_i128 << 64))
            })
        );
    }

    /// A short target sells the book through zero in two orders: 3 held and 2 short wanted sells the 3, then 2 short.
    #[test]
    fn test_a_short_target_sells_through_zero() {
        let book = holding(&BTreeMap::from([(
            symbol("SPY"),
            Holding::from(Shares::whole(3).unwrap()),
        )]));
        let target = Target::new(BTreeMap::from([(
            symbol("SPY"),
            Holding::short(Shares::whole(2).unwrap()).unwrap(),
        )]));
        let sides: Vec<_> = orders(&book, &target)
            .iter()
            .map(|order| (order.side(), order.shares(), order.leg(), order.borrows()))
            .collect();
        assert_eq!(
            sides,
            [
                (Side::Sell, Shares::whole(3).unwrap(), Leg::Reducing, false),
                (Side::Sell, Shares::whole(2).unwrap(), Leg::Increasing, true),
            ]
        );
        let back = orders(
            &holding(target.holdings()),
            &Target::new(BTreeMap::from([(symbol("SPY"), Shares::whole(1).unwrap())])),
        );
        let sides: Vec<_> = back
            .iter()
            .map(|order| (order.side(), order.shares(), order.leg(), order.borrows()))
            .collect();
        assert_eq!(
            sides,
            [
                (Side::Buy, Shares::whole(2).unwrap(), Leg::Reducing, false),
                (Side::Buy, Shares::whole(1).unwrap(), Leg::Increasing, false),
            ]
        );
    }

    /// Halfway from 1 short to 4 short is 2.5 short, truncated toward zero to 2; capping a 3-share short at 2.5 keeps 2.
    #[test]
    fn test_a_short_rolls_and_caps_to_whole_shares() {
        let short = |count| Holding::short(Shares::whole(count).unwrap()).unwrap();
        let from = Target::new(BTreeMap::from([(symbol("SPY"), short(1))]));
        let to = Target::new(BTreeMap::from([(symbol("SPY"), short(4))]));
        assert_eq!(
            roll(&from, &to, Progress(500_000)),
            Target::new(BTreeMap::from([(symbol("SPY"), short(2))]))
        );
        assert_eq!(short(3).capped(2_500_000), short(2));
        let flat = Holding::from(Shares::from_units(0));
        assert_eq!(short(1).capped(500_000), flat);
        assert_eq!(Holding::truncated(Position::from_units(-500_000)), flat);
        assert_eq!(
            Holding::from(Shares::whole(3).unwrap()).capped(2_500_000),
            Holding::from(Shares::from_units(2_500_000))
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
                let (start, end, now) = (from.wanted(symbol), to.wanted(symbol), rolled.wanted(symbol));
                prop_assert!(start.min(end) <= now && now <= start.max(end));
            }
        }

        /// Filling every order at any price leaves the book holding exactly the target, short or long.
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
            let wanted = positions(&target);
            prop_assert_eq!(after.positions().keys().collect::<Vec<_>>(), wanted.keys().collect::<Vec<_>>());
            prop_assert_eq!(after.positions(), &wanted);
        }

        /// Filled one at a time, no order takes a holding across zero, each reduces or increases its size as its leg
        /// says, and an order borrows exactly when it leaves the holding further short.
        #[test]
        fn property_no_order_crosses_zero(held in arbitrary_holdings(), wanted in arbitrary_holdings()) {
            let book = holding(&held);
            let mut positions: BTreeMap<Symbol, i128> =
                book.positions().iter().map(|(symbol, position)| (symbol.clone(), position.units())).collect();
            for order in orders(&book, &Target::new(wanted)) {
                let before = positions.get(order.symbol()).copied().unwrap_or(0);
                let units = i128::from(order.shares().units());
                let after = match order.side() {
                    Side::Buy => before + units,
                    Side::Sell => before - units,
                };
                prop_assert!(before.signum() * after.signum() >= 0, "{before} to {after}");
                let reducing = after.unsigned_abs() < before.unsigned_abs();
                prop_assert_eq!(order.leg() == Leg::Reducing, reducing);
                prop_assert_eq!(order.borrows(), after < 0 && after < before);
                positions.insert(order.symbol().clone(), after);
            }
        }

        /// A long-only target orders exactly as it did when targets were long-only: each gap from the book, in shares.
        #[test]
        fn property_a_long_target_orders_its_unsigned_gap(
            held in arbitrary_long_holdings(),
            wanted in arbitrary_long_holdings(),
        ) {
            let held: BTreeMap<Symbol, Holding> =
                held.into_iter().map(|(symbol, shares)| (symbol, Holding::from(shares))).collect();
            let book = holding(&held);
            let target = Target::new(wanted.clone());
            let expected: BTreeMap<Symbol, (Side, Shares)> = held
                .keys()
                .chain(wanted.keys())
                .filter_map(|symbol| {
                    let before = held.get(symbol).map_or(0, |holding| holding.shares().units());
                    let after = wanted.get(symbol).map_or(0, |shares| shares.units());
                    let (side, gap) = match after.cmp(&before) {
                        std::cmp::Ordering::Equal => return None,
                        std::cmp::Ordering::Greater => (Side::Buy, after - before),
                        std::cmp::Ordering::Less => (Side::Sell, before - after),
                    };
                    Some((symbol.clone(), (side, Shares::from_units(gap))))
                })
                .collect();
            let ordered: BTreeMap<Symbol, (Side, Shares)> = orders(&book, &target)
                .iter()
                .map(|order| (order.symbol().clone(), (order.side(), order.shares())))
                .collect();
            prop_assert_eq!(ordered.keys().collect::<Vec<_>>(), expected.keys().collect::<Vec<_>>());
            prop_assert_eq!(ordered, expected);
        }

        /// A holding reads back from its journaled form, and its signed count converts back to it.
        #[test]
        fn property_a_holding_round_trips(held in arbitrary_holdings()) {
            for holding in held.values() {
                let written = serde_json::to_string(holding).unwrap();
                prop_assert_eq!(&serde_json::from_str::<Holding>(&written).unwrap(), holding);
                prop_assert_eq!(&Holding::try_from(holding.position()).unwrap(), holding);
            }
            let target = Target::new(held);
            let written = serde_json::to_string(&target).unwrap();
            prop_assert_eq!(serde_json::from_str::<Target>(&written).unwrap(), target);
        }

        /// A target equal to the book's holdings orders nothing: reconciling is the identity there.
        #[test]
        fn property_a_book_at_its_target_orders_nothing(held in arbitrary_holdings()) {
            prop_assert!(orders(&holding(&held), &Target::new(held)).is_empty());
        }
    }
}
