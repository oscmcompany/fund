//! Replays a strategy over archived bars: each decision bar's close is decided on and filled at the next bar's open.
//! The stream acts on a replay, so replaying two consecutive stretches in turn equals replaying both at once.

use std::collections::BTreeMap;
use std::num::NonZeroU32;

use chrono::{DateTime, Utc};

use crate::common::book::{Book, Cash, Fill, Side, ValuationRefusal};
use crate::common::laboratory::cost::{BasisPoints, CostModel, CostRefusal, FillStyle};
use crate::common::market::DollarVolume;
use crate::common::market::record::{Bar, BarInterval};
use crate::common::market::state::{MarketEvent, MarketState};
use crate::common::monoid::{Monoid, concatenate};
use crate::common::strategy::{Order, Strategy, orders};

/// The grid a crossing's charge is rounded to: hundred-millionths of the notional, so a basis point is 10,000.
const RATE_SCALE: u128 = 100_000_000;

/// What one crossing charges: half the quoted spread's round trip, on a grid of hundred-millionths of the notional.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct FillModel {
    style: FillStyle,
    quoted_spread: BasisPoints,
    rate: u64,
}

impl FillModel {
    /// Refused for a style whose cost turns on a fill rate the archive does not measure.
    pub fn new(style: FillStyle, quoted_spread: BasisPoints) -> Result<Self, CostRefusal> {
        let round_trip = CostModel::new(style, NonZeroU32::MIN).cost(quoted_spread)?;
        // A round trip crosses twice; rounded up so the grid never flatters a strategy.
        let rate = (round_trip.value() / 2.0 * (RATE_SCALE / 10_000) as f64).ceil();
        if rate >= u64::MAX as f64 {
            return Err(CostRefusal::Unrepresentable {
                quoted_spread,
                names: NonZeroU32::MIN,
            });
        }
        let rate = rate as u64;
        Ok(Self {
            style,
            quoted_spread,
            rate,
        })
    }

    pub fn style(self) -> FillStyle {
        self.style
    }

    pub fn quoted_spread(self) -> BasisPoints {
        self.quoted_spread
    }

    /// The charge on `notional`, rounded up so rounding never flatters a strategy.
    pub fn charge(self, notional: DollarVolume) -> DollarVolume {
        let scaled = notional
            .units()
            .checked_mul(u128::from(self.rate))
            .expect("a charge fits u128");
        DollarVolume::from_units(scaled.div_ceil(RATE_SCALE))
    }
}

/// Why an order went unfilled.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UnfilledCause {
    /// The next instant to close a decision bar held none of the symbol to fill against.
    NoBar,
    /// A buy needed `needed` with its charge and the book held `held`.
    InsufficientCash { needed: DollarVolume, held: Cash },
    /// The stream ended before another bar arrived.
    EndOfData,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Unfilled {
    order: Order,
    /// The instant the order was due to fill, or the last instant replayed when the stream ended.
    at: DateTime<Utc>,
    cause: UnfilledCause,
}

impl Unfilled {
    pub fn order(&self) -> &Order {
        &self.order
    }

    pub fn at(&self) -> DateTime<Utc> {
        self.at
    }

    pub fn cause(&self) -> &UnfilledCause {
        &self.cause
    }
}

/// The strategy, its fill model and the interval it decides on: what acts on a replay.
pub struct Replayer<S> {
    strategy: S,
    fills: FillModel,
    decision: BarInterval,
}

/// A replay so far: the state and book it has reached, the orders awaiting the next bar, and what happened.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Replay {
    state: MarketState,
    book: Book,
    pending: Vec<Order>,
    /// The latest instant replayed, after which the next stretch must start.
    reached: Option<DateTime<Utc>>,
    fills: Vec<Fill>,
    unfilled: Vec<Unfilled>,
    marks: BTreeMap<DateTime<Utc>, Result<Cash, ValuationRefusal>>,
}

/// Why a stretch of bars was refused.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReplayRefusal {
    /// A bar ends at or before the latest instant already replayed.
    OutOfOrder {
        ends: DateTime<Utc>,
        reached: DateTime<Utc>,
    },
}

impl Replay {
    /// A replay that has seen nothing, holding `book`.
    pub fn open(book: Book) -> Self {
        Self {
            state: MarketState::empty(),
            book,
            pending: Vec::new(),
            reached: None,
            fills: Vec::new(),
            unfilled: Vec::new(),
            marks: BTreeMap::new(),
        }
    }

    /// The book, equal to the opening combined with every fill's `Book::of`.
    pub fn book(&self) -> &Book {
        &self.book
    }

    pub fn fills(&self) -> &[Fill] {
        &self.fills
    }

    pub fn unfilled(&self) -> &[Unfilled] {
        &self.unfilled
    }

    /// The book marked at each decision, to the decision interval's closes, before that decision's orders fill.
    pub fn marks(&self) -> &BTreeMap<DateTime<Utc>, Result<Cash, ValuationRefusal>> {
        &self.marks
    }

    /// Closes the replay: orders still pending are recorded unfilled at the end of data.
    pub fn finish(mut self) -> Self {
        if let Some(at) = self.reached {
            self.unfilled
                .extend(self.pending.drain(..).map(|order| Unfilled {
                    order,
                    at,
                    cause: UnfilledCause::EndOfData,
                }));
        }
        self
    }
}

impl<S: Strategy> Replayer<S> {
    pub fn new(strategy: S, fills: FillModel, decision: BarInterval) -> Self {
        Self {
            strategy,
            fills,
            decision,
        }
    }

    pub fn fills(&self) -> FillModel {
        self.fills
    }

    pub fn decision(&self) -> BarInterval {
        self.decision
    }

    /// Replays `bars` from where `replay` stopped, grouped by the instant each ends; refused, unchanged, when a bar
    /// ends no later than what was already replayed.
    pub fn act(
        &self,
        replay: Replay,
        bars: impl IntoIterator<Item = Bar>,
    ) -> Result<Replay, ReplayRefusal> {
        let mut stretch: BTreeMap<DateTime<Utc>, Vec<Bar>> = BTreeMap::new();
        for bar in bars {
            stretch.entry(bar.ends()).or_default().push(bar);
        }
        if let (Some(reached), Some(&ends)) = (replay.reached, stretch.keys().next())
            && ends <= reached
        {
            return Err(ReplayRefusal::OutOfOrder { ends, reached });
        }
        Ok(stretch
            .into_iter()
            .fold(replay, |replay, (ends, bars)| self.step(replay, ends, bars)))
    }

    /// When a decision bar closes here, fills the pending orders at these bars' opens, folds the bars in and decides.
    fn step(&self, mut replay: Replay, ends: DateTime<Utc>, bars: Vec<Bar>) -> Replay {
        let decides = bars.iter().any(|bar| bar.interval() == self.decision);
        let pending = if decides {
            std::mem::take(&mut replay.pending)
        } else {
            Vec::new()
        };
        for order in pending {
            let opening = bars
                .iter()
                .find(|bar| bar.symbol() == order.symbol() && bar.interval() == self.decision);
            match self.fill(&replay.book, order, opening, ends) {
                Ok(fill) => {
                    replay.book = replay.book.combine(Book::of(&fill));
                    replay.fills.push(fill);
                }
                Err(unfilled) => replay.unfilled.push(unfilled),
            }
        }
        replay.state = replay.state.combine(concatenate(
            bars.into_iter()
                .map(MarketEvent::Bar)
                .chain([MarketEvent::Clock(ends)])
                .map(MarketState::of),
        ));
        replay.reached = Some(ends);
        if decides {
            let state = &replay.state;
            let mark = replay
                .book
                .value(|symbol| state.last_price(symbol, self.decision));
            replay.marks.insert(ends, mark);
            replay.pending = orders(&replay.book, &self.strategy.decide(state, &replay.book));
        }
        replay
    }

    fn fill(
        &self,
        book: &Book,
        order: Order,
        opening: Option<&Bar>,
        at: DateTime<Utc>,
    ) -> Result<Fill, Unfilled> {
        let Some(bar) = opening else {
            return Err(Unfilled {
                order,
                at,
                cause: UnfilledCause::NoBar,
            });
        };
        let price = bar.prices().open();
        let notional = DollarVolume::of(price, order.shares());
        let cost = self.fills.charge(notional);
        match order.side() {
            Side::Buy => {
                let needed = notional.plus(cost);
                let held = book.cash();
                if i128::try_from(needed.units()).map_or(true, |units| units > held.units()) {
                    return Err(Unfilled {
                        order,
                        at,
                        cause: UnfilledCause::InsufficientCash { needed, held },
                    });
                }
            }
            Side::Sell => {}
        }
        Ok(Fill::new(
            bar.timestamp(),
            order.symbol().clone(),
            order.side(),
            order.shares(),
            price,
            cost,
        ))
    }
}

#[cfg(test)]
mod tests {
    use chrono::{NaiveDate, TimeDelta};
    use proptest::prelude::*;
    use proptest::strategy::Strategy as _;

    use super::*;
    use crate::common::market::record::Ohlc;
    use crate::common::market::{Price, Shares, Symbol};
    use crate::common::strategy::Strategy;
    use crate::common::strategy::Target;
    use crate::common::time::SessionDate;

    /// Wants `shares` of each named symbol, whatever it has seen.
    struct Hold(Vec<(&'static str, u64)>);

    impl Strategy for Hold {
        fn decide(&self, _: &MarketState, _: &Book) -> Target {
            Target::new(
                self.0
                    .iter()
                    .map(|(raw, shares)| (symbol(raw), Shares::whole(*shares).unwrap()))
                    .collect(),
            )
        }
    }

    /// Holds one share of AAPL while its last daily close is an even number of ticks, so each decision reads state.
    struct Even;

    impl Strategy for Even {
        fn decide(&self, state: &MarketState, _: &Book) -> Target {
            let even = state
                .last_price(&symbol("AAPL"), BarInterval::OneDay)
                .is_some_and(|price| price.ticks() % 2 == 0);
            Target::new(BTreeMap::from([(
                symbol("AAPL"),
                Shares::whole(u64::from(even)).unwrap(),
            )]))
        }
    }

    fn symbol(raw: &str) -> Symbol {
        Symbol::new(raw).unwrap()
    }

    fn dollars(units: u128) -> DollarVolume {
        DollarVolume::from_units(units * 1_000_000_000_000)
    }

    fn day(day: i64) -> DateTime<Utc> {
        SessionDate::from_date(NaiveDate::from_ymd_opt(2026, 9, 21).unwrap())
            .plus_calendar_days(day)
            .regular_close()
    }

    fn bar(
        raw: &str,
        interval: BarInterval,
        timestamp: DateTime<Utc>,
        open: i64,
        close: i64,
    ) -> Bar {
        let (open, close) = (
            Price::from_ticks(open).unwrap(),
            Price::from_ticks(close).unwrap(),
        );
        Bar::new(
            symbol(raw),
            interval,
            timestamp,
            Ohlc::new(open, open.max(close), open.min(close), close).unwrap(),
            Shares::whole(100).unwrap(),
            None,
            None,
        )
        .unwrap()
    }

    fn daily(raw: &str, index: i64, open: i64, close: i64) -> Bar {
        bar(raw, BarInterval::OneDay, day(index), open, close)
    }

    fn free() -> FillModel {
        FillModel::new(FillStyle::Aggressive, BasisPoints::new(0.0).unwrap()).unwrap()
    }

    fn replayer<S: Strategy>(strategy: S) -> Replayer<S> {
        Replayer::new(strategy, free(), BarInterval::OneDay)
    }

    /// A 10bp spread charges 5bp a crossing, rounded up to the next unit.
    #[test]
    fn test_a_crossing_charges_half_the_quoted_spread() {
        let model = FillModel::new(FillStyle::Aggressive, BasisPoints::new(10.0).unwrap()).unwrap();
        assert_eq!(model.charge(dollars(10_000)), dollars(5));
        assert_eq!(
            model.charge(DollarVolume::from_units(1)),
            DollarVolume::from_units(1)
        );
        assert_eq!(
            model.charge(DollarVolume::from_units(0)),
            DollarVolume::from_units(0)
        );
        // A twentieth of a grid step still charges one step rather than rounding to free.
        let tiny =
            FillModel::new(FillStyle::Aggressive, BasisPoints::new(0.00001).unwrap()).unwrap();
        assert_eq!(
            tiny.charge(DollarVolume::from_units(100_000_000)),
            DollarVolume::from_units(1)
        );
        assert_eq!(
            FillModel::new(FillStyle::Aggressive, BasisPoints::new(1e30).unwrap()),
            Err(CostRefusal::Unrepresentable {
                quoted_spread: BasisPoints::new(1e30).unwrap(),
                names: NonZeroU32::MIN,
            })
        );
        assert_eq!(
            FillModel::new(FillStyle::Passive, BasisPoints::new(10.0).unwrap()),
            Err(CostRefusal::FillRateUnmeasured {
                style: FillStyle::Passive,
                quoted_spread: BasisPoints::new(10.0).unwrap(),
            })
        );
    }

    /// Decided on day 0's close, filled at day 1's open, and marked to each close before that close's orders fill.
    #[test]
    fn test_a_decision_fills_at_the_next_open() {
        let replay = replayer(Hold(vec![("AAPL", 1)]))
            .act(
                Replay::open(Book::funded(dollars(100))),
                [
                    daily("AAPL", 0, 10_000_000, 11_000_000),
                    daily("AAPL", 1, 12_000_000, 13_000_000),
                ],
            )
            .unwrap();
        let fills: Vec<_> = replay
            .fills()
            .iter()
            .map(|fill| (fill.filled_against(), fill.side(), fill.price().ticks()))
            .collect();
        assert_eq!(fills, [(day(1), Side::Buy, 12_000_000)]);
        let marks: Vec<_> = replay
            .marks()
            .iter()
            .map(|(at, mark)| (*at, mark.clone()))
            .collect();
        let cash = |whole: i128| Ok(Cash::from_units(whole * 1_000_000_000_000));
        assert_eq!(marks, [(day(0), cash(100)), (day(1), cash(100 - 12 + 13))]);
        assert!(replay.unfilled().is_empty());
    }

    /// A minute strategy decides on each minute's close and fills at the next minute's open.
    #[test]
    fn test_an_intraday_decision_fills_at_the_next_bar() {
        let open: DateTime<Utc> = "2026-09-21T13:30:00Z".parse().unwrap();
        let minute = |index: i64, price: i64| {
            bar(
                "AAPL",
                BarInterval::OneMinute,
                open + TimeDelta::minutes(index),
                price,
                price,
            )
        };
        let replay = Replayer::new(Hold(vec![("AAPL", 1)]), free(), BarInterval::OneMinute)
            .act(
                Replay::open(Book::funded(dollars(100))),
                [
                    minute(0, 10_000_000),
                    minute(1, 11_000_000),
                    daily("AAPL", 0, 9_000_000, 9_000_000),
                ],
            )
            .unwrap();
        let fills: Vec<_> = replay
            .fills()
            .iter()
            .map(|fill| (fill.filled_against(), fill.price().ticks()))
            .collect();
        assert_eq!(fills, [(open + TimeDelta::minutes(1), 11_000_000)]);
        assert_eq!(
            replay.marks().keys().copied().collect::<Vec<_>>(),
            [open + TimeDelta::minutes(1), open + TimeDelta::minutes(2)]
        );
    }

    /// A daily decision waits through the next session's minute bars and fills at its daily open.
    #[test]
    fn test_a_daily_decision_fills_past_bars_of_another_interval() {
        let minute = bar(
            "AAPL",
            BarInterval::OneMinute,
            "2026-09-22T13:30:00Z".parse().unwrap(),
            1,
            1,
        );
        let replay = replayer(Hold(vec![("AAPL", 1)]))
            .act(
                Replay::open(Book::funded(dollars(100))),
                [
                    daily("AAPL", 0, 10_000_000, 10_000_000),
                    minute,
                    daily("AAPL", 1, 12_000_000, 12_000_000),
                ],
            )
            .unwrap();
        let fills: Vec<_> = replay
            .fills()
            .iter()
            .map(|fill| (fill.filled_against(), fill.price().ticks()))
            .collect();
        assert_eq!(fills, [(day(1), 12_000_000)]);
        assert!(replay.unfilled().is_empty());
    }

    #[test]
    fn test_an_order_without_a_bar_or_cash_goes_unfilled_with_its_cause() {
        let replay = replayer(Hold(vec![("AAPL", 1), ("MSFT", 50)]))
            .act(
                Replay::open(Book::funded(dollars(100))),
                [
                    daily("AAPL", 0, 10_000_000, 10_000_000),
                    daily("MSFT", 0, 10_000_000, 10_000_000),
                    daily("MSFT", 1, 10_000_000, 10_000_000),
                ],
            )
            .unwrap()
            .finish();
        let causes: Vec<_> = replay
            .unfilled()
            .iter()
            .map(|unfilled| {
                (
                    unfilled.order().symbol().as_str(),
                    unfilled.at(),
                    unfilled.cause().clone(),
                )
            })
            .collect();
        assert_eq!(
            causes,
            [
                ("AAPL", day(1), UnfilledCause::NoBar),
                (
                    "MSFT",
                    day(1),
                    UnfilledCause::InsufficientCash {
                        needed: dollars(500),
                        held: Cash::from_units(100 * 1_000_000_000_000),
                    }
                ),
                ("AAPL", day(1), UnfilledCause::EndOfData),
                ("MSFT", day(1), UnfilledCause::EndOfData),
            ]
        );
        assert!(replay.fills().is_empty());
    }

    /// A buy costing exactly the cash held fills; a tick more would not.
    #[test]
    fn test_a_buy_of_exactly_the_cash_held_fills() {
        let replay = replayer(Hold(vec![("AAPL", 10)]))
            .act(
                Replay::open(Book::funded(dollars(100))),
                [
                    daily("AAPL", 0, 10_000_000, 10_000_000),
                    daily("AAPL", 1, 10_000_000, 10_000_000),
                ],
            )
            .unwrap();
        assert_eq!(replay.fills().len(), 1);
        assert_eq!(replay.book().cash(), Cash::empty());
    }

    #[test]
    fn test_a_stretch_ending_before_the_replay_is_refused() {
        let replayer = replayer(Hold(vec![]));
        let replay = replayer
            .act(Replay::open(Book::empty()), [daily("AAPL", 1, 1, 1)])
            .unwrap();
        assert_eq!(
            replayer.act(replay, [daily("AAPL", 1, 1, 1)]),
            Err(ReplayRefusal::OutOfOrder {
                ends: day(1),
                reached: day(1)
            })
        );
    }

    /// Daily AAPL and MSFT bars over `days` days with arbitrary opens and closes.
    fn arbitrary_stream(days: usize) -> impl prop::strategy::Strategy<Value = Vec<Bar>> {
        prop::collection::vec((1..50_000_000i64, 1..50_000_000i64, prop::bool::ANY), days).prop_map(
            |sessions| {
                sessions
                    .into_iter()
                    .enumerate()
                    .flat_map(|(index, (open, close, msft))| {
                        let index = index as i64;
                        let aapl = daily("AAPL", index, open, close);
                        let msft = msft.then(|| daily("MSFT", index, close, open));
                        [Some(aapl), msft].into_iter().flatten()
                    })
                    .collect()
            },
        )
    }

    proptest! {
        /// Replaying a stream in two consecutive stretches equals replaying it whole, and no bars change nothing.
        #[test]
        fn property_replaying_in_stretches_equals_replaying_whole(
            bars in arbitrary_stream(12),
            split in 0..12i64,
            spread in 0.0..50.0f64,
        ) {
            let replayer = Replayer::new(
                Even,
                FillModel::new(FillStyle::Aggressive, BasisPoints::new(spread).unwrap()).unwrap(),
                BarInterval::OneDay,
            );
            let opening = Replay::open(Book::funded(dollars(1_000)));
            let (before, after): (Vec<_>, Vec<_>) =
                bars.iter().cloned().partition(|bar| bar.ends() <= day(split));
            let stretched = replayer
                .act(replayer.act(opening.clone(), before).unwrap(), after)
                .unwrap();
            let whole = replayer.act(opening.clone(), bars).unwrap();
            prop_assert_eq!(&stretched, &whole);
            prop_assert_eq!(replayer.act(whole.clone(), []).unwrap(), whole);
        }

        /// The book a replay reaches is its opening combined with each fill, so no cash appears outside a fill.
        #[test]
        fn property_the_book_is_the_opening_and_its_fills(
            bars in arbitrary_stream(12),
            spread in 0.0..50.0f64,
        ) {
            let replayer = Replayer::new(
                Even,
                FillModel::new(FillStyle::Aggressive, BasisPoints::new(spread).unwrap()).unwrap(),
                BarInterval::OneDay,
            );
            let opening = Book::funded(dollars(1_000));
            let replay = replayer.act(Replay::open(opening.clone()), bars).unwrap();
            prop_assert_eq!(
                replay.book(),
                &opening.combine(concatenate(replay.fills().iter().map(Book::of)))
            );
        }
    }
}
