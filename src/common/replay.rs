//! Replays a strategy over archived bars: each decision bar's close is decided on and filled at the next bar's open,
//! through risk and the guard when restrained as the trader is live. The stream acts on a replay, so replaying two
//! consecutive stretches in turn equals replaying both at once.

use std::collections::BTreeMap;
use std::num::NonZeroU32;

use chrono::{DateTime, Utc};

use crate::common::book::{Book, Cash, Fill, Side, ValuationRefusal};
use crate::common::guard::{OrderGuarded, Tradability, guard};
use crate::common::laboratory::cost::{BasisPoints, CostModel, CostRefusal, FillStyle};
use crate::common::laboratory::series::{Series, SeriesRefusal};
use crate::common::market::record::{Bar, BarInterval};
use crate::common::market::state::{MarketEvent, MarketState};
use crate::common::market::{DollarVolume, Symbol};
use crate::common::monoid::{Monoid, concatenate};
use crate::common::risk::{Cut, Limits, risk};
use crate::common::strategy::{Order, Strategy, orders};
use crate::common::time::SessionDate;
use crate::common::time::calendar::TradingCalendar;

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
    /// Refused for a style whose cost turns on a fill rate the archive does not measure, or a spread charging more
    /// than the whole notional per crossing.
    pub fn new(style: FillStyle, quoted_spread: BasisPoints) -> Result<Self, CostRefusal> {
        let round_trip = CostModel::new(style, NonZeroU32::MIN).cost(quoted_spread)?;
        // A round trip crosses twice; rounded up so the grid never flatters a strategy.
        let rate = (round_trip.value() / 2.0 * (RATE_SCALE / 10_000) as f64).ceil();
        // Past the whole notional a crossing would cost more than it trades, and the charge could leave `Cash`.
        if rate > RATE_SCALE as f64 {
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

    /// The charge on `notional`, rounded up so rounding never flatters a strategy; never more than the notional.
    pub fn charge(self, notional: DollarVolume) -> DollarVolume {
        // Divided first, so a notional near the top of its range charges without overflowing.
        let (whole, part) = (notional.units() / RATE_SCALE, notional.units() % RATE_SCALE);
        let rate = u128::from(self.rate);
        DollarVolume::from_units(whole * rate + (part * rate).div_ceil(RATE_SCALE))
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

/// The strategy, its fill model, the interval it decides on and what stands between its target and its orders: what
/// acts on a replay.
pub struct Replayer<S> {
    strategy: S,
    fill_model: FillModel,
    decision: BarInterval,
    controls: Controls,
}

/// What stands between a replayed strategy's target and its orders.
#[derive(Debug, Clone, PartialEq, Eq, strum::IntoStaticStr)]
#[strum(serialize_all = "snake_case")]
pub enum Controls {
    /// Every target becomes orders as decided.
    Unrestrained,
    /// Risk and then the guard apply, as the trader applies them live.
    Restrained(Restraint),
}

/// The limits, calendar and tradability a restrained replay applies. Risk prices off the decision interval's last
/// close where the trader prices off a fresh one-minute close, a known seam.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Restraint {
    limits: Limits,
    calendar: TradingCalendar,
    tradability: BTreeMap<Symbol, Tradability>,
}

impl Restraint {
    /// A symbol `tradability` does not name trades whole and fractional shares.
    pub fn new(
        limits: Limits,
        calendar: TradingCalendar,
        tradability: BTreeMap<Symbol, Tradability>,
    ) -> Self {
        Self {
            limits,
            calendar,
            tradability,
        }
    }

    pub fn limits(&self) -> &Limits {
        &self.limits
    }

    pub fn tradability(&self) -> &BTreeMap<Symbol, Tradability> {
        &self.tradability
    }

    /// Every symbol `orders` name, read from the map or else fractionable.
    fn tradability_of(&self, orders: &[Order]) -> BTreeMap<Symbol, Tradability> {
        orders
            .iter()
            .map(|order| {
                let symbol = order.symbol().clone();
                let tradability = self
                    .tradability
                    .get(&symbol)
                    .copied()
                    .unwrap_or(Tradability::Fractionable);
                (symbol, tradability)
            })
            .collect()
    }
}

/// Why a replayer was refused its controls.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum ControlsRefusal {
    /// A daily replay decides at the close, where the limits would flatten every target.
    #[error("limits flatten the book before the close, where a daily replay decides")]
    DailyDecision,
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
    /// Under restraint, what risk cut at each decision, or why it could not judge and nothing was sent.
    restraints: BTreeMap<DateTime<Utc>, Result<Vec<Cut>, ValuationRefusal>>,
    held: Vec<(DateTime<Utc>, OrderGuarded)>,
}

/// Why a stretch of bars was refused.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReplayRefusal {
    /// Two different bars of one series end at the same instant.
    Conflicting {
        symbol: Symbol,
        interval: BarInterval,
        ends: DateTime<Utc>,
    },
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
            restraints: BTreeMap::new(),
            held: Vec::new(),
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

    /// Under restraint, what risk cut at each decision, or why it could not judge it.
    pub fn restraints(&self) -> &BTreeMap<DateTime<Utc>, Result<Vec<Cut>, ValuationRefusal>> {
        &self.restraints
    }

    /// Under restraint, each order the guard held and the decision it came from.
    pub fn held(&self) -> &[(DateTime<Utc>, OrderGuarded)] {
        &self.held
    }

    /// Each marked session's return from the previous session's last mark, or `opening` before the first, to its own
    /// last mark.
    pub fn session_returns(&self, opening: Cash) -> Result<Series, SeriesRefusal> {
        session_returns(&self.marks, opening)
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

/// The worth a session at `at` opened with: the previous session's last mark, or this session's first before any.
fn session_opening(
    marks: &BTreeMap<DateTime<Utc>, Result<Cash, ValuationRefusal>>,
    at: DateTime<Utc>,
) -> Result<Cash, ValuationRefusal> {
    let (start, _) = SessionDate::at(at).bounds();
    marks
        .range(..start)
        .next_back()
        .or_else(|| marks.range(start..).next())
        .map(|(_, mark)| mark.clone())
        .expect("the decision at `at` is marked before its session's opening is read")
}

/// Unmeasured where either end of a session went unpriced or its start was worth nothing or less.
fn session_returns(
    marks: &BTreeMap<DateTime<Utc>, Result<Cash, ValuationRefusal>>,
    opening: Cash,
) -> Result<Series, SeriesRefusal> {
    let mut ends: BTreeMap<SessionDate, Option<Cash>> = BTreeMap::new();
    for (at, mark) in marks {
        ends.insert(SessionDate::at(*at), mark.as_ref().ok().copied());
    }
    let mut start = Some(opening);
    Series::new(ends.into_iter().map(|(session, end)| {
        let reading = match (start, end) {
            (Some(start), Some(end)) => (start.units() > 0)
                .then(|| (end.units() - start.units()) as f64 / start.units() as f64),
            (None, Some(_)) | (Some(_), None) | (None, None) => None,
        };
        start = end;
        (session, reading)
    }))
}

impl<S: Strategy> Replayer<S> {
    pub fn new(strategy: S, fill_model: FillModel, decision: BarInterval) -> Self {
        Self {
            strategy,
            fill_model,
            decision,
            controls: Controls::Unrestrained,
        }
    }

    /// A replayer under `controls`, refused limits on a daily decision.
    pub fn controlled(
        strategy: S,
        fill_model: FillModel,
        decision: BarInterval,
        controls: Controls,
    ) -> Result<Self, ControlsRefusal> {
        match (&controls, decision) {
            (Controls::Restrained(_), BarInterval::OneDay) => Err(ControlsRefusal::DailyDecision),
            (Controls::Restrained(_), BarInterval::OneMinute | BarInterval::FiveMinute)
            | (Controls::Unrestrained, _) => Ok(Self {
                strategy,
                fill_model,
                decision,
                controls,
            }),
        }
    }

    pub fn controls(&self) -> &Controls {
        &self.controls
    }

    pub fn fill_model(&self) -> FillModel {
        self.fill_model
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
        // Keyed per series and instant, so a repeated bar counts once and a conflicting one cannot pick the fill.
        let mut distinct: BTreeMap<(DateTime<Utc>, Symbol, BarInterval), Bar> = BTreeMap::new();
        for bar in bars {
            let key = (bar.ends(), bar.symbol().clone(), bar.interval());
            match distinct.get(&key) {
                Some(kept) if kept != &bar => {
                    return Err(ReplayRefusal::Conflicting {
                        symbol: key.1,
                        interval: key.2,
                        ends: key.0,
                    });
                }
                Some(_) => {}
                None => {
                    distinct.insert(key, bar);
                }
            }
        }
        let mut stretch: BTreeMap<DateTime<Utc>, Vec<Bar>> = BTreeMap::new();
        for ((ends, _, _), bar) in distinct {
            stretch.entry(ends).or_default().push(bar);
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
            let wanted = self.strategy.decide(state, &replay.book);
            replay.pending = match &self.controls {
                Controls::Unrestrained => orders(&replay.book, &wanted),
                Controls::Restrained(restraint) => {
                    let price = |symbol: &Symbol| state.last_price(symbol, self.decision);
                    let restrained = session_opening(&replay.marks, ends).and_then(|opening| {
                        risk(
                            &restraint.limits,
                            restraint.calendar.phase_at(ends),
                            opening,
                            &replay.book,
                            price,
                            wanted,
                        )
                    });
                    match restrained {
                        Ok(restrained) => {
                            replay
                                .restraints
                                .insert(ends, Ok(restrained.cuts().to_vec()));
                            let wanted = orders(&replay.book, restrained.target());
                            let tradability = restraint.tradability_of(&wanted);
                            let guarded = guard(wanted, &tradability, price);
                            replay
                                .held
                                .extend(guarded.held().iter().cloned().map(|held| (ends, held)));
                            guarded.passed().to_vec()
                        }
                        Err(refusal) => {
                            replay.restraints.insert(ends, Err(refusal));
                            Vec::new()
                        }
                    }
                }
            };
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
        let cost = self.fill_model.charge(notional);
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
        )
        .expect("a charge is at most its notional"))
    }
}

#[cfg(test)]
mod tests {
    use chrono::{NaiveDate, TimeDelta};
    use proptest::prelude::*;
    use proptest::strategy::Strategy as _;

    use super::*;
    use crate::common::market::record::BarPrices;
    use crate::common::market::{Price, Shares};
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

    fn cash(whole: i128) -> Cash {
        Cash::from_units(whole * 1_000_000_000_000)
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
            BarPrices::new(open, open.max(close), open.min(close), close).unwrap(),
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

    /// Monday 2026-09-21, `minutes` before its 16:00 Eastern close.
    fn before_close(minutes: i64) -> DateTime<Utc> {
        day(0) - TimeDelta::minutes(minutes)
    }

    /// $1,000 on every dollar limit, flat for the last 15 minutes of a 09:30 to 16:00 Monday.
    fn restraint(tradability: BTreeMap<Symbol, Tradability>) -> Controls {
        let monday = SessionDate::at(day(0));
        let session = crate::common::time::calendar::TradingSession::new(
            monday,
            chrono::NaiveTime::from_hms_opt(9, 30, 0).unwrap(),
            chrono::NaiveTime::from_hms_opt(16, 0, 0).unwrap(),
        )
        .unwrap();
        let calendar = TradingCalendar::new(
            vec![session],
            crate::common::time::SessionRange::new(monday, monday).unwrap(),
        )
        .unwrap();
        let limits = Limits::new(
            cash(1_000),
            cash(1_000),
            cash(1_000),
            TimeDelta::minutes(15),
        )
        .unwrap();
        Controls::Restrained(Restraint::new(limits, calendar, tradability))
    }

    /// One share of AAPL wanted at a minute bar's close and filled at the next one's open.
    fn one_minute_of(controls: Controls, minutes_before_close: i64) -> Replay {
        let replayer = Replayer::controlled(
            Hold(vec![("AAPL", 1)]),
            free(),
            BarInterval::OneMinute,
            controls,
        )
        .unwrap();
        let first = before_close(minutes_before_close);
        let bars = [
            bar("AAPL", BarInterval::OneMinute, first, 1000, 1000),
            bar(
                "AAPL",
                BarInterval::OneMinute,
                first + TimeDelta::minutes(1),
                1000,
                1000,
            ),
        ];
        replayer
            .act(Replay::open(Book::funded(cash(100))), bars)
            .unwrap()
            .finish()
    }

    #[test]
    fn test_limits_are_refused_on_a_daily_decision() {
        let refused = Replayer::controlled(
            Hold(vec![]),
            free(),
            BarInterval::OneDay,
            restraint(BTreeMap::new()),
        );
        assert_eq!(refused.err(), Some(ControlsRefusal::DailyDecision));
        let unrestrained = Replayer::controlled(
            Hold(vec![]),
            free(),
            BarInterval::OneDay,
            Controls::Unrestrained,
        );
        assert!(unrestrained.is_ok());
    }

    /// Mid-session the wanted share fills as it would unrestrained; inside the flat window risk empties the target.
    #[test]
    fn test_a_restrained_replay_goes_flat_before_the_close() {
        let midday = one_minute_of(restraint(BTreeMap::new()), 240);
        assert_eq!(midday.fills().len(), 1);
        assert_eq!(midday.restraints().values().next(), Some(&Ok(Vec::new())));
        let late = one_minute_of(restraint(BTreeMap::new()), 10);
        assert_eq!(late.fills().len(), 0);
        let cuts = late.restraints().values().next().unwrap().as_ref().unwrap();
        assert!(matches!(
            cuts.as_slice(),
            [Cut::OutsideTradingWindow { .. }]
        ));
        assert_eq!(one_minute_of(Controls::Unrestrained, 10).fills().len(), 1);
    }

    /// The guard holds the untradable buy at each decision, as the trader retries it each tick.
    #[test]
    fn test_a_restrained_replay_holds_what_the_guard_holds() {
        let untradable = BTreeMap::from([(symbol("AAPL"), Tradability::Untradable)]);
        let replay = one_minute_of(restraint(untradable), 240);
        assert_eq!(replay.fills().len(), 0);
        let held: Vec<_> = replay.held().iter().map(|(at, _)| *at).collect();
        assert_eq!(held, [before_close(239), before_close(238)]);
    }

    /// A session opens at the previous session's last mark, and the first session at its own first mark.
    #[test]
    fn test_a_session_opens_at_the_last_mark_before_it() {
        let units = |units: i128| Ok(Cash::from_units(units));
        let marks = BTreeMap::from([
            (day(0) - TimeDelta::hours(5), units(100)),
            (day(0) - TimeDelta::hours(1), units(90)),
            (day(1) - TimeDelta::hours(5), units(80)),
        ]);
        assert_eq!(
            session_opening(&marks, day(0) - TimeDelta::hours(1)),
            units(100)
        );
        assert_eq!(
            session_opening(&marks, day(1) - TimeDelta::hours(5)),
            units(90)
        );
    }

    /// A session runs from the last mark before it to its own last; an unpriced end leaves it and the next session
    /// unmeasured, and so does a start worth nothing or less.
    #[test]
    fn test_a_session_returns_from_the_last_mark_before_it() {
        let unpriced = || {
            Err(ValuationRefusal::Unpriced {
                symbol: symbol("AAPL"),
            })
        };
        let units = |units: i128| Ok(Cash::from_units(units));
        let marks = BTreeMap::from([
            (day(0) - TimeDelta::hours(2), units(105_000_000_000_000)),
            (day(0), units(110_000_000_000_000)),
            (day(1), unpriced()),
            (day(2), units(121_000_000_000_000)),
            (day(3), units(133_100_000_000_000)),
            (day(4), units(0)),
            (day(5), units(5_000_000_000_000)),
        ]);
        let returns = session_returns(&marks, cash(100)).unwrap();
        let readings: Vec<_> = returns.readings().values().copied().collect();
        assert_eq!(
            readings,
            [Some(0.1), None, None, Some(0.1), Some(-1.0), None]
        );
        let sessions: Vec<_> = returns.readings().keys().copied().collect();
        assert_eq!(
            sessions,
            (0..6)
                .map(|offset| SessionDate::at(day(offset)))
                .collect::<Vec<_>>()
        );
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
                Replay::open(Book::funded(cash(100))),
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
                Replay::open(Book::funded(cash(100))),
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
                Replay::open(Book::funded(cash(100))),
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
                Replay::open(Book::funded(cash(100))),
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
                Replay::open(Book::funded(cash(100))),
                [
                    daily("AAPL", 0, 10_000_000, 10_000_000),
                    daily("AAPL", 1, 10_000_000, 10_000_000),
                ],
            )
            .unwrap();
        assert_eq!(replay.fills().len(), 1);
        assert_eq!(replay.book().cash(), Cash::empty());
    }

    /// Repeated identical bars count once; two different bars of one series at one instant are refused.
    #[test]
    fn test_conflicting_bars_are_refused_and_repeats_count_once() {
        let replayer = replayer(Hold(vec![("AAPL", 1)]));
        let opening = Replay::open(Book::funded(cash(100)));
        let once = replayer
            .act(
                opening.clone(),
                [
                    daily("AAPL", 0, 1_000_000, 1_000_000),
                    daily("AAPL", 1, 1_000_000, 1_000_000),
                ],
            )
            .unwrap();
        let twice = replayer
            .act(
                opening.clone(),
                [
                    daily("AAPL", 0, 1_000_000, 1_000_000),
                    daily("AAPL", 1, 1_000_000, 1_000_000),
                    daily("AAPL", 1, 1_000_000, 1_000_000),
                ],
            )
            .unwrap();
        assert_eq!(once, twice);
        assert_eq!(
            replayer.act(
                opening,
                [
                    daily("AAPL", 1, 2_000_000, 1_000_000),
                    daily("AAPL", 1, 1_000_000, 1_000_000)
                ]
            ),
            Err(ReplayRefusal::Conflicting {
                symbol: symbol("AAPL"),
                interval: BarInterval::OneDay,
                ends: day(1),
            })
        );
    }

    /// A notional near the top of its range charges without overflowing, and a spread past the notional is refused.
    #[test]
    fn test_a_charge_is_bounded_by_its_notional() {
        let whole =
            FillModel::new(FillStyle::Aggressive, BasisPoints::new(20_000.0).unwrap()).unwrap();
        let notional = DollarVolume::from_units(u128::MAX / 2);
        assert_eq!(whole.charge(notional), notional);
        let model = FillModel::new(FillStyle::Aggressive, BasisPoints::new(10.0).unwrap()).unwrap();
        assert_eq!(
            model.charge(notional),
            DollarVolume::from_units((u128::MAX / 2).div_ceil(2_000))
        );
        assert_eq!(
            FillModel::new(FillStyle::Aggressive, BasisPoints::new(20_000.1).unwrap()),
            Err(CostRefusal::Unrepresentable {
                quoted_spread: BasisPoints::new(20_000.1).unwrap(),
                names: NonZeroU32::MIN,
            })
        );
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
        /// Replaying a stream in two consecutive stretches, or with every bar repeated, equals replaying it whole, and no
        /// bars change nothing.
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
            let opening = Replay::open(Book::funded(cash(1_000)));
            let (before, after): (Vec<_>, Vec<_>) =
                bars.iter().cloned().partition(|bar| bar.ends() <= day(split));
            let stretched = replayer
                .act(replayer.act(opening.clone(), before).unwrap(), after)
                .unwrap();
            let repeated = bars.iter().chain(&bars).cloned().collect::<Vec<_>>();
            prop_assert_eq!(&replayer.act(opening.clone(), repeated).unwrap(), &stretched);
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
            let opening = Book::funded(cash(1_000));
            let replay = replayer.act(Replay::open(opening.clone()), bars).unwrap();
            prop_assert_eq!(
                replay.book(),
                &opening.combine(concatenate(replay.fills().iter().map(Book::of)))
            );
        }
    }
}
