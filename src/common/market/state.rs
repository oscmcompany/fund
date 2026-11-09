//! What the trader knows of the market at an instant, folded from the events seen so far. A monoid, so states folded
//! from separate chunks of one stream combine into the state of the whole.

use std::collections::BTreeMap;

use chrono::{DateTime, TimeDelta, Utc};
use serde::{Deserialize, Serialize};

use super::record::{BarInterval, Quote, QuoteRefusal};
use super::trade_bars::TradeBar;
use super::{Price, StampedPrice, Symbol};
use crate::common::monoid::Monoid;

/// The latest bars a state keeps for each symbol and interval.
pub const RETAINED_BARS: usize = 100;

/// One input to the fold: time arrives as an event like any other, never from a clock the fold reads.
#[derive(Debug, Clone, PartialEq)]
pub enum MarketEvent {
    /// A bar built from the tape's prints, as the archive derives them and the trader builds them live.
    Trades(TradeBar),
    /// A top-of-book quote, of which the state keeps each symbol's latest.
    Quote(Quote),
    Clock(DateTime<Utc>),
}

/// A symbol's latest quote as a state holds it, its bid never above its ask; ordered by when it was quoted first, so a
/// combine keeps the latest and breaks a tie on the sides.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(try_from = "UncheckedLastQuote")]
pub struct LastQuote {
    quoted_at: DateTime<Utc>,
    bid: Price,
    ask: Price,
}

/// A `LastQuote` as read, before its sides are checked.
#[derive(Deserialize)]
struct UncheckedLastQuote {
    quoted_at: DateTime<Utc>,
    bid: Price,
    ask: Price,
}

impl TryFrom<UncheckedLastQuote> for LastQuote {
    type Error = QuoteRefusal;

    fn try_from(unchecked: UncheckedLastQuote) -> Result<Self, Self::Error> {
        QuoteRefusal::check(unchecked.bid, unchecked.ask)?;
        Ok(Self {
            quoted_at: unchecked.quoted_at,
            bid: unchecked.bid,
            ask: unchecked.ask,
        })
    }
}

impl LastQuote {
    pub fn of(quote: &Quote) -> Self {
        Self {
            quoted_at: quote.timestamp(),
            bid: quote.bid(),
            ask: quote.ask(),
        }
    }

    pub fn quoted_at(self) -> DateTime<Utc> {
        self.quoted_at
    }

    pub fn bid(self) -> Price {
        self.bid
    }

    pub fn ask(self) -> Price {
        self.ask
    }
}

/// What one bar leaves in the state; ordered so a repeated timestamp keeps the greater and the combine commutes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct Retained {
    /// The close and the instant its closing print was set; `None` for a bar no print was allowed to price, such as a
    /// minute of odd lots.
    close: Option<StampedPrice>,
}

/// How old a close may be before risk treats it as unpriced: whole seconds, so a journal records it exactly, and at least a
/// minute, since a bar is folded only after its minute ends.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Staleness(TimeDelta);

/// Why a staleness was refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum StalenessRefusal {
    #[error("a staleness of {stale_after} is under a minute")]
    UnderAMinute { stale_after: TimeDelta },
    #[error("a staleness of {stale_after} is not whole seconds")]
    Fractional { stale_after: TimeDelta },
}

impl Staleness {
    pub fn new(stale_after: TimeDelta) -> Result<Self, StalenessRefusal> {
        match (
            stale_after < TimeDelta::minutes(1),
            stale_after.subsec_nanos() != 0,
        ) {
            (true, _) => Err(StalenessRefusal::UnderAMinute { stale_after }),
            (false, true) => Err(StalenessRefusal::Fractional { stale_after }),
            (false, false) => Ok(Self(stale_after)),
        }
    }

    pub fn get(self) -> TimeDelta {
        self.0
    }

    /// The staleness in seconds, exact since it holds whole seconds.
    pub fn seconds(self) -> i64 {
        self.0.num_seconds()
    }
}

/// The latest clock, each series' latest `RETAINED_BARS` bars, and each symbol's latest quote.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MarketState {
    /// The latest instant a clock event reported; a bar's timestamp names its period, not when it was seen.
    clock: Option<DateTime<Utc>>,
    series: BTreeMap<(Symbol, BarInterval), BTreeMap<DateTime<Utc>, Retained>>,
    quotes: BTreeMap<Symbol, LastQuote>,
}

impl MarketState {
    /// One event as a state, so a stream folds as `concatenate(events.map(MarketState::of))`.
    pub fn of(event: MarketEvent) -> Self {
        match event {
            MarketEvent::Clock(instant) => Self {
                clock: Some(instant),
                ..Self::default()
            },
            MarketEvent::Quote(quote) => Self {
                quotes: BTreeMap::from([(quote.symbol().clone(), LastQuote::of(&quote))]),
                ..Self::default()
            },
            MarketEvent::Trades(bar) => Self::retaining(
                bar.symbol(),
                bar.interval(),
                bar.timestamp(),
                Retained {
                    close: bar.sums().open_close().map(|prices| prices.close()),
                },
            ),
        }
    }

    fn retaining(
        symbol: &Symbol,
        interval: BarInterval,
        timestamp: DateTime<Utc>,
        retained: Retained,
    ) -> Self {
        Self {
            series: BTreeMap::from([(
                (symbol.clone(), interval),
                BTreeMap::from([(timestamp, retained)]),
            )]),
            ..Self::default()
        }
    }

    pub fn clock(&self) -> Option<DateTime<Utc>> {
        self.clock
    }

    /// The close of the series' latest priced bar, `None` when no retained bar of it has a close.
    pub fn last_price(&self, symbol: &Symbol, interval: BarInterval) -> Option<Price> {
        self.last_close(symbol, interval).map(StampedPrice::price)
    }

    /// `last_price`, unless the print that set it is more than `stale_after` before `now`.
    pub fn fresh_price(
        &self,
        symbol: &Symbol,
        interval: BarInterval,
        now: DateTime<Utc>,
        stale_after: Staleness,
    ) -> Option<Price> {
        let close = self.last_close(symbol, interval)?;
        (now - close.at() <= stale_after.get()).then_some(close.price())
    }

    /// `last_price` with the instant its closing print was set, so a caller can judge how old it is.
    pub fn last_close(&self, symbol: &Symbol, interval: BarInterval) -> Option<StampedPrice> {
        self.bars(symbol, interval)?
            .values()
            .rev()
            .find_map(|retained| retained.close)
    }

    /// Each symbol's latest quote; a symbol is absent only when none of its quotes was folded.
    pub fn quotes(&self) -> &BTreeMap<Symbol, LastQuote> {
        &self.quotes
    }

    fn bars(
        &self,
        symbol: &Symbol,
        interval: BarInterval,
    ) -> Option<&BTreeMap<DateTime<Utc>, Retained>> {
        self.series.get(&(symbol.clone(), interval))
    }
}

impl Monoid for MarketState {
    fn empty() -> Self {
        Self::default()
    }

    fn combine(mut self, other: Self) -> Self {
        self.clock = self.clock.max(other.clock);
        for (key, bars) in other.series {
            let held = self.series.entry(key).or_default();
            for (timestamp, retained) in bars {
                held.entry(timestamp)
                    .and_modify(|kept| *kept = (*kept).max(retained))
                    .or_insert(retained);
            }
            while held.len() > RETAINED_BARS {
                held.pop_first();
            }
        }
        for (symbol, quote) in other.quotes {
            self.quotes
                .entry(symbol)
                .and_modify(|kept| *kept = (*kept).max(quote))
                .or_insert(quote);
        }
        self
    }
}

#[cfg(test)]
mod tests {
    use chrono::{NaiveDate, TimeDelta};
    use proptest::prelude::*;

    use super::*;
    use crate::common::market::aggregate::TradeTotals;
    use crate::common::market::trade_bars::{OpenClose, TradeSums};
    use crate::common::market::{DollarVolume, Shares, TradeCount};
    use crate::common::monoid::{concatenate, laws};
    use crate::common::time::SessionDate;

    fn session() -> SessionDate {
        SessionDate::from_date(NaiveDate::from_ymd_opt(2026, 9, 25).unwrap())
    }

    fn symbol(raw: &str) -> Symbol {
        Symbol::new(raw).unwrap()
    }

    /// A one-minute trade bar `minute` minutes after the session's 13:30 UTC open, priced at `close`.
    fn minute_bar(raw: &str, minute: i64, close: i64, volume: u64) -> TradeBar {
        minute_trade_bar(raw, minute, Some(close), volume)
    }

    /// A one-minute trade bar `minute` minutes after the 13:30 UTC open, priced at `close` when one is given.
    fn minute_trade_bar(raw: &str, minute: i64, close: Option<i64>, volume: u64) -> TradeBar {
        let at =
            "2026-09-25T13:30:00Z".parse::<DateTime<Utc>>().unwrap() + TimeDelta::minutes(minute);
        let open_close = close.map(|ticks| {
            let close = StampedPrice::new(at, Price::from_ticks(ticks).unwrap());
            OpenClose::new(close, close).unwrap()
        });
        let totals = TradeTotals::new(
            TradeCount::new(1),
            Shares::whole(volume).unwrap(),
            DollarVolume::default(),
        );
        TradeBar::new(
            symbol(raw),
            BarInterval::OneMinute,
            at,
            TradeSums::new(totals, open_close, None),
        )
        .unwrap()
    }

    /// A daily trade bar `day` days after the session, closing at `close` a second before 16:00 Eastern.
    fn daily_bar(raw: &str, day: i64, close: i64, volume: u64) -> TradeBar {
        let ends = session().plus_calendar_days(day).regular_close();
        let close = StampedPrice::new(
            ends - TimeDelta::seconds(1),
            Price::from_ticks(close).unwrap(),
        );
        let totals = TradeTotals::new(
            TradeCount::new(1),
            Shares::whole(volume).unwrap(),
            DollarVolume::default(),
        );
        TradeBar::new(
            symbol(raw),
            BarInterval::OneDay,
            ends,
            TradeSums::new(totals, Some(OpenClose::new(close, close).unwrap()), None),
        )
        .unwrap()
    }

    fn fold(events: impl IntoIterator<Item = MarketEvent>) -> MarketState {
        concatenate(events.into_iter().map(MarketState::of))
    }

    /// 150 bars of one series keep the latest 100: the state equals one folded from minutes 50 to 149 alone.
    #[test]
    fn test_a_series_keeps_its_latest_bars() {
        let state = fold((0..150).map(|minute| {
            MarketEvent::Trades(minute_bar(
                "AAPL",
                minute,
                1_000_000 + minute,
                minute as u64,
            ))
        }));
        let aapl = symbol("AAPL");
        assert_eq!(
            state.last_price(&aapl, BarInterval::OneMinute),
            Some(Price::from_ticks(1_000_149).unwrap())
        );
        let latest = fold((50..150).map(|minute| {
            MarketEvent::Trades(minute_bar(
                "AAPL",
                minute,
                1_000_000 + minute,
                minute as u64,
            ))
        }));
        assert_eq!(state, latest);
    }

    /// A close prices until it is exactly `stale_after` old and not a second after.
    #[test]
    fn test_a_close_older_than_the_staleness_prices_nothing() {
        let state = MarketState::of(MarketEvent::Trades(minute_bar("AAPL", 0, 1000, 100)));
        let set = "2026-09-25T13:30:00Z".parse::<DateTime<Utc>>().unwrap();
        let fresh = |now| {
            state.fresh_price(
                &symbol("AAPL"),
                BarInterval::OneMinute,
                now,
                Staleness::new(TimeDelta::minutes(2)).unwrap(),
            )
        };
        assert_eq!(
            fresh(set + TimeDelta::minutes(2)),
            Some(Price::from_ticks(1000).unwrap())
        );
        assert_eq!(fresh(set + TimeDelta::seconds(121)), None);
        assert_eq!(
            state.fresh_price(
                &symbol("MSFT"),
                BarInterval::OneMinute,
                set,
                Staleness::new(TimeDelta::minutes(2)).unwrap()
            ),
            None
        );
    }

    /// A staleness is whole seconds of at least a minute, so the journal's seconds are exactly what risk applied.
    #[test]
    fn test_a_staleness_under_a_minute_or_in_part_seconds_is_refused() {
        for (stale_after, expected) in [
            (
                TimeDelta::seconds(59),
                Err(StalenessRefusal::UnderAMinute {
                    stale_after: TimeDelta::seconds(59),
                }),
            ),
            (
                TimeDelta::milliseconds(-120_500),
                Err(StalenessRefusal::UnderAMinute {
                    stale_after: TimeDelta::milliseconds(-120_500),
                }),
            ),
            (
                TimeDelta::milliseconds(120_500),
                Err(StalenessRefusal::Fractional {
                    stale_after: TimeDelta::milliseconds(120_500),
                }),
            ),
            (TimeDelta::seconds(60), Ok(60)),
            (TimeDelta::seconds(120), Ok(120)),
        ] {
            assert_eq!(
                Staleness::new(stale_after).map(Staleness::seconds),
                expected,
                "{stale_after}"
            );
        }
    }

    /// A minute of trades with no print allowed to price it leaves the last price where the last priced minute set it.
    #[test]
    fn test_a_trade_bar_with_no_close_keeps_the_last_price() {
        let state = fold([
            MarketEvent::Trades(minute_trade_bar("AAPL", 0, Some(150_000_000), 100)),
            MarketEvent::Trades(minute_trade_bar("AAPL", 1, None, 30)),
        ]);
        let aapl = symbol("AAPL");
        assert_eq!(
            state.last_price(&aapl, BarInterval::OneMinute),
            Some(Price::from_ticks(150_000_000).unwrap())
        );
        let unpriced = MarketState::of(MarketEvent::Trades(minute_trade_bar("AAPL", 1, None, 30)));
        assert_eq!(unpriced.last_price(&aapl, BarInterval::OneMinute), None);
    }

    /// Minute and daily bars of one symbol are separate series.
    #[test]
    fn test_each_interval_is_its_own_series() {
        let state = fold([
            MarketEvent::Trades(minute_bar("AAPL", 0, 2_000_000, 5)),
            MarketEvent::Trades(daily_bar("AAPL", 0, 3_000_000, 700)),
        ]);
        let aapl = symbol("AAPL");
        assert_eq!(
            [BarInterval::OneMinute, BarInterval::OneDay]
                .map(|interval| state.last_price(&aapl, interval)),
            [
                Some(Price::from_ticks(2_000_000).unwrap()),
                Some(Price::from_ticks(3_000_000).unwrap())
            ]
        );
    }

    /// Two bars claiming one timestamp keep the greater, whichever arrives first.
    #[test]
    fn test_a_repeated_bar_keeps_the_greater_in_either_order() {
        let low = MarketEvent::Trades(minute_bar("AAPL", 0, 1_000_000, 5));
        let high = MarketEvent::Trades(minute_bar("AAPL", 0, 2_000_000, 1));
        let aapl = symbol("AAPL");
        for events in [[low.clone(), high.clone()], [high, low]] {
            let state = fold(events);
            assert_eq!(
                state.last_price(&aapl, BarInterval::OneMinute),
                Some(Price::from_ticks(2_000_000).unwrap())
            );
        }
    }

    /// A quote `minute` minutes after the session's 13:30 UTC open, its sides in ticks.
    fn quote(raw: &str, minute: i64, bid: i64, ask: i64) -> Quote {
        Quote::new(
            symbol(raw),
            "2026-09-25T13:30:00Z".parse::<DateTime<Utc>>().unwrap() + TimeDelta::minutes(minute),
            Price::from_ticks(bid).unwrap(),
            Price::from_ticks(ask).unwrap(),
            Shares::whole(1).unwrap(),
            Shares::whole(1).unwrap(),
        )
        .unwrap()
    }

    /// A symbol keeps its latest quote whatever order the quotes arrive in, and a symbol never quoted has none.
    #[test]
    fn test_a_symbol_keeps_its_latest_quote() {
        let early = MarketEvent::Quote(quote("AAPL", 1, 1_000_000, 1_010_000));
        let late = MarketEvent::Quote(quote("AAPL", 2, 1_020_000, 1_030_000));
        for events in [[early.clone(), late.clone()], [late, early]] {
            let state = fold(events);
            assert_eq!(state.quotes().keys().collect::<Vec<_>>(), [&symbol("AAPL")]);
            let kept = state.quotes()[&symbol("AAPL")];
            assert_eq!(
                (kept.quoted_at(), kept.bid().ticks(), kept.ask().ticks()),
                (
                    "2026-09-25T13:32:00Z".parse::<DateTime<Utc>>().unwrap(),
                    1_020_000,
                    1_030_000
                )
            );
        }
    }

    /// The clock is the latest reported, whatever order the reports arrive in.
    #[test]
    fn test_the_clock_is_the_latest_reported() {
        assert_eq!(MarketState::empty().clock(), None);
        let instant = |text: &str| text.parse::<DateTime<Utc>>().unwrap();
        let state = fold([
            MarketEvent::Clock(instant("2026-09-25T19:00:00Z")),
            MarketEvent::Clock(instant("2026-09-25T13:00:00Z")),
        ]);
        assert_eq!(state.clock(), Some(instant("2026-09-25T19:00:00Z")));
    }

    /// Trade bars, priced or not, for two symbols at both intervals, with timestamps dense enough to repeat
    /// and, for minutes, to pass the retained count, interleaved with clock reports.
    fn any_event() -> impl Strategy<Value = MarketEvent> {
        prop_oneof![
            6 => (prop::sample::select(vec!["AAPL", "MSFT"]), 0_i64..130, 1_i64..4, 0_u64..1_000)
                .prop_map(|(raw, minute, close, volume)| MarketEvent::Trades(minute_bar(raw, minute, close, volume))),
            3 => (prop::sample::select(vec!["AAPL", "MSFT"]), 0_i64..130, prop::option::of(1_i64..4), 0_u64..1_000)
                .prop_map(|(raw, minute, close, volume)| MarketEvent::Trades(minute_trade_bar(raw, minute, close, volume))),
            2 => (prop::sample::select(vec!["AAPL", "MSFT"]), 0_i64..5, 1_i64..4, 0_u64..1_000)
                .prop_map(|(raw, day, close, volume)| MarketEvent::Trades(daily_bar(raw, day, close, volume))),
            2 => (prop::sample::select(vec!["AAPL", "MSFT"]), 0_i64..5, 1_i64..4, 0_i64..3)
                .prop_map(|(raw, minute, bid, width)| MarketEvent::Quote(quote(raw, minute, bid, bid + width))),
            1 => (0_i64..1_000).prop_map(|minute| MarketEvent::Clock(session().regular_close() + TimeDelta::minutes(minute))),
        ]
    }

    /// One series' bars at 120 or more distinct minutes out of 200, so any two of them overlap by at least 40.
    fn full_history() -> impl Strategy<Value = Vec<MarketEvent>> {
        prop::sample::subsequence((0_i64..200).collect::<Vec<_>>(), 120..200).prop_flat_map(
            |minutes| {
                let count = minutes.len();
                (
                    Just(minutes),
                    prop::collection::vec((1_i64..4, 0_u64..1_000), count),
                )
                    .prop_map(|(minutes, values)| {
                        minutes
                            .into_iter()
                            .zip(values)
                            .map(|(minute, (close, volume))| {
                                MarketEvent::Trades(minute_bar("AAPL", minute, close, volume))
                            })
                            .collect()
                    })
            },
        )
    }

    fn any_state() -> impl Strategy<Value = MarketState> {
        prop::collection::vec(any_event(), 0..80).prop_map(fold)
    }

    proptest! {
        #[test]
        fn property_market_states_are_a_commutative_monoid(
            first in any_state(),
            second in any_state(),
            third in any_state(),
        ) {
            laws::check(first, second, third)?;
        }

        #[test]
        fn property_events_fold_to_one_state_in_any_order(
            (events, shuffled) in prop::collection::vec(any_event(), 0..300)
                .prop_flat_map(|events| (Just(events.clone()), Just(events).prop_shuffle())),
        ) {
            let states = |events: Vec<MarketEvent>| events.into_iter().map(MarketState::of).collect();
            laws::check_any_order(states(events), states(shuffled))?;
        }

        /// Two chunks each holding a full series, at least 40 of their minutes shared, combine in either order to
        /// the state of the whole stream, which holds exactly the latest minutes.
        #[test]
        fn property_full_histories_combine_to_the_whole(
            head in full_history(),
            tail in full_history(),
        ) {
            let aapl = (symbol("AAPL"), BarInterval::OneMinute);
            let mut latest: Vec<DateTime<Utc>> = head
                .iter()
                .chain(&tail)
                .map(|event| match event {
                    MarketEvent::Trades(bar) => bar.timestamp(),
                    MarketEvent::Quote(quote) => quote.timestamp(),
                    MarketEvent::Clock(instant) => *instant,
                })
                .collect();
            latest.sort();
            latest.dedup();
            let latest = latest.split_off(latest.len() - RETAINED_BARS);
            let whole = fold(head.iter().chain(&tail).cloned());
            prop_assert_eq!(whole.series[&aapl].keys().copied().collect::<Vec<_>>(), latest);
            let (head, tail) = (fold(head), fold(tail));
            for chunk in [&head, &tail] {
                prop_assert_eq!(chunk.series[&aapl].len(), RETAINED_BARS);
            }
            prop_assert_eq!(head.clone().combine(tail.clone()), whole.clone());
            prop_assert_eq!(tail.combine(head), whole);
        }

        /// Folding a stream one event at a time, as the live loop does, equals folding any two halves apart and
        /// combining them, as a parallel replay does.
        #[test]
        fn property_a_split_stream_folds_to_the_whole(
            (events, split) in prop::collection::vec(any_event(), 0..300)
                .prop_flat_map(|events| { let length = events.len(); (Just(events), 0..=length) }),
        ) {
            let stepwise = events
                .iter()
                .cloned()
                .fold(MarketState::empty(), |state, event| state.combine(MarketState::of(event)));
            let (head, tail) = events.split_at(split);
            prop_assert_eq!(fold(head.to_vec()).combine(fold(tail.to_vec())), stepwise);
        }
    }
}
