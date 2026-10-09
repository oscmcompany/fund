//! What the trader knows of the market at an instant, folded from the events seen so far. A monoid, so states folded
//! from separate chunks of one stream combine into the state of the whole.

use std::collections::BTreeMap;

use chrono::{DateTime, Utc};

use super::record::{Bar, BarInterval};
use super::trade_bars::TradeBar;
use super::{Price, StampedPrice, Symbol};
use crate::common::monoid::Monoid;
use crate::common::time::calendar::{SessionPhase, TradingCalendar};

/// The latest bars a state keeps for each symbol and interval.
pub const RETAINED_BARS: usize = 100;

/// One input to the fold: time arrives as an event like any other, never from a clock the fold reads.
#[derive(Debug, Clone, PartialEq)]
pub enum MarketEvent {
    Bar(Bar),
    /// A bar built from the tape's prints, as the archive derives them and the trader builds them live.
    Trades(TradeBar),
    Clock(DateTime<Utc>),
}

/// What one bar leaves in the state; ordered so a repeated timestamp keeps the greater and the combine commutes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct Retained {
    /// The close and the instant it was set: the closing print's for a trade bar, the bar's end for a vendor bar;
    /// `None` for a trade bar no print was allowed to price, such as a minute of odd lots.
    close: Option<StampedPrice>,
}

/// The latest clock and each series' latest `RETAINED_BARS` bars.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MarketState {
    /// The latest instant a clock event reported; a bar's timestamp names its period, not when it was seen.
    clock: Option<DateTime<Utc>>,
    series: BTreeMap<(Symbol, BarInterval), BTreeMap<DateTime<Utc>, Retained>>,
}

impl MarketState {
    /// One event as a state, so a stream folds as `concatenate(events.map(MarketState::of))`.
    pub fn of(event: MarketEvent) -> Self {
        match event {
            MarketEvent::Clock(instant) => Self {
                clock: Some(instant),
                series: BTreeMap::new(),
            },
            MarketEvent::Bar(bar) => Self::retaining(
                bar.symbol(),
                bar.interval(),
                bar.timestamp(),
                Retained {
                    close: Some(StampedPrice::new(bar.ends(), bar.prices().close())),
                },
            ),
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
            clock: None,
            series: BTreeMap::from([(
                (symbol.clone(), interval),
                BTreeMap::from([(timestamp, retained)]),
            )]),
        }
    }

    pub fn clock(&self) -> Option<DateTime<Utc>> {
        self.clock
    }

    /// The close of the series' latest priced bar, `None` when no retained bar of it has a close.
    pub fn last_price(&self, symbol: &Symbol, interval: BarInterval) -> Option<Price> {
        self.last_close(symbol, interval).map(StampedPrice::price)
    }

    /// `last_price` with the instant it was set, so a caller can judge how old it is: the closing print's for a trade
    /// bar, the bar's end for a vendor bar.
    pub fn last_close(&self, symbol: &Symbol, interval: BarInterval) -> Option<StampedPrice> {
        self.bars(symbol, interval)?
            .values()
            .rev()
            .find_map(|retained| retained.close)
    }

    /// Where the clock falls in `calendar`'s sessions, `None` before any clock event.
    pub fn phase(&self, calendar: &TradingCalendar) -> Option<SessionPhase> {
        self.clock.map(|instant| calendar.phase_at(instant))
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
        self
    }
}

#[cfg(test)]
mod tests {
    use chrono::{NaiveDate, NaiveTime, TimeDelta};
    use proptest::prelude::*;

    use super::*;
    use crate::common::market::aggregate::TradeTotals;
    use crate::common::market::record::BarPrices;
    use crate::common::market::trade_bars::{OpenClose, TradeSums};
    use crate::common::market::{DollarVolume, Shares, TradeCount};
    use crate::common::monoid::{concatenate, laws};
    use crate::common::time::calendar::TradingSession;
    use crate::common::time::{SessionDate, SessionRange};

    fn session() -> SessionDate {
        SessionDate::from_date(NaiveDate::from_ymd_opt(2026, 9, 25).unwrap())
    }

    fn symbol(raw: &str) -> Symbol {
        Symbol::new(raw).unwrap()
    }

    /// A one-minute bar `minute` minutes after the session's 13:30 UTC open.
    fn minute_bar(raw: &str, minute: i64, close: i64, volume: u64) -> Bar {
        let price = Price::from_ticks(close).unwrap();
        Bar::new(
            symbol(raw),
            BarInterval::OneMinute,
            "2026-09-25T13:30:00Z".parse::<DateTime<Utc>>().unwrap() + TimeDelta::minutes(minute),
            BarPrices::new(price, price, price, price).unwrap(),
            Shares::whole(volume).unwrap(),
            None,
            None,
        )
        .unwrap()
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

    fn daily_bar(raw: &str, day: i64, close: i64, volume: u64) -> Bar {
        let price = Price::from_ticks(close).unwrap();
        Bar::new(
            symbol(raw),
            BarInterval::OneDay,
            session().plus_calendar_days(day).regular_close(),
            BarPrices::new(price, price, price, price).unwrap(),
            Shares::whole(volume).unwrap(),
            None,
            None,
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
            MarketEvent::Bar(minute_bar(
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
            MarketEvent::Bar(minute_bar(
                "AAPL",
                minute,
                1_000_000 + minute,
                minute as u64,
            ))
        }));
        assert_eq!(state, latest);
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
            MarketEvent::Bar(minute_bar("AAPL", 0, 2_000_000, 5)),
            MarketEvent::Bar(daily_bar("AAPL", 0, 3_000_000, 700)),
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
        let low = MarketEvent::Bar(minute_bar("AAPL", 0, 1_000_000, 5));
        let high = MarketEvent::Bar(minute_bar("AAPL", 0, 2_000_000, 1));
        let aapl = symbol("AAPL");
        for events in [[low.clone(), high.clone()], [high, low]] {
            let state = fold(events);
            assert_eq!(
                state.last_price(&aapl, BarInterval::OneMinute),
                Some(Price::from_ticks(2_000_000).unwrap())
            );
        }
    }

    /// The clock is the latest reported, whatever order the reports arrive in, and phases read from it.
    #[test]
    fn test_the_phase_reads_the_latest_clock() {
        let calendar = TradingCalendar::new(
            vec![
                TradingSession::new(
                    session(),
                    NaiveTime::from_hms_opt(9, 30, 0).unwrap(),
                    NaiveTime::from_hms_opt(16, 0, 0).unwrap(),
                )
                .unwrap(),
            ],
            SessionRange::single(session()),
        )
        .unwrap();
        assert_eq!(MarketState::empty().phase(&calendar), None);
        let instant = |text: &str| text.parse::<DateTime<Utc>>().unwrap();
        let state = fold([
            MarketEvent::Clock(instant("2026-09-25T19:00:00Z")),
            MarketEvent::Clock(instant("2026-09-25T13:00:00Z")),
        ]);
        assert_eq!(state.clock(), Some(instant("2026-09-25T19:00:00Z")));
        assert_eq!(
            state.phase(&calendar),
            Some(SessionPhase::Open {
                until_close: TimeDelta::hours(1)
            })
        );
    }

    /// Bars and trade bars, priced or not, for two symbols at both intervals, with timestamps dense enough to repeat
    /// and, for minutes, to pass the retained count, interleaved with clock reports.
    fn any_event() -> impl Strategy<Value = MarketEvent> {
        prop_oneof![
            6 => (prop::sample::select(vec!["AAPL", "MSFT"]), 0_i64..130, 1_i64..4, 0_u64..1_000)
                .prop_map(|(raw, minute, close, volume)| MarketEvent::Bar(minute_bar(raw, minute, close, volume))),
            3 => (prop::sample::select(vec!["AAPL", "MSFT"]), 0_i64..130, prop::option::of(1_i64..4), 0_u64..1_000)
                .prop_map(|(raw, minute, close, volume)| MarketEvent::Trades(minute_trade_bar(raw, minute, close, volume))),
            2 => (prop::sample::select(vec!["AAPL", "MSFT"]), 0_i64..5, 1_i64..4, 0_u64..1_000)
                .prop_map(|(raw, day, close, volume)| MarketEvent::Bar(daily_bar(raw, day, close, volume))),
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
                                MarketEvent::Bar(minute_bar("AAPL", minute, close, volume))
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
                    MarketEvent::Bar(bar) => bar.timestamp(),
                    MarketEvent::Trades(bar) => bar.timestamp(),
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
