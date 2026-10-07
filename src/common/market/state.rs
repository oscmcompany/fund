//! What the trader knows of the market at an instant, folded from the events seen so far. A monoid, so states folded
//! from separate chunks of one stream combine into the state of the whole.

use std::collections::BTreeMap;
use std::num::NonZeroUsize;

use chrono::{DateTime, Utc};

use super::record::{Bar, BarInterval};
use super::trade_bars::TradeBar;
use super::{Price, Shares, Symbol};
use crate::common::monoid::Monoid;
use crate::common::time::calendar::{SessionPhase, TradingCalendar};

/// The latest bars a state keeps for each symbol and interval, so a `VolumeDepth` reads at most this many.
pub const RETAINED_BARS: usize = 100;

/// One input to the fold: time arrives as an event like any other, never from a clock the fold reads.
#[derive(Debug, Clone, PartialEq)]
pub enum MarketEvent {
    Bar(Bar),
    /// A bar built from the tape's prints, as the archive derives them and the trader builds them live.
    Trades(TradeBar),
    Clock(DateTime<Utc>),
}

/// How many of a series' latest bars a rolling volume sums.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VolumeDepth(NonZeroUsize);

/// Why a depth was refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VolumeDepthRefusal {
    Zero,
    BeyondRetained { depth: usize },
}

impl VolumeDepth {
    pub fn new(depth: usize) -> Result<Self, VolumeDepthRefusal> {
        match NonZeroUsize::new(depth) {
            None => Err(VolumeDepthRefusal::Zero),
            Some(_) if depth > RETAINED_BARS => Err(VolumeDepthRefusal::BeyondRetained { depth }),
            Some(depth) => Ok(Self(depth)),
        }
    }

    pub fn get(self) -> usize {
        self.0.get()
    }
}

/// Why a rolling volume was refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RollingVolumeRefusal {
    /// The series holds fewer bars than the depth, `held` of them.
    ShortOfDepth { held: usize },
    /// The latest `depth` volumes sum past what `Shares` holds.
    BeyondRange { depth: usize },
}

/// What one bar leaves in the state; ordered so a repeated timestamp keeps the greater and the combine commutes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct Retained {
    /// The close and the instant it was set: the closing print's for a trade bar, the bar's end for a vendor bar;
    /// `None` for a trade bar no print was allowed to price, such as a minute of odd lots.
    close: Option<(DateTime<Utc>, Price)>,
    volume: Shares,
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
                    close: Some((bar.ends(), bar.prices().close())),
                    volume: bar.volume(),
                },
            ),
            MarketEvent::Trades(bar) => Self::retaining(
                bar.symbol(),
                bar.interval(),
                bar.timestamp(),
                Retained {
                    close: bar.sums().open_close().map(|prices| prices.close()),
                    volume: bar.sums().totals().volume(),
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
        self.last_close(symbol, interval).map(|(_, price)| price)
    }

    /// `last_price` with the instant it was set, so a caller can judge how old it is: the closing print's for a trade
    /// bar, the bar's end for a vendor bar.
    pub fn last_close(
        &self,
        symbol: &Symbol,
        interval: BarInterval,
    ) -> Option<(DateTime<Utc>, Price)> {
        self.bars(symbol, interval)?
            .values()
            .rev()
            .find_map(|retained| retained.close)
    }

    /// The volume of the series' latest `depth` bars, refused until that many are held.
    pub fn rolling_volume(
        &self,
        symbol: &Symbol,
        interval: BarInterval,
        depth: VolumeDepth,
    ) -> Result<Shares, RollingVolumeRefusal> {
        let bars = self.bars(symbol, interval);
        let held = bars.map_or(0, BTreeMap::len);
        if held < depth.get() {
            return Err(RollingVolumeRefusal::ShortOfDepth { held });
        }
        bars.into_iter()
            .flat_map(BTreeMap::values)
            .rev()
            .take(depth.get())
            .try_fold(Shares::empty(), |total, retained| {
                total.checked_plus(retained.volume)
            })
            .ok_or(RollingVolumeRefusal::BeyondRange { depth: depth.get() })
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
    use crate::common::market::record::Ohlc;
    use crate::common::market::trade_bars::{OpenClose, TradeSums};
    use crate::common::market::{DollarVolume, TradeCount};
    use crate::common::monoid::{concatenate, laws};
    use crate::common::time::SessionDate;
    use crate::common::time::calendar::TradingSession;

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
            Ohlc::new(price, price, price, price).unwrap(),
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
            let price = Price::from_ticks(ticks).unwrap();
            OpenClose::new((at, price), (at, price)).unwrap()
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
            Ohlc::new(price, price, price, price).unwrap(),
            Shares::whole(volume).unwrap(),
            None,
            None,
        )
        .unwrap()
    }

    fn fold(events: impl IntoIterator<Item = MarketEvent>) -> MarketState {
        concatenate(events.into_iter().map(MarketState::of))
    }

    fn depth(depth: usize) -> VolumeDepth {
        VolumeDepth::new(depth).unwrap()
    }

    #[test]
    fn test_a_depth_is_between_one_and_the_bars_retained() {
        assert_eq!(VolumeDepth::new(0), Err(VolumeDepthRefusal::Zero));
        assert_eq!(VolumeDepth::new(1).map(VolumeDepth::get), Ok(1));
        assert_eq!(VolumeDepth::new(100).map(VolumeDepth::get), Ok(100));
        assert_eq!(
            VolumeDepth::new(101),
            Err(VolumeDepthRefusal::BeyondRetained { depth: 101 })
        );
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
        let volume = |depth| state.rolling_volume(&aapl, BarInterval::OneMinute, depth);
        assert_eq!(volume(self::depth(100)), Ok(Shares::whole(9_950).unwrap()));
        assert_eq!(volume(self::depth(1)), Ok(Shares::whole(149).unwrap()));
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

    /// A volume is refused with the count held until the depth is held, and is measured exactly at it.
    #[test]
    fn test_a_rolling_volume_needs_its_whole_depth() {
        let state =
            fold((0..3).map(|minute| MarketEvent::Bar(minute_bar("AAPL", minute, 1_000_000, 10))));
        let aapl = symbol("AAPL");
        assert_eq!(
            state.rolling_volume(&aapl, BarInterval::OneMinute, depth(3)),
            Ok(Shares::whole(30).unwrap())
        );
        assert_eq!(
            state.rolling_volume(&aapl, BarInterval::OneMinute, depth(4)),
            Err(RollingVolumeRefusal::ShortOfDepth { held: 3 })
        );
        assert_eq!(
            state.rolling_volume(&aapl, BarInterval::OneDay, depth(1)),
            Err(RollingVolumeRefusal::ShortOfDepth { held: 0 })
        );
        assert_eq!(
            state.rolling_volume(&symbol("MSFT"), BarInterval::OneMinute, depth(1)),
            Err(RollingVolumeRefusal::ShortOfDepth { held: 0 })
        );
    }

    /// Volumes whose sum passes the share count's range are refused, not summed into a panic.
    #[test]
    fn test_a_rolling_volume_past_the_range_is_refused() {
        let bar = |minute: i64, units: u64| {
            let price = Price::from_ticks(1_000_000).unwrap();
            MarketEvent::Bar(
                Bar::new(
                    symbol("AAPL"),
                    BarInterval::OneMinute,
                    "2026-09-25T13:30:00Z".parse::<DateTime<Utc>>().unwrap()
                        + TimeDelta::minutes(minute),
                    Ohlc::new(price, price, price, price).unwrap(),
                    Shares::from_units(units),
                    None,
                    None,
                )
                .unwrap(),
            )
        };
        let state = fold([bar(0, u64::MAX), bar(1, 1)]);
        let aapl = symbol("AAPL");
        assert_eq!(
            state.rolling_volume(&aapl, BarInterval::OneMinute, depth(2)),
            Err(RollingVolumeRefusal::BeyondRange { depth: 2 })
        );
        assert_eq!(
            state.rolling_volume(&aapl, BarInterval::OneMinute, depth(1)),
            Ok(Shares::from_units(1))
        );
    }

    /// A minute of trades with no print allowed to price it adds volume and leaves the last price where the last
    /// priced minute set it.
    #[test]
    fn test_a_trade_bar_with_no_close_adds_volume_and_keeps_the_last_price() {
        let state = fold([
            MarketEvent::Trades(minute_trade_bar("AAPL", 0, Some(150_000_000), 100)),
            MarketEvent::Trades(minute_trade_bar("AAPL", 1, None, 30)),
        ]);
        let aapl = symbol("AAPL");
        assert_eq!(
            state.last_price(&aapl, BarInterval::OneMinute),
            Some(Price::from_ticks(150_000_000).unwrap())
        );
        assert_eq!(
            state.rolling_volume(&aapl, BarInterval::OneMinute, depth(2)),
            Ok(Shares::whole(130).unwrap())
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
            assert_eq!(
                state.rolling_volume(&aapl, BarInterval::OneMinute, depth(1)),
                Ok(Shares::whole(1).unwrap())
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
            session(),
            session(),
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
