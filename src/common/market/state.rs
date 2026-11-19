//! What the trader knows of the market at an instant, folded from the events seen so far. A monoid, so states folded
//! from separate chunks of one stream combine into the state of the whole.

use std::collections::BTreeMap;

use chrono::{DateTime, TimeDelta, Utc};
use serde::{Deserialize, Serialize};

use super::record::{BarInterval, Quote, QuoteRefusal};
use super::trade_bars::TradeBar;
use super::{Price, Shares, StampedPrice, Symbol};
use crate::common::monoid::Monoid;
use crate::common::order::{DecisionQuote, QuoteAbsence};

/// The latest bars a state keeps for each symbol and interval.
pub const RETAINED_BARS: usize = 100;

/// One input to the fold: time arrives as an event like any other, never from a clock the fold reads.
#[derive(Debug, Clone, PartialEq)]
pub enum MarketEvent {
    /// A bar built from the tape's prints, as the archive derives them and the trader builds them live.
    Trades(TradeBar),
    /// A top-of-book quote, of which the state keeps each symbol's latest.
    Quote(Quote),
    /// A quote missing a side, which leaves its symbol with no top of book until a two-sided quote at or after it.
    OneSided {
        symbol: Symbol,
        quoted_at: DateTime<Utc>,
    },
    /// A quote with its bid above its ask, which outdates its symbol's top of book as a one-sided quote does.
    Crossed {
        symbol: Symbol,
        quoted_at: DateTime<Utc>,
        bid: Price,
        ask: Price,
    },
    /// The quote stream stopped, so no symbol has a top of book until a quote for it stamped after `at` is folded.
    StreamLost {
        at: DateTime<Utc>,
    },
    Clock(DateTime<Utc>),
}

/// A quote that left its symbol with no top of book; ordered by when it was quoted first, so a combine keeps the latest.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct Outdating {
    quoted_at: DateTime<Utc>,
    kind: OutdatingKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum OutdatingKind {
    OneSided,
    Crossed { bid: Price, ask: Price },
}

impl Outdating {
    fn decision(self) -> DecisionQuote {
        match self.kind {
            OutdatingKind::OneSided => DecisionQuote::OneSided {
                quoted_at: self.quoted_at,
            },
            OutdatingKind::Crossed { bid, ask } => DecisionQuote::Crossed {
                quoted_at: self.quoted_at,
                bid,
                ask,
            },
        }
    }
}

/// A symbol's latest quote as a state holds it, its bid never above its ask; ordered by when it was quoted first, so a
/// combine keeps the latest and breaks a tie on the sides and then the sizes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(try_from = "UncheckedLastQuote")]
pub struct LastQuote {
    quoted_at: DateTime<Utc>,
    bid: Price,
    ask: Price,
    bid_size: Shares,
    ask_size: Shares,
}

/// A `LastQuote` as read, before its sides are checked.
#[derive(Deserialize)]
struct UncheckedLastQuote {
    quoted_at: DateTime<Utc>,
    bid: Price,
    ask: Price,
    bid_size: Shares,
    ask_size: Shares,
}

impl TryFrom<UncheckedLastQuote> for LastQuote {
    type Error = QuoteRefusal;

    fn try_from(unchecked: UncheckedLastQuote) -> Result<Self, Self::Error> {
        QuoteRefusal::check(unchecked.bid, unchecked.ask)?;
        Ok(Self {
            quoted_at: unchecked.quoted_at,
            bid: unchecked.bid,
            ask: unchecked.ask,
            bid_size: unchecked.bid_size,
            ask_size: unchecked.ask_size,
        })
    }
}

impl LastQuote {
    pub fn of(quote: &Quote) -> Self {
        Self {
            quoted_at: quote.timestamp(),
            bid: quote.bid(),
            ask: quote.ask(),
            bid_size: quote.bid_size(),
            ask_size: quote.ask_size(),
        }
    }

    pub fn bid_size(self) -> Shares {
        self.bid_size
    }

    pub fn ask_size(self) -> Shares {
        self.ask_size
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

/// The latest clock, each series' latest `RETAINED_BARS` bars, each symbol's latest quote, and when the quote stream was
/// last lost.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MarketState {
    /// The latest instant a clock event reported; a bar's timestamp names its period, not when it was seen.
    clock: Option<DateTime<Utc>>,
    series: BTreeMap<(Symbol, BarInterval), BTreeMap<DateTime<Utc>, Retained>>,
    quotes: BTreeMap<Symbol, LastQuote>,
    /// Each symbol's latest one-sided or crossed quote, which outdates any two-sided quote before it.
    outdated: BTreeMap<Symbol, Outdating>,
    stream_lost: Option<DateTime<Utc>>,
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
            MarketEvent::OneSided { symbol, quoted_at } => Self::outdating(
                symbol,
                Outdating {
                    quoted_at,
                    kind: OutdatingKind::OneSided,
                },
            ),
            MarketEvent::Crossed {
                symbol,
                quoted_at,
                bid,
                ask,
            } => Self::outdating(
                symbol,
                Outdating {
                    quoted_at,
                    kind: OutdatingKind::Crossed { bid, ask },
                },
            ),
            MarketEvent::StreamLost { at } => Self {
                stream_lost: Some(at),
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

    fn outdating(symbol: Symbol, outdating: Outdating) -> Self {
        Self {
            outdated: BTreeMap::from([(symbol, outdating)]),
            ..Self::default()
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

    /// Each retained bar of the series by its timestamp, with its close, `None` for a bar with no priced print.
    #[cfg(test)]
    pub fn closes(
        &self,
        symbol: &Symbol,
        interval: BarInterval,
    ) -> BTreeMap<DateTime<Utc>, Option<StampedPrice>> {
        self.bars(symbol, interval)
            .into_iter()
            .flatten()
            .map(|(timestamp, retained)| (*timestamp, retained.close))
            .collect()
    }

    /// Each symbol's latest two-sided quote; a symbol is absent only when none of its two-sided quotes was folded.
    #[cfg(test)]
    pub fn quotes(&self) -> &BTreeMap<Symbol, LastQuote> {
        &self.quotes
    }

    /// The symbol's top of book: its latest two-sided quote, unless a one-sided or crossed quote came after it, or the
    /// stream was lost at or after all of them.
    pub fn decision_quote(&self, symbol: &Symbol) -> DecisionQuote {
        let (quote, outdating) = (self.quotes.get(symbol), self.outdated.get(symbol));
        let latest = quote
            .map(|quote| quote.quoted_at())
            .max(outdating.map(|outdating| outdating.quoted_at));
        match (self.stream_lost, quote, outdating) {
            (Some(since), _, _) if latest.is_none_or(|latest| latest <= since) => {
                DecisionQuote::Unquoted {
                    cause: QuoteAbsence::StreamLost { since },
                }
            }
            (_, Some(quote), Some(outdating)) if outdating.quoted_at > quote.quoted_at() => {
                outdating.decision()
            }
            (_, Some(quote), _) => DecisionQuote::Quoted(*quote),
            (_, None, Some(outdating)) => outdating.decision(),
            (_, None, None) => DecisionQuote::Unquoted {
                cause: QuoteAbsence::NoneFolded,
            },
        }
    }

    /// The latest stamp among the state's quotes, one-sided and crossed included; `None` when it holds no quote.
    pub fn latest_quoted_at(&self) -> Option<DateTime<Utc>> {
        self.quotes
            .values()
            .map(|quote| quote.quoted_at())
            .chain(self.outdated.values().map(|outdating| outdating.quoted_at))
            .max()
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
        for (symbol, outdating) in other.outdated {
            self.outdated
                .entry(symbol)
                .and_modify(|kept| *kept = (*kept).max(outdating))
                .or_insert(outdating);
        }
        self.stream_lost = self.stream_lost.max(other.stream_lost);
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

    /// A quote `minute` minutes after the session's 13:30 UTC open, its sides in ticks and its sizes in whole shares.
    fn quote(
        raw: &str,
        minute: i64,
        (bid, ask): (i64, i64),
        (bid_size, ask_size): (u64, u64),
    ) -> Quote {
        Quote::new(
            symbol(raw),
            "2026-09-25T13:30:00Z".parse::<DateTime<Utc>>().unwrap() + TimeDelta::minutes(minute),
            Price::from_ticks(bid).unwrap(),
            Price::from_ticks(ask).unwrap(),
            Shares::whole(bid_size).unwrap(),
            Shares::whole(ask_size).unwrap(),
        )
        .unwrap()
    }

    /// A symbol keeps its latest quote whatever order the quotes arrive in, and a symbol never quoted has none.
    #[test]
    fn test_a_symbol_keeps_its_latest_quote() {
        let early = MarketEvent::Quote(quote("AAPL", 1, (1_000_000, 1_010_000), (500, 600)));
        let late = MarketEvent::Quote(quote("AAPL", 2, (1_020_000, 1_030_000), (300, 400)));
        for events in [[early.clone(), late.clone()], [late, early]] {
            let state = fold(events);
            assert_eq!(state.quotes().keys().collect::<Vec<_>>(), [&symbol("AAPL")]);
            let kept = state.quotes()[&symbol("AAPL")];
            assert_eq!(
                (
                    kept.quoted_at(),
                    kept.bid().ticks(),
                    kept.ask().ticks(),
                    kept.bid_size().units(),
                    kept.ask_size().units()
                ),
                (
                    "2026-09-25T13:32:00Z".parse::<DateTime<Utc>>().unwrap(),
                    1_020_000,
                    1_030_000,
                    300_000_000,
                    400_000_000
                )
            );
        }
    }

    /// The instant `minute` minutes after the session's 13:30 UTC open.
    fn minute_at(minute: i64) -> DateTime<Utc> {
        "2026-09-25T13:30:00Z".parse::<DateTime<Utc>>().unwrap() + TimeDelta::minutes(minute)
    }

    /// A one-sided quote `minute` minutes after the session's 13:30 UTC open.
    fn one_sided(raw: &str, minute: i64) -> MarketEvent {
        MarketEvent::OneSided {
            symbol: symbol(raw),
            quoted_at: minute_at(minute),
        }
    }

    /// A crossed quote `minute` minutes after the session's 13:30 UTC open, its sides in ticks.
    fn crossed(raw: &str, minute: i64, (bid, ask): (i64, i64)) -> MarketEvent {
        MarketEvent::Crossed {
            symbol: symbol(raw),
            quoted_at: minute_at(minute),
            bid: Price::from_ticks(bid).unwrap(),
            ask: Price::from_ticks(ask).unwrap(),
        }
    }

    /// A crossed quote after a symbol's two-sided one leaves it with no top of book and names its sides, one at or
    /// before it leaves the two-sided quote standing, and a crossed quote after a one-sided one is the latest cause.
    #[test]
    fn test_a_later_crossed_quote_outdates_the_top_of_book() {
        let two_sided = quote("AAPL", 2, (1_000_000, 1_010_000), (5, 6));
        let quoted = DecisionQuote::Quoted(LastQuote::of(&two_sided));
        let crossed_at = |minute: i64| DecisionQuote::Crossed {
            quoted_at: minute_at(minute),
            bid: Price::from_ticks(1_010_001).unwrap(),
            ask: Price::from_ticks(1_010_000).unwrap(),
        };
        for (minute, expected) in [(1, quoted), (2, quoted), (3, crossed_at(3))] {
            let events = [
                MarketEvent::Quote(two_sided.clone()),
                crossed("AAPL", minute, (1_010_001, 1_010_000)),
            ];
            for state in [fold(events.clone()), fold(events.into_iter().rev())] {
                assert_eq!(state.decision_quote(&symbol("AAPL")), expected, "{minute}");
            }
        }
        let events = [
            MarketEvent::Quote(two_sided),
            one_sided("AAPL", 3),
            crossed("AAPL", 4, (1_010_001, 1_010_000)),
        ];
        for state in [fold(events.clone()), fold(events.into_iter().rev())] {
            assert_eq!(state.decision_quote(&symbol("AAPL")), crossed_at(4));
        }
    }

    /// A lost stream refuses every symbol until a quote for it stamped after the loss, naming when it was lost: a quote
    /// stamped at the loss, as the last one delivered before it is, stays refused, and the order of arrival changes
    /// nothing.
    #[test]
    fn test_a_lost_stream_refuses_each_symbol_until_it_quotes_after_the_loss() {
        let lost = DecisionQuote::Unquoted {
            cause: QuoteAbsence::StreamLost {
                since: minute_at(5),
            },
        };
        let held = |events: Vec<MarketEvent>| {
            let forward = fold(events.clone());
            assert_eq!(forward, fold(events.into_iter().rev()));
            ["AAPL", "MSFT", "QQQ"].map(|raw| forward.decision_quote(&symbol(raw)))
        };
        let aapl = quote("AAPL", 2, (1_000_000, 1_010_000), (5, 6));
        let msft_at_loss = quote("MSFT", 5, (2_000_000, 2_010_000), (5, 6));
        let msft_after = quote("MSFT", 6, (2_000_000, 2_010_000), (5, 6));
        let before = vec![
            MarketEvent::Quote(aapl.clone()),
            MarketEvent::Quote(msft_at_loss.clone()),
        ];
        let with = |extra: &[MarketEvent]| {
            let mut events = before.clone();
            events.extend_from_slice(extra);
            events
        };
        let loss = MarketEvent::StreamLost { at: minute_at(5) };
        assert_eq!(
            held(with(&[])),
            [
                DecisionQuote::Quoted(LastQuote::of(&aapl)),
                DecisionQuote::Quoted(LastQuote::of(&msft_at_loss)),
                DecisionQuote::Unquoted {
                    cause: QuoteAbsence::NoneFolded
                }
            ]
        );
        assert_eq!(held(with(std::slice::from_ref(&loss))), [lost, lost, lost]);
        assert_eq!(
            held(with(&[
                loss.clone(),
                MarketEvent::Quote(msft_after.clone())
            ])),
            [
                lost,
                DecisionQuote::Quoted(LastQuote::of(&msft_after)),
                lost
            ]
        );
        assert_eq!(
            held(with(&[loss, one_sided("AAPL", 6)])),
            [
                DecisionQuote::OneSided {
                    quoted_at: minute_at(6)
                },
                lost,
                lost
            ]
        );
    }

    /// The latest instant any quote in a state was stamped, one-sided and crossed quotes included, and none for a state
    /// holding no quote.
    #[test]
    fn test_the_latest_quote_time_covers_every_kind_of_quote() {
        assert_eq!(
            fold([MarketEvent::Clock(minute_at(9))]).latest_quoted_at(),
            None
        );
        let two_sided = MarketEvent::Quote(quote("AAPL", 2, (1_000_000, 1_010_000), (5, 6)));
        for (last, expected) in [
            (one_sided("MSFT", 3), 3),
            (crossed("MSFT", 4, (2, 1)), 4),
            (one_sided("MSFT", 1), 2),
        ] {
            assert_eq!(
                fold([two_sided.clone(), last]).latest_quoted_at(),
                Some(minute_at(expected))
            );
        }
    }

    /// A one-sided quote after a symbol's two-sided one leaves it with no top of book, one at or before it leaves the
    /// two-sided quote standing, whatever order they arrive in, and a symbol never quoted holds none.
    #[test]
    fn test_a_later_one_sided_quote_outdates_the_top_of_book() {
        let two_sided = quote("AAPL", 2, (1_000_000, 1_010_000), (5, 6));
        let quoted = DecisionQuote::Quoted(LastQuote::of(&two_sided));
        let at = |minute: i64| {
            "2026-09-25T13:30:00Z".parse::<DateTime<Utc>>().unwrap() + TimeDelta::minutes(minute)
        };
        for (minute, expected) in [
            (1, quoted),
            (2, quoted),
            (3, DecisionQuote::OneSided { quoted_at: at(3) }),
        ] {
            let events = [
                MarketEvent::Quote(two_sided.clone()),
                one_sided("AAPL", minute),
            ];
            for state in [fold(events.clone()), fold(events.into_iter().rev())] {
                assert_eq!(state.decision_quote(&symbol("AAPL")), expected, "{minute}");
            }
        }
        assert_eq!(
            fold([one_sided("AAPL", 3)]).decision_quote(&symbol("AAPL")),
            DecisionQuote::OneSided { quoted_at: at(3) }
        );
        assert_eq!(
            fold([one_sided("AAPL", 3)]).decision_quote(&symbol("MSFT")),
            DecisionQuote::Unquoted {
                cause: QuoteAbsence::NoneFolded
            }
        );
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
            2 => (prop::sample::select(vec!["AAPL", "MSFT"]), 0_i64..5, 1_i64..4, 0_i64..3, (1_u64..3, 1_u64..3))
                .prop_map(|(raw, minute, bid, width, sizes)| MarketEvent::Quote(quote(raw, minute, (bid, bid + width), sizes))),
            1 => (prop::sample::select(vec!["AAPL", "MSFT"]), 0_i64..5)
                .prop_map(|(raw, minute)| one_sided(raw, minute)),
            1 => (prop::sample::select(vec!["AAPL", "MSFT"]), 0_i64..5, 1_i64..4)
                .prop_map(|(raw, minute, ask)| crossed(raw, minute, (ask + 1, ask))),
            1 => (0_i64..5).prop_map(|minute| MarketEvent::StreamLost { at: minute_at(minute) }),
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
                    MarketEvent::OneSided { quoted_at, .. } | MarketEvent::Crossed { quoted_at, .. } => {
                        *quoted_at
                    }
                    MarketEvent::StreamLost { at } => *at,
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
