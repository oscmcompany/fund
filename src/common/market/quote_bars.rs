//! Quote bars: the top of book a session's quotes held during its regular hours, each quote weighted by how long it
//! stood, as exact sums that roll up from one minute to five and to the day.

use std::collections::BTreeMap;

use chrono::{DateTime, TimeDelta, Timelike, Utc};

use super::record::{BarInterval, Quote};
use super::{Price, Shares, Symbol};
use crate::common::monoid::Monoid;
use crate::common::time::SessionDate;

/// Relative spreads are held in millionths of the midpoint, so one unit is a hundredth of a basis point.
pub const RELATIVE_SPREAD_SCALE: u128 = 1_000_000;

/// The gap between ask and bid in ticks; zero is a locked book, which is a quote.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Spread(u64);

impl Spread {
    /// Private so only a `Quote`'s sides reach it: `Quote` refuses a crossed book, so the ask is never below the bid.
    fn of(bid: Price, ask: Price) -> Self {
        Self(ask.ticks().abs_diff(bid.ticks()))
    }

    pub fn from_ticks(ticks: u64) -> Self {
        Self(ticks)
    }

    pub fn ticks(self) -> u64 {
        self.0
    }
}

/// One quote as the bar that closes with it standing saw it.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct StandingQuote {
    since: DateTime<Utc>,
    bid: Price,
    ask: Price,
    bid_size: Shares,
    ask_size: Shares,
}

/// Why a standing quote or a bar's sums were refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QuoteSumsRefusal {
    Crossed {
        bid: Price,
        ask: Price,
    },
    /// The narrowest spread is wider than the widest.
    Inverted {
        narrowest: Spread,
        widest: Spread,
    },
}

impl std::fmt::Display for QuoteSumsRefusal {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Crossed { bid, ask } => write!(formatter, "the bid {bid} is above the ask {ask}"),
            Self::Inverted { narrowest, widest } => write!(
                formatter,
                "the narrowest spread of {} ticks is wider than the widest of {}",
                narrowest.ticks(),
                widest.ticks()
            ),
        }
    }
}

impl std::error::Error for QuoteSumsRefusal {}

impl StandingQuote {
    /// A quote whose bid is not above its ask, as `Quote` guarantees for one read from a vendor.
    pub fn new(
        since: DateTime<Utc>,
        bid: Price,
        ask: Price,
        bid_size: Shares,
        ask_size: Shares,
    ) -> Result<Self, QuoteSumsRefusal> {
        if bid > ask {
            return Err(QuoteSumsRefusal::Crossed { bid, ask });
        }
        Ok(Self {
            since,
            bid,
            ask,
            bid_size,
            ask_size,
        })
    }

    fn of(quote: &Quote) -> Self {
        Self {
            since: quote.timestamp(),
            bid: quote.bid(),
            ask: quote.ask(),
            bid_size: quote.bid_size(),
            ask_size: quote.ask_size(),
        }
    }

    /// When the quote began, which may precede the bar it closes.
    pub fn since(&self) -> DateTime<Utc> {
        self.since
    }

    pub fn bid(&self) -> Price {
        self.bid
    }

    pub fn ask(&self) -> Price {
        self.ask
    }

    pub fn bid_size(&self) -> Shares {
        self.bid_size
    }

    pub fn ask_size(&self) -> Shares {
        self.ask_size
    }
}

/// The exact sums of one bar; every `_time` field is a quantity multiplied by the nanoseconds it stood.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QuoteSums {
    /// Quotes that began inside the bar; a bar can be covered by a quote that began before it and count none.
    quote_count: u64,
    covered_nanoseconds: u64,
    spread_time: u128,
    relative_spread_time: u128,
    bid_size_time: u128,
    ask_size_time: u128,
    narrowest: Spread,
    widest: Spread,
    /// The latest quote standing in the bar; ties on start break on the quote so the combine stays commutative.
    closing: StandingQuote,
}

impl QuoteSums {
    /// The sums of `quote` standing for `nanoseconds`, counting no quote; the fold adds counts by minute.
    fn standing(quote: &StandingQuote, nanoseconds: u64) -> Self {
        let spread = Spread::of(quote.bid, quote.ask);
        let midpoint_doubled = u128::from(quote.bid.ticks().unsigned_abs())
            + u128::from(quote.ask.ticks().unsigned_abs());
        let time = u128::from(nanoseconds);
        Self {
            quote_count: 0,
            covered_nanoseconds: nanoseconds,
            spread_time: u128::from(spread.0) * time,
            // Weighted before dividing, so a tight spread keeps its precision; the one floor loses under a unit.
            relative_spread_time: u128::from(spread.0) * 2 * RELATIVE_SPREAD_SCALE * time
                / midpoint_doubled,
            bid_size_time: u128::from(quote.bid_size.units()) * time,
            ask_size_time: u128::from(quote.ask_size.units()) * time,
            narrowest: spread,
            widest: spread,
            closing: quote.clone(),
        }
    }

    pub fn new(
        quote_count: u64,
        covered_nanoseconds: u64,
        time_weighted: [u128; 4],
        narrowest: Spread,
        widest: Spread,
        closing: StandingQuote,
    ) -> Result<Self, QuoteSumsRefusal> {
        if narrowest > widest {
            return Err(QuoteSumsRefusal::Inverted { narrowest, widest });
        }
        let [
            spread_time,
            relative_spread_time,
            bid_size_time,
            ask_size_time,
        ] = time_weighted;
        Ok(Self {
            quote_count,
            covered_nanoseconds,
            spread_time,
            relative_spread_time,
            bid_size_time,
            ask_size_time,
            narrowest,
            widest,
            closing,
        })
    }

    fn combine(self, other: Self) -> Self {
        let time = |left: u128, right: u128, name: &str| {
            left.checked_add(right)
                .unwrap_or_else(|| panic!("{name} fits u128"))
        };
        Self {
            quote_count: self
                .quote_count
                .checked_add(other.quote_count)
                .expect("quote count fits u64"),
            covered_nanoseconds: self
                .covered_nanoseconds
                .checked_add(other.covered_nanoseconds)
                .expect("covered nanoseconds fit u64"),
            spread_time: time(self.spread_time, other.spread_time, "spread time"),
            relative_spread_time: time(
                self.relative_spread_time,
                other.relative_spread_time,
                "relative spread time",
            ),
            bid_size_time: time(self.bid_size_time, other.bid_size_time, "bid size time"),
            ask_size_time: time(self.ask_size_time, other.ask_size_time, "ask size time"),
            narrowest: self.narrowest.min(other.narrowest),
            widest: self.widest.max(other.widest),
            closing: self.closing.max(other.closing),
        }
    }

    pub fn quote_count(&self) -> u64 {
        self.quote_count
    }

    pub fn covered_nanoseconds(&self) -> u64 {
        self.covered_nanoseconds
    }

    /// Spread ticks multiplied by nanoseconds; over `covered_nanoseconds` it is the time-weighted mean spread.
    pub fn spread_time(&self) -> u128 {
        self.spread_time
    }

    /// Spread over midpoint in `RELATIVE_SPREAD_SCALE` units, multiplied by nanoseconds.
    pub fn relative_spread_time(&self) -> u128 {
        self.relative_spread_time
    }

    /// Bid size in share units multiplied by nanoseconds.
    pub fn bid_size_time(&self) -> u128 {
        self.bid_size_time
    }

    /// Ask size in share units multiplied by nanoseconds.
    pub fn ask_size_time(&self) -> u128 {
        self.ask_size_time
    }

    pub fn narrowest(&self) -> Spread {
        self.narrowest
    }

    pub fn widest(&self) -> Spread {
        self.widest
    }

    pub fn closing(&self) -> &StandingQuote {
        &self.closing
    }
}

/// One symbol's quote bar; it exists only for an interval some quote stood in.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QuoteBar {
    symbol: Symbol,
    interval: BarInterval,
    timestamp: DateTime<Utc>,
    sums: QuoteSums,
}

/// Why a quote bar was refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QuoteBarRefusal {
    Misaligned {
        interval: BarInterval,
        timestamp: DateTime<Utc>,
    },
    /// Covered for no time, or longer than the interval it is stamped for.
    Coverage { covered_nanoseconds: u64 },
}

impl std::fmt::Display for QuoteBarRefusal {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Misaligned {
                interval,
                timestamp,
            } => write!(formatter, "{timestamp} does not end a {interval} bar"),
            Self::Coverage {
                covered_nanoseconds,
            } => write!(
                formatter,
                "{covered_nanoseconds} nanoseconds covered is none or longer than the interval"
            ),
        }
    }
}

impl std::error::Error for QuoteBarRefusal {}

impl QuoteBar {
    /// A bar on its interval's grid, covered for some time and for no longer than its interval, a day for a daily bar.
    pub fn new(
        symbol: Symbol,
        interval: BarInterval,
        timestamp: DateTime<Utc>,
        sums: QuoteSums,
    ) -> Result<Self, QuoteBarRefusal> {
        if bucket(timestamp, interval) != timestamp {
            return Err(QuoteBarRefusal::Misaligned {
                interval,
                timestamp,
            });
        }
        let longest: u64 = match interval {
            BarInterval::OneMinute => 60_000_000_000,
            BarInterval::FiveMinute => 300_000_000_000,
            BarInterval::OneDay => 86_400_000_000_000,
        };
        let covered = sums.covered_nanoseconds;
        if covered == 0 || covered > longest {
            return Err(QuoteBarRefusal::Coverage {
                covered_nanoseconds: covered,
            });
        }
        Ok(Self {
            symbol,
            interval,
            timestamp,
            sums,
        })
    }

    pub fn symbol(&self) -> &Symbol {
        &self.symbol
    }

    pub fn interval(&self) -> BarInterval {
        self.interval
    }

    /// The bar's start for an intraday bar, and the 16:00 Eastern close for a daily one, as for `Bar`.
    pub fn timestamp(&self) -> DateTime<Utc> {
        self.timestamp
    }

    pub fn sums(&self) -> &QuoteSums {
        &self.sums
    }
}

/// The bucket an instant falls in at `interval`; a daily bucket is its session's close.
fn bucket(instant: DateTime<Utc>, interval: BarInterval) -> DateTime<Utc> {
    let minute = instant
        .with_second(0)
        .and_then(|instant| instant.with_nanosecond(0))
        .expect("zero seconds and nanoseconds exist in every minute");
    match interval {
        BarInterval::OneMinute => minute,
        BarInterval::FiveMinute => minute - TimeDelta::minutes(i64::from(minute.minute() % 5)),
        BarInterval::OneDay => SessionDate::at(instant).regular_close(),
    }
}

/// Quote bars built so far, one per symbol, interval and bucket.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct QuoteRollup(BTreeMap<(Symbol, BarInterval, DateTime<Utc>), QuoteSums>);

/// Why a quote bar could not be rolled up.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QuoteRollupRefusal {
    Finer { from: BarInterval, to: BarInterval },
}

impl std::fmt::Display for QuoteRollupRefusal {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Finer { from, to } => write!(
                formatter,
                "a {from} quote bar cannot roll up into the finer {to}"
            ),
        }
    }
}

impl std::error::Error for QuoteRollupRefusal {}

impl QuoteRollup {
    /// The fragment `bar` contributes to the `interval` bar containing it.
    pub fn of(bar: &QuoteBar, interval: BarInterval) -> Result<Self, QuoteRollupRefusal> {
        if interval < bar.interval {
            return Err(QuoteRollupRefusal::Finer {
                from: bar.interval,
                to: interval,
            });
        }
        let key = (
            bar.symbol.clone(),
            interval,
            bucket(bar.timestamp, interval),
        );
        Ok(Self(BTreeMap::from([(key, bar.sums.clone())])))
    }

    /// Every bar built, ordered by symbol, interval and timestamp.
    pub fn into_bars(self) -> Vec<QuoteBar> {
        self.0
            .into_iter()
            .map(|((symbol, interval, timestamp), sums)| QuoteBar {
                symbol,
                interval,
                timestamp,
                sums,
            })
            .collect()
    }
}

impl Monoid for QuoteRollup {
    fn empty() -> Self {
        Self::default()
    }

    fn combine(mut self, other: Self) -> Self {
        for (key, sums) in other.0 {
            let merged = match self.0.remove(&key) {
                Some(existing) => existing.combine(sums),
                None => sums,
            };
            self.0.insert(key, merged);
        }
        self
    }
}

/// What a session's fold did with the quotes it was offered.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct QuoteFoldCounts {
    accepted: u64,
    /// Older than the quote standing before it, which a time weighting cannot place.
    out_of_order: u64,
}

impl QuoteFoldCounts {
    pub fn accepted(&self) -> u64 {
        self.accepted
    }

    pub fn out_of_order(&self) -> u64 {
        self.out_of_order
    }
}

/// One session's quotes folded into one-minute quote bars over `[open, close)`; each quote stands until the
/// symbol's next quote or the close, and a quote standing at the open counts from the open.
pub struct QuoteFold {
    open: DateTime<Utc>,
    close: DateTime<Utc>,
    standing: BTreeMap<Symbol, StandingQuote>,
    minutes: QuoteRollup,
    /// Quotes that began in each minute, added once every minute's coverage is known.
    began: BTreeMap<(Symbol, DateTime<Utc>), u64>,
    counts: QuoteFoldCounts,
}

/// Why a fold could not start.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QuoteFoldRefusal {
    CloseNotAfterOpen {
        open: DateTime<Utc>,
        close: DateTime<Utc>,
    },
}

impl std::fmt::Display for QuoteFoldRefusal {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::CloseNotAfterOpen { open, close } => {
                write!(formatter, "the close {close} is not after the open {open}")
            }
        }
    }
}

impl std::error::Error for QuoteFoldRefusal {}

impl QuoteFold {
    pub fn new(open: DateTime<Utc>, close: DateTime<Utc>) -> Result<Self, QuoteFoldRefusal> {
        if close <= open {
            return Err(QuoteFoldRefusal::CloseNotAfterOpen { open, close });
        }
        Ok(Self {
            open,
            close,
            standing: BTreeMap::new(),
            minutes: QuoteRollup::empty(),
            began: BTreeMap::new(),
            counts: QuoteFoldCounts::default(),
        })
    }

    pub fn push(&mut self, quote: &Quote) {
        let symbol = quote.symbol();
        if let Some(previous) = self.standing.get(symbol) {
            if quote.timestamp() < previous.since {
                self.counts.out_of_order += 1;
                return;
            }
            let previous = previous.clone();
            self.stand(symbol, &previous, quote.timestamp());
        }
        self.counts.accepted += 1;
        if (self.open..self.close).contains(&quote.timestamp()) {
            let minute = bucket(quote.timestamp(), BarInterval::OneMinute);
            *self.began.entry((symbol.clone(), minute)).or_insert(0) += 1;
        }
        self.standing
            .insert(symbol.clone(), StandingQuote::of(quote));
    }

    /// The one-minute bars, every standing quote run to the close, with what the fold accepted and dropped.
    pub fn finish(mut self) -> (Vec<QuoteBar>, QuoteFoldCounts) {
        let standing = std::mem::take(&mut self.standing);
        for (symbol, quote) in &standing {
            self.stand(symbol, quote, self.close);
        }
        for ((symbol, minute), count) in std::mem::take(&mut self.began) {
            // A quote replaced within its own nanosecond covers nothing, and its minute holds the one that replaced it.
            if let Some(sums) = self
                .minutes
                .0
                .get_mut(&(symbol, BarInterval::OneMinute, minute))
            {
                sums.quote_count += count;
            }
        }
        let bars = self.minutes.into_bars();
        (bars, self.counts)
    }

    /// Credits `quote` with the part of `[since, until)` inside the session, split at minute boundaries.
    fn stand(&mut self, symbol: &Symbol, quote: &StandingQuote, until: DateTime<Utc>) {
        let mut from = quote.since.max(self.open);
        let until = until.min(self.close);
        while from < until {
            let minute = bucket(from, BarInterval::OneMinute);
            let end = BarInterval::OneMinute.ends(minute).min(until);
            let nanoseconds = (end - from)
                .num_nanoseconds()
                .and_then(|nanoseconds| u64::try_from(nanoseconds).ok())
                .expect("a span inside one minute is a positive count of nanoseconds");
            self.add(symbol, quote, minute, nanoseconds);
            from = end;
        }
    }

    fn add(
        &mut self,
        symbol: &Symbol,
        quote: &StandingQuote,
        minute: DateTime<Utc>,
        nanoseconds: u64,
    ) {
        let key = (symbol.clone(), BarInterval::OneMinute, minute);
        let sums = QuoteSums::standing(quote, nanoseconds);
        let merged = match self.minutes.0.remove(&key) {
            Some(existing) => existing.combine(sums),
            None => sums,
        };
        self.minutes.0.insert(key, merged);
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;
    use crate::common::monoid::{concatenate, laws};

    fn instant(text: &str) -> DateTime<Utc> {
        text.parse().unwrap()
    }

    fn quote(symbol: &str, at: &str, bid: f64, ask: f64, bid_size: u64, ask_size: u64) -> Quote {
        Quote::new(
            Symbol::new(symbol).unwrap(),
            instant(at),
            Price::from_dollars(bid).unwrap(),
            Price::from_dollars(ask).unwrap(),
            Shares::whole(bid_size).unwrap(),
            Shares::whole(ask_size).unwrap(),
        )
        .unwrap()
    }

    /// Each sum past its type panics naming that sum in every build rather than wrapping in release.
    #[test]
    fn test_sums_that_overflow_panic_by_name() {
        let standing =
            StandingQuote::of(&quote("SPY", "2026-10-08T14:00:00Z", 100.0, 100.01, 1, 1));
        let one = QuoteSums::standing(&standing, 1);
        let cases: [(&str, QuoteSums); 6] = [
            (
                "quote count fits u64",
                QuoteSums {
                    quote_count: u64::MAX,
                    ..one.clone()
                },
            ),
            (
                "covered nanoseconds fit u64",
                QuoteSums {
                    covered_nanoseconds: u64::MAX,
                    ..one.clone()
                },
            ),
            (
                "spread time fits u128",
                QuoteSums {
                    spread_time: u128::MAX,
                    ..one.clone()
                },
            ),
            (
                "relative spread time fits u128",
                QuoteSums {
                    relative_spread_time: u128::MAX,
                    ..one.clone()
                },
            ),
            (
                "bid size time fits u128",
                QuoteSums {
                    bid_size_time: u128::MAX,
                    ..one.clone()
                },
            ),
            (
                "ask size time fits u128",
                QuoteSums {
                    ask_size_time: u128::MAX,
                    ..one.clone()
                },
            ),
        ];
        for (expected, full) in cases {
            let addend = QuoteSums {
                quote_count: 1,
                ..one.clone()
            };
            let panicked = std::panic::catch_unwind(|| full.combine(addend)).expect_err(expected);
            let message = panicked
                .downcast_ref::<String>()
                .cloned()
                .or_else(|| panicked.downcast_ref::<&str>().map(|text| text.to_string()))
                .expect("panic message is text");
            assert_eq!(message, expected);
        }
    }

    /// 2026-10-02, 09:30 to 16:00 Eastern.
    fn session() -> QuoteFold {
        QuoteFold::new(
            instant("2026-10-02T13:30:00Z"),
            instant("2026-10-02T20:00:00Z"),
        )
        .unwrap()
    }

    const MINUTE: u64 = 60_000_000_000;

    #[test]
    fn test_a_quote_standing_from_before_the_open_covers_the_whole_session() {
        let mut fold = session();
        fold.push(&quote("AAPL", "2026-10-02T13:00:00Z", 100.00, 100.02, 3, 5));
        let (bars, counts) = fold.finish();
        assert_eq!(bars.len(), 390);
        assert_eq!(counts.accepted(), 1);
        for bar in &bars {
            assert_eq!(bar.sums().covered_nanoseconds(), MINUTE);
            assert_eq!(bar.sums().quote_count(), 0);
            assert_eq!(bar.sums().spread_time(), 20_000 * u128::from(MINUTE));
            // 0.02 over a 100.01 midpoint is 1.99980 basis points, 199.98 hundredths, kept to the nanosecond.
            assert_eq!(bar.sums().relative_spread_time(), 11_998_800_119_988);
            assert_eq!(bar.sums().bid_size_time(), 3_000_000 * u128::from(MINUTE));
        }
        assert_eq!(bars[0].timestamp(), instant("2026-10-02T13:30:00Z"));
        assert_eq!(bars[389].timestamp(), instant("2026-10-02T19:59:00Z"));
    }

    #[test]
    fn test_a_quote_stands_until_the_next_and_is_split_at_the_minute() {
        let mut fold = session();
        fold.push(&quote("AAPL", "2026-10-02T13:30:30Z", 100.00, 100.04, 1, 1));
        fold.push(&quote("AAPL", "2026-10-02T13:31:15Z", 100.00, 100.02, 1, 1));
        fold.push(&quote("AAPL", "2026-10-02T13:31:10Z", 99.00, 101.00, 1, 1));
        let (bars, counts) = fold.finish();
        assert_eq!(counts.out_of_order(), 1);
        let first = &bars[0];
        assert_eq!(first.timestamp(), instant("2026-10-02T13:30:00Z"));
        assert_eq!(first.sums().covered_nanoseconds(), MINUTE / 2);
        assert_eq!(first.sums().quote_count(), 1);
        let second = &bars[1];
        assert_eq!(second.sums().covered_nanoseconds(), MINUTE);
        assert_eq!(second.sums().quote_count(), 1);
        // 15 seconds at four cents, then 45 at two.
        assert_eq!(
            second.sums().spread_time(),
            40_000 * 15_000_000_000 + 20_000 * 45_000_000_000
        );
        assert_eq!(second.sums().narrowest(), Spread::from_ticks(20_000));
        assert_eq!(second.sums().widest(), Spread::from_ticks(40_000));
        assert_eq!(
            second.sums().closing().since(),
            instant("2026-10-02T13:31:15Z")
        );
    }

    #[test]
    fn test_a_penny_on_a_thousand_dollars_keeps_its_relative_spread() {
        let mut fold = session();
        fold.push(&quote(
            "BRK.A",
            "2026-10-02T13:30:00Z",
            1_000.00,
            1_000.01,
            1,
            1,
        ));
        let (bars, _) = fold.finish();
        // 0.01 over a 1,000.005 midpoint is 0.0999995 basis points: 9.99995 hundredths, not the 9 a floor leaves.
        assert_eq!(bars[0].sums().relative_spread_time(), 599_997_000_014);
    }

    #[test]
    fn test_a_crossed_quote_or_inverted_spreads_are_refused() {
        let price = |dollars| Price::from_dollars(dollars).unwrap();
        let at = instant("2026-10-02T13:30:00Z");
        assert_eq!(
            StandingQuote::new(
                at,
                price(10.01),
                price(10.00),
                Shares::from_units(1),
                Shares::from_units(1)
            ),
            Err(QuoteSumsRefusal::Crossed {
                bid: price(10.01),
                ask: price(10.00)
            })
        );
        let closing = StandingQuote::new(
            at,
            price(10.00),
            price(10.01),
            Shares::from_units(1),
            Shares::from_units(1),
        )
        .unwrap();
        assert_eq!(
            QuoteSums::new(
                1,
                1,
                [0; 4],
                Spread::from_ticks(2),
                Spread::from_ticks(1),
                closing
            ),
            Err(QuoteSumsRefusal::Inverted {
                narrowest: Spread::from_ticks(2),
                widest: Spread::from_ticks(1)
            })
        );
    }

    #[test]
    fn test_nothing_after_the_close_is_counted() {
        let mut fold = session();
        fold.push(&quote("AAPL", "2026-10-02T20:00:00Z", 100.00, 100.02, 1, 1));
        let (bars, counts) = fold.finish();
        assert!(bars.is_empty());
        assert_eq!(counts.accepted(), 1);
    }

    fn any_sums() -> impl Strategy<Value = QuoteSums> {
        (
            0_u64..10,
            1_u64..MINUTE,
            1_i64..200_000,
            0_i64..50_000,
            0_i64..1_000,
        )
            .prop_map(|(count, covered, bid, spread, second)| {
                let bid = Price::from_ticks(bid).unwrap();
                let ask = Price::from_ticks(bid.ticks() + spread).unwrap();
                let quote = StandingQuote::new(
                    instant("2026-10-02T13:30:00Z") + TimeDelta::seconds(second),
                    bid,
                    ask,
                    Shares::from_units(7),
                    Shares::from_units(9),
                )
                .unwrap();
                let mut sums = QuoteSums::standing(&quote, covered);
                sums.quote_count = count;
                sums
            })
    }

    fn any_rollup() -> impl Strategy<Value = QuoteRollup> {
        (any_sums(), 0_i64..3).prop_map(|(sums, minute)| {
            let key = (
                Symbol::new("AAPL").unwrap(),
                BarInterval::OneMinute,
                instant("2026-10-02T13:30:00Z") + TimeDelta::minutes(minute),
            );
            QuoteRollup(BTreeMap::from([(key, sums)]))
        })
    }

    proptest! {
        #[test]
        fn property_quote_rollups_are_a_commutative_monoid(
            first in any_rollup(),
            second in any_rollup(),
            third in any_rollup(),
        ) {
            laws::check(first, second, third)?;
        }

        /// Rolling minutes up to the day keeps every sum: the daily bar is the minutes' total.
        #[test]
        fn property_a_daily_bar_holds_the_sum_of_its_minutes(
            quotes in prop::collection::vec((0_i64..23_400, 1_i64..2_000, 0_i64..400), 1..40),
        ) {
            let mut fold = session();
            let mut sorted = quotes.clone();
            sorted.sort();
            for (second, bid, spread) in &sorted {
                let at = instant("2026-10-02T13:30:00Z") + TimeDelta::seconds(*second);
                let bid = Price::from_ticks(bid * 10_000).unwrap();
                let ask = Price::from_ticks(bid.ticks() + spread * 100).unwrap();
                fold.push(&Quote::new(Symbol::new("AAPL").unwrap(), at, bid, ask, Shares::from_units(1), Shares::from_units(1)).unwrap());
            }
            let (minutes, counts) = fold.finish();
            let total_covered: u64 = minutes.iter().map(|bar| bar.sums().covered_nanoseconds()).sum();
            let daily = concatenate(minutes.iter().map(|bar| QuoteRollup::of(bar, BarInterval::OneDay).unwrap())).into_bars();
            prop_assert_eq!(daily.len(), 1);
            prop_assert_eq!(daily[0].timestamp(), instant("2026-10-02T20:00:00Z"));
            prop_assert_eq!(daily[0].sums().covered_nanoseconds(), total_covered);
            prop_assert_eq!(daily[0].sums().quote_count(), counts.accepted());
            // The first quote stands from its start to the close, so coverage is exactly that span.
            let first = sorted[0].0;
            prop_assert_eq!(total_covered, (23_400 - first as u64) * 1_000_000_000);
        }
    }
}
