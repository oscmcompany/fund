//! Quote bars: the top of book a session's quotes held during its regular hours, each quote weighted by how long it
//! stood, as exact sums that roll up from one minute to five and to the day.

use std::collections::BTreeMap;

use chrono::{DateTime, Utc};

use super::aggregate::{BarKey, RollsUp, Rollup};
use super::record::{BarInterval, Quote};
use super::{Price, QuoteCount, Shares, Symbol};
use crate::common::monoid::{Monoid, Semigroup};

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
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum QuoteSumsRefusal {
    #[error("the bid {bid} is above the ask {ask}")]
    Crossed { bid: Price, ask: Price },
    /// The narrowest spread is wider than the widest.
    #[error("the narrowest spread of {} ticks is wider than the widest of {}", .narrowest.ticks(), .widest.ticks())]
    Inverted { narrowest: Spread, widest: Spread },
    /// The time-weighted spread is outside the narrowest and widest spreads standing for the covered time.
    #[error("a time-weighted spread of {spread} is outside {} to {} ticks over {covered_nanoseconds} nanoseconds", .narrowest.ticks(), .widest.ticks())]
    SpreadOutsideRange {
        spread: u128,
        narrowest: Spread,
        widest: Spread,
        covered_nanoseconds: u64,
    },
}

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

/// A bar's quantities, each multiplied by the nanoseconds it stood; over the covered time, each is its mean.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TimeWeighted {
    /// Spread ticks.
    pub spread: u128,
    /// Spread over midpoint in `RELATIVE_SPREAD_SCALE` units.
    pub relative_spread: u128,
    /// Bid size in share units.
    pub bid_size: u128,
    /// Ask size in share units.
    pub ask_size: u128,
}

impl TimeWeighted {
    fn combine(self, other: Self) -> Self {
        let sum = |left: u128, right: u128, name: &str| {
            left.checked_add(right)
                .unwrap_or_else(|| panic!("{name} fits u128"))
        };
        Self {
            spread: sum(self.spread, other.spread, "spread time"),
            relative_spread: sum(
                self.relative_spread,
                other.relative_spread,
                "relative spread time",
            ),
            bid_size: sum(self.bid_size, other.bid_size, "bid size time"),
            ask_size: sum(self.ask_size, other.ask_size, "ask size time"),
        }
    }
}

/// The exact sums of one bar.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QuoteSums {
    /// Quotes that began inside the bar; a bar can be covered by a quote that began before it and count none.
    quote_count: QuoteCount,
    covered_nanoseconds: u64,
    time_weighted: TimeWeighted,
    narrowest: Spread,
    widest: Spread,
    /// The latest quote standing in the bar; ties on start break on the quote so the combine stays commutative.
    closing: StandingQuote,
}

impl QuoteSums {
    /// The sums of `quote` standing for `nanoseconds`, counting no quote; the fold adds counts by minute.
    fn standing(quote: &StandingQuote, nanoseconds: u64) -> Self {
        let spread = Spread::of(quote.bid, quote.ask);
        let midpoint_doubled =
            u128::from(quote.bid.ticks_unsigned()) + u128::from(quote.ask.ticks_unsigned());
        let time = u128::from(nanoseconds);
        Self {
            quote_count: QuoteCount::default(),
            covered_nanoseconds: nanoseconds,
            time_weighted: TimeWeighted {
                spread: u128::from(spread.0) * time,
                // Weighted before dividing, so a tight spread keeps its precision; the one floor loses under a unit.
                relative_spread: u128::from(spread.0) * 2 * RELATIVE_SPREAD_SCALE * time
                    / midpoint_doubled,
                bid_size: u128::from(quote.bid_size.units()) * time,
                ask_size: u128::from(quote.ask_size.units()) * time,
            },
            narrowest: spread,
            widest: spread,
            closing: quote.clone(),
        }
    }

    /// Sums whose time-weighted spread lies between the narrowest and widest spreads standing for the covered time.
    pub fn new(
        quote_count: QuoteCount,
        covered_nanoseconds: u64,
        time_weighted: TimeWeighted,
        narrowest: Spread,
        widest: Spread,
        closing: StandingQuote,
    ) -> Result<Self, QuoteSumsRefusal> {
        if narrowest > widest {
            return Err(QuoteSumsRefusal::Inverted { narrowest, widest });
        }
        let covered = u128::from(covered_nanoseconds);
        let spread = time_weighted.spread;
        if !(u128::from(narrowest.0) * covered..=u128::from(widest.0) * covered).contains(&spread) {
            return Err(QuoteSumsRefusal::SpreadOutsideRange {
                spread,
                narrowest,
                widest,
                covered_nanoseconds,
            });
        }
        Ok(Self {
            quote_count,
            covered_nanoseconds,
            time_weighted,
            narrowest,
            widest,
            closing,
        })
    }

    pub fn quote_count(&self) -> QuoteCount {
        self.quote_count
    }

    pub fn covered_nanoseconds(&self) -> u64 {
        self.covered_nanoseconds
    }

    pub fn time_weighted(&self) -> TimeWeighted {
        self.time_weighted
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

impl Semigroup for QuoteSums {
    fn combine(self, other: Self) -> Self {
        Self {
            quote_count: self.quote_count.plus(other.quote_count),
            covered_nanoseconds: self
                .covered_nanoseconds
                .checked_add(other.covered_nanoseconds)
                .expect("covered nanoseconds fit u64"),
            time_weighted: self.time_weighted.combine(other.time_weighted),
            narrowest: self.narrowest.min(other.narrowest),
            widest: self.widest.max(other.widest),
            closing: self.closing.max(other.closing),
        }
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
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum QuoteBarRefusal {
    #[error("{timestamp} does not end a {interval} bar")]
    Misaligned {
        interval: BarInterval,
        timestamp: DateTime<Utc>,
    },
    /// Covered for no time, or longer than the interval it is stamped for.
    #[error("{covered_nanoseconds} nanoseconds covered is none or longer than the interval")]
    Coverage { covered_nanoseconds: u64 },
}

impl QuoteBar {
    /// A bar on its interval's grid, covered for some time and for no longer than its interval, a day for a daily bar.
    pub fn new(
        symbol: Symbol,
        interval: BarInterval,
        timestamp: DateTime<Utc>,
        sums: QuoteSums,
    ) -> Result<Self, QuoteBarRefusal> {
        if interval.bucket(timestamp) != timestamp {
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

impl RollsUp for QuoteBar {
    type Sums = QuoteSums;

    fn parts(&self) -> (BarKey, QuoteSums) {
        let key = BarKey {
            symbol: self.symbol.clone(),
            interval: self.interval,
            timestamp: self.timestamp,
        };
        (key, self.sums.clone())
    }

    fn from_parts(key: BarKey, sums: QuoteSums) -> Self {
        Self::new(key.symbol, key.interval, key.timestamp, sums).expect(
            "a rolled-up bucket sits on its grid and is covered no longer than its interval",
        )
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
    minutes: Rollup<QuoteBar>,
    /// Quotes that began in each minute, added once every minute's coverage is known.
    began: BTreeMap<BarKey, u64>,
    counts: QuoteFoldCounts,
}

/// Why a fold could not start.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum QuoteFoldRefusal {
    #[error("the close {close} is not after the open {open}")]
    CloseNotAfterOpen {
        open: DateTime<Utc>,
        close: DateTime<Utc>,
    },
}

impl QuoteFold {
    pub fn new(open: DateTime<Utc>, close: DateTime<Utc>) -> Result<Self, QuoteFoldRefusal> {
        if close <= open {
            return Err(QuoteFoldRefusal::CloseNotAfterOpen { open, close });
        }
        Ok(Self {
            open,
            close,
            standing: BTreeMap::new(),
            minutes: Rollup::empty(),
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
            let key = BarKey {
                symbol: symbol.clone(),
                interval: BarInterval::OneMinute,
                timestamp: BarInterval::OneMinute.bucket(quote.timestamp()),
            };
            *self.began.entry(key).or_insert(0) += 1;
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
        for (key, count) in std::mem::take(&mut self.began) {
            // A quote replaced within its own nanosecond covers nothing, and its minute holds the one that replaced it.
            if let Some(sums) = self.minutes.get_mut(&key) {
                sums.quote_count = sums.quote_count.plus(QuoteCount::new(count));
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
            let minute = BarInterval::OneMinute.bucket(from);
            let end = BarInterval::OneMinute.ends(minute).min(until);
            let nanoseconds = (end - from)
                .num_nanoseconds()
                .and_then(|nanoseconds| u64::try_from(nanoseconds).ok())
                .expect("a span inside one minute is a positive count of nanoseconds");
            let key = BarKey {
                symbol: symbol.clone(),
                interval: BarInterval::OneMinute,
                timestamp: minute,
            };
            self.minutes
                .add(key, QuoteSums::standing(quote, nanoseconds));
            from = end;
        }
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use chrono::TimeDelta;

    use super::*;
    use crate::common::market::aggregate::{check_rollups_compose, roll_up};
    use crate::common::monoid::laws;

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
                    quote_count: QuoteCount::new(u64::MAX),
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
                    time_weighted: TimeWeighted {
                        spread: u128::MAX,
                        ..one.time_weighted
                    },
                    ..one.clone()
                },
            ),
            (
                "relative spread time fits u128",
                QuoteSums {
                    time_weighted: TimeWeighted {
                        relative_spread: u128::MAX,
                        ..one.time_weighted
                    },
                    ..one.clone()
                },
            ),
            (
                "bid size time fits u128",
                QuoteSums {
                    time_weighted: TimeWeighted {
                        bid_size: u128::MAX,
                        ..one.time_weighted
                    },
                    ..one.clone()
                },
            ),
            (
                "ask size time fits u128",
                QuoteSums {
                    time_weighted: TimeWeighted {
                        ask_size: u128::MAX,
                        ..one.time_weighted
                    },
                    ..one.clone()
                },
            ),
        ];
        for (expected, full) in cases {
            let addend = QuoteSums {
                quote_count: QuoteCount::new(1),
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
            assert_eq!(bar.sums().quote_count().count(), 0);
            assert_eq!(
                bar.sums().time_weighted().spread,
                20_000 * u128::from(MINUTE)
            );
            // 0.02 over a 100.01 midpoint is 1.99980 basis points, 199.98 hundredths, kept to the nanosecond.
            assert_eq!(
                bar.sums().time_weighted().relative_spread,
                11_998_800_119_988
            );
            assert_eq!(
                bar.sums().time_weighted().bid_size,
                3_000_000 * u128::from(MINUTE)
            );
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
        assert_eq!(first.sums().quote_count().count(), 1);
        let second = &bars[1];
        assert_eq!(second.sums().covered_nanoseconds(), MINUTE);
        assert_eq!(second.sums().quote_count().count(), 1);
        // 15 seconds at four cents, then 45 at two.
        assert_eq!(
            second.sums().time_weighted().spread,
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
        assert_eq!(
            bars[0].sums().time_weighted().relative_spread,
            599_997_000_014
        );
    }

    #[test]
    fn test_a_crossed_quote_inverted_spreads_or_a_spread_outside_them_are_refused() {
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
                QuoteCount::new(1),
                1,
                weighted(0),
                Spread::from_ticks(2),
                Spread::from_ticks(1),
                closing.clone()
            ),
            Err(QuoteSumsRefusal::Inverted {
                narrowest: Spread::from_ticks(2),
                widest: Spread::from_ticks(1)
            })
        );
        // Spreads of one to three ticks standing for ten nanoseconds weigh between 10 and 30.
        let sums = |spread| {
            QuoteSums::new(
                QuoteCount::new(1),
                10,
                weighted(spread),
                Spread::from_ticks(1),
                Spread::from_ticks(3),
                closing.clone(),
            )
            .map(|sums| sums.time_weighted().spread)
        };
        let outside = |spread| {
            Err(QuoteSumsRefusal::SpreadOutsideRange {
                spread,
                narrowest: Spread::from_ticks(1),
                widest: Spread::from_ticks(3),
                covered_nanoseconds: 10,
            })
        };
        assert_eq!(
            [9, 10, 30, 31].map(sums),
            [outside(9), Ok(10), Ok(30), outside(31)]
        );
    }

    fn weighted(spread: u128) -> TimeWeighted {
        TimeWeighted {
            spread,
            relative_spread: 0,
            bid_size: 0,
            ask_size: 0,
        }
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
                sums.quote_count = QuoteCount::new(count);
                sums
            })
    }

    fn any_rollup() -> impl Strategy<Value = Rollup<QuoteBar>> {
        (any_sums(), 0_i64..3).prop_map(|(sums, minute)| {
            let bar = QuoteBar::new(
                Symbol::new("AAPL").unwrap(),
                BarInterval::OneMinute,
                instant("2026-10-02T13:30:00Z") + TimeDelta::minutes(minute),
                sums,
            )
            .unwrap();
            Rollup::of(&bar, BarInterval::OneMinute).unwrap()
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

        /// Rolling minutes up to the day keeps every sum, the daily bar being the minutes' total, and in stages or at once.
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
            check_rollups_compose(&minutes)?;
            let daily = roll_up(&minutes, BarInterval::OneDay).unwrap();
            prop_assert_eq!(daily.len(), 1);
            prop_assert_eq!(daily[0].timestamp(), instant("2026-10-02T20:00:00Z"));
            prop_assert_eq!(daily[0].sums().covered_nanoseconds(), total_covered);
            prop_assert_eq!(daily[0].sums().quote_count().count(), counts.accepted());
            // The first quote stands from its start to the close, so coverage is exactly that span.
            let first = sorted[0].0;
            prop_assert_eq!(total_covered, (23_400 - first as u64) * 1_000_000_000);
        }
    }
}
