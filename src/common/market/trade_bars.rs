//! Trade bars: what a session's prints, extended hours and closing cross included, add up to under the consolidated
//! tape's condition rules, as exact totals and the prices only eligible prints may set, rolling up from one minute to five and the day.

use std::collections::BTreeMap;

use chrono::{DateTime, TimeDelta, Timelike, Utc};

use super::aggregate::TradeTotals;
use super::record::{BarInterval, Trade};
use super::{Price, Symbol};
use crate::common::monoid::Monoid;
use crate::common::time::SessionDate;

/// What a print carrying one sale condition may update on the consolidated tape.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UpdateRules {
    volume: bool,
    high_low: bool,
    open_close: bool,
}

impl UpdateRules {
    pub fn new(volume: bool, high_low: bool, open_close: bool) -> Self {
        Self {
            volume,
            high_low,
            open_close,
        }
    }

    pub fn volume(&self) -> bool {
        self.volume
    }

    pub fn high_low(&self) -> bool {
        self.high_low
    }

    pub fn open_close(&self) -> bool {
        self.open_close
    }
}

/// The vendor's sale conditions by code, with the rules each imposes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TradeConditions(BTreeMap<u16, UpdateRules>);

/// What a print's conditions let it update; a code the table does not hold leaves the print unresolved.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Eligibility {
    /// Every condition is known; a print updates what all of them allow.
    Resolved(UpdateRules),
    /// A condition the table does not hold: the print still counts toward volume, as the legacy fold counted it, but
    /// sets no price, since nothing says it may.
    Unresolved { code: u16 },
}

impl TradeConditions {
    pub fn new(rules: BTreeMap<u16, UpdateRules>) -> Self {
        Self(rules)
    }

    pub fn rules(&self) -> &BTreeMap<u16, UpdateRules> {
        &self.0
    }

    /// The rules a print with `codes` falls under: each update allowed only if every code allows it.
    pub fn eligibility(&self, codes: &[u16]) -> Eligibility {
        let mut allowed = UpdateRules::new(true, true, true);
        for code in codes {
            match self.0.get(code) {
                Some(rules) => {
                    allowed = UpdateRules::new(
                        allowed.volume && rules.volume,
                        allowed.high_low && rules.high_low,
                        allowed.open_close && rules.open_close,
                    );
                }
                None => return Eligibility::Unresolved { code: *code },
            }
        }
        Eligibility::Resolved(allowed)
    }
}

/// One print as the fold sees it: a trade, or a price published with no shares, as the corrected consolidated close
/// is after the session.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Print {
    Trade(Trade),
    Unsized {
        symbol: Symbol,
        timestamp: DateTime<Utc>,
        price: Price,
    },
}

impl Print {
    pub fn symbol(&self) -> &Symbol {
        match self {
            Self::Trade(trade) => trade.symbol(),
            Self::Unsized { symbol, .. } => symbol,
        }
    }

    pub fn timestamp(&self) -> DateTime<Utc> {
        match self {
            Self::Trade(trade) => trade.timestamp(),
            Self::Unsized { timestamp, .. } => *timestamp,
        }
    }

    pub fn price(&self) -> Price {
        match self {
            Self::Trade(trade) => trade.price(),
            Self::Unsized { price, .. } => *price,
        }
    }
}

/// The earliest and latest prices a bar's eligible prints set; equal instants break on price so the combine is
/// commutative.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OpenClose {
    open: (DateTime<Utc>, Price),
    close: (DateTime<Utc>, Price),
}

impl OpenClose {
    pub fn new(open: (DateTime<Utc>, Price), close: (DateTime<Utc>, Price)) -> Self {
        Self { open, close }
    }

    fn of(print: &Print) -> Self {
        Self::new(
            (print.timestamp(), print.price()),
            (print.timestamp(), print.price()),
        )
    }

    fn combine(self, other: Self) -> Self {
        Self {
            open: self.open.min(other.open),
            close: self.close.max(other.close),
        }
    }

    /// The opening print's time and price.
    pub fn open(&self) -> (DateTime<Utc>, Price) {
        self.open
    }

    /// The closing print's time and price.
    pub fn close(&self) -> (DateTime<Utc>, Price) {
        self.close
    }
}

/// The highest and lowest prices a bar's eligible prints set.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HighLow {
    high: Price,
    low: Price,
}

/// Why a high and low were refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HighLowRefusal {
    Inverted { high: Price, low: Price },
}

impl HighLow {
    pub fn new(high: Price, low: Price) -> Result<Self, HighLowRefusal> {
        if low > high {
            return Err(HighLowRefusal::Inverted { high, low });
        }
        Ok(Self { high, low })
    }

    fn of(print: &Print) -> Self {
        Self {
            high: print.price(),
            low: print.price(),
        }
    }

    fn combine(self, other: Self) -> Self {
        Self {
            high: self.high.max(other.high),
            low: self.low.min(other.low),
        }
    }

    pub fn high(&self) -> Price {
        self.high
    }

    pub fn low(&self) -> Price {
        self.low
    }
}

/// One bar's sums: totals over prints eligible for volume, and each price pair over the prints eligible to set it,
/// `None` when no print in the bar was.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TradeSums {
    totals: TradeTotals,
    open_close: Option<OpenClose>,
    high_low: Option<HighLow>,
}

impl TradeSums {
    pub fn new(
        totals: TradeTotals,
        open_close: Option<OpenClose>,
        high_low: Option<HighLow>,
    ) -> Self {
        Self {
            totals,
            open_close,
            high_low,
        }
    }

    /// The sums one print contributes under the updates it is allowed; a print with no shares adds no totals.
    fn of(print: &Print, allowed: UpdateRules) -> Self {
        let totals = match print {
            Print::Trade(trade) if allowed.volume => TradeTotals::of(trade),
            Print::Trade(_) | Print::Unsized { .. } => TradeTotals::empty(),
        };
        Self {
            totals,
            open_close: allowed.open_close.then(|| OpenClose::of(print)),
            high_low: allowed.high_low.then(|| HighLow::of(print)),
        }
    }

    fn combine(self, other: Self) -> Self {
        Self {
            totals: self.totals.combine(other.totals),
            open_close: either(self.open_close, other.open_close, OpenClose::combine),
            high_low: either(self.high_low, other.high_low, HighLow::combine),
        }
    }

    pub fn totals(&self) -> TradeTotals {
        self.totals
    }

    pub fn open_close(&self) -> Option<OpenClose> {
        self.open_close
    }

    pub fn high_low(&self) -> Option<HighLow> {
        self.high_low
    }
}

/// Two optional fragments combined, `None` being the identity.
fn either<T>(left: Option<T>, right: Option<T>, combine: fn(T, T) -> T) -> Option<T> {
    match (left, right) {
        (Some(left), Some(right)) => Some(combine(left, right)),
        (Some(only), None) | (None, Some(only)) => Some(only),
        (None, None) => None,
    }
}

/// One symbol's trade bar; it exists only for an interval some print in the session's hours fell in.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TradeBar {
    symbol: Symbol,
    interval: BarInterval,
    timestamp: DateTime<Utc>,
    sums: TradeSums,
}

/// Why a trade bar was refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TradeBarRefusal {
    Misaligned {
        interval: BarInterval,
        timestamp: DateTime<Utc>,
    },
}

impl TradeBar {
    pub fn new(
        symbol: Symbol,
        interval: BarInterval,
        timestamp: DateTime<Utc>,
        sums: TradeSums,
    ) -> Result<Self, TradeBarRefusal> {
        if bucket(timestamp, interval) != timestamp {
            return Err(TradeBarRefusal::Misaligned {
                interval,
                timestamp,
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

    pub fn sums(&self) -> &TradeSums {
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

/// Trade bars built so far, one per symbol, interval and bucket.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TradeRollup(BTreeMap<(Symbol, BarInterval, DateTime<Utc>), TradeSums>);

/// Why a trade bar could not be rolled up.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TradeRollupRefusal {
    Finer { from: BarInterval, to: BarInterval },
}

impl TradeRollup {
    /// The fragment `bar` contributes to the `interval` bar containing it.
    pub fn of(bar: &TradeBar, interval: BarInterval) -> Result<Self, TradeRollupRefusal> {
        if interval < bar.interval {
            return Err(TradeRollupRefusal::Finer {
                from: bar.interval,
                to: interval,
            });
        }
        let key = (
            bar.symbol.clone(),
            interval,
            bucket(bar.timestamp, interval),
        );
        Ok(Self(BTreeMap::from([(key, bar.sums)])))
    }

    fn print(print: &Print, allowed: UpdateRules) -> Self {
        let key = (
            print.symbol().clone(),
            BarInterval::OneMinute,
            bucket(print.timestamp(), BarInterval::OneMinute),
        );
        Self(BTreeMap::from([(key, TradeSums::of(print, allowed))]))
    }

    /// Every bar built, ordered by symbol, interval and timestamp.
    pub fn into_bars(self) -> Vec<TradeBar> {
        self.0
            .into_iter()
            .map(|((symbol, interval, timestamp), sums)| TradeBar {
                symbol,
                interval,
                timestamp,
                sums,
            })
            .collect()
    }
}

impl Monoid for TradeRollup {
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

/// What a session's fold did with the prints it was offered.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TradeFoldCounts {
    folded: u64,
    /// Stamped for another session's Eastern date.
    other_session: u64,
    /// Marked as corrected by the vendor, which leaves them out entirely.
    corrected: u64,
    volume_ineligible: u64,
    /// Published with no shares, which may set prices and never volume.
    unsized_prints: u64,
    unresolved: u64,
}

impl TradeFoldCounts {
    pub fn folded(&self) -> u64 {
        self.folded
    }

    pub fn other_session(&self) -> u64 {
        self.other_session
    }

    pub fn corrected(&self) -> u64 {
        self.corrected
    }

    pub fn volume_ineligible(&self) -> u64 {
        self.volume_ineligible
    }

    pub fn unsized_prints(&self) -> u64 {
        self.unsized_prints
    }

    pub fn unresolved(&self) -> u64 {
        self.unresolved
    }
}

/// One session's prints folded into one-minute trade bars over its whole Eastern day; the condition rules, not the
/// hours, decide what a print may set, so an extended-hours print adds volume and the closing cross sets the close.
pub struct TradeFold {
    session: SessionDate,
    conditions: TradeConditions,
    minutes: TradeRollup,
    counts: TradeFoldCounts,
}

impl TradeFold {
    pub fn new(session: SessionDate, conditions: TradeConditions) -> Self {
        Self {
            session,
            conditions,
            minutes: TradeRollup::empty(),
            counts: TradeFoldCounts::default(),
        }
    }

    /// Folds one print carrying the vendor's condition `codes`, unless it is another session's or was corrected.
    pub fn push(&mut self, print: &Print, codes: &[u16], corrected: bool) {
        if SessionDate::at(print.timestamp()) != self.session {
            self.counts.other_session += 1;
            return;
        }
        if corrected {
            self.counts.corrected += 1;
            return;
        }
        let allowed = match self.conditions.eligibility(codes) {
            Eligibility::Resolved(allowed) => allowed,
            Eligibility::Unresolved { .. } => {
                self.counts.unresolved += 1;
                UpdateRules::new(true, false, false)
            }
        };
        match print {
            Print::Trade(_) if !allowed.volume => self.counts.volume_ineligible += 1,
            Print::Unsized { .. } => self.counts.unsized_prints += 1,
            Print::Trade(_) => {}
        }
        self.counts.folded += 1;
        let minutes = std::mem::take(&mut self.minutes);
        self.minutes = minutes.combine(TradeRollup::print(print, allowed));
    }

    /// The one-minute bars and what the fold did with every print.
    pub fn finish(self) -> (Vec<TradeBar>, TradeFoldCounts) {
        (self.minutes.into_bars(), self.counts)
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;
    use crate::common::market::Shares;
    use crate::common::monoid::{concatenate, laws};

    fn instant(text: &str) -> DateTime<Utc> {
        text.parse().unwrap()
    }

    fn trade(at: &str, dollars: f64, shares: f64) -> Print {
        Print::Trade(trade_record(at, dollars, shares))
    }

    fn trade_record(at: &str, dollars: f64, shares: f64) -> Trade {
        Trade::new(
            Symbol::new("AAPL").unwrap(),
            instant(at),
            Price::from_dollars(dollars).unwrap(),
            Shares::from_float(shares).unwrap(),
        )
        .unwrap()
    }

    /// Codes 10 (derivatively priced), 37 (odd lot) and 15 (official close) as Massive's table gives them on
    /// 2026-10-05.
    fn conditions() -> TradeConditions {
        TradeConditions::new(BTreeMap::from([
            (10, UpdateRules::new(true, true, false)),
            (15, UpdateRules::new(false, false, false)),
            (37, UpdateRules::new(true, false, false)),
        ]))
    }

    fn october_second() -> SessionDate {
        SessionDate::from_date(chrono::NaiveDate::from_ymd_opt(2026, 10, 2).unwrap())
    }

    fn session() -> TradeFold {
        TradeFold::new(october_second(), conditions())
    }

    #[test]
    fn test_each_price_comes_only_from_prints_allowed_to_set_it() {
        let mut fold = session();
        fold.push(&trade("2026-10-02T13:30:01Z", 100.00, 50.0), &[37], false);
        fold.push(&trade("2026-10-02T13:30:02Z", 101.00, 200.0), &[], false);
        fold.push(&trade("2026-10-02T13:30:03Z", 99.00, 100.0), &[10], false);
        fold.push(&trade("2026-10-02T13:30:04Z", 150.00, 900.0), &[15], false);
        fold.push(&trade("2026-10-02T13:30:05Z", 100.50, 10.0), &[], true);
        fold.push(&trade("2026-10-02T13:31:00Z", 100.25, 0.5), &[37], false);
        let (bars, counts) = fold.finish();
        assert_eq!(bars.len(), 2);
        let first = bars[0].sums();
        assert_eq!(first.totals().count().count(), 3);
        assert_eq!(first.totals().volume().units(), 350_000_000);
        let open_close = first.open_close().unwrap();
        assert_eq!(open_close.open().1.ticks(), 101_000_000);
        assert_eq!(open_close.close().1.ticks(), 101_000_000);
        let high_low = first.high_low().unwrap();
        assert_eq!(
            (high_low.high().ticks(), high_low.low().ticks()),
            (101_000_000, 99_000_000)
        );
        // An odd-lot-only minute has volume but nothing to set its prices.
        let second = bars[1].sums();
        assert_eq!(second.totals().volume().units(), 500_000);
        assert_eq!((second.open_close(), second.high_low()), (None, None));
        assert_eq!(
            (
                counts.folded(),
                counts.corrected(),
                counts.volume_ineligible()
            ),
            (5, 1, 1)
        );
    }

    #[test]
    fn test_the_closing_cross_after_four_sets_the_close_and_after_hours_adds_volume_only() {
        // The closing print's code sets everything and lands after 16:00 Eastern; Form T only adds volume.
        let conditions = TradeConditions::new(BTreeMap::from([
            (8, UpdateRules::new(true, true, true)),
            (12, UpdateRules::new(true, false, false)),
        ]));
        let mut fold = TradeFold::new(october_second(), conditions);
        fold.push(&trade("2026-10-02T19:59:59Z", 100.00, 100.0), &[], false);
        fold.push(&trade("2026-10-02T20:02:10Z", 100.05, 7_000.0), &[8], false);
        fold.push(&trade("2026-10-02T21:30:00Z", 101.00, 50.0), &[12], false);
        let minutes = fold.finish().0;
        let daily = concatenate(
            minutes
                .iter()
                .map(|bar| TradeRollup::of(bar, BarInterval::OneDay).unwrap()),
        )
        .into_bars();
        let sums = daily[0].sums();
        assert_eq!(sums.totals().volume().units(), 7_150_000_000);
        assert_eq!(sums.open_close().unwrap().close().1.ticks(), 100_050_000);
        assert_eq!(sums.high_low().unwrap().high().ticks(), 100_050_000);
    }

    #[test]
    fn test_the_unsized_corrected_close_sets_the_close_and_no_volume() {
        // Code 38 as Massive's table gives it: no volume, but the high, low, open and close.
        let conditions =
            TradeConditions::new(BTreeMap::from([(38, UpdateRules::new(false, true, true))]));
        let mut fold = TradeFold::new(october_second(), conditions);
        fold.push(&trade("2026-10-02T19:59:55Z", 87.67, 100.0), &[], false);
        let corrected_close = Print::Unsized {
            symbol: Symbol::new("AAPL").unwrap(),
            timestamp: instant("2026-10-02T20:10:00.003861Z"),
            price: Price::from_dollars(87.68).unwrap(),
        };
        fold.push(&corrected_close, &[38], false);
        let (minutes, counts) = fold.finish();
        let daily = concatenate(
            minutes
                .iter()
                .map(|bar| TradeRollup::of(bar, BarInterval::OneDay).unwrap()),
        )
        .into_bars();
        let sums = daily[0].sums();
        assert_eq!(sums.open_close().unwrap().close().1.ticks(), 87_680_000);
        assert_eq!(sums.totals().count().count(), 1);
        assert_eq!(sums.totals().volume().units(), 100_000_000);
        assert_eq!(counts.unsized_prints(), 1);
    }

    #[test]
    fn test_an_unknown_condition_counts_volume_and_sets_no_price() {
        let mut fold = session();
        fold.push(&trade("2026-10-02T13:30:01Z", 100.00, 50.0), &[99], false);
        // 00:30 Eastern on the next day is another session's.
        fold.push(&trade("2026-10-03T04:30:00Z", 100.00, 50.0), &[], false);
        let (bars, counts) = fold.finish();
        assert_eq!(bars.len(), 1);
        assert_eq!(bars[0].sums().totals().volume().units(), 50_000_000);
        assert_eq!(bars[0].sums().open_close(), None);
        assert_eq!((counts.unresolved(), counts.other_session()), (1, 1));
    }

    fn any_rollup() -> impl Strategy<Value = TradeRollup> {
        (
            0_i64..120,
            1_i64..2_000_000,
            1_u64..1_000_000,
            any::<[bool; 3]>(),
        )
            .prop_map(|(second, ticks, units, [volume, high_low, open_close])| {
                let trade = Trade::new(
                    Symbol::new("AAPL").unwrap(),
                    instant("2026-10-02T13:30:00Z") + TimeDelta::seconds(second),
                    Price::from_ticks(ticks).unwrap(),
                    Shares::from_units(units),
                )
                .unwrap();
                TradeRollup::print(
                    &Print::Trade(trade),
                    UpdateRules::new(volume, high_low, open_close),
                )
            })
    }

    proptest! {
        #[test]
        fn property_trade_rollups_are_a_commutative_monoid(
            first in any_rollup(),
            second in any_rollup(),
            third in any_rollup(),
        ) {
            laws::check(first, second, third)?;
        }

        /// The daily bar's totals are its minutes' totals, whatever the grouping.
        #[test]
        fn property_a_daily_bar_totals_its_minutes(minutes in prop::collection::vec(any_rollup(), 1..30)) {
            let minute_bars = concatenate(minutes).into_bars();
            let daily = concatenate(minute_bars.iter().map(|bar| TradeRollup::of(bar, BarInterval::OneDay).unwrap())).into_bars();
            prop_assert_eq!(daily.len(), 1);
            let volume: u64 = minute_bars.iter().map(|bar| bar.sums().totals().volume().units()).sum();
            prop_assert_eq!(daily[0].sums().totals().volume().units(), volume);
            let high = minute_bars.iter().filter_map(|bar| bar.sums().high_low()).map(|pair| pair.high()).max();
            prop_assert_eq!(daily[0].sums().high_low().map(|pair| pair.high()), high);
        }
    }
}
