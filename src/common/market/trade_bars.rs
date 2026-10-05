//! Trade bars: what a session's prints during its regular hours add up to under the consolidated tape's condition
//! rules, as exact totals and the prices only eligible prints may set, rolling up from one minute to five and the day.

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

    fn of(trade: &Trade) -> Self {
        Self::new(
            (trade.timestamp(), trade.price()),
            (trade.timestamp(), trade.price()),
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

    fn of(trade: &Trade) -> Self {
        Self {
            high: trade.price(),
            low: trade.price(),
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

    /// The sums one print contributes under the updates it is allowed.
    fn of(trade: &Trade, allowed: UpdateRules) -> Self {
        Self {
            totals: if allowed.volume {
                TradeTotals::of(trade)
            } else {
                TradeTotals::empty()
            },
            open_close: allowed.open_close.then(|| OpenClose::of(trade)),
            high_low: allowed.high_low.then(|| HighLow::of(trade)),
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

    fn print(trade: &Trade, allowed: UpdateRules) -> Self {
        let key = (
            trade.symbol().clone(),
            BarInterval::OneMinute,
            bucket(trade.timestamp(), BarInterval::OneMinute),
        );
        Self(BTreeMap::from([(key, TradeSums::of(trade, allowed))]))
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
    outside_hours: u64,
    /// Marked as corrected by the vendor, which leaves them out entirely.
    corrected: u64,
    volume_ineligible: u64,
    unresolved: u64,
}

impl TradeFoldCounts {
    pub fn folded(&self) -> u64 {
        self.folded
    }

    pub fn outside_hours(&self) -> u64 {
        self.outside_hours
    }

    pub fn corrected(&self) -> u64 {
        self.corrected
    }

    pub fn volume_ineligible(&self) -> u64 {
        self.volume_ineligible
    }

    pub fn unresolved(&self) -> u64 {
        self.unresolved
    }
}

/// One session's prints folded into one-minute trade bars over `[open, close)`.
pub struct TradeFold {
    open: DateTime<Utc>,
    close: DateTime<Utc>,
    conditions: TradeConditions,
    minutes: TradeRollup,
    counts: TradeFoldCounts,
}

/// Why a fold could not start.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TradeFoldRefusal {
    CloseNotAfterOpen {
        open: DateTime<Utc>,
        close: DateTime<Utc>,
    },
}

impl TradeFold {
    pub fn new(
        open: DateTime<Utc>,
        close: DateTime<Utc>,
        conditions: TradeConditions,
    ) -> Result<Self, TradeFoldRefusal> {
        if close <= open {
            return Err(TradeFoldRefusal::CloseNotAfterOpen { open, close });
        }
        Ok(Self {
            open,
            close,
            conditions,
            minutes: TradeRollup::empty(),
            counts: TradeFoldCounts::default(),
        })
    }

    /// Folds one print carrying the vendor's condition `codes`, unless it falls outside the hours or was corrected.
    pub fn push(&mut self, trade: &Trade, codes: &[u16], corrected: bool) {
        if !(self.open..self.close).contains(&trade.timestamp()) {
            self.counts.outside_hours += 1;
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
        if !allowed.volume {
            self.counts.volume_ineligible += 1;
        }
        self.counts.folded += 1;
        let minutes = std::mem::take(&mut self.minutes);
        self.minutes = minutes.combine(TradeRollup::print(trade, allowed));
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

    fn trade(at: &str, dollars: f64, shares: f64) -> Trade {
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

    fn session() -> TradeFold {
        TradeFold::new(
            instant("2026-10-02T13:30:00Z"),
            instant("2026-10-02T20:00:00Z"),
            conditions(),
        )
        .unwrap()
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
    fn test_an_unknown_condition_counts_volume_and_sets_no_price() {
        let mut fold = session();
        fold.push(&trade("2026-10-02T13:30:01Z", 100.00, 50.0), &[99], false);
        fold.push(&trade("2026-10-02T20:00:00Z", 100.00, 50.0), &[], false);
        let (bars, counts) = fold.finish();
        assert_eq!(bars.len(), 1);
        assert_eq!(bars[0].sums().totals().volume().units(), 50_000_000);
        assert_eq!(bars[0].sums().open_close(), None);
        assert_eq!((counts.unresolved(), counts.outside_hours()), (1, 1));
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
                TradeRollup::print(&trade, UpdateRules::new(volume, high_low, open_close))
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
