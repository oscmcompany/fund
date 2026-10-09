//! Aggregates over market records, each a commutative monoid so fragments merge in any grouping and order.

use std::collections::BTreeMap;

use chrono::{DateTime, TimeDelta, Timelike, Utc};

use super::record::{Bar, BarInterval, BarPrices, Trade};
use super::{DollarVolume, Price, Shares, StampedPrice, Symbol, TradeCount};
use crate::common::monoid::Monoid;
use crate::common::time::SessionDate;

/// Count, volume and dollar volume of a set of trades, all exact.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TradeTotals {
    count: TradeCount,
    volume: Shares,
    dollar_volume: DollarVolume,
}

impl TradeTotals {
    /// Totals read back from storage, where each was summed exactly when written.
    pub fn new(count: TradeCount, volume: Shares, dollar_volume: DollarVolume) -> Self {
        Self {
            count,
            volume,
            dollar_volume,
        }
    }

    pub fn of(trade: &Trade) -> Self {
        Self {
            count: TradeCount::new(1),
            volume: trade.size(),
            dollar_volume: DollarVolume::of(trade.price(), trade.size()),
        }
    }

    pub fn count(&self) -> TradeCount {
        self.count
    }

    pub fn volume(&self) -> Shares {
        self.volume
    }

    pub fn dollar_volume(&self) -> DollarVolume {
        self.dollar_volume
    }

    /// In dollars, derived from the exact sums; `None` when nothing traded.
    pub fn volume_weighted_average_price(&self) -> Option<f64> {
        self.dollar_volume.average_over(self.volume)
    }
}

impl Monoid for TradeTotals {
    fn empty() -> Self {
        Self::default()
    }

    fn combine(self, other: Self) -> Self {
        Self {
            count: self.count.plus(other.count),
            volume: self.volume.plus(other.volume),
            dollar_volume: self.dollar_volume.plus(other.dollar_volume),
        }
    }
}

/// The bars built so far, one per symbol, interval and bucket, so fragments of different bars never mix.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct BarRollup(BTreeMap<BarKey, Span>);

/// The bar a fragment belongs to, taken from the source bar rather than from the caller.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct BarKey {
    symbol: Symbol,
    interval: BarInterval,
    timestamp: DateTime<Utc>,
}

/// One bar's extremes: its open is the earliest fragment's, its close the latest's, and fragments stamped alike
/// break the tie on price so the combine stays commutative.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Span {
    first: StampedPrice,
    last: StampedPrice,
    high: Price,
    low: Price,
    volume: Shares,
    /// Unreported in any fragment makes it unreported for the bar, since a partial sum reads as a whole one.
    trade_count: Option<TradeCount>,
    dollar_volume: Option<DollarVolume>,
}

impl Span {
    fn combine(self, other: Self) -> Self {
        Self {
            first: self.first.min(other.first),
            last: self.last.max(other.last),
            high: self.high.max(other.high),
            low: self.low.min(other.low),
            volume: self.volume.plus(other.volume),
            trade_count: self
                .trade_count
                .zip(other.trade_count)
                .map(|(left, right)| left.plus(right)),
            dollar_volume: self
                .dollar_volume
                .zip(other.dollar_volume)
                .map(|(left, right)| left.plus(right)),
        }
    }
}

/// Why a bar could not be rolled up.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RollupRefusal {
    Finer { from: BarInterval, to: BarInterval },
}

impl std::fmt::Display for RollupRefusal {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Finer { from, to } => {
                write!(formatter, "a {from} bar cannot roll up into the finer {to}")
            }
        }
    }
}

impl std::error::Error for RollupRefusal {}

impl BarRollup {
    /// The fragment `bar` contributes to the `interval` bar containing it; a daily bucket is its session's close.
    pub fn of(bar: &Bar, interval: BarInterval) -> Result<Self, RollupRefusal> {
        if interval < bar.interval() {
            return Err(RollupRefusal::Finer {
                from: bar.interval(),
                to: interval,
            });
        }
        let start = bar.timestamp();
        let timestamp = match interval {
            BarInterval::OneMinute => start,
            BarInterval::FiveMinute => start - TimeDelta::minutes(i64::from(start.minute() % 5)),
            BarInterval::OneDay => SessionDate::at(start).regular_close(),
        };
        let key = BarKey {
            symbol: bar.symbol().clone(),
            interval,
            timestamp,
        };
        let prices = bar.prices();
        let span = Span {
            first: StampedPrice::new(start, prices.open()),
            last: StampedPrice::new(start, prices.close()),
            high: prices.high(),
            low: prices.low(),
            volume: bar.volume(),
            trade_count: bar.trade_count(),
            dollar_volume: bar.dollar_volume(),
        };
        Ok(Self(BTreeMap::from([(key, span)])))
    }

    /// Every bar built, ordered by symbol, interval and timestamp.
    pub fn into_bars(self) -> Vec<Bar> {
        self.0
            .into_iter()
            .map(|(key, span)| {
                let prices =
                    BarPrices::new(span.first.price(), span.high, span.low, span.last.price())
                        .expect("every combined open and close lies within the combined range");
                Bar::new(
                    key.symbol,
                    key.interval,
                    key.timestamp,
                    prices,
                    span.volume,
                    span.trade_count,
                    span.dollar_volume,
                )
                .expect("a bucket timestamp sits on its interval's grid")
            })
            .collect()
    }
}

impl Monoid for BarRollup {
    fn empty() -> Self {
        Self::default()
    }

    fn combine(mut self, other: Self) -> Self {
        for (key, span) in other.0 {
            let merged = match self.0.remove(&key) {
                Some(existing) => existing.combine(span),
                None => span,
            };
            self.0.insert(key, merged);
        }
        self
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;
    use crate::common::monoid::{concatenate, laws};

    fn price(dollars: f64) -> Price {
        Price::from_dollars(dollars).unwrap()
    }

    fn instant(text: &str) -> DateTime<Utc> {
        text.parse().unwrap()
    }

    fn symbol() -> Symbol {
        Symbol::new("AAPL").unwrap()
    }

    fn trade(dollars: f64, size: u64) -> Trade {
        Trade::new(
            symbol(),
            instant("2026-07-31T14:31:00Z"),
            price(dollars),
            Shares::whole(size).unwrap(),
        )
        .unwrap()
    }

    fn minute_bar(timestamp: &str, open: f64, high: f64, low: f64, close: f64, volume: u64) -> Bar {
        Bar::new(
            symbol(),
            BarInterval::OneMinute,
            instant(timestamp),
            BarPrices::new(price(open), price(high), price(low), price(close)).unwrap(),
            Shares::whole(volume).unwrap(),
            Some(TradeCount::new(volume / 10)),
            Some(DollarVolume::of(
                price(close),
                Shares::whole(volume).unwrap(),
            )),
        )
        .unwrap()
    }

    #[test]
    fn test_trade_totals_derive_the_average_price() {
        let totals = concatenate(
            [trade(10.0, 100), trade(10.2, 300)]
                .iter()
                .map(TradeTotals::of),
        );
        assert_eq!(totals.count(), TradeCount::new(2));
        assert_eq!(totals.volume(), Shares::whole(400).unwrap());
        assert_eq!(totals.dollar_volume().to_string(), "4060.00");
        assert_eq!(totals.volume_weighted_average_price(), Some(10.15));
        assert_eq!(TradeTotals::empty().volume_weighted_average_price(), None);
    }

    fn roll(bars: &[Bar], interval: BarInterval) -> Vec<Bar> {
        concatenate(bars.iter().map(|bar| BarRollup::of(bar, interval).unwrap())).into_bars()
    }

    #[test]
    fn test_minute_bars_roll_into_their_own_symbol_and_window() {
        let other = Bar::new(
            Symbol::new("MSFT").unwrap(),
            BarInterval::OneMinute,
            instant("2026-07-31T14:31:00Z"),
            BarPrices::new(price(400.0), price(401.0), price(399.0), price(400.5)).unwrap(),
            Shares::whole(10).unwrap(),
            None,
            None,
        )
        .unwrap();
        let bars = [
            minute_bar("2026-07-31T14:32:00Z", 10.3, 10.9, 10.1, 10.15, 50),
            minute_bar("2026-07-31T14:30:00Z", 10.25, 10.5, 9.8, 10.2, 100),
            other,
            minute_bar("2026-07-31T14:36:00Z", 10.8, 11.0, 10.7, 10.9, 30),
            minute_bar("2026-07-31T14:31:00Z", 10.2, 10.4, 9.5, 10.3, 70),
        ];
        let rolled = roll(&bars, BarInterval::FiveMinute);
        let keys: Vec<(&str, DateTime<Utc>)> = rolled
            .iter()
            .map(|bar| (bar.symbol().as_str(), bar.timestamp()))
            .collect();
        assert_eq!(
            keys,
            [
                ("AAPL", instant("2026-07-31T14:30:00Z")),
                ("AAPL", instant("2026-07-31T14:35:00Z")),
                ("MSFT", instant("2026-07-31T14:30:00Z")),
            ]
        );
        let prices = rolled[0].prices();
        assert_eq!(
            (prices.open(), prices.high(), prices.low(), prices.close()),
            (price(10.25), price(10.9), price(9.5), price(10.15))
        );
        assert_eq!(
            rolled.iter().map(|bar| bar.volume()).collect::<Vec<_>>(),
            [
                Shares::whole(220).unwrap(),
                Shares::whole(30).unwrap(),
                Shares::whole(10).unwrap()
            ]
        );
        assert_eq!(rolled[2].prices().open(), price(400.0));
        // The AAPL 14:30 bucket sums reported counts; MSFT reported none, so neither does its bar.
        assert_eq!(
            rolled
                .iter()
                .map(|bar| bar.trade_count())
                .collect::<Vec<_>>(),
            [Some(TradeCount::new(22)), Some(TradeCount::new(3)), None]
        );
        assert_eq!(rolled[0].dollar_volume().unwrap().to_string(), "2248.50");
        assert_eq!(rolled[2].volume_weighted_average_price(), None);
    }

    /// The laws alone also admit reading unreported as zero, so the absorbing rule is asserted directly.
    #[test]
    fn test_one_unreported_fragment_leaves_the_bucket_unreported() {
        let reported = minute_bar("2026-07-31T14:30:00Z", 10.0, 10.5, 9.8, 10.2, 100);
        let unreported = Bar::new(
            symbol(),
            BarInterval::OneMinute,
            instant("2026-07-31T14:31:00Z"),
            BarPrices::new(price(10.2), price(10.4), price(9.5), price(10.3)).unwrap(),
            Shares::whole(70).unwrap(),
            None,
            None,
        )
        .unwrap();
        let rolled = roll(&[reported, unreported], BarInterval::FiveMinute);
        assert_eq!(rolled.len(), 1);
        assert_eq!(rolled[0].volume(), Shares::whole(170).unwrap());
        assert_eq!(rolled[0].trade_count(), None);
        assert_eq!(rolled[0].dollar_volume(), None);
    }

    #[test]
    fn test_a_daily_rollup_is_stamped_at_its_session_close() {
        let bars = [
            minute_bar("2026-07-31T14:30:00Z", 10.0, 10.5, 9.8, 10.2, 100),
            minute_bar("2026-01-14T15:30:00Z", 10.0, 10.5, 9.8, 10.2, 100),
        ];
        let stamps: Vec<DateTime<Utc>> = roll(&bars, BarInterval::OneDay)
            .iter()
            .map(Bar::timestamp)
            .collect();
        // 16:00 Eastern is 21:00 UTC in winter and 20:00 UTC in summer.
        assert_eq!(
            stamps,
            [
                instant("2026-01-14T21:00:00Z"),
                instant("2026-07-31T20:00:00Z")
            ]
        );
    }

    #[test]
    fn test_a_bar_is_not_rolled_into_a_finer_one() {
        let five = roll(
            &[minute_bar(
                "2026-07-31T14:30:00Z",
                10.0,
                10.5,
                9.8,
                10.2,
                100,
            )],
            BarInterval::FiveMinute,
        );
        assert_eq!(
            BarRollup::of(&five[0], BarInterval::OneMinute),
            Err(RollupRefusal::Finer {
                from: BarInterval::FiveMinute,
                to: BarInterval::OneMinute
            })
        );
        assert_eq!(BarRollup::empty().into_bars(), Vec::new());
    }

    /// Prices under $10,000 and sizes under a billion shares, so a few hundred sums stay far from overflow.
    fn any_trade() -> impl Strategy<Value = TradeTotals> {
        (1_i64..100_000_000, 1_u64..1_000_000_000).prop_map(|(ticks, size)| {
            TradeTotals::of(
                &Trade::new(
                    symbol(),
                    instant("2026-07-31T14:31:00Z"),
                    Price::from_ticks(ticks).unwrap(),
                    Shares::whole(size).unwrap(),
                )
                .unwrap(),
            )
        })
    }

    /// One-minute bars for two symbols over a dozen minutes on two sessions, so buckets are shared and
    /// timestamps tie.
    fn any_minute_bar() -> impl Strategy<Value = Bar> {
        (
            prop::sample::select(vec!["AAPL", "MSFT"]),
            0_i64..2,
            0_i64..12,
            prop::array::uniform4(1_i64..1_000),
            0_u64..1_000_000,
            prop::option::of(0_u64..10_000),
            prop::option::of(0_u128..1_000_000_000_000_000),
        )
            .prop_map(
                |(symbol, day, minute, mut ticks, volume, trades, dollar_units)| {
                    ticks.sort();
                    let [low, first, second, high] =
                        ticks.map(|tick| Price::from_ticks(tick).unwrap());
                    Bar::new(
                        Symbol::new(symbol).unwrap(),
                        BarInterval::OneMinute,
                        instant("2026-07-30T14:30:00Z")
                            + TimeDelta::days(day)
                            + TimeDelta::minutes(minute),
                        BarPrices::new(first, high, low, second).unwrap(),
                        Shares::whole(volume).unwrap(),
                        trades.map(TradeCount::new),
                        dollar_units.map(DollarVolume::from_units),
                    )
                    .unwrap()
                },
            )
    }

    fn any_bar() -> impl Strategy<Value = BarRollup> {
        (
            any_minute_bar(),
            prop::sample::select(vec![
                BarInterval::OneMinute,
                BarInterval::FiveMinute,
                BarInterval::OneDay,
            ]),
        )
            .prop_map(|(bar, interval)| BarRollup::of(&bar, interval).unwrap())
    }

    fn any_rollup() -> impl Strategy<Value = BarRollup> {
        prop_oneof![1 => Just(BarRollup::empty()), 9 => any_bar()]
    }

    proptest! {
        #[test]
        fn property_trade_totals_are_a_commutative_monoid(
            first in any_trade(), second in any_trade(), third in any_trade()
        ) {
            laws::check(first, second, third)?;
        }

        #[test]
        fn property_trade_totals_merge_in_any_order(
            (ordered, shuffled) in prop::collection::vec(any_trade(), 0..50)
                .prop_flat_map(|trades| (Just(trades.clone()), Just(trades).prop_shuffle()))
        ) {
            laws::check_any_order(ordered, shuffled)?;
        }

        #[test]
        fn property_bar_rollups_are_a_commutative_monoid(
            first in any_rollup(), second in any_rollup(), third in any_rollup()
        ) {
            laws::check(first, second, third)?;
        }

        /// Rolling up in stages builds the same bars as rolling up at once.
        #[test]
        fn property_rollups_compose(bars in prop::collection::vec(any_minute_bar(), 0..50)) {
            let direct = roll(&bars, BarInterval::OneDay);
            prop_assert_eq!(
                roll(&roll(&bars, BarInterval::FiveMinute), BarInterval::OneDay),
                direct.clone()
            );
            prop_assert_eq!(
                roll(&roll(&bars, BarInterval::OneMinute), BarInterval::OneDay),
                direct
            );
        }

        #[test]
        fn property_bar_rollups_merge_in_any_order(
            (ordered, shuffled) in prop::collection::vec(any_rollup(), 0..50)
                .prop_flat_map(|bars| (Just(bars.clone()), Just(bars).prop_shuffle()))
        ) {
            laws::check_any_order(ordered, shuffled)?;
        }
    }
}
