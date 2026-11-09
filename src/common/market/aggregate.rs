//! Aggregates over market records, each a commutative monoid so fragments merge in any grouping and order.

use std::collections::BTreeMap;

use chrono::{DateTime, Utc};

use super::record::{BarInterval, Partition, Trade};
use super::{DollarVolume, Shares, Symbol, TradeCount};
use crate::common::monoid::{Monoid, Semigroup};

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

/// The bar a fragment belongs to: its symbol, interval and bucket.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct BarKey {
    pub(super) symbol: Symbol,
    pub(super) interval: BarInterval,
    pub(super) timestamp: DateTime<Utc>,
}

/// A bar that splits into its key and sums and is rebuilt from them, so a `Rollup` can merge any kind of bar.
pub trait RollsUp: Sized {
    type Sums: Semigroup + Clone + PartialEq + Eq + std::fmt::Debug;

    fn parts(&self) -> (BarKey, Self::Sums);

    /// Panics on parts no bar of this kind could hold, such as a quote bar covered for longer than its interval.
    fn from_parts(key: BarKey, sums: Self::Sums) -> Self;
}

/// The bars built so far, one per symbol, interval and bucket, so fragments of different bars never mix.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Rollup<B: RollsUp>(BTreeMap<BarKey, B::Sums>);

/// Why a bar could not be rolled up.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum RollupRefusal {
    #[error("a {from} bar cannot roll up into the finer {to}")]
    Finer { from: BarInterval, to: BarInterval },
}

impl<B: RollsUp> Rollup<B> {
    /// The fragment `bar` contributes to the `interval` bar containing it.
    pub fn of(bar: &B, interval: BarInterval) -> Result<Self, RollupRefusal> {
        let (key, sums) = bar.parts();
        if interval < key.interval {
            return Err(RollupRefusal::Finer {
                from: key.interval,
                to: interval,
            });
        }
        let key = BarKey {
            timestamp: interval.bucket(key.timestamp),
            interval,
            ..key
        };
        Ok(Self(BTreeMap::from([(key, sums)])))
    }

    /// Merges `sums` into the bar at `key` in place, as combining with a rollup of that one fragment would.
    pub(super) fn add(&mut self, key: BarKey, sums: B::Sums) {
        let merged = match self.0.remove(&key) {
            Some(existing) => existing.combine(sums),
            None => sums,
        };
        self.0.insert(key, merged);
    }

    pub(super) fn get_mut(&mut self, key: &BarKey) -> Option<&mut B::Sums> {
        self.0.get_mut(key)
    }

    /// Takes out every bar that has ended by `through`, leaving the rest.
    pub(super) fn split_through(&mut self, through: DateTime<Utc>) -> Self {
        let (ended, open) = std::mem::take(&mut self.0)
            .into_iter()
            .partition(|(key, _)| key.interval.ends(key.timestamp) <= through);
        self.0 = open;
        Self(ended)
    }

    /// Every bar built, ordered by symbol, interval and timestamp.
    pub fn into_bars(self) -> Vec<B> {
        self.0
            .into_iter()
            .map(|(key, sums)| B::from_parts(key, sums))
            .collect()
    }
}

impl<B: RollsUp> Monoid for Rollup<B> {
    fn empty() -> Self {
        Self(BTreeMap::new())
    }

    fn combine(mut self, other: Self) -> Self {
        for (key, sums) in other.0 {
            self.add(key, sums);
        }
        self
    }
}

/// `bars` rolled up into `interval` bars, ordered by symbol, interval and timestamp.
pub fn roll_up<B: RollsUp>(bars: &[B], interval: BarInterval) -> Result<Vec<B>, RollupRefusal> {
    bars.iter()
        .try_fold(Rollup::empty(), |rolled, bar| {
            Ok(rolled.combine(Rollup::of(bar, interval)?))
        })
        .map(Rollup::into_bars)
}

/// A fold's one-minute bars beside their five-minute and daily rollups, each with its interval and the daily last, as a
/// session's ticks are written; panics on a daily bar, which no fold hands out.
pub fn session_bars<B: RollsUp>(minutes: Partition<B>) -> [(BarInterval, Partition<B>); 3] {
    let roll = |interval| {
        let bars =
            roll_up(minutes.rows(), interval).expect("a fold's minutes roll up to coarser bars");
        let bars =
            Partition::try_from(bars).expect("a partition's minutes roll up to at least one bar");
        (interval, bars)
    };
    let five_minutes = roll(BarInterval::FiveMinute);
    let daily = roll(BarInterval::OneDay);
    [(BarInterval::OneMinute, minutes), five_minutes, daily]
}

/// The daily bars `session_bars` writes are its five-minute bars rolled up, and the minutes rolled up at once.
#[cfg(test)]
pub(super) fn check_rollups_compose<B: RollsUp + PartialEq + std::fmt::Debug>(
    minutes: &[B],
) -> Result<(), proptest::test_runner::TestCaseError> {
    let roll = |bars: &[B], interval| roll_up(bars, interval).unwrap();
    let rolled = match Partition::try_from(roll(minutes, BarInterval::OneMinute)) {
        Ok(rolled) => rolled,
        Err(super::record::PartitionRefusal::Empty) => return Ok(()),
    };
    let [(minute, minute_bars), (five, five_minutes), (day, daily)] = session_bars(rolled);
    proptest::prop_assert_eq!(
        (minute, five, day),
        (
            BarInterval::OneMinute,
            BarInterval::FiveMinute,
            BarInterval::OneDay
        )
    );
    proptest::prop_assert_eq!(
        minute_bars.into_rows(),
        roll(minutes, BarInterval::OneMinute)
    );
    proptest::prop_assert_eq!(
        roll(five_minutes.rows(), BarInterval::OneDay),
        roll(minutes, BarInterval::OneDay)
    );
    proptest::prop_assert_eq!(
        roll(five_minutes.rows(), BarInterval::OneDay),
        daily.into_rows()
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use chrono::TimeDelta;
    use proptest::prelude::*;

    use super::*;
    use crate::common::market::trade_bars::{HighLow, OpenClose, TradeBar, TradeSums};
    use crate::common::market::{Price, StampedPrice};
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

    /// A minute opened at its start and closed a second before its end, with a tenth of its volume in trades.
    fn minute_bar(
        name: &str,
        timestamp: &str,
        [open, high, low, close]: [f64; 4],
        volume: u64,
    ) -> TradeBar {
        let at = instant(timestamp);
        let shares = Shares::whole(volume).unwrap();
        TradeBar::new(
            Symbol::new(name).unwrap(),
            BarInterval::OneMinute,
            at,
            TradeSums::new(
                TradeTotals::new(
                    TradeCount::new(volume / 10),
                    shares,
                    DollarVolume::of(price(close), shares),
                ),
                Some(
                    OpenClose::new(
                        StampedPrice::new(at, price(open)),
                        StampedPrice::new(at + TimeDelta::seconds(59), price(close)),
                    )
                    .unwrap(),
                ),
                Some(HighLow::new(price(high), price(low)).unwrap()),
            ),
        )
        .unwrap()
    }

    #[test]
    fn test_trade_totals_sum_exactly() {
        let totals = concatenate(
            [trade(10.0, 100), trade(10.2, 300)]
                .iter()
                .map(TradeTotals::of),
        );
        assert_eq!(totals.count(), TradeCount::new(2));
        assert_eq!(totals.volume(), Shares::whole(400).unwrap());
        assert_eq!(totals.dollar_volume().to_string(), "4060.00");
        assert_eq!(totals.dollar_volume().units(), 4_060_000_000_000_000);
        assert_eq!(TradeTotals::empty().dollar_volume().units(), 0);
    }

    fn roll(bars: &[TradeBar], interval: BarInterval) -> Vec<TradeBar> {
        roll_up(bars, interval).unwrap()
    }

    #[test]
    fn test_minute_bars_roll_into_their_own_symbol_and_window() {
        let bars = [
            minute_bar(
                "AAPL",
                "2026-07-31T14:32:00Z",
                [10.3, 10.9, 10.1, 10.15],
                50,
            ),
            minute_bar(
                "AAPL",
                "2026-07-31T14:30:00Z",
                [10.25, 10.5, 9.8, 10.2],
                100,
            ),
            minute_bar(
                "MSFT",
                "2026-07-31T14:31:00Z",
                [400.0, 401.0, 399.0, 400.5],
                10,
            ),
            minute_bar("AAPL", "2026-07-31T14:36:00Z", [10.8, 11.0, 10.7, 10.9], 30),
            minute_bar("AAPL", "2026-07-31T14:31:00Z", [10.2, 10.4, 9.5, 10.3], 70),
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
        let sums = rolled[0].sums();
        let (open_close, high_low) = (sums.open_close().unwrap(), sums.high_low().unwrap());
        assert_eq!(
            (
                open_close.open().price(),
                high_low.high(),
                high_low.low(),
                open_close.close().price()
            ),
            (price(10.25), price(10.9), price(9.5), price(10.15))
        );
        assert_eq!(
            rolled
                .iter()
                .map(|bar| bar.sums().totals().volume())
                .collect::<Vec<_>>(),
            [
                Shares::whole(220).unwrap(),
                Shares::whole(30).unwrap(),
                Shares::whole(10).unwrap()
            ]
        );
        assert_eq!(
            rolled[2].sums().open_close().unwrap().open().price(),
            price(400.0)
        );
        assert_eq!(
            rolled
                .iter()
                .map(|bar| bar.sums().totals().count())
                .collect::<Vec<_>>(),
            [TradeCount::new(22), TradeCount::new(3), TradeCount::new(1)]
        );
        assert_eq!(
            rolled[0].sums().totals().dollar_volume().to_string(),
            "2248.50"
        );
    }

    #[test]
    fn test_a_daily_rollup_is_stamped_at_its_session_close() {
        let bars = [
            minute_bar("AAPL", "2026-07-31T14:30:00Z", [10.0, 10.5, 9.8, 10.2], 100),
            minute_bar("AAPL", "2026-01-14T15:30:00Z", [10.0, 10.5, 9.8, 10.2], 100),
        ];
        let stamps: Vec<DateTime<Utc>> = roll(&bars, BarInterval::OneDay)
            .iter()
            .map(TradeBar::timestamp)
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
                "AAPL",
                "2026-07-31T14:30:00Z",
                [10.0, 10.5, 9.8, 10.2],
                100,
            )],
            BarInterval::FiveMinute,
        );
        assert_eq!(
            Rollup::of(&five[0], BarInterval::OneMinute),
            Err(RollupRefusal::Finer {
                from: BarInterval::FiveMinute,
                to: BarInterval::OneMinute
            })
        );
        assert_eq!(Rollup::<TradeBar>::empty().into_bars(), Vec::new());
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
    /// timestamps tie; a bar may set no prices, as one of only volume-only prints does.
    fn any_minute_bar() -> impl Strategy<Value = TradeBar> {
        (
            prop::sample::select(vec!["AAPL", "MSFT"]),
            0_i64..2,
            0_i64..12,
            prop::array::uniform4(1_i64..1_000),
            0_i64..60,
            0_u64..1_000_000,
            0_u64..10_000,
            any::<bool>(),
        )
            .prop_map(
                |(symbol, day, minute, mut ticks, second, volume, trades, priced)| {
                    ticks.sort();
                    let [low, first, second_price, high] =
                        ticks.map(|tick| Price::from_ticks(tick).unwrap());
                    let at = instant("2026-07-30T14:30:00Z")
                        + TimeDelta::days(day)
                        + TimeDelta::minutes(minute);
                    let shares = Shares::whole(volume).unwrap();
                    let open_close = OpenClose::new(
                        StampedPrice::new(at, first),
                        StampedPrice::new(at + TimeDelta::seconds(second), second_price),
                    )
                    .unwrap();
                    TradeBar::new(
                        Symbol::new(symbol).unwrap(),
                        BarInterval::OneMinute,
                        at,
                        TradeSums::new(
                            TradeTotals::new(
                                TradeCount::new(trades),
                                shares,
                                DollarVolume::of(first, shares),
                            ),
                            priced.then_some(open_close),
                            priced.then(|| HighLow::new(high, low).unwrap()),
                        ),
                    )
                    .unwrap()
                },
            )
    }

    fn any_bar() -> impl Strategy<Value = Rollup<TradeBar>> {
        (
            any_minute_bar(),
            prop::sample::select(vec![
                BarInterval::OneMinute,
                BarInterval::FiveMinute,
                BarInterval::OneDay,
            ]),
        )
            .prop_map(|(bar, interval)| Rollup::of(&bar, interval).unwrap())
    }

    fn any_rollup() -> impl Strategy<Value = Rollup<TradeBar>> {
        prop_oneof![1 => Just(Rollup::empty()), 9 => any_bar()]
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

        #[test]
        fn property_rollups_compose(bars in prop::collection::vec(any_minute_bar(), 1..50)) {
            check_rollups_compose(&bars)?;
        }

        /// A bar splits into parts it is rebuilt from unchanged.
        #[test]
        fn property_a_bar_is_rebuilt_from_its_parts(bar in any_minute_bar()) {
            let (key, sums) = bar.parts();
            prop_assert_eq!(TradeBar::from_parts(key, sums), bar);
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
