//! Aggregates over market records, each a commutative monoid so fragments merge in any grouping and order.

use chrono::{DateTime, Utc};

use super::record::{Bar, BarInterval, BarRefusal, Ohlc, Trade};
use super::{DollarVolume, Price, Shares, Symbol};
use crate::common::monoid::Monoid;

/// Count, volume and dollar volume of a set of trades, all exact.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TradeTotals {
    count: u64,
    volume: Shares,
    dollar_volume: DollarVolume,
}

impl TradeTotals {
    pub fn of(trade: &Trade) -> Self {
        Self {
            count: 1,
            volume: trade.size(),
            dollar_volume: DollarVolume::of(trade.price(), trade.size()),
        }
    }

    pub fn count(&self) -> u64 {
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
        match self.volume.count() {
            0 => None,
            shares => Some(self.dollar_volume.dollars() / shares as f64),
        }
    }
}

impl Monoid for TradeTotals {
    fn empty() -> Self {
        Self::default()
    }

    fn combine(self, other: Self) -> Self {
        Self {
            count: self.count + other.count,
            volume: self.volume.plus(other.volume),
            dollar_volume: self.dollar_volume.plus(other.dollar_volume),
        }
    }
}

/// The bars combined so far, whose open is the earliest bar's and close the latest's.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BarRollup {
    Empty,
    Span(Span),
}

/// Two bars stamped alike break the tie on price, so the combine stays commutative.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Span {
    first: (DateTime<Utc>, Price),
    last: (DateTime<Utc>, Price),
    high: Price,
    low: Price,
    volume: Shares,
}

/// Why a rollup did not become a bar.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RollupRefusal {
    Empty,
    Bar(BarRefusal),
}

impl BarRollup {
    pub fn of(bar: &Bar) -> Self {
        let prices = bar.prices();
        Self::Span(Span {
            first: (bar.timestamp(), prices.open()),
            last: (bar.timestamp(), prices.close()),
            high: prices.high(),
            low: prices.low(),
            volume: bar.volume(),
        })
    }

    /// The bar these fragments make at `interval`, stamped `timestamp`.
    pub fn into_bar(
        self,
        symbol: Symbol,
        interval: BarInterval,
        timestamp: DateTime<Utc>,
    ) -> Result<Bar, RollupRefusal> {
        match self {
            Self::Empty => Err(RollupRefusal::Empty),
            Self::Span(span) => {
                let prices = Ohlc::new(span.first.1, span.high, span.low, span.last.1)
                    .expect("every combined open and close lies within the combined range");
                Bar::new(symbol, interval, timestamp, prices, span.volume)
                    .map_err(RollupRefusal::Bar)
            }
        }
    }
}

impl Monoid for BarRollup {
    fn empty() -> Self {
        Self::Empty
    }

    fn combine(self, other: Self) -> Self {
        match (self, other) {
            (Self::Empty, rollup) | (rollup, Self::Empty) => rollup,
            (Self::Span(left), Self::Span(right)) => Self::Span(Span {
                first: left.first.min(right.first),
                last: left.last.max(right.last),
                high: left.high.max(right.high),
                low: left.low.min(right.low),
                volume: left.volume.plus(right.volume),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use chrono::TimeDelta;
    use proptest::prelude::*;

    use super::*;
    use crate::common::monoid::{concatenate, laws};
    use crate::common::time::SessionDate;

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
            Shares::new(size),
        )
        .unwrap()
    }

    fn minute_bar(timestamp: &str, open: f64, high: f64, low: f64, close: f64, volume: u64) -> Bar {
        Bar::new(
            symbol(),
            BarInterval::OneMinute,
            instant(timestamp),
            Ohlc::new(price(open), price(high), price(low), price(close)).unwrap(),
            Shares::new(volume),
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
        assert_eq!(totals.count(), 2);
        assert_eq!(totals.volume(), Shares::new(400));
        assert_eq!(totals.dollar_volume().to_string(), "4060.0000");
        assert_eq!(totals.volume_weighted_average_price(), Some(10.15));
        assert_eq!(TradeTotals::empty().volume_weighted_average_price(), None);
    }

    #[test]
    fn test_minute_bars_roll_into_a_five_minute_bar() {
        let bars = [
            minute_bar("2026-07-31T14:32:00Z", 10.3, 10.9, 10.1, 10.8, 50),
            minute_bar("2026-07-31T14:30:00Z", 10.0, 10.5, 9.8, 10.2, 100),
            minute_bar("2026-07-31T14:31:00Z", 10.2, 10.4, 9.5, 10.3, 70),
        ];
        let rolled = concatenate(bars.iter().map(BarRollup::of))
            .into_bar(
                symbol(),
                BarInterval::FiveMinute,
                instant("2026-07-31T14:30:00Z"),
            )
            .unwrap();
        let prices = rolled.prices();
        assert_eq!(
            (prices.open(), prices.high(), prices.low(), prices.close()),
            (price(10.0), price(10.9), price(9.5), price(10.8))
        );
        assert_eq!(rolled.volume(), Shares::new(220));
    }

    #[test]
    fn test_a_daily_rollup_is_stamped_at_the_close() {
        let session = SessionDate::at(instant("2026-07-31T14:30:00Z"));
        let bar = minute_bar("2026-07-31T14:30:00Z", 10.0, 10.5, 9.8, 10.2, 100);
        let daily = BarRollup::of(&bar)
            .into_bar(symbol(), BarInterval::OneDay, session.regular_close())
            .unwrap();
        assert_eq!(daily.timestamp(), instant("2026-07-31T20:00:00Z"));
        assert_eq!(
            BarRollup::of(&bar).into_bar(symbol(), BarInterval::OneDay, session.midnight()),
            Err(RollupRefusal::Bar(BarRefusal::Misaligned {
                interval: BarInterval::OneDay,
                timestamp: session.midnight()
            }))
        );
        assert_eq!(
            BarRollup::Empty.into_bar(symbol(), BarInterval::OneDay, session.regular_close()),
            Err(RollupRefusal::Empty)
        );
    }

    /// Prices under $10,000 and sizes under a billion shares, so a few hundred sums stay far from overflow.
    fn any_trade() -> impl Strategy<Value = TradeTotals> {
        (1_i64..100_000_000, 1_u64..1_000_000_000).prop_map(|(ticks, size)| {
            TradeTotals::of(
                &Trade::new(
                    symbol(),
                    instant("2026-07-31T14:31:00Z"),
                    Price::from_ticks(ticks).unwrap(),
                    Shares::new(size),
                )
                .unwrap(),
            )
        })
    }

    /// One-minute bars within a day, few enough minutes that timestamp ties occur.
    fn any_bar() -> impl Strategy<Value = BarRollup> {
        (
            0_i64..30,
            prop::array::uniform4(1_i64..1_000),
            0_u64..1_000_000,
        )
            .prop_map(|(minute, mut ticks, volume)| {
                ticks.sort();
                let [low, first, second, high] = ticks.map(|tick| Price::from_ticks(tick).unwrap());
                BarRollup::of(
                    &Bar::new(
                        symbol(),
                        BarInterval::OneMinute,
                        instant("2026-07-31T14:30:00Z") + TimeDelta::minutes(minute),
                        Ohlc::new(first, high, low, second).unwrap(),
                        Shares::new(volume),
                    )
                    .unwrap(),
                )
            })
    }

    fn any_rollup() -> impl Strategy<Value = BarRollup> {
        prop_oneof![1 => Just(BarRollup::Empty), 9 => any_bar()]
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
        fn property_bar_rollups_merge_in_any_order(
            (ordered, shuffled) in prop::collection::vec(any_rollup(), 0..50)
                .prop_flat_map(|bars| (Just(bars.clone()), Just(bars).prop_shuffle()))
        ) {
            laws::check_any_order(ordered, shuffled)?;
        }
    }
}
