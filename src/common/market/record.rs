//! The market records every reader maps into: bars, quotes and trades, each valid by construction.

use chrono::{DateTime, TimeDelta, Timelike, Utc};

use super::{DollarVolume, Price, Shares, Symbol, TradeCount};
use crate::common::time::SessionDate;

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    serde::Serialize,
    serde::Deserialize,
    strum::Display,
    strum::EnumString,
    strum::IntoStaticStr,
    strum::EnumIter,
)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum BarInterval {
    OneMinute,
    FiveMinute,
    OneDay,
}

/// Open, high, low and close, with the open and close inside `[low, high]`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Ohlc {
    open: Price,
    high: Price,
    low: Price,
    close: Price,
}

/// Why a set of bar prices was refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OhlcRefusal {
    OutsideRange {
        open: Price,
        high: Price,
        low: Price,
        close: Price,
    },
}

impl Ohlc {
    pub fn new(open: Price, high: Price, low: Price, close: Price) -> Result<Self, OhlcRefusal> {
        let range = low..=high;
        if range.contains(&open) && range.contains(&close) {
            Ok(Self {
                open,
                high,
                low,
                close,
            })
        } else {
            Err(OhlcRefusal::OutsideRange {
                open,
                high,
                low,
                close,
            })
        }
    }

    pub fn open(&self) -> Price {
        self.open
    }

    pub fn high(&self) -> Price {
        self.high
    }

    pub fn low(&self) -> Price {
        self.low
    }

    pub fn close(&self) -> Price {
        self.close
    }
}

/// A bar stamped at its period's open, or for a daily bar at the 16:00 Eastern close of its session.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Bar {
    symbol: Symbol,
    interval: BarInterval,
    timestamp: DateTime<Utc>,
    prices: Ohlc,
    volume: Shares,
    /// `None` when the vendor did not report it.
    trade_count: Option<TradeCount>,
    /// `None` when the vendor did not report an average to derive it from.
    dollar_volume: Option<DollarVolume>,
}

/// Why a bar was refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BarRefusal {
    Misaligned {
        interval: BarInterval,
        timestamp: DateTime<Utc>,
    },
}

impl Bar {
    /// A bar whose timestamp sits on its interval's grid.
    pub fn new(
        symbol: Symbol,
        interval: BarInterval,
        timestamp: DateTime<Utc>,
        prices: Ohlc,
        volume: Shares,
        trade_count: Option<TradeCount>,
        dollar_volume: Option<DollarVolume>,
    ) -> Result<Self, BarRefusal> {
        let on_minute = timestamp.second() == 0 && timestamp.nanosecond() == 0;
        let aligned = match interval {
            BarInterval::OneMinute => on_minute,
            BarInterval::FiveMinute => on_minute && timestamp.minute().is_multiple_of(5),
            BarInterval::OneDay => timestamp == SessionDate::at(timestamp).regular_close(),
        };
        if !aligned {
            return Err(BarRefusal::Misaligned {
                interval,
                timestamp,
            });
        }
        Ok(Self {
            symbol,
            interval,
            timestamp,
            prices,
            volume,
            trade_count,
            dollar_volume,
        })
    }

    pub fn symbol(&self) -> &Symbol {
        &self.symbol
    }

    pub fn interval(&self) -> BarInterval {
        self.interval
    }

    pub fn timestamp(&self) -> DateTime<Utc> {
        self.timestamp
    }

    /// The instant the bar's period ends: an intraday bar is stamped at its start and a daily bar at its close.
    pub fn ends(&self) -> DateTime<Utc> {
        match self.interval {
            BarInterval::OneMinute => self.timestamp + TimeDelta::minutes(1),
            BarInterval::FiveMinute => self.timestamp + TimeDelta::minutes(5),
            BarInterval::OneDay => self.timestamp,
        }
    }

    pub fn prices(&self) -> Ohlc {
        self.prices
    }

    pub fn volume(&self) -> Shares {
        self.volume
    }

    pub fn trade_count(&self) -> Option<TradeCount> {
        self.trade_count
    }

    pub fn dollar_volume(&self) -> Option<DollarVolume> {
        self.dollar_volume
    }

    /// In dollars, for presentation; `None` when unreported or when nothing traded.
    pub fn volume_weighted_average_price(&self) -> Option<f64> {
        self.dollar_volume?.average_over(self.volume)
    }
}

/// A top-of-book quote in shares; a locked book is a quote, a crossed one is not.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Quote {
    symbol: Symbol,
    timestamp: DateTime<Utc>,
    bid: Price,
    ask: Price,
    bid_size: Shares,
    ask_size: Shares,
}

/// Why a quote was refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QuoteRefusal {
    Crossed { bid: Price, ask: Price },
}

impl Quote {
    pub fn new(
        symbol: Symbol,
        timestamp: DateTime<Utc>,
        bid: Price,
        ask: Price,
        bid_size: Shares,
        ask_size: Shares,
    ) -> Result<Self, QuoteRefusal> {
        if bid > ask {
            return Err(QuoteRefusal::Crossed { bid, ask });
        }
        Ok(Self {
            symbol,
            timestamp,
            bid,
            ask,
            bid_size,
            ask_size,
        })
    }

    pub fn symbol(&self) -> &Symbol {
        &self.symbol
    }

    pub fn timestamp(&self) -> DateTime<Utc> {
        self.timestamp
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

/// One print.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Trade {
    symbol: Symbol,
    timestamp: DateTime<Utc>,
    price: Price,
    size: Shares,
}

/// Why a trade was refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TradeRefusal {
    NoShares { price: Price },
}

impl Trade {
    pub fn new(
        symbol: Symbol,
        timestamp: DateTime<Utc>,
        price: Price,
        size: Shares,
    ) -> Result<Self, TradeRefusal> {
        if size.is_zero() {
            return Err(TradeRefusal::NoShares { price });
        }
        Ok(Self {
            symbol,
            timestamp,
            price,
            size,
        })
    }

    pub fn symbol(&self) -> &Symbol {
        &self.symbol
    }

    pub fn timestamp(&self) -> DateTime<Utc> {
        self.timestamp
    }

    pub fn price(&self) -> Price {
        self.price
    }

    pub fn size(&self) -> Shares {
        self.size
    }
}

#[cfg(test)]
mod tests {
    use strum::IntoEnumIterator;

    use super::*;

    fn price(dollars: f64) -> Price {
        Price::from_dollars(dollars).unwrap()
    }

    fn instant(text: &str) -> DateTime<Utc> {
        text.parse().unwrap()
    }

    fn prices() -> Ohlc {
        Ohlc::new(price(10.0), price(11.0), price(9.0), price(10.5)).unwrap()
    }

    fn bar(interval: BarInterval, timestamp: &str) -> Result<Bar, BarRefusal> {
        Bar::new(
            Symbol::new("AAPL").unwrap(),
            interval,
            instant(timestamp),
            prices(),
            Shares::whole(100).unwrap(),
            None,
            None,
        )
    }

    #[test]
    fn test_bar_interval_names_round_trip() {
        let names: Vec<&str> = BarInterval::iter().map(Into::into).collect();
        assert_eq!(names, ["one_minute", "five_minute", "one_day"]);
        for interval in BarInterval::iter() {
            assert_eq!(interval.to_string().parse(), Ok(interval));
            let json = serde_json::to_string(&interval).unwrap();
            assert_eq!(json, format!("\"{interval}\""));
            assert_eq!(
                serde_json::from_str::<BarInterval>(&json).unwrap(),
                interval
            );
        }
    }

    #[test]
    fn test_an_open_or_close_outside_the_range_is_refused() {
        assert_eq!(
            Ohlc::new(price(12.0), price(11.0), price(9.0), price(10.0)),
            Err(OhlcRefusal::OutsideRange {
                open: price(12.0),
                high: price(11.0),
                low: price(9.0),
                close: price(10.0),
            })
        );
        assert!(Ohlc::new(price(9.0), price(9.0), price(9.0), price(9.0)).is_ok());
        assert!(Ohlc::new(price(10.0), price(11.0), price(9.0), price(8.0)).is_err());
    }

    #[test]
    fn test_a_bar_sits_on_its_interval_grid() {
        assert!(bar(BarInterval::OneMinute, "2026-07-31T14:31:00Z").is_ok());
        assert!(bar(BarInterval::FiveMinute, "2026-07-31T14:35:00Z").is_ok());
        // 16:00 Eastern is 20:00 UTC in summer and 21:00 UTC in winter.
        assert!(bar(BarInterval::OneDay, "2026-07-31T20:00:00Z").is_ok());
        assert!(bar(BarInterval::OneDay, "2026-01-14T21:00:00Z").is_ok());
        for (interval, timestamp) in [
            (BarInterval::OneMinute, "2026-07-31T14:31:30Z"),
            (BarInterval::FiveMinute, "2026-07-31T14:31:00Z"),
            (BarInterval::OneDay, "2026-01-14T20:00:00Z"),
            (BarInterval::OneDay, "2026-07-31T04:00:00Z"),
        ] {
            assert_eq!(
                bar(interval, timestamp),
                Err(BarRefusal::Misaligned {
                    interval,
                    timestamp: instant(timestamp)
                }),
                "{interval} {timestamp}"
            );
        }
    }

    #[test]
    fn test_a_bar_ends_after_its_period_and_a_daily_bar_at_its_stamp() {
        for (interval, timestamp, ends) in [
            (
                BarInterval::OneMinute,
                "2026-07-31T14:31:00Z",
                "2026-07-31T14:32:00Z",
            ),
            (
                BarInterval::FiveMinute,
                "2026-07-31T14:35:00Z",
                "2026-07-31T14:40:00Z",
            ),
            (
                BarInterval::OneDay,
                "2026-07-31T20:00:00Z",
                "2026-07-31T20:00:00Z",
            ),
        ] {
            assert_eq!(
                bar(interval, timestamp).unwrap().ends(),
                instant(ends),
                "{interval}"
            );
        }
    }

    #[test]
    fn test_a_bar_derives_its_average_from_its_dollar_volume() {
        let volume = Shares::whole(200).unwrap();
        let bar = Bar::new(
            Symbol::new("AAPL").unwrap(),
            BarInterval::OneMinute,
            instant("2026-07-31T14:31:00Z"),
            prices(),
            volume,
            Some(TradeCount::new(3)),
            Some(DollarVolume::of(price(10.25), volume)),
        )
        .unwrap();
        assert_eq!(bar.volume_weighted_average_price(), Some(10.25));
        assert_eq!(bar.trade_count(), Some(TradeCount::new(3)));
        let quiet = Bar::new(
            Symbol::new("AAPL").unwrap(),
            BarInterval::OneMinute,
            instant("2026-07-31T14:31:00Z"),
            prices(),
            Shares::default(),
            Some(TradeCount::new(0)),
            Some(DollarVolume::default()),
        )
        .unwrap();
        assert_eq!(quiet.volume_weighted_average_price(), None);
    }

    #[test]
    fn test_a_crossed_quote_is_refused_and_a_locked_one_is_not() {
        let quote = |bid: f64, ask: f64| {
            Quote::new(
                Symbol::new("AAPL").unwrap(),
                instant("2026-07-31T14:31:00Z"),
                price(bid),
                price(ask),
                Shares::whole(100).unwrap(),
                Shares::whole(200).unwrap(),
            )
        };
        assert_eq!(
            quote(10.01, 10.0),
            Err(QuoteRefusal::Crossed {
                bid: price(10.01),
                ask: price(10.0)
            })
        );
        let locked = quote(10.0, 10.0).unwrap();
        assert_eq!(
            (locked.bid_size(), locked.ask_size()),
            (Shares::whole(100).unwrap(), Shares::whole(200).unwrap())
        );
    }

    #[test]
    fn test_a_trade_of_no_shares_is_refused() {
        let trade = |size: u64| {
            Trade::new(
                Symbol::new("AAPL").unwrap(),
                instant("2026-07-31T14:31:00Z"),
                price(10.0),
                Shares::whole(size).unwrap(),
            )
        };
        assert_eq!(trade(0), Err(TradeRefusal::NoShares { price: price(10.0) }));
        assert_eq!(trade(1).unwrap().size(), Shares::whole(1).unwrap());
    }
}
