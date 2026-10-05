//! Quote bars as Parquet: every time-weighted sum stored as an exact decimal at our six-digit scale, so a reader sees
//! dollars, fractions of the midpoint and shares, each multiplied by nanoseconds, and the run's provenance alongside.

use std::sync::Arc;

use arrow_array::builder::{
    Decimal128Builder, StringBuilder, TimestampMicrosecondBuilder, TimestampNanosecondBuilder,
    UInt64Builder,
};
use arrow_array::{
    ArrayRef, Decimal128Array, StringArray, TimestampMicrosecondArray, TimestampNanosecondArray,
    UInt64Array,
};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use chrono::DateTime;

use super::bars::{Provenance, provenance_from};
use super::parquet;
use crate::common::market::quote_bars::{QuoteBar, QuoteSums, Spread, StandingQuote};
use crate::common::market::record::BarInterval;
use crate::common::market::{Price, Shares, Symbol};
use crate::common::storage::{Key, Provider};
use crate::common::time::SessionDate;

/// The file layout this build writes, read back from the metadata before any row.
const LAYOUT_VERSION: &str = "1";

const PRICE_TYPE: DataType = DataType::Decimal128(18, 6);
const SHARES_TYPE: DataType = DataType::Decimal128(20, 6);
/// A sum of six-digit quantities multiplied by nanoseconds; thirty-eight digits hold a day of any of them.
const TIME_WEIGHTED_TYPE: DataType = DataType::Decimal128(38, 6);

/// Why quote bars were not written under a key.
#[derive(Debug, Clone, PartialEq)]
pub enum EncodeRefusal {
    NotAQuotesKey,
    SubscriptionProvider {
        provenance: Provenance,
        key: Provider,
    },
    /// A bar whose interval or session is not the key's.
    OutsideKey {
        symbol: Symbol,
        timestamp: DateTime<chrono::Utc>,
    },
    Duplicate {
        symbol: Symbol,
        timestamp: DateTime<chrono::Utc>,
    },
    /// A sum past what a thirty-eight-digit decimal holds.
    Unrepresentable {
        symbol: Symbol,
        timestamp: DateTime<chrono::Utc>,
    },
    Parquet {
        reason: String,
    },
}

/// Why a file was not read as quote bars.
#[derive(Debug, Clone, PartialEq)]
pub enum DecodeRefusal {
    NotAQuotesKey,
    Parquet {
        reason: String,
    },
    Metadata {
        name: &'static str,
    },
    Layout {
        version: String,
    },
    Row {
        index: usize,
        reason: String,
    },
    Schema {
        found: String,
    },
    Provider {
        provenance: Provenance,
        key: Provider,
    },
}

fn quotes_key(key: &Key) -> Option<(Provider, BarInterval, SessionDate)> {
    match key {
        Key::Quotes {
            provider,
            interval,
            session,
            ..
        } => Some((*provider, *interval, *session)),
        Key::Bars { .. }
        | Key::Trades { .. }
        | Key::Reference { .. }
        | Key::RawBars { .. }
        | Key::RawQuotes { .. }
        | Key::RawTrades { .. }
        | Key::Journal { .. }
        | Key::Logs { .. } => None,
    }
}

fn schema() -> Schema {
    let price = |name: &str| Field::new(name, PRICE_TYPE, false);
    let shares = |name: &str| Field::new(name, SHARES_TYPE, false);
    let time_weighted = |name: &str| Field::new(name, TIME_WEIGHTED_TYPE, false);
    Schema::new(vec![
        Field::new("symbol", DataType::Utf8, false),
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            false,
        ),
        Field::new("quote_count", DataType::UInt64, false),
        Field::new("covered_nanoseconds", DataType::UInt64, false),
        time_weighted("spread_time"),
        time_weighted("relative_spread_time"),
        time_weighted("bid_size_time"),
        time_weighted("ask_size_time"),
        price("narrowest_spread"),
        price("widest_spread"),
        Field::new(
            "closing_since",
            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
            false,
        ),
        price("closing_bid"),
        price("closing_ask"),
        shares("closing_bid_size"),
        shares("closing_ask_size"),
    ])
}

/// The file for `key`, rows ordered by symbol and then timestamp so the same bars always make the same bytes.
pub fn encode(
    key: &Key,
    bars: &[QuoteBar],
    provenance: &Provenance,
) -> Result<Vec<u8>, EncodeRefusal> {
    let (provider, interval, session) = quotes_key(key).ok_or(EncodeRefusal::NotAQuotesKey)?;
    if provenance.subscription().provider() != provider {
        return Err(EncodeRefusal::SubscriptionProvider {
            provenance: provenance.clone(),
            key: provider,
        });
    }
    let mut ordered: Vec<&QuoteBar> = bars.iter().collect();
    ordered.sort_by(|left, right| {
        (left.symbol(), left.timestamp()).cmp(&(right.symbol(), right.timestamp()))
    });
    let mut symbols = StringBuilder::new();
    let mut timestamps = TimestampMicrosecondBuilder::new().with_timezone("UTC");
    let mut quote_counts = UInt64Builder::new();
    let mut covered = UInt64Builder::new();
    let mut time_weighted: [Decimal128Builder; 4] =
        std::array::from_fn(|_| Decimal128Builder::new());
    let mut prices: [Decimal128Builder; 4] = std::array::from_fn(|_| Decimal128Builder::new());
    let mut closing_since = TimestampNanosecondBuilder::new().with_timezone("UTC");
    let mut sizes: [Decimal128Builder; 2] = std::array::from_fn(|_| Decimal128Builder::new());
    let mut previous: Option<(&Symbol, DateTime<chrono::Utc>)> = None;
    for bar in ordered {
        let (symbol, timestamp) = (bar.symbol(), bar.timestamp());
        if bar.interval() != interval || SessionDate::at(timestamp) != session {
            return Err(EncodeRefusal::OutsideKey {
                symbol: symbol.clone(),
                timestamp,
            });
        }
        if previous == Some((symbol, timestamp)) {
            return Err(EncodeRefusal::Duplicate {
                symbol: symbol.clone(),
                timestamp,
            });
        }
        previous = Some((symbol, timestamp));
        let unrepresentable = || EncodeRefusal::Unrepresentable {
            symbol: symbol.clone(),
            timestamp,
        };
        let sums = bar.sums();
        for (builder, value) in time_weighted.iter_mut().zip([
            sums.spread_time(),
            sums.relative_spread_time(),
            sums.bid_size_time(),
            sums.ask_size_time(),
        ]) {
            let value = i128::try_from(value)
                .ok()
                .filter(|value| *value < 10_i128.pow(38))
                .ok_or_else(unrepresentable)?;
            builder.append_value(value);
        }
        let closing = sums.closing();
        let since = closing
            .since()
            .timestamp_nanos_opt()
            .ok_or_else(unrepresentable)?;
        symbols.append_value(symbol.as_str());
        timestamps.append_value(timestamp.timestamp_micros());
        quote_counts.append_value(sums.quote_count());
        covered.append_value(sums.covered_nanoseconds());
        for (builder, ticks) in prices.iter_mut().zip([
            i128::from(sums.narrowest().ticks()),
            i128::from(sums.widest().ticks()),
            i128::from(closing.bid().ticks()),
            i128::from(closing.ask().ticks()),
        ]) {
            builder.append_value(ticks);
        }
        closing_since.append_value(since);
        for (builder, units) in sizes
            .iter_mut()
            .zip([closing.bid_size().units(), closing.ask_size().units()])
        {
            builder.append_value(i128::from(units));
        }
    }
    let [spread, relative, bid_time, ask_time] = time_weighted.map(|mut builder| {
        Arc::new(builder.finish().with_data_type(TIME_WEIGHTED_TYPE)) as ArrayRef
    });
    let [narrowest, widest, bid, ask] =
        prices.map(|mut builder| Arc::new(builder.finish().with_data_type(PRICE_TYPE)) as ArrayRef);
    let [bid_size, ask_size] =
        sizes.map(|mut builder| Arc::new(builder.finish().with_data_type(SHARES_TYPE)) as ArrayRef);
    let columns: Vec<ArrayRef> = vec![
        Arc::new(symbols.finish()),
        Arc::new(timestamps.finish()),
        Arc::new(quote_counts.finish()),
        Arc::new(covered.finish()),
        spread,
        relative,
        bid_time,
        ask_time,
        narrowest,
        widest,
        Arc::new(closing_since.finish()),
        bid,
        ask,
        bid_size,
        ask_size,
    ];
    let metadata = provenance
        .entries()
        .into_iter()
        .map(|(name, value)| ::parquet::file::metadata::KeyValue::new(name.to_string(), value))
        .collect();
    parquet::write(schema(), columns, LAYOUT_VERSION, metadata)
        .map_err(|reason| EncodeRefusal::Parquet { reason })
}

/// The quote bars and provenance a file written by `encode` under `key` holds, each rebuilt through `QuoteBar::new`.
pub fn decode(key: &Key, bytes: Vec<u8>) -> Result<(Vec<QuoteBar>, Provenance), DecodeRefusal> {
    let (provider, interval, session) = quotes_key(key).ok_or(DecodeRefusal::NotAQuotesKey)?;
    let (batches, entries) = parquet::read(bytes, &schema(), LAYOUT_VERSION)?;
    let provenance = provenance_from(&entries).map_err(|name| DecodeRefusal::Metadata { name })?;
    if provenance.subscription().provider() != provider {
        return Err(DecodeRefusal::Provider {
            provenance,
            key: provider,
        });
    }
    let mut bars = Vec::new();
    for batch in batches {
        let decimals = |index: usize| {
            parquet::downcast::<Decimal128Array>(batch.column(index)).map_err(DecodeRefusal::from)
        };
        let symbols =
            parquet::downcast::<StringArray>(batch.column(0)).map_err(DecodeRefusal::from)?;
        let timestamps = parquet::downcast::<TimestampMicrosecondArray>(batch.column(1))
            .map_err(DecodeRefusal::from)?;
        let quote_counts =
            parquet::downcast::<UInt64Array>(batch.column(2)).map_err(DecodeRefusal::from)?;
        let covered =
            parquet::downcast::<UInt64Array>(batch.column(3)).map_err(DecodeRefusal::from)?;
        let time_weighted = [decimals(4)?, decimals(5)?, decimals(6)?, decimals(7)?];
        let prices = [decimals(8)?, decimals(9)?, decimals(11)?, decimals(12)?];
        let closing_since = parquet::downcast::<TimestampNanosecondArray>(batch.column(10))
            .map_err(DecodeRefusal::from)?;
        let sizes = [decimals(13)?, decimals(14)?];
        for row in 0..batch.num_rows() {
            let index = bars.len();
            let refused = |reason: String| DecodeRefusal::Row { index, reason };
            let unsigned = |array: &Decimal128Array| {
                u128::try_from(array.value(row)).map_err(|error| refused(error.to_string()))
            };
            let ticks = |array: &Decimal128Array| {
                i64::try_from(array.value(row)).map_err(|error| refused(error.to_string()))
            };
            let price = |array: &Decimal128Array| {
                ticks(array).and_then(|ticks| {
                    Price::from_ticks(ticks).map_err(|error| refused(format!("{error:?}")))
                })
            };
            let spread = |array: &Decimal128Array| {
                ticks(array).and_then(|ticks| {
                    u64::try_from(ticks)
                        .map(Spread::from_ticks)
                        .map_err(|error| refused(error.to_string()))
                })
            };
            let size = |array: &Decimal128Array| {
                u64::try_from(array.value(row))
                    .map(Shares::from_units)
                    .map_err(|error| refused(error.to_string()))
            };
            let symbol =
                Symbol::new(symbols.value(row)).map_err(|error| refused(format!("{error:?}")))?;
            let timestamp = DateTime::from_timestamp_micros(timestamps.value(row))
                .ok_or_else(|| refused("timestamp out of range".to_string()))?;
            if SessionDate::at(timestamp) != session {
                return Err(refused(format!("{timestamp} is outside session {session}")));
            }
            let closing = StandingQuote::new(
                DateTime::from_timestamp_nanos(closing_since.value(row)),
                price(prices[2])?,
                price(prices[3])?,
                size(sizes[0])?,
                size(sizes[1])?,
            );
            let sums = QuoteSums::new(
                quote_counts.value(row),
                covered.value(row),
                [
                    unsigned(time_weighted[0])?,
                    unsigned(time_weighted[1])?,
                    unsigned(time_weighted[2])?,
                    unsigned(time_weighted[3])?,
                ],
                spread(prices[0])?,
                spread(prices[1])?,
                closing,
            );
            let bar = QuoteBar::new(symbol, interval, timestamp, sums)
                .map_err(|error| refused(format!("{error:?}")))?;
            bars.push(bar);
        }
    }
    Ok((bars, provenance))
}

impl From<parquet::ReadRefusal> for DecodeRefusal {
    fn from(refusal: parquet::ReadRefusal) -> Self {
        match refusal {
            parquet::ReadRefusal::Parquet { reason } => Self::Parquet { reason },
            parquet::ReadRefusal::Schema { found } => Self::Schema { found },
            parquet::ReadRefusal::Metadata { name } => Self::Metadata { name },
            parquet::ReadRefusal::Layout { version } => Self::Layout { version },
        }
    }
}

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use uuid::Uuid;

    use super::*;
    use crate::archive::bars::Subscription;
    use crate::common::journal::{Commit, RunId};
    use crate::common::market::quote_bars::QuoteFold;
    use crate::common::market::record::Quote;
    use crate::common::storage::Origin;

    fn session() -> SessionDate {
        SessionDate::from_date(chrono::NaiveDate::from_ymd_opt(2026, 10, 2).unwrap())
    }

    fn key() -> Key {
        Key::Quotes {
            provider: Provider::Massive,
            origin: Origin::Derived,
            interval: BarInterval::OneMinute,
            session: session(),
        }
    }

    fn provenance() -> Provenance {
        Provenance::new(
            Subscription::StocksAdvanced,
            "2026-10-03T07:00:00Z".parse::<DateTime<Utc>>().unwrap(),
            RunId::new(Uuid::from_u128(9)),
            Some(Commit::new("0123456789abcdef0123456789abcdef01234567").unwrap()),
        )
    }

    fn bars() -> Vec<QuoteBar> {
        let mut fold = QuoteFold::new(
            "2026-10-02T13:30:00Z".parse().unwrap(),
            "2026-10-02T20:00:00Z".parse().unwrap(),
        )
        .unwrap();
        for (symbol, at, bid, ask) in [
            ("MSFT", "2026-10-02T13:30:00.000000001Z", 400.01, 400.05),
            ("AAPL", "2026-10-02T13:30:30Z", 100.00, 100.02),
            ("AAPL", "2026-10-02T13:31:10Z", 100.01, 100.01),
        ] {
            fold.push(
                &Quote::new(
                    Symbol::new(symbol).unwrap(),
                    at.parse().unwrap(),
                    Price::from_dollars(bid).unwrap(),
                    Price::from_dollars(ask).unwrap(),
                    Shares::from_float(250.5).unwrap(),
                    Shares::whole(300).unwrap(),
                )
                .unwrap(),
            );
        }
        fold.finish().0
    }

    #[test]
    fn test_quote_bars_read_back_exactly_with_their_provenance() {
        let written = bars();
        let (read, provenance_read) =
            decode(&key(), encode(&key(), &written, &provenance()).unwrap()).unwrap();
        assert_eq!(read.len(), 780);
        assert_eq!(read, written);
        assert_eq!(provenance_read, provenance());
    }

    #[test]
    fn test_a_bar_from_another_session_or_key_kind_is_refused() {
        let other = Key::Quotes {
            provider: Provider::Massive,
            origin: Origin::Derived,
            interval: BarInterval::OneMinute,
            session: session().plus_calendar_days(1),
        };
        assert!(matches!(
            encode(&other, &bars(), &provenance()),
            Err(EncodeRefusal::OutsideKey { .. })
        ));
        let bars_key = Key::Bars {
            provider: Provider::Massive,
            origin: Origin::Vendor,
            interval: BarInterval::OneMinute,
            session: session(),
        };
        assert_eq!(
            encode(&bars_key, &bars(), &provenance()),
            Err(EncodeRefusal::NotAQuotesKey)
        );
    }
}
