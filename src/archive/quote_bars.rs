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
use super::parquet::{self, PlacementRefusal, ReadRefusal, RowCause};
use crate::common::market::quote_bars::{QuoteBar, QuoteSums, Spread, StandingQuote};
use crate::common::market::record::BarInterval;
use crate::common::market::{Shares, Symbol};
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
    Placement(PlacementRefusal),
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
    File(ReadRefusal),
    Row {
        index: usize,
        cause: RowCause,
    },
    Provider {
        provenance: Provenance,
        key: Provider,
    },
}

impl From<ReadRefusal> for DecodeRefusal {
    fn from(refusal: ReadRefusal) -> Self {
        Self::File(refusal)
    }
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
    let ordered = parquet::place(bars, interval, session, |bar| {
        (bar.symbol(), bar.interval(), bar.timestamp())
    })
    .map_err(EncodeRefusal::Placement)?;
    let mut symbols = StringBuilder::new();
    let mut timestamps = TimestampMicrosecondBuilder::new().with_timezone("UTC");
    let mut quote_counts = UInt64Builder::new();
    let mut covered = UInt64Builder::new();
    let mut time_weighted: [Decimal128Builder; 4] =
        std::array::from_fn(|_| Decimal128Builder::new());
    let mut prices: [Decimal128Builder; 4] = std::array::from_fn(|_| Decimal128Builder::new());
    let mut closing_since = TimestampNanosecondBuilder::new().with_timezone("UTC");
    let mut sizes: [Decimal128Builder; 2] = std::array::from_fn(|_| Decimal128Builder::new());
    for bar in ordered {
        let (symbol, timestamp) = (bar.symbol(), bar.timestamp());
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
            let value = parquet::widest_decimal(value).ok_or_else(unrepresentable)?;
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
    parquet::write(schema(), columns, LAYOUT_VERSION, provenance.metadata())
        .map_err(|reason| EncodeRefusal::Parquet { reason })
}

/// The quote bars and provenance a file written by `encode` under `key` holds, each rebuilt through `QuoteBar::new`.
pub fn decode(key: &Key, bytes: Vec<u8>) -> Result<(Vec<QuoteBar>, Provenance), DecodeRefusal> {
    let (provider, interval, session) = quotes_key(key).ok_or(DecodeRefusal::NotAQuotesKey)?;
    let (batches, entries) = parquet::read(bytes, &schema(), LAYOUT_VERSION)?;
    let provenance = provenance_from(&entries).map_err(|name| ReadRefusal::Metadata { name })?;
    if provenance.subscription().provider() != provider {
        return Err(DecodeRefusal::Provider {
            provenance,
            key: provider,
        });
    }
    let mut bars = Vec::new();
    for batch in batches {
        let decimals = |index: usize| parquet::column::<Decimal128Array>(&batch, index);
        let symbols = parquet::column::<StringArray>(&batch, 0)?;
        let timestamps = parquet::column::<TimestampMicrosecondArray>(&batch, 1)?;
        let quote_counts = parquet::column::<UInt64Array>(&batch, 2)?;
        let covered = parquet::column::<UInt64Array>(&batch, 3)?;
        let time_weighted = [decimals(4)?, decimals(5)?, decimals(6)?, decimals(7)?];
        let [narrowest, widest, bid, ask] =
            [decimals(8)?, decimals(9)?, decimals(11)?, decimals(12)?];
        let closing_since = parquet::column::<TimestampNanosecondArray>(&batch, 10)?;
        let [bid_size, ask_size] = [decimals(13)?, decimals(14)?];
        let read = |row: usize| -> Result<QuoteBar, RowCause> {
            let closing = StandingQuote::new(
                DateTime::from_timestamp_nanos(closing_since.value(row)),
                bid.price(row)?,
                ask.price(row)?,
                Shares::from_units(bid_size.integer(row)?),
                Shares::from_units(ask_size.integer(row)?),
            )
            .map_err(RowCause::QuoteSums)?;
            let sums = QuoteSums::new(
                quote_counts.value(row),
                covered.value(row),
                [
                    time_weighted[0].integer(row)?,
                    time_weighted[1].integer(row)?,
                    time_weighted[2].integer(row)?,
                    time_weighted[3].integer(row)?,
                ],
                Spread::from_ticks(narrowest.integer(row)?),
                Spread::from_ticks(widest.integer(row)?),
                closing,
            )
            .map_err(RowCause::QuoteSums)?;
            QuoteBar::new(
                Symbol::new(symbols.value(row)).map_err(RowCause::Symbol)?,
                interval,
                timestamps.instant_in(row, session)?,
                sums,
            )
            .map_err(RowCause::QuoteBar)
        };
        for row in 0..batch.num_rows() {
            let index = bars.len();
            bars.push(read(row).map_err(|cause| DecodeRefusal::Row { index, cause })?);
        }
    }
    Ok((bars, provenance))
}

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use uuid::Uuid;

    use super::*;
    use crate::archive::bars::Subscription;
    use crate::common::journal::{Commit, RunId};
    use crate::common::market::Price;
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
            Err(EncodeRefusal::Placement(
                PlacementRefusal::OutsideKey { .. }
            ))
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

    /// A valid bar of `interval` in session 2026-10-02, its bucket chosen by `slot`.
    fn any_bar(interval: BarInterval) -> impl proptest::strategy::Strategy<Value = QuoteBar> {
        use proptest::prelude::*;
        (
            "[A-Z]{1,4}",
            0_i64..78,
            1_i64..2_000_000,
            0_i64..50_000,
            0_u64..1_000,
            any::<[u64; 4]>(),
            0_i64..86_400_000_000_000,
        )
            .prop_map(move |(symbol, slot, bid, spread, count, sums, since)| {
                let open: DateTime<chrono::Utc> = "2026-10-02T13:30:00Z".parse().unwrap();
                let (timestamp, longest) = match interval {
                    BarInterval::OneMinute => {
                        (open + chrono::TimeDelta::minutes(slot), 60_000_000_000)
                    }
                    BarInterval::FiveMinute => {
                        (open + chrono::TimeDelta::minutes(5 * slot), 300_000_000_000)
                    }
                    BarInterval::OneDay => (session().regular_close(), 23_400_000_000_000),
                };
                let bid = Price::from_ticks(bid).unwrap();
                let ask = Price::from_ticks(bid.ticks() + spread).unwrap();
                let closing = StandingQuote::new(
                    session().midnight() + chrono::TimeDelta::nanoseconds(since),
                    bid,
                    ask,
                    Shares::from_units(sums[2]),
                    Shares::from_units(sums[3]),
                )
                .unwrap();
                let narrowest = Spread::from_ticks(u64::try_from(spread).unwrap() / 2);
                let widest = Spread::from_ticks(u64::try_from(spread).unwrap());
                let quote_sums = QuoteSums::new(
                    count,
                    // A twelfth of the interval, so up to twelve combined into one bucket still fit it.
                    1 + sums[0] % (longest / 12),
                    sums.map(u128::from),
                    narrowest,
                    widest,
                    closing,
                )
                .unwrap();
                QuoteBar::new(
                    Symbol::new(&symbol).unwrap(),
                    interval,
                    timestamp,
                    quote_sums,
                )
                .unwrap()
            })
    }

    proptest::proptest! {
        /// Every interval's bars read back as written, ordered by symbol and timestamp, with their provenance.
        #[test]
        fn property_quote_bars_round_trip(
            interval in proptest::sample::select(vec![BarInterval::OneMinute, BarInterval::FiveMinute, BarInterval::OneDay]),
            bars in proptest::collection::vec(any_bar(BarInterval::OneMinute), 0..12),
        ) {
            let bars: Vec<QuoteBar> = match interval {
                BarInterval::OneMinute => bars,
                BarInterval::FiveMinute | BarInterval::OneDay => bars
                    .iter()
                    .map(|bar| crate::common::market::quote_bars::QuoteRollup::of(bar, interval).unwrap())
                    .fold(<crate::common::market::quote_bars::QuoteRollup as crate::common::monoid::Monoid>::empty(), crate::common::monoid::Monoid::combine)
                    .into_bars(),
            };
            let mut unique: Vec<QuoteBar> = Vec::new();
            for bar in bars {
                if !unique.iter().any(|kept| kept.symbol() == bar.symbol() && kept.timestamp() == bar.timestamp()) {
                    unique.push(bar);
                }
            }
            let key = Key::Quotes { provider: Provider::Massive, origin: Origin::Derived, interval, session: session() };
            let (read, read_provenance) = decode(&key, encode(&key, &unique, &provenance()).unwrap()).unwrap();
            unique.sort_by(|left, right| (left.symbol(), left.timestamp()).cmp(&(right.symbol(), right.timestamp())));
            proptest::prop_assert_eq!(read, unique);
            proptest::prop_assert_eq!(read_provenance, provenance());
        }
    }
}
