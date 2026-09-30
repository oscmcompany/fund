//! Bars as Parquet: our integers stored unchanged as exact decimals, so a reader sees dollars and shares, and the
//! fetch's provenance in the file's own key-value metadata, so the record cannot drift from its rows.

use std::sync::Arc;

use arrow_array::builder::{
    Decimal128Builder, StringBuilder, TimestampMicrosecondBuilder, UInt64Builder,
};
use arrow_array::{
    Array, ArrayRef, Decimal128Array, RecordBatch, StringArray, TimestampMicrosecondArray,
    UInt64Array,
};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use chrono::{DateTime, Utc};
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::basic::Compression;
use parquet::file::metadata::KeyValue;
use parquet::file::properties::WriterProperties;

use crate::common::journal::{Commit, RunId};
use crate::common::market::record::{Bar, BarInterval, Ohlc};
use crate::common::market::{DollarVolume, Price, Shares, Symbol, TradeCount};
use crate::common::storage::{Key, Provider};
use crate::common::time::SessionDate;

/// The file layout this build writes, read back from the metadata before any row.
const LAYOUT_VERSION: &str = "1";

/// Prices are millionths of a dollar, so `Decimal(18, 6)` holds the ten-million-dollar cap exactly.
const PRICE_TYPE: DataType = DataType::Decimal128(18, 6);
/// Share counts are millionths of a share up to `u64::MAX`, twenty digits.
const SHARES_TYPE: DataType = DataType::Decimal128(20, 6);
/// Dollar volume is price × shares, twelve fractional digits; thirty-eight is the widest a decimal can be.
const DOLLAR_VOLUME_TYPE: DataType = DataType::Decimal128(38, 12);

/// The account a fetch was made under, which a provider's key alone does not say.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    strum::Display,
    strum::EnumString,
    strum::IntoStaticStr,
    strum::EnumIter,
)]
#[strum(serialize_all = "snake_case")]
pub enum Subscription {
    AlgoTraderPlus,
    StocksStarter,
}

impl Subscription {
    pub fn provider(self) -> Provider {
        match self {
            Self::AlgoTraderPlus => Provider::Alpaca,
            Self::StocksStarter => Provider::Massive,
        }
    }
}

/// Where the rows came from and which run wrote them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Provenance {
    subscription: Subscription,
    fetched_at: DateTime<Utc>,
    run_id: RunId,
    commit: Option<Commit>,
}

impl Provenance {
    pub fn new(
        subscription: Subscription,
        fetched_at: DateTime<Utc>,
        run_id: RunId,
        commit: Option<Commit>,
    ) -> Self {
        Self {
            subscription,
            fetched_at,
            run_id,
            commit,
        }
    }

    pub fn subscription(&self) -> Subscription {
        self.subscription
    }

    pub fn fetched_at(&self) -> DateTime<Utc> {
        self.fetched_at
    }

    pub fn run_id(&self) -> RunId {
        self.run_id
    }

    pub fn commit(&self) -> Option<&Commit> {
        self.commit.as_ref()
    }
}

/// Why bars were not written under a key.
#[derive(Debug, Clone, PartialEq)]
pub enum EncodeRefusal {
    NotABarsKey,
    /// The subscription belongs to another provider than the key's.
    SubscriptionProvider {
        subscription: Subscription,
        key: Provider,
    },
    /// A bar whose interval or session is not the key's.
    OutsideKey {
        symbol: Symbol,
        timestamp: DateTime<Utc>,
    },
    /// Two bars for one symbol and instant.
    Duplicate {
        symbol: Symbol,
        timestamp: DateTime<Utc>,
    },
    /// A dollar volume past what a thirty-eight-digit decimal holds.
    Unrepresentable {
        symbol: Symbol,
        timestamp: DateTime<Utc>,
    },
    Parquet {
        reason: String,
    },
}

/// Why a file was not read as bars.
#[derive(Debug, Clone, PartialEq)]
pub enum DecodeRefusal {
    NotABarsKey,
    Parquet {
        reason: String,
    },
    /// Metadata absent or unreadable, named by its key.
    Metadata {
        name: &'static str,
    },
    /// Written under another layout than this build reads.
    Layout {
        version: String,
    },
    /// A row that no longer passes the domain's own checks.
    Row {
        index: usize,
        reason: String,
    },
    /// Columns other than this layout's, named as found.
    Schema {
        found: String,
    },
    /// Provenance naming another provider than the key's.
    Provider {
        subscription: Subscription,
        key: Provider,
    },
}

/// The key's interval and session, refused unless it is a bars key.
fn bars_key(key: &Key) -> Option<(Provider, BarInterval, SessionDate)> {
    match key {
        Key::Bars {
            provider,
            interval,
            session,
            ..
        } => Some((*provider, *interval, *session)),
        Key::Quotes { .. }
        | Key::Trades { .. }
        | Key::Reference { .. }
        | Key::Journal { .. }
        | Key::Logs { .. } => None,
    }
}

fn schema() -> Schema {
    let price = |name: &str| Field::new(name, PRICE_TYPE, false);
    Schema::new(vec![
        Field::new("symbol", DataType::Utf8, false),
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            false,
        ),
        price("open"),
        price("high"),
        price("low"),
        price("close"),
        Field::new("volume", SHARES_TYPE, false),
        Field::new("trade_count", DataType::UInt64, true),
        Field::new("dollar_volume", DOLLAR_VOLUME_TYPE, true),
    ])
}

/// The file for `key`, rows ordered by symbol and then timestamp so the same bars always make the same bytes.
pub fn encode(key: &Key, bars: &[Bar], provenance: &Provenance) -> Result<Vec<u8>, EncodeRefusal> {
    let (provider, interval, session) = bars_key(key).ok_or(EncodeRefusal::NotABarsKey)?;
    if provenance.subscription.provider() != provider {
        return Err(EncodeRefusal::SubscriptionProvider {
            subscription: provenance.subscription,
            key: provider,
        });
    }
    let mut ordered: Vec<&Bar> = bars.iter().collect();
    ordered.sort_by(|left, right| {
        (left.symbol(), left.timestamp()).cmp(&(right.symbol(), right.timestamp()))
    });
    let mut symbols = StringBuilder::new();
    let mut timestamps = TimestampMicrosecondBuilder::new().with_timezone("UTC");
    let mut prices: [Decimal128Builder; 4] = std::array::from_fn(|_| Decimal128Builder::new());
    let mut volumes = Decimal128Builder::new();
    let mut trade_counts = UInt64Builder::new();
    let mut dollar_volumes = Decimal128Builder::new();
    let mut previous: Option<(&Symbol, DateTime<Utc>)> = None;
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
        let dollar_volume = match bar.dollar_volume() {
            Some(dollar_volume) => Some(
                i128::try_from(dollar_volume.units())
                    .ok()
                    .filter(|units| *units < 10_i128.pow(38))
                    .ok_or_else(|| EncodeRefusal::Unrepresentable {
                        symbol: symbol.clone(),
                        timestamp,
                    })?,
            ),
            None => None,
        };
        symbols.append_value(symbol.as_str());
        timestamps.append_value(timestamp.timestamp_micros());
        let ohlc = bar.prices();
        for (builder, price) in
            prices
                .iter_mut()
                .zip([ohlc.open(), ohlc.high(), ohlc.low(), ohlc.close()])
        {
            builder.append_value(i128::from(price.ticks()));
        }
        volumes.append_value(i128::from(bar.volume().units()));
        trade_counts.append_option(bar.trade_count().map(TradeCount::count));
        dollar_volumes.append_option(dollar_volume);
    }
    let parquet = |error: &dyn std::fmt::Display| EncodeRefusal::Parquet {
        reason: error.to_string(),
    };
    let [open, high, low, close] =
        prices.map(|mut builder| Arc::new(builder.finish().with_data_type(PRICE_TYPE)) as ArrayRef);
    let schema = Arc::new(schema());
    let columns: Vec<ArrayRef> = vec![
        Arc::new(symbols.finish()),
        Arc::new(timestamps.finish()),
        open,
        high,
        low,
        close,
        Arc::new(volumes.finish().with_data_type(SHARES_TYPE)),
        Arc::new(trade_counts.finish()),
        Arc::new(dollar_volumes.finish().with_data_type(DOLLAR_VOLUME_TYPE)),
    ];
    let batch = RecordBatch::try_new(schema.clone(), columns).map_err(|error| parquet(&error))?;
    let properties = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .set_key_value_metadata(Some(metadata(provenance)))
        .build();
    let mut bytes = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut bytes, schema, Some(properties))
        .map_err(|error| parquet(&error))?;
    writer.write(&batch).map_err(|error| parquet(&error))?;
    writer.close().map_err(|error| parquet(&error))?;
    Ok(bytes)
}

fn metadata(provenance: &Provenance) -> Vec<KeyValue> {
    let entry = |name: &str, value: Option<String>| KeyValue::new(name.to_string(), value);
    vec![
        entry("fund.layout_version", Some(LAYOUT_VERSION.to_string())),
        entry(
            "fund.subscription",
            Some(provenance.subscription.to_string()),
        ),
        entry("fund.fetched_at", Some(provenance.fetched_at.to_rfc3339())),
        entry("fund.run_id", Some(provenance.run_id.to_string())),
        entry(
            "fund.commit",
            provenance
                .commit
                .as_ref()
                .map(|commit| commit.as_str().to_string()),
        ),
    ]
}

/// The bars and provenance a file written by `encode` under `key` holds, each row rebuilt through the domain's own
/// constructors so a file edited out of band cannot hand back an invalid bar.
pub fn decode(key: &Key, bytes: Vec<u8>) -> Result<(Vec<Bar>, Provenance), DecodeRefusal> {
    let (provider, interval, session) = bars_key(key).ok_or(DecodeRefusal::NotABarsKey)?;
    let parquet = |error: &dyn std::fmt::Display| DecodeRefusal::Parquet {
        reason: error.to_string(),
    };
    let builder = ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(bytes))
        .map_err(|error| parquet(&error))?;
    // One comparison covers column count, order, names, decimal scales and nullability, so a non-null column is
    // guaranteed by the reader rather than rechecked per row.
    if builder.schema().fields() != schema().fields() {
        let found = builder
            .schema()
            .fields()
            .iter()
            .map(|field| {
                let optional = if field.is_nullable() { "?" } else { "" };
                format!("{} {}{optional}", field.name(), field.data_type())
            })
            .collect::<Vec<_>>()
            .join(", ");
        return Err(DecodeRefusal::Schema { found });
    }
    let entries: Vec<KeyValue> = builder
        .metadata()
        .file_metadata()
        .key_value_metadata()
        .cloned()
        .unwrap_or_default();
    let value = |name: &'static str| {
        entries
            .iter()
            .find(|entry| entry.key == name)
            .and_then(|entry| entry.value.clone())
    };
    let required = |name: &'static str| value(name).ok_or(DecodeRefusal::Metadata { name });
    let version = required("fund.layout_version")?;
    if version != LAYOUT_VERSION {
        return Err(DecodeRefusal::Layout { version });
    }
    let provenance = Provenance {
        subscription: required("fund.subscription")?.parse().map_err(|_| {
            DecodeRefusal::Metadata {
                name: "fund.subscription",
            }
        })?,
        fetched_at: required("fund.fetched_at")?
            .parse()
            .map_err(|_| DecodeRefusal::Metadata {
                name: "fund.fetched_at",
            })?,
        run_id: RunId::new(required("fund.run_id")?.parse().map_err(|_| {
            DecodeRefusal::Metadata {
                name: "fund.run_id",
            }
        })?),
        commit: value("fund.commit")
            .map(|raw| Commit::new(&raw))
            .transpose()
            .map_err(|_| DecodeRefusal::Metadata {
                name: "fund.commit",
            })?,
    };
    if provenance.subscription.provider() != provider {
        return Err(DecodeRefusal::Provider {
            subscription: provenance.subscription,
            key: provider,
        });
    }
    let mut bars = Vec::new();
    for batch in builder.build().map_err(|error| parquet(&error))? {
        let batch = batch.map_err(|error| parquet(&error))?;
        let symbols = downcast::<StringArray>(batch.column(0))?;
        let timestamps = downcast::<TimestampMicrosecondArray>(batch.column(1))?;
        let prices: Vec<&Decimal128Array> = (2..6)
            .map(|index| downcast::<Decimal128Array>(batch.column(index)))
            .collect::<Result<_, _>>()?;
        let volumes = downcast::<Decimal128Array>(batch.column(6))?;
        let trade_counts = downcast::<UInt64Array>(batch.column(7))?;
        let dollar_volumes = downcast::<Decimal128Array>(batch.column(8))?;
        for row in 0..batch.num_rows() {
            let index = bars.len();
            let refused = |reason: String| DecodeRefusal::Row { index, reason };
            let price = |array: &Decimal128Array| {
                i64::try_from(array.value(row))
                    .map_err(|error| error.to_string())
                    .and_then(|ticks| {
                        Price::from_ticks(ticks).map_err(|error| format!("{error:?}"))
                    })
                    .map_err(refused)
            };
            let symbol =
                Symbol::new(symbols.value(row)).map_err(|error| refused(format!("{error:?}")))?;
            let timestamp = DateTime::from_timestamp_micros(timestamps.value(row))
                .ok_or_else(|| refused("timestamp out of range".to_string()))?;
            if SessionDate::at(timestamp) != session {
                return Err(refused(format!("{timestamp} is outside session {session}")));
            }
            let prices = Ohlc::new(
                price(prices[0])?,
                price(prices[1])?,
                price(prices[2])?,
                price(prices[3])?,
            )
            .map_err(|error| refused(format!("{error:?}")))?;
            let volume = u64::try_from(volumes.value(row))
                .map(Shares::from_units)
                .map_err(|error| refused(error.to_string()))?;
            let trade_count = trade_counts
                .is_valid(row)
                .then(|| TradeCount::new(trade_counts.value(row)));
            let dollar_volume = match dollar_volumes.is_valid(row) {
                true => Some(
                    u128::try_from(dollar_volumes.value(row))
                        .map(DollarVolume::from_units)
                        .map_err(|error| refused(error.to_string()))?,
                ),
                false => None,
            };
            let bar = Bar::new(
                symbol,
                interval,
                timestamp,
                prices,
                volume,
                trade_count,
                dollar_volume,
            )
            .map_err(|error| refused(format!("{error:?}")))?;
            bars.push(bar);
        }
    }
    Ok((bars, provenance))
}

fn downcast<T: 'static>(column: &ArrayRef) -> Result<&T, DecodeRefusal> {
    column
        .as_any()
        .downcast_ref::<T>()
        .ok_or(DecodeRefusal::Parquet {
            reason: format!("column is {}", column.data_type()),
        })
}

#[cfg(test)]
mod tests {
    use chrono::{NaiveDate, TimeDelta};
    use proptest::prelude::*;
    use uuid::Uuid;

    use super::*;
    use crate::common::storage::Origin;

    fn session() -> SessionDate {
        SessionDate::from_date(NaiveDate::from_ymd_opt(2026, 9, 25).unwrap())
    }

    fn minute_key() -> Key {
        Key::Bars {
            provider: Provider::Alpaca,
            origin: Origin::Fetched,
            interval: BarInterval::OneMinute,
            session: session(),
        }
    }

    fn provenance(subscription: Subscription) -> Provenance {
        Provenance::new(
            subscription,
            "2026-09-26T07:00:00Z".parse().unwrap(),
            RunId::new(Uuid::from_u128(7)),
            Some(Commit::new("0123456789abcdef0123456789abcdef01234567").unwrap()),
        )
    }

    fn bar(symbol: &str, timestamp: &str, close: f64, dollar_volume: Option<DollarVolume>) -> Bar {
        let price = |dollars: f64| Price::from_dollars(dollars).unwrap();
        Bar::new(
            Symbol::new(symbol).unwrap(),
            BarInterval::OneMinute,
            timestamp.parse().unwrap(),
            Ohlc::new(price(10.0), price(12.0), price(9.5), price(close)).unwrap(),
            Shares::from_float(213_849.305_802).unwrap(),
            Some(TradeCount::new(3)),
            dollar_volume,
        )
        .unwrap()
    }

    #[test]
    fn test_bars_read_back_sorted_with_their_provenance() {
        let bars = [
            bar("MSFT", "2026-09-25T14:31:00Z", 10.182_05, None),
            bar(
                "AAPL",
                "2026-09-25T14:31:00Z",
                11.843_871,
                Some(DollarVolume::from_units(u128::from(u64::MAX) * 1_000)),
            ),
            bar("AAPL", "2026-09-25T14:30:00Z", 10.0, None),
        ];
        let written = provenance(Subscription::AlgoTraderPlus);
        let bytes = encode(&minute_key(), &bars, &written).unwrap();
        let (read, provenance) = decode(&minute_key(), bytes.clone()).unwrap();
        assert_eq!(provenance, written);
        assert_eq!(read, [bars[2].clone(), bars[1].clone(), bars[0].clone()]);
        // The same bars in any order make the same bytes, so a rerun overwrites with an identical object.
        let reordered = [bars[1].clone(), bars[0].clone(), bars[2].clone()];
        assert_eq!(encode(&minute_key(), &reordered, &written).unwrap(), bytes);
    }

    /// What DuckDB sees: the stored integers read as dollars and shares at their declared scale.
    #[test]
    fn test_columns_read_as_dollars_and_shares() {
        let bars = [bar(
            "AAPL",
            "2026-09-25T14:31:00Z",
            11.843_871,
            Some(DollarVolume::of(
                Price::from_dollars(1.5).unwrap(),
                Shares::whole(3).unwrap(),
            )),
        )];
        let bytes = encode(
            &minute_key(),
            &bars,
            &provenance(Subscription::AlgoTraderPlus),
        )
        .unwrap();
        let batch = ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(bytes))
            .unwrap()
            .build()
            .unwrap()
            .next()
            .unwrap()
            .unwrap();
        let decimal = |index: usize| {
            downcast::<Decimal128Array>(batch.column(index))
                .unwrap()
                .value_as_string(0)
        };
        assert_eq!(decimal(5), "11.843871");
        assert_eq!(decimal(6), "213849.305802");
        assert_eq!(decimal(8), "4.500000000000");
    }

    #[test]
    fn test_a_bar_outside_the_key_or_repeated_is_refused() {
        let written = provenance(Subscription::AlgoTraderPlus);
        let next_day = bar("AAPL", "2026-09-26T14:30:00Z", 10.0, None);
        assert_eq!(
            encode(&minute_key(), &[next_day], &written),
            Err(EncodeRefusal::OutsideKey {
                symbol: Symbol::new("AAPL").unwrap(),
                timestamp: "2026-09-26T14:30:00Z".parse().unwrap()
            })
        );
        let twice = bar("AAPL", "2026-09-25T14:30:00Z", 10.0, None);
        assert_eq!(
            encode(&minute_key(), &[twice.clone(), twice], &written),
            Err(EncodeRefusal::Duplicate {
                symbol: Symbol::new("AAPL").unwrap(),
                timestamp: "2026-09-25T14:30:00Z".parse().unwrap()
            })
        );
    }

    #[test]
    fn test_a_subscription_must_belong_to_the_keys_provider() {
        assert_eq!(
            encode(&minute_key(), &[], &provenance(Subscription::StocksStarter)),
            Err(EncodeRefusal::SubscriptionProvider {
                subscription: Subscription::StocksStarter,
                key: Provider::Alpaca
            })
        );
        let journal = Key::Journal {
            host: crate::common::storage::Host::Archiver,
            session: session(),
        };
        assert_eq!(
            encode(&journal, &[], &provenance(Subscription::AlgoTraderPlus)),
            Err(EncodeRefusal::NotABarsKey)
        );
    }

    fn replace(bytes: &[u8], from: &[u8], to: &[u8]) -> Vec<u8> {
        let at = bytes
            .windows(from.len())
            .position(|window| window == from)
            .unwrap();
        [&bytes[..at], to, &bytes[at + from.len()..]].concat()
    }

    #[test]
    fn test_a_file_without_its_layout_or_from_another_is_refused() {
        let written = provenance(Subscription::AlgoTraderPlus);
        let bytes = encode(&minute_key(), &[], &written).unwrap();
        let unnamed = replace(&bytes, b"fund.layout_version", b"fund.layout_versioX");
        assert_eq!(
            decode(&minute_key(), unnamed).map(|_| ()),
            Err(DecodeRefusal::Metadata {
                name: "fund.layout_version"
            })
        );
        let mut entries = metadata(&written);
        entries[0] = KeyValue::new("fund.layout_version".to_string(), Some("2".to_string()));
        let properties = WriterProperties::builder()
            .set_key_value_metadata(Some(entries))
            .build();
        let mut later = Vec::new();
        let writer =
            ArrowWriter::try_new(&mut later, Arc::new(schema()), Some(properties)).unwrap();
        writer.close().unwrap();
        assert_eq!(
            decode(&minute_key(), later).map(|_| ()),
            Err(DecodeRefusal::Layout {
                version: "2".to_string()
            })
        );
    }

    /// A file carrying valid provenance under `schema`, with no rows.
    fn file_with(schema: Schema) -> Vec<u8> {
        let properties = WriterProperties::builder()
            .set_key_value_metadata(Some(metadata(&provenance(Subscription::AlgoTraderPlus))))
            .build();
        let mut bytes = Vec::new();
        ArrowWriter::try_new(&mut bytes, Arc::new(schema), Some(properties))
            .unwrap()
            .close()
            .unwrap();
        bytes
    }

    #[test]
    fn test_a_file_whose_columns_differ_from_the_layout_is_refused() {
        let fields = |edit: &dyn Fn(&mut Vec<Field>)| {
            let mut fields: Vec<Field> = schema()
                .fields()
                .iter()
                .map(|field| field.as_ref().clone())
                .collect();
            edit(&mut fields);
            Schema::new(fields)
        };
        let dropped = fields(&|fields| {
            fields.pop();
        });
        let rescaled = fields(&|fields| {
            fields[2] = Field::new("open", DataType::Decimal128(18, 4), false);
        });
        let nullable = fields(&|fields| {
            fields[6] = Field::new("volume", SHARES_TYPE, true);
        });
        let swapped = fields(&|fields| fields.swap(2, 5));
        for (name, schema) in [
            ("dropped", dropped),
            ("rescaled", rescaled),
            ("nullable", nullable),
            ("swapped", swapped),
        ] {
            assert!(
                matches!(
                    decode(&minute_key(), file_with(schema)),
                    Err(DecodeRefusal::Schema { .. })
                ),
                "{name}"
            );
        }
        assert!(decode(&minute_key(), file_with(schema())).is_ok());
    }

    #[test]
    fn test_a_file_is_refused_under_another_session_or_provider() {
        let bars = [bar("AAPL", "2026-09-25T14:30:00Z", 10.0, None)];
        let bytes = encode(
            &minute_key(),
            &bars,
            &provenance(Subscription::AlgoTraderPlus),
        )
        .unwrap();
        let next_day = Key::Bars {
            provider: Provider::Alpaca,
            origin: Origin::Fetched,
            interval: BarInterval::OneMinute,
            session: SessionDate::from_date(NaiveDate::from_ymd_opt(2026, 9, 26).unwrap()),
        };
        assert!(matches!(
            decode(&next_day, bytes.clone()),
            Err(DecodeRefusal::Row { index: 0, .. })
        ));
        let massive = Key::Bars {
            provider: Provider::Massive,
            origin: Origin::Fetched,
            interval: BarInterval::OneMinute,
            session: session(),
        };
        assert_eq!(
            decode(&massive, bytes).map(|_| ()),
            Err(DecodeRefusal::Provider {
                subscription: Subscription::AlgoTraderPlus,
                key: Provider::Massive
            })
        );
    }

    fn any_bar() -> impl Strategy<Value = Bar> {
        (
            prop::sample::select(vec!["AAPL", "BRK.B", "BC.PRC"]),
            0_i64..1440,
            prop::array::uniform4(1_i64..10_000_000_000_000),
            0_u64..u64::MAX,
            prop::option::of(any::<u64>()),
            prop::option::of(0_u128..10_u128.pow(37)),
        )
            .prop_map(
                |(symbol, minute, mut ticks, volume, trades, dollar_units)| {
                    ticks.sort();
                    let [low, first, second, high] =
                        ticks.map(|tick| Price::from_ticks(tick).unwrap());
                    Bar::new(
                        Symbol::new(symbol).unwrap(),
                        BarInterval::OneMinute,
                        session().bounds().0 + TimeDelta::minutes(minute),
                        Ohlc::new(first, high, low, second).unwrap(),
                        Shares::from_units(volume),
                        trades.map(TradeCount::new),
                        dollar_units.map(DollarVolume::from_units),
                    )
                    .unwrap()
                },
            )
    }

    proptest! {
        #[test]
        fn property_bars_survive_the_file(bars in prop::collection::vec(any_bar(), 0..40)) {
            let mut unique: Vec<Bar> = Vec::new();
            for bar in bars {
                if !unique.iter().any(|kept| (kept.symbol(), kept.timestamp()) == (bar.symbol(), bar.timestamp())) {
                    unique.push(bar);
                }
            }
            unique.sort_by(|left, right| (left.symbol(), left.timestamp()).cmp(&(right.symbol(), right.timestamp())));
            let written = provenance(Subscription::AlgoTraderPlus);
            let bytes = encode(&minute_key(), &unique, &written).unwrap();
            prop_assert_eq!(decode(&minute_key(), bytes).unwrap(), (unique, written));
        }
    }
}
