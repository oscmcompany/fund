//! Trade bars as Parquet: exact totals as decimals, and each price pair with the instants that set it, null exactly
//! when no print in the bar was eligible to set it.

use std::sync::Arc;

use arrow_array::builder::{
    Decimal128Builder, StringBuilder, TimestampMicrosecondBuilder, TimestampNanosecondBuilder,
    UInt64Builder,
};
use arrow_array::{
    Array, ArrayRef, Decimal128Array, StringArray, TimestampMicrosecondArray,
    TimestampNanosecondArray, UInt64Array,
};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use chrono::{DateTime, Utc};

use super::bars::{Provenance, provenance_from};
use super::parquet;
use crate::common::market::aggregate::TradeTotals;
use crate::common::market::record::BarInterval;
use crate::common::market::trade_bars::{HighLow, OpenClose, TradeBar, TradeSums};
use crate::common::market::{DollarVolume, Price, Shares, Symbol, TradeCount};
use crate::common::storage::{Key, Provider};
use crate::common::time::SessionDate;

/// The file layout this build writes, read back from the metadata before any row.
const LAYOUT_VERSION: &str = "1";

const PRICE_TYPE: DataType = DataType::Decimal128(18, 6);
const SHARES_TYPE: DataType = DataType::Decimal128(20, 6);
const DOLLAR_VOLUME_TYPE: DataType = DataType::Decimal128(38, 12);

/// Why trade bars were not written under a key.
#[derive(Debug, Clone, PartialEq)]
pub enum EncodeRefusal {
    NotATradesKey,
    SubscriptionProvider {
        provenance: Provenance,
        key: Provider,
    },
    OutsideKey {
        symbol: Symbol,
        timestamp: DateTime<Utc>,
    },
    Duplicate {
        symbol: Symbol,
        timestamp: DateTime<Utc>,
    },
    Unrepresentable {
        symbol: Symbol,
        timestamp: DateTime<Utc>,
    },
    Parquet {
        reason: String,
    },
}

/// Why a file was not read as trade bars.
#[derive(Debug, Clone, PartialEq)]
pub enum DecodeRefusal {
    NotATradesKey,
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

fn trades_key(key: &Key) -> Option<(Provider, BarInterval, SessionDate)> {
    match key {
        Key::Trades {
            provider,
            interval,
            session,
            ..
        } => Some((*provider, *interval, *session)),
        Key::Bars { .. }
        | Key::Quotes { .. }
        | Key::Reference { .. }
        | Key::RawBars { .. }
        | Key::RawQuotes { .. }
        | Key::RawTrades { .. }
        | Key::Journal { .. }
        | Key::Logs { .. } => None,
    }
}

fn schema() -> Schema {
    let instant = |name: &str| {
        Field::new(
            name,
            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
            true,
        )
    };
    let price = |name: &str| Field::new(name, PRICE_TYPE, true);
    Schema::new(vec![
        Field::new("symbol", DataType::Utf8, false),
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            false,
        ),
        Field::new("trade_count", DataType::UInt64, false),
        Field::new("volume", SHARES_TYPE, false),
        Field::new("dollar_volume", DOLLAR_VOLUME_TYPE, false),
        instant("opened_at"),
        price("open"),
        instant("closed_at"),
        price("close"),
        price("high"),
        price("low"),
    ])
}

/// The file for `key`, rows ordered by symbol and then timestamp so the same bars always make the same bytes.
pub fn encode(
    key: &Key,
    bars: &[TradeBar],
    provenance: &Provenance,
) -> Result<Vec<u8>, EncodeRefusal> {
    let (provider, interval, session) = trades_key(key).ok_or(EncodeRefusal::NotATradesKey)?;
    if provenance.subscription().provider() != provider {
        return Err(EncodeRefusal::SubscriptionProvider {
            provenance: provenance.clone(),
            key: provider,
        });
    }
    let mut ordered: Vec<&TradeBar> = bars.iter().collect();
    ordered.sort_by(|left, right| {
        (left.symbol(), left.timestamp()).cmp(&(right.symbol(), right.timestamp()))
    });
    let mut symbols = StringBuilder::new();
    let mut timestamps = TimestampMicrosecondBuilder::new().with_timezone("UTC");
    let mut counts = UInt64Builder::new();
    let mut volumes = Decimal128Builder::new();
    let mut dollar_volumes = Decimal128Builder::new();
    let mut opened_at = TimestampNanosecondBuilder::new().with_timezone("UTC");
    let mut closed_at = TimestampNanosecondBuilder::new().with_timezone("UTC");
    let mut prices: [Decimal128Builder; 4] = std::array::from_fn(|_| Decimal128Builder::new());
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
        let unrepresentable = || EncodeRefusal::Unrepresentable {
            symbol: symbol.clone(),
            timestamp,
        };
        let sums = bar.sums();
        let totals = sums.totals();
        let dollar_volume = i128::try_from(totals.dollar_volume().units())
            .ok()
            .filter(|units| *units < 10_i128.pow(38))
            .ok_or_else(unrepresentable)?;
        let nanoseconds = |instant: DateTime<Utc>| instant.timestamp_nanos_opt();
        let open_close = sums.open_close();
        let (open_instant, close_instant) = match open_close {
            Some(pair) => (
                Some(nanoseconds(pair.open().0).ok_or_else(unrepresentable)?),
                Some(nanoseconds(pair.close().0).ok_or_else(unrepresentable)?),
            ),
            None => (None, None),
        };
        symbols.append_value(symbol.as_str());
        timestamps.append_value(timestamp.timestamp_micros());
        counts.append_value(totals.count().count());
        volumes.append_value(i128::from(totals.volume().units()));
        dollar_volumes.append_value(dollar_volume);
        opened_at.append_option(open_instant);
        closed_at.append_option(close_instant);
        let high_low = sums.high_low();
        for (builder, price) in prices.iter_mut().zip([
            open_close.map(|pair| pair.open().1),
            open_close.map(|pair| pair.close().1),
            high_low.map(|pair| pair.high()),
            high_low.map(|pair| pair.low()),
        ]) {
            builder.append_option(price.map(|price| i128::from(price.ticks())));
        }
    }
    let [open, close, high, low] =
        prices.map(|mut builder| Arc::new(builder.finish().with_data_type(PRICE_TYPE)) as ArrayRef);
    let columns: Vec<ArrayRef> = vec![
        Arc::new(symbols.finish()),
        Arc::new(timestamps.finish()),
        Arc::new(counts.finish()),
        Arc::new(volumes.finish().with_data_type(SHARES_TYPE)),
        Arc::new(dollar_volumes.finish().with_data_type(DOLLAR_VOLUME_TYPE)),
        Arc::new(opened_at.finish()),
        open,
        Arc::new(closed_at.finish()),
        close,
        high,
        low,
    ];
    let metadata = provenance
        .entries()
        .into_iter()
        .map(|(name, value)| ::parquet::file::metadata::KeyValue::new(name.to_string(), value))
        .collect();
    parquet::write(schema(), columns, LAYOUT_VERSION, metadata)
        .map_err(|reason| EncodeRefusal::Parquet { reason })
}

/// The trade bars and provenance a file written by `encode` under `key` holds, each rebuilt through `TradeBar::new`.
pub fn decode(key: &Key, bytes: Vec<u8>) -> Result<(Vec<TradeBar>, Provenance), DecodeRefusal> {
    let (provider, interval, session) = trades_key(key).ok_or(DecodeRefusal::NotATradesKey)?;
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
        let decimals = |index: usize| parquet::downcast::<Decimal128Array>(batch.column(index));
        let instants =
            |index: usize| parquet::downcast::<TimestampNanosecondArray>(batch.column(index));
        let symbols = parquet::downcast::<StringArray>(batch.column(0))?;
        let timestamps = parquet::downcast::<TimestampMicrosecondArray>(batch.column(1))?;
        let counts = parquet::downcast::<UInt64Array>(batch.column(2))?;
        let volumes = decimals(3)?;
        let dollar_volumes = decimals(4)?;
        let (opened_at, open, closed_at, close) =
            (instants(5)?, decimals(6)?, instants(7)?, decimals(8)?);
        let (high, low) = (decimals(9)?, decimals(10)?);
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
            let totals = TradeTotals::new(
                TradeCount::new(counts.value(row)),
                u64::try_from(volumes.value(row))
                    .map(Shares::from_units)
                    .map_err(|error| refused(error.to_string()))?,
                u128::try_from(dollar_volumes.value(row))
                    .map(DollarVolume::from_units)
                    .map_err(|error| refused(error.to_string()))?,
            );
            let open_close = match opened_at.is_valid(row) {
                true => Some(OpenClose::new(
                    (
                        DateTime::from_timestamp_nanos(opened_at.value(row)),
                        price(open)?,
                    ),
                    (
                        DateTime::from_timestamp_nanos(closed_at.value(row)),
                        price(close)?,
                    ),
                )),
                false => None,
            };
            let high_low = match high.is_valid(row) {
                true => Some(
                    HighLow::new(price(high)?, price(low)?)
                        .map_err(|error| refused(format!("{error:?}")))?,
                ),
                false => None,
            };
            let bar = TradeBar::new(
                symbol,
                interval,
                timestamp,
                TradeSums::new(totals, open_close, high_low),
            )
            .map_err(|error| refused(format!("{error:?}")))?;
            bars.push(bar);
        }
    }
    Ok((bars, provenance))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use uuid::Uuid;

    use super::*;
    use crate::archive::bars::Subscription;
    use crate::common::journal::RunId;
    use crate::common::market::record::Trade;
    use crate::common::market::trade_bars::{Print, TradeConditions, TradeFold, UpdateRules};
    use crate::common::storage::Origin;

    #[test]
    fn test_trade_bars_read_back_exactly_with_their_missing_prices() {
        let session = SessionDate::from_date(chrono::NaiveDate::from_ymd_opt(2026, 10, 2).unwrap());
        let key = Key::Trades {
            provider: Provider::Massive,
            origin: Origin::Derived,
            interval: BarInterval::OneMinute,
            session,
        };
        let mut fold = TradeFold::new(
            session,
            TradeConditions::new(BTreeMap::from([(37, UpdateRules::new(true, false, false))])),
        );
        for (at, dollars, shares, codes) in [
            ("2026-10-02T13:30:01.000000123Z", 100.01, 300.0, vec![]),
            ("2026-10-02T13:31:00Z", 100.02, 0.25, vec![37]),
        ] {
            let trade = Trade::new(
                Symbol::new("AAPL").unwrap(),
                at.parse().unwrap(),
                Price::from_dollars(dollars).unwrap(),
                Shares::from_float(shares).unwrap(),
            )
            .unwrap();
            fold.push(&Print::Trade(trade), &codes, false);
        }
        let (written, _) = fold.finish();
        let provenance = Provenance::new(
            Subscription::StocksAdvanced,
            "2026-10-03T07:00:00Z".parse().unwrap(),
            RunId::new(Uuid::from_u128(5)),
            None,
        );
        let (read, read_provenance) =
            decode(&key, encode(&key, &written, &provenance).unwrap()).unwrap();
        assert_eq!(read.len(), 2);
        assert_eq!(read, written);
        assert_eq!(read[1].sums().open_close(), None);
        assert_eq!(read_provenance, provenance);
    }
}
