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
use super::parquet::{self, PlacementRefusal, ReadRefusal, RowCause};
use crate::common::market::aggregate::TradeTotals;
use crate::common::market::trade_bars::{HighLow, OpenClose, TradeBar, TradeSums};
use crate::common::market::{DollarVolume, Shares, StampedPrice, Symbol, TradeCount};
use crate::common::storage::{Provider, TradesKey};

/// The file layout this build writes, read back from the metadata before any row.
const LAYOUT_VERSION: &str = "1";

const PRICE_TYPE: DataType = DataType::Decimal128(18, 6);
const SHARES_TYPE: DataType = DataType::Decimal128(20, 6);
const DOLLAR_VOLUME_TYPE: DataType = DataType::Decimal128(38, 12);

/// Why trade bars were not written under a key.
#[derive(Debug, Clone, PartialEq)]
pub enum EncodeRefusal {
    SubscriptionProvider {
        provenance: Provenance,
        key: Provider,
    },
    Placement(PlacementRefusal),
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
    key: &TradesKey,
    bars: &[TradeBar],
    provenance: &Provenance,
) -> Result<Vec<u8>, EncodeRefusal> {
    let (provider, interval, session) = (key.provider(), key.interval(), key.session());
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
    let mut counts = UInt64Builder::new();
    let mut volumes = Decimal128Builder::new();
    let mut dollar_volumes = Decimal128Builder::new();
    let mut opened_at = TimestampNanosecondBuilder::new().with_timezone("UTC");
    let mut closed_at = TimestampNanosecondBuilder::new().with_timezone("UTC");
    let mut prices: [Decimal128Builder; 4] = std::array::from_fn(|_| Decimal128Builder::new());
    for bar in ordered {
        let (symbol, timestamp) = (bar.symbol(), bar.timestamp());
        let unrepresentable = || EncodeRefusal::Unrepresentable {
            symbol: symbol.clone(),
            timestamp,
        };
        let sums = bar.sums();
        let totals = sums.totals();
        let dollar_volume =
            parquet::widest_decimal(totals.dollar_volume().units()).ok_or_else(unrepresentable)?;
        let nanoseconds = |instant: DateTime<Utc>| instant.timestamp_nanos_opt();
        let open_close = sums.open_close();
        let (open_instant, close_instant) = match open_close {
            Some(pair) => (
                Some(nanoseconds(pair.open().at()).ok_or_else(unrepresentable)?),
                Some(nanoseconds(pair.close().at()).ok_or_else(unrepresentable)?),
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
            open_close.map(|pair| pair.open().price()),
            open_close.map(|pair| pair.close().price()),
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
    parquet::write(schema(), columns, LAYOUT_VERSION, provenance.metadata())
        .map_err(|reason| EncodeRefusal::Parquet { reason })
}

/// The trade bars and provenance a file written by `encode` under `key` holds, each rebuilt through `TradeBar::new`.
pub fn decode(
    key: &TradesKey,
    bytes: Vec<u8>,
) -> Result<(Vec<TradeBar>, Provenance), DecodeRefusal> {
    let (provider, interval, session) = (key.provider(), key.interval(), key.session());
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
        let instants = |index: usize| parquet::column::<TimestampNanosecondArray>(&batch, index);
        let symbols = parquet::column::<StringArray>(&batch, 0)?;
        let timestamps = parquet::column::<TimestampMicrosecondArray>(&batch, 1)?;
        let counts = parquet::column::<UInt64Array>(&batch, 2)?;
        let (volumes, dollar_volumes) = (decimals(3)?, decimals(4)?);
        let (opened_at, open, closed_at, close) =
            (instants(5)?, decimals(6)?, instants(7)?, decimals(8)?);
        let (high, low) = (decimals(9)?, decimals(10)?);
        let read = |row: usize| -> Result<TradeBar, RowCause> {
            let totals = TradeTotals::new(
                TradeCount::new(counts.value(row)),
                Shares::from_units(volumes.integer(row)?),
                DollarVolume::from_units(dollar_volumes.integer(row)?),
            );
            let instant = |column: &TimestampNanosecondArray| {
                DateTime::from_timestamp_nanos(column.value(row))
            };
            // A pair is whole or absent; a null read as a value would be a price or instant no print set.
            let pair = [
                (opened_at.name(), opened_at.is_valid(row)),
                (open.name(), open.is_valid(row)),
                (closed_at.name(), closed_at.is_valid(row)),
                (close.name(), close.is_valid(row)),
            ];
            let open_close = match pair.map(|(_, valid)| valid) {
                [true, true, true, true] => Some(
                    OpenClose::new(
                        StampedPrice::new(instant(&opened_at), open.price(row)?),
                        StampedPrice::new(instant(&closed_at), close.price(row)?),
                    )
                    .map_err(RowCause::OpenClose)?,
                ),
                [false, false, false, false] => None,
                _partial => return Err(parquet::partly_null(&pair)),
            };
            let pair = [
                (high.name(), high.is_valid(row)),
                (low.name(), low.is_valid(row)),
            ];
            let high_low = match pair.map(|(_, valid)| valid) {
                [true, true] => Some(
                    HighLow::new(high.price(row)?, low.price(row)?).map_err(RowCause::HighLow)?,
                ),
                [false, false] => None,
                _partial => return Err(parquet::partly_null(&pair)),
            };
            TradeBar::new(
                Symbol::new(symbols.value(row)).map_err(RowCause::Symbol)?,
                interval,
                timestamps.instant_in(row, session)?,
                TradeSums::new(totals, open_close, high_low),
            )
            .map_err(RowCause::TradeBar)
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
    use std::collections::BTreeMap;

    use uuid::Uuid;

    use super::*;
    use crate::archive::bars::Subscription;
    use crate::common::journal::RunId;
    use crate::common::market::record::BarInterval;
    use crate::common::market::record::Trade;
    use crate::common::market::trade_bars::{
        Condition, ConditionCode, ConditionStatus, Correction, Print, TradeConditions, TradeFold,
        UpdateRules,
    };
    use crate::common::market::{Price, PriceRefusal};
    use crate::common::storage::Origin;
    use crate::common::time::SessionDate;

    #[test]
    fn test_trade_bars_read_back_exactly_with_their_missing_prices() {
        let session = SessionDate::from_date(chrono::NaiveDate::from_ymd_opt(2026, 10, 2).unwrap());
        let key = TradesKey::new(
            Provider::Massive,
            Origin::Derived,
            BarInterval::OneMinute,
            session,
        );
        let mut fold = TradeFold::new(
            session,
            TradeConditions::new(BTreeMap::from([(
                ConditionCode::new(37),
                Condition::new(
                    UpdateRules::VOLUME_ONLY,
                    None,
                    None,
                    ConditionStatus::Current,
                ),
            )])),
        );
        for (at, dollars, shares, codes) in [
            ("2026-10-02T13:30:01.000000123Z", 100.01, 300.0, vec![]),
            (
                "2026-10-02T13:31:00Z",
                100.02,
                0.25,
                vec![ConditionCode::new(37)],
            ),
        ] {
            let trade = Trade::new(
                Symbol::new("AAPL").unwrap(),
                at.parse().unwrap(),
                Price::from_dollars(dollars).unwrap(),
                Shares::from_float(shares).unwrap(),
            )
            .unwrap();
            fold.push(&Print::Trade(trade), &codes, Correction::Stands);
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

    #[test]
    fn test_a_row_is_refused_with_its_typed_cause() {
        let session = SessionDate::from_date(chrono::NaiveDate::from_ymd_opt(2026, 10, 2).unwrap());
        let key = TradesKey::new(
            Provider::Massive,
            Origin::Derived,
            BarInterval::OneMinute,
            session,
        );
        let provenance = Provenance::new(
            Subscription::StocksAdvanced,
            "2026-10-03T07:00:00Z".parse().unwrap(),
            RunId::new(Uuid::from_u128(5)),
            None,
        );
        let opened = "2026-10-02T13:30:01Z"
            .parse::<DateTime<Utc>>()
            .unwrap()
            .timestamp_nanos_opt();
        let price = |ticks: Option<i128>| {
            Arc::new(Decimal128Array::from(vec![ticks]).with_data_type(PRICE_TYPE)) as ArrayRef
        };
        let file = |closed: Option<i64>, high: Option<i128>| {
            let columns: Vec<ArrayRef> = vec![
                Arc::new(StringArray::from(vec!["AAPL"])),
                Arc::new(
                    TimestampMicrosecondArray::from(vec![1_790_947_800_000_000])
                        .with_timezone("UTC"),
                ),
                Arc::new(UInt64Array::from(vec![1])),
                Arc::new(Decimal128Array::from(vec![1_000_000]).with_data_type(SHARES_TYPE)),
                Arc::new(
                    Decimal128Array::from(vec![100_000_000_000_000])
                        .with_data_type(DOLLAR_VOLUME_TYPE),
                ),
                Arc::new(TimestampNanosecondArray::from(vec![opened]).with_timezone("UTC")),
                price(Some(100_000_000)),
                Arc::new(TimestampNanosecondArray::from(vec![closed]).with_timezone("UTC")),
                price(Some(100_000_000)),
                price(high),
                price(Some(1)),
            ];
            parquet::write(schema(), columns, LAYOUT_VERSION, provenance.metadata()).unwrap()
        };
        assert_eq!(
            decode(&key, file(None, Some(2))).map(|_| ()),
            Err(DecodeRefusal::Row {
                index: 0,
                cause: RowCause::PartlyNull {
                    null: vec!["closed_at".to_string()]
                },
            })
        );
        assert_eq!(
            decode(&key, file(opened, Some(0))).map(|_| ()),
            Err(DecodeRefusal::Row {
                index: 0,
                cause: RowCause::Price(PriceRefusal::OutOfRange { ticks: 0 }),
            })
        );
        assert!(decode(&key, file(opened, Some(2))).is_ok());
    }

    proptest::proptest! {
        /// Bars at any interval, with or without either price pair, read back exactly as written and in order.
        #[test]
        fn property_trade_bars_round_trip(
            interval in proptest::sample::select(vec![BarInterval::OneMinute, BarInterval::FiveMinute, BarInterval::OneDay]),
            rows in proptest::collection::btree_map(
                ("[A-Z]{1,4}", 0_i64..78),
                (
                    proptest::prelude::any::<(u32, u32, u64)>(),
                    proptest::option::of((1_i64..2_000_000, 0_i64..5_000, 0_i64..3_600_000_000_000)),
                    proptest::option::of((1_i64..2_000_000, 0_i64..5_000)),
                ),
                0..12,
            ),
        ) {
            let session = SessionDate::from_date(chrono::NaiveDate::from_ymd_opt(2026, 10, 2).unwrap());
            let open: DateTime<Utc> = "2026-10-02T13:30:00Z".parse().unwrap();
            let bars: Vec<TradeBar> = rows
                .iter()
                .filter_map(|((symbol, slot), ((count, volume, dollars), open_close, high_low))| {
                    let timestamp = match interval {
                        BarInterval::OneMinute => open + chrono::TimeDelta::minutes(*slot),
                        BarInterval::FiveMinute => open + chrono::TimeDelta::minutes(5 * slot),
                        BarInterval::OneDay if *slot == 0 => session.regular_close(),
                        BarInterval::OneDay => return None,
                    };
                    let price = |ticks| Price::from_ticks(ticks).unwrap();
                    let open_close = open_close.map(|(ticks, step, span)| {
                        let first = open + chrono::TimeDelta::nanoseconds(span / 2);
                        OpenClose::new(
                            StampedPrice::new(first, price(ticks)),
                            StampedPrice::new(first + chrono::TimeDelta::nanoseconds(span / 2), price(ticks + step)),
                        )
                        .unwrap()
                    });
                    let high_low = high_low.map(|(ticks, step)| HighLow::new(price(ticks + step), price(ticks)).unwrap());
                    let totals = TradeTotals::new(
                        TradeCount::new(u64::from(*count)),
                        Shares::from_units(u64::from(*volume)),
                        DollarVolume::from_units(u128::from(*dollars)),
                    );
                    Some(TradeBar::new(Symbol::new(symbol).unwrap(), interval, timestamp, TradeSums::new(totals, open_close, high_low)).unwrap())
                })
                .collect();
            let key = TradesKey::new(Provider::Massive, Origin::Derived, interval, session);
            let provenance = Provenance::new(
                Subscription::StocksAdvanced,
                "2026-10-03T07:00:00Z".parse().unwrap(),
                RunId::new(Uuid::from_u128(5)),
                None,
            );
            let (read, read_provenance) = decode(&key, encode(&key, &bars, &provenance).unwrap()).unwrap();
            proptest::prop_assert_eq!(read, bars);
            proptest::prop_assert_eq!(read_provenance, provenance);
        }
    }
}
