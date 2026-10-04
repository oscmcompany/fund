//! The legacy archiver's daily bars under `data/derived/`, read into the current types so a study sees one shape.
//! Temporary: archive task A6 moves this history into the `Key` layout and deletes the module.

use std::collections::BTreeMap;
use std::collections::btree_map::Entry;

use arrow_array::{Array, Float64Array, Int64Array, RecordBatch, StringArray};
use chrono::DateTime;
use parquet::arrow::arrow_reader::{ArrowReaderOptions, ParquetRecordBatchReaderBuilder};

use crate::archive::Archive;
use crate::common::laboratory::dataset::DatasetLeg;
use crate::common::market::record::{Bar, BarInterval, Ohlc};
use crate::common::market::{DollarVolume, Price, Shares, SharesRefusal, Symbol, TradeCount};
use crate::common::storage::date_partition;
use crate::common::time::SessionDate;
use crate::common::time::calendar::TradingCalendar;
use crate::ingest::RowRefusal;
use crate::ingest::massive::is_exchange_test_ticker;
use crate::laboratory::Study;
use crate::laboratory::dataset::{Dataset, DatasetError, load};

/// Why a legacy partition was not read; any row that fails refuses the whole partition.
#[derive(Debug, Clone, PartialEq)]
pub enum LegacyRefusal {
    Parquet {
        reason: String,
    },
    /// Absent, or not the type the legacy archiver wrote, named as found.
    Column {
        name: &'static str,
        found: Option<String>,
    },
    Null {
        column: &'static str,
        row: usize,
    },
    Negative {
        column: &'static str,
        row: usize,
        value: i64,
    },
    Row {
        ticker: String,
        cause: RowRefusal,
    },
}

/// Legacy daily bars for every trading session from `first` to `last`, journaled to `study` before it is returned. Its
/// sessions hold no preferreds, warrants or rights, and volume rounded to whole shares, where the current layout's
/// keeps the vendor's fraction.
pub async fn legacy_daily_bars(
    archive: &Archive,
    calendar: &TradingCalendar,
    first: SessionDate,
    last: SessionDate,
    study: &mut Study,
) -> Result<Dataset, DatasetError> {
    load(
        DatasetLeg::LegacyDailyBars,
        archive,
        calendar,
        first,
        last,
        study,
    )
    .await
}

/// Where the legacy archiver wrote `session`'s daily bars, which no `Key` names.
pub(crate) fn path(session: SessionDate) -> String {
    format!(
        "data/derived/equity/bars/interval=one_day/{}/data.parquet",
        date_partition(session)
    )
}

/// `session`'s bars in symbol order, exchange test tickers dropped.
pub(crate) fn decode(session: SessionDate, body: Vec<u8>) -> Result<Vec<Bar>, LegacyRefusal> {
    let parquet = |error: &dyn std::fmt::Display| LegacyRefusal::Parquet {
        reason: error.to_string(),
    };
    // Polars stored its own Arrow schema beside the file's; skipping it reads every string column as plain UTF-8.
    let options = ArrowReaderOptions::new().with_skip_arrow_metadata(true);
    let batches =
        ParquetRecordBatchReaderBuilder::try_new_with_options(bytes::Bytes::from(body), options)
            .map_err(|error| parquet(&error))?
            .build()
            .map_err(|error| parquet(&error))?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| parquet(&error))?;
    let mut bars = BTreeMap::new();
    let mut offset = 0;
    for batch in batches {
        for row in 0..batch.num_rows() {
            if let Some(bar) = bar(&batch, row, offset + row, session)? {
                match bars.entry(bar.symbol().clone()) {
                    Entry::Vacant(vacant) => vacant.insert(bar),
                    Entry::Occupied(occupied) => {
                        return Err(LegacyRefusal::Row {
                            ticker: occupied.key().to_string(),
                            cause: RowRefusal::Duplicate,
                        });
                    }
                };
            }
        }
        offset += batch.num_rows();
    }
    Ok(bars.into_values().collect())
}

/// The bar in `row` of `batch`, which is row `index` of the file; `None` for an exchange test ticker.
fn bar(
    batch: &RecordBatch,
    row: usize,
    index: usize,
    session: SessionDate,
) -> Result<Option<Bar>, LegacyRefusal> {
    let ticker = required(
        column::<StringArray>(batch, "ticker")?,
        "ticker",
        row,
        index,
    )?
    .value(row);
    if is_exchange_test_ticker(ticker) {
        return Ok(None);
    }
    let refused = |cause: RowRefusal| LegacyRefusal::Row {
        ticker: ticker.to_string(),
        cause,
    };
    let symbol = Symbol::new(ticker).map_err(|refusal| refused(RowRefusal::Symbol(refusal)))?;
    let milliseconds = required(
        column::<Int64Array>(batch, "timestamp")?,
        "timestamp",
        row,
        index,
    )?
    .value(row);
    let timestamp = DateTime::from_timestamp_millis(milliseconds)
        .filter(|instant| SessionDate::at(*instant) == session)
        .ok_or_else(|| {
            refused(RowRefusal::Session {
                timestamp: milliseconds.to_string(),
            })
        })?;
    let mut prices = Vec::new();
    for name in ["open_price", "high_price", "low_price", "close_price"] {
        let dollars = required(column::<Float64Array>(batch, name)?, name, row, index)?.value(row);
        prices.push(
            Price::from_dollars(dollars).map_err(|refusal| refused(RowRefusal::Price(refusal)))?,
        );
    }
    let prices = Ohlc::new(prices[0], prices[1], prices[2], prices[3])
        .map_err(|refusal| refused(RowRefusal::Prices(refusal)))?;
    let volumes = required(column::<Int64Array>(batch, "volume")?, "volume", row, index)?;
    let volume = Shares::whole(count(volumes.value(row), "volume", index)?)
        .map_err(|refusal: SharesRefusal| refused(RowRefusal::Shares(refusal)))?;
    let transactions = column::<Int64Array>(batch, "transactions")?;
    let trade_count = transactions
        .is_valid(row)
        .then(|| count(transactions.value(row), "transactions", index))
        .transpose()?
        .map(TradeCount::new);
    let averages = column::<Float64Array>(batch, "volume_weighted_average_price")?;
    let dollar_volume = averages
        .is_valid(row)
        .then(|| DollarVolume::from_average(averages.value(row), volume))
        .transpose()
        .map_err(|refusal| refused(RowRefusal::DollarVolume(refusal)))?;
    Bar::new(
        symbol,
        BarInterval::OneDay,
        timestamp,
        prices,
        volume,
        trade_count,
        dollar_volume,
    )
    .map(Some)
    .map_err(|refusal| refused(RowRefusal::Bar(refusal)))
}

fn column<'a, T: 'static>(
    batch: &'a RecordBatch,
    name: &'static str,
) -> Result<&'a T, LegacyRefusal> {
    let column = batch
        .column_by_name(name)
        .ok_or(LegacyRefusal::Column { name, found: None })?;
    column
        .as_any()
        .downcast_ref::<T>()
        .ok_or_else(|| LegacyRefusal::Column {
            name,
            found: Some(column.data_type().to_string()),
        })
}

fn required<'a, T: Array>(
    array: &'a T,
    column: &'static str,
    row: usize,
    index: usize,
) -> Result<&'a T, LegacyRefusal> {
    match array.is_valid(row) {
        true => Ok(array),
        false => Err(LegacyRefusal::Null { column, row: index }),
    }
}

/// A count as the unsigned value it is; a negative one is refused rather than read as zero.
fn count(value: i64, column: &'static str, index: usize) -> Result<u64, LegacyRefusal> {
    u64::try_from(value).map_err(|_| LegacyRefusal::Negative {
        column,
        row: index,
        value,
    })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{ArrayRef, LargeStringArray};
    use arrow_schema::{Field, Schema};
    use chrono::{NaiveDate, TimeDelta};
    use parquet::arrow::ArrowWriter;
    use parquet::basic::{Compression, ZstdLevel};
    use parquet::file::properties::WriterProperties;
    use proptest::prelude::*;

    use super::*;
    use crate::common::market::record::BarRefusal;
    use crate::ingest::alpaca::Alpaca;
    use crate::laboratory::dataset::{daily_bars, lineage};

    fn session() -> SessionDate {
        SessionDate::from_date(NaiveDate::from_ymd_opt(2026, 9, 25).unwrap())
    }

    /// One row as the legacy archiver wrote it.
    #[derive(Debug, Clone)]
    struct Row {
        ticker: String,
        timestamp: Option<i64>,
        prices: [f64; 4],
        volume: Option<i64>,
        average: Option<f64>,
        transactions: Option<i64>,
    }

    fn row(ticker: &str) -> Row {
        Row {
            ticker: ticker.to_string(),
            timestamp: Some(session().regular_close().timestamp_millis()),
            prices: [10.0, 12.0, 9.5, 11.0],
            volume: Some(100),
            average: Some(10.75),
            transactions: Some(7),
        }
    }

    /// A file in the legacy archiver's columns, compressed with zstd and with strings as `LargeUtf8` the way Polars
    /// stored them, `except` left out.
    fn file(rows: &[Row], except: Option<&str>) -> Vec<u8> {
        let floats = |read: &dyn Fn(&Row) -> Option<f64>| -> ArrayRef {
            Arc::new(Float64Array::from(
                rows.iter().map(read).collect::<Vec<_>>(),
            ))
        };
        let integers = |read: &dyn Fn(&Row) -> Option<i64>| -> ArrayRef {
            Arc::new(Int64Array::from(rows.iter().map(read).collect::<Vec<_>>()))
        };
        let columns: Vec<(&str, ArrayRef)> = vec![
            (
                "ticker",
                Arc::new(LargeStringArray::from(
                    rows.iter()
                        .map(|row| row.ticker.as_str())
                        .collect::<Vec<_>>(),
                )),
            ),
            (
                "bar_interval",
                Arc::new(LargeStringArray::from(vec!["one_day"; rows.len()])),
            ),
            ("timestamp", integers(&|row| row.timestamp)),
            ("open_price", floats(&|row| Some(row.prices[0]))),
            ("high_price", floats(&|row| Some(row.prices[1]))),
            ("low_price", floats(&|row| Some(row.prices[2]))),
            ("close_price", floats(&|row| Some(row.prices[3]))),
            ("volume", integers(&|row| row.volume)),
            ("volume_weighted_average_price", floats(&|row| row.average)),
            ("transactions", integers(&|row| row.transactions)),
        ];
        let columns: Vec<(&str, ArrayRef)> = columns
            .into_iter()
            .filter(|(name, _)| Some(*name) != except)
            .collect();
        let schema = Arc::new(Schema::new(
            columns
                .iter()
                .map(|(name, array)| Field::new(*name, array.data_type().clone(), true))
                .collect::<Vec<_>>(),
        ));
        let batch = RecordBatch::try_new(
            schema.clone(),
            columns.into_iter().map(|(_, array)| array).collect(),
        )
        .unwrap();
        let mut bytes = Vec::new();
        let properties = WriterProperties::builder()
            .set_compression(Compression::ZSTD(ZstdLevel::default()))
            .build();
        let mut writer = ArrowWriter::try_new(&mut bytes, schema, Some(properties)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        bytes
    }

    #[test]
    fn test_the_path_is_the_legacy_archivers() {
        assert_eq!(
            path(session()),
            "data/derived/equity/bars/interval=one_day/year=2026/month=09/day=25/data.parquet"
        );
    }

    #[test]
    fn test_a_row_reads_as_the_current_bar() {
        let bars = decode(session(), file(&[row("BRK.B")], None)).unwrap();
        let price = |dollars: f64| Price::from_dollars(dollars).unwrap();
        let shares = Shares::whole(100).unwrap();
        assert_eq!(
            bars,
            [Bar::new(
                Symbol::new("BRK.B").unwrap(),
                BarInterval::OneDay,
                session().regular_close(),
                Ohlc::new(price(10.0), price(12.0), price(9.5), price(11.0)).unwrap(),
                shares,
                Some(TradeCount::new(7)),
                Some(DollarVolume::from_units(1_075_000_000_000_000)),
            )
            .unwrap()]
        );
    }

    /// Test tickers are dropped by exact name, so the real `CBOE` beside `CBOX` survives.
    #[test]
    fn test_exchange_test_tickers_are_dropped() {
        let rows = ["ZVZZT", "CBOE", "NTEST.H", "CBOX", "AAPL"].map(row);
        let symbols: Vec<String> = decode(session(), file(&rows, None))
            .unwrap()
            .iter()
            .map(|bar| bar.symbol().to_string())
            .collect();
        assert_eq!(symbols, ["AAPL", "CBOE"]);
    }

    #[test]
    fn test_a_bar_not_stamped_at_the_close_is_refused() {
        let midnight = Row {
            timestamp: Some(session().midnight().timestamp_millis()),
            ..row("AAPL")
        };
        assert_eq!(
            decode(session(), file(&[midnight], None)),
            Err(LegacyRefusal::Row {
                ticker: "AAPL".to_string(),
                cause: RowRefusal::Bar(BarRefusal::Misaligned {
                    interval: BarInterval::OneDay,
                    timestamp: session().midnight(),
                }),
            })
        );
        let tomorrow = Row {
            timestamp: Some((session().regular_close() + TimeDelta::days(1)).timestamp_millis()),
            ..row("AAPL")
        };
        assert!(matches!(
            decode(session(), file(&[tomorrow], None)),
            Err(LegacyRefusal::Row {
                cause: RowRefusal::Session { .. },
                ..
            })
        ));
    }

    #[test]
    fn test_a_partition_with_a_bad_row_is_refused_whole() {
        let refused = |rows: &[Row]| decode(session(), file(rows, None)).unwrap_err();
        assert_eq!(
            refused(&[row("AAPL"), row("AAPL")]),
            LegacyRefusal::Row {
                ticker: "AAPL".to_string(),
                cause: RowRefusal::Duplicate
            }
        );
        assert_eq!(
            refused(&[
                row("AAPL"),
                Row {
                    volume: None,
                    ..row("MSFT")
                }
            ]),
            LegacyRefusal::Null {
                column: "volume",
                row: 1
            }
        );
        assert_eq!(
            refused(&[Row {
                transactions: Some(-1),
                ..row("AAPL")
            }]),
            LegacyRefusal::Negative {
                column: "transactions",
                row: 0,
                value: -1
            }
        );
        assert!(matches!(
            refused(&[row("aapl")]),
            LegacyRefusal::Row {
                cause: RowRefusal::Symbol(_),
                ..
            }
        ));
    }

    /// A refusal names its row in the file, not in the reader's batch of 1,024.
    #[test]
    fn test_a_refusal_past_the_first_batch_names_its_row_in_the_file() {
        let mut rows: Vec<Row> = (0..1100_u32)
            .map(|index| {
                let letters = [index / 676, index / 26 % 26, index % 26]
                    .map(|place| char::from(b'A' + place as u8));
                row(&String::from_iter(letters))
            })
            .collect();
        rows[1050].volume = None;
        assert_eq!(
            decode(session(), file(&rows, None)),
            Err(LegacyRefusal::Null {
                column: "volume",
                row: 1050
            })
        );
    }

    #[test]
    fn test_a_missing_column_is_refused() {
        assert_eq!(
            decode(session(), file(&[row("AAPL")], Some("transactions"))),
            Err(LegacyRefusal::Column {
                name: "transactions",
                found: None
            })
        );
    }

    /// A null average or trade count is unreported, not zero.
    #[test]
    fn test_an_unreported_average_or_count_reads_as_none() {
        let bars = decode(
            session(),
            file(
                &[Row {
                    average: None,
                    transactions: None,
                    ..row("AAPL")
                }],
                None,
            ),
        )
        .unwrap();
        assert_eq!(
            (bars[0].trade_count(), bars[0].dollar_volume()),
            (None, None)
        );
    }

    fn any_row() -> impl Strategy<Value = Row> {
        (
            prop::sample::select(vec!["AAPL", "BRK.B", "MSFT", "ZTS", "CBOE"]),
            prop::array::uniform4(1_i64..10_000_000_000_000),
            0_i64..10_000_000_000,
            prop::option::of(1_i64..10_000_000_000),
            prop::option::of(0_i64..1_000_000),
        )
            .prop_map(|(ticker, mut ticks, volume, average, transactions)| {
                ticks.sort();
                let [low, first, second, high] = ticks.map(|tick| tick as f64 / 1_000_000.0);
                Row {
                    prices: [first, high, low, second],
                    volume: Some(volume),
                    average: average.map(|tick| tick as f64 / 1_000_000.0),
                    transactions,
                    ..row(ticker)
                }
            })
    }

    proptest! {
        /// Every row the legacy archiver could write reads back as the bar its values name, one per symbol.
        #[test]
        fn property_a_legacy_row_reads_back_as_its_bar(rows in prop::collection::vec(any_row(), 0..20)) {
            let mut unique: BTreeMap<String, Row> = BTreeMap::new();
            for row in rows {
                unique.entry(row.ticker.clone()).or_insert(row);
            }
            let rows: Vec<Row> = unique.into_values().collect();
            let bars = decode(session(), file(&rows, None)).unwrap();
            prop_assert_eq!(bars.len(), rows.len());
            for (bar, row) in bars.iter().zip(&rows) {
                let ticks = row.prices.map(|dollars| (dollars * 1_000_000.0).round() as i64);
                let prices = bar.prices();
                prop_assert_eq!(bar.symbol().as_str(), row.ticker.as_str());
                prop_assert_eq!(bar.timestamp(), session().regular_close());
                prop_assert_eq!(
                    [prices.open(), prices.high(), prices.low(), prices.close()].map(Price::ticks),
                    ticks
                );
                prop_assert_eq!(bar.volume().units(), row.volume.unwrap() as u64 * 1_000_000);
                prop_assert_eq!(bar.trade_count().map(TradeCount::count), row.transactions.map(|count| count as u64));
                prop_assert_eq!(bar.dollar_volume().is_some(), row.average.is_some());
            }
        }
    }

    /// Whether two layouts' bars for one symbol agree: exactly, but for the legacy volume rounded to whole shares, which
    /// leaves the volume within half a share and the average price within float precision.
    fn agrees(legacy: &Bar, current: &Bar) -> bool {
        let averages = (
            legacy.volume_weighted_average_price(),
            current.volume_weighted_average_price(),
        );
        (legacy.timestamp(), legacy.prices(), legacy.trade_count())
            == (current.timestamp(), current.prices(), current.trade_count())
            && legacy.volume().units().abs_diff(current.volume().units()) <= 500_000
            && match averages {
                (Some(legacy), Some(current)) => (legacy - current).abs() <= 1e-9 * current,
                (None, None) => true,
                (Some(_), None) | (None, Some(_)) => false,
            }
    }

    /// Read-only, under secretspec: the week the legacy and current layouts both hold. A symbol in both reads as the
    /// same bar but for rounded volume, so a study crossing 2026-09-28 sees one series; preferreds, warrants and rights
    /// are only in the current.
    #[tokio::test]
    #[ignore = "reads the live archive and the Alpaca calendar; run deliberately under secretspec"]
    async fn live_the_legacy_and_current_layouts_agree_where_both_hold_a_symbol() {
        let configuration = aws_config::load_from_env().await;
        let archive = Archive::market_data(&configuration).unwrap();
        let alpaca = Alpaca::from_environment(reqwest::Client::new()).unwrap();
        let session_on =
            |month, day| SessionDate::from_date(NaiveDate::from_ymd_opt(2026, month, day).unwrap());
        let (first, last) = (session_on(9, 28), session_on(10, 2));
        let calendar = alpaca.calendar(first, last).await.unwrap();
        let directory = std::env::temp_dir().join(format!("fund-study-{}", uuid::Uuid::new_v4()));
        let label = crate::common::laboratory::experiment::Label::new("legacy seam check").unwrap();
        let mut study = Study::open(label, &directory).unwrap();
        let legacy = legacy_daily_bars(&archive, &calendar, first, last, &mut study)
            .await
            .unwrap();
        let current = daily_bars(&archive, &calendar, first, last, &mut study)
            .await
            .unwrap();
        std::fs::remove_dir_all(&directory).unwrap();
        assert_eq!(legacy.fingerprint().leg(), DatasetLeg::LegacyDailyBars);
        assert_eq!(lineage(&archive, legacy.fingerprint()).await.unwrap(), []);
        let sessions: Vec<_> = legacy
            .bars()
            .keys()
            .filter(|session| current.bars().contains_key(session))
            .collect();
        assert!(!sessions.is_empty(), "{:?}", legacy.fingerprint());
        for session in sessions {
            let by_symbol = |bars: &[Bar]| -> BTreeMap<Symbol, Bar> {
                bars.iter()
                    .map(|bar| (bar.symbol().clone(), bar.clone()))
                    .collect()
            };
            let (old, new) = (
                by_symbol(&legacy.bars()[session]),
                by_symbol(&current.bars()[session]),
            );
            let shared: Vec<&Symbol> = old
                .keys()
                .filter(|symbol| new.contains_key(*symbol))
                .collect();
            let differing: Vec<&&Symbol> = shared
                .iter()
                .filter(|symbol| !agrees(&old[**symbol], &new[**symbol]))
                .collect();
            let only_legacy = old.len() - shared.len();
            let only_current = new.len() - shared.len();
            println!(
                "{session}: {} legacy / {} current / {} shared / {} differing / {only_legacy} only legacy / {only_current} only current",
                old.len(),
                new.len(),
                shared.len(),
                differing.len()
            );
            assert!(shared.len() > 10_000);
            assert_eq!(only_legacy, 0);
            // Measured 2026-10-04: the two archivers fetched 09-29 and 10-02 either side of a Massive revision of late prints.
            if [session_on(9, 28), session_on(9, 30), session_on(10, 1)].contains(session) {
                assert_eq!(differing, Vec::<&&Symbol>::new());
            }
        }
    }
}
