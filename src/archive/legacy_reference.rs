//! The legacy archiver's quarterly security snapshots under `data/derived/equity/reference/`, read into
//! `SecurityDetails` so they can be written once under their key. Temporary: archive task A6 deletes the module.

use arrow_array::{Array, Float64Array, LargeStringArray, RecordBatch};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use crate::common::market::security_details::{
    CentralIndexKey, IndustryCode, MarketIdentifierCode, SecurityDetails,
};
use crate::common::market::{Shares, Symbol};
use crate::common::time::SessionDate;
use crate::ingest::massive::{capitalization_to_the_cent, security_type};

/// Where the legacy archiver kept its snapshots.
pub const LEGACY_SNAPSHOT_ROOT: &str = "data/derived/equity/reference/";

/// A legacy row that did not become details, named by its ticker with the reason.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RefusedSnapshotRow {
    pub ticker: String,
    pub reason: String,
}

/// A snapshot read from its legacy file: the date it is for, every row that read, and every row that did not.
#[derive(Debug, Clone, PartialEq)]
pub struct LegacySnapshot {
    pub as_of: SessionDate,
    pub details: Vec<SecurityDetails>,
    pub refused: Vec<RefusedSnapshotRow>,
}

/// A text cell, `None` where the legacy file holds a null.
fn text(array: &LargeStringArray, row: usize) -> Option<&str> {
    array.is_valid(row).then(|| array.value(row))
}

/// Reads one legacy snapshot file; a file whose columns or dates are not the legacy archiver's refuses whole.
pub fn read_legacy_snapshot(bytes: Vec<u8>) -> Result<LegacySnapshot, String> {
    let batches = ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(bytes))
        .and_then(|builder| builder.build())
        .map_err(|error| error.to_string())?
        .collect::<Result<Vec<RecordBatch>, _>>()
        .map_err(|error| error.to_string())?;
    let mut as_of: Option<SessionDate> = None;
    let mut details = Vec::new();
    let mut refused = Vec::new();
    for batch in batches {
        let strings = |name: &str| {
            batch
                .column_by_name(name)
                .and_then(|column| column.as_any().downcast_ref::<LargeStringArray>())
                .ok_or_else(|| format!("no text column {name}"))
        };
        let floats = |name: &str| {
            batch
                .column_by_name(name)
                .and_then(|column| column.as_any().downcast_ref::<Float64Array>())
                .ok_or_else(|| format!("no float column {name}"))
        };
        let (tickers, dates, kinds) = (
            strings("ticker")?,
            strings("as_of")?,
            strings("security_type")?,
        );
        let (codes, descriptions) = (strings("sic_code")?, strings("sic_description")?);
        let (exchanges, central_index_keys) = (strings("primary_exchange")?, strings("cik")?);
        let (shares, capitalizations) = (
            floats("shares_outstanding")?,
            floats("reported_market_capitalization")?,
        );
        for row in 0..batch.num_rows() {
            let date = dates
                .value(row)
                .parse()
                .map(SessionDate::from_date)
                .map_err(|_| format!("as_of `{}` is not a date", dates.value(row)))?;
            match as_of {
                Some(seen) if seen != date => {
                    return Err(format!("one file holds {seen} and {date}"));
                }
                Some(_) | None => as_of = Some(date),
            }
            let text = |array| text(array, row);
            let float = |array: &Float64Array| array.is_valid(row).then(|| array.value(row));
            let ticker = tickers.value(row);
            let read = (|| {
                Ok::<_, String>(SecurityDetails::new(
                    Symbol::new(ticker).map_err(|error| format!("{error:?}"))?,
                    text(kinds)
                        .map(|code| security_type(code).ok_or(format!("security type `{code}`")))
                        .transpose()?,
                    text(codes)
                        .map(IndustryCode::new)
                        .transpose()
                        .map_err(|error| format!("{error:?}"))?,
                    text(descriptions).map(String::from),
                    float(shares)
                        .map(Shares::from_float)
                        .transpose()
                        .map_err(|error| format!("shares outstanding: {error:?}"))?,
                    float(capitalizations)
                        .map(capitalization_to_the_cent)
                        .transpose()
                        .map_err(|error| format!("market capitalization: {error:?}"))?,
                    text(exchanges)
                        .map(MarketIdentifierCode::new)
                        .transpose()
                        .map_err(|error| format!("{error:?}"))?,
                    text(central_index_keys)
                        .map(|raw| {
                            raw.parse::<u64>()
                                .map(CentralIndexKey::new)
                                .map_err(|_| format!("central index key `{raw}`"))
                        })
                        .transpose()?,
                ))
            })();
            match read {
                Ok(row) => details.push(row),
                Err(reason) => refused.push(RefusedSnapshotRow {
                    ticker: ticker.to_string(),
                    reason,
                }),
            }
        }
    }
    let as_of = as_of.ok_or("the file holds no rows")?;
    Ok(LegacySnapshot {
        as_of,
        details,
        refused,
    })
}
