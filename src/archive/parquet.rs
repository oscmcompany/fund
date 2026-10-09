//! One Parquet file written and read in memory, shared by every layout the buckets hold: a reader checks the exact
//! schema and the layout version before any row.

use std::sync::Arc;

use arrow_array::{ArrayRef, Decimal128Array, RecordBatch, TimestampMicrosecondArray};
use arrow_schema::Schema;
use chrono::{DateTime, Utc};
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::basic::Compression;
use parquet::file::metadata::KeyValue;
use parquet::file::properties::WriterProperties;

use super::bars::Provenance;

use crate::common::market::corporate_actions::{
    ActionIdRefusal, BoundaryChangeRefusal, SeriesBoundaryRefusal, SplitRatioRefusal,
};
use crate::common::market::quote_bars::{QuoteBarRefusal, QuoteSumsRefusal};
use crate::common::market::record::{BarInterval, BarPricesRefusal, BarRefusal};
use crate::common::market::security_details::{
    CentralIndexKeyRefusal, IndustryCodeRefusal, MarketIdentifierCodeRefusal,
};
use crate::common::market::trade_bars::{HighLowRefusal, OpenCloseRefusal, TradeBarRefusal};
use crate::common::market::{Price, PriceRefusal, Symbol, SymbolRefusal};
use crate::common::storage::Provider;
use crate::common::time::SessionDate;

/// The metadata key every layout names its version under.
pub(crate) const LAYOUT_VERSION_KEY: &str = "fund.layout_version";

/// Why a file was not read.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ReadRefusal {
    #[error("the file is not readable Parquet: {reason}")]
    Parquet { reason: String },
    /// Columns other than the layout's, named as found.
    #[error("the file holds other columns: {found}")]
    Schema { found: String },
    #[error("the metadata `{name}` is absent or unreadable")]
    Metadata { name: &'static str },
    /// Written under another layout than this build reads.
    #[error("the file is written under layout {version}")]
    Layout { version: String },
}

/// One file of `columns` under `schema`, compressed with Snappy and carrying `metadata` beside the layout version.
pub(crate) fn write(
    schema: Schema,
    columns: Vec<ArrayRef>,
    layout_version: &str,
    metadata: Vec<KeyValue>,
) -> Result<Vec<u8>, String> {
    let schema = Arc::new(schema);
    let batch = RecordBatch::try_new(schema.clone(), columns).map_err(|error| error.to_string())?;
    let mut entries = vec![KeyValue::new(
        LAYOUT_VERSION_KEY.to_string(),
        layout_version.to_string(),
    )];
    entries.extend(metadata);
    let properties = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .set_key_value_metadata(Some(entries))
        .build();
    let mut bytes = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut bytes, schema, Some(properties))
        .map_err(|error| error.to_string())?;
    writer.write(&batch).map_err(|error| error.to_string())?;
    writer.close().map_err(|error| error.to_string())?;
    Ok(bytes)
}

/// The batches and metadata of a file whose schema is exactly `expected` and whose layout is `layout_version`.
///
/// One schema comparison covers column count, order, names, types and nullability, so a non-null column is
/// guaranteed by the reader rather than rechecked per row.
pub(crate) fn read(
    bytes: Vec<u8>,
    expected: &Schema,
    layout_version: &str,
) -> Result<(Vec<RecordBatch>, Vec<KeyValue>), ReadRefusal> {
    let parquet = |error: &dyn std::fmt::Display| ReadRefusal::Parquet {
        reason: error.to_string(),
    };
    let builder = ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(bytes))
        .map_err(|error| parquet(&error))?;
    if builder.schema().fields() != expected.fields() {
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
        return Err(ReadRefusal::Schema { found });
    }
    let entries: Vec<KeyValue> = builder
        .metadata()
        .file_metadata()
        .key_value_metadata()
        .cloned()
        .unwrap_or_default();
    let versions = entries
        .iter()
        .filter(|entry| entry.key == LAYOUT_VERSION_KEY)
        .count();
    // Two versions would leave the reader choosing between them.
    let version = value(&entries, LAYOUT_VERSION_KEY)
        .filter(|_| versions == 1)
        .ok_or(ReadRefusal::Metadata {
            name: LAYOUT_VERSION_KEY,
        })?;
    if version != layout_version {
        return Err(ReadRefusal::Layout { version });
    }
    let batches = builder
        .build()
        .map_err(|error| parquet(&error))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| parquet(&error))?;
    Ok((batches, entries))
}

/// The value stored under `name`, if any.
pub(crate) fn value(entries: &[KeyValue], name: &str) -> Option<String> {
    entries
        .iter()
        .find(|entry| entry.key == name)
        .and_then(|entry| entry.value.clone())
}

/// A column as the array type its schema promises; the schema check makes a mismatch a corrupt file.
pub(crate) fn downcast<T: 'static>(column: &ArrayRef) -> Result<&T, ReadRefusal> {
    column
        .as_any()
        .downcast_ref::<T>()
        .ok_or(ReadRefusal::Parquet {
            reason: format!("column is {}", column.data_type()),
        })
}

/// Why a row read back from a file no longer passes its domain's checks.
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum RowCause {
    /// A stored number past what its column's domain type holds.
    #[error("{column} holds {value}, past its type's range")]
    OutOfRange { column: String, value: i128 },
    #[error("{timestamp} is outside session {session}")]
    OutsideSession {
        timestamp: DateTime<Utc>,
        session: SessionDate,
    },
    /// Some columns of a group stored whole or not at all are null, named here.
    #[error("{} null alone", .null.join(" and "))]
    PartlyNull { null: Vec<String> },
    /// Text that names no value of its column's type.
    #[error("{column} holds `{raw}`")]
    Unparsable { column: String, raw: String },
    #[error("{0}")]
    Symbol(SymbolRefusal),
    #[error("{0}")]
    Price(PriceRefusal),
    #[error("{0}")]
    BarPrices(BarPricesRefusal),
    #[error("{0}")]
    Bar(BarRefusal),
    #[error("{0}")]
    QuoteSums(QuoteSumsRefusal),
    #[error("{0}")]
    QuoteBar(QuoteBarRefusal),
    #[error("{0}")]
    OpenClose(OpenCloseRefusal),
    #[error("{0}")]
    HighLow(HighLowRefusal),
    #[error("{0}")]
    TradeBar(TradeBarRefusal),
    #[error("{0}")]
    ActionId(ActionIdRefusal),
    #[error("{0}")]
    SplitRatio(SplitRatioRefusal),
    #[error("{0}")]
    BoundaryChange(BoundaryChangeRefusal),
    #[error("{0}")]
    SeriesBoundary(SeriesBoundaryRefusal),
    #[error("{0}")]
    IndustryCode(IndustryCodeRefusal),
    #[error("{0}")]
    MarketIdentifierCode(MarketIdentifierCodeRefusal),
    #[error("{0}")]
    CentralIndexKey(CentralIndexKeyRefusal),
}

/// A column as the array type its schema promises, named for a row's refusal.
pub(crate) struct Column<'a, T> {
    name: &'a str,
    array: &'a T,
}

/// Column `index` of `batch`; the schema check makes a type mismatch a corrupt file.
pub(crate) fn column<T: 'static>(
    batch: &RecordBatch,
    index: usize,
) -> Result<Column<'_, T>, ReadRefusal> {
    Ok(Column {
        name: batch.schema_ref().field(index).name(),
        array: downcast(batch.column(index))?,
    })
}

impl<'a, T> Column<'a, T> {
    pub(crate) fn name(&self) -> &'a str {
        self.name
    }

    fn out_of_range(&self, value: impl Into<i128>) -> RowCause {
        RowCause::OutOfRange {
            column: self.name.to_string(),
            value: value.into(),
        }
    }
}

impl<T> std::ops::Deref for Column<'_, T> {
    type Target = T;

    fn deref(&self) -> &T {
        self.array
    }
}

impl Column<'_, Decimal128Array> {
    /// The stored decimal's unscaled integer as `N`, refused when it does not fit.
    pub(crate) fn integer<N: TryFrom<i128>>(&self, row: usize) -> Result<N, RowCause> {
        let value = self.array.value(row);
        N::try_from(value).map_err(|_| self.out_of_range(value))
    }

    pub(crate) fn price(&self, row: usize) -> Result<Price, RowCause> {
        Price::from_ticks(self.integer(row)?).map_err(RowCause::Price)
    }
}

impl Column<'_, TimestampMicrosecondArray> {
    /// The stored instant, refused unless it lies in `session`.
    pub(crate) fn instant_in(
        &self,
        row: usize,
        session: SessionDate,
    ) -> Result<DateTime<Utc>, RowCause> {
        let micros = self.array.value(row);
        let timestamp =
            DateTime::from_timestamp_micros(micros).ok_or_else(|| self.out_of_range(micros))?;
        match SessionDate::at(timestamp) == session {
            true => Ok(timestamp),
            false => Err(RowCause::OutsideSession { timestamp, session }),
        }
    }
}

/// The refusal of a group stored whole or not at all, naming each `(name, valid)` column that is null.
pub(crate) fn partly_null(columns: &[(&str, bool)]) -> RowCause {
    RowCause::PartlyNull {
        null: columns
            .iter()
            .filter(|(_, valid)| !valid)
            .map(|(name, _)| name.to_string())
            .collect(),
    }
}

/// `units` as a thirty-eight-digit decimal's unscaled integer, `None` past what one holds.
pub(crate) fn widest_decimal(units: u128) -> Option<i128> {
    i128::try_from(units)
        .ok()
        .filter(|units| *units < 10_i128.pow(38))
}

/// Why a bar was not placed in a file.
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum PlacementRefusal {
    /// A bar whose interval or session is not the key's.
    #[error("{symbol} at {timestamp} lies outside the key")]
    OutsideKey {
        symbol: Symbol,
        timestamp: DateTime<Utc>,
    },
    /// Two bars for one symbol and instant.
    #[error("{symbol} has two bars at {timestamp}")]
    Duplicate {
        symbol: Symbol,
        timestamp: DateTime<Utc>,
    },
}

/// Why bars of any layout (vendor, quote or trade) were not written under a key.
#[derive(Debug, Clone, PartialEq)]
pub enum EncodeRefusal {
    /// The subscription belongs to another provider than the key's.
    SubscriptionProvider {
        provenance: Provenance,
        key: Provider,
    },
    Placement(PlacementRefusal),
    /// A total past what its column's decimal holds.
    Unrepresentable {
        symbol: Symbol,
        timestamp: DateTime<Utc>,
    },
    Parquet {
        reason: String,
    },
}

/// Why a file was not read as bars of any layout.
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum DecodeRefusal {
    #[error("{0}")]
    File(ReadRefusal),
    /// A row that no longer passes the domain's own checks.
    #[error("row {index} refused: {cause}")]
    Row { index: usize, cause: RowCause },
    /// Provenance naming another provider than the key's.
    #[error("the file was fetched under {} but the key names {key}", .provenance.subscription())]
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

/// `bars` ordered by symbol and then timestamp, so the same bars always make the same bytes, each one checked to
/// belong under a key of `interval` and `session` and to be the only bar at its symbol and instant.
pub(crate) fn place<B>(
    bars: &[B],
    interval: BarInterval,
    session: SessionDate,
    stamp: impl Fn(&B) -> (&Symbol, BarInterval, DateTime<Utc>),
) -> Result<Vec<&B>, PlacementRefusal> {
    let mut ordered: Vec<&B> = bars.iter().collect();
    ordered.sort_by(|left, right| {
        let (left, right) = (stamp(left), stamp(right));
        (left.0, left.2).cmp(&(right.0, right.2))
    });
    let mut previous = None;
    for bar in ordered.iter().copied() {
        let (symbol, bar_interval, timestamp) = stamp(bar);
        if bar_interval != interval || SessionDate::at(timestamp) != session {
            return Err(PlacementRefusal::OutsideKey {
                symbol: symbol.clone(),
                timestamp,
            });
        }
        if previous == Some((symbol, timestamp)) {
            return Err(PlacementRefusal::Duplicate {
                symbol: symbol.clone(),
                timestamp,
            });
        }
        previous = Some((symbol, timestamp));
    }
    Ok(ordered)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_a_row_cause_reads_as_the_refusal_it_carries() {
        assert_eq!(
            RowCause::Symbol(SymbolRefusal::Malformed {
                raw: "brk.b".to_string()
            })
            .to_string(),
            "`brk.b` is not a ticker"
        );
        assert_eq!(
            partly_null(&[("opened_at", true), ("closed_at", false)]).to_string(),
            "closed_at null alone"
        );
    }
}
