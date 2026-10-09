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
use crate::common::time::SessionDate;

/// The metadata key every layout names its version under.
pub(crate) const LAYOUT_VERSION_KEY: &str = "fund.layout_version";

/// Why a file was not read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReadRefusal {
    Parquet {
        reason: String,
    },
    /// Columns other than the layout's, named as found.
    Schema {
        found: String,
    },
    Metadata {
        name: &'static str,
    },
    /// Written under another layout than this build reads.
    Layout {
        version: String,
    },
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

impl std::fmt::Display for ReadRefusal {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Parquet { reason } => {
                write!(formatter, "the file is not readable Parquet: {reason}")
            }
            Self::Schema { found } => write!(formatter, "the file holds other columns: {found}"),
            Self::Metadata { name } => {
                write!(formatter, "the metadata `{name}` is absent or unreadable")
            }
            Self::Layout { version } => {
                write!(formatter, "the file is written under layout {version}")
            }
        }
    }
}

impl std::error::Error for ReadRefusal {}

/// Why a row read back from a file no longer passes its domain's checks.
#[derive(Debug, Clone, PartialEq)]
pub enum RowCause {
    /// A stored number past what its column's domain type holds.
    OutOfRange {
        column: String,
        value: i128,
    },
    OutsideSession {
        timestamp: DateTime<Utc>,
        session: SessionDate,
    },
    /// Some columns of a group stored whole or not at all are null, named here.
    PartlyNull {
        null: Vec<String>,
    },
    /// Text that names no value of its column's type.
    Unparsable {
        column: String,
        raw: String,
    },
    Symbol(SymbolRefusal),
    Price(PriceRefusal),
    BarPrices(BarPricesRefusal),
    Bar(BarRefusal),
    QuoteSums(QuoteSumsRefusal),
    QuoteBar(QuoteBarRefusal),
    OpenClose(OpenCloseRefusal),
    HighLow(HighLowRefusal),
    TradeBar(TradeBarRefusal),
    ActionId(ActionIdRefusal),
    SplitRatio(SplitRatioRefusal),
    BoundaryChange(BoundaryChangeRefusal),
    SeriesBoundary(SeriesBoundaryRefusal),
    IndustryCode(IndustryCodeRefusal),
    MarketIdentifierCode(MarketIdentifierCodeRefusal),
    CentralIndexKey(CentralIndexKeyRefusal),
}

impl std::fmt::Display for RowCause {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::OutOfRange { column, value } => {
                write!(formatter, "{column} holds {value}, past its type's range")
            }
            Self::OutsideSession { timestamp, session } => {
                write!(formatter, "{timestamp} is outside session {session}")
            }
            Self::PartlyNull { null } => write!(formatter, "{} null alone", null.join(" and ")),
            Self::Unparsable { column, raw } => write!(formatter, "{column} holds `{raw}`"),
            Self::Symbol(refusal) => refusal.fmt(formatter),
            Self::Price(refusal) => refusal.fmt(formatter),
            Self::BarPrices(refusal) => refusal.fmt(formatter),
            Self::Bar(refusal) => refusal.fmt(formatter),
            Self::QuoteSums(refusal) => refusal.fmt(formatter),
            Self::QuoteBar(refusal) => refusal.fmt(formatter),
            Self::OpenClose(refusal) => refusal.fmt(formatter),
            Self::HighLow(refusal) => refusal.fmt(formatter),
            Self::TradeBar(refusal) => refusal.fmt(formatter),
            Self::ActionId(refusal) => refusal.fmt(formatter),
            Self::SplitRatio(refusal) => refusal.fmt(formatter),
            Self::BoundaryChange(refusal) => refusal.fmt(formatter),
            Self::SeriesBoundary(refusal) => refusal.fmt(formatter),
            Self::IndustryCode(refusal) => refusal.fmt(formatter),
            Self::MarketIdentifierCode(refusal) => refusal.fmt(formatter),
            Self::CentralIndexKey(refusal) => refusal.fmt(formatter),
        }
    }
}

impl std::error::Error for RowCause {}

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
#[derive(Debug, Clone, PartialEq)]
pub enum PlacementRefusal {
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
}

impl std::fmt::Display for PlacementRefusal {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::OutsideKey { symbol, timestamp } => {
                write!(formatter, "{symbol} at {timestamp} lies outside the key")
            }
            Self::Duplicate { symbol, timestamp } => {
                write!(formatter, "{symbol} has two bars at {timestamp}")
            }
        }
    }
}

impl std::error::Error for PlacementRefusal {}

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
