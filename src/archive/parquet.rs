//! One Parquet file written and read in memory, shared by every layout the buckets hold: a reader checks the exact
//! schema and the layout version before any row.

use std::sync::Arc;

use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::Schema;
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::basic::Compression;
use parquet::file::metadata::KeyValue;
use parquet::file::properties::WriterProperties;

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
    let version = value(&entries, LAYOUT_VERSION_KEY).ok_or(ReadRefusal::Metadata {
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
