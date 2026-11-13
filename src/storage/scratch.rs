//! Databento rows a study pulls into the profile bucket's `scratch/` prefix: one gzipped CSV per dataset, schema, symbol
//! and session, as served, beside a sidecar naming the request. Nothing here is archive, so no `Key` names it.

use std::io::{BufRead, BufReader};

use chrono::{DateTime, Datelike, Utc};
use flate2::read::GzDecoder;
use serde::{Deserialize, Serialize};

use super::{Bucket, BucketError};
use crate::common::journal::Commit;
use crate::common::market::{Dollars, Symbol};
use crate::common::storage::{ObjectKey, StorageClass};
use crate::common::time::SessionDate;
use crate::ingest::databento::{AvailableSession, Dataset, KNOWN_AT_COLUMN, Schema};

/// The scratch place of one symbol's rows over one session; rows and sidecar sit side by side under it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScratchKey {
    dataset: Dataset,
    schema: Schema,
    symbol: Symbol,
    session: SessionDate,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ScratchFile {
    Rows,
    Sidecar,
}

struct ScratchObject<'a> {
    key: &'a ScratchKey,
    file: ScratchFile,
}

impl ScratchKey {
    pub fn new(available: &AvailableSession, schema: Schema, symbol: Symbol) -> Self {
        Self {
            dataset: available.dataset().clone(),
            schema,
            symbol,
            session: available.session(),
        }
    }

    fn object(&self, file: ScratchFile) -> ScratchObject<'_> {
        ScratchObject { key: self, file }
    }
}

impl ObjectKey for ScratchObject<'_> {
    fn path(&self) -> String {
        let key = self.key;
        let date = key.session.date();
        let name = match self.file {
            ScratchFile::Rows => "rows.csv.gz",
            ScratchFile::Sidecar => "sidecar.json",
        };
        format!(
            "scratch/databento/dataset={}/schema={}/symbol={}/year={}/month={:02}/day={:02}/{name}",
            key.dataset.as_str(),
            key.schema,
            key.symbol.as_str(),
            date.year(),
            date.month(),
            date.day(),
        )
    }

    fn storage_class(&self) -> StorageClass {
        StorageClass::Standard
    }
}

/// What a scratch file holds and the request that fetched it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Sidecar {
    dataset: Dataset,
    schema: Schema,
    symbol: Symbol,
    session: SessionDate,
    /// The half-open UTC interval requested: the session's Eastern calendar day.
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    /// What `get_cost` priced the request at, in millionths of a dollar, rounded up.
    request_cost_millionths: Dollars,
    fetched_at: DateTime<Utc>,
    /// The column holding each row's `known_at`, which `land` checked the header names.
    known_at_column: String,
    rows: u64,
    commit: Option<Commit>,
}

/// Why rows were not landed.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum LandRefusal {
    #[error("the rows did not read as gzipped lines: {reason}")]
    Unreadable { reason: String },
    #[error("the header `{header}` names no `known_at` column")]
    NoKnownAt { header: String },
    #[error(transparent)]
    Bucket(#[from] BucketError),
    #[error("the sidecar did not encode: {reason}")]
    Sidecar { reason: String },
}

/// Whether the key's sidecar is written, which `land` writes only after its rows.
pub async fn landed(bucket: &Bucket, key: &ScratchKey) -> Result<bool, BucketError> {
    Ok(bucket
        .tag(&key.object(ScratchFile::Sidecar))
        .await?
        .is_some())
}

/// Writes the rows, then their sidecar, refusing rows whose header names no `known_at` column.
pub async fn land(
    bucket: &Bucket,
    key: &ScratchKey,
    gzipped: Vec<u8>,
    cost: Dollars,
    fetched_at: DateTime<Utc>,
    commit: Option<Commit>,
) -> Result<Sidecar, LandRefusal> {
    let rows = count_rows(&gzipped)?;
    bucket.put(&key.object(ScratchFile::Rows), gzipped).await?;
    let (start, end) = key.session.bounds();
    let sidecar = Sidecar {
        dataset: key.dataset.clone(),
        schema: key.schema,
        symbol: key.symbol.clone(),
        session: key.session,
        start,
        end,
        request_cost_millionths: cost,
        fetched_at,
        known_at_column: KNOWN_AT_COLUMN.to_string(),
        rows,
        commit,
    };
    let body = serde_json::to_vec(&sidecar).map_err(|error| LandRefusal::Sidecar {
        reason: error.to_string(),
    })?;
    bucket.put(&key.object(ScratchFile::Sidecar), body).await?;
    Ok(sidecar)
}

/// The data lines beneath a header that names the `known_at` column, read without holding the CSV uncompressed.
fn count_rows(gzipped: &[u8]) -> Result<u64, LandRefusal> {
    let unreadable = |error: std::io::Error| LandRefusal::Unreadable {
        reason: error.to_string(),
    };
    let mut reader = BufReader::new(GzDecoder::new(gzipped));
    let mut line = Vec::new();
    reader.read_until(b'\n', &mut line).map_err(unreadable)?;
    let header = String::from_utf8_lossy(&line).trim_end().to_string();
    if !header.split(',').any(|column| column == KNOWN_AT_COLUMN) {
        return Err(LandRefusal::NoKnownAt { header });
    }
    let mut rows = 0;
    loop {
        line.clear();
        match reader.read_until(b'\n', &mut line).map_err(unreadable)? {
            0 => return Ok(rows),
            _ if line.trim_ascii().is_empty() => {}
            _ => rows += 1,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use flate2::Compression;
    use flate2::write::GzEncoder;

    use super::*;

    fn gzip(text: &str) -> Vec<u8> {
        let mut encoder = GzEncoder::new(Vec::new(), Compression::fast());
        encoder.write_all(text.as_bytes()).unwrap();
        encoder.finish().unwrap()
    }

    /// Synthetic rows: only the header naming `ts_recv` is relied on, and no vendor row is reproduced.
    #[test]
    fn test_rows_are_counted_beneath_a_header_naming_known_at() {
        let body = "ts_recv,ts_event,price,symbol\n1,1,100,AAA\n2,2,101,AAA\n\n3,3,102,AAA";
        assert_eq!(count_rows(&gzip(body)), Ok(3));
        assert_eq!(count_rows(&gzip("ts_event,ts_recv\n")), Ok(0));
        assert_eq!(
            count_rows(&gzip("ts_event,price\n1,100\n")),
            Err(LandRefusal::NoKnownAt {
                header: "ts_event,price".to_string()
            })
        );
        assert_eq!(
            count_rows(&gzip("")),
            Err(LandRefusal::NoKnownAt {
                header: String::new()
            })
        );
        assert!(matches!(
            count_rows(b"not gzip"),
            Err(LandRefusal::Unreadable { .. })
        ));
    }

    /// The rows and sidecar of one key sit side by side under the profile bucket's `scratch/`, by session date.
    #[test]
    fn test_a_scratch_key_is_partitioned_by_dataset_schema_symbol_and_session() {
        let key = ScratchKey {
            dataset: Dataset::new("XNAS.ITCH").unwrap(),
            schema: Schema::TenLevels,
            symbol: Symbol::new("BRK.B").unwrap(),
            session: SessionDate::from_date("2026-03-09".parse().unwrap()),
        };
        let folder = "scratch/databento/dataset=XNAS.ITCH/schema=mbp-10/symbol=BRK.B/year=2026/month=03/day=09";
        assert_eq!(
            key.object(ScratchFile::Rows).path(),
            format!("{folder}/rows.csv.gz")
        );
        assert_eq!(
            key.object(ScratchFile::Sidecar).path(),
            format!("{folder}/sidecar.json")
        );
    }
}
