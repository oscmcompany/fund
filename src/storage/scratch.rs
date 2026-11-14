//! Databento rows a study pulls into the profile bucket's `scratch/` prefix: gzipped CSV per dataset, schema, symbol
//! and session, as served, beside a sidecar that claims the key for one fetch. Nothing here is archive, so no `Key`
//! names it.

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

/// The scratch place of one symbol's rows over one session; each fetch's rows and the one sidecar sit under it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScratchKey {
    dataset: Dataset,
    schema: Schema,
    symbol: Symbol,
    session: SessionDate,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ScratchFile {
    /// One fetch's rows, named by when it was fetched so overlapping fetches never overwrite each other.
    Rows {
        fetched_at: DateTime<Utc>,
    },
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
            ScratchFile::Rows { fetched_at } => {
                format!("rows-{}.csv.gz", fetched_at.format("%Y%m%dT%H%M%S%.9fZ"))
            }
            ScratchFile::Sidecar => "sidecar.json".to_string(),
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
    /// When the rows were fetched, which also names the rows file this sidecar describes.
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

/// Writes this fetch's rows, then creates the sidecar only if no other fetch has, refusing rows whose header names no
/// `known_at` column; rows are deleted when another fetch's sidecar already claims the key.
pub async fn land(
    bucket: &Bucket,
    key: &ScratchKey,
    gzipped: Vec<u8>,
    cost: Dollars,
    fetched_at: DateTime<Utc>,
    commit: Option<Commit>,
) -> Result<Sidecar, LandRefusal> {
    let rows = count_rows(&gzipped)?;
    let rows_object = key.object(ScratchFile::Rows { fetched_at });
    bucket.put(&rows_object, gzipped).await?;
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
    match bucket.create(&key.object(ScratchFile::Sidecar), body).await {
        Ok(()) => Ok(sidecar),
        Err(error @ BucketError::Contended { .. }) => {
            match bucket.delete(&rows_object).await {
                Ok(()) => {}
                Err(cleanup) => {
                    tracing::warn!(path = rows_object.path(), %cleanup, "Unclaimed rows not deleted")
                }
            }
            Err(LandRefusal::Bucket(error))
        }
        // Any other failure may have written the sidecar after all, so its rows stay where it names them.
        Err(error) => Err(LandRefusal::Bucket(error)),
    }
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

    /// A key's sidecar and each fetch's rows sit side by side under the profile bucket's `scratch/`, by session date.
    #[test]
    fn test_a_scratch_key_is_partitioned_by_dataset_schema_symbol_and_session() {
        let key = ScratchKey {
            dataset: Dataset::new("XNAS.ITCH").unwrap(),
            schema: Schema::TenLevels,
            symbol: Symbol::new("BRK.B").unwrap(),
            session: SessionDate::from_date("2026-03-09".parse().unwrap()),
        };
        let folder = "scratch/databento/dataset=XNAS.ITCH/schema=mbp-10/symbol=BRK.B/year=2026/month=03/day=09";
        let fetched = |raw: &str| ScratchFile::Rows {
            fetched_at: DateTime::parse_from_rfc3339(raw).unwrap().to_utc(),
        };
        assert_eq!(
            key.object(fetched("2026-11-13T15:04:05.000000001Z")).path(),
            format!("{folder}/rows-20261113T150405.000000001Z.csv.gz")
        );
        assert_eq!(
            key.object(fetched("2026-11-13T15:04:05.000000002Z")).path(),
            format!("{folder}/rows-20261113T150405.000000002Z.csv.gz")
        );
        assert_eq!(
            key.object(ScratchFile::Sidecar).path(),
            format!("{folder}/sidecar.json")
        );
    }
}
