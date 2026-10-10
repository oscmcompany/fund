//! The fund's S3 buckets: the shared market-data archive and each profile's records, with objects written under their
//! `Key`, checked by S3 against a SHA-256 on upload and read back byte for byte before a write counts as done.

pub mod bars;
pub mod journal;
pub mod logs;
pub mod parquet;
pub mod quote_bars;
pub mod raw;
pub mod reference;
pub mod trade_bars;

use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{ChecksumAlgorithm, ChecksumMode};

use crate::common::storage::{EntityTag, ObjectKey, SeriesPrefix, StorageClass};
use crate::ingest::VariableRefusal;

/// One S3 bucket the fund writes: the shared market data or a profile's records.
#[derive(Clone)]
pub struct Archive {
    s3_client: aws_sdk_s3::Client,
    bucket_name: String,
}

/// Why a write or read did not complete.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ArchiveError {
    #[error("writing {path} failed: {reason}")]
    Put { path: String, reason: String },
    #[error("reading {path} failed: {reason}")]
    Get { path: String, reason: String },
    #[error("listing {series} failed: {reason}")]
    List {
        series: SeriesPrefix,
        reason: String,
    },
    #[error("deleting {path} failed: {reason}")]
    Delete { path: String, reason: String },
    /// A create found the key already written, or a replace found it changed since it was read.
    #[error("{path} was written by someone else first; read it again")]
    Contended { path: String },
    /// The key's storage class cannot be read back, so this path cannot verify it; raw ticks go through `raw`.
    #[error("{path} lands in Deep Archive, which cannot be read back")]
    Unverifiable { path: String },
    #[error("{path} read back {read} bytes where {written} were written")]
    ReadBackMismatch {
        path: String,
        written: usize,
        read: usize,
    },
    #[error("{path} was absent when read back after {written} bytes were written")]
    VanishedAfterWrite { path: String, written: usize },
}

/// Why a file of any layout was not encoded, kept as the layout's own refusal.
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum EncodeRefusal {
    /// Vendor, quote and trade bars share one refusal, so the layout is named by the key the caller holds.
    #[error("bars not encoded: {0:?}")]
    Bars(parquet::EncodeRefusal),
    #[error("reference table not encoded: {0:?}")]
    Reference(reference::ReferenceRefusal),
    #[error("journal not encoded: {0:?}")]
    Journal(journal::EncodeRefusal),
    #[error("logs not encoded: {0:?}")]
    Logs(logs::EncodeRefusal),
}

/// Why a file of any layout was not decoded, kept as the layout's own refusal.
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum DecodeRefusal {
    /// Quote and trade bars share one refusal, so the layout is named by the key the caller holds.
    #[error("bars not decoded: {0}")]
    Bars(parquet::DecodeRefusal),
    /// Vendor bars, at any interval, add the refusal of a file holding none.
    #[error("bars not decoded: {0}")]
    VendorBars(bars::DecodeRefusal),
    #[error("reference table not decoded: {0:?}")]
    Reference(reference::ReferenceRefusal),
    #[error("journal not decoded: {0:?}")]
    Journal(journal::DecodeRefusal),
}

/// Wraps each layout's refusal into the one enum that keeps it, so `?` reaches it through `From`.
macro_rules! wrap_refusal {
    ($($outer:ident :: $variant:ident ($inner:ty)),* $(,)?) => {$(
        impl From<$inner> for $outer {
            fn from(refusal: $inner) -> Self {
                Self::$variant(refusal)
            }
        }
    )*};
}

wrap_refusal!(
    EncodeRefusal::Bars(parquet::EncodeRefusal),
    EncodeRefusal::Reference(reference::ReferenceRefusal),
    EncodeRefusal::Journal(journal::EncodeRefusal),
    EncodeRefusal::Logs(logs::EncodeRefusal),
    DecodeRefusal::Bars(parquet::DecodeRefusal),
    DecodeRefusal::VendorBars(bars::DecodeRefusal),
    DecodeRefusal::Reference(reference::ReferenceRefusal),
    DecodeRefusal::Journal(journal::DecodeRefusal),
);

impl Archive {
    /// The shared market-data archive named by `AWS_S3_ARCHIVE_BUCKET_NAME`, which only the archiver writes.
    pub fn market_data(configuration: &aws_config::SdkConfig) -> Result<Self, VariableRefusal> {
        Self::named(configuration, "AWS_S3_ARCHIVE_BUCKET_NAME")
    }

    /// This profile's journals and logs, named by `AWS_S3_RECORDS_BUCKET_NAME`.
    pub fn records(configuration: &aws_config::SdkConfig) -> Result<Self, VariableRefusal> {
        Self::named(configuration, "AWS_S3_RECORDS_BUCKET_NAME")
    }

    fn named(
        configuration: &aws_config::SdkConfig,
        variable: &'static str,
    ) -> Result<Self, VariableRefusal> {
        Ok(Self {
            s3_client: aws_sdk_s3::Client::new(configuration),
            bucket_name: crate::ingest::variable(variable)?,
        })
    }

    /// Writes `body` under `key` and returns once the same bytes have been read back.
    pub async fn put(&self, key: &impl ObjectKey, body: Vec<u8>) -> Result<(), ArchiveError> {
        self.write(key, body, Condition::Any).await
    }

    /// Writes `body` only if nothing is under `key` yet, so two writers can never both take it.
    pub async fn create(&self, key: &impl ObjectKey, body: Vec<u8>) -> Result<(), ArchiveError> {
        self.write(key, body, Condition::Absent).await
    }

    /// Writes `body` only if `key` still holds the version `tag` names.
    pub async fn replace(
        &self,
        key: &impl ObjectKey,
        body: Vec<u8>,
        tag: &EntityTag,
    ) -> Result<(), ArchiveError> {
        self.write(key, body, Condition::Unchanged(tag)).await
    }

    /// Writes and verifies `body`, logging the outcome here so no caller has to.
    async fn write(
        &self,
        key: &impl ObjectKey,
        body: Vec<u8>,
        condition: Condition<'_>,
    ) -> Result<(), ArchiveError> {
        let bytes = body.len();
        let condition_name: &'static str = condition.into();
        let written = self.write_verified(key, body, condition).await;
        match &written {
            Ok(()) => {
                tracing::debug!(
                    path = key.path(),
                    bytes,
                    condition = condition_name,
                    "Wrote an object"
                )
            }
            Err(error) => {
                tracing::warn!(path = key.path(), bytes, condition = condition_name, %error, "Object not written")
            }
        }
        written
    }

    async fn write_verified(
        &self,
        key: &impl ObjectKey,
        body: Vec<u8>,
        condition: Condition<'_>,
    ) -> Result<(), ArchiveError> {
        let path = key.path();
        match key.storage_class() {
            StorageClass::Standard => {}
            StorageClass::DeepArchive => return Err(ArchiveError::Unverifiable { path }),
        }
        let request = self
            .s3_client
            .put_object()
            .bucket(&self.bucket_name)
            .key(&path)
            .checksum_algorithm(ChecksumAlgorithm::Sha256)
            .body(ByteStream::from(body.clone()));
        let request = match condition {
            Condition::Any => request,
            Condition::Absent => request.if_none_match("*"),
            Condition::Unchanged(tag) => request.if_match(tag.as_str()),
        };
        request.send().await.map_err(|error| {
            // 412 is a precondition that failed; 409 is a conditional write that raced another.
            match error
                .raw_response()
                .map(|response| response.status().as_u16())
            {
                Some(409 | 412) => ArchiveError::Contended { path: path.clone() },
                Some(_) | None => ArchiveError::Put {
                    path: path.clone(),
                    reason: aws_sdk_s3::error::DisplayErrorContext(error).to_string(),
                },
            }
        })?;
        read_back(path, &body, self.get(key).await?)
    }

    /// Deletes the object under `key`; deleting one already gone succeeds, as S3 answers it.
    pub async fn delete(&self, key: &impl ObjectKey) -> Result<(), ArchiveError> {
        let path = key.path();
        match self
            .s3_client
            .delete_object()
            .bucket(&self.bucket_name)
            .key(&path)
            .send()
            .await
        {
            Ok(_) => {
                tracing::debug!(path, "Deleted an object");
                Ok(())
            }
            Err(error) => {
                let error = ArchiveError::Delete {
                    path: path.clone(),
                    reason: aws_sdk_s3::error::DisplayErrorContext(error).to_string(),
                };
                tracing::warn!(path, %error, "Object not deleted");
                Err(error)
            }
        }
    }

    /// Every path under `series`, across as many pages as S3 answers with.
    pub async fn list(&self, series: &SeriesPrefix) -> Result<Vec<String>, ArchiveError> {
        let mut pages = self
            .s3_client
            .list_objects_v2()
            .bucket(&self.bucket_name)
            .prefix(series.as_str())
            .into_paginator()
            .send();
        let mut paths = Vec::new();
        while let Some(page) = pages.next().await {
            let page = page.map_err(|error| ArchiveError::List {
                series: series.clone(),
                reason: aws_sdk_s3::error::DisplayErrorContext(error).to_string(),
            })?;
            paths.extend(
                page.contents()
                    .iter()
                    .filter_map(|object| object.key().map(String::from)),
            );
        }
        Ok(paths)
    }

    /// The object under `key`, or `None` when nothing is there; S3 verifies the stored checksum as it streams.
    pub async fn get(&self, key: &impl ObjectKey) -> Result<Option<Vec<u8>>, ArchiveError> {
        Ok(self.get_tagged(key).await?.map(|(body, _)| body))
    }

    /// The tag of the version under `key` now, without reading it; `None` when the object is gone.
    pub async fn tag(&self, key: &impl ObjectKey) -> Result<Option<EntityTag>, ArchiveError> {
        let path = key.path();
        let failed = |reason: String| ArchiveError::Get {
            path: path.clone(),
            reason,
        };
        match self
            .s3_client
            .head_object()
            .bucket(&self.bucket_name)
            .key(&path)
            .send()
            .await
        {
            Ok(response) => Ok(Some(EntityTag::new(
                response
                    .e_tag()
                    .ok_or_else(|| failed("no entity tag".to_string()))?,
            ))),
            Err(error)
                if error
                    .as_service_error()
                    .is_some_and(|error| error.is_not_found()) =>
            {
                Ok(None)
            }
            Err(error) => Err(failed(
                aws_sdk_s3::error::DisplayErrorContext(error).to_string(),
            )),
        }
    }

    /// The object under `key` with the tag of the version read, which a `replace` must still match.
    pub async fn get_tagged(
        &self,
        key: &impl ObjectKey,
    ) -> Result<Option<(Vec<u8>, EntityTag)>, ArchiveError> {
        let path = key.path();
        let failed = |reason: String| ArchiveError::Get {
            path: path.clone(),
            reason,
        };
        let response = match self
            .s3_client
            .get_object()
            .bucket(&self.bucket_name)
            .key(&path)
            .checksum_mode(ChecksumMode::Enabled)
            .send()
            .await
        {
            Ok(response) => response,
            Err(error)
                if error
                    .as_service_error()
                    .is_some_and(|error| error.is_no_such_key()) =>
            {
                return Ok(None);
            }
            Err(error) => {
                return Err(failed(
                    aws_sdk_s3::error::DisplayErrorContext(error).to_string(),
                ));
            }
        };
        let tag = EntityTag::new(
            response
                .e_tag()
                .ok_or_else(|| failed("no entity tag".to_string()))?,
        );
        let body = response
            .body
            .collect()
            .await
            .map_err(|error| failed(error.to_string()))?;
        Ok(Some((body.into_bytes().to_vec(), tag)))
    }
}

/// Whether `read` is the `written` body, with an absent object told apart from a short one.
fn read_back(path: String, written: &[u8], read: Option<Vec<u8>>) -> Result<(), ArchiveError> {
    match read {
        Some(read) if read == written => Ok(()),
        Some(read) => Err(ArchiveError::ReadBackMismatch {
            path,
            written: written.len(),
            read: read.len(),
        }),
        None => Err(ArchiveError::VanishedAfterWrite {
            path,
            written: written.len(),
        }),
    }
}

#[derive(Clone, Copy, strum::IntoStaticStr)]
#[strum(serialize_all = "snake_case")]
enum Condition<'a> {
    Any,
    Absent,
    Unchanged(&'a EntityTag),
}

#[cfg(test)]
mod tests {
    use chrono::{NaiveDate, Utc};
    use uuid::Uuid;

    use super::bars::{Provenance, Subscription};
    use super::*;
    use crate::common::journal::RunId;
    use crate::common::storage::{Key, Provider};
    use crate::common::time::SessionDate;
    use crate::ingest::massive::Massive;

    #[tokio::test]
    async fn test_a_deep_archive_key_is_refused_before_any_request() {
        let configuration = aws_sdk_s3::Config::builder()
            .behavior_version_latest()
            .region(aws_sdk_s3::config::Region::new("us-east-1"))
            .build();
        let archive = Archive {
            s3_client: aws_sdk_s3::Client::from_conf(configuration),
            bucket_name: "unused".to_string(),
        };
        let key = Key::RawQuotes {
            provider: Provider::Massive,
            session: SessionDate::from_date(NaiveDate::from_ymd_opt(2021, 8, 23).unwrap()),
        };
        assert_eq!(
            archive.put(&key, vec![1]).await,
            Err(ArchiveError::Unverifiable {
                path: "data/equity/stage=raw/quotes/provider=massive/year=2021/month=08/day=23/data.csv.gz"
                    .to_string()
            })
        );
    }

    #[test]
    fn test_an_absent_read_back_is_not_an_empty_one() {
        let path = || "data/a.parquet".to_string();
        assert_eq!(read_back(path(), &[1, 2], Some(vec![1, 2])), Ok(()));
        assert_eq!(
            read_back(path(), &[1, 2], Some(vec![])),
            Err(ArchiveError::ReadBackMismatch {
                path: path(),
                written: 2,
                read: 0
            })
        );
        assert_eq!(
            read_back(path(), &[1, 2], None),
            Err(ArchiveError::VanishedAfterWrite {
                path: path(),
                written: 2
            })
        );
    }

    /// The first real object of the new archive: the key the archiver would own for this session anyway.
    #[tokio::test]
    #[ignore = "writes one real object to the shared archive bucket; run once, deliberately, under secretspec"]
    async fn live_writes_and_reads_back_one_real_key() {
        let session = SessionDate::from_date(NaiveDate::from_ymd_opt(2026, 9, 29).unwrap());
        let key = crate::common::heal::massive_daily_bars(session);
        let daily = Massive::from_environment(reqwest::Client::new())
            .unwrap()
            .grouped_daily(session)
            .await
            .unwrap();
        let provenance = Provenance::new(
            Subscription::StocksStarter,
            Utc::now(),
            RunId::new(Uuid::new_v4()),
            None,
        );
        let written =
            crate::common::market::record::BarPartition::try_from(daily.bars().to_vec()).unwrap();
        let body = bars::encode(&key, &written, &provenance).unwrap();
        let configuration = aws_config::load_from_env().await;
        let archive = Archive::market_data(&configuration).unwrap();
        archive.put(&Key::from(key), body.clone()).await.unwrap();
        let (bars, read) =
            bars::decode(&key, archive.get(&Key::from(key)).await.unwrap().unwrap()).unwrap();
        println!(
            "{} bars, {} bytes, {}",
            bars.bars().len(),
            body.len(),
            Key::from(key).path()
        );
        assert_eq!(bars, written);
        assert_eq!(read, provenance);
    }
}
