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

use crate::common::storage::{Key, StorageClass};
use crate::ingest::VariableRefusal;

/// One S3 bucket the fund writes: the shared market data or a profile's records.
#[derive(Clone)]
pub struct Archive {
    s3_client: aws_sdk_s3::Client,
    bucket_name: String,
}

/// Why a write or read did not complete.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ArchiveError {
    Put {
        path: String,
        reason: String,
    },
    Get {
        path: String,
        reason: String,
    },
    List {
        prefix: String,
        reason: String,
    },
    Delete {
        path: String,
        reason: String,
    },
    /// A create found the key already written, or a replace found it changed since it was read.
    Contended {
        path: String,
    },
    /// The key's storage class cannot be read back, so this path cannot verify it; raw ticks go through `raw`.
    Unverifiable {
        path: String,
    },
    /// Read back different bytes than were written.
    ReadBackMismatch {
        path: String,
        written: usize,
        read: usize,
    },
}

impl std::fmt::Display for ArchiveError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Put { path, reason } => write!(formatter, "writing {path} failed: {reason}"),
            Self::Get { path, reason } => write!(formatter, "reading {path} failed: {reason}"),
            Self::List { prefix, reason } => write!(formatter, "listing {prefix} failed: {reason}"),
            Self::Delete { path, reason } => write!(formatter, "deleting {path} failed: {reason}"),
            Self::Contended { path } => {
                write!(
                    formatter,
                    "{path} was written by someone else first; read it again"
                )
            }
            Self::Unverifiable { path } => {
                write!(
                    formatter,
                    "{path} lands in Deep Archive, which cannot be read back"
                )
            }
            Self::ReadBackMismatch {
                path,
                written,
                read,
            } => write!(
                formatter,
                "{path} read back {read} bytes where {written} were written"
            ),
        }
    }
}

impl std::error::Error for ArchiveError {}

/// Why a file of any layout was not encoded, kept as the layout's own refusal.
#[derive(Debug, Clone, PartialEq)]
pub enum EncodeRefusal {
    Bars(bars::EncodeRefusal),
    QuoteBars(quote_bars::EncodeRefusal),
    TradeBars(trade_bars::EncodeRefusal),
    Reference(reference::ReferenceRefusal),
    Journal(journal::EncodeRefusal),
    Logs(logs::EncodeRefusal),
}

/// Why a file of any layout was not decoded, kept as the layout's own refusal.
#[derive(Debug, Clone, PartialEq)]
pub enum DecodeRefusal {
    Bars(bars::DecodeRefusal),
    QuoteBars(quote_bars::DecodeRefusal),
    TradeBars(trade_bars::DecodeRefusal),
    Reference(reference::ReferenceRefusal),
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
    EncodeRefusal::Bars(bars::EncodeRefusal),
    EncodeRefusal::QuoteBars(quote_bars::EncodeRefusal),
    EncodeRefusal::TradeBars(trade_bars::EncodeRefusal),
    EncodeRefusal::Journal(journal::EncodeRefusal),
    EncodeRefusal::Logs(logs::EncodeRefusal),
    DecodeRefusal::Bars(bars::DecodeRefusal),
    DecodeRefusal::QuoteBars(quote_bars::DecodeRefusal),
    DecodeRefusal::TradeBars(trade_bars::DecodeRefusal),
    DecodeRefusal::Journal(journal::DecodeRefusal),
);

impl std::fmt::Display for EncodeRefusal {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Bars(refusal) => write!(formatter, "bars not encoded: {refusal:?}"),
            Self::QuoteBars(refusal) => write!(formatter, "quote bars not encoded: {refusal:?}"),
            Self::TradeBars(refusal) => write!(formatter, "trade bars not encoded: {refusal:?}"),
            Self::Reference(refusal) => {
                write!(formatter, "reference table not encoded: {refusal:?}")
            }
            Self::Journal(refusal) => write!(formatter, "journal not encoded: {refusal:?}"),
            Self::Logs(refusal) => write!(formatter, "logs not encoded: {refusal:?}"),
        }
    }
}

impl std::error::Error for EncodeRefusal {}

impl std::fmt::Display for DecodeRefusal {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Bars(refusal) => write!(formatter, "bars not decoded: {refusal:?}"),
            Self::QuoteBars(refusal) => write!(formatter, "quote bars not decoded: {refusal:?}"),
            Self::TradeBars(refusal) => write!(formatter, "trade bars not decoded: {refusal:?}"),
            Self::Reference(refusal) => {
                write!(formatter, "reference table not decoded: {refusal:?}")
            }
            Self::Journal(refusal) => write!(formatter, "journal not decoded: {refusal:?}"),
        }
    }
}

impl std::error::Error for DecodeRefusal {}

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
    pub async fn put(&self, key: &Key, body: Vec<u8>) -> Result<(), ArchiveError> {
        self.write(key, body, Condition::Any).await
    }

    /// Writes `body` only if nothing is under `key` yet, so two writers can never both take it.
    pub async fn create(&self, key: &Key, body: Vec<u8>) -> Result<(), ArchiveError> {
        self.write(key, body, Condition::Absent).await
    }

    /// Writes `body` only if `key` still holds the version `tag` names.
    pub async fn replace(&self, key: &Key, body: Vec<u8>, tag: &Tag) -> Result<(), ArchiveError> {
        self.write(key, body, Condition::Unchanged(tag)).await
    }

    async fn write(
        &self,
        key: &Key,
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
            Condition::Unchanged(tag) => request.if_match(&tag.0),
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
        match self.get(key).await? {
            Some(read) if read == body => Ok(()),
            read => Err(ArchiveError::ReadBackMismatch {
                path,
                written: body.len(),
                read: read.map_or(0, |read| read.len()),
            }),
        }
    }

    /// Deletes the object under `key`; deleting one already gone succeeds, as S3 answers it.
    pub async fn delete(&self, key: &Key) -> Result<(), ArchiveError> {
        let path = key.path();
        self.s3_client
            .delete_object()
            .bucket(&self.bucket_name)
            .key(&path)
            .send()
            .await
            .map_err(|error| ArchiveError::Delete {
                path,
                reason: aws_sdk_s3::error::DisplayErrorContext(error).to_string(),
            })?;
        Ok(())
    }

    /// Every path under `prefix`, across as many pages as S3 answers with.
    pub async fn list(&self, prefix: &str) -> Result<Vec<String>, ArchiveError> {
        let mut pages = self
            .s3_client
            .list_objects_v2()
            .bucket(&self.bucket_name)
            .prefix(prefix)
            .into_paginator()
            .send();
        let mut paths = Vec::new();
        while let Some(page) = pages.next().await {
            let page = page.map_err(|error| ArchiveError::List {
                prefix: prefix.to_string(),
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
    pub async fn get(&self, key: &Key) -> Result<Option<Vec<u8>>, ArchiveError> {
        Ok(self.get_tagged(key).await?.map(|(body, _)| body))
    }

    /// The tag of the version under `key` now, without reading it; `None` when the object is gone.
    pub async fn tag(&self, key: &Key) -> Result<Option<Tag>, ArchiveError> {
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
            Ok(response) => Ok(Some(Tag(response
                .e_tag()
                .ok_or_else(|| failed("no entity tag".to_string()))?
                .to_string()))),
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
    pub async fn get_tagged(&self, key: &Key) -> Result<Option<(Vec<u8>, Tag)>, ArchiveError> {
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
        let tag = Tag(response
            .e_tag()
            .ok_or_else(|| failed("no entity tag".to_string()))?
            .to_string());
        let body = response
            .body
            .collect()
            .await
            .map_err(|error| failed(error.to_string()))?;
        Ok(Some((body.into_bytes().to_vec(), tag)))
    }
}

/// The version of an object a read saw.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Tag(String);

impl Tag {
    /// The entity tag S3 answered for a version.
    pub fn new(raw: &str) -> Self {
        Self(raw.to_string())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

enum Condition<'a> {
    Any,
    Absent,
    Unchanged(&'a Tag),
}

#[cfg(test)]
mod tests {
    use chrono::{NaiveDate, Utc};
    use uuid::Uuid;

    use super::bars::{Provenance, Subscription, decode, encode};
    use super::*;
    use crate::common::journal::RunId;
    use crate::common::market::record::BarInterval;
    use crate::common::storage::{Origin, Provider};
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

    /// The first real object of the new archive: the key the archiver would own for this session anyway.
    #[tokio::test]
    #[ignore = "writes one real object to the shared archive bucket; run once, deliberately, under secretspec"]
    async fn live_writes_and_reads_back_one_real_key() {
        let session = SessionDate::from_date(NaiveDate::from_ymd_opt(2026, 9, 29).unwrap());
        let key = Key::Bars {
            provider: Provider::Massive,
            origin: Origin::Vendor,
            interval: BarInterval::OneDay,
            session,
        };
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
        let body = encode(&key, daily.bars(), &provenance).unwrap();
        let configuration = aws_config::load_from_env().await;
        let archive = Archive::market_data(&configuration).unwrap();
        archive.put(&key, body.clone()).await.unwrap();
        let (bars, read) = decode(&key, archive.get(&key).await.unwrap().unwrap()).unwrap();
        println!("{} bars, {} bytes, {}", bars.len(), body.len(), key.path());
        assert_eq!(bars, daily.bars());
        assert_eq!(read, provenance);
    }
}
