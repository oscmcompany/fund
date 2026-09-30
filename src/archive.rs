//! The market-data archive in S3: objects written under their `Key`, checked by S3 against a SHA-256 on upload and
//! read back byte for byte before a write counts as done.

pub mod bars;

use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{ChecksumAlgorithm, ChecksumMode};

use crate::common::storage::Key;
use crate::ingest::MissingVariable;

pub struct Archive {
    s3_client: aws_sdk_s3::Client,
    bucket: String,
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

impl Archive {
    /// The shared archive bucket named by `AWS_S3_ARCHIVE_BUCKET_NAME`.
    pub fn from_environment(
        configuration: &aws_config::SdkConfig,
    ) -> Result<Self, MissingVariable> {
        Ok(Self {
            s3_client: aws_sdk_s3::Client::new(configuration),
            bucket: crate::ingest::variable("AWS_S3_ARCHIVE_BUCKET_NAME")?,
        })
    }

    /// Writes `body` under `key` and returns once the same bytes have been read back.
    pub async fn put(&self, key: &Key, body: Vec<u8>) -> Result<(), ArchiveError> {
        let path = key.path();
        self.s3_client
            .put_object()
            .bucket(&self.bucket)
            .key(&path)
            .checksum_algorithm(ChecksumAlgorithm::Sha256)
            .body(ByteStream::from(body.clone()))
            .send()
            .await
            .map_err(|error| ArchiveError::Put {
                path: path.clone(),
                reason: aws_sdk_s3::error::DisplayErrorContext(error).to_string(),
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

    /// The object under `key`, or `None` when nothing is there; S3 verifies the stored checksum as it streams.
    pub async fn get(&self, key: &Key) -> Result<Option<Vec<u8>>, ArchiveError> {
        let path = key.path();
        let failed = |reason: String| ArchiveError::Get {
            path: path.clone(),
            reason,
        };
        let response = match self
            .s3_client
            .get_object()
            .bucket(&self.bucket)
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
        let body = response
            .body
            .collect()
            .await
            .map_err(|error| failed(error.to_string()))?;
        Ok(Some(body.into_bytes().to_vec()))
    }
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

    /// The first real object of the new archive: the key the archiver would own for this session anyway.
    #[tokio::test]
    #[ignore = "writes one real object to the shared archive bucket; run once, deliberately, under secretspec"]
    async fn live_writes_and_reads_back_one_real_key() {
        let session = SessionDate::from_date(NaiveDate::from_ymd_opt(2026, 9, 29).unwrap());
        let key = Key::Bars {
            provider: Provider::Massive,
            origin: Origin::Fetched,
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
        let archive = Archive::from_environment(&configuration).unwrap();
        archive.put(&key, body.clone()).await.unwrap();
        let (bars, read) = decode(&key, archive.get(&key).await.unwrap().unwrap()).unwrap();
        println!("{} bars, {} bytes, {}", bars.len(), body.len(), key.path());
        assert_eq!(bars, daily.bars());
        assert_eq!(read, provenance);
    }
}
