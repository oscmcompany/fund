//! Vendor bytes stored as served: fetched response pages written a line each into one gzip stream, so no response set
//! is ever held whole, under a full-object CRC64 that S3 computes and any later copy of the same bytes must match.

use std::collections::HashMap;
use std::io::Write;
use std::time::Duration;

use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{
    ChecksumAlgorithm, ChecksumMode, ChecksumType, CompletedMultipartUpload, CompletedPart,
};
use chrono::{DateTime, Utc};

use super::provenance::{MetadataEntry, Provenance};
use super::{Bucket, BucketError};
use crate::common::storage::{EntityTag, Key, ReferenceKey, StorageClass};

/// The length a gzip part reaches before it is uploaded.
const PART_LENGTH: usize = 64 * 1024 * 1024;

/// Attempts at uploading one part before the upload is abandoned.
const PART_ATTEMPTS: u32 = 5;

/// What the bucket holds under a key: its length and the checksum S3 holds for it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Stored {
    length: u64,
    checksum: Checksum,
    /// When the vendor's bytes were fetched, from the object's metadata; `None` on a copy that never recorded it.
    fetched_at: Option<DateTime<Utc>>,
    /// The version read, which a conditional delete must still match.
    tag: EntityTag,
}

impl Stored {
    pub fn tag(&self) -> &EntityTag {
        &self.tag
    }

    pub fn fetched_at(&self) -> Option<DateTime<Utc>> {
        self.fetched_at
    }

    pub fn length(&self) -> u64 {
        self.length
    }

    pub fn checksum(&self) -> &Checksum {
        &self.checksum
    }
}

/// Base64 of the CRC64/NVME over a whole object, the same whatever part size wrote it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Crc64(String);

impl Crc64 {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// The checksum S3 holds for an object.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Checksum {
    FullObject(Crc64),
    /// A checksum S3 did not name as over the whole object, such as one over the parts, so no other copy can match it.
    Composite,
    Absent,
}

impl Checksum {
    fn of(checksum_type: Option<&ChecksumType>, crc64: Option<&str>) -> Self {
        match (checksum_type, crc64) {
            (Some(ChecksumType::FullObject), Some(crc64)) => {
                Self::FullObject(Crc64(crc64.to_string()))
            }
            (Some(_) | None, Some(_)) => Self::Composite,
            (Some(_) | None, None) => Self::Absent,
        }
    }
}

impl Bucket {
    /// Opens a multipart upload under `key` stamped with `provenance`, returning its id; finish or abort it, or its
    /// parts keep billing.
    async fn create_upload(
        &self,
        key: &Key,
        provenance: &Provenance,
    ) -> Result<String, BucketError> {
        let path = key.path();
        let failed = |reason: String| BucketError::Put {
            path: path.clone(),
            reason,
        };
        let created = self
            .s3_client
            .create_multipart_upload()
            .bucket(&self.bucket_name)
            .key(&path)
            .storage_class(storage_class(key))
            .checksum_algorithm(ChecksumAlgorithm::Crc64Nvme)
            .checksum_type(ChecksumType::FullObject)
            .set_metadata(Some(metadata(provenance)))
            .send()
            .await
            .map_err(|error| failed(aws_sdk_s3::error::DisplayErrorContext(error).to_string()))?;
        Ok(created
            .upload_id()
            .ok_or_else(|| failed("no upload id".to_string()))?
            .to_string())
    }

    /// Discards an upload and its parts; a failed abort is logged with the id to abort by hand.
    async fn abort_upload(&self, path: &str, upload_id: &str) {
        if let Err(abort) = self
            .s3_client
            .abort_multipart_upload()
            .bucket(&self.bucket_name)
            .key(path)
            .upload_id(upload_id)
            .send()
            .await
        {
            tracing::error!(
                path,
                upload_id,
                error = %aws_sdk_s3::error::DisplayErrorContext(abort),
                "Multipart upload not aborted"
            );
        }
    }

    /// What is stored under `key`, read from its metadata alone, so a Deep Archive object needs no restore.
    pub async fn stored(&self, key: &Key) -> Result<Option<Stored>, BucketError> {
        let path = key.path();
        let failed = |reason: String| BucketError::Get {
            path: path.clone(),
            reason,
        };
        match self
            .s3_client
            .head_object()
            .bucket(&self.bucket_name)
            .key(&path)
            .checksum_mode(ChecksumMode::Enabled)
            .send()
            .await
        {
            Ok(response) => {
                let length = response
                    .content_length()
                    .and_then(|length| u64::try_from(length).ok())
                    .ok_or_else(|| failed("no length".to_string()))?;
                let fetched_at = fetched_at(response.metadata()).map_err(failed)?;
                let tag = EntityTag::new(
                    response
                        .e_tag()
                        .ok_or_else(|| failed("no entity tag".to_string()))?,
                );
                Ok(Some(Stored {
                    tag,
                    length,
                    checksum: Checksum::of(
                        response.checksum_type(),
                        response.checksum_crc64_nvme(),
                    ),
                    fetched_at,
                }))
            }
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
}

/// Why a session's fetched pages were not stored.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum TeeError {
    #[error("{0}")]
    Bucket(BucketError),
    /// A page holding a line break would split into two lines that are neither the page.
    #[error("{path} was sent a page holding a line break")]
    LineBreak { path: String },
    /// No page was sent, which a multipart upload cannot hold.
    #[error("{path} was sent no pages")]
    Empty { path: String },
    /// Stored at a different length than was written.
    #[error("{path} stored {stored:?} bytes where {written} were written")]
    Length {
        path: String,
        written: u64,
        stored: Option<u64>,
    },
}

/// Pages gzipped into one stream a line each, cut into parts of at least `part_length` bytes as it grows.
struct GzipLines {
    encoder: flate2::write::GzEncoder<Vec<u8>>,
    part_length: usize,
    lines: u64,
}

impl GzipLines {
    fn new(part_length: usize) -> Self {
        Self {
            encoder: flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default()),
            part_length,
            lines: 0,
        }
    }

    /// Appends `line` and its line break, returning a part once the compressed bytes reach `part_length`.
    fn push(&mut self, line: &[u8]) -> Result<Option<Vec<u8>>, LineBreak> {
        if line.contains(&b'\n') {
            return Err(LineBreak);
        }
        self.encoder
            .write_all(line)
            .and_then(|()| self.encoder.write_all(b"\n"))
            .expect("writing to a vector cannot fail");
        self.lines += 1;
        Ok((self.encoder.get_ref().len() >= self.part_length)
            .then(|| std::mem::take(self.encoder.get_mut())))
    }

    /// The last part, holding the stream's trailer, or `None` when no line was pushed.
    fn finish(self) -> Option<Vec<u8>> {
        let lines = self.lines;
        let rest = self
            .encoder
            .finish()
            .expect("writing to a vector cannot fail");
        (lines > 0).then_some(rest)
    }
}

/// A page refused for holding a line break.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct LineBreak;

/// A session's fetched pages being stored under one raw key, a line each, replacing whatever the key held.
pub struct RawWriter {
    archive: Bucket,
    key: Key,
    upload_id: String,
    lines: GzipLines,
    parts: Vec<CompletedPart>,
    written: u64,
}

impl Bucket {
    /// Opens an upload under `key` stamped with `provenance`; finish or abort it, or its parts keep billing.
    pub async fn begin_raw(
        &self,
        key: Key,
        provenance: &Provenance,
    ) -> Result<RawWriter, TeeError> {
        let upload_id = self
            .create_upload(&key, provenance)
            .await
            .map_err(TeeError::Bucket)?;
        Ok(RawWriter {
            archive: self.clone(),
            key,
            upload_id,
            lines: GzipLines::new(PART_LENGTH),
            parts: Vec::new(),
            written: 0,
        })
    }
}

impl Bucket {
    /// Stores a reference snapshot's response bodies as served, one per line, under its raw key.
    pub async fn store_responses(
        &self,
        key: ReferenceKey,
        provenance: &Provenance,
        bodies: &[Vec<u8>],
    ) -> Result<Stored, TeeError> {
        let mut writer = self.begin_raw(Key::RawReference(key), provenance).await?;
        for body in bodies {
            if let Err(error) = writer.push_line(body).await {
                writer.abort().await;
                return Err(error);
            }
        }
        writer.finish().await
    }
}

impl RawWriter {
    /// Appends one page as a line, uploading a part whenever enough has accumulated.
    pub async fn push_line(&mut self, page: &[u8]) -> Result<(), TeeError> {
        match self.lines.push(page) {
            Ok(Some(part)) => self.upload(part).await,
            Ok(None) => Ok(()),
            Err(LineBreak) => Err(TeeError::LineBreak {
                path: self.key.path(),
            }),
        }
    }

    /// Uploads the last part and completes the object, aborting the upload on any failure.
    pub async fn finish(mut self) -> Result<Stored, TeeError> {
        let path = self.key.path();
        let last = std::mem::replace(&mut self.lines, GzipLines::new(0)).finish();
        let completed = match last {
            Some(part) => match self.upload(part).await {
                Ok(()) => self.complete().await,
                Err(error) => Err(error),
            },
            None => Err(TeeError::Empty { path: path.clone() }),
        };
        if let Err(error) = completed {
            self.abort().await;
            return Err(error);
        }
        match self
            .archive
            .stored(&self.key)
            .await
            .map_err(TeeError::Bucket)?
        {
            Some(stored) if stored.length == self.written => Ok(stored),
            stored => Err(TeeError::Length {
                path,
                written: self.written,
                stored: stored.map(|stored| stored.length),
            }),
        }
    }

    /// Discards the upload and its parts; a failed abort is logged with the id to abort by hand.
    pub async fn abort(self) {
        self.archive
            .abort_upload(&self.key.path(), &self.upload_id)
            .await;
    }

    async fn upload(&mut self, part: Vec<u8>) -> Result<(), TeeError> {
        let path = self.key.path();
        let number =
            i32::try_from(self.parts.len() + 1).expect("an upload holds at most 10,000 parts");
        let length = part.len() as u64;
        let body = bytes::Bytes::from(part);
        let mut attempt = 1;
        let uploaded = loop {
            match self
                .archive
                .s3_client
                .upload_part()
                .bucket(&self.archive.bucket_name)
                .key(&path)
                .upload_id(&self.upload_id)
                .part_number(number)
                .checksum_algorithm(ChecksumAlgorithm::Crc64Nvme)
                .body(ByteStream::from(body.clone()))
                .send()
                .await
            {
                Ok(uploaded) => break uploaded,
                Err(error) if attempt < PART_ATTEMPTS => {
                    tracing::warn!(path, part = number, attempt, error = %aws_sdk_s3::error::DisplayErrorContext(error), "Retrying a part");
                    tokio::time::sleep(Duration::from_secs(2_u64.pow(attempt))).await;
                    attempt += 1;
                }
                Err(error) => return Err(put_failed(&path, error)),
            }
        };
        self.parts.push(
            CompletedPart::builder()
                .part_number(number)
                .set_e_tag(uploaded.e_tag().map(String::from))
                .set_checksum_crc64_nvme(uploaded.checksum_crc64_nvme().map(String::from))
                .build(),
        );
        self.written += length;
        Ok(())
    }

    async fn complete(&mut self) -> Result<(), TeeError> {
        let path = self.key.path();
        self.archive
            .s3_client
            .complete_multipart_upload()
            .bucket(&self.archive.bucket_name)
            .key(&path)
            .upload_id(&self.upload_id)
            .checksum_type(ChecksumType::FullObject)
            .multipart_upload(
                CompletedMultipartUpload::builder()
                    .set_parts(Some(std::mem::take(&mut self.parts)))
                    .build(),
            )
            .send()
            .await
            .map_err(|error| put_failed(&path, error))?;
        Ok(())
    }
}

fn put_failed<E: std::error::Error + 'static>(path: &str, error: E) -> TeeError {
    TeeError::Bucket(BucketError::Put {
        path: path.to_string(),
        reason: aws_sdk_s3::error::DisplayErrorContext(error).to_string(),
    })
}

/// The provenance entries an object's metadata carries.
fn metadata(provenance: &Provenance) -> HashMap<String, String> {
    provenance
        .entries()
        .into_iter()
        .filter_map(|(name, value)| value.map(|value| (<&str>::from(name).to_string(), value)))
        .collect()
}

/// The fetch time an object's metadata records, refusing a stamp that is present but unreadable.
fn fetched_at(metadata: Option<&HashMap<String, String>>) -> Result<Option<DateTime<Utc>>, String> {
    metadata
        .and_then(|metadata| metadata.get(<&str>::from(MetadataEntry::FetchedAt)))
        .map(|raw| {
            DateTime::parse_from_rfc3339(raw)
                .map(|instant| instant.with_timezone(&Utc))
                .map_err(|error| format!("fetch time {raw:?} unreadable: {error}"))
        })
        .transpose()
}

fn storage_class(key: &Key) -> aws_sdk_s3::types::StorageClass {
    match key.storage_class() {
        StorageClass::Standard => aws_sdk_s3::types::StorageClass::Standard,
        StorageClass::DeepArchive => aws_sdk_s3::types::StorageClass::DeepArchive,
    }
}

#[cfg(test)]
mod tests {
    use proptest::strategy::Strategy;

    use super::*;

    #[test]
    fn test_a_fetch_stamp_is_read_absent_or_refused_never_dropped() {
        let stamped = |raw: &str| HashMap::from([("fund.fetched_at".to_string(), raw.to_string())]);
        assert_eq!(
            fetched_at(Some(&stamped("2026-10-02T20:00:00-04:00")))
                .map(|instant| instant.map(|instant| instant.to_rfc3339())),
            Ok(Some("2026-10-03T00:00:00+00:00".to_string()))
        );
        assert_eq!(fetched_at(None), Ok(None));
        assert_eq!(fetched_at(Some(&HashMap::new())), Ok(None));
        assert!(fetched_at(Some(&stamped("yesterday"))).is_err());
    }

    #[test]
    fn test_only_a_full_object_checksum_is_kept_as_one() {
        assert_eq!(
            Checksum::of(Some(&ChecksumType::FullObject), Some("AAAAAAAAAAA=")),
            Checksum::FullObject(Crc64("AAAAAAAAAAA=".to_string()))
        );
        assert_eq!(
            Checksum::of(Some(&ChecksumType::Composite), Some("AAAAAAAAAAA=-3")),
            Checksum::Composite
        );
        assert_eq!(
            Checksum::of(None, Some("AAAAAAAAAAA=")),
            Checksum::Composite
        );
        assert_eq!(
            Checksum::of(Some(&ChecksumType::FullObject), None),
            Checksum::Absent
        );
    }

    /// Every part `GzipLines` cut for `lines`, the last included.
    fn gzip_parts(lines: &[Vec<u8>], part_length: usize) -> Vec<Vec<u8>> {
        let mut gzip = GzipLines::new(part_length);
        let mut parts: Vec<Vec<u8>> = lines
            .iter()
            .filter_map(|line| gzip.push(line).unwrap())
            .collect();
        parts.extend(gzip.finish());
        parts
    }

    fn gunzip(parts: &[Vec<u8>]) -> Vec<u8> {
        let mut text = Vec::new();
        std::io::Read::read_to_end(
            &mut flate2::read::GzDecoder::new(&parts.concat()[..]),
            &mut text,
        )
        .unwrap();
        text
    }

    #[test]
    fn test_pages_are_stored_a_line_each() {
        let parts = gzip_parts(
            &[
                br#"{"quotes":{}}"#.to_vec(),
                br#"{"next_page_token":null}"#.to_vec(),
            ],
            1 << 20,
        );
        assert_eq!(parts.len(), 1);
        assert_eq!(
            gunzip(&parts),
            b"{\"quotes\":{}}\n{\"next_page_token\":null}\n"
        );
    }

    #[test]
    fn test_a_page_holding_a_line_break_is_refused() {
        assert_eq!(GzipLines::new(1 << 20).push(b"{}\n{}"), Err(LineBreak));
    }

    #[test]
    fn test_no_pages_is_no_part() {
        assert_eq!(GzipLines::new(1 << 20).finish(), None);
    }

    proptest::proptest! {
        /// However the stream is cut, every part but the last reaches the part length and together they unzip to the
        /// pages in order, a line each.
        #[test]
        fn property_parts_unzip_to_the_pages(
            lines in proptest::collection::vec(proptest::collection::vec(0_u8..=255, 0..2_000).prop_map(|line| line.into_iter().filter(|byte| *byte != b'\n').collect::<Vec<_>>()), 1..40),
            part_length in 1_usize..4_000,
        ) {
            let parts = gzip_parts(&lines, part_length);
            for part in &parts[..parts.len() - 1] {
                proptest::prop_assert!(part.len() >= part_length);
            }
            let mut expected = Vec::new();
            for line in &lines {
                expected.extend_from_slice(line);
                expected.push(b'\n');
            }
            proptest::prop_assert_eq!(gunzip(&parts), expected);
        }
    }
}
