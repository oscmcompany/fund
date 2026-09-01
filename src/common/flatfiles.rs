//! Massive's flat files: one gzipped CSV per day, streamed and handed to a fold row by row.
//!
//! Separate endpoint and credentials from [`crate::common::massive`]; nothing holds the file.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use aws_sdk_s3::Client as S3Client;
use chrono::{DateTime, Datelike, NaiveDate, TimeZone, Utc};
use flate2::read::GzDecoder;
use tracing::{info, warn};

use crate::common::alpaca::{QuoteTick, TradeTick};
use crate::common::types::Ticker;

/// The bucket every dataset lives under, which Massive support named on 2026-08-24.
const FLAT_FILE_BUCKET: &str = "flatfiles";

/// The region the signer needs. Massive is not AWS, so nothing resolves this from an instance or a
/// profile — it is a constant the signature requires rather than a place anything is stored.
const FLAT_FILE_REGION: &str = "us-east-1";

/// How much of the object one range request asks for.
///
/// Small enough that a failure costs little and the reorder buffer stays bounded, large enough that
/// per-request overhead does not dominate.
const CHUNK_BYTES: i64 = 2 * 1024 * 1024;

/// How many ranges are outstanding at once, which is what buys the throughput.
///
/// Measured against one session: 1 connection reads 1.15 MB/s, 64 read 14.1, 128 read 26.8 — the
/// throttle is per connection, so concurrency is the only lever. Massive resets connections when
/// pushed, which [`RANGE_ATTEMPTS`] absorbs rather than this number avoiding.
const RANGES_IN_FLIGHT: usize = 128;

/// How many times one range is asked for before the file gives up.
const RANGE_ATTEMPTS: usize = 5;

/// The first wait after a failed range, doubling per attempt.
const RETRY_BACKOFF: std::time::Duration = std::time::Duration::from_millis(250);

/// What one part of the raw tee's multipart upload carries.
///
/// Chosen once the bytes are local, which is the whole reason the tee stages to disk: the 2 MiB
/// ranges the download arrives in are below S3's 5 MiB part floor and never have to meet it. At this
/// size the largest session measured, 9.0 GB of quotes, is 135 parts against a 10,000 limit.
const TEE_PART_BYTES: usize = 64 * 1024 * 1024;

/// Chunks allowed to sit finished in the channel ahead of the parser.
///
/// This is not the whole reorder cost: a completed request keeps its own chunk until the ones before
/// it have been sent, so the peak is `(RANGES_IN_FLIGHT + READY_CHUNKS) * CHUNK_BYTES`, 272 MiB.
const READY_CHUNKS: usize = 8;

/// How long a range may read nothing before it is abandoned.
///
/// The default is five seconds, which a request waiting its turn behind [`RANGES_IN_FLIGHT`] others
/// exceeds routinely — that default is what killed the first whole-file read, mid-download and with
/// no explanation. Long enough that only a genuinely dead connection trips it.
const STALL_GRACE_PERIOD: std::time::Duration = std::time::Duration::from_secs(60);

/// The columns [`QuoteColumns`] resolves out of a quote file's header.
///
/// Declared here rather than as positions because the column *order* is undocumented and could not
/// be checked before the subscription was bought. A file that renames one of these fails loudly on
/// the header rather than silently folding the wrong field.
const TICKER_COLUMN: &str = "ticker";
const TIMESTAMP_COLUMN: &str = "sip_timestamp";
const BID_PRICE_COLUMN: &str = "bid_price";
const BID_SIZE_COLUMN: &str = "bid_size";
const ASK_PRICE_COLUMN: &str = "ask_price";
const ASK_SIZE_COLUMN: &str = "ask_size";
const PRICE_COLUMN: &str = "price";
const SIZE_COLUMN: &str = "size";
const CONDITIONS_COLUMN: &str = "conditions";
const CORRECTION_COLUMN: &str = "correction";

/// Why a flat-file read failed.
#[derive(Debug, thiserror::Error)]
pub enum FlatFileError {
    #[error("{variable} environment variable is not set")]
    Missing { variable: &'static str },
    #[error("{field} must not be empty")]
    Empty { field: &'static str },
    #[error("could not fetch {key}: {source}")]
    Fetch {
        key: String,
        source: Box<dyn std::error::Error + Send + Sync>,
    },
    #[error("could not read {key}: {source}")]
    Read { key: String, source: std::io::Error },
    /// The header named none of the columns the fold needs, or renamed one of them.
    #[error("{key} has no {column} column; its header is {header}")]
    Column {
        key: String,
        column: &'static str,
        header: String,
    },
    /// More rows stepped backwards in time than stepped forwards, which is what a file sorted the
    /// wrong way looks like. See [`FlatFileFold::require_ascending`].
    #[error(
        "{key} is not in ascending time within a ticker: {backwards} rows stepped back against \
         {forwards} forwards, so the fold would weigh almost every one at zero"
    )]
    Descending {
        key: String,
        backwards: usize,
        forwards: usize,
    },
    /// A range answered with a different number of bytes than it asked for.
    #[error("{key} {range} returned {received} bytes rather than {expected}")]
    ShortRange {
        key: String,
        range: String,
        expected: usize,
        received: usize,
    },
    /// The raw tee could not store the vendor's bytes.
    #[error("could not archive the raw object for {key}: {source}")]
    Tee {
        key: String,
        source: Box<dyn std::error::Error + Send + Sync>,
    },
    /// The bytes that landed are not the bytes the source reports, so the capture is not the object.
    #[error("the raw object for {key} is {staged} bytes rather than {expected}")]
    TeeLength {
        key: String,
        staged: u64,
        expected: i64,
    },
}

/// Credentials for `files.massive.com`: issued by Massive, spoken in S3's dialect, not AWS's.
///
/// Named `MASSIVE_S3_*` rather than anything containing `AWS` on purpose. This process also holds
/// real AWS credentials and writes the archive with them, and `aws_config::load_defaults` reads
/// `AWS_ACCESS_KEY_ID` from the environment — so a name in that family would hand Massive's key to
/// every genuine AWS call in the same binary.
///
/// Deliberately does not derive `Debug`: it holds a secret key, and a derived `Debug` puts that key
/// into any log line or panic message that formats a struct containing one.
#[derive(Clone)]
pub struct FlatFileCredentials {
    endpoint_url: String,
    access_key_id: String,
    secret_access_key: String,
}

impl FlatFileCredentials {
    /// Constructs from explicit values, rejecting empties.
    pub fn new(
        endpoint_url: String,
        access_key_id: String,
        secret_access_key: String,
    ) -> Result<Self, FlatFileError> {
        if endpoint_url.is_empty() {
            return Err(FlatFileError::Empty {
                field: "endpoint_url",
            });
        }
        if access_key_id.is_empty() {
            return Err(FlatFileError::Empty {
                field: "access_key_id",
            });
        }
        if secret_access_key.is_empty() {
            return Err(FlatFileError::Empty {
                field: "secret_access_key",
            });
        }
        Ok(Self {
            endpoint_url,
            access_key_id,
            secret_access_key,
        })
    }

    /// Reads the three `MASSIVE_S3_*` variables from the environment.
    pub fn from_env() -> Result<Self, FlatFileError> {
        let read = |variable: &'static str| {
            std::env::var(variable).map_err(|_| FlatFileError::Missing { variable })
        };
        Self::new(
            read("MASSIVE_S3_ENDPOINT")?,
            read("MASSIVE_S3_ACCESS_KEY_ID")?,
            read("MASSIVE_S3_SECRET_ACCESS_KEY")?,
        )
    }
}

/// Somewhere for a file's ticks to go.
///
/// A trait rather than a closure because the caller needs its accumulator back, and a closure's
/// captures cannot be reached once it has been moved onto the blocking thread.
pub trait QuoteSink {
    fn push(&mut self, ticker: Ticker, tick: QuoteTick);
}

/// Somewhere for a file's trades to go, on the same reasoning as [`QuoteSink`].
pub trait TradeSink {
    fn push(&mut self, ticker: Ticker, tick: TradeTick);
}

/// Adapts a plain closure, for a caller that keeps nothing — counting a file, or a test.
pub struct ForEach<F>(pub F);

impl<F> QuoteSink for ForEach<F>
where
    F: FnMut(Ticker, QuoteTick),
{
    fn push(&mut self, ticker: Ticker, tick: QuoteTick) {
        (self.0)(ticker, tick)
    }
}

/// The trade half of [`ForEach`], distinct because one closure cannot implement both traits.
pub struct ForEachTrade<F>(pub F);

impl<F> TradeSink for ForEachTrade<F>
where
    F: FnMut(Ticker, TradeTick),
{
    fn push(&mut self, ticker: Ticker, tick: TradeTick) {
        (self.0)(ticker, tick)
    }
}

/// The S3 key holding one day of consolidated quotes.
///
/// The `us_stocks_sip` prefix is Massive's own and corroborates the SIP sourcing behind their NBBO
/// answer — a file under it is the consolidated book rather than one venue's.
pub fn quote_key(date: NaiveDate) -> String {
    use chrono::Datelike;
    format!(
        "us_stocks_sip/quotes_v1/{}/{:02}/{}.csv.gz",
        date.year(),
        date.month(),
        date.format("%Y-%m-%d")
    )
}

/// The S3 key holding one day of consolidated trades.
///
/// The same layout as [`quote_key`] under a sibling dataset, which is what lets one pass read both
/// halves of a session without a second convention.
pub fn trade_key(date: NaiveDate) -> String {
    use chrono::Datelike;
    format!(
        "us_stocks_sip/trades_v1/{}/{:02}/{}.csv.gz",
        date.year(),
        date.month(),
        date.format("%Y-%m-%d")
    )
}

/// What one file cost and what it yielded.
///
/// `unusable` counts rows whose book no spread can be read off — a crossed or zero-priced quote,
/// which is ordinary around the open rather than a defect. Reported because it is the only trace a
/// discarded row leaves, and a file that is suddenly half unusable is a vendor change.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct FlatFileFold {
    /// Names whose rows are not contiguous, which is what a fold releasing at the switch must know.
    pub split_tickers: SplitTickers,
    pub rows_read: usize,
    pub ticks_folded: usize,
    pub unusable: usize,
    pub tickers: usize,
    /// Rows whose ticker differs from the row before, which is what [`FlatFileFold::layout`] reads.
    pub ticker_runs: usize,
    /// The object's own size, which cannot be obtained before paying — an unauthenticated `HEAD`
    /// returns 403 — and is what turns a download estimate from arithmetic into a measurement.
    pub compressed_bytes: i64,
    backwards: usize,
}

/// Names that start a second run after their first has ended, which a ticker-major file should have
/// none of and every measured session has two of.
///
/// Kept separately from the counts because a fold that releases at the ticker switch is correct only
/// for names absent from this set — for the rest it would write two partial summaries.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct SplitTickers(std::collections::BTreeSet<Ticker>);

impl SplitTickers {
    pub fn names(&self) -> impl Iterator<Item = &Ticker> {
        self.0.iter()
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

/// How a file's rows are grouped, which is what decides how many folds must be held at once.
///
/// Massive documents no row order and it cannot be inspected before paying, so this is measured on
/// the first file rather than assumed. It is the difference between holding one name's ticks and
/// holding every name's.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RowLayout {
    /// Each ticker's rows are contiguous, so a fold can be finished and dropped at the switch.
    TickerMajor,
    /// Tickers interleave, so every fold stays open until the file ends.
    TimeMajor,
}

impl RowLayout {
    /// The name to print, separate from [`std::fmt::Display`] so a caller holding an
    /// `Option<RowLayout>` can render the absent case without one.
    pub fn as_str(&self) -> &'static str {
        match self {
            RowLayout::TickerMajor => "ticker-major",
            RowLayout::TimeMajor => "time-major",
        }
    }
}

impl std::fmt::Display for RowLayout {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl FlatFileFold {
    /// Which way the file is grouped, or `None` when it folded nothing to judge.
    ///
    /// Ticker-major puts each name in one contiguous run, so runs equal names; time-major
    /// interleaves, so nearly every row starts one. On a real day those are twelve thousand against
    /// four hundred million, so what separates them is which end the count sits nearer — there is no
    /// threshold for anyone to choose, and nothing real lands in between.
    pub fn layout(&self) -> Option<RowLayout> {
        if self.ticks_folded == 0 {
            return None;
        }
        let above_tickers = self.ticker_runs.saturating_sub(self.tickers);
        let below_rows = self.rows_read.saturating_sub(self.ticker_runs);
        if above_tickers < below_rows {
            Some(RowLayout::TickerMajor)
        } else {
            Some(RowLayout::TimeMajor)
        }
    }

    /// Refuses a file whose rows descend in time within a ticker.
    ///
    /// The quote fold weighs each tick by the interval to the next, so a descending run weighs every one
    /// at zero and still produces a summary — a silent wrong answer rather than a failure. Massive
    /// does not document the row order and it cannot be inspected before paying, so it is asserted.
    ///
    /// The threshold is not a tuning knob: an ascending file inverts only where the SIP itself ties
    /// or reorders, a handful of rows per name, while a descending file inverts every row after the
    /// first. Nothing real sits between them.
    fn require_ascending(&self, key: &str) -> Result<(), FlatFileError> {
        let forwards = self.ticks_folded.saturating_sub(self.backwards);
        if self.backwards <= forwards {
            return Ok(());
        }
        Err(FlatFileError::Descending {
            key: key.to_string(),
            backwards: self.backwards,
            forwards,
        })
    }
}

/// Reads one day of flat files from Massive's object store.
/// Keeps the vendor's own bytes, so a re-fold never has to be a re-purchase.
///
/// The corpus is irreproducible once the subscription lapses, and every cadence the archive stores is
/// a lossy read of it — so the raw object is the only thing that makes a later cadence a compute cost
/// rather than a subscription one.
#[derive(Debug, Clone)]
pub struct RawTee {
    s3_client: S3Client,
    bucket: String,
    staging_directory: PathBuf,
}

impl RawTee {
    /// `s3_client` is ours, not Massive's: the two endpoints share no credentials and no server-side
    /// copy reaches between them, so the bytes transit this machine either way.
    pub fn new(s3_client: S3Client, bucket: String, staging_directory: PathBuf) -> Self {
        Self {
            s3_client,
            bucket,
            staging_directory,
        }
    }

    /// Where a staged object waits between the download finishing and the upload starting.
    ///
    /// Carries the process id because the name is otherwise a function of the object alone: two
    /// tee-enabled passes folding one session on one host would then stage to the same path and
    /// truncate each other's bytes, and the length check would call the survivor corrupt.
    fn staging_path(&self, key: &str) -> PathBuf {
        self.staging_directory
            .join(format!("{}.{}", key.replace('/', "_"), std::process::id()))
    }

    /// Uploads `staged` under `key` as Deep Archive, unless an object of the right length is there.
    ///
    /// Verified on length rather than ETag: a multipart ETag is a function of the part size, so ours
    /// never matches the vendor's and would fail every comparison it was asked to make.
    async fn store(&self, key: &str, staged: &Path, expected: i64) -> Result<(), FlatFileError> {
        let written = std::fs::metadata(staged)
            .map_err(|source| FlatFileError::Tee {
                key: key.to_string(),
                source: Box::new(source),
            })?
            .len();
        if written != expected as u64 {
            return Err(FlatFileError::TeeLength {
                key: key.to_string(),
                staged: written,
                expected,
            });
        }

        if self.already_stored(key, expected).await? {
            info!(
                key,
                bytes = expected,
                "Raw object already archived; skipping the upload"
            );
            return Ok(());
        }

        self.upload_multipart(key, staged, expected).await?;

        // Read back rather than trusting the completion response: an upload that reported success and
        // landed short is the failure this tee exists to make impossible.
        let stored = self.stored_length(key).await?;
        if stored != Some(expected) {
            return Err(FlatFileError::TeeLength {
                key: key.to_string(),
                staged: stored.unwrap_or_default() as u64,
                expected,
            });
        }
        info!(
            bucket = self.bucket,
            key,
            bytes = expected,
            "Archived the vendor's raw object"
        );
        Ok(())
    }

    /// Whether `key` is already present at the length the source reports.
    async fn already_stored(&self, key: &str, expected: i64) -> Result<bool, FlatFileError> {
        Ok(self.stored_length(key).await? == Some(expected))
    }

    /// The stored object's length, or `None` where there is no object.
    ///
    /// A `HEAD` answers for a Deep Archive object without restoring it — the class governs the body,
    /// not the metadata — which is what makes the idempotence check free.
    async fn stored_length(&self, key: &str) -> Result<Option<i64>, FlatFileError> {
        match self
            .s3_client
            .head_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
        {
            Ok(head) => Ok(head.content_length()),
            Err(error)
                if error
                    .as_service_error()
                    .is_some_and(|inner| inner.is_not_found()) =>
            {
                Ok(None)
            }
            Err(error) => Err(FlatFileError::Tee {
                key: key.to_string(),
                source: Box::new(error),
            }),
        }
    }

    /// Sends the staged file as one multipart upload, aborting it rather than leaving parts behind.
    async fn upload_multipart(
        &self,
        key: &str,
        staged: &Path,
        expected: i64,
    ) -> Result<(), FlatFileError> {
        let tee_error = |source: Box<dyn std::error::Error + Send + Sync>| FlatFileError::Tee {
            key: key.to_string(),
            source,
        };

        let created = self
            .s3_client
            .create_multipart_upload()
            .bucket(&self.bucket)
            .key(key)
            .storage_class(aws_sdk_s3::types::StorageClass::DeepArchive)
            .send()
            .await
            .map_err(|error| tee_error(Box::new(error)))?;
        let Some(upload_id) = created.upload_id() else {
            return Err(FlatFileError::Empty { field: "upload_id" });
        };

        // Every failure path aborts, completion included: a `complete` that fails leaves the parts
        // just as open as a `send_parts` that fails, and they bill until the bucket's seven-day rule
        // sweeps them. Written as one fallible block rather than a match, so a future step added
        // between the two cannot quietly acquire an exit that skips the abort.
        let outcome = async {
            let parts = self.send_parts(key, staged, upload_id, expected).await?;
            self.s3_client
                .complete_multipart_upload()
                .bucket(&self.bucket)
                .key(key)
                .upload_id(upload_id)
                .multipart_upload(
                    aws_sdk_s3::types::CompletedMultipartUpload::builder()
                        .set_parts(Some(parts))
                        .build(),
                )
                .send()
                .await
                .map_err(|error| tee_error(Box::new(error)))?;
            Ok(())
        }
        .await;

        if let Err(error) = outcome {
            if let Err(abort) = self
                .s3_client
                .abort_multipart_upload()
                .bucket(&self.bucket)
                .key(key)
                .upload_id(upload_id)
                .send()
                .await
            {
                warn!(key, error = %abort, "Could not abort the failed multipart upload");
            }
            return Err(error);
        }
        Ok(())
    }

    /// Reads the staged file a part at a time, so a 9 GB object never sits in memory whole.
    async fn send_parts(
        &self,
        key: &str,
        staged: &Path,
        upload_id: &str,
        expected: i64,
    ) -> Result<Vec<aws_sdk_s3::types::CompletedPart>, FlatFileError> {
        let mut file = std::fs::File::open(staged).map_err(|source| FlatFileError::Tee {
            key: key.to_string(),
            source: Box::new(source),
        })?;
        let mut parts = Vec::new();
        let mut buffer = vec![0u8; TEE_PART_BYTES];
        let mut sent = 0i64;
        let mut number = 1i32;

        loop {
            // Filling 64 MiB is a real disk read and would hold a runtime worker for the whole of
            // it. The file and the buffer travel to the blocking pool and back rather than the file
            // being reopened per part, so the read position stays the loop's own.
            let filled;
            (file, buffer, filled) = tokio::task::spawn_blocking(move || {
                let filled = read_fully(&mut file, &mut buffer);
                (file, buffer, filled)
            })
            .await
            .map_err(|source| FlatFileError::Tee {
                key: key.to_string(),
                source: Box::new(source),
            })?;
            let filled = filled.map_err(|source| FlatFileError::Tee {
                key: key.to_string(),
                source: Box::new(source),
            })?;
            if filled == 0 {
                break;
            }
            let uploaded = self
                .s3_client
                .upload_part()
                .bucket(&self.bucket)
                .key(key)
                .upload_id(upload_id)
                .part_number(number)
                .body(aws_sdk_s3::primitives::ByteStream::from(
                    buffer[..filled].to_vec(),
                ))
                .send()
                .await
                .map_err(|error| FlatFileError::Tee {
                    key: key.to_string(),
                    source: Box::new(error),
                })?;
            parts.push(
                aws_sdk_s3::types::CompletedPart::builder()
                    .part_number(number)
                    .set_e_tag(uploaded.e_tag().map(str::to_string))
                    .build(),
            );
            sent += filled as i64;
            number += 1;
        }

        if sent != expected {
            return Err(FlatFileError::TeeLength {
                key: key.to_string(),
                staged: sent as u64,
                expected,
            });
        }
        Ok(parts)
    }
}

/// Fills `buffer` as far as the file allows, since one `read` may stop short of a part boundary.
///
/// A short part that is not the last one fails the whole upload, so this cannot be a bare `read`.
fn read_fully(file: &mut std::fs::File, buffer: &mut [u8]) -> std::io::Result<usize> {
    use std::io::Read;
    let mut filled = 0;
    while filled < buffer.len() {
        match file.read(&mut buffer[filled..])? {
            0 => break,
            read => filled += read,
        }
    }
    Ok(filled)
}

/// The key one day of a dataset's raw bytes is stored under.
///
/// `schema=v1` is a hive dimension carrying Massive's *schema* version rather than their filename:
/// the column layout and the quote size-unit change are both properties of `quotes_v1`, so a re-fold
/// years from now knows which parser the bytes want.
pub fn raw_key(dataset: &str, date: NaiveDate) -> String {
    format!(
        "data/raw/massive/equity/{}/schema=v1/year={}/month={:02}/day={:02}/data.csv.gz",
        dataset,
        date.year(),
        date.month(),
        date.day()
    )
}

pub struct FlatFileClient {
    s3_client: S3Client,
    tee: Option<RawTee>,
}

impl FlatFileClient {
    /// Builds a client against Massive's endpoint rather than AWS's.
    ///
    /// Path-style addressing, because the endpoint is not AWS and a virtual-host bucket would
    /// resolve `flatfiles.files.massive.com`, which does not exist.
    pub fn new(credentials: FlatFileCredentials) -> Self {
        Self::from_configuration(Self::configuration(credentials).build())
    }

    /// The signed, path-style configuration, separated so a test can attach its own transport to
    /// exactly the configuration production uses rather than to an approximation of it.
    fn configuration(credentials: FlatFileCredentials) -> aws_sdk_s3::config::Builder {
        aws_sdk_s3::Config::builder()
            .behavior_version(aws_sdk_s3::config::BehaviorVersion::latest())
            .region(aws_sdk_s3::config::Region::new(FLAT_FILE_REGION))
            .endpoint_url(credentials.endpoint_url)
            .force_path_style(true)
            .stalled_stream_protection(
                aws_sdk_s3::config::StalledStreamProtectionConfig::enabled()
                    .grace_period(STALL_GRACE_PERIOD)
                    .build(),
            )
            .credentials_provider(aws_sdk_s3::config::Credentials::new(
                credentials.access_key_id,
                credentials.secret_access_key,
                None,
                None,
                "massive-flat-files",
            ))
    }

    fn from_configuration(configuration: aws_sdk_s3::Config) -> Self {
        Self {
            s3_client: S3Client::from_conf(configuration),
            tee: None,
        }
    }

    /// Constructs from the environment.
    pub fn from_env() -> Result<Self, FlatFileError> {
        Ok(Self::new(FlatFileCredentials::from_env()?))
    }

    /// Keeps the vendor's own bytes, teeing every object this client reads into Deep Archive.
    ///
    /// Off unless asked for, because a pass that only measures should not be writing 9 GB an object.
    pub fn teeing_raw_to(mut self, tee: RawTee) -> Self {
        self.tee = Some(tee);
        self
    }

    /// Streams one day of quotes, handing every usable tick to `fold`.
    ///
    /// Nothing is collected and nothing touches disk: the object arrives as concurrent byte ranges
    /// which are decompressed, parsed and folded in order as they land. The parse runs on a blocking
    /// thread because decompression and CSV parsing are the CPU cost of the download, not a wait.
    pub async fn fold_quotes<S: QuoteSink + Send + 'static>(
        &self,
        date: NaiveDate,
        fold: S,
    ) -> Result<(FlatFileFold, S), FlatFileError> {
        let key = quote_key(date);
        info!(bucket = FLAT_FILE_BUCKET, key, %date, "Reading a flat file of quotes");

        let scoped = key.clone();
        let ((mut summary, fold), compressed_bytes) = self
            .fold_object(key.clone(), "quotes", date, move |reader| {
                fold_gzipped_quotes(reader, &scoped, fold)
            })
            .await?;

        summary.compressed_bytes = compressed_bytes;
        info!(
            key,
            rows_read = summary.rows_read,
            ticks_folded = summary.ticks_folded,
            unusable = summary.unusable,
            tickers = summary.tickers,
            layout = summary
                .layout()
                .map_or("unmeasured", |layout| layout.as_str()),
            compressed_bytes,
            "Folded a flat file of quotes"
        );
        Ok((summary, fold))
    }

    /// Streams one day of trades, handing every usable print to `fold`.
    ///
    /// The same shape as [`FlatFileClient::fold_quotes`] over the sibling dataset — the ranges, the
    /// blocking parse and the ordering guarantee are shared, and only the row schema differs.
    pub async fn fold_trades<S: TradeSink + Send + 'static>(
        &self,
        date: NaiveDate,
        fold: S,
    ) -> Result<(FlatFileFold, S), FlatFileError> {
        let key = trade_key(date);
        info!(bucket = FLAT_FILE_BUCKET, key, %date, "Reading a flat file of trades");

        let scoped = key.clone();
        let ((mut summary, fold), compressed_bytes) = self
            .fold_object(key.clone(), "trades", date, move |reader| {
                fold_gzipped_trades(reader, &scoped, fold)
            })
            .await?;

        summary.compressed_bytes = compressed_bytes;
        info!(
            key,
            rows_read = summary.rows_read,
            ticks_folded = summary.ticks_folded,
            unusable = summary.unusable,
            tickers = summary.tickers,
            layout = summary
                .layout()
                .map_or("unmeasured", |layout| layout.as_str()),
            compressed_bytes,
            "Folded a flat file of trades"
        );
        Ok((summary, fold))
    }

    /// The object's size, which the range requests need before any of them can be addressed.
    async fn object_length(&self, key: &str) -> Result<i64, FlatFileError> {
        let head = self
            .s3_client
            .head_object()
            .bucket(FLAT_FILE_BUCKET)
            .key(key)
            .send()
            .await
            .map_err(|error| FlatFileError::Fetch {
                key: key.to_string(),
                source: Box::new(error),
            })?;
        match head.content_length() {
            Some(length) if length > 0 => Ok(length),
            _ => Err(FlatFileError::Empty { field: "object" }),
        }
    }

    /// A reader over the whole object, fetched as concurrent ranges and delivered in order.
    ///
    /// One connection is both too slow and too fragile for a file this size: Massive throttles per
    /// connection, and a stream held open for the length of a whole file does not reliably survive.
    fn ranged_reader(
        &self,
        key: &str,
        length: i64,
        staging: Option<PathBuf>,
    ) -> (ChunkReader, tokio::task::JoinHandle<()>) {
        let (sender, receiver) = tokio::sync::mpsc::channel(READY_CHUNKS);
        let client = self.s3_client.clone();
        let key = key.to_string();
        let producer =
            tokio::spawn(async move { fetch_ranges(client, key, length, sender, staging).await });
        (ChunkReader::new(receiver), producer)
    }

    /// Reads the whole object, folds it, and archives the vendor's bytes whichever way the fold went.
    ///
    /// The tee is not the caller's to forget: the download is the expensive half and a fold bug must
    /// never cost it, so the raw object is stored here rather than in an error arm someone has to
    /// remember to write.
    async fn fold_object<T>(
        &self,
        key: String,
        dataset: &str,
        date: NaiveDate,
        parse: impl FnOnce(ChunkReader) -> Result<T, FlatFileError> + Send + 'static,
    ) -> Result<(T, i64), FlatFileError>
    where
        T: Send + 'static,
    {
        let compressed_bytes = self.object_length(&key).await?;
        let staged = self.tee.as_ref().map(|tee| tee.staging_path(&key));
        let (reader, producer) = self.ranged_reader(&key, compressed_bytes, staged.clone());

        let folded = tokio::task::spawn_blocking(move || parse(reader))
            .await
            .map_err(|error| FlatFileError::Read {
                key: key.clone(),
                source: std::io::Error::other(error),
            });

        // Awaited before anything is decided, so the staged file is final rather than still growing.
        if let Err(error) = producer.await {
            warn!(key, %error, "The range producer did not finish cleanly");
        }

        if let (Some(tee), Some(staged)) = (self.tee.as_ref(), staged.as_ref()) {
            // Removed only once the bytes are known to be in the archive. Deleting them on a failed
            // upload would destroy the one local copy of the expensive half and force the whole
            // object to be downloaded again — the exact cost this tee exists to stop paying.
            match tee
                .store(&raw_key(dataset, date), staged, compressed_bytes)
                .await
            {
                Ok(()) => {
                    if let Err(error) = std::fs::remove_file(staged) {
                        warn!(path = %staged.display(), %error, "Could not remove the staged file");
                    }
                }
                Err(error) => {
                    warn!(
                        key,
                        path = %staged.display(),
                        %error,
                        "Could not archive the vendor's raw object; the staged bytes are kept for recovery"
                    );
                    return Err(error);
                }
            }
        }

        Ok((folded??, compressed_bytes))
    }
}

/// Fetches every range of `key` with [`RANGES_IN_FLIGHT`] outstanding, sending them in file order.
///
/// Order is what makes this usable: gzip decodes only forwards, so a chunk that arrives early waits
/// for its predecessors. Awaiting the oldest request while newer ones are still running is what
/// keeps the pipe full without buffering the file.
async fn fetch_ranges(
    client: S3Client,
    key: String,
    length: i64,
    sender: tokio::sync::mpsc::Sender<Result<Vec<u8>, FlatFileError>>,
    staging: Option<PathBuf>,
) {
    let mut staged = match staging.as_ref() {
        None => None,
        Some(path) => match stage_file(path).await {
            Ok(file) => Some(file),
            Err(error) => {
                warn!(key, %error, "Could not open the staging file; this object will not be archived");
                None
            }
        },
    };
    let mut in_flight: std::collections::VecDeque<
        tokio::task::JoinHandle<Result<Vec<u8>, FlatFileError>>,
    > = std::collections::VecDeque::with_capacity(RANGES_IN_FLIGHT);
    let mut next_offset = 0i64;
    // Set once the parse has stopped reading. The download keeps going: the raw capture is the half
    // that cannot be repeated without paying for the month again, and a fold bug must not cost it.
    let mut listening = true;

    loop {
        while in_flight.len() < RANGES_IN_FLIGHT && next_offset < length {
            let (range, after) = chunk_range(next_offset, length);
            let expected = (after - next_offset) as usize;
            let (client, key) = (client.clone(), key.clone());
            in_flight.push_back(tokio::spawn(async move {
                fetch_one_range(client, key, range, expected).await
            }));
            next_offset = after;
        }

        let Some(oldest) = in_flight.pop_front() else {
            // `tokio::fs::File` buffers, and dropping one with writes outstanding loses them. The
            // length check downstream would call that a short capture rather than a missing flush.
            if let Some(file) = staged.as_mut() {
                if let Err(error) = tokio::io::AsyncWriteExt::flush(file).await {
                    warn!(key, %error, "Could not flush the staged object");
                }
            }
            return;
        };
        let chunk = match oldest.await {
            Ok(chunk) => chunk,
            Err(error) => Err(FlatFileError::Read {
                key: key.clone(),
                source: std::io::Error::other(error),
            }),
        };
        if let (Some(file), Ok(bytes)) = (staged.as_mut(), chunk.as_ref()) {
            if let Err(error) = tokio::io::AsyncWriteExt::write_all(file, bytes).await {
                warn!(key, %error, "Could not stage a chunk; this object will not be archived");
                staged = None;
            }
        }

        // A closed receiver means the parse stopped early, which is its answer rather than an error.
        // With nothing to stage there is nothing left to do; with a staging file the download runs to
        // the end regardless, because the bytes are what the subscription bought.
        if listening && sender.send(chunk).await.is_err() {
            listening = false;
            if staged.is_none() {
                // Dropping a handle does not cancel its request, so the rest are stopped rather than
                // left downloading megabytes nothing will read.
                for pending in in_flight {
                    pending.abort();
                }
                return;
            }
            warn!(
                key,
                "The parse stopped early; finishing the download for the raw archive"
            );
        }
    }
}

/// Opens the staging file, creating its directory, and truncates any partial file left behind.
///
/// Async because the chunks are written from the range producer, which is on the runtime: a 2 MiB
/// write is usually page cache, but it is a real write whenever the cache is under pressure.
async fn stage_file(path: &Path) -> std::io::Result<tokio::fs::File> {
    if let Some(directory) = path.parent() {
        tokio::fs::create_dir_all(directory).await?;
    }
    tokio::fs::File::create(path).await
}

/// Every layer of an error, because the outermost one is routinely the least informative.
fn error_chain(error: &dyn std::error::Error) -> String {
    let mut rendered = error.to_string();
    let mut source = error.source();
    while let Some(inner) = source {
        rendered.push_str(&format!(" <- {inner}"));
        source = inner.source();
    }
    rendered
}

/// The `Range` header covering the chunk at `offset`, and the offset the next one starts at.
///
/// Inclusive on both ends, which is what the header means and where the off-by-one lives: the last
/// chunk of a file stops at `length - 1` rather than a chunk boundary.
fn chunk_range(offset: i64, length: i64) -> (String, i64) {
    let last = (offset + CHUNK_BYTES - 1).min(length - 1);
    (format!("bytes={offset}-{last}"), last + 1)
}

/// Fetches one range, retrying it on its own.
///
/// The point of addressing bytes by offset: a reset range is simply asked for again, where a reset
/// whole-file stream had nothing to resume from. Massive resets connections under load, so this is
/// the ordinary case rather than the exceptional one.
async fn fetch_one_range(
    client: S3Client,
    key: String,
    range: String,
    expected: usize,
) -> Result<Vec<u8>, FlatFileError> {
    let mut attempt = 1;
    loop {
        match try_fetch_one_range(&client, &key, &range, expected).await {
            Ok(bytes) => return Ok(bytes),
            Err(error) if attempt < RANGE_ATTEMPTS => {
                warn!(key, range, attempt, chain = %error_chain(&error), "Retrying a range");
                tokio::time::sleep(RETRY_BACKOFF * 2u32.pow(attempt as u32 - 1)).await;
                attempt += 1;
            }
            Err(error) => return Err(error),
        }
    }
}

async fn try_fetch_one_range(
    client: &S3Client,
    key: &str,
    range: &str,
    expected: usize,
) -> Result<Vec<u8>, FlatFileError> {
    let object = client
        .get_object()
        .bucket(FLAT_FILE_BUCKET)
        .key(key)
        .range(range)
        .send()
        .await
        .map_err(|error| FlatFileError::Fetch {
            key: format!("{key} {range}"),
            source: Box::new(error),
        })?;
    let body = object
        .body
        .collect()
        .await
        .map_err(|error| FlatFileError::Fetch {
            key: format!("{key} {range}"),
            source: Box::new(error),
        })?;
    let bytes = body.to_vec();
    // A short body puts a hole in the gzip stream, and an endpoint that ignores Range altogether
    // answers with the whole object, which decodes to its first member and reads as a clean success.
    if bytes.len() != expected {
        return Err(FlatFileError::ShortRange {
            key: key.to_string(),
            range: range.to_string(),
            expected,
            received: bytes.len(),
        });
    }
    Ok(bytes)
}

/// Presents the ordered chunks as something the gzip decoder can read.
///
/// Blocking by construction: it is handed to `spawn_blocking` alongside the decoder and the parser,
/// so waiting for the next chunk is the same wait the old single stream did.
struct ChunkReader {
    receiver: tokio::sync::mpsc::Receiver<Result<Vec<u8>, FlatFileError>>,
    current: std::io::Cursor<Vec<u8>>,
}

impl ChunkReader {
    fn new(receiver: tokio::sync::mpsc::Receiver<Result<Vec<u8>, FlatFileError>>) -> Self {
        Self {
            receiver,
            current: std::io::Cursor::new(Vec::new()),
        }
    }
}

impl std::io::Read for ChunkReader {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        loop {
            let read = std::io::Read::read(&mut self.current, buffer)?;
            if read > 0 {
                return Ok(read);
            }
            match self.receiver.blocking_recv() {
                None => return Ok(0),
                Some(Ok(chunk)) => self.current = std::io::Cursor::new(chunk),
                Some(Err(error)) => return Err(std::io::Error::other(error)),
            }
        }
    }
}

/// Where `column` sits in `header`, or the error naming what the file called its columns instead.
///
/// Shared by both resolvers so a renamed column fails identically whichever dataset met it first.
fn column_index(
    header: &csv::StringRecord,
    key: &str,
    column: &'static str,
) -> Result<usize, FlatFileError> {
    header
        .iter()
        .position(|found| found.trim() == column)
        .ok_or_else(|| FlatFileError::Column {
            key: key.to_string(),
            column,
            header: header.iter().collect::<Vec<_>>().join(","),
        })
}

/// Where each field the fold needs sits in this particular file.
///
/// Resolved from the header rather than fixed, so a column inserted or reordered upstream costs
/// nothing and a column *renamed* fails on the first row rather than folding zeros.
struct QuoteColumns {
    ticker: usize,
    timestamp: usize,
    bid_price: usize,
    bid_size: usize,
    ask_price: usize,
    ask_size: usize,
}

impl QuoteColumns {
    fn resolve(header: &csv::StringRecord, key: &str) -> Result<Self, FlatFileError> {
        let index = |column: &'static str| column_index(header, key, column);
        Ok(Self {
            ticker: index(TICKER_COLUMN)?,
            timestamp: index(TIMESTAMP_COLUMN)?,
            bid_price: index(BID_PRICE_COLUMN)?,
            bid_size: index(BID_SIZE_COLUMN)?,
            ask_price: index(ASK_PRICE_COLUMN)?,
            ask_size: index(ASK_SIZE_COLUMN)?,
        })
    }

    /// Reads one row, or `None` if it is not a book a spread can be read off.
    fn tick(&self, row: &csv::StringRecord) -> Option<(Ticker, QuoteTick)> {
        let field = |index: usize| row.get(index).map(str::trim);
        let ticker = Ticker::new(field(self.ticker)?)?;
        let timestamp = nanoseconds_to_instant(field(self.timestamp)?.parse::<i64>().ok()?)?;
        let tick = QuoteTick::new(
            timestamp,
            field(self.bid_price)?.parse::<f64>().ok()?,
            field(self.ask_price)?.parse::<f64>().ok()?,
            field(self.bid_size)?.parse::<i32>().ok()?,
            field(self.ask_size)?.parse::<i32>().ok()?,
        )?;
        Some((ticker, tick))
    }
}

/// Where each field the trade fold needs sits in this particular file.
///
/// Resolved from the header on the same reasoning as [`QuoteColumns`]: a renamed column fails on the
/// first row rather than folding zeros.
struct TradeColumns {
    ticker: usize,
    timestamp: usize,
    price: usize,
    size: usize,
    conditions: usize,
    correction: usize,
}

impl TradeColumns {
    fn resolve(header: &csv::StringRecord, key: &str) -> Result<Self, FlatFileError> {
        let index = |column: &'static str| column_index(header, key, column);
        Ok(Self {
            ticker: index(TICKER_COLUMN)?,
            // `sip_timestamp`, never `participant_timestamp`: the quote file is stamped in SIP time
            // and an effective spread pairs the two, so the venue's clock would compare two clocks.
            timestamp: index(TIMESTAMP_COLUMN)?,
            price: index(PRICE_COLUMN)?,
            size: index(SIZE_COLUMN)?,
            conditions: index(CONDITIONS_COLUMN)?,
            correction: index(CORRECTION_COLUMN)?,
        })
    }

    /// Reads one row, or `None` if it is not a print that can carry weight.
    fn tick(&self, row: &csv::StringRecord) -> Option<(Ticker, TradeTick)> {
        let field = |index: usize| row.get(index).map(str::trim);
        let ticker = Ticker::new(field(self.ticker)?)?;
        let timestamp = nanoseconds_to_instant(field(self.timestamp)?.parse::<i64>().ok()?)?;
        let tick = TradeTick::new(
            timestamp,
            field(self.price)?.parse::<f64>().ok()?,
            field(self.size)?.parse::<f64>().ok()?,
            parse_conditions(field(self.conditions)?),
            // A blank cell is "no correction", not an unreadable row. The provider has never
            // emitted one in the data measured, and a row rejected for it would disappear into the
            // `unusable` count with nothing naming the cause.
            match field(self.correction)? {
                "" => false,
                marker => marker.parse::<u32>().ok()? != 0,
            },
        )?;
        Some((ticker, tick))
    }
}

/// Splits the comma-separated condition set, dropping codes that are not numbers.
///
/// Parsed to integers rather than matched as text, because the field holds a *set*: `"14,12,37,41"`
/// contains 41, and a substring test for `"4"` would find it inside that and inside 14.
fn parse_conditions(field: &str) -> Vec<u32> {
    field
        .split(',')
        .filter_map(|code| code.trim().parse::<u32>().ok())
        .collect()
}

/// Massive stamps a SIP quote in nanoseconds since the epoch, and `None` rejects any other unit.
///
/// A stamp in seconds, millis or micros converts without complaint and lands in January 1970, where
/// it would count as usable and weigh a whole session at one interval.
fn nanoseconds_to_instant(nanoseconds: i64) -> Option<DateTime<Utc>> {
    // Below any real quote file, and far above a recent date stamped in seconds, millis or micros.
    const YEAR_2000_NANOSECONDS: i64 = 946_684_800_000_000_000;
    if nanoseconds < YEAR_2000_NANOSECONDS {
        return None;
    }
    Some(Utc.timestamp_nanos(nanoseconds))
}

/// The per-name bookkeeping every ticker-major flat file needs, whatever its rows hold.
///
/// Extracted rather than written twice: quotes and trades share a layout, and the split-ticker and
/// backwards counts are what a fold trusts to decide whether it saw a whole session. Two copies that
/// drifted would give the two datasets different integrity guarantees without saying so.
#[derive(Default)]
struct TickerRuns {
    latest: HashMap<Ticker, DateTime<Utc>>,
    previous: Option<Ticker>,
}

impl TickerRuns {
    /// Records that `ticker` appeared stamped `timestamp`, updating `summary` in place.
    fn observe(&mut self, ticker: &Ticker, timestamp: DateTime<Utc>, summary: &mut FlatFileFold) {
        // An owned key is needed only the first time a name appears, and a day is hundreds of
        // millions of rows.
        let seen_before = match self.latest.get_mut(ticker) {
            Some(previous) => {
                if timestamp < *previous {
                    summary.backwards += 1;
                }
                *previous = timestamp;
                true
            }
            None => {
                self.latest.insert(ticker.clone(), timestamp);
                summary.tickers += 1;
                false
            }
        };
        if self.previous.as_ref() != Some(ticker) {
            summary.ticker_runs += 1;
            // Already seen, and yet starting a run: its rows are split across the file.
            if seen_before {
                summary.split_tickers.0.insert(ticker.clone());
            }
            self.previous = Some(ticker.clone());
        }
    }
}

/// Decompresses, parses and folds, on whichever thread the caller put this on.
///
/// Separate from the request so it can be tested against a file that never went over a network.
fn fold_gzipped_quotes<R, S>(
    reader: R,
    key: &str,
    mut fold: S,
) -> Result<(FlatFileFold, S), FlatFileError>
where
    R: std::io::Read,
    S: QuoteSink,
{
    let mut records = csv::Reader::from_reader(GzDecoder::new(reader));
    let header = records.headers().map_err(|error| read_error(key, error))?;
    let columns = QuoteColumns::resolve(header, key)?;

    let mut summary = FlatFileFold::default();
    let mut runs = TickerRuns::default();
    let mut row = csv::StringRecord::new();
    while records
        .read_record(&mut row)
        .map_err(|error| read_error(key, error))?
    {
        summary.rows_read += 1;
        let Some((ticker, tick)) = columns.tick(&row) else {
            summary.unusable += 1;
            continue;
        };
        runs.observe(&ticker, tick.timestamp(), &mut summary);
        summary.ticks_folded += 1;
        fold.push(ticker, tick);
    }

    // Asserted here because this is what read the file. An error means the fold must be discarded:
    // the rows were handed over before the order was known.
    summary.require_ascending(key)?;

    if summary.unusable > 0 {
        // Ordinary rather than alarming — a crossed consolidated book is common around the open —
        // but a file that is suddenly half unusable is a vendor change nobody announced.
        warn!(
            key,
            unusable = summary.unusable,
            rows_read = summary.rows_read,
            "Skipped rows no spread can be read off"
        );
    }
    Ok((summary, fold))
}

/// The trade half of [`fold_gzipped_quotes`], sharing its bookkeeping and its ordering guarantee.
fn fold_gzipped_trades<R, S>(
    reader: R,
    key: &str,
    mut fold: S,
) -> Result<(FlatFileFold, S), FlatFileError>
where
    R: std::io::Read,
    S: TradeSink,
{
    let mut records = csv::Reader::from_reader(GzDecoder::new(reader));
    let header = records.headers().map_err(|error| read_error(key, error))?;
    let columns = TradeColumns::resolve(header, key)?;

    let mut summary = FlatFileFold::default();
    let mut runs = TickerRuns::default();
    let mut row = csv::StringRecord::new();
    while records
        .read_record(&mut row)
        .map_err(|error| read_error(key, error))?
    {
        summary.rows_read += 1;
        let Some((ticker, tick)) = columns.tick(&row) else {
            summary.unusable += 1;
            continue;
        };
        runs.observe(&ticker, tick.timestamp(), &mut summary);
        summary.ticks_folded += 1;
        fold.push(ticker, tick);
    }

    summary.require_ascending(key)?;

    if summary.unusable > 0 {
        // A zero-size or zero-price print, which is rare but real — 51 of 871,159 rows on
        // 2026-08-21. A file suddenly half unusable is a vendor change nobody announced.
        warn!(
            key,
            unusable = summary.unusable,
            rows_read = summary.rows_read,
            "Skipped trades that can carry no weight"
        );
    }
    Ok((summary, fold))
}

fn read_error(key: &str, error: csv::Error) -> FlatFileError {
    FlatFileError::Read {
        key: key.to_string(),
        source: std::io::Error::other(error),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::io::Write;

    use aws_smithy_http_client::test_util::infallible_client_fn;
    use aws_smithy_types::body::SdkBody;

    const HEADER: &str =
        "ticker,ask_exchange,ask_price,ask_size,bid_exchange,bid_price,bid_size,sip_timestamp";

    /// A stamp `offset` nanoseconds into 2026-03-09T14:30:00Z, the session the fixtures pretend to
    /// be. A stamp near the epoch is a unit mistake rather than a quote, so these sit where real
    /// ones would.
    fn stamp(offset: i64) -> i64 {
        1_773_066_600_000_000_000 + offset
    }

    /// A row in the column order the header above declares, which is deliberately not the order the
    /// fold reads them in — that is the whole point of resolving against the header.
    fn row(
        ticker: &str,
        ask: &str,
        ask_size: &str,
        bid: &str,
        bid_size: &str,
        nanos: i64,
    ) -> String {
        format!("{ticker},12,{ask},{ask_size},11,{bid},{bid_size},{nanos}\n")
    }

    fn gzipped(body: &str) -> Vec<u8> {
        let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        encoder
            .write_all(body.as_bytes())
            .expect("writing to a vector cannot fail");
        encoder.finish().expect("the encoder flushes")
    }

    /// Folds a body and returns what the fold saw alongside the summary.
    fn fold(body: &str) -> (Result<FlatFileFold, FlatFileError>, Vec<(String, i64)>) {
        let seen = std::rc::Rc::new(std::cell::RefCell::new(Vec::new()));
        let collector = std::rc::Rc::clone(&seen);
        let summary = fold_gzipped_quotes(
            std::io::Cursor::new(gzipped(body)),
            "test.csv.gz",
            ForEach(move |ticker: Ticker, tick: QuoteTick| {
                collector.borrow_mut().push((
                    ticker.as_str().to_string(),
                    tick.timestamp().timestamp_nanos_opt().unwrap_or_default(),
                ));
            }),
        );
        let observed = seen.borrow().clone();
        (summary.map(|(summary, _)| summary), observed)
    }

    #[test]
    fn test_the_key_is_the_layout_massive_named() {
        let date = NaiveDate::from_ymd_opt(2026, 3, 9).expect("a real date");
        assert_eq!(
            quote_key(date),
            "us_stocks_sip/quotes_v1/2026/03/2026-03-09.csv.gz"
        );
        assert_eq!(
            trade_key(date),
            "us_stocks_sip/trades_v1/2026/03/2026-03-09.csv.gz"
        );
    }

    /// The real trade header, in the provider's own column order, read 2026-08-31.
    const TRADE_HEADER: &str = "ticker,conditions,correction,exchange,id,participant_timestamp,\
         price,sequence_number,sip_timestamp,size,tape,trf_id,trf_timestamp";

    fn trade_row(
        ticker: &str,
        conditions: &str,
        correction: &str,
        price: &str,
        size: &str,
        nanos: i64,
    ) -> String {
        // `participant_timestamp` is deliberately a different instant from `sip_timestamp`: the fold
        // must read the SIP clock, and a fixture where both agree could not tell the two apart.
        format!(
            "{ticker},\"{conditions}\",{correction},4,71675223161163,{},{price},3519,{nanos},{size},1,202,{}\n",
            nanos - 10_000_000_000,
            nanos - 500
        )
    }

    fn fold_trades_body(
        body: &str,
    ) -> (
        Result<FlatFileFold, FlatFileError>,
        Vec<(String, TradeTick)>,
    ) {
        let seen = std::rc::Rc::new(std::cell::RefCell::new(Vec::new()));
        let collector = std::rc::Rc::clone(&seen);
        let summary = fold_gzipped_trades(
            std::io::Cursor::new(gzipped(body)),
            "trades.csv.gz",
            ForEachTrade(move |ticker: Ticker, tick: TradeTick| {
                collector
                    .borrow_mut()
                    .push((ticker.as_str().to_string(), tick));
            }),
        );
        let observed = seen.borrow().clone();
        (summary.map(|(summary, _)| summary), observed)
    }

    /// The trade fold reads the SIP clock, the fractional size, and the condition set.
    ///
    /// The size is asserted at 0.0008 because 16.5% of a real session's prints are fractional; an
    /// `i32` here would truncate them to zero and silently drop a sixth of the tape.
    #[test]
    fn test_a_trade_row_is_read_off_the_sip_clock_with_its_marks() {
        let body = format!(
            "{TRADE_HEADER}\n{}",
            trade_row("AAPL", "14,12,37,41", "0", "156.30", "0.000800", stamp(0))
        );
        let (summary, seen) = fold_trades_body(&body);
        let summary = summary.expect("a usable file");
        assert_eq!(summary.rows_read, 1);
        assert_eq!(summary.ticks_folded, 1);

        let (ticker, tick) = seen.first().expect("one print");
        assert_eq!(ticker, "AAPL");
        assert_eq!(tick.timestamp().timestamp_nanos_opt(), Some(stamp(0)));
        assert_eq!(tick.price(), 156.30);
        assert_eq!(tick.size(), 0.000_8);
        assert_eq!(tick.conditions(), &[14, 12, 37, 41]);
        assert!(!tick.corrected());
    }

    /// The condition field is a set of integers, not a string to search.
    ///
    /// `"14,12,37,41"` contains the digit 4 inside both 14 and 41, so a substring test would report
    /// condition 4 — Derivatively Priced is 10, but 4 is Bunched Trade — on a print carrying neither.
    #[test]
    fn test_conditions_are_parsed_as_a_set_rather_than_matched_as_text() {
        assert_eq!(parse_conditions("14,12,37,41"), vec![14, 12, 37, 41]);
        assert_eq!(parse_conditions(""), Vec::<u32>::new());
        assert_eq!(parse_conditions("37"), vec![37]);
        // Whitespace and unparsable codes are dropped rather than poisoning the set.
        assert_eq!(parse_conditions(" 14 , 37 "), vec![14, 37]);
        assert_eq!(parse_conditions("14,,37"), vec![14, 37]);
    }

    /// A print that can carry no weight is counted and dropped, never folded at zero.
    ///
    /// Zero sizes are rare and real — 51 of 871,159 rows on 2026-08-21 — and one reaching a VWAP
    /// divisor would take the whole bar with it.
    #[test]
    fn test_a_trade_with_no_size_or_no_price_is_unusable() {
        let body = format!(
            "{TRADE_HEADER}\n{}{}{}",
            trade_row("AAPL", "37", "0", "156.30", "0.000000", stamp(0)),
            trade_row("AAPL", "37", "0", "0.000000", "100", stamp(1)),
            trade_row("AAPL", "37", "0", "156.30", "100", stamp(2)),
        );
        let (summary, seen) = fold_trades_body(&body);
        let summary = summary.expect("a usable file");
        assert_eq!(summary.rows_read, 3);
        assert_eq!(summary.unusable, 2, "the zero size and the zero price");
        assert_eq!(summary.ticks_folded, 1);
        assert_eq!(seen.len(), 1);
    }

    /// The correction marker survives into the tick, because the fold is what has to drop it.
    ///
    /// 30 corrected prints of 871,159 carried 4.5% of the session's dollar volume on 2026-08-21, so
    /// this is a rare flag on enormous trades rather than noise.
    #[test]
    fn test_a_corrected_print_is_marked_rather_than_discarded_by_the_reader() {
        let body = format!(
            "{TRADE_HEADER}\n{}{}",
            trade_row("AAPL", "37", "8", "156.30", "100", stamp(0)),
            trade_row("AAPL", "37", "0", "156.31", "100", stamp(1)),
        );
        let (_, seen) = fold_trades_body(&body);
        assert_eq!(seen.len(), 2, "both reach the fold");
        assert!(seen[0].1.corrected(), "correction 8 is a correction");
        assert!(!seen[1].1.corrected(), "correction 0 is not");
    }

    /// A renamed column fails on the header rather than folding zeros, as it does for quotes.
    #[test]
    fn test_a_trade_file_missing_a_column_fails_on_the_header() {
        let renamed = TRADE_HEADER.replace("conditions", "sale_conditions");
        let body = format!("{renamed}\n");
        let error = fold_trades_body(&body).0.expect_err("a renamed column");
        assert!(
            matches!(error, FlatFileError::Column { column, .. } if column == "conditions"),
            "got {error:?}"
        );
    }

    /// A file interleaves every ticker, so the fold is handed the ticker with each tick rather than
    /// being told once whose quotes these are.
    #[test]
    fn test_every_row_is_folded_with_its_own_ticker() {
        let body = format!(
            "{HEADER}\n{}{}{}",
            row("AAPL", "100.05", "200", "99.95", "100", stamp(0)),
            row("CBOE", "200.20", "50", "199.80", "40", stamp(1)),
            row("AAPL", "100.02", "300", "99.98", "150", stamp(2)),
        );
        let (summary, seen) = fold(&body);
        let summary = summary.expect("a usable file");

        assert_eq!(summary.rows_read, 3);
        assert_eq!(summary.ticks_folded, 3);
        assert_eq!(summary.unusable, 0);
        assert_eq!(summary.tickers, 2, "AAPL is one ticker across two rows");
        assert_eq!(
            seen,
            vec![
                ("AAPL".to_string(), stamp(0)),
                ("CBOE".to_string(), stamp(1)),
                ("AAPL".to_string(), stamp(2)),
            ]
        );
    }

    /// The column order is undocumented and could not be checked before the subscription was
    /// bought, so the header is what says where each field is. Reordering it must change nothing.
    #[test]
    fn test_the_columns_are_read_off_the_header_rather_than_by_position() {
        let reordered = "sip_timestamp,bid_size,bid_price,ask_size,ask_price,ticker";
        let body = format!("{reordered}\n1773066600000000000,100,99.95,200,100.05,AAPL\n");
        let (summary, seen) = fold(&body);

        assert_eq!(summary.expect("a usable file").ticks_folded, 1);
        assert_eq!(seen, vec![("AAPL".to_string(), stamp(0))]);
    }

    /// A renamed column would otherwise fold whatever sat in that position, so it fails on the
    /// header rather than on the data.
    #[test]
    fn test_a_missing_column_is_refused_by_name() {
        let body = "ticker,bid_price,bid_size,ask_price,ask_size\nAAPL,99.95,100,100.05,200\n";
        let error = fold(body).0.expect_err("no sip_timestamp column");
        assert!(
            matches!(&error, FlatFileError::Column { column, .. } if *column == "sip_timestamp"),
            "{error}"
        );
        assert!(error.to_string().contains("ticker,bid_price"), "{error}");
    }

    /// A crossed or zero-priced book is ordinary around the open, so those rows are counted and
    /// skipped rather than failing the file.
    #[test]
    fn test_a_book_no_spread_reads_off_is_counted_and_skipped() {
        let body = format!(
            "{HEADER}\n{}{}{}{}",
            row("AAPL", "100.05", "200", "99.95", "100", stamp(0)),
            row("AAPL", "99.90", "200", "100.10", "100", stamp(1)),
            row("AAPL", "0", "200", "0", "100", stamp(2)),
            row("AAPL", "100.05", "200", "not-a-price", "100", stamp(3)),
        );
        let (summary, seen) = fold(&body);
        let summary = summary.expect("a usable file");

        assert_eq!(summary.rows_read, 4);
        assert_eq!(summary.ticks_folded, 1);
        assert_eq!(summary.unusable, 3, "crossed, zero-priced and unparseable");
        assert_eq!(seen.len(), 1);
    }

    /// The fold weighs each quote by the interval to the next, so a descending file weighs every one
    /// at zero and still produces a summary. Massive documents no row order, so it is asserted.
    #[test]
    fn test_a_descending_file_is_refused_rather_than_folded_to_zero() {
        let descending: String = (0..10)
            .map(|index| row("AAPL", "100.05", "200", "99.95", "100", stamp(-index)))
            .collect();
        let error = fold(&format!("{HEADER}\n{descending}"))
            .0
            .expect_err("a descending file");
        assert!(
            matches!(
                &error,
                FlatFileError::Descending { backwards, forwards, .. } if *backwards == 9 && *forwards == 1
            ),
            "{error}"
        );
    }

    /// An ascending file inverts only where the SIP itself ties or reorders, so the assertion must
    /// not fire on one — a handful of steps back among many forwards is the real shape.
    #[test]
    fn test_a_few_inversions_in_an_ascending_file_are_tolerated() {
        let mut body = String::from(HEADER);
        body.push('\n');
        for index in 0..20 {
            body.push_str(&row("AAPL", "100.05", "200", "99.95", "100", stamp(index)));
        }
        // Three quotes the SIP reordered, which is ordinary rather than a wrongly sorted file.
        for index in [5, 9, 14] {
            body.push_str(&row("AAPL", "100.05", "200", "99.95", "100", stamp(index)));
        }
        let summary = fold(&body).0.expect("an ascending file with ties");
        assert_eq!(summary.ticks_folded, 23);
    }

    /// Ordering is asserted per ticker rather than across the file, and the ordering that proves it
    /// is the one a **ticker-major** file has: each name ascends while the file as a whole zig-zags
    /// back every time it switches names. Judged across the file, this would read as descending and
    /// be refused — and a ticker-major file is one of the two layouts Massive might hand over.
    #[test]
    fn test_ordering_is_judged_within_a_ticker_not_across_the_file() {
        let body = format!(
            "{HEADER}\n{}{}{}{}",
            row("AAPL", "100.05", "200", "99.95", "100", stamp(100)),
            row("AAPL", "100.05", "200", "99.95", "100", stamp(200)),
            row("CBOE", "200.20", "50", "199.80", "40", stamp(1)),
            row("CBOE", "200.20", "50", "199.80", "40", stamp(2)),
        );
        let summary = fold(&body).0.expect("each ticker ascends");
        assert_eq!(summary.ticks_folded, 4);
        assert_eq!(summary.tickers, 2);
    }

    /// `bytes=first-last`, or `None` for anything else.
    fn parse_range(header: &str) -> Option<(usize, usize)> {
        let (first, last) = header.strip_prefix("bytes=")?.split_once('-')?;
        Some((first.parse().ok()?, last.parse().ok()?))
    }

    /// Serves `missing` bytes fewer than each range asked for, which is what a truncating endpoint
    /// looks like from the client's side.
    fn client_serving_short(
        body: Vec<u8>,
        missing: usize,
    ) -> (
        FlatFileClient,
        std::sync::Arc<std::sync::Mutex<Vec<String>>>,
    ) {
        let requested = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let http_client = infallible_client_fn(move |request| {
            match (
                request.method().clone(),
                parse_range(
                    request
                        .headers()
                        .get("range")
                        .and_then(|value| value.to_str().ok())
                        .unwrap_or_default(),
                ),
            ) {
                (http::Method::HEAD, _) => http::Response::builder()
                    .status(200)
                    .header("content-length", body.len().to_string())
                    .body(SdkBody::empty())
                    .expect("a valid response"),
                (http::Method::GET, Some((first, last))) => {
                    let last = last.min(body.len() - 1).saturating_sub(missing);
                    let slice = body[first..=last.max(first)].to_vec();
                    http::Response::builder()
                        .status(206)
                        .body(SdkBody::from(slice))
                        .expect("a valid response")
                }
                _ => http::Response::builder()
                    .status(405)
                    .body(SdkBody::empty())
                    .expect("a valid response"),
            }
        });
        let credentials = FlatFileCredentials::new(
            "https://files.massive.com".to_string(),
            "key".to_string(),
            "secret".to_string(),
        )
        .expect("usable credentials");
        let configuration = FlatFileClient::configuration(credentials)
            .http_client(http_client)
            .build();
        (FlatFileClient::from_configuration(configuration), requested)
    }

    /// An object store, near enough: `HEAD` reports the length and `GET` serves only what its
    /// `Range` asks for. A `GET` without one is refused, so a regression to whole-object reads —
    /// which is what could not survive a file this size — fails rather than quietly passing.
    fn client_serving(
        body: Vec<u8>,
    ) -> (
        FlatFileClient,
        std::sync::Arc<std::sync::Mutex<Vec<String>>>,
    ) {
        let requested = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let recorder = std::sync::Arc::clone(&requested);
        let http_client = infallible_client_fn(move |request| {
            let range = request
                .headers()
                .get("range")
                .and_then(|value| value.to_str().ok())
                .unwrap_or_default()
                .to_string();
            recorder
                .lock()
                .expect("no test thread panics holding this")
                .push(format!(
                    "{} {}{}",
                    request.method(),
                    request.uri().path(),
                    {
                        if range.is_empty() {
                            String::new()
                        } else {
                            format!(" {range}")
                        }
                    }
                ));
            match (request.method().clone(), parse_range(&range)) {
                (http::Method::HEAD, _) => http::Response::builder()
                    .status(200)
                    .header("content-length", body.len().to_string())
                    .body(SdkBody::empty())
                    .expect("a valid response"),
                (http::Method::GET, Some((first, last))) => {
                    let slice = body[first..=last.min(body.len() - 1)].to_vec();
                    http::Response::builder()
                        .status(206)
                        .body(SdkBody::from(slice))
                        .expect("a valid response")
                }
                _ => http::Response::builder()
                    .status(405)
                    .body(SdkBody::empty())
                    .expect("a valid response"),
            }
        });
        let credentials = FlatFileCredentials::new(
            "https://files.massive.com".to_string(),
            "key".to_string(),
            "secret".to_string(),
        )
        .expect("usable credentials");
        let configuration = FlatFileClient::configuration(credentials)
            .http_client(http_client)
            .build();
        (FlatFileClient::from_configuration(configuration), requested)
    }

    /// The whole path a real run takes: a signed request against Massive's endpoint, a gzip stream
    /// off the response body, and a fold that never sees the file. Only the network is scripted.
    #[tokio::test]
    async fn test_a_day_is_streamed_from_the_endpoint_and_folded() {
        let body = format!(
            "{HEADER}\n{}{}",
            row("AAPL", "100.05", "200", "99.95", "100", stamp(0)),
            row("CBOE", "200.20", "50", "199.80", "40", stamp(1)),
        );
        let compressed = gzipped(&body);
        let body_length = compressed.len();
        let (client, requested) = client_serving(compressed);
        let folded = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let collector = std::sync::Arc::clone(&folded);

        let summary = client
            .fold_quotes(
                NaiveDate::from_ymd_opt(2026, 3, 9).expect("a real date"),
                ForEach(move |ticker: Ticker, _tick: QuoteTick| {
                    collector
                        .lock()
                        .expect("no fold thread panics holding this")
                        .push(ticker.as_str().to_string());
                }),
            )
            .await
            .expect("a usable file")
            .0;

        assert_eq!(summary.ticks_folded, 2);
        assert_eq!(summary.tickers, 2);
        assert_eq!(
            *folded.lock().expect("the fold finished"),
            vec!["AAPL".to_string(), "CBOE".to_string()]
        );

        // Path-style, because the endpoint is not AWS: a virtual-host bucket would resolve
        // `flatfiles.files.massive.com`, which does not exist. The length is asked for first, then
        // the bytes are asked for by range rather than as a whole object.
        let path = "/flatfiles/us_stocks_sip/quotes_v1/2026/03/2026-03-09.csv.gz";
        let seen = requested.lock().expect("the request was recorded").clone();
        assert_eq!(seen.len(), 2, "{seen:?}");
        assert_eq!(seen[0], format!("HEAD {path}"));
        assert_eq!(seen[1], format!("GET {path} bytes=0-{}", body_length - 1));
    }

    /// A short body puts a hole in the gzip stream, and an endpoint that ignores Range altogether
    /// answers with the whole object — which decodes to its first member and reads as a clean
    /// success. Both are refused by comparing what came back against what was asked for.
    #[tokio::test]
    async fn test_a_range_answered_with_the_wrong_number_of_bytes_is_refused() {
        let body = format!(
            "{HEADER}\n{}",
            row("AAPL", "100.05", "200", "99.95", "100", stamp(0)),
        );
        let compressed = gzipped(&body);
        // Reports the real length but serves one byte fewer than every range asks for.
        let (client, _) = client_serving_short(compressed.clone(), 1);
        let error = client
            .fold_quotes(
                NaiveDate::from_ymd_opt(2026, 3, 9).expect("a real date"),
                ForEach(|_ticker: Ticker, _tick: QuoteTick| {}),
            )
            .await
            .map(|(summary, _)| summary)
            .expect_err("a truncated range");
        // Wrapped by the reader boundary on its way out, so the refusal is read off the message
        // rather than the variant — what matters is that the byte count is named and the file fails.
        let rendered = error.to_string();
        assert!(
            rendered.contains(&format!(
                "returned {} bytes rather than {}",
                compressed.len() - 1,
                compressed.len()
            )),
            "{rendered}"
        );
    }

    /// Both ends inclusive, and the last chunk of a file stops where the file does rather than on a
    /// chunk boundary — which is where an off-by-one would drop or duplicate bytes.
    #[test]
    fn test_ranges_are_contiguous_and_stop_at_the_final_byte() {
        let length = CHUNK_BYTES * 2 + 5;
        let mut offset = 0;
        let mut headers = Vec::new();
        while offset < length {
            let (header, after) = chunk_range(offset, length);
            headers.push(header);
            offset = after;
        }

        assert_eq!(
            headers,
            vec![
                "bytes=0-2097151".to_string(),
                "bytes=2097152-4194303".to_string(),
                "bytes=4194304-4194308".to_string(),
            ]
        );
        assert_eq!(offset, length, "the walk ends exactly at the length");
    }

    /// The reason the tee runs before anything parses: a fold that dies mid-file must not cost the
    /// download, because the bytes are the half that cannot be had again without buying the month
    /// back. The receiver is dropped before a single chunk is taken, which is the worst case — the
    /// parser died on the header — and every byte must still reach the staging file.
    #[tokio::test]
    async fn test_a_dead_parse_does_not_cost_the_raw_capture() {
        let body: Vec<u8> = (0..(CHUNK_BYTES * 2 + 1234))
            .map(|value| (value % 251) as u8)
            .collect();
        let (client, _requested) = client_serving(body.clone());

        let directory = std::env::temp_dir().join("fund-tee-decoupling-test");
        std::fs::create_dir_all(&directory).expect("a staging directory");
        let staged = directory.join("data.csv.gz");

        let (sender, receiver) = tokio::sync::mpsc::channel(READY_CHUNKS);
        drop(receiver);
        fetch_ranges(
            client.s3_client.clone(),
            "us_stocks_sip/trades_v1/2021/09/2021-09-01.csv.gz".to_string(),
            body.len() as i64,
            sender,
            Some(staged.clone()),
        )
        .await;

        let captured = std::fs::read(&staged).expect("the staged object");
        assert_eq!(
            captured.len(),
            body.len(),
            "the download must run to the end even though nothing was reading it"
        );
        assert_eq!(captured, body, "and the bytes must be the vendor's own");

        std::fs::remove_dir_all(&directory).ok();
    }

    /// A staged object that could not be uploaded must survive the failure. Deleting it would throw
    /// away the local copy of the expensive half and make the retry a fresh download of the whole
    /// vendor object — the exact cost this tee exists to stop paying, spent on its own error path.
    #[tokio::test]
    async fn test_a_failed_upload_keeps_the_staged_bytes() {
        let body: Vec<u8> = (0..4096u32).map(|value| (value % 251) as u8).collect();
        let (mut client, _requested) = client_serving(body.clone());

        let directory = std::env::temp_dir().join("fund-tee-retention-test");
        std::fs::create_dir_all(&directory).expect("a staging directory");
        // A destination that refuses everything, so `store` fails after the download succeeded.
        client = client.teeing_raw_to(RawTee::new(
            S3Client::from_conf(
                aws_sdk_s3::Config::builder()
                    .behavior_version(aws_sdk_s3::config::BehaviorVersion::latest())
                    .region(aws_sdk_s3::config::Region::new("us-east-1"))
                    .credentials_provider(aws_sdk_s3::config::Credentials::for_tests())
                    .http_client(infallible_client_fn(|_| {
                        http::Response::builder()
                            .status(500)
                            .body(SdkBody::from("no"))
                            .expect("a valid response")
                    }))
                    .build(),
            ),
            "oscm-fund-archive".to_string(),
            directory.clone(),
        ));

        let date = NaiveDate::from_ymd_opt(2021, 9, 1).expect("a real date");
        let outcome = client
            .fold_object(trade_key(date), "trades", date, |mut reader| {
                let mut sink = Vec::new();
                std::io::copy(&mut reader, &mut sink).expect("the staged stream");
                Ok(sink.len())
            })
            .await;
        assert!(
            outcome.is_err(),
            "an unusable destination must fail the pass"
        );

        let staged: Vec<_> = std::fs::read_dir(&directory)
            .expect("the staging directory")
            .filter_map(Result::ok)
            .collect();
        assert_eq!(
            staged.len(),
            1,
            "the staged object must outlive the failed upload"
        );
        assert_eq!(
            std::fs::metadata(staged[0].path())
                .expect("the staged file")
                .len(),
            body.len() as u64,
            "and it must be the whole object, not a truncated one"
        );

        std::fs::remove_dir_all(&directory).ok();
    }

    /// The raw key is what a re-fold years from now has to find the bytes by, and every segment of
    /// it carries meaning: the lifecycle rule matches on `data/raw/`, and `schema=v1` says which
    /// parser the bytes want. Pinned to a literal, because deriving it from the builder under test
    /// would move with any change rather than catching one.
    #[test]
    fn test_the_raw_key_carries_the_prefix_the_lifecycle_rule_matches() {
        let date = NaiveDate::from_ymd_opt(2026, 8, 28).unwrap();
        assert_eq!(
            raw_key("quotes", date),
            "data/raw/massive/equity/quotes/schema=v1/year=2026/month=08/day=28/data.csv.gz"
        );
        assert_eq!(
            raw_key("trades", NaiveDate::from_ymd_opt(2021, 1, 4).unwrap()),
            "data/raw/massive/equity/trades/schema=v1/year=2021/month=01/day=04/data.csv.gz"
        );
    }

    /// One `read` may stop short of the buffer, and a multipart part that is short without being the
    /// last one fails the whole upload — so the fill has to loop rather than trust a single read.
    #[test]
    fn test_a_part_is_filled_across_short_reads() {
        let directory = std::env::temp_dir().join("fund-tee-part-test");
        std::fs::create_dir_all(&directory).unwrap();
        let path = directory.join("staged.bin");
        let bytes: Vec<u8> = (0..5000u32).map(|value| (value % 251) as u8).collect();
        std::fs::write(&path, &bytes).unwrap();

        let mut file = std::fs::File::open(&path).unwrap();
        let mut buffer = vec![0u8; 4096];
        assert_eq!(read_fully(&mut file, &mut buffer).unwrap(), 4096);
        assert_eq!(buffer[..], bytes[..4096]);
        // The tail is shorter than the buffer, which is the only legitimate short part.
        assert_eq!(read_fully(&mut file, &mut buffer).unwrap(), 904);
        assert_eq!(buffer[..904], bytes[4096..]);
        assert_eq!(read_fully(&mut file, &mut buffer).unwrap(), 0);

        std::fs::remove_dir_all(&directory).ok();
    }

    /// A staging path has to be one file per object: the vendor's key contains slashes, and joining
    /// it unflattened would write into directories that do not exist and collide across datasets.
    #[test]
    fn test_a_staging_path_is_one_flat_file_per_object() {
        let tee = RawTee::new(
            S3Client::from_conf(
                aws_sdk_s3::Config::builder()
                    .behavior_version(aws_sdk_s3::config::BehaviorVersion::latest())
                    .build(),
            ),
            "oscm-fund-archive".to_string(),
            PathBuf::from("/var/tmp/fund-flat-files"),
        );
        let path = tee.staging_path("us_stocks_sip/quotes_v1/2026/08/2026-08-28.csv.gz");
        let rendered = path.to_str().expect("a printable path");
        assert!(
            rendered.starts_with(
                "/var/tmp/fund-flat-files/us_stocks_sip_quotes_v1_2026_08_2026-08-28.csv.gz."
            ),
            "the vendor's key must flatten into one filename: {rendered}"
        );
        // The suffix is the process, which is what stops two passes over one session on one host
        // from truncating each other. Read back rather than restated, since a literal cannot name a
        // process id — but the assertion above pins everything that is not the process.
        assert_eq!(
            rendered
                .rsplit('.')
                .next()
                .and_then(|tail| tail.parse().ok()),
            Some(std::process::id()),
            "{rendered}"
        );
        assert_eq!(
            path.parent(),
            Some(Path::new("/var/tmp/fund-flat-files")),
            "one directory, so a run cannot scatter staged objects"
        );
    }

    /// The chunks arrive as separate reads and have to read back as one stream, because gzip decodes
    /// only forwards and a boundary landing mid-token would corrupt the parse rather than fail it.
    #[test]
    fn test_ordered_chunks_read_back_as_one_stream() {
        let (sender, receiver) = tokio::sync::mpsc::channel(4);
        for chunk in ["ticker,sip", "_timestamp\nAAPL", ",1773066600000000000\n"] {
            sender
                .blocking_send(Ok(chunk.as_bytes().to_vec()))
                .expect("the receiver is alive");
        }
        drop(sender);

        let mut read = String::new();
        std::io::Read::read_to_string(&mut ChunkReader::new(receiver), &mut read)
            .expect("the chunks concatenate");
        assert_eq!(read, "ticker,sip_timestamp\nAAPL,1773066600000000000\n");
    }

    /// A failed range must reach the parser as an error rather than as a short read, which would
    /// look like a truncated file and produce a summary of whatever arrived before it.
    #[test]
    fn test_a_failed_range_surfaces_rather_than_truncating() {
        let (sender, receiver) = tokio::sync::mpsc::channel(4);
        sender
            .blocking_send(Ok(b"ticker,sip_timestamp\n".to_vec()))
            .expect("the receiver is alive");
        sender
            .blocking_send(Err(FlatFileError::Empty { field: "object" }))
            .expect("the receiver is alive");
        drop(sender);

        let mut read = String::new();
        let error = std::io::Read::read_to_string(&mut ChunkReader::new(receiver), &mut read)
            .expect_err("the failed range");
        assert_eq!(error.kind(), std::io::ErrorKind::Other);
    }

    /// The measurement the probe exists to take, because it decides whether the backfill holds one
    /// name's ticks or every name's — thirteen megabytes against several gigabytes.
    #[test]
    fn test_the_layout_is_read_off_where_the_ticker_changes() {
        let ticker_major = format!(
            "{HEADER}\n{}{}{}{}",
            row("AAPL", "100.05", "200", "99.95", "100", stamp(100)),
            row("AAPL", "100.05", "200", "99.95", "100", stamp(200)),
            row("CBOE", "200.20", "50", "199.80", "40", stamp(1)),
            row("CBOE", "200.20", "50", "199.80", "40", stamp(2)),
        );
        let summary = fold(&ticker_major).0.expect("each ticker ascends");
        assert_eq!(summary.ticker_runs, 2, "one run per name");
        assert_eq!(summary.layout(), Some(RowLayout::TickerMajor));

        let time_major = format!(
            "{HEADER}\n{}{}{}{}",
            row("AAPL", "100.05", "200", "99.95", "100", stamp(0)),
            row("CBOE", "200.20", "50", "199.80", "40", stamp(1)),
            row("AAPL", "100.05", "200", "99.95", "100", stamp(2)),
            row("CBOE", "200.20", "50", "199.80", "40", stamp(3)),
        );
        let summary = fold(&time_major).0.expect("interleaved and ascending");
        assert_eq!(summary.ticker_runs, 4, "every row starts a run");
        assert_eq!(summary.layout(), Some(RowLayout::TimeMajor));
    }

    /// A ticker-major file still puts two real names in two runs each — BCPC and TPC, on every
    /// session measured. A fold that releases at the switch is wrong for exactly those, so the names
    /// are reported rather than the count alone: two out of thirteen thousand is invisible in a
    /// total and actionable as a list.
    #[test]
    fn test_a_name_returning_after_its_run_is_named_not_just_counted() {
        let body = format!(
            "{HEADER}\n{}{}{}",
            row("AAPL", "100.05", "200", "99.95", "100", stamp(0)),
            row("CBOE", "200.20", "50", "199.80", "40", stamp(1)),
            row("AAPL", "100.02", "300", "99.98", "150", stamp(2)),
        );
        let summary = fold(&body).0.expect("a usable file");

        assert_eq!(summary.tickers, 2);
        assert_eq!(summary.ticker_runs, 3, "AAPL opens two of them");
        assert_eq!(summary.split_tickers.len(), 1);
        assert_eq!(
            summary
                .split_tickers
                .names()
                .map(|ticker| ticker.as_str())
                .collect::<Vec<_>>(),
            vec!["AAPL"]
        );

        let contiguous = format!(
            "{HEADER}\n{}{}",
            row("AAPL", "100.05", "200", "99.95", "100", stamp(0)),
            row("CBOE", "200.20", "50", "199.80", "40", stamp(1)),
        );
        let summary = fold(&contiguous).0.expect("a usable file");
        assert!(summary.split_tickers.is_empty());
    }

    /// The layout decides whether the backfill holds one name's ticks or every name's, so a file
    /// that folded nothing must report no layout rather than the expensive one by arithmetic.
    #[test]
    fn test_a_file_that_folded_nothing_reports_no_layout() {
        let header_only = fold(&format!("{HEADER}\n")).0.expect("an empty file");
        assert_eq!(header_only.rows_read, 0);
        assert_eq!(header_only.ticks_folded, 0);
        assert_eq!(header_only.layout(), None);

        // Rows that parse but carry no readable book: the guard is on what was folded, not on what
        // was read.
        let all_unusable = format!(
            "{HEADER}\n{}{}",
            row("AAPL", "0", "200", "0", "100", stamp(0)),
            row("CBOE", "0", "50", "0", "40", stamp(1)),
        );
        let summary = fold(&all_unusable).0.expect("a file of unusable rows");
        assert_eq!(summary.rows_read, 2);
        assert_eq!(summary.ticks_folded, 0);
        assert_eq!(summary.unusable, 2);
        assert_eq!(summary.layout(), None);
    }

    /// The column names are a guess and so are the units. A stamp in the wrong one converts without
    /// complaint and lands in 1970, where it would fold as a usable tick and weigh a whole session.
    #[test]
    fn test_a_stamp_in_the_wrong_unit_is_unusable_rather_than_a_1970_tick() {
        let milliseconds = stamp(0) / 1_000_000;
        let body = format!(
            "{HEADER}\n{}{}",
            row("AAPL", "100.05", "200", "99.95", "100", milliseconds),
            row("AAPL", "100.05", "200", "99.95", "100", stamp(0)),
        );
        let (summary, seen) = fold(&body);
        let summary = summary.expect("one usable row");

        assert_eq!(summary.rows_read, 2);
        assert_eq!(summary.ticks_folded, 1);
        assert_eq!(summary.unusable, 1, "the millisecond stamp");
        assert_eq!(seen, vec![("AAPL".to_string(), 1_773_066_600_000_000_000)]);
    }

    #[test]
    fn test_credentials_reject_an_empty_field() {
        assert!(
            FlatFileCredentials::new(String::new(), "key".to_string(), "secret".to_string())
                .is_err()
        );
        assert!(FlatFileCredentials::new(
            "https://files.massive.com".to_string(),
            String::new(),
            "secret".to_string()
        )
        .is_err());
        assert!(FlatFileCredentials::new(
            "https://files.massive.com".to_string(),
            "key".to_string(),
            String::new()
        )
        .is_err());
        assert!(FlatFileCredentials::new(
            "https://files.massive.com".to_string(),
            "key".to_string(),
            "secret".to_string()
        )
        .is_ok());
    }
}
