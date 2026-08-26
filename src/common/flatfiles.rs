//! Massive's flat files: one gzipped CSV per day, streamed and handed to a fold row by row.
//!
//! Separate endpoint and credentials from [`crate::common::massive`]; nothing holds the file.

use std::collections::HashMap;

use aws_sdk_s3::Client as S3Client;
use chrono::{DateTime, NaiveDate, TimeZone, Utc};
use flate2::read::GzDecoder;
use tracing::{info, warn};

use crate::common::alpaca::QuoteTick;
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
    /// More quotes stepped backwards in time than stepped forwards, which is what a file sorted the
    /// wrong way looks like. See [`QuoteFileFold::require_ascending`].
    #[error(
        "{key} is not in ascending time within a ticker: {backwards} quotes stepped back against \
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

/// What one file cost and what it yielded.
///
/// `unusable` counts rows whose book no spread can be read off — a crossed or zero-priced quote,
/// which is ordinary around the open rather than a defect. Reported because it is the only trace a
/// discarded row leaves, and a file that is suddenly half unusable is a vendor change.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct QuoteFileFold {
    /// Names whose rows are not contiguous, which is what a fold releasing at the switch must know.
    pub split_tickers: SplitTickers,
    pub rows_read: usize,
    pub ticks_folded: usize,
    pub unusable: usize,
    pub tickers: usize,
    /// Rows whose ticker differs from the row before, which is what [`QuoteFileFold::layout`] reads.
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

impl QuoteFileFold {
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

    /// Refuses a file whose quotes descend in time within a ticker.
    ///
    /// The fold weighs each quote by the interval to the next, so a descending run weighs every one
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
pub struct FlatFileClient {
    s3_client: S3Client,
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
        }
    }

    /// Constructs from the environment.
    pub fn from_env() -> Result<Self, FlatFileError> {
        Ok(Self::new(FlatFileCredentials::from_env()?))
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
    ) -> Result<(QuoteFileFold, S), FlatFileError> {
        let key = quote_key(date);
        info!(bucket = FLAT_FILE_BUCKET, key, %date, "Reading a flat file of quotes");

        let compressed_bytes = self.object_length(&key).await?;
        let reader = self.ranged_reader(&key, compressed_bytes);
        let scoped = key.clone();
        let (mut summary, fold) =
            tokio::task::spawn_blocking(move || fold_gzipped_quotes(reader, &scoped, fold))
                .await
                .map_err(|error| FlatFileError::Read {
                    key: key.clone(),
                    source: std::io::Error::other(error),
                })??;

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
    fn ranged_reader(&self, key: &str, length: i64) -> ChunkReader {
        let (sender, receiver) = tokio::sync::mpsc::channel(READY_CHUNKS);
        let client = self.s3_client.clone();
        let key = key.to_string();
        tokio::spawn(async move { fetch_ranges(client, key, length, sender).await });
        ChunkReader::new(receiver)
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
) {
    let mut in_flight: std::collections::VecDeque<
        tokio::task::JoinHandle<Result<Vec<u8>, FlatFileError>>,
    > = std::collections::VecDeque::with_capacity(RANGES_IN_FLIGHT);
    let mut next_offset = 0i64;

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
            return;
        };
        let chunk = match oldest.await {
            Ok(chunk) => chunk,
            Err(error) => Err(FlatFileError::Read {
                key: key.clone(),
                source: std::io::Error::other(error),
            }),
        };
        // A closed receiver means the parse stopped early, which is its answer rather than an error.
        // Dropping a handle does not cancel its request, so the rest are stopped rather than left
        // downloading megabytes nothing will read.
        if sender.send(chunk).await.is_err() {
            for pending in in_flight {
                pending.abort();
            }
            return;
        }
    }
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
        let index = |column: &'static str| {
            header
                .iter()
                .position(|found| found.trim() == column)
                .ok_or_else(|| FlatFileError::Column {
                    key: key.to_string(),
                    column,
                    header: header.iter().collect::<Vec<_>>().join(","),
                })
        };
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

/// Decompresses, parses and folds, on whichever thread the caller put this on.
///
/// Separate from the request so it can be tested against a file that never went over a network.
fn fold_gzipped_quotes<R, S>(
    reader: R,
    key: &str,
    mut fold: S,
) -> Result<(QuoteFileFold, S), FlatFileError>
where
    R: std::io::Read,
    S: QuoteSink,
{
    let mut records = csv::Reader::from_reader(GzDecoder::new(reader));
    let header = records.headers().map_err(|error| read_error(key, error))?;
    let columns = QuoteColumns::resolve(header, key)?;

    let mut summary = QuoteFileFold::default();
    let mut latest: HashMap<Ticker, DateTime<Utc>> = HashMap::new();
    let mut previous_ticker: Option<Ticker> = None;
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
        // An owned key is needed only the first time a name appears, and a day is hundreds of
        // millions of rows.
        let seen_before = match latest.get_mut(&ticker) {
            Some(previous) => {
                if tick.timestamp() < *previous {
                    summary.backwards += 1;
                }
                *previous = tick.timestamp();
                true
            }
            None => {
                latest.insert(ticker.clone(), tick.timestamp());
                summary.tickers += 1;
                false
            }
        };
        if previous_ticker.as_ref() != Some(&ticker) {
            summary.ticker_runs += 1;
            // Already seen, and yet starting a run: its rows are split across the file.
            if seen_before {
                summary.split_tickers.0.insert(ticker.clone());
            }
            previous_ticker = Some(ticker.clone());
        }
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
    fn fold(body: &str) -> (Result<QuoteFileFold, FlatFileError>, Vec<(String, i64)>) {
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
