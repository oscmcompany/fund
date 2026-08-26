//! Massive's flat files: one gzipped CSV per day, streamed and handed to a fold row by row.
//!
//! A separate endpoint and separate credentials from [`crate::common::massive`]'s REST API, and not
//! AWS despite speaking S3. Nothing reaches local disk; nothing holds the file.

use std::collections::BTreeMap;

use aws_sdk_s3::Client as S3Client;
use chrono::{DateTime, NaiveDate, TimeZone, Utc};
use flate2::read::GzDecoder;
use tokio_util::io::SyncIoBridge;
use tracing::{info, warn};

use crate::common::alpaca::QuoteTick;
use crate::common::types::Ticker;

/// The bucket every dataset lives under, which Massive support named on 2026-08-24.
const FLAT_FILE_BUCKET: &str = "flatfiles";

/// The region the signer needs. Massive is not AWS, so nothing resolves this from an instance or a
/// profile — it is a constant the signature requires rather than a place anything is stored.
const FLAT_FILE_REGION: &str = "us-east-1";

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
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct QuoteFileFold {
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

impl std::fmt::Display for RowLayout {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            RowLayout::TickerMajor => "ticker-major",
            RowLayout::TimeMajor => "time-major",
        })
    }
}

impl QuoteFileFold {
    /// Which way the file is grouped.
    ///
    /// Ticker-major puts each name in one contiguous run, so runs equal names; time-major
    /// interleaves, so nearly every row starts one. On a real day those are twelve thousand against
    /// four hundred million, so what separates them is which end the count sits nearer — there is no
    /// threshold for anyone to choose, and nothing real lands in between.
    pub fn layout(&self) -> RowLayout {
        let above_tickers = self.ticker_runs.saturating_sub(self.tickers);
        let below_rows = self.rows_read.saturating_sub(self.ticker_runs);
        if above_tickers < below_rows {
            RowLayout::TickerMajor
        } else {
            RowLayout::TimeMajor
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
    /// Nothing is collected and nothing touches disk: `GetObject` gives a byte stream, which is
    /// decompressed, parsed and folded as it arrives. The whole pipeline runs on a blocking thread
    /// because decompression and CSV parsing are the CPU cost of the download, not a wait.
    pub async fn fold_quotes<F>(
        &self,
        date: NaiveDate,
        fold: F,
    ) -> Result<QuoteFileFold, FlatFileError>
    where
        F: FnMut(Ticker, QuoteTick) + Send + 'static,
    {
        let key = quote_key(date);
        info!(bucket = FLAT_FILE_BUCKET, key, %date, "Reading a flat file of quotes");

        let object = self
            .s3_client
            .get_object()
            .bucket(FLAT_FILE_BUCKET)
            .key(&key)
            .send()
            .await
            .map_err(|error| FlatFileError::Fetch {
                key: key.clone(),
                source: Box::new(error),
            })?;

        let compressed_bytes = object.content_length().unwrap_or_default();
        let reader = object.body.into_async_read();
        let scoped = key.clone();
        let mut summary = tokio::task::spawn_blocking(move || {
            fold_gzipped_quotes(SyncIoBridge::new(reader), &scoped, fold)
        })
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
            layout = %summary.layout(),
            compressed_bytes,
            "Folded a flat file of quotes"
        );
        Ok(summary)
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

/// Massive stamps a SIP quote in nanoseconds since the epoch.
///
/// Nanoseconds rather than millis, which is the same distinction that cost AAPL 211 seconds of a
/// session on the Alpaca path when an interval was truncated to whole milliseconds.
fn nanoseconds_to_instant(nanoseconds: i64) -> Option<DateTime<Utc>> {
    Utc.timestamp_nanos(nanoseconds).into()
}

/// Decompresses, parses and folds, on whichever thread the caller put this on.
///
/// Separate from the request so it can be tested against a file that never went over a network.
fn fold_gzipped_quotes<R, F>(
    reader: R,
    key: &str,
    mut fold: F,
) -> Result<QuoteFileFold, FlatFileError>
where
    R: std::io::Read,
    F: FnMut(Ticker, QuoteTick),
{
    let mut records = csv::Reader::from_reader(GzDecoder::new(reader));
    let header = records.headers().map_err(|error| read_error(key, error))?;
    let columns = QuoteColumns::resolve(header, key)?;

    let mut summary = QuoteFileFold::default();
    let mut latest: BTreeMap<Ticker, DateTime<Utc>> = BTreeMap::new();
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
        match latest.insert(ticker.clone(), tick.timestamp()) {
            Some(previous) if tick.timestamp() < previous => summary.backwards += 1,
            Some(_) => {}
            None => summary.tickers += 1,
        }
        if previous_ticker.as_ref() != Some(&ticker) {
            summary.ticker_runs += 1;
            previous_ticker = Some(ticker.clone());
        }
        summary.ticks_folded += 1;
        fold(ticker, tick);
    }

    // Asserted here rather than by the caller, because this is what read the file and so this is
    // what can answer for it. A caller that gets an error must discard whatever the fold accumulated:
    // the rows were handed over before the order was known, which is the nature of a single pass.
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
    Ok(summary)
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
            move |ticker, tick| {
                collector.borrow_mut().push((
                    ticker.as_str().to_string(),
                    tick.timestamp().timestamp_nanos_opt().unwrap_or_default(),
                ));
            },
        );
        let observed = seen.borrow().clone();
        (summary, observed)
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
            row("AAPL", "100.05", "200", "99.95", "100", 1_000_000_000),
            row("CBOE", "200.20", "50", "199.80", "40", 1_000_000_001),
            row("AAPL", "100.02", "300", "99.98", "150", 1_000_000_002),
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
                ("AAPL".to_string(), 1_000_000_000),
                ("CBOE".to_string(), 1_000_000_001),
                ("AAPL".to_string(), 1_000_000_002),
            ]
        );
    }

    /// The column order is undocumented and could not be checked before the subscription was
    /// bought, so the header is what says where each field is. Reordering it must change nothing.
    #[test]
    fn test_the_columns_are_read_off_the_header_rather_than_by_position() {
        let reordered = "sip_timestamp,bid_size,bid_price,ask_size,ask_price,ticker";
        let body = format!("{reordered}\n1000000000,100,99.95,200,100.05,AAPL\n");
        let (summary, seen) = fold(&body);

        assert_eq!(summary.expect("a usable file").ticks_folded, 1);
        assert_eq!(seen, vec![("AAPL".to_string(), 1_000_000_000)]);
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
            row("AAPL", "100.05", "200", "99.95", "100", 1_000_000_000),
            row("AAPL", "99.90", "200", "100.10", "100", 1_000_000_001),
            row("AAPL", "0", "200", "0", "100", 1_000_000_002),
            row("AAPL", "100.05", "200", "not-a-price", "100", 1_000_000_003),
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
            .map(|index| {
                row(
                    "AAPL",
                    "100.05",
                    "200",
                    "99.95",
                    "100",
                    1_000_000_000 - index,
                )
            })
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
            body.push_str(&row(
                "AAPL",
                "100.05",
                "200",
                "99.95",
                "100",
                1_000_000_000 + index,
            ));
        }
        // Three quotes the SIP reordered, which is ordinary rather than a wrongly sorted file.
        for index in [5, 9, 14] {
            body.push_str(&row(
                "AAPL",
                "100.05",
                "200",
                "99.95",
                "100",
                1_000_000_000 + index,
            ));
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
            row("AAPL", "100.05", "200", "99.95", "100", 1_000_000_100),
            row("AAPL", "100.05", "200", "99.95", "100", 1_000_000_200),
            row("CBOE", "200.20", "50", "199.80", "40", 1_000_000_001),
            row("CBOE", "200.20", "50", "199.80", "40", 1_000_000_002),
        );
        let summary = fold(&body).0.expect("each ticker ascends");
        assert_eq!(summary.ticks_folded, 4);
        assert_eq!(summary.tickers, 2);
    }

    /// Serves `body` for any `GET`, and refuses anything else so a change of verb is visible.
    fn client_serving(
        body: Vec<u8>,
    ) -> (
        FlatFileClient,
        std::sync::Arc<std::sync::Mutex<Vec<String>>>,
    ) {
        let requested = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let recorder = std::sync::Arc::clone(&requested);
        let http_client = infallible_client_fn(move |request| {
            recorder
                .lock()
                .expect("no test thread panics holding this")
                .push(request.uri().path().to_string());
            match *request.method() {
                http::Method::GET => http::Response::builder()
                    .status(200)
                    .body(SdkBody::from(body.clone()))
                    .expect("a valid response"),
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
            row("AAPL", "100.05", "200", "99.95", "100", 1_000_000_000),
            row("CBOE", "200.20", "50", "199.80", "40", 1_000_000_001),
        );
        let (client, requested) = client_serving(gzipped(&body));
        let folded = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let collector = std::sync::Arc::clone(&folded);

        let summary = client
            .fold_quotes(
                NaiveDate::from_ymd_opt(2026, 3, 9).expect("a real date"),
                move |ticker, _tick| {
                    collector
                        .lock()
                        .expect("no fold thread panics holding this")
                        .push(ticker.as_str().to_string());
                },
            )
            .await
            .expect("a usable file");

        assert_eq!(summary.ticks_folded, 2);
        assert_eq!(summary.tickers, 2);
        assert_eq!(
            *folded.lock().expect("the fold finished"),
            vec!["AAPL".to_string(), "CBOE".to_string()]
        );

        // Path-style, because the endpoint is not AWS: a virtual-host bucket would resolve
        // `flatfiles.files.massive.com`, which does not exist.
        assert_eq!(
            *requested.lock().expect("the request was recorded"),
            vec!["/flatfiles/us_stocks_sip/quotes_v1/2026/03/2026-03-09.csv.gz".to_string()]
        );
    }

    /// The measurement the probe exists to take, because it decides whether the backfill holds one
    /// name's ticks or every name's — thirteen megabytes against several gigabytes.
    #[test]
    fn test_the_layout_is_read_off_where_the_ticker_changes() {
        let ticker_major = format!(
            "{HEADER}\n{}{}{}{}",
            row("AAPL", "100.05", "200", "99.95", "100", 1_000_000_100),
            row("AAPL", "100.05", "200", "99.95", "100", 1_000_000_200),
            row("CBOE", "200.20", "50", "199.80", "40", 1_000_000_001),
            row("CBOE", "200.20", "50", "199.80", "40", 1_000_000_002),
        );
        let summary = fold(&ticker_major).0.expect("each ticker ascends");
        assert_eq!(summary.ticker_runs, 2, "one run per name");
        assert_eq!(summary.layout(), RowLayout::TickerMajor);

        let time_major = format!(
            "{HEADER}\n{}{}{}{}",
            row("AAPL", "100.05", "200", "99.95", "100", 1_000_000_000),
            row("CBOE", "200.20", "50", "199.80", "40", 1_000_000_001),
            row("AAPL", "100.05", "200", "99.95", "100", 1_000_000_002),
            row("CBOE", "200.20", "50", "199.80", "40", 1_000_000_003),
        );
        let summary = fold(&time_major).0.expect("interleaved and ascending");
        assert_eq!(summary.ticker_runs, 4, "every row starts a run");
        assert_eq!(summary.layout(), RowLayout::TimeMajor);
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
