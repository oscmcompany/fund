//! Massive's flat files: one gzipped CSV per dataset per session, served from Massive's own S3 endpoint under the
//! Stocks Advanced keys, which lapse on 2026-10-26.

use aws_sdk_s3::config::retry::RetryConfig;
use aws_sdk_s3::config::{
    Credentials, Region, RequestChecksumCalculation, ResponseChecksumValidation,
};
use bytes::Bytes;
use chrono::{DateTime, NaiveDate, Utc};
use serde::Deserialize;

use super::massive::{alpaca_symbol, is_exchange_test_ticker};
use super::{Accepted, RefusedRow, RowRefusal, VariableRefusal, variable};
use crate::common::market::record::{Bar, BarInterval, Ohlc, Quote, Trade};
use crate::common::market::{Price, Shares, Symbol, TradeCount};
use crate::common::storage::{Key, Provider};
use crate::common::time::SessionDate;

const BUCKET: &str = "flatfiles";

/// The SDK's own retries, each with backoff, before a request is reported failed.
const ATTEMPTS: u32 = 10;

/// A flat-file dataset, named in our terms; its vendor prefix appears only here.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    strum::Display,
    strum::EnumString,
    strum::IntoStaticStr,
    strum::EnumIter,
)]
#[strum(serialize_all = "snake_case")]
pub enum FlatFileDataset {
    DailyBars,
    MinuteBars,
    Quotes,
    Trades,
}

impl FlatFileDataset {
    fn prefix(self) -> &'static str {
        match self {
            Self::DailyBars => "us_stocks_sip/day_aggs_v1/",
            Self::MinuteBars => "us_stocks_sip/minute_aggs_v1/",
            Self::Quotes => "us_stocks_sip/quotes_v1/",
            Self::Trades => "us_stocks_sip/trades_v1/",
        }
    }

    fn path(self, session: SessionDate) -> String {
        let date = session.date();
        format!("{}{}/{date}.csv.gz", self.prefix(), date.format("%Y/%m"))
    }

    /// Where the archive keeps this dataset's file for `session`.
    pub fn key(self, session: SessionDate) -> Key {
        let provider = Provider::Massive;
        match self {
            Self::DailyBars => Key::RawBars {
                provider,
                interval: BarInterval::OneDay,
                session,
            },
            Self::MinuteBars => Key::RawBars {
                provider,
                interval: BarInterval::OneMinute,
                session,
            },
            Self::Quotes => Key::RawQuotes { provider, session },
            Self::Trades => Key::RawTrades { provider, session },
        }
    }

    /// The legacy archiver's copy of the same file; archive task A6 deletes it once every session is verified.
    pub fn legacy_path(self, session: SessionDate) -> String {
        format!(
            "{}{}/data.csv.gz",
            self.legacy_prefix(),
            crate::common::storage::date_partition(session)
        )
    }

    /// The prefix every session of the legacy archiver's copies shares.
    pub fn legacy_prefix(self) -> String {
        let segment = match self {
            Self::DailyBars => "day_aggs",
            Self::MinuteBars => "minute_aggs",
            Self::Quotes => "quotes",
            Self::Trades => "trades",
        };
        format!("data/raw/massive/equity/{segment}/schema=v1/")
    }
}

/// One file Massive serves, with its length in bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Listed {
    session: SessionDate,
    length: u64,
    /// The version listed, which every ranged read must still match so one copy never mixes two versions.
    tag: String,
}

impl Listed {
    pub fn session(&self) -> SessionDate {
        self.session
    }

    pub fn length(&self) -> u64 {
        self.length
    }
}

/// Why a flat-file request produced nothing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FlatFileError {
    List {
        prefix: String,
        reason: String,
    },
    Get {
        path: String,
        reason: String,
    },
    /// A range answered with a different number of bytes than asked for.
    ShortRange {
        path: String,
        asked: u64,
        received: u64,
    },
}

impl std::fmt::Display for FlatFileError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::List { prefix, reason } => write!(formatter, "listing {prefix} failed: {reason}"),
            Self::Get { path, reason } => write!(formatter, "reading {path} failed: {reason}"),
            Self::ShortRange {
                path,
                asked,
                received,
            } => write!(
                formatter,
                "{path} answered {received} bytes where {asked} were asked for"
            ),
        }
    }
}

#[derive(Clone)]
pub struct FlatFiles {
    s3_client: aws_sdk_s3::Client,
}

impl FlatFiles {
    /// Reads `MASSIVE_S3_ENDPOINT`, `MASSIVE_S3_ACCESS_KEY_ID` and `MASSIVE_S3_SECRET_ACCESS_KEY`.
    pub fn from_environment() -> Result<Self, VariableRefusal> {
        let credentials = Credentials::new(
            variable("MASSIVE_S3_ACCESS_KEY_ID")?,
            variable("MASSIVE_S3_SECRET_ACCESS_KEY")?,
            None,
            None,
            "massive",
        );
        // Massive's endpoint is not AWS: it takes path-style requests and sends no checksums to validate.
        let configuration = aws_sdk_s3::Config::builder()
            .behavior_version_latest()
            .endpoint_url(variable("MASSIVE_S3_ENDPOINT")?)
            .region(Region::new("us-east-1"))
            .credentials_provider(credentials)
            .force_path_style(true)
            .request_checksum_calculation(RequestChecksumCalculation::WhenRequired)
            .response_checksum_validation(ResponseChecksumValidation::WhenRequired)
            .retry_config(RetryConfig::standard().with_max_attempts(ATTEMPTS))
            .build();
        Ok(Self {
            s3_client: aws_sdk_s3::Client::from_conf(configuration),
        })
    }

    /// Every session Massive serves for `dataset`, in session order.
    pub async fn listing(&self, dataset: FlatFileDataset) -> Result<Vec<Listed>, FlatFileError> {
        let prefix = dataset.prefix();
        let failed = |reason: String| FlatFileError::List {
            prefix: prefix.to_string(),
            reason,
        };
        let mut pages = self
            .s3_client
            .list_objects_v2()
            .bucket(BUCKET)
            .prefix(prefix)
            .into_paginator()
            .send();
        let mut listed = Vec::new();
        while let Some(page) = pages.next().await {
            let page = page.map_err(|error| {
                failed(aws_sdk_s3::error::DisplayErrorContext(error).to_string())
            })?;
            for object in page.contents() {
                let path = object.key().unwrap_or_default();
                let session = path
                    .rsplit('/')
                    .next()
                    .and_then(|name| name.strip_suffix(".csv.gz"))
                    .and_then(|date| NaiveDate::parse_from_str(date, "%Y-%m-%d").ok())
                    .map(SessionDate::from_date)
                    .filter(|session| dataset.path(*session) == path)
                    .ok_or_else(|| failed(format!("{path} names no session")))?;
                let length = object
                    .size()
                    .and_then(|size| u64::try_from(size).ok())
                    .ok_or_else(|| failed(format!("{path} has no length")))?;
                let tag = object
                    .e_tag()
                    .ok_or_else(|| failed(format!("{path} has no entity tag")))?
                    .to_string();
                listed.push(Listed {
                    session,
                    length,
                    tag,
                });
            }
        }
        listed.sort_by_key(Listed::session);
        Ok(listed)
    }

    /// The `length` bytes of `dataset`'s file for `session` starting at `start`.
    pub async fn range(
        &self,
        dataset: FlatFileDataset,
        listed: &Listed,
        start: u64,
        length: u64,
    ) -> Result<Bytes, FlatFileError> {
        let path = dataset.path(listed.session);
        let failed = |reason: String| FlatFileError::Get {
            path: path.clone(),
            reason,
        };
        let response = self
            .s3_client
            .get_object()
            .bucket(BUCKET)
            .key(&path)
            .range(format!("bytes={start}-{}", start + length - 1))
            .if_match(&listed.tag)
            .send()
            .await
            .map_err(|error| failed(aws_sdk_s3::error::DisplayErrorContext(error).to_string()))?;
        let body = response
            .body
            .collect()
            .await
            .map_err(|error| failed(error.to_string()))?
            .into_bytes();
        if body.len() as u64 != length {
            return Err(FlatFileError::ShortRange {
                path,
                asked: length,
                received: body.len() as u64,
            });
        }
        Ok(body)
    }
}

/// One session's bars from a flat bar file, with every row that did not become one: each row is exactly one of a bar, a
/// test ticker or a refusal.
#[derive(Debug, Clone, PartialEq)]
pub struct FlatFileBars {
    bars: Vec<Bar>,
    test_tickers: Vec<String>,
    refused: Vec<RefusedRow>,
}

impl FlatFileBars {
    /// In symbol, then timestamp, order.
    pub fn bars(&self) -> &[Bar] {
        &self.bars
    }

    pub fn test_tickers(&self) -> &[String] {
        &self.test_tickers
    }

    pub fn refused(&self) -> &[RefusedRow] {
        &self.refused
    }
}

/// Why a flat bar file was not read at all.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ParseRefusal {
    /// Only bar files parse into bars.
    NotBars { dataset: FlatFileDataset },
    /// A gzip or CSV error, with the line it stopped on where the reader knows it.
    Malformed { line: Option<u64>, reason: String },
}

impl std::fmt::Display for ParseRefusal {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotBars { dataset } => write!(formatter, "{dataset} is not a bar file"),
            Self::Malformed { line, reason } => {
                write!(formatter, "malformed at line {line:?}: {reason}")
            }
        }
    }
}

/// A flat bar file's row, which carries no volume-weighted price, so its bars carry no dollar volume.
#[derive(Deserialize)]
struct BarRow {
    ticker: String,
    volume: f64,
    open: f64,
    close: f64,
    high: f64,
    low: f64,
    /// Nanoseconds since the epoch: the minute's start, or midnight Eastern for a daily row.
    window_start: i64,
    transactions: Option<u64>,
}

impl FlatFileDataset {
    /// Reads `gzipped`, this dataset's file for `session`, into bars in our notation; exchange test tickers are set
    /// aside and every other row that cannot become a bar is refused with its cause.
    pub fn parse_bars(
        self,
        gzipped: &[u8],
        session: SessionDate,
    ) -> Result<FlatFileBars, ParseRefusal> {
        let interval = match self {
            Self::DailyBars => BarInterval::OneDay,
            Self::MinuteBars => BarInterval::OneMinute,
            Self::Quotes | Self::Trades => return Err(ParseRefusal::NotBars { dataset: self }),
        };
        let mut reader = csv::Reader::from_reader(flate2::read::GzDecoder::new(gzipped));
        let mut test_tickers = Vec::new();
        // Keyed by symbol and instant, so two tickers the notation map sends to one symbol keep neither.
        let mut accepted: Accepted<(Symbol, DateTime<Utc>)> = Accepted::new();
        for row in reader.deserialize::<BarRow>() {
            let row = row.map_err(|error| ParseRefusal::Malformed {
                line: error.position().map(csv::Position::line),
                reason: error.to_string(),
            })?;
            if is_exchange_test_ticker(&row.ticker) {
                test_tickers.push(row.ticker);
                continue;
            }
            match flat_file_bar(&row, interval, session) {
                Ok(bar) => accepted.offer((bar.symbol().clone(), bar.timestamp()), row.ticker, bar),
                Err(cause) => accepted.refuse(row.ticker, cause),
            }
        }
        let (bars, refused) = accepted.finish();
        Ok(FlatFileBars {
            bars,
            test_tickers,
            refused,
        })
    }
}

fn flat_file_bar(
    row: &BarRow,
    interval: BarInterval,
    session: SessionDate,
) -> Result<Bar, RowRefusal> {
    let symbol = alpaca_symbol(&row.ticker).map_err(RowRefusal::Symbol)?;
    let start = DateTime::from_timestamp_nanos(row.window_start);
    if SessionDate::at(start) != session {
        return Err(RowRefusal::Session {
            timestamp: row.window_start.to_string(),
        });
    }
    // A daily bar is stamped at the close, as every daily bar in the archive is.
    let timestamp = match interval {
        BarInterval::OneDay => session.regular_close(),
        BarInterval::OneMinute | BarInterval::FiveMinute => start,
    };
    let price = |dollars: f64| Price::from_dollars(dollars).map_err(RowRefusal::Price);
    let prices = Ohlc::new(
        price(row.open)?,
        price(row.high)?,
        price(row.low)?,
        price(row.close)?,
    )
    .map_err(RowRefusal::Prices)?;
    let volume = Shares::from_float(row.volume).map_err(RowRefusal::Shares)?;
    Bar::new(
        symbol,
        interval,
        timestamp,
        prices,
        volume,
        row.transactions.map(TradeCount::new),
        None,
    )
    .map_err(RowRefusal::Bar)
}

/// Bytes per ranged read when a file is streamed rather than copied.
const STREAM_CHUNK: u64 = 16 * 1024 * 1024;

/// A file's bytes in order as a blocking `Read`, with up to `ahead` ranged reads in flight on the runtime; read it
/// from a blocking thread.
pub struct FlatFileStream {
    chunks: tokio::sync::mpsc::Receiver<tokio::task::JoinHandle<Result<Bytes, FlatFileError>>>,
    runtime: tokio::runtime::Handle,
    current: Bytes,
}

impl FlatFiles {
    pub fn stream(
        &self,
        dataset: FlatFileDataset,
        listed: &Listed,
        ahead: usize,
    ) -> FlatFileStream {
        let (sender, chunks) = tokio::sync::mpsc::channel(ahead.max(1));
        let flat_files = self.clone();
        let listed = listed.clone();
        tokio::spawn(async move {
            let mut start = 0;
            while start < listed.length {
                let length = STREAM_CHUNK.min(listed.length - start);
                let (flat_files, listed_chunk) = (flat_files.clone(), listed.clone());
                let fetch = tokio::spawn(async move {
                    flat_files
                        .range(dataset, &listed_chunk, start, length)
                        .await
                });
                // A closed receiver means the reader stopped early, so nothing more is wanted.
                if sender.send(fetch).await.is_err() {
                    return;
                }
                start += length;
            }
        });
        FlatFileStream {
            chunks,
            runtime: tokio::runtime::Handle::current(),
            current: Bytes::new(),
        }
    }
}

impl std::io::Read for FlatFileStream {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        while self.current.is_empty() {
            let Some(fetch) = self.chunks.blocking_recv() else {
                return Ok(0);
            };
            self.current = self
                .runtime
                .block_on(fetch)
                .map_err(std::io::Error::other)?
                .map_err(|error| std::io::Error::other(error.to_string()))?;
        }
        let count = buffer.len().min(self.current.len());
        buffer[..count].copy_from_slice(&self.current.split_to(count));
        Ok(count)
    }
}

/// A flat quote file's row; only the consolidated tape's timestamp and the top of book are read.
#[derive(Deserialize)]
struct QuoteRow {
    ticker: String,
    bid_price: f64,
    bid_size: f64,
    ask_price: f64,
    ask_size: f64,
    /// Nanoseconds since the epoch at which the SIP published the quote.
    sip_timestamp: i64,
}

/// What one quote row became.
#[derive(Debug, Clone, PartialEq)]
pub enum QuoteRowOutcome {
    Quote(Quote),
    TestTicker,
    /// A side with no price, which is no top of book; the quote standing before it keeps standing.
    OneSided,
    Refused(RefusedRow),
}

/// Reads a gzipped flat quote file from `gzipped`, handing each row's outcome to `each` in file order.
pub fn read_quotes(
    gzipped: impl std::io::Read,
    mut each: impl FnMut(QuoteRowOutcome),
) -> Result<(), ParseRefusal> {
    let mut reader = csv::Reader::from_reader(flate2::read::GzDecoder::new(gzipped));
    for row in reader.deserialize::<QuoteRow>() {
        let row = row.map_err(|error| ParseRefusal::Malformed {
            line: error.position().map(csv::Position::line),
            reason: error.to_string(),
        })?;
        each(quote_outcome(row));
    }
    Ok(())
}

fn quote_outcome(row: QuoteRow) -> QuoteRowOutcome {
    if is_exchange_test_ticker(&row.ticker) {
        return QuoteRowOutcome::TestTicker;
    }
    if row.bid_price <= 0.0 || row.ask_price <= 0.0 {
        return QuoteRowOutcome::OneSided;
    }
    let refused = |cause: RowRefusal| {
        QuoteRowOutcome::Refused(RefusedRow {
            ticker: row.ticker.clone(),
            cause,
        })
    };
    let symbol = match alpaca_symbol(&row.ticker) {
        Ok(symbol) => symbol,
        Err(cause) => return refused(RowRefusal::Symbol(cause)),
    };
    let price = |dollars: f64| Price::from_dollars(dollars).map_err(RowRefusal::Price);
    let size = |shares: f64| Shares::from_float(shares).map_err(RowRefusal::Shares);
    let parts = (|| {
        Ok::<_, RowRefusal>((
            price(row.bid_price)?,
            price(row.ask_price)?,
            size(row.bid_size)?,
            size(row.ask_size)?,
        ))
    })();
    match parts {
        Ok((bid, ask, bid_size, ask_size)) => {
            let timestamp = DateTime::from_timestamp_nanos(row.sip_timestamp);
            match Quote::new(symbol, timestamp, bid, ask, bid_size, ask_size) {
                Ok(quote) => QuoteRowOutcome::Quote(quote),
                Err(cause) => refused(RowRefusal::Quote(cause)),
            }
        }
        Err(cause) => refused(cause),
    }
}

/// A flat trade file's row; the condition codes come comma-joined in one field.
#[derive(Deserialize)]
struct TradeRow {
    ticker: String,
    conditions: String,
    /// Zero or blank for an uncorrected print.
    correction: Option<u32>,
    price: f64,
    size: f64,
    /// Nanoseconds since the epoch at which the SIP published the print.
    sip_timestamp: i64,
}

/// What one trade row became.
#[derive(Debug, Clone, PartialEq)]
pub enum TradeRowOutcome {
    Trade {
        trade: Trade,
        conditions: Vec<u16>,
        corrected: bool,
    },
    TestTicker,
    Refused(RefusedRow),
}

/// Reads a gzipped flat trade file from `gzipped`, handing each row's outcome to `each` in file order.
pub fn read_trades(
    gzipped: impl std::io::Read,
    mut each: impl FnMut(TradeRowOutcome),
) -> Result<(), ParseRefusal> {
    let mut reader = csv::Reader::from_reader(flate2::read::GzDecoder::new(gzipped));
    for row in reader.deserialize::<TradeRow>() {
        let row = row.map_err(|error| ParseRefusal::Malformed {
            line: error.position().map(csv::Position::line),
            reason: error.to_string(),
        })?;
        each(trade_outcome(row));
    }
    Ok(())
}

fn trade_outcome(row: TradeRow) -> TradeRowOutcome {
    if is_exchange_test_ticker(&row.ticker) {
        return TradeRowOutcome::TestTicker;
    }
    let refused = |cause: RowRefusal| {
        TradeRowOutcome::Refused(RefusedRow {
            ticker: row.ticker.clone(),
            cause,
        })
    };
    let conditions: Result<Vec<u16>, _> = row
        .conditions
        .split(',')
        .map(str::trim)
        .filter(|code| !code.is_empty())
        .map(str::parse)
        .collect();
    let Ok(conditions) = conditions else {
        return refused(RowRefusal::Conditions {
            raw: row.conditions.clone(),
        });
    };
    let symbol = match alpaca_symbol(&row.ticker) {
        Ok(symbol) => symbol,
        Err(cause) => return refused(RowRefusal::Symbol(cause)),
    };
    let price = match Price::from_dollars(row.price) {
        Ok(price) => price,
        Err(cause) => return refused(RowRefusal::Price(cause)),
    };
    let size = match Shares::from_float(row.size) {
        Ok(size) => size,
        Err(cause) => return refused(RowRefusal::Shares(cause)),
    };
    let timestamp = DateTime::from_timestamp_nanos(row.sip_timestamp);
    match Trade::new(symbol, timestamp, price, size) {
        Ok(trade) => TradeRowOutcome::Trade {
            trade,
            conditions,
            corrected: row.correction.is_some_and(|correction| correction != 0),
        },
        Err(cause) => refused(RowRefusal::Trade(cause)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn session() -> SessionDate {
        SessionDate::from_date(NaiveDate::from_ymd_opt(2021, 8, 23).unwrap())
    }

    #[test]
    fn test_each_dataset_names_the_vendors_file_and_our_key() {
        let cases = [
            (
                FlatFileDataset::DailyBars,
                "us_stocks_sip/day_aggs_v1/2021/08/2021-08-23.csv.gz",
                "data/equity/stage=raw/bars/provider=massive/interval=one_day/year=2021/month=08/day=23/data.csv.gz",
                "data/raw/massive/equity/day_aggs/schema=v1/year=2021/month=08/day=23/data.csv.gz",
            ),
            (
                FlatFileDataset::MinuteBars,
                "us_stocks_sip/minute_aggs_v1/2021/08/2021-08-23.csv.gz",
                "data/equity/stage=raw/bars/provider=massive/interval=one_minute/year=2021/month=08/day=23/data.csv.gz",
                "data/raw/massive/equity/minute_aggs/schema=v1/year=2021/month=08/day=23/data.csv.gz",
            ),
            (
                FlatFileDataset::Quotes,
                "us_stocks_sip/quotes_v1/2021/08/2021-08-23.csv.gz",
                "data/equity/stage=raw/quotes/provider=massive/year=2021/month=08/day=23/data.csv.gz",
                "data/raw/massive/equity/quotes/schema=v1/year=2021/month=08/day=23/data.csv.gz",
            ),
            (
                FlatFileDataset::Trades,
                "us_stocks_sip/trades_v1/2021/08/2021-08-23.csv.gz",
                "data/equity/stage=raw/trades/provider=massive/year=2021/month=08/day=23/data.csv.gz",
                "data/raw/massive/equity/trades/schema=v1/year=2021/month=08/day=23/data.csv.gz",
            ),
        ];
        for (dataset, vendor, ours, legacy) in cases {
            assert_eq!(dataset.path(session()), vendor);
            assert_eq!(dataset.key(session()).path(), ours);
            assert_eq!(dataset.legacy_path(session()), legacy);
        }
    }

    fn gzipped(text: &str) -> Vec<u8> {
        use std::io::Write;
        let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
        encoder.write_all(text.as_bytes()).unwrap();
        encoder.finish().unwrap()
    }

    fn october_second() -> SessionDate {
        SessionDate::from_date(NaiveDate::from_ymd_opt(2026, 10, 2).unwrap())
    }

    /// Rows copied from Massive's `day_aggs_v1` file for 2026-10-02.
    const DAILY: &str = "ticker,volume,open,close,high,low,window_start,transactions
AAPL,33278552.153126,333.260000,333.690000,334.540000,330.610000,1790913600000000000,628334
BApA,1733619.463400,60.550000,60.040000,60.770000,59.550000,1790913600000000000,1344
BCATrw,89401.000000,0.005500,0.007500,0.007500,0.005000,1790913600000000000,101
BRK.B,4193461.418487,500.100000,502.650000,503.620000,499.010000,1790913600000000000,102058
DCOMp,10461.533600,16.780000,16.820000,16.870000,16.700100,1790913600000000000,79
NE.WS.A,364.000000,16.000000,15.580000,16.000000,15.580000,1790913600000000000,7
NMCOr,1223988.000000,0.022500,0.030000,0.034000,0.022500,1790913600000000000,2420
ZZZTA,6944.000000,4974.800000,5500.000000,5500.000000,4974.800000,1790913600000000000,942
";

    #[test]
    fn test_a_daily_file_maps_notation_and_accounts_for_every_row() {
        let parsed = FlatFileDataset::DailyBars
            .parse_bars(&gzipped(DAILY), october_second())
            .unwrap();
        let symbols: Vec<&str> = parsed
            .bars()
            .iter()
            .map(|bar| bar.symbol().as_str())
            .collect();
        assert_eq!(symbols, ["AAPL", "BA.PRA", "BRK.B", "NMCO.RT"]);
        assert_eq!(parsed.test_tickers(), ["ZZZTA"]);
        let refused: Vec<&str> = parsed.refused().iter().map(RefusedRow::ticker).collect();
        // The same three the grouped daily refuses: no rule maps them and `Symbol` takes one suffix.
        assert_eq!(refused, ["BCATrw", "DCOMp", "NE.WS.A"]);
        let apple = &parsed.bars()[0];
        assert_eq!(apple.timestamp().to_rfc3339(), "2026-10-02T20:00:00+00:00");
        assert_eq!(apple.prices().close().ticks(), 333_690_000);
        assert_eq!(apple.volume().units(), 33_278_552_153_126);
        assert_eq!(apple.trade_count().map(TradeCount::count), Some(628_334));
        assert_eq!(apple.dollar_volume(), None);
    }

    #[test]
    fn test_a_minute_file_keeps_each_minute_at_its_start() {
        let minute = "ticker,volume,open,close,high,low,window_start,transactions
AAPL,13809.484049,331.050000,331.170000,331.501900,330.880000,1790928000000000000,801
AAPL,7171.021026,331.330000,331.310000,331.590000,330.550000,1790928060000000000,452
BApA,250.116310,60.550000,60.550000,60.550000,60.550000,1790947800000000000,12
";
        let parsed = FlatFileDataset::MinuteBars
            .parse_bars(&gzipped(minute), october_second())
            .unwrap();
        let stamped: Vec<(String, String)> = parsed
            .bars()
            .iter()
            .map(|bar| {
                (
                    bar.symbol().as_str().to_string(),
                    bar.timestamp().to_rfc3339(),
                )
            })
            .collect();
        assert_eq!(
            stamped,
            [
                ("AAPL".to_string(), "2026-10-02T08:00:00+00:00".to_string()),
                ("AAPL".to_string(), "2026-10-02T08:01:00+00:00".to_string()),
                (
                    "BA.PRA".to_string(),
                    "2026-10-02T13:30:00+00:00".to_string()
                ),
            ]
        );
        assert!(parsed.refused().is_empty());
    }

    #[test]
    fn test_a_row_for_another_session_or_claimed_twice_is_refused() {
        let rows = "ticker,volume,open,close,high,low,window_start,transactions
AAPL,1.0,1.0,1.0,1.0,1.0,1790913600000000000,1
AAPL,2.0,2.0,2.0,2.0,2.0,1790913600000000000,1
MSFT,1.0,1.0,1.0,1.0,1.0,1790827200000000000,1
";
        let parsed = FlatFileDataset::DailyBars
            .parse_bars(&gzipped(rows), october_second())
            .unwrap();
        assert!(parsed.bars().is_empty());
        let causes: Vec<(&str, &'static str)> = parsed
            .refused()
            .iter()
            .map(|row| (row.ticker(), row.cause().into()))
            .collect();
        assert_eq!(
            causes,
            [
                ("AAPL", "duplicate"),
                ("AAPL", "duplicate"),
                ("MSFT", "session")
            ]
        );
    }

    #[test]
    fn test_only_bar_files_parse_and_a_broken_file_names_its_line() {
        assert_eq!(
            FlatFileDataset::Quotes.parse_bars(&gzipped(DAILY), october_second()),
            Err(ParseRefusal::NotBars {
                dataset: FlatFileDataset::Quotes
            })
        );
        let broken = format!("{DAILY}AAPL,not-a-number,1,1,1,1,1790913600000000000,1\n");
        assert!(matches!(
            FlatFileDataset::DailyBars.parse_bars(&gzipped(&broken), october_second()),
            Err(ParseRefusal::Malformed { line: Some(10), .. })
        ));
    }

    #[test]
    fn test_each_quote_row_is_a_quote_a_test_ticker_one_sided_or_refused() {
        // The header and first rows of Massive's quotes for 2021-08-23 and 2026-09-18, plus a crossed and a test row.
        let rows = "ticker,ask_exchange,ask_price,ask_size,bid_exchange,bid_price,bid_size,conditions,indicators,participant_timestamp,sequence_number,sip_timestamp,tape,trf_timestamp
A,8,180.0,100,11,164.28,100,\"1,81\",,1629716400001245000,79497,1629716400044243200,1,0
A,12,0.0,0,12,0.0,0,\"1,81\",,1789715092739044279,172,1789715092739508637,1,0
BApA,11,60.10,200,8,60.20,100,\"1,81\",,1629716446119912192,81265,1629716446119946496,1,0
ZTST,11,10.0,100,8,9.0,100,\"1,81\",,1629716446119912192,81265,1629716446119946496,1,0
";
        let mut outcomes = Vec::new();
        read_quotes(gzipped(rows).as_slice(), |outcome| outcomes.push(outcome)).unwrap();
        assert_eq!(outcomes.len(), 4);
        match &outcomes[0] {
            QuoteRowOutcome::Quote(quote) => {
                assert_eq!(quote.symbol().as_str(), "A");
                assert_eq!(quote.bid().ticks(), 164_280_000);
                assert_eq!(quote.ask_size().units(), 100_000_000);
                assert_eq!(
                    quote.timestamp().to_rfc3339(),
                    "2021-08-23T11:00:00.044243200+00:00"
                );
            }
            other @ (QuoteRowOutcome::TestTicker
            | QuoteRowOutcome::OneSided
            | QuoteRowOutcome::Refused(_)) => panic!("{other:?}"),
        }
        assert_eq!(outcomes[1], QuoteRowOutcome::OneSided);
        assert!(matches!(
            &outcomes[2],
            QuoteRowOutcome::Refused(row) if row.ticker() == "BApA"
        ));
        assert_eq!(outcomes[3], QuoteRowOutcome::TestTicker);
    }

    #[test]
    fn test_each_trade_row_carries_its_conditions_and_correction() {
        // The header and first rows of Massive's trades for 2026-09-18, a corrected copy, and two refusals.
        let rows = "ticker,conditions,correction,exchange,id,participant_timestamp,price,sequence_number,sip_timestamp,size,tape,trf_id,trf_timestamp
A,\"12,37\",0,4,71675222901845,1789718400814766587,157.350000,3372,1789718400831008119,10.000000,1,202,1789718400830651291
A,,1,4,71675225257543,1789706368198846000,156.340000,3610,1789718406372522227,0.001500,1,202,1789718406372164990
A,\"12,x\",0,4,71675225257544,1789706368198859000,156.340000,3611,1789718406372684563,0.711800,1,202,1789718406372327693
A,,,4,71675225257545,1789706368198859000,156.340000,3612,1789718406372684563,0,1,202,1789718406372327693
";
        let mut outcomes = Vec::new();
        read_trades(gzipped(rows).as_slice(), |outcome| outcomes.push(outcome)).unwrap();
        assert_eq!(outcomes.len(), 4);
        match &outcomes[0] {
            TradeRowOutcome::Trade {
                trade,
                conditions,
                corrected,
            } => {
                assert_eq!(trade.price().ticks(), 157_350_000);
                assert_eq!(trade.size().units(), 10_000_000);
                assert_eq!(conditions, &[12, 37]);
                assert!(!corrected);
            }
            other @ (TradeRowOutcome::TestTicker | TradeRowOutcome::Refused(_)) => {
                panic!("{other:?}")
            }
        }
        assert!(matches!(
            &outcomes[1],
            TradeRowOutcome::Trade { conditions, corrected: true, .. } if conditions.is_empty()
        ));
        let causes: Vec<&'static str> = outcomes[2..]
            .iter()
            .map(|outcome| match outcome {
                TradeRowOutcome::Refused(row) => row.cause().into(),
                TradeRowOutcome::Trade { .. } | TradeRowOutcome::TestTicker => "kept",
            })
            .collect();
        assert_eq!(causes, ["conditions", "trade"]);
    }
}
