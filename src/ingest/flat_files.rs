//! Massive's flat files: one gzipped CSV per dataset per session, served from Massive's own S3 endpoint under the
//! Stocks Advanced keys, which lapse on 2026-10-26.

use std::num::NonZeroU64;

use aws_sdk_s3::config::retry::RetryConfig;
use aws_sdk_s3::config::{
    Credentials, Region, RequestChecksumCalculation, ResponseChecksumValidation,
};
use bytes::Bytes;
use chrono::{DateTime, NaiveDate, Utc};
use serde::Deserialize;

use super::massive::{alpaca_symbol, is_exchange_test_ticker};
use super::{Accepted, RefusedRow, RowRefusal, SessionBars, VariableRefusal, one_sided, variable};
use crate::common::market::record::{Bar, BarInterval, BarPrices, Quote};
use crate::common::market::trade_bars::{ConditionCode, Correction, Print};
use crate::common::market::{Price, Shares, Symbol, TradeCount};
use crate::common::storage::{EntityTag, Key, Provider};
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
}

/// One file Massive serves, with its length in bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Listed {
    session: SessionDate,
    length: u64,
    /// The version listed, which every ranged read must still match so one copy never mixes two versions.
    tag: EntityTag,
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
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum FlatFileError {
    #[error("listing {prefix} failed: {reason}")]
    List { prefix: String, reason: String },
    #[error("reading {path} failed: {reason}")]
    Get { path: String, reason: String },
    /// A range answered with a different number of bytes than asked for.
    #[error("{path} answered {received} bytes where {asked} were asked for")]
    ShortRange {
        path: String,
        asked: u64,
        received: u64,
    },
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
                let tag = EntityTag::new(
                    object
                        .e_tag()
                        .ok_or_else(|| failed(format!("{path} has no entity tag")))?,
                );
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
        length: NonZeroU64,
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
            .range(format!("bytes={start}-{}", start + length.get() - 1))
            .if_match(listed.tag.as_str())
            .send()
            .await
            .map_err(|error| failed(aws_sdk_s3::error::DisplayErrorContext(error).to_string()))?;
        let body = response
            .body
            .collect()
            .await
            .map_err(|error| failed(error.to_string()))?
            .into_bytes();
        if body.len() as u64 != length.get() {
            return Err(FlatFileError::ShortRange {
                path,
                asked: length.get(),
                received: body.len() as u64,
            });
        }
        Ok(body)
    }
}

/// Why a flat bar file was not read at all.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ParseRefusal {
    /// A gzip or CSV error, with the line it stopped on where the reader knows it.
    #[error("malformed at line {line:?}: {reason}")]
    Malformed { line: Option<u64>, reason: String },
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

/// A flat-file dataset that holds bars, spelled on a command line as its dataset is.
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
pub enum BarFile {
    #[strum(serialize = "daily_bars")]
    Daily,
    #[strum(serialize = "minute_bars")]
    Minute,
}

impl BarFile {
    pub fn dataset(self) -> FlatFileDataset {
        match self {
            Self::Daily => FlatFileDataset::DailyBars,
            Self::Minute => FlatFileDataset::MinuteBars,
        }
    }

    pub fn interval(self) -> BarInterval {
        match self {
            Self::Daily => BarInterval::OneDay,
            Self::Minute => BarInterval::OneMinute,
        }
    }

    /// Reads `gzipped`, this file for `session`, into bars in our notation; exchange test tickers are set aside and
    /// every other row that cannot become a bar is refused with its cause.
    pub fn parse_bars(
        self,
        gzipped: &[u8],
        session: SessionDate,
    ) -> Result<SessionBars, ParseRefusal> {
        let interval = self.interval();
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
        Ok(SessionBars::new(accepted, test_tickers))
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
    let prices = BarPrices::new(
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

/// Attempts at one streamed range, each a fresh request, before the stream fails.
const RANGE_ATTEMPTS: u32 = 5;

/// Bytes per ranged read when a file is streamed rather than copied.
const STREAM_CHUNK: u64 = 16 * 1024 * 1024;

/// A file's bytes in order as a blocking `Read`, with `ahead` ranged reads queued past the one being read and one
/// more waiting for room; read it from a blocking thread.
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
                let length = NonZeroU64::new(STREAM_CHUNK.min(listed.length - start))
                    .expect("a chunk starts before the file ends");
                let (flat_files, listed_chunk) = (flat_files.clone(), listed.clone());
                let fetch = tokio::spawn(async move {
                    // A body that breaks partway is not retried by the client, and losing it loses the whole file.
                    let mut attempt = 1;
                    loop {
                        match flat_files
                            .range(dataset, &listed_chunk, start, length)
                            .await
                        {
                            Err(error) if attempt < RANGE_ATTEMPTS => {
                                tracing::warn!(%error, attempt, start, "Retrying a streamed range");
                                tokio::time::sleep(std::time::Duration::from_secs(
                                    2_u64.pow(attempt),
                                ))
                                .await;
                                attempt += 1;
                            }
                            outcome => return outcome,
                        }
                    }
                });
                // A closed receiver means the reader stopped early, so nothing more is wanted.
                if sender.send(fetch).await.is_err() {
                    return;
                }
                start += length.get();
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
    if one_sided(row.bid_price, row.ask_price) {
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
    /// The SIP's correction indicator, zero or blank for a regular print.
    correction: Option<u32>,
    price: f64,
    size: f64,
    /// Nanoseconds since the epoch at which the SIP published the print.
    sip_timestamp: i64,
}

/// What one trade row became.
#[derive(Debug, Clone, PartialEq)]
pub enum TradeRowOutcome {
    Print {
        print: Print,
        conditions: Vec<ConditionCode>,
        correction: Correction,
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
    let conditions: Result<Vec<ConditionCode>, _> = row
        .conditions
        .split(',')
        .map(str::trim)
        .filter(|code| !code.is_empty())
        .map(|code| code.parse().map(ConditionCode::new))
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
    let correction = match correction(row.correction) {
        Ok(correction) => correction,
        Err(cause) => return refused(cause),
    };
    let print = Print::new(symbol, timestamp, price, size);
    TradeRowOutcome::Print {
        print,
        conditions,
        correction,
    }
}

/// Reads the SIP's correction indicator: 12 is the record that replaces a corrected print; 1 marks an original later
/// corrected, 8 one later canceled and 10 the cancel's own record; any other code is refused with itself.
fn correction(indicator: Option<u32>) -> Result<Correction, RowRefusal> {
    match indicator {
        None | Some(0 | 12) => Ok(Correction::Stands),
        Some(1 | 8 | 10) => Ok(Correction::Withdrawn),
        Some(code) => Err(RowRefusal::Correction {
            raw: code.to_string(),
        }),
    }
}

#[cfg(test)]
mod tests {
    use strum::IntoEnumIterator;

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
            ),
            (
                FlatFileDataset::MinuteBars,
                "us_stocks_sip/minute_aggs_v1/2021/08/2021-08-23.csv.gz",
                "data/equity/stage=raw/bars/provider=massive/interval=one_minute/year=2021/month=08/day=23/data.csv.gz",
            ),
            (
                FlatFileDataset::Quotes,
                "us_stocks_sip/quotes_v1/2021/08/2021-08-23.csv.gz",
                "data/equity/stage=raw/quotes/provider=massive/year=2021/month=08/day=23/data.csv.gz",
            ),
            (
                FlatFileDataset::Trades,
                "us_stocks_sip/trades_v1/2021/08/2021-08-23.csv.gz",
                "data/equity/stage=raw/trades/provider=massive/year=2021/month=08/day=23/data.csv.gz",
            ),
        ];
        for (dataset, vendor, ours) in cases {
            assert_eq!(dataset.path(session()), vendor);
            assert_eq!(dataset.key(session()).path(), ours);
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

    /// A `day_aggs_v1` file for 2026-10-02 with invented values in Massive's shape, one row per notation case.
    const DAILY: &str = "ticker,volume,open,close,high,low,window_start,transactions
ABC,33000000.125000,333.250000,333.750000,334.500000,330.500000,1790913600000000000,630000
ABC.B,4200000.250000,500.000000,502.500000,503.750000,499.000000,1790913600000000000,100000
ABFpA,1700000.500000,60.500000,60.000000,60.750000,59.500000,1790913600000000000,1300
ABHp,10500.250000,16.750000,16.800000,16.875000,16.700100,1790913600000000000,80
ABJ.WS.A,400.000000,16.000000,15.500000,16.000000,15.500000,1790913600000000000,7
ABLrw,89000.000000,0.005500,0.007500,0.007500,0.005000,1790913600000000000,100
ABMr,1200000.000000,0.022500,0.030000,0.034000,0.022500,1790913600000000000,2400
ZZZTA,7000.000000,5000.000000,5500.000000,5500.000000,5000.000000,1790913600000000000,900
";

    #[test]
    fn test_a_daily_file_maps_notation_and_accounts_for_every_row() {
        let parsed = BarFile::Daily
            .parse_bars(&gzipped(DAILY), october_second())
            .unwrap();
        let symbols: Vec<&str> = parsed
            .bars()
            .iter()
            .map(|bar| bar.symbol().as_str())
            .collect();
        assert_eq!(symbols, ["ABC", "ABC.B", "ABF.PRA", "ABM.RT"]);
        assert_eq!(parsed.test_tickers(), ["ZZZTA"]);
        let refused: Vec<&str> = parsed.refused().iter().map(RefusedRow::ticker).collect();
        // The same three the grouped daily refuses: no rule maps them and `Symbol` takes one suffix.
        assert_eq!(refused, ["ABHp", "ABJ.WS.A", "ABLrw"]);
        let abc = &parsed.bars()[0];
        assert_eq!(abc.timestamp().to_rfc3339(), "2026-10-02T20:00:00+00:00");
        assert_eq!(abc.prices().close().ticks(), 333_750_000);
        assert_eq!(abc.volume().units(), 33_000_000_125_000);
        assert_eq!(abc.trade_count().map(TradeCount::count), Some(630_000));
        assert_eq!(abc.dollar_volume(), None);
    }

    #[test]
    fn test_a_minute_file_keeps_each_minute_at_its_start() {
        let minute = "ticker,volume,open,close,high,low,window_start,transactions
ABC,13800.125000,331.000000,331.250000,331.500000,330.875000,1790928000000000000,800
ABC,7200.500000,331.250000,331.300000,331.600000,330.550000,1790928060000000000,450
ABFpA,250.125000,60.500000,60.500000,60.500000,60.500000,1790947800000000000,12
";
        let parsed = BarFile::Minute
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
                ("ABC".to_string(), "2026-10-02T08:00:00+00:00".to_string()),
                ("ABC".to_string(), "2026-10-02T08:01:00+00:00".to_string()),
                (
                    "ABF.PRA".to_string(),
                    "2026-10-02T13:30:00+00:00".to_string()
                ),
            ]
        );
        assert!(parsed.refused().is_empty());
    }

    #[test]
    fn test_a_row_for_another_session_or_claimed_twice_is_refused() {
        let rows = "ticker,volume,open,close,high,low,window_start,transactions
ABC,1.0,1.0,1.0,1.0,1.0,1790913600000000000,1
ABC,2.0,2.0,2.0,2.0,2.0,1790913600000000000,1
ABD,1.0,1.0,1.0,1.0,1.0,1790827200000000000,1
";
        let parsed = BarFile::Daily
            .parse_bars(&gzipped(rows), october_second())
            .unwrap();
        assert!(parsed.bars().is_empty());
        let causes: Vec<(&str, &'static str)> = parsed
            .refused()
            .iter()
            .map(|row| (row.ticker(), row.cause().kind().into()))
            .collect();
        assert_eq!(
            causes,
            [
                ("ABC", "duplicate"),
                ("ABC", "duplicate"),
                ("ABD", "session")
            ]
        );
    }

    /// A bar file is spelled on a command line as its dataset is, and reads back as itself.
    #[test]
    fn test_a_bar_file_is_spelled_as_its_dataset() {
        let spelled: Vec<(String, String, BarInterval)> = BarFile::iter()
            .map(|file| {
                (
                    file.to_string(),
                    file.dataset().to_string(),
                    file.interval(),
                )
            })
            .collect();
        assert_eq!(
            spelled,
            [
                (
                    "daily_bars".to_string(),
                    "daily_bars".to_string(),
                    BarInterval::OneDay
                ),
                (
                    "minute_bars".to_string(),
                    "minute_bars".to_string(),
                    BarInterval::OneMinute
                ),
            ]
        );
        for file in BarFile::iter() {
            assert_eq!(file.to_string().parse::<BarFile>(), Ok(file));
        }
        assert!("quotes".parse::<BarFile>().is_err());
    }

    #[test]
    fn test_a_broken_bar_file_names_its_line() {
        let broken = format!("{DAILY}ABC,not-a-number,1,1,1,1,1790913600000000000,1\n");
        assert!(matches!(
            BarFile::Daily.parse_bars(&gzipped(&broken), october_second()),
            Err(ParseRefusal::Malformed { line: Some(10), .. })
        ));
    }

    #[test]
    fn test_each_quote_row_is_a_quote_a_test_ticker_one_sided_or_refused() {
        // Massive's quote header with invented rows for 2021-08-23 and 2026-09-18: a quote, an empty book, a crossed, a
        // negative-bid and a test row.
        let rows = "ticker,ask_exchange,ask_price,ask_size,bid_exchange,bid_price,bid_size,conditions,indicators,participant_timestamp,sequence_number,sip_timestamp,tape,trf_timestamp
ABC,8,51.0,100,11,50.25,100,\"1,81\",,1629716400001234500,1001,1629716400012345600,1,0
ABC,12,0.0,0,12,0.0,0,\"1,81\",,1789715092000000123,2,1789715092000000456,1,0
ABFpA,11,60.00,200,8,60.25,100,\"1,81\",,1629716446000000256,2001,1629716446000000512,1,0
ABC,8,51.0,100,11,-1.0,100,\"1,81\",,1629716446000000256,2002,1629716446000000512,1,0
ZTST,11,10.0,100,8,9.0,100,\"1,81\",,1629716446000000256,2001,1629716446000000512,1,0
";
        let mut outcomes = Vec::new();
        read_quotes(gzipped(rows).as_slice(), |outcome| outcomes.push(outcome)).unwrap();
        assert_eq!(outcomes.len(), 5);
        match &outcomes[0] {
            QuoteRowOutcome::Quote(quote) => {
                assert_eq!(quote.symbol().as_str(), "ABC");
                assert_eq!(quote.bid().ticks(), 50_250_000);
                assert_eq!(quote.ask_size().units(), 100_000_000);
                assert_eq!(
                    quote.timestamp().to_rfc3339(),
                    "2021-08-23T11:00:00.012345600+00:00"
                );
            }
            other @ (QuoteRowOutcome::TestTicker
            | QuoteRowOutcome::OneSided
            | QuoteRowOutcome::Refused(_)) => panic!("{other:?}"),
        }
        assert_eq!(outcomes[1], QuoteRowOutcome::OneSided);
        assert!(matches!(
            &outcomes[2],
            QuoteRowOutcome::Refused(row) if row.ticker() == "ABFpA"
        ));
        // A negative bid is a bad price, not a missing side, as Alpaca's rows read it.
        assert!(matches!(
            &outcomes[3],
            QuoteRowOutcome::Refused(row)
                if row.ticker() == "ABC" && <&'static str>::from(row.cause().kind()) == "price"
        ));
        assert_eq!(outcomes[4], QuoteRowOutcome::TestTicker);
    }

    #[test]
    fn test_each_trade_row_carries_its_conditions_and_correction() {
        // Massive's trade header with invented rows for 2026-09-18: a print, a late corrected copy, a refusal and an unsized print.
        let rows = "ticker,conditions,correction,exchange,id,participant_timestamp,price,sequence_number,sip_timestamp,size,tape,trf_id,trf_timestamp
ABC,\"12,37\",0,4,70000000000001,1789718400000000123,50.250000,1001,1789718400000000456,10.000000,1,202,1789718400000000400
ABC,,1,4,70000000000002,1789706400000000000,50.000000,1002,1789718406000000123,0.002500,1,202,1789718406000000100
ABC,\"12,x\",0,4,70000000000003,1789706400000000500,50.000000,1003,1789718406000000789,0.500000,1,202,1789718406000000700
ABC,,,4,70000000000004,1789706400000000500,50.000000,1004,1789718406000000789,0,1,202,1789718406000000700
";
        let mut outcomes = Vec::new();
        read_trades(gzipped(rows).as_slice(), |outcome| outcomes.push(outcome)).unwrap();
        assert_eq!(outcomes.len(), 4);
        match &outcomes[0] {
            TradeRowOutcome::Print {
                print: Print::Trade(trade),
                conditions,
                correction,
            } => {
                assert_eq!(trade.price().ticks(), 50_250_000);
                assert_eq!(trade.size().units(), 10_000_000);
                assert_eq!(
                    conditions,
                    &[ConditionCode::new(12), ConditionCode::new(37)]
                );
                assert_eq!(correction, &Correction::Stands);
            }
            other @ (TradeRowOutcome::Print { .. }
            | TradeRowOutcome::TestTicker
            | TradeRowOutcome::Refused(_)) => panic!("{other:?}"),
        }
        assert!(matches!(
            &outcomes[1],
            TradeRowOutcome::Print { conditions, correction: Correction::Withdrawn, .. }
                if conditions.is_empty()
        ));
        assert!(matches!(
            &outcomes[2],
            TradeRowOutcome::Refused(row) if <&'static str>::from(row.cause().kind()) == "conditions"
        ));
        assert!(matches!(
            &outcomes[3],
            TradeRowOutcome::Print {
                print: Print::Unsized { .. },
                ..
            }
        ));
    }

    #[test]
    fn test_a_correction_record_stands_and_what_it_replaces_or_cancels_is_withdrawn() {
        // An invented 1,000,000-share print on 2025-11-04 and the later record that replaces it, in Massive's shape.
        let rows = "ticker,conditions,correction,exchange,id,participant_timestamp,price,sequence_number,sip_timestamp,size,tape,trf_id,trf_timestamp
ABC,\"53,32,35,41\",1,4,1,1762272000500000000,6.000000,1,1762272000500000123,1000000,3,202,1762272000500000000
ABC,\"53,35,41\",12,4,2,1762286400250000000,6.000000,2,1762286400250000456,1000000,3,202,1762286400250000000
";
        let mut outcomes = Vec::new();
        read_trades(gzipped(rows).as_slice(), |outcome| outcomes.push(outcome)).unwrap();
        let corrections: Vec<_> = outcomes
            .iter()
            .map(|outcome| match outcome {
                TradeRowOutcome::Print { correction, .. } => *correction,
                other @ (TradeRowOutcome::TestTicker | TradeRowOutcome::Refused(_)) => {
                    panic!("{other:?}")
                }
            })
            .collect();
        assert_eq!(corrections, [Correction::Withdrawn, Correction::Stands]);
        let read = [None, Some(0), Some(1), Some(8), Some(10), Some(12), Some(7)].map(correction);
        assert_eq!(
            read,
            [
                Ok(Correction::Stands),
                Ok(Correction::Stands),
                Ok(Correction::Withdrawn),
                Ok(Correction::Withdrawn),
                Ok(Correction::Withdrawn),
                Ok(Correction::Stands),
                Err(RowRefusal::Correction {
                    raw: "7".to_string()
                }),
            ]
        );
    }
}
