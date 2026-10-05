//! Massive's flat files: one gzipped CSV per dataset per session, served from Massive's own S3 endpoint under the
//! Stocks Advanced keys, which lapse on 2026-10-26.

use aws_sdk_s3::config::retry::RetryConfig;
use aws_sdk_s3::config::{
    Credentials, Region, RequestChecksumCalculation, ResponseChecksumValidation,
};
use bytes::Bytes;
use chrono::NaiveDate;

use super::{VariableRefusal, variable};
use crate::common::market::record::BarInterval;
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
}
