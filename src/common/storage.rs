//! Object keys for the archive and the records. Each key is a hive path whose partition values a reader surfaces as
//! columns, each parses back to the parts that built it, and each has exactly one host allowed to write it.

use chrono::{Datelike, NaiveDate};
use serde::{Deserialize, Serialize};

use crate::common::market::record::BarInterval;
use crate::common::time::SessionDate;

/// Everything this layout writes lives under these roots.
const DATA_ROOT: &str = "data/equity";
const RECORDS_ROOT: &str = "records";

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    strum::Display,
    strum::EnumString,
    strum::IntoStaticStr,
    strum::EnumIter,
)]
#[strum(serialize_all = "snake_case")]
pub enum Provider {
    Alpaca,
    Massive,
}

/// Whether a parsed series was aggregated by the vendor or built by us from finer data, which never share a partition series.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    strum::Display,
    strum::EnumString,
    strum::IntoStaticStr,
    strum::EnumIter,
)]
#[strum(serialize_all = "snake_case")]
pub enum Origin {
    Vendor,
    Derived,
}

/// Which reference table a snapshot holds.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    strum::Display,
    strum::EnumString,
    strum::IntoStaticStr,
    strum::EnumIter,
)]
#[strum(serialize_all = "snake_case")]
pub enum ReferenceTable {
    Conditions,
    SecurityDetails,
    Splits,
    SeriesBoundaries,
}

/// The S3 storage class an object is written in.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StorageClass {
    Standard,
    DeepArchive,
}

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    strum::Display,
    strum::EnumString,
    strum::IntoStaticStr,
    strum::EnumIter,
)]
#[strum(serialize_all = "snake_case")]
pub enum Host {
    Archiver,
    Trader,
    Researcher,
}

/// The binary a log came from: lowercase letters, digits, `-` and `_`, so it cannot break a path segment.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Service(String);

/// Why a service name was refused.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ServiceRefusal {
    Malformed { raw: String },
}

impl Service {
    pub fn new(raw: &str) -> Result<Self, ServiceRefusal> {
        let mut bytes = raw.bytes();
        let starts_with_letter = bytes.next().is_some_and(|byte| byte.is_ascii_lowercase());
        let rest_allowed = bytes.all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-' || byte == b'_'
        });
        if starts_with_letter && rest_allowed {
            Ok(Self(raw.to_string()))
        } else {
            Err(ServiceRefusal::Malformed {
                raw: raw.to_string(),
            })
        }
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Which parsed series a key belongs to, named in its path.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    strum::Display,
    strum::EnumString,
    strum::IntoStaticStr,
    strum::EnumIter,
)]
#[strum(serialize_all = "snake_case")]
pub enum ParsedKind {
    Bars,
    Quotes,
    Trades,
}

mod sealed {
    pub trait Sealed {}

    impl Sealed for super::Bars {}
    impl Sealed for super::Quotes {}
    impl Sealed for super::Trades {}
}

/// A parsed kind as a type, so a codec's key admits only the kind it reads; sealed to the three below.
pub trait Family: sealed::Sealed + Copy + Default {
    const KIND: ParsedKind;

    fn wrap(key: ParsedKey<Self>) -> Key;
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Bars;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Quotes;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Trades;

impl Family for Bars {
    const KIND: ParsedKind = ParsedKind::Bars;

    fn wrap(key: ParsedKey<Self>) -> Key {
        Key::Bars(key)
    }
}

impl Family for Quotes {
    const KIND: ParsedKind = ParsedKind::Quotes;

    fn wrap(key: ParsedKey<Self>) -> Key {
        Key::Quotes(key)
    }
}

impl Family for Trades {
    const KIND: ParsedKind = ParsedKind::Trades;

    fn wrap(key: ParsedKey<Self>) -> Key {
        Key::Trades(key)
    }
}

/// One session of a parsed series of family `F`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ParsedKey<F> {
    provider: Provider,
    origin: Origin,
    interval: BarInterval,
    session: SessionDate,
    family: F,
}

pub type BarsKey = ParsedKey<Bars>;
pub type QuotesKey = ParsedKey<Quotes>;
pub type TradesKey = ParsedKey<Trades>;

impl<F: Family> ParsedKey<F> {
    pub fn new(
        provider: Provider,
        origin: Origin,
        interval: BarInterval,
        session: SessionDate,
    ) -> Self {
        Self {
            provider,
            origin,
            interval,
            session,
            family: F::default(),
        }
    }

    pub fn provider(&self) -> Provider {
        self.provider
    }

    pub fn interval(&self) -> BarInterval {
        self.interval
    }

    pub fn session(&self) -> SessionDate {
        self.session
    }

    /// The same provider, origin, kind and session at another interval, which names another series.
    pub fn at_interval(self, interval: BarInterval) -> Self {
        Self { interval, ..self }
    }
}

impl<F: Family> From<ParsedKey<F>> for Key {
    fn from(key: ParsedKey<F>) -> Self {
        F::wrap(key)
    }
}

/// One snapshot of a reference table.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReferenceKey {
    provider: Provider,
    table: ReferenceTable,
    as_of: SessionDate,
}

impl ReferenceKey {
    pub fn new(provider: Provider, table: ReferenceTable, as_of: SessionDate) -> Self {
        Self {
            provider,
            table,
            as_of,
        }
    }

    pub fn provider(&self) -> Provider {
        self.provider
    }

    pub fn table(&self) -> ReferenceTable {
        self.table
    }

    pub fn as_of(&self) -> SessionDate {
        self.as_of
    }
}

impl From<ReferenceKey> for Key {
    fn from(key: ReferenceKey) -> Self {
        Self::Reference(key)
    }
}

impl TryFrom<Key> for ReferenceKey {
    type Error = Key;

    fn try_from(key: Key) -> Result<Self, Key> {
        match key {
            Key::Reference(key) => Ok(key),
            Key::Bars(..)
            | Key::Quotes(..)
            | Key::Trades(..)
            | Key::RawBars { .. }
            | Key::RawQuotes { .. }
            | Key::RawTrades { .. }
            | Key::Journal(..)
            | Key::Logs(..) => Err(key),
        }
    }
}

/// One session of a host's journal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct JournalKey {
    host: Host,
    session: SessionDate,
}

impl JournalKey {
    pub fn new(host: Host, session: SessionDate) -> Self {
        Self { host, session }
    }

    pub fn session(&self) -> SessionDate {
        self.session
    }
}

impl From<JournalKey> for Key {
    fn from(key: JournalKey) -> Self {
        Self::Journal(key)
    }
}

/// One session of one service's log on a host.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogsKey {
    host: Host,
    service: Service,
    session: SessionDate,
}

impl LogsKey {
    pub fn new(host: Host, service: Service, session: SessionDate) -> Self {
        Self {
            host,
            service,
            session,
        }
    }
}

impl From<LogsKey> for Key {
    fn from(key: LogsKey) -> Self {
        Self::Logs(key)
    }
}

/// One object's place in the bucket, serialized as its path.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub enum Key {
    Bars(BarsKey),
    Quotes(QuotesKey),
    Trades(TradesKey),
    Reference(ReferenceKey),
    /// A vendor's bar file exactly as served.
    RawBars {
        provider: Provider,
        interval: BarInterval,
        session: SessionDate,
    },
    /// A vendor's quote file exactly as served.
    RawQuotes {
        provider: Provider,
        session: SessionDate,
    },
    /// A vendor's trade file exactly as served.
    RawTrades {
        provider: Provider,
        session: SessionDate,
    },
    Journal(JournalKey),
    Logs(LogsKey),
}

/// The version of an object a read or listing saw, as the store's entity tag names it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct EntityTag(String);

impl EntityTag {
    pub fn new(raw: &str) -> Self {
        Self(raw.to_string())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

fn reference_series(provider: Provider, table: ReferenceTable) -> String {
    format!("{DATA_ROOT}/stage=parsed/reference/provider={provider}/table={table}/")
}

/// The prefix every session of one series shares, so listing it finds what is held.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SeriesPrefix(String);

impl SeriesPrefix {
    /// Every snapshot of `provider`'s `table`, whatever its date.
    pub fn reference(provider: Provider, table: ReferenceTable) -> Self {
        Self(reference_series(provider, table))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for SeriesPrefix {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

/// Why a path was not read as a key.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum KeyRefusal {
    #[error("`{path}` is not a key")]
    Unrecognized { path: String },
}

/// Accepts only the exact path `path()` writes, so no two paths name one key.
impl std::str::FromStr for Key {
    type Err = KeyRefusal;

    fn from_str(path: &str) -> Result<Self, Self::Err> {
        parse_segments(&path.split('/').collect::<Vec<_>>())
            .filter(|key| key.path() == path)
            .ok_or_else(|| KeyRefusal::Unrecognized {
                path: path.to_string(),
            })
    }
}

impl TryFrom<String> for Key {
    type Error = KeyRefusal;

    fn try_from(path: String) -> Result<Self, Self::Error> {
        path.parse()
    }
}

impl From<Key> for String {
    fn from(key: Key) -> Self {
        key.path()
    }
}

impl Key {
    pub fn path(&self) -> String {
        let series = self.series();
        match self {
            Self::Reference(key) => format!("{series}as_of={}/data.parquet", key.as_of),
            Self::Bars(..)
            | Self::Quotes(..)
            | Self::Trades(..)
            | Self::Journal(..)
            | Self::Logs(..) => {
                format!("{series}{}/data.parquet", date_partition(self.session()))
            }
            Self::RawBars { .. } | Self::RawQuotes { .. } | Self::RawTrades { .. } => {
                format!("{series}{}/data.csv.gz", date_partition(self.session()))
            }
        }
    }

    pub fn series(&self) -> SeriesPrefix {
        SeriesPrefix(match self {
            Self::Bars(key) => parsed_series(key),
            Self::Quotes(key) => parsed_series(key),
            Self::Trades(key) => parsed_series(key),
            Self::Reference(key) => reference_series(key.provider, key.table),
            Self::RawBars {
                provider, interval, ..
            } => format!("{DATA_ROOT}/stage=raw/bars/provider={provider}/interval={interval}/"),
            Self::RawQuotes { provider, .. } => {
                format!("{DATA_ROOT}/stage=raw/quotes/provider={provider}/")
            }
            Self::RawTrades { provider, .. } => {
                format!("{DATA_ROOT}/stage=raw/trades/provider={provider}/")
            }
            Self::Journal(key) => format!("{RECORDS_ROOT}/journal/producer={}/", key.host),
            Self::Logs(key) => format!(
                "{RECORDS_ROOT}/logs/producer={}/service={}/",
                key.host,
                key.service.as_str()
            ),
        })
    }

    /// The session a key is for.
    pub fn session(&self) -> SessionDate {
        match self {
            Self::Bars(key) => key.session,
            Self::Quotes(key) => key.session,
            Self::Trades(key) => key.session,
            Self::Reference(key) => key.as_of,
            Self::Journal(key) => key.session,
            Self::Logs(key) => key.session,
            Self::RawBars { session, .. }
            | Self::RawQuotes { session, .. }
            | Self::RawTrades { session, .. } => *session,
        }
    }

    /// The one host that may write this object: the archiver for data, the producer for a record.
    pub fn writer(&self) -> Host {
        match self {
            Self::Bars(..)
            | Self::Quotes(..)
            | Self::Trades(..)
            | Self::Reference(..)
            | Self::RawBars { .. }
            | Self::RawQuotes { .. }
            | Self::RawTrades { .. } => Host::Archiver,
            Self::Journal(key) => key.host,
            Self::Logs(key) => key.host,
        }
    }

    /// The class this key is written in: Deep Archive for raw quotes and trades, Standard for everything else.
    pub fn storage_class(&self) -> StorageClass {
        match self {
            Self::RawQuotes { .. } | Self::RawTrades { .. } => StorageClass::DeepArchive,
            Self::Bars(..)
            | Self::Quotes(..)
            | Self::Trades(..)
            | Self::Reference(..)
            | Self::RawBars { .. }
            | Self::Journal(..)
            | Self::Logs(..) => StorageClass::Standard,
        }
    }
}

impl Host {
    /// The prefixes this host may write, which its IAM grant is built from.
    pub fn writable_prefixes(self) -> Vec<String> {
        let records = ["journal", "logs"]
            .map(|kind| format!("{RECORDS_ROOT}/{kind}/producer={self}/"))
            .to_vec();
        match self {
            Self::Archiver => [vec![format!("{DATA_ROOT}/")], records].concat(),
            Self::Trader | Self::Researcher => records,
        }
    }
}

fn date_partition(session: SessionDate) -> String {
    let date = session.date();
    format!(
        "year={}/month={:02}/day={:02}",
        date.year(),
        date.month(),
        date.day()
    )
}

fn parse_segments(segments: &[&str]) -> Option<Key> {
    match segments {
        [
            "data",
            "equity",
            "stage=parsed",
            kind,
            provider,
            origin,
            interval,
            year,
            month,
            day,
            "data.parquet",
        ] => {
            let (provider, origin, interval, session) = (
                hive(provider, "provider")?,
                hive(origin, "origin")?,
                hive(interval, "interval")?,
                session(year, month, day)?,
            );
            Some(match kind.parse::<ParsedKind>().ok()? {
                ParsedKind::Bars => BarsKey::new(provider, origin, interval, session).into(),
                ParsedKind::Quotes => QuotesKey::new(provider, origin, interval, session).into(),
                ParsedKind::Trades => TradesKey::new(provider, origin, interval, session).into(),
            })
        }
        [
            "data",
            "equity",
            "stage=parsed",
            "reference",
            provider,
            table,
            as_of,
            "data.parquet",
        ] => Some(Key::Reference(ReferenceKey::new(
            hive(provider, "provider")?,
            hive(table, "table")?,
            hive::<NaiveDate>(as_of, "as_of").map(SessionDate::from_date)?,
        ))),
        [
            "data",
            "equity",
            "stage=raw",
            "bars",
            provider,
            interval,
            year,
            month,
            day,
            "data.csv.gz",
        ] => Some(Key::RawBars {
            provider: hive(provider, "provider")?,
            interval: hive(interval, "interval")?,
            session: session(year, month, day)?,
        }),
        [
            "data",
            "equity",
            "stage=raw",
            "quotes",
            provider,
            year,
            month,
            day,
            "data.csv.gz",
        ] => Some(Key::RawQuotes {
            provider: hive(provider, "provider")?,
            session: session(year, month, day)?,
        }),
        [
            "data",
            "equity",
            "stage=raw",
            "trades",
            provider,
            year,
            month,
            day,
            "data.csv.gz",
        ] => Some(Key::RawTrades {
            provider: hive(provider, "provider")?,
            session: session(year, month, day)?,
        }),
        ["records", "journal", host, year, month, day, "data.parquet"] => Some(Key::Journal(
            JournalKey::new(hive(host, "producer")?, session(year, month, day)?),
        )),
        [
            "records",
            "logs",
            host,
            service,
            year,
            month,
            day,
            "data.parquet",
        ] => Some(Key::Logs(LogsKey::new(
            hive(host, "producer")?,
            Service::new(service.strip_prefix("service=")?).ok()?,
            session(year, month, day)?,
        ))),
        _ => None,
    }
}

fn parsed_series<F: Family>(key: &ParsedKey<F>) -> String {
    format!(
        "{DATA_ROOT}/stage=parsed/{}/provider={}/origin={}/interval={}/",
        F::KIND,
        key.provider,
        key.origin,
        key.interval
    )
}

/// The value of a `name=value` segment.
fn hive<T: std::str::FromStr>(segment: &str, name: &str) -> Option<T> {
    segment.strip_prefix(name)?.strip_prefix('=')?.parse().ok()
}

fn session(year: &str, month: &str, day: &str) -> Option<SessionDate> {
    NaiveDate::from_ymd_opt(
        hive(year, "year")?,
        hive(month, "month")?,
        hive(day, "day")?,
    )
    .map(SessionDate::from_date)
}

#[cfg(test)]
pub(crate) mod tests {
    use proptest::prelude::*;
    use strum::IntoEnumIterator;

    use super::*;

    fn session() -> SessionDate {
        SessionDate::from_date(NaiveDate::from_ymd_opt(2026, 8, 3).unwrap())
    }

    #[test]
    fn test_each_key_has_its_path() {
        let cases = [
            (
                BarsKey::new(
                    Provider::Alpaca,
                    Origin::Vendor,
                    BarInterval::OneMinute,
                    session(),
                )
                .into(),
                "data/equity/stage=parsed/bars/provider=alpaca/origin=vendor/interval=one_minute/year=2026/month=08/day=03/data.parquet",
            ),
            (
                QuotesKey::new(
                    Provider::Alpaca,
                    Origin::Derived,
                    BarInterval::FiveMinute,
                    session(),
                )
                .into(),
                "data/equity/stage=parsed/quotes/provider=alpaca/origin=derived/interval=five_minute/year=2026/month=08/day=03/data.parquet",
            ),
            (
                TradesKey::new(
                    Provider::Massive,
                    Origin::Derived,
                    BarInterval::OneDay,
                    session(),
                )
                .into(),
                "data/equity/stage=parsed/trades/provider=massive/origin=derived/interval=one_day/year=2026/month=08/day=03/data.parquet",
            ),
            (
                ReferenceKey::new(
                    Provider::Massive,
                    ReferenceTable::SecurityDetails,
                    session(),
                )
                .into(),
                "data/equity/stage=parsed/reference/provider=massive/table=security_details/as_of=2026-08-03/data.parquet",
            ),
            (
                ReferenceKey::new(
                    Provider::Alpaca,
                    ReferenceTable::SeriesBoundaries,
                    session(),
                )
                .into(),
                "data/equity/stage=parsed/reference/provider=alpaca/table=series_boundaries/as_of=2026-08-03/data.parquet",
            ),
            (
                Key::RawBars {
                    provider: Provider::Massive,
                    interval: BarInterval::OneDay,
                    session: session(),
                },
                "data/equity/stage=raw/bars/provider=massive/interval=one_day/year=2026/month=08/day=03/data.csv.gz",
            ),
            (
                Key::RawQuotes {
                    provider: Provider::Massive,
                    session: session(),
                },
                "data/equity/stage=raw/quotes/provider=massive/year=2026/month=08/day=03/data.csv.gz",
            ),
            (
                Key::RawTrades {
                    provider: Provider::Massive,
                    session: session(),
                },
                "data/equity/stage=raw/trades/provider=massive/year=2026/month=08/day=03/data.csv.gz",
            ),
            (
                JournalKey::new(Host::Trader, session()).into(),
                "records/journal/producer=trader/year=2026/month=08/day=03/data.parquet",
            ),
            (
                LogsKey::new(Host::Archiver, Service::new("archiver").unwrap(), session()).into(),
                "records/logs/producer=archiver/service=archiver/year=2026/month=08/day=03/data.parquet",
            ),
        ];
        for (key, path) in cases {
            assert_eq!(key.path(), path);
            assert_eq!(path.parse::<Key>(), Ok(key));
        }
    }

    #[test]
    fn test_a_path_outside_the_layout_is_refused_with_itself() {
        for path in [
            "data/derived/equity/bars/interval=one_day/year=2026/month=08/day=03/data.parquet",
            "data/equity/bars/provider=massive/origin=fetched/interval=one_day/year=2026/month=08/day=03/data.parquet",
            "data/equity/stage=parsed/bars/provider=massive/origin=fetched/interval=one_day/year=2026/month=08/day=03/data.parquet",
            "data/equity/stage=parsed/bars/provider=databento/origin=vendor/interval=one_day/year=2026/month=08/day=03/data.parquet",
            "data/equity/stage=parsed/bars/provider=alpaca/origin=vendor/interval=one_day/year=2026/month=8/day=03/data.parquet",
            "data/equity/stage=parsed/bars/provider=alpaca/origin=vendor/interval=one_day/year=2026/month=02/day=30/data.parquet",
            "data/equity/stage=parsed/bars/origin=vendor/provider=alpaca/interval=one_day/year=2026/month=08/day=03/data.parquet",
            "data/equity/stage=parsed/bars/provider=alpaca/origin=vendor/interval=one_day/year=2026/month=08/day=03/data.csv.gz",
            "data/equity/stage=parsed/options/provider=alpaca/origin=vendor/interval=one_day/year=2026/month=08/day=03/data.parquet",
            "data/equity/stage=parsed/Bars/provider=alpaca/origin=vendor/interval=one_day/year=2026/month=08/day=03/data.parquet",
            "data/equity/stage=parsed/reference/provider=alpaca/origin=vendor/interval=one_day/year=2026/month=08/day=03/data.parquet",
            "data/equity/stage=raw/bars/provider=massive/interval=one_day/year=2026/month=08/day=03/data.parquet",
            "data/equity/stage=raw/quotes/provider=massive/interval=one_day/year=2026/month=08/day=03/data.csv.gz",
            "data/equity/stage=raw/reference/provider=massive/table=conditions/as_of=2026-08-03/data.parquet",
            "data/equity/stage=parsed/reference/provider=massive/as_of=2026-08-03/data.parquet",
            "data/equity/stage=parsed/reference/provider=massive/table=conditions/as_of=2026-8-3/data.parquet",
            "records/logs/producer=archiver/service=Archiver/year=2026/month=08/day=03/data.parquet",
            "records/journal/producer=archiver/year=2026/month=08/day=03/data.parquet.metadata",
        ] {
            assert_eq!(
                path.parse::<Key>(),
                Err(KeyRefusal::Unrecognized {
                    path: path.to_string()
                }),
                "{path}"
            );
            let refused = serde_json::from_str::<Key>(&format!("\"{path}\""));
            assert!(refused.is_err(), "{path}");
        }
    }

    #[test]
    fn test_each_host_writes_only_its_own_prefixes() {
        assert_eq!(
            Host::Archiver.writable_prefixes(),
            [
                "data/equity/",
                "records/journal/producer=archiver/",
                "records/logs/producer=archiver/"
            ]
        );
        assert_eq!(
            Host::Trader.writable_prefixes(),
            [
                "records/journal/producer=trader/",
                "records/logs/producer=trader/"
            ]
        );
    }

    #[test]
    fn test_a_service_is_one_safe_path_segment() {
        assert!(Service::new("archiver").is_ok());
        assert!(Service::new("laboratory_null-2").is_ok());
        for raw in ["", "Archiver", "2archiver", "a/b", "a=b", "a b"] {
            assert_eq!(
                Service::new(raw),
                Err(ServiceRefusal::Malformed {
                    raw: raw.to_string()
                }),
                "{raw}"
            );
        }
    }

    /// `key` moved to `to`'s session, within its own series.
    fn moved(key: &Key, to: &Key) -> Key {
        let session = to.session();
        match key.clone() {
            Key::Bars(key) => Key::Bars(ParsedKey { session, ..key }),
            Key::Quotes(key) => Key::Quotes(ParsedKey { session, ..key }),
            Key::Trades(key) => Key::Trades(ParsedKey { session, ..key }),
            Key::Reference(key) => Key::Reference(ReferenceKey {
                as_of: session,
                ..key
            }),
            Key::RawBars {
                provider, interval, ..
            } => Key::RawBars {
                provider,
                interval,
                session,
            },
            Key::RawQuotes { provider, .. } => Key::RawQuotes { provider, session },
            Key::RawTrades { provider, .. } => Key::RawTrades { provider, session },
            Key::Journal(key) => Key::Journal(JournalKey { session, ..key }),
            Key::Logs(key) => Key::Logs(LogsKey { session, ..key }),
        }
    }

    pub(crate) fn any_key() -> impl Strategy<Value = Key> {
        let provider = prop::sample::select(Provider::iter().collect::<Vec<_>>());
        let origin = prop::sample::select(Origin::iter().collect::<Vec<_>>());
        let interval = prop::sample::select(BarInterval::iter().collect::<Vec<_>>());
        let table = prop::sample::select(ReferenceTable::iter().collect::<Vec<_>>());
        let host = prop::sample::select(Host::iter().collect::<Vec<_>>());
        let session = (0_i64..47_000).prop_map(|days| {
            SessionDate::from_date(
                NaiveDate::from_ymd_opt(1970, 1, 1).unwrap() + chrono::TimeDelta::days(days),
            )
        });
        let service = "[a-z][a-z0-9_-]{0,15}".prop_map(|raw| Service::new(&raw).unwrap());
        let kind = prop::sample::select(ParsedKind::iter().collect::<Vec<_>>());
        prop_oneof![
            (
                kind,
                provider.clone(),
                origin,
                interval.clone(),
                session.clone()
            )
                .prop_map(|(kind, provider, origin, interval, session)| match kind {
                    ParsedKind::Bars => BarsKey::new(provider, origin, interval, session).into(),
                    ParsedKind::Quotes => {
                        QuotesKey::new(provider, origin, interval, session).into()
                    }
                    ParsedKind::Trades => {
                        TradesKey::new(provider, origin, interval, session).into()
                    }
                }),
            (provider.clone(), table, session.clone()).prop_map(|(provider, table, as_of)| {
                ReferenceKey::new(provider, table, as_of).into()
            }),
            (provider.clone(), interval, session.clone()).prop_map(
                |(provider, interval, session)| Key::RawBars {
                    provider,
                    interval,
                    session
                }
            ),
            (provider.clone(), session.clone())
                .prop_map(|(provider, session)| Key::RawQuotes { provider, session }),
            (provider, session.clone())
                .prop_map(|(provider, session)| Key::RawTrades { provider, session }),
            (host.clone(), session.clone())
                .prop_map(|(host, session)| JournalKey::new(host, session).into()),
            (host, service, session)
                .prop_map(|(host, service, session)| LogsKey::new(host, service, session).into()),
        ]
    }

    proptest! {
        /// A round trip makes `path` injective: two keys that shared a path would parse back to the same key.
        #[test]
        fn property_a_path_parses_back_to_its_key(key in any_key()) {
            prop_assert_eq!(key.path().parse::<Key>(), Ok(key));
        }

        /// A key serializes as its path string and deserializes back to itself.
        #[test]
        fn property_a_key_serializes_as_its_path(key in any_key()) {
            let json = serde_json::to_string(&key).unwrap();
            prop_assert_eq!(&json, &format!("\"{}\"", key.path()));
            prop_assert_eq!(serde_json::from_str::<Key>(&json).unwrap(), key);
        }

        #[test]
        fn property_only_raw_ticks_go_to_deep_archive(key in any_key()) {
            let path = key.path();
            let raw_tick = path.starts_with("data/equity/stage=raw/quotes/")
                || path.starts_with("data/equity/stage=raw/trades/");
            let expected = if raw_tick { StorageClass::DeepArchive } else { StorageClass::Standard };
            prop_assert_eq!(key.storage_class(), expected, "{}", path);
        }

        #[test]
        fn property_a_path_lies_under_its_series(key in any_key()) {
            let path = key.path();
            let rest = path.strip_prefix(key.series().as_str());
            prop_assert!(rest.is_some(), "{} outside {}", path, key.series());
            prop_assert!(!rest.unwrap().contains("provider="), "{}", path);
            prop_assert_eq!(path.parse::<Key>().map(|parsed| parsed.session()), Ok(key.session()));
        }

        /// A listing under one series finds exactly one key's series, and never a neighbour's.
        #[test]
        fn property_two_keys_share_a_series_when_only_their_position_differs(
            first in any_key(),
            second in any_key(),
        ) {
            prop_assert_eq!(first.series() == second.series(), moved(&first, &second) == second);
            prop_assert_eq!(moved(&first, &second).series(), first.series());
        }

        #[test]
        fn property_only_the_writer_may_write_a_key(key in any_key()) {
            let path = key.path();
            for host in Host::iter() {
                let allowed = host
                    .writable_prefixes()
                    .iter()
                    .any(|prefix| path.starts_with(prefix));
                prop_assert_eq!(allowed, host == key.writer(), "{} {}", host, path);
            }
        }
    }
}
