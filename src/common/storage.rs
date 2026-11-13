//! Object keys for the archive and the records. Each `Key` is a hive path whose partition values a reader surfaces as
//! columns, each parses back to the parts that built it, and each has exactly one producer allowed to write it; a
//! `ConfigurationKey` is a host's configuration, which an operator writes outside any session.

use chrono::{Datelike, NaiveDate};
use serde::{Deserialize, Serialize};

use crate::common::journal::RunId;
use crate::common::laboratory::experiment::SourceDigest;
use crate::common::market::record::BarInterval;
use crate::common::time::SessionDate;

/// Everything this layout writes lives under these roots.
const DATA_ROOT: &str = "data/equity";
const RECORDS_ROOT: &str = "records";
const CONFIGURATION_ROOT: &str = "configuration";

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize,
    strum::Display,
    strum::EnumString,
    strum::IntoStaticStr,
    strum::EnumIter,
)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum Provider {
    Alpaca,
    Massive,
    /// The SEC's EDGAR filings.
    Sec,
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
    /// Massive's grouped daily bars for a session, kept only as the list of symbols that traded, from the endpoint the
    /// tick legs take their symbols from; never read as prices.
    TradedSymbols,
    /// The industry code each annual report's header assigned its filer, for the filers of common stock or a depositary
    /// receipt Massive gives no code.
    FiledIndustryCodes,
    /// Whether each active asset is shortable and easy to borrow, as the broker's asset listing reported it after a
    /// session's close.
    BorrowFlags,
}

/// Where a reference table's `known_at` comes from: the earliest instant a decision may read one of its rows.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KnownAt {
    /// The snapshot's fetch, since the vendor stamps no publication time; earlier sessions may be described, never
    /// backtested.
    Fetched,
    /// The vendor's own publication stamp, held in each row's `column`.
    Published { column: &'static str },
}

impl ReferenceTable {
    /// Where this table's `known_at` comes from, which its view in `views.sql` must derive alike.
    pub fn known_at(self) -> KnownAt {
        match self {
            Self::Conditions
            | Self::SecurityDetails
            | Self::Splits
            | Self::SeriesBoundaries
            | Self::TradedSymbols
            | Self::BorrowFlags => KnownAt::Fetched,
            Self::FiledIndustryCodes => KnownAt::Published {
                column: "accepted_at",
            },
        }
    }
}

/// The S3 storage class an object is written in.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StorageClass {
    Standard,
    DeepArchive,
}

/// What a `Bucket` needs of any key: where the object lives and the class it is written in.
pub trait ObjectKey {
    fn path(&self) -> String;
    fn storage_class(&self) -> StorageClass;
}

/// A host's configuration in the profile bucket, written by an operator rather than a host.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConfigurationKey {
    /// The trader's playbook, as TOML.
    Playbook,
}

impl ObjectKey for ConfigurationKey {
    fn path(&self) -> String {
        match self {
            Self::Playbook => format!("{CONFIGURATION_ROOT}/playbook.toml"),
        }
    }

    fn storage_class(&self) -> StorageClass {
        match self {
            Self::Playbook => StorageClass::Standard,
        }
    }
}

/// A study's captured source, keyed by its digest so an unchanged rerun names the object already written.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceKey(SourceDigest);

impl SourceKey {
    pub fn new(digest: SourceDigest) -> Self {
        Self(digest)
    }
}

impl ObjectKey for SourceKey {
    fn path(&self) -> String {
        format!(
            "{RECORDS_ROOT}/sources/producer={}/sha256={}/source.json",
            Host::Researcher,
            self.0.as_str()
        )
    }

    fn storage_class(&self) -> StorageClass {
        StorageClass::Standard
    }
}

/// A file a study run wrote beside its journal, kept under the source it ran so the notes read with the code.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NoteKey {
    digest: SourceDigest,
    run: RunId,
    name: NoteName,
}

impl NoteKey {
    pub fn new(digest: SourceDigest, run: RunId, name: NoteName) -> Self {
        Self { digest, run, name }
    }
}

impl ObjectKey for NoteKey {
    fn path(&self) -> String {
        format!(
            "{RECORDS_ROOT}/sources/producer={}/sha256={}/notes/run={}/{}",
            Host::Researcher,
            self.digest.as_str(),
            self.run,
            self.name.as_str()
        )
    }

    fn storage_class(&self) -> StorageClass {
        StorageClass::Standard
    }
}

/// A note's file name: ASCII letters, digits, `.`, `_` and `-`, not starting with `.`, so it is one path segment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NoteName(String);

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum NoteNameRefusal {
    #[error("`{raw}` is not a note file name")]
    Malformed { raw: String },
}

impl NoteName {
    pub fn new(raw: &str) -> Result<Self, NoteNameRefusal> {
        let allowed = |character: char| {
            character.is_ascii_alphanumeric() || matches!(character, '.' | '_' | '-')
        };
        if !raw.is_empty() && !raw.starts_with('.') && raw.chars().all(allowed) {
            Ok(Self(raw.to_string()))
        } else {
            Err(NoteNameRefusal::Malformed {
                raw: raw.to_string(),
            })
        }
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl ObjectKey for Key {
    fn path(&self) -> String {
        Key::path(self)
    }

    fn storage_class(&self) -> StorageClass {
        Key::storage_class(self)
    }
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
    Quotes,
    Trades,
}

mod sealed {
    pub trait Sealed {}

    impl Sealed for super::Quotes {}
    impl Sealed for super::Trades {}
}

/// A parsed kind as a type, so a codec's key admits only the kind it reads; sealed to the two below.
pub trait Family: sealed::Sealed + Copy + Default {
    const KIND: ParsedKind;

    fn wrap(key: ParsedKey<Self>) -> Key;
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Quotes;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Trades;

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
    interval: BarInterval,
    session: SessionDate,
    family: F,
}

pub type QuotesKey = ParsedKey<Quotes>;
pub type TradesKey = ParsedKey<Trades>;

impl<F: Family> ParsedKey<F> {
    pub fn new(provider: Provider, interval: BarInterval, session: SessionDate) -> Self {
        Self {
            provider,
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

    /// The same provider, kind and session at another interval, which names another series.
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
            Key::Quotes(..)
            | Key::Trades(..)
            | Key::RawQuotes { .. }
            | Key::RawTrades { .. }
            | Key::RawReference(..)
            | Key::Journal(..)
            | Key::Logs(..) => Err(key),
        }
    }
}

/// Who writes a journal: one of the hosts, or an operator recording a change made by hand, which no host makes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Producer {
    Host(Host),
    Operator,
}

impl std::fmt::Display for Producer {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Host(host) => write!(formatter, "{host}"),
            Self::Operator => formatter.write_str("operator"),
        }
    }
}

impl std::str::FromStr for Producer {
    type Err = strum::ParseError;

    fn from_str(raw: &str) -> Result<Self, Self::Err> {
        match raw {
            "operator" => Ok(Self::Operator),
            _ => raw.parse().map(Self::Host),
        }
    }
}

/// One session of a producer's journal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct JournalKey {
    producer: Producer,
    session: SessionDate,
}

impl JournalKey {
    pub fn new(host: Host, session: SessionDate) -> Self {
        Self {
            producer: Producer::Host(host),
            session,
        }
    }

    /// The session's journal of changes an operator made by hand.
    pub fn operator(session: SessionDate) -> Self {
        Self {
            producer: Producer::Operator,
            session,
        }
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
    Quotes(QuotesKey),
    Trades(TradesKey),
    Reference(ReferenceKey),
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
    /// The vendor's responses fetched for a reference snapshot, one per line.
    RawReference(ReferenceKey),
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

fn reference_series(stage: &str, provider: Provider, table: ReferenceTable) -> String {
    format!("{DATA_ROOT}/stage={stage}/reference/provider={provider}/table={table}/")
}

/// The prefix every session of one series shares, so listing it finds what is held.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SeriesPrefix(String);

impl SeriesPrefix {
    /// Every series of market data, raw and parsed.
    pub fn market_data() -> Self {
        Self(format!("{DATA_ROOT}/"))
    }

    /// Every snapshot of `provider`'s `table`, whatever its date.
    pub fn reference(provider: Provider, table: ReferenceTable) -> Self {
        Self(reference_series("parsed", provider, table))
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
            Self::RawReference(key) => format!("{series}as_of={}/data.jsonl.gz", key.as_of),
            Self::Quotes(..) | Self::Trades(..) | Self::Journal(..) | Self::Logs(..) => {
                format!("{series}{}/data.parquet", date_partition(self.session()))
            }
            Self::RawQuotes { provider, .. } | Self::RawTrades { provider, .. } => format!(
                "{series}{}/{}",
                date_partition(self.session()),
                raw_ticks_file(*provider)
            ),
        }
    }

    pub fn series(&self) -> SeriesPrefix {
        SeriesPrefix(match self {
            Self::Quotes(key) => parsed_series(key),
            Self::Trades(key) => parsed_series(key),
            Self::Reference(key) => reference_series("parsed", key.provider, key.table),
            Self::RawReference(key) => reference_series("raw", key.provider, key.table),
            Self::RawQuotes { provider, .. } => {
                format!("{DATA_ROOT}/stage=raw/quotes/provider={provider}/")
            }
            Self::RawTrades { provider, .. } => {
                format!("{DATA_ROOT}/stage=raw/trades/provider={provider}/")
            }
            Self::Journal(key) => format!("{RECORDS_ROOT}/journal/producer={}/", key.producer),
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
            Self::Quotes(key) => key.session,
            Self::Trades(key) => key.session,
            Self::Reference(key) | Self::RawReference(key) => key.as_of,
            Self::Journal(key) => key.session,
            Self::Logs(key) => key.session,
            Self::RawQuotes { session, .. } | Self::RawTrades { session, .. } => *session,
        }
    }

    /// The class this key is written in: Deep Archive for raw quotes, trades and reference responses, Standard for
    /// everything else.
    pub fn storage_class(&self) -> StorageClass {
        match self {
            Self::RawQuotes { .. } | Self::RawTrades { .. } | Self::RawReference(..) => {
                StorageClass::DeepArchive
            }
            Self::Quotes(..)
            | Self::Trades(..)
            | Self::Reference(..)
            | Self::Journal(..)
            | Self::Logs(..) => StorageClass::Standard,
        }
    }
}

impl Host {
    /// The prefixes this host may write; `tests/test_host.rs` checks its IAM grant allows each.
    pub fn writable_prefixes(self) -> Vec<String> {
        let records = |kinds: &[&str]| {
            kinds
                .iter()
                .map(|kind| format!("{RECORDS_ROOT}/{kind}/producer={self}/"))
                .collect::<Vec<_>>()
        };
        match self {
            Self::Archiver => {
                [vec![format!("{DATA_ROOT}/")], records(&["journal", "logs"])].concat()
            }
            Self::Trader => records(&["journal", "logs"]),
            Self::Researcher => records(&["journal", "logs", "sources"]),
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

/// The file a vendor's tick session is stored as: Massive's flat file, or Alpaca's REST pages one per line.
fn raw_ticks_file(provider: Provider) -> &'static str {
    match provider {
        Provider::Massive => "data.csv.gz",
        Provider::Alpaca | Provider::Sec => "data.jsonl.gz",
    }
}

fn parse_segments(segments: &[&str]) -> Option<Key> {
    match segments {
        [
            "data",
            "equity",
            "stage=parsed",
            kind,
            provider,
            interval,
            year,
            month,
            day,
            "data.parquet",
        ] => {
            let (provider, interval, session) = (
                hive(provider, "provider")?,
                hive(interval, "interval")?,
                session(year, month, day)?,
            );
            Some(match kind.parse::<ParsedKind>().ok()? {
                ParsedKind::Quotes => QuotesKey::new(provider, interval, session).into(),
                ParsedKind::Trades => TradesKey::new(provider, interval, session).into(),
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
            "reference",
            provider,
            table,
            as_of,
            "data.jsonl.gz",
        ] => Some(Key::RawReference(ReferenceKey::new(
            hive(provider, "provider")?,
            hive(table, "table")?,
            hive::<NaiveDate>(as_of, "as_of").map(SessionDate::from_date)?,
        ))),
        [
            "data",
            "equity",
            "stage=raw",
            "quotes",
            provider,
            year,
            month,
            day,
            _file,
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
            _file,
        ] => Some(Key::RawTrades {
            provider: hive(provider, "provider")?,
            session: session(year, month, day)?,
        }),
        [
            "records",
            "journal",
            producer,
            year,
            month,
            day,
            "data.parquet",
        ] => Some(Key::Journal(JournalKey {
            producer: hive(producer, "producer")?,
            session: session(year, month, day)?,
        })),
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
        "{DATA_ROOT}/stage=parsed/{}/provider={}/interval={}/",
        F::KIND,
        key.provider,
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

    /// The one producer that may write `key`: the archiver for data, the producer for a record.
    fn writer(key: &Key) -> Producer {
        match key {
            Key::Quotes(..)
            | Key::Trades(..)
            | Key::Reference(..)
            | Key::RawQuotes { .. }
            | Key::RawTrades { .. }
            | Key::RawReference(..) => Producer::Host(Host::Archiver),
            Key::Journal(key) => key.producer,
            Key::Logs(key) => Producer::Host(key.host),
        }
    }

    #[test]
    fn test_a_provider_serializes_under_its_display_name() {
        let names: Vec<(String, String)> = Provider::iter()
            .map(|provider| {
                (
                    provider.to_string(),
                    serde_json::to_string(&provider).unwrap(),
                )
            })
            .collect();
        assert_eq!(
            names,
            [
                ("alpaca".to_string(), "\"alpaca\"".to_string()),
                ("massive".to_string(), "\"massive\"".to_string()),
                ("sec".to_string(), "\"sec\"".to_string()),
            ]
        );
        for provider in Provider::iter() {
            let json = serde_json::to_string(&provider).unwrap();
            assert_eq!(serde_json::from_str::<Provider>(&json).unwrap(), provider);
        }
    }

    fn session() -> SessionDate {
        SessionDate::from_date(NaiveDate::from_ymd_opt(2026, 8, 3).unwrap())
    }

    #[test]
    fn test_the_playbook_lives_beside_the_records() {
        assert_eq!(
            ObjectKey::path(&ConfigurationKey::Playbook),
            "configuration/playbook.toml"
        );
    }

    #[test]
    fn test_each_key_has_its_path() {
        let cases = [
            (
                QuotesKey::new(Provider::Alpaca, BarInterval::FiveMinute, session()).into(),
                "data/equity/stage=parsed/quotes/provider=alpaca/interval=five_minute/year=2026/month=08/day=03/data.parquet",
            ),
            (
                TradesKey::new(Provider::Massive, BarInterval::OneDay, session()).into(),
                "data/equity/stage=parsed/trades/provider=massive/interval=one_day/year=2026/month=08/day=03/data.parquet",
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
                ReferenceKey::new(Provider::Sec, ReferenceTable::FiledIndustryCodes, session())
                    .into(),
                "data/equity/stage=parsed/reference/provider=sec/table=filed_industry_codes/as_of=2026-08-03/data.parquet",
            ),
            (
                Key::RawReference(ReferenceKey::new(
                    Provider::Sec,
                    ReferenceTable::FiledIndustryCodes,
                    session(),
                )),
                "data/equity/stage=raw/reference/provider=sec/table=filed_industry_codes/as_of=2026-08-03/data.jsonl.gz",
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
                ReferenceKey::new(Provider::Alpaca, ReferenceTable::BorrowFlags, session()).into(),
                "data/equity/stage=parsed/reference/provider=alpaca/table=borrow_flags/as_of=2026-08-03/data.parquet",
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
                Key::RawQuotes {
                    provider: Provider::Alpaca,
                    session: session(),
                },
                "data/equity/stage=raw/quotes/provider=alpaca/year=2026/month=08/day=03/data.jsonl.gz",
            ),
            (
                Key::RawTrades {
                    provider: Provider::Alpaca,
                    session: session(),
                },
                "data/equity/stage=raw/trades/provider=alpaca/year=2026/month=08/day=03/data.jsonl.gz",
            ),
            (
                Key::RawReference(ReferenceKey::new(
                    Provider::Massive,
                    ReferenceTable::Splits,
                    session(),
                )),
                "data/equity/stage=raw/reference/provider=massive/table=splits/as_of=2026-08-03/data.jsonl.gz",
            ),
            (
                JournalKey::new(Host::Trader, session()).into(),
                "records/journal/producer=trader/year=2026/month=08/day=03/data.parquet",
            ),
            (
                JournalKey::operator(session()).into(),
                "records/journal/producer=operator/year=2026/month=08/day=03/data.parquet",
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
            "data/derived/equity/trades/interval=one_day/year=2026/month=08/day=03/data.parquet",
            "data/equity/trades/provider=massive/interval=one_day/year=2026/month=08/day=03/data.parquet",
            // The retired origin segment no longer names a key.
            "data/equity/stage=parsed/trades/provider=massive/origin=derived/interval=one_day/year=2026/month=08/day=03/data.parquet",
            "data/equity/stage=parsed/trades/provider=databento/interval=one_day/year=2026/month=08/day=03/data.parquet",
            "data/equity/stage=parsed/trades/provider=alpaca/interval=one_day/year=2026/month=8/day=03/data.parquet",
            "data/equity/stage=parsed/trades/provider=alpaca/interval=one_day/year=2026/month=02/day=30/data.parquet",
            "data/equity/stage=parsed/trades/interval=one_day/provider=alpaca/year=2026/month=08/day=03/data.parquet",
            "data/equity/stage=parsed/trades/provider=alpaca/interval=one_day/year=2026/month=08/day=03/data.csv.gz",
            "data/equity/stage=parsed/options/provider=alpaca/interval=one_day/year=2026/month=08/day=03/data.parquet",
            "data/equity/stage=parsed/Trades/provider=alpaca/interval=one_day/year=2026/month=08/day=03/data.parquet",
            "data/equity/stage=parsed/reference/provider=alpaca/interval=one_day/year=2026/month=08/day=03/data.parquet",
            // Vendor bars are retired, so their once-valid paths are no keys.
            "data/equity/stage=parsed/bars/provider=alpaca/interval=one_minute/year=2026/month=08/day=03/data.parquet",
            "data/equity/stage=raw/bars/provider=massive/interval=one_day/year=2026/month=08/day=03/data.csv.gz",
            "data/equity/stage=raw/bars/provider=massive/interval=one_day/year=2026/month=08/day=03/data.parquet",
            "data/equity/stage=raw/quotes/provider=massive/interval=one_day/year=2026/month=08/day=03/data.csv.gz",
            "data/equity/stage=raw/quotes/provider=alpaca/year=2026/month=08/day=03/data.csv.gz",
            "data/equity/stage=raw/trades/provider=massive/year=2026/month=08/day=03/data.jsonl.gz",
            "data/equity/stage=raw/reference/provider=massive/table=conditions/as_of=2026-08-03/data.parquet",
            "data/equity/stage=parsed/reference/provider=massive/table=conditions/as_of=2026-08-03/data.jsonl.gz",
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

    /// Every producer reads back from the name its partition is written under, and the operator's name is no host's.
    #[test]
    fn test_a_producer_round_trips_through_its_partition_name() {
        let producers: Vec<Producer> = Host::iter()
            .map(Producer::Host)
            .chain([Producer::Operator])
            .collect();
        let names: Vec<String> = producers.iter().map(Producer::to_string).collect();
        assert_eq!(names, ["archiver", "trader", "researcher", "operator"]);
        for (producer, name) in producers.iter().zip(&names) {
            assert_eq!(name.parse::<Producer>(), Ok(*producer));
        }
        assert!("operator".parse::<Host>().is_err());
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
        assert_eq!(
            Host::Researcher.writable_prefixes(),
            [
                "records/journal/producer=researcher/",
                "records/logs/producer=researcher/",
                "records/sources/producer=researcher/"
            ]
        );
    }

    #[test]
    fn test_a_note_is_keyed_by_its_run_under_its_source() {
        let key = NoteKey::new(
            SourceDigest::new(&"0a".repeat(32)).unwrap(),
            RunId::new(uuid::Uuid::from_u128(1)),
            NoteName::new("findings_v2.md").unwrap(),
        );
        assert_eq!(
            key.path(),
            "records/sources/producer=researcher/\
             sha256=0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a/\
             notes/run=00000000-0000-0000-0000-000000000001/findings_v2.md"
        );
        assert!(
            Host::Researcher
                .writable_prefixes()
                .iter()
                .any(|prefix| key.path().starts_with(prefix))
        );
        for raw in ["", ".hidden", "a/b", "..", "with space.md", "é.md"] {
            assert_eq!(
                NoteName::new(raw),
                Err(NoteNameRefusal::Malformed {
                    raw: raw.to_string()
                })
            );
        }
    }

    #[test]
    fn test_a_source_is_keyed_by_its_digest_under_the_researcher() {
        let key = SourceKey::new(SourceDigest::new(&"0a".repeat(32)).unwrap());
        assert_eq!(
            key.path(),
            "records/sources/producer=researcher/\
             sha256=0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a/source.json"
        );
        assert!(
            Host::Researcher
                .writable_prefixes()
                .iter()
                .any(|prefix| key.path().starts_with(prefix))
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
            Key::Quotes(key) => Key::Quotes(ParsedKey { session, ..key }),
            Key::Trades(key) => Key::Trades(ParsedKey { session, ..key }),
            Key::Reference(key) => Key::Reference(ReferenceKey {
                as_of: session,
                ..key
            }),
            Key::RawReference(key) => Key::RawReference(ReferenceKey {
                as_of: session,
                ..key
            }),
            Key::RawQuotes { provider, .. } => Key::RawQuotes { provider, session },
            Key::RawTrades { provider, .. } => Key::RawTrades { provider, session },
            Key::Journal(key) => Key::Journal(JournalKey { session, ..key }),
            Key::Logs(key) => Key::Logs(LogsKey { session, ..key }),
        }
    }

    pub(crate) fn any_key() -> impl Strategy<Value = Key> {
        let provider = prop::sample::select(Provider::iter().collect::<Vec<_>>());
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
            (kind, provider.clone(), interval.clone(), session.clone()).prop_map(
                |(kind, provider, interval, session)| match kind {
                    ParsedKind::Quotes => {
                        QuotesKey::new(provider, interval, session).into()
                    }
                    ParsedKind::Trades => {
                        TradesKey::new(provider, interval, session).into()
                    }
                }
            ),
            (provider.clone(), table.clone(), session.clone()).prop_map(
                |(provider, table, as_of)| ReferenceKey::new(provider, table, as_of).into()
            ),
            (provider.clone(), table, session.clone()).prop_map(|(provider, table, as_of)| {
                Key::RawReference(ReferenceKey::new(provider, table, as_of))
            }),
            (provider.clone(), session.clone())
                .prop_map(|(provider, session)| Key::RawQuotes { provider, session }),
            (provider, session.clone())
                .prop_map(|(provider, session)| Key::RawTrades { provider, session }),
            (host.clone(), session.clone())
                .prop_map(|(host, session)| JournalKey::new(host, session).into()),
            session
                .clone()
                .prop_map(|session| JournalKey::operator(session).into()),
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
        fn property_only_raw_ticks_and_responses_go_to_deep_archive(key in any_key()) {
            let path = key.path();
            let deep = path.starts_with("data/equity/stage=raw/quotes/")
                || path.starts_with("data/equity/stage=raw/trades/")
                || path.starts_with("data/equity/stage=raw/reference/");
            let expected = if deep { StorageClass::DeepArchive } else { StorageClass::Standard };
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
                prop_assert_eq!(allowed, Producer::Host(host) == writer(&key), "{} {}", host, path);
            }
        }
    }
}
