//! Object keys for the archive and the records. Each key is a hive path whose partition values a reader surfaces as
//! columns, each parses back to the parts that built it, and each has exactly one host allowed to write it.

use chrono::{Datelike, NaiveDate};

use crate::common::market::record::BarInterval;
use crate::common::time::SessionDate;

/// Everything this layout writes lives under these roots; legacy's `data/derived/` and `exports/` are never among
/// them, so no glob reads the two together and no key is written by both.
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

/// One object's place in the bucket.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Key {
    Bars {
        provider: Provider,
        origin: Origin,
        interval: BarInterval,
        session: SessionDate,
    },
    Quotes {
        provider: Provider,
        origin: Origin,
        interval: BarInterval,
        session: SessionDate,
    },
    Trades {
        provider: Provider,
        origin: Origin,
        interval: BarInterval,
        session: SessionDate,
    },
    Reference {
        provider: Provider,
        table: ReferenceTable,
        as_of: SessionDate,
    },
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
    Journal {
        host: Host,
        session: SessionDate,
    },
    Logs {
        host: Host,
        service: Service,
        session: SessionDate,
    },
}

/// Who may write a key: the one host whose grant covers its prefix.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Writer {
    Host(Host),
}

/// Why a path was not read as a key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KeyRefusal {
    Unrecognized { path: String },
}

impl Key {
    pub fn path(&self) -> String {
        let series = self.series();
        match self {
            Self::Reference { as_of, .. } => format!("{series}as_of={as_of}/data.parquet"),
            Self::Bars { session, .. }
            | Self::Quotes { session, .. }
            | Self::Trades { session, .. }
            | Self::Journal { session, .. }
            | Self::Logs { session, .. } => {
                format!("{series}{}/data.parquet", date_partition(*session))
            }
            Self::RawBars { session, .. }
            | Self::RawQuotes { session, .. }
            | Self::RawTrades { session, .. } => {
                format!("{series}{}/data.csv.gz", date_partition(*session))
            }
        }
    }

    /// The prefix every session of this key's series shares, so listing it finds what is held.
    pub fn series(&self) -> String {
        match self {
            Self::Bars {
                provider,
                origin,
                interval,
                ..
            } => format!(
                "{DATA_ROOT}/stage=parsed/bars/provider={provider}/origin={origin}/interval={interval}/"
            ),
            Self::Quotes {
                provider,
                origin,
                interval,
                ..
            } => format!(
                "{DATA_ROOT}/stage=parsed/quotes/provider={provider}/origin={origin}/interval={interval}/"
            ),
            Self::Trades {
                provider,
                origin,
                interval,
                ..
            } => format!(
                "{DATA_ROOT}/stage=parsed/trades/provider={provider}/origin={origin}/interval={interval}/"
            ),
            Self::Reference {
                provider, table, ..
            } => format!("{DATA_ROOT}/stage=parsed/reference/provider={provider}/table={table}/"),
            Self::RawBars {
                provider, interval, ..
            } => format!("{DATA_ROOT}/stage=raw/bars/provider={provider}/interval={interval}/"),
            Self::RawQuotes { provider, .. } => {
                format!("{DATA_ROOT}/stage=raw/quotes/provider={provider}/")
            }
            Self::RawTrades { provider, .. } => {
                format!("{DATA_ROOT}/stage=raw/trades/provider={provider}/")
            }
            Self::Journal { host, .. } => format!("{RECORDS_ROOT}/journal/producer={host}/"),
            Self::Logs { host, service, .. } => format!(
                "{RECORDS_ROOT}/logs/producer={host}/service={}/",
                service.as_str()
            ),
        }
    }

    /// The session a key is for.
    pub fn session(&self) -> SessionDate {
        match self {
            Self::Reference { as_of, .. } => *as_of,
            Self::Bars { session, .. }
            | Self::Quotes { session, .. }
            | Self::Trades { session, .. }
            | Self::RawBars { session, .. }
            | Self::RawQuotes { session, .. }
            | Self::RawTrades { session, .. }
            | Self::Journal { session, .. }
            | Self::Logs { session, .. } => *session,
        }
    }

    /// The one writer of this object: the archiver for data, the producer for a record.
    pub fn writer(&self) -> Writer {
        match self {
            Self::Bars { .. }
            | Self::Quotes { .. }
            | Self::Trades { .. }
            | Self::Reference { .. }
            | Self::RawBars { .. }
            | Self::RawQuotes { .. }
            | Self::RawTrades { .. } => Writer::Host(Host::Archiver),
            Self::Journal { host, .. } | Self::Logs { host, .. } => Writer::Host(*host),
        }
    }

    /// The class this key is written in: Deep Archive for raw quotes and trades, Standard for everything else.
    pub fn storage_class(&self) -> StorageClass {
        match self {
            Self::RawQuotes { .. } | Self::RawTrades { .. } => StorageClass::DeepArchive,
            Self::Bars { .. }
            | Self::Quotes { .. }
            | Self::Trades { .. }
            | Self::Reference { .. }
            | Self::RawBars { .. }
            | Self::Journal { .. }
            | Self::Logs { .. } => StorageClass::Standard,
        }
    }

    /// Accepts only the exact path `path()` writes, so no two paths name one key.
    pub fn parse(path: &str) -> Result<Self, KeyRefusal> {
        parse_segments(&path.split('/').collect::<Vec<_>>())
            .filter(|key| key.path() == path)
            .ok_or_else(|| KeyRefusal::Unrecognized {
                path: path.to_string(),
            })
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

/// Crate-visible for the legacy reader; archive task A6 makes it private again when it deletes that reader.
pub(crate) fn date_partition(session: SessionDate) -> String {
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
            "bars",
            provider,
            origin,
            interval,
            year,
            month,
            day,
            "data.parquet",
        ] => Some(Key::Bars {
            provider: hive(provider, "provider")?,
            origin: hive(origin, "origin")?,
            interval: hive(interval, "interval")?,
            session: session(year, month, day)?,
        }),
        [
            "data",
            "equity",
            "stage=parsed",
            "quotes",
            provider,
            origin,
            interval,
            year,
            month,
            day,
            "data.parquet",
        ] => Some(Key::Quotes {
            provider: hive(provider, "provider")?,
            origin: hive(origin, "origin")?,
            interval: hive(interval, "interval")?,
            session: session(year, month, day)?,
        }),
        [
            "data",
            "equity",
            "stage=parsed",
            "trades",
            provider,
            origin,
            interval,
            year,
            month,
            day,
            "data.parquet",
        ] => Some(Key::Trades {
            provider: hive(provider, "provider")?,
            origin: hive(origin, "origin")?,
            interval: hive(interval, "interval")?,
            session: session(year, month, day)?,
        }),
        [
            "data",
            "equity",
            "stage=parsed",
            "reference",
            provider,
            table,
            as_of,
            "data.parquet",
        ] => Some(Key::Reference {
            provider: hive(provider, "provider")?,
            table: hive(table, "table")?,
            as_of: hive::<NaiveDate>(as_of, "as_of").map(SessionDate::from_date)?,
        }),
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
        ["records", "journal", host, year, month, day, "data.parquet"] => Some(Key::Journal {
            host: hive(host, "producer")?,
            session: session(year, month, day)?,
        }),
        [
            "records",
            "logs",
            host,
            service,
            year,
            month,
            day,
            "data.parquet",
        ] => Some(Key::Logs {
            host: hive(host, "producer")?,
            service: Service::new(service.strip_prefix("service=")?).ok()?,
            session: session(year, month, day)?,
        }),
        _ => None,
    }
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
mod tests {
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
                Key::Bars {
                    provider: Provider::Alpaca,
                    origin: Origin::Vendor,
                    interval: BarInterval::OneMinute,
                    session: session(),
                },
                "data/equity/stage=parsed/bars/provider=alpaca/origin=vendor/interval=one_minute/year=2026/month=08/day=03/data.parquet",
            ),
            (
                Key::Quotes {
                    provider: Provider::Alpaca,
                    origin: Origin::Derived,
                    interval: BarInterval::FiveMinute,
                    session: session(),
                },
                "data/equity/stage=parsed/quotes/provider=alpaca/origin=derived/interval=five_minute/year=2026/month=08/day=03/data.parquet",
            ),
            (
                Key::Trades {
                    provider: Provider::Massive,
                    origin: Origin::Derived,
                    interval: BarInterval::OneDay,
                    session: session(),
                },
                "data/equity/stage=parsed/trades/provider=massive/origin=derived/interval=one_day/year=2026/month=08/day=03/data.parquet",
            ),
            (
                Key::Reference {
                    provider: Provider::Massive,
                    table: ReferenceTable::SecurityDetails,
                    as_of: session(),
                },
                "data/equity/stage=parsed/reference/provider=massive/table=security_details/as_of=2026-08-03/data.parquet",
            ),
            (
                Key::Reference {
                    provider: Provider::Alpaca,
                    table: ReferenceTable::SeriesBoundaries,
                    as_of: session(),
                },
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
                Key::Journal {
                    host: Host::Trader,
                    session: session(),
                },
                "records/journal/producer=trader/year=2026/month=08/day=03/data.parquet",
            ),
            (
                Key::Logs {
                    host: Host::Archiver,
                    service: Service::new("archiver").unwrap(),
                    session: session(),
                },
                "records/logs/producer=archiver/service=archiver/year=2026/month=08/day=03/data.parquet",
            ),
        ];
        for (key, path) in cases {
            assert_eq!(key.path(), path);
            assert_eq!(Key::parse(path), Ok(key));
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
            "data/equity/stage=raw/bars/provider=massive/interval=one_day/year=2026/month=08/day=03/data.parquet",
            "data/equity/stage=raw/quotes/provider=massive/interval=one_day/year=2026/month=08/day=03/data.csv.gz",
            "data/equity/stage=raw/reference/provider=massive/table=conditions/as_of=2026-08-03/data.parquet",
            "data/equity/stage=parsed/reference/provider=massive/as_of=2026-08-03/data.parquet",
            "data/equity/stage=parsed/reference/provider=massive/table=conditions/as_of=2026-8-3/data.parquet",
            "records/logs/producer=archiver/service=Archiver/year=2026/month=08/day=03/data.parquet",
            "records/journal/producer=archiver/year=2026/month=08/day=03/data.parquet.metadata",
        ] {
            assert_eq!(
                Key::parse(path),
                Err(KeyRefusal::Unrecognized {
                    path: path.to_string()
                }),
                "{path}"
            );
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
            Key::Bars {
                provider,
                origin,
                interval,
                ..
            } => Key::Bars {
                provider,
                origin,
                interval,
                session,
            },
            Key::Quotes {
                provider,
                origin,
                interval,
                ..
            } => Key::Quotes {
                provider,
                origin,
                interval,
                session,
            },
            Key::Trades {
                provider,
                origin,
                interval,
                ..
            } => Key::Trades {
                provider,
                origin,
                interval,
                session,
            },
            Key::Reference {
                provider, table, ..
            } => Key::Reference {
                provider,
                table,
                as_of: session,
            },
            Key::RawBars {
                provider, interval, ..
            } => Key::RawBars {
                provider,
                interval,
                session,
            },
            Key::RawQuotes { provider, .. } => Key::RawQuotes { provider, session },
            Key::RawTrades { provider, .. } => Key::RawTrades { provider, session },
            Key::Journal { host, .. } => Key::Journal { host, session },
            Key::Logs { host, service, .. } => Key::Logs {
                host,
                service,
                session,
            },
        }
    }

    fn any_key() -> impl Strategy<Value = Key> {
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
        prop_oneof![
            (
                provider.clone(),
                origin.clone(),
                interval.clone(),
                session.clone()
            )
                .prop_map(|(provider, origin, interval, session)| Key::Bars {
                    provider,
                    origin,
                    interval,
                    session
                }),
            (
                provider.clone(),
                origin.clone(),
                interval.clone(),
                session.clone()
            )
                .prop_map(|(provider, origin, interval, session)| Key::Quotes {
                    provider,
                    origin,
                    interval,
                    session
                }),
            (provider.clone(), origin, interval.clone(), session.clone()).prop_map(
                |(provider, origin, interval, session)| Key::Trades {
                    provider,
                    origin,
                    interval,
                    session
                }
            ),
            (provider.clone(), table, session.clone()).prop_map(|(provider, table, as_of)| {
                Key::Reference {
                    provider,
                    table,
                    as_of,
                }
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
                .prop_map(|(host, session)| Key::Journal { host, session }),
            (host, service, session).prop_map(|(host, service, session)| Key::Logs {
                host,
                service,
                session
            }),
        ]
    }

    proptest! {
        /// A round trip makes `path` injective: two keys that shared a path would parse back to the same key.
        #[test]
        fn property_a_path_parses_back_to_its_key(key in any_key()) {
            prop_assert_eq!(Key::parse(&key.path()), Ok(key));
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
            let rest = path.strip_prefix(&key.series());
            prop_assert!(rest.is_some(), "{} outside {}", path, key.series());
            prop_assert!(!rest.unwrap().contains("provider="), "{}", path);
            prop_assert_eq!(Key::parse(&path).map(|parsed| parsed.session()), Ok(key.session()));
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
                prop_assert_eq!(allowed, Writer::Host(host) == key.writer(), "{} {}", host, path);
            }
        }
    }
}
