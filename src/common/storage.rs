//! Object keys for the archive and the exports. Each key is a hive path whose partition values a reader surfaces as
//! columns, each parses back to the parts that built it, and each has exactly one host allowed to write it.

use chrono::{Datelike, NaiveDate};

use crate::common::market::record::BarInterval;
use crate::common::time::SessionDate;

/// Everything this layout writes lives under these roots; the legacy archive's `data/derived/` is never among them,
/// so no glob reads the two together.
const DATA_ROOT: &str = "data/equity";
const EXPORT_ROOT: &str = "exports";

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

/// Whether bars came from the vendor or were built from finer bars, which never share a partition series.
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
    Fetched,
    Derived,
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
        interval: BarInterval,
        session: SessionDate,
    },
    Trades {
        provider: Provider,
        interval: BarInterval,
        session: SessionDate,
    },
    Reference {
        provider: Provider,
        as_of: SessionDate,
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

/// Why a path was not read as a key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KeyRefusal {
    Unrecognized { path: String },
}

impl Key {
    pub fn path(&self) -> String {
        match self {
            Self::Bars {
                provider,
                origin,
                interval,
                session,
            } => format!(
                "{DATA_ROOT}/bars/provider={provider}/origin={origin}/interval={interval}/{}/data.parquet",
                date_partition(*session)
            ),
            Self::Quotes {
                provider,
                interval,
                session,
            } => format!(
                "{DATA_ROOT}/quotes/provider={provider}/interval={interval}/{}/data.parquet",
                date_partition(*session)
            ),
            Self::Trades {
                provider,
                interval,
                session,
            } => format!(
                "{DATA_ROOT}/trades/provider={provider}/interval={interval}/{}/data.parquet",
                date_partition(*session)
            ),
            Self::Reference { provider, as_of } => {
                format!("{DATA_ROOT}/reference/provider={provider}/as_of={as_of}/data.parquet")
            }
            Self::Journal { host, session } => format!(
                "{EXPORT_ROOT}/journal/producer={host}/{}/data.parquet",
                date_partition(*session)
            ),
            Self::Logs {
                host,
                service,
                session,
            } => format!(
                "{EXPORT_ROOT}/logs/producer={host}/service={}/{}/data.parquet",
                service.as_str(),
                date_partition(*session)
            ),
        }
    }

    /// The one host allowed to write this object: the archiver for data, the producer for an export.
    pub fn writer(&self) -> Host {
        match self {
            Self::Bars { .. }
            | Self::Quotes { .. }
            | Self::Trades { .. }
            | Self::Reference { .. } => Host::Archiver,
            Self::Journal { host, .. } | Self::Logs { host, .. } => *host,
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
        let exports = ["journal", "logs"]
            .map(|kind| format!("{EXPORT_ROOT}/{kind}/producer={self}/"))
            .to_vec();
        match self {
            Self::Archiver => [vec![format!("{DATA_ROOT}/")], exports].concat(),
            Self::Trader | Self::Researcher => exports,
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
            "quotes",
            provider,
            interval,
            year,
            month,
            day,
            "data.parquet",
        ] => Some(Key::Quotes {
            provider: hive(provider, "provider")?,
            interval: hive(interval, "interval")?,
            session: session(year, month, day)?,
        }),
        [
            "data",
            "equity",
            "trades",
            provider,
            interval,
            year,
            month,
            day,
            "data.parquet",
        ] => Some(Key::Trades {
            provider: hive(provider, "provider")?,
            interval: hive(interval, "interval")?,
            session: session(year, month, day)?,
        }),
        [
            "data",
            "equity",
            "reference",
            provider,
            as_of,
            "data.parquet",
        ] => Some(Key::Reference {
            provider: hive(provider, "provider")?,
            as_of: hive::<NaiveDate>(as_of, "as_of").map(SessionDate::from_date)?,
        }),
        ["exports", "journal", host, year, month, day, "data.parquet"] => Some(Key::Journal {
            host: hive(host, "producer")?,
            session: session(year, month, day)?,
        }),
        [
            "exports",
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
                    origin: Origin::Fetched,
                    interval: BarInterval::OneMinute,
                    session: session(),
                },
                "data/equity/bars/provider=alpaca/origin=fetched/interval=one_minute/year=2026/month=08/day=03/data.parquet",
            ),
            (
                Key::Quotes {
                    provider: Provider::Alpaca,
                    interval: BarInterval::FiveMinute,
                    session: session(),
                },
                "data/equity/quotes/provider=alpaca/interval=five_minute/year=2026/month=08/day=03/data.parquet",
            ),
            (
                Key::Trades {
                    provider: Provider::Alpaca,
                    interval: BarInterval::OneDay,
                    session: session(),
                },
                "data/equity/trades/provider=alpaca/interval=one_day/year=2026/month=08/day=03/data.parquet",
            ),
            (
                Key::Reference {
                    provider: Provider::Massive,
                    as_of: session(),
                },
                "data/equity/reference/provider=massive/as_of=2026-08-03/data.parquet",
            ),
            (
                Key::Journal {
                    host: Host::Trader,
                    session: session(),
                },
                "exports/journal/producer=trader/year=2026/month=08/day=03/data.parquet",
            ),
            (
                Key::Logs {
                    host: Host::Archiver,
                    service: Service::new("archiver").unwrap(),
                    session: session(),
                },
                "exports/logs/producer=archiver/service=archiver/year=2026/month=08/day=03/data.parquet",
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
            "data/equity/bars/provider=databento/origin=fetched/interval=one_day/year=2026/month=08/day=03/data.parquet",
            "data/equity/bars/provider=alpaca/origin=fetched/interval=one_day/year=2026/month=8/day=03/data.parquet",
            "data/equity/bars/provider=alpaca/origin=fetched/interval=one_day/year=2026/month=02/day=30/data.parquet",
            "data/equity/bars/origin=fetched/provider=alpaca/interval=one_day/year=2026/month=08/day=03/data.parquet",
            "exports/logs/producer=archiver/service=Archiver/year=2026/month=08/day=03/data.parquet",
            "data/equity/reference/provider=massive/as_of=2026-8-3/data.parquet",
            "exports/journal/producer=archiver/year=2026/month=08/day=03/data.parquet.metadata",
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
                "exports/journal/producer=archiver/",
                "exports/logs/producer=archiver/"
            ]
        );
        assert_eq!(
            Host::Trader.writable_prefixes(),
            [
                "exports/journal/producer=trader/",
                "exports/logs/producer=trader/"
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

    fn any_key() -> impl Strategy<Value = Key> {
        let provider = prop::sample::select(Provider::iter().collect::<Vec<_>>());
        let origin = prop::sample::select(Origin::iter().collect::<Vec<_>>());
        let interval = prop::sample::select(BarInterval::iter().collect::<Vec<_>>());
        let host = prop::sample::select(Host::iter().collect::<Vec<_>>());
        let session = (0_i64..47_000).prop_map(|days| {
            SessionDate::from_date(
                NaiveDate::from_ymd_opt(1970, 1, 1).unwrap() + chrono::TimeDelta::days(days),
            )
        });
        let service = "[a-z][a-z0-9_-]{0,15}".prop_map(|raw| Service::new(&raw).unwrap());
        prop_oneof![
            (provider.clone(), origin, interval.clone(), session.clone()).prop_map(
                |(provider, origin, interval, session)| Key::Bars {
                    provider,
                    origin,
                    interval,
                    session
                }
            ),
            (provider.clone(), interval.clone(), session.clone()).prop_map(
                |(provider, interval, session)| Key::Quotes {
                    provider,
                    interval,
                    session
                }
            ),
            (provider.clone(), interval, session.clone()).prop_map(
                |(provider, interval, session)| Key::Trades {
                    provider,
                    interval,
                    session
                }
            ),
            (provider, session.clone())
                .prop_map(|(provider, as_of)| Key::Reference { provider, as_of }),
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
