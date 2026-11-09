//! Where a written object's rows came from and which run wrote them, carried in Parquet's key-value metadata and S3's
//! object metadata alike, so the record cannot drift from its rows.

use ::parquet::file::metadata::KeyValue;
use chrono::{DateTime, Utc};

use super::parquet;

use crate::common::journal::{Commit, RunId};
use crate::common::storage::Provider;

/// The names a file's provenance is written under, in Parquet's key-value metadata and S3's object metadata alike.
#[derive(Debug, Clone, Copy, PartialEq, Eq, strum::IntoStaticStr)]
pub(crate) enum MetadataEntry {
    #[strum(serialize = "fund.subscription")]
    Subscription,
    #[strum(serialize = "fund.fetched_at")]
    FetchedAt,
    #[strum(serialize = "fund.run_id")]
    RunId,
    #[strum(serialize = "fund.commit")]
    Commit,
}

/// The account a fetch was made under, which a provider's key alone does not say.
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
pub enum Subscription {
    AlgoTraderPlus,
    StocksAdvanced,
    StocksStarter,
    /// EDGAR's public filings, which need no account.
    EdgarPublic,
    /// Alpaca's trading API, which any brokerage account reads, such as its asset listing.
    BrokerageAccount,
}

impl Subscription {
    pub fn provider(self) -> Provider {
        match self {
            Self::AlgoTraderPlus | Self::BrokerageAccount => Provider::Alpaca,
            Self::StocksAdvanced | Self::StocksStarter => Provider::Massive,
            Self::EdgarPublic => Provider::Sec,
        }
    }
}

/// Where the rows came from and which run wrote them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Provenance {
    subscription: Subscription,
    fetched_at: DateTime<Utc>,
    run_id: RunId,
    commit: Option<Commit>,
}

impl Provenance {
    pub fn new(
        subscription: Subscription,
        fetched_at: DateTime<Utc>,
        run_id: RunId,
        commit: Option<Commit>,
    ) -> Self {
        Self {
            subscription,
            fetched_at,
            run_id,
            commit,
        }
    }

    pub fn subscription(&self) -> Subscription {
        self.subscription
    }

    /// Whether the subscription that fetched this belongs to `provider`.
    pub fn fetched_from(&self, provider: Provider) -> bool {
        self.subscription.provider() == provider
    }

    pub fn fetched_at(&self) -> DateTime<Utc> {
        self.fetched_at
    }

    pub fn run_id(&self) -> RunId {
        self.run_id
    }

    pub fn commit(&self) -> Option<&Commit> {
        self.commit.as_ref()
    }

    /// The named values a file's metadata carries, shared by Parquet's key-value metadata and S3's object metadata.
    pub(crate) fn entries(&self) -> Vec<(MetadataEntry, Option<String>)> {
        vec![
            (
                MetadataEntry::Subscription,
                Some(self.subscription.to_string()),
            ),
            (MetadataEntry::FetchedAt, Some(self.fetched_at.to_rfc3339())),
            (MetadataEntry::RunId, Some(self.run_id.to_string())),
            (
                MetadataEntry::Commit,
                self.commit.as_ref().map(Commit::to_string),
            ),
        ]
    }

    /// The entries as a Parquet file's key-value metadata.
    pub(crate) fn metadata(&self) -> Vec<KeyValue> {
        self.entries()
            .into_iter()
            .map(|(name, value)| KeyValue::new(<&str>::from(name).to_string(), value))
            .collect()
    }
}

/// The provenance `Provenance::entries` wrote, or the name of the first entry absent or unreadable.
pub(crate) fn provenance_from(entries: &[KeyValue]) -> Result<Provenance, &'static str> {
    let name = |entry: MetadataEntry| -> &'static str { entry.into() };
    let value = |entry: MetadataEntry| parquet::value(entries, name(entry));
    let required = |entry: MetadataEntry| value(entry).ok_or(name(entry));
    Ok(Provenance {
        subscription: required(MetadataEntry::Subscription)?
            .parse()
            .map_err(|_| name(MetadataEntry::Subscription))?,
        fetched_at: required(MetadataEntry::FetchedAt)?
            .parse()
            .map_err(|_| name(MetadataEntry::FetchedAt))?,
        run_id: RunId::new(
            required(MetadataEntry::RunId)?
                .parse()
                .map_err(|_| name(MetadataEntry::RunId))?,
        ),
        commit: value(MetadataEntry::Commit)
            .map(|raw| Commit::new(&raw))
            .transpose()
            .map_err(|_| name(MetadataEntry::Commit))?,
    })
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;
    use uuid::Uuid;

    use super::*;

    fn provenance(subscription: Subscription) -> Provenance {
        Provenance::new(
            subscription,
            "2026-09-26T07:00:00Z".parse().unwrap(),
            RunId::new(Uuid::from_u128(7)),
            Some(Commit::new("0123456789abcdef0123456789abcdef01234567").unwrap()),
        )
    }

    /// A provenance under `subscription` with any fetch time, run, and commit, clean, dirty or unknown.
    fn any_provenance(subscription: Subscription) -> impl Strategy<Value = Provenance> {
        (
            0_i64..4_000_000_000,
            0_u32..1_000_000_000,
            any::<u128>(),
            prop::option::of(("[0-9a-f]{40}", any::<bool>())),
        )
            .prop_map(move |(seconds, nanoseconds, run, commit)| {
                Provenance::new(
                    subscription,
                    DateTime::from_timestamp(seconds, nanoseconds).unwrap(),
                    RunId::new(Uuid::from_u128(run)),
                    commit.map(|(sha, dirty)| {
                        let suffix = if dirty { "-dirty" } else { "" };
                        Commit::new(&format!("{sha}{suffix}")).unwrap()
                    }),
                )
            })
    }

    #[test]
    fn test_provenance_is_written_under_its_stored_names() {
        let names: Vec<String> = provenance(Subscription::StocksStarter)
            .metadata()
            .into_iter()
            .map(|entry| entry.key)
            .collect();
        assert_eq!(
            names,
            [
                "fund.subscription",
                "fund.fetched_at",
                "fund.run_id",
                "fund.commit"
            ]
        );
    }

    proptest! {
        /// Every provenance reads back from the metadata it writes, whichever entries it leaves out.
        #[test]
        fn property_provenance_round_trips(
            provenance in prop::sample::select(<Subscription as strum::IntoEnumIterator>::iter().collect::<Vec<_>>())
                .prop_flat_map(any_provenance),
        ) {
            prop_assert_eq!(provenance_from(&provenance.metadata()), Ok(provenance));
        }
    }
}
