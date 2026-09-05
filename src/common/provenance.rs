//! Which vendor and which subscription answer for a dataset.
//!
//! Declared in the type system rather than in prose so a new dataset cannot be added without saying
//! where it comes from, and so "what stops working when a subscription lapses" is a query.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// The Alpaca subscription a dataset is served under.
///
/// One variant today, kept as an enum because the plan is a fact about the account rather than a
/// constant of the vendor.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AlpacaPlan {
    AlgoTraderPlus,
}

/// The Massive subscription a dataset is served under.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MassivePlan {
    StocksStarter,
    StocksAdvanced,
}

/// How a Massive dataset arrives: per-request, or as a whole-session file.
///
/// The distinction is not cosmetic — the flat files are the only affordable bulk route, and the
/// REST tail is what survives a downgrade.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MassiveTransport {
    Rest,
    FlatFile,
}

/// Which vendor answers for a dataset, under which subscription, and over which route.
///
/// Each provider carries its own plan, so a Massive subscription cannot be attached to Alpaca; and
/// only Massive carries a transport, because Alpaca publishes no bulk files.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "provider", rename_all = "snake_case")]
pub enum Provenance {
    Alpaca {
        subscription: AlpacaPlan,
    },
    Massive {
        subscription: MassivePlan,
        transport: MassiveTransport,
    },
}

impl Provenance {
    /// Alpaca over REST, which is every route Alpaca has.
    pub const fn alpaca(subscription: AlpacaPlan) -> Self {
        Provenance::Alpaca { subscription }
    }

    /// Massive, over the named route.
    pub const fn massive(subscription: MassivePlan, transport: MassiveTransport) -> Self {
        Provenance::Massive {
            subscription,
            transport,
        }
    }

    /// Whether this route stops answering when the Massive Advanced subscription lapses.
    ///
    /// The question the shutdown checklist asks, answered by the compiler rather than by memory.
    pub const fn requires_massive_advanced(self) -> bool {
        matches!(
            self,
            Provenance::Massive {
                subscription: MassivePlan::StocksAdvanced,
                ..
            }
        )
    }

    /// The vendor's name, for a log field or a sidecar a human reads.
    pub const fn provider_name(self) -> &'static str {
        match self {
            Provenance::Alpaca { .. } => "alpaca",
            Provenance::Massive { .. } => "massive",
        }
    }

    /// The subscription's name, flattened across providers for the same purpose.
    pub const fn subscription_name(self) -> &'static str {
        match self {
            Provenance::Alpaca {
                subscription: AlpacaPlan::AlgoTraderPlus,
            } => "algo_trader_plus",
            Provenance::Massive {
                subscription: MassivePlan::StocksStarter,
                ..
            } => "stocks_starter",
            Provenance::Massive {
                subscription: MassivePlan::StocksAdvanced,
                ..
            } => "stocks_advanced",
        }
    }
}

/// What a partition's sidecar records: every route that has contributed to it, and when.
///
/// **A set rather than a single route, because a partition is not sourced once.** The quote archive
/// is the worked example: the whole-market walk folded 1,254 sessions from Massive flat files, and
/// a repair pass then wrote 1,107 names into *every one of those same sessions* from Alpaca. A
/// single-valued record would name whichever route wrote last and silently disown the other.
///
/// `written_at` is deliberately *content* rather than metadata. A server-side copy rewrites an
/// object's `LastModified` — the 2026-09-03 prefix rewrite did exactly that to the whole archive —
/// so the only durable answer to "when was this session built" is one written inside the object.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartitionProvenance {
    pub dataset: String,
    pub session: String,
    pub routes: Vec<Provenance>,
    pub written_at: DateTime<Utc>,
}

impl PartitionProvenance {
    /// The record for one partition, stamped now, naming the single route that just wrote.
    pub fn new(dataset: &str, session: &str, provenance: Provenance) -> Self {
        PartitionProvenance {
            dataset: dataset.to_string(),
            session: session.to_string(),
            routes: vec![provenance],
            written_at: Utc::now(),
        }
    }

    /// Folds a further route into an existing record, keeping the set ordered and duplicate-free.
    ///
    /// Order is insertion order rather than sorted, so the first route to touch a partition stays
    /// first and a reader can tell the bulk source from the repair that followed it.
    pub fn contributed(mut self, provenance: Provenance) -> Self {
        if !self.routes.contains(&provenance) {
            self.routes.push(provenance);
        }
        self.written_at = Utc::now();
        self
    }

    /// Whether any route behind this partition stops answering when Advanced lapses.
    pub fn requires_massive_advanced(&self) -> bool {
        self.routes
            .iter()
            .any(|route| route.requires_massive_advanced())
    }

    /// The sidecar's key, beside the partition it describes.
    ///
    /// Takes the partition key rather than rebuilding one, so the two can never disagree about
    /// which session they belong to.
    pub fn sidecar_key(partition_key: &str) -> String {
        match partition_key.rsplit_once('/') {
            Some((directory, _)) => format!("{directory}/provenance.json"),
            None => "provenance.json".to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn advanced_is_the_only_route_that_lapses() {
        assert!(
            Provenance::massive(MassivePlan::StocksAdvanced, MassiveTransport::FlatFile)
                .requires_massive_advanced()
        );
        assert!(
            Provenance::massive(MassivePlan::StocksAdvanced, MassiveTransport::Rest)
                .requires_massive_advanced()
        );
        assert!(
            !Provenance::massive(MassivePlan::StocksStarter, MassiveTransport::FlatFile)
                .requires_massive_advanced()
        );
        assert!(!Provenance::alpaca(AlpacaPlan::AlgoTraderPlus).requires_massive_advanced());
    }

    #[test]
    fn names_are_pinned_to_literals() {
        let alpaca = Provenance::alpaca(AlpacaPlan::AlgoTraderPlus);
        assert_eq!(alpaca.provider_name(), "alpaca");
        assert_eq!(alpaca.subscription_name(), "algo_trader_plus");

        let starter = Provenance::massive(MassivePlan::StocksStarter, MassiveTransport::Rest);
        assert_eq!(starter.provider_name(), "massive");
        assert_eq!(starter.subscription_name(), "stocks_starter");

        let advanced = Provenance::massive(MassivePlan::StocksAdvanced, MassiveTransport::FlatFile);
        assert_eq!(advanced.subscription_name(), "stocks_advanced");
    }

    #[test]
    fn the_sidecar_sits_beside_its_partition() {
        assert_eq!(
            PartitionProvenance::sidecar_key(
                "data/derived/equity/trades/interval=one_day/year=2023/month=06/day=14/data.parquet"
            ),
            "data/derived/equity/trades/interval=one_day/year=2023/month=06/day=14/provenance.json"
        );
    }

    #[test]
    fn a_bare_key_still_yields_a_sidecar() {
        assert_eq!(
            PartitionProvenance::sidecar_key("data.parquet"),
            "provenance.json"
        );
    }

    #[test]
    fn the_serialized_shape_names_both_provider_and_subscription() {
        let record = PartitionProvenance {
            dataset: "equity_trades".to_string(),
            session: "2023-06-14".to_string(),
            routes: vec![Provenance::massive(
                MassivePlan::StocksAdvanced,
                MassiveTransport::FlatFile,
            )],
            written_at: "2026-09-04T00:36:48Z".parse().expect("a valid instant"),
        };

        let value: serde_json::Value =
            serde_json::to_value(&record).expect("the record serializes");
        assert_eq!(value["dataset"], "equity_trades");
        assert_eq!(value["session"], "2023-06-14");
        assert_eq!(value["routes"][0]["provider"], "massive");
        assert_eq!(value["routes"][0]["subscription"], "stocks_advanced");
        assert_eq!(value["routes"][0]["transport"], "flat_file");
        assert_eq!(value["written_at"], "2026-09-04T00:36:48Z");
    }

    #[test]
    fn an_alpaca_route_carries_no_transport() {
        let record = PartitionProvenance::new(
            "equity_quotes",
            "2026-08-25",
            Provenance::alpaca(AlpacaPlan::AlgoTraderPlus),
        );

        let value: serde_json::Value =
            serde_json::to_value(&record).expect("the record serializes");
        assert_eq!(value["routes"][0]["provider"], "alpaca");
        assert_eq!(value["routes"][0]["subscription"], "algo_trader_plus");
        assert!(value["routes"][0].get("transport").is_none());
    }

    /// The quote archive's actual shape: a whole-market flat-file walk, then an Alpaca repair that
    /// wrote 1,107 names into every one of the same sessions.
    #[test]
    fn a_repaired_partition_names_both_routes_that_built_it() {
        let bulk = Provenance::massive(MassivePlan::StocksAdvanced, MassiveTransport::FlatFile);
        let repair = Provenance::alpaca(AlpacaPlan::AlgoTraderPlus);

        let record = PartitionProvenance::new("equity_quotes", "2025-11-14", bulk)
            .contributed(repair)
            // The same route twice must not grow the set; a re-run is not a new source.
            .contributed(repair);

        assert_eq!(record.routes, vec![bulk, repair]);
        assert!(
            record.requires_massive_advanced(),
            "a partition the flat files built still depends on them, whatever wrote last"
        );
    }

    #[test]
    fn a_partition_alpaca_alone_built_survives_the_lapse() {
        let record = PartitionProvenance::new(
            "equity_quotes",
            "2026-08-25",
            Provenance::alpaca(AlpacaPlan::AlgoTraderPlus),
        );
        assert!(!record.requires_massive_advanced());
    }

    #[test]
    fn a_record_round_trips() {
        let record = PartitionProvenance::new(
            "equity_bars",
            "2026-08-28",
            Provenance::massive(MassivePlan::StocksStarter, MassiveTransport::Rest),
        );
        let encoded = serde_json::to_string(&record).expect("the record serializes");
        let decoded: PartitionProvenance =
            serde_json::from_str(&encoded).expect("the record deserializes");
        assert_eq!(record, decoded);
    }
}
