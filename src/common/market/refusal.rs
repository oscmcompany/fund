//! Why a vendor row did not become a record, and the kind of that cause, which the journal counts refusals by.

use super::corporate_actions::{ActionIdRefusal, SeriesBoundaryRefusal, SplitRatioRefusal};
use super::record::{BarPricesRefusal, BarRefusal, QuoteRefusal};
use super::security_details::{IndustryCodeRefusal, MarketIdentifierCodeRefusal};
use super::{DollarVolumeRefusal, DollarsRefusal, PriceRefusal, SharesRefusal, SymbolRefusal};

/// Why a vendor row did not become a record.
#[derive(Debug, Clone, PartialEq, strum::EnumDiscriminants)]
#[strum_discriminants(
    name(RowRefusalKind),
    vis(pub),
    derive(
        Hash,
        serde::Serialize,
        serde::Deserialize,
        strum::Display,
        strum::EnumString,
        strum::IntoStaticStr,
        strum::EnumIter
    ),
    serde(rename_all = "snake_case"),
    strum(serialize_all = "snake_case")
)]
pub enum RowRefusal {
    Symbol(SymbolRefusal),
    /// Stamped for a session other than the one requested.
    Session {
        timestamp: String,
    },
    Price(PriceRefusal),
    Prices(BarPricesRefusal),
    Shares(SharesRefusal),
    DollarVolume(DollarVolumeRefusal),
    Bar(BarRefusal),
    Quote(QuoteRefusal),
    /// A tape letter other than A, B or C.
    Tape {
        raw: String,
    },
    /// A condition field holding something other than comma-separated codes.
    Conditions {
        raw: String,
    },
    /// A correction code or label no rule reads, so whether the print stands is unknown.
    Correction {
        raw: String,
    },
    /// Answered for a symbol that was not asked for, as when a vendor normalizes a name into another security's.
    Unrequested,
    /// One of several rows claiming the same record; none is kept, since nothing says which is true.
    Duplicate,
    ActionId(ActionIdRefusal),
    SplitRatio(SplitRatioRefusal),
    Boundary(SeriesBoundaryRefusal),
    /// A corporate action with no date to place it on.
    Undated,
    /// A security type code no variant names.
    SecurityType {
        raw: String,
    },
    IndustryCode(IndustryCodeRefusal),
    Exchange(MarketIdentifierCodeRefusal),
    Dollars(DollarsRefusal),
    /// A Central Index Key that is not a number, or is zero, which the SEC never issues.
    CentralIndexKey {
        raw: String,
    },
}

impl RowRefusal {
    pub fn kind(&self) -> RowRefusalKind {
        self.into()
    }
}

/// Ordered by name, so a tally keyed by kind serializes its keys in the order the names sort.
impl Ord for RowRefusalKind {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        let name: &'static str = self.into();
        name.cmp(other.into())
    }
}

impl PartialOrd for RowRefusalKind {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

#[cfg(test)]
mod tests {
    use strum::IntoEnumIterator;

    use super::*;

    /// The journal keys refusals by these names, so serde and strum must spell each the same.
    #[test]
    fn test_each_kind_has_one_name() {
        let names: Vec<String> = RowRefusalKind::iter()
            .map(|kind| kind.to_string())
            .collect();
        assert_eq!(names.len(), 22);
        for kind in RowRefusalKind::iter() {
            let named = serde_json::to_string(&kind).unwrap();
            assert_eq!(named, format!("\"{kind}\""));
            assert_eq!(
                serde_json::from_str::<RowRefusalKind>(&named).unwrap(),
                kind
            );
            assert_eq!(kind.to_string().parse::<RowRefusalKind>().unwrap(), kind);
        }
        assert_eq!(&names[..4], ["symbol", "session", "price", "prices"]);
        assert_eq!(RowRefusalKind::DollarVolume.to_string(), "dollar_volume");
        assert_eq!(
            RowRefusalKind::CentralIndexKey.to_string(),
            "central_index_key"
        );
    }

    /// A journaled tally sorts its keys by name, as the string-keyed map it replaced did.
    #[test]
    fn test_kinds_sort_by_name() {
        let tally = crate::common::monoid::concatenate(
            [
                RowRefusalKind::Symbol,
                RowRefusalKind::Price,
                RowRefusalKind::Duplicate,
            ]
            .map(crate::common::monoid::Tally::of),
        );
        assert_eq!(
            serde_json::to_string(&tally).unwrap(),
            r#"{"duplicate":1,"price":1,"symbol":1}"#
        );
    }

    #[test]
    fn test_a_refusal_is_of_its_own_kind() {
        assert_eq!(RowRefusal::Duplicate.kind(), RowRefusalKind::Duplicate);
        assert_eq!(
            RowRefusal::Tape {
                raw: "D".to_string()
            }
            .kind(),
            RowRefusalKind::Tape
        );
    }
}
