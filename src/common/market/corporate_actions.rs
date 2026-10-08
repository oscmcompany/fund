//! The corporate actions a price series must answer to: splits, which rescale it, and the boundaries it may not be
//! read across, each as the vendor identified it so a later snapshot can replace or withdraw it.

use std::collections::BTreeMap;

use super::{SHARE_SCALE, Shares, SharesRefusal, Symbol};
use crate::common::time::SessionDate;

/// The vendor's identifier for one corporate action.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ActionId(String);

/// Why an action identifier was refused.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ActionIdRefusal {
    Empty,
}

impl ActionId {
    pub fn new(raw: &str) -> Result<Self, ActionIdRefusal> {
        match raw.is_empty() {
            true => Err(ActionIdRefusal::Empty),
            false => Ok(Self(raw.to_string())),
        }
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// How many shares a split turns into how many: `from` old shares become `to` new ones, so 4-for-1 is from 1 to 4.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SplitRatio {
    from: Shares,
    to: Shares,
}

/// Why a split ratio was refused.
#[derive(Debug, Clone, PartialEq)]
pub enum SplitRatioRefusal {
    Shares(SharesRefusal),
    /// A side of zero shares, which no split has.
    Zero,
}

impl SplitRatio {
    pub fn new(from: Shares, to: Shares) -> Result<Self, SplitRatioRefusal> {
        match from.is_zero() || to.is_zero() {
            true => Err(SplitRatioRefusal::Zero),
            false => Ok(Self { from, to }),
        }
    }

    /// The vendor's float sides, each rounded to the millionth of a share: a stock dividend's side such as
    /// 1.0392706872370265 is finer, and the rounding moves the ratio by under five parts in ten million.
    pub fn from_floats(from: f64, to: f64) -> Result<Self, SplitRatioRefusal> {
        let side = |shares: f64| {
            let scaled = (shares * SHARE_SCALE as f64).round();
            match (
                shares.is_finite(),
                shares >= 0.0 && scaled < u64::MAX as f64,
            ) {
                (false, _) => Err(SplitRatioRefusal::Shares(SharesRefusal::NotFinite {
                    shares,
                })),
                (true, false) => Err(SplitRatioRefusal::Shares(SharesRefusal::OutOfRange {
                    shares,
                })),
                (true, true) => Ok(Shares::from_units(scaled as u64)),
            }
        };
        Self::new(side(from)?, side(to)?)
    }

    pub fn from(self) -> Shares {
        self.from
    }

    pub fn to(self) -> Shares {
        self.to
    }
}

/// One split of one symbol.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Split {
    id: ActionId,
    symbol: Symbol,
    executed_on: SessionDate,
    ratio: SplitRatio,
}

impl Split {
    pub fn new(id: ActionId, symbol: Symbol, executed_on: SessionDate, ratio: SplitRatio) -> Self {
        Self {
            id,
            symbol,
            executed_on,
            ratio,
        }
    }

    pub fn id(&self) -> &ActionId {
        &self.id
    }

    pub fn symbol(&self) -> &Symbol {
        &self.symbol
    }

    pub fn executed_on(&self) -> SessionDate {
        self.executed_on
    }

    pub fn ratio(&self) -> SplitRatio {
        self.ratio
    }
}

/// What ends a symbol's series on a boundary's date, with the symbol the series continues under where there is one.
#[derive(Debug, Clone, PartialEq, Eq, strum::IntoStaticStr, strum::EnumDiscriminants)]
#[strum(serialize_all = "snake_case")]
#[strum_discriminants(
    name(BoundaryKind),
    derive(strum::Display, strum::EnumString, strum::EnumIter),
    strum(serialize_all = "snake_case")
)]
pub enum BoundaryChange {
    Renamed { to: Symbol },
    SpunOff { company: Symbol },
    RightsDistributed,
    UnitSeparated,
    Reorganized,
}

/// Why a boundary change was refused.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BoundaryChangeRefusal {
    /// A rename or spin-off that names no other symbol, or another kind that names one.
    Related {
        kind: BoundaryKind,
        related: Option<Symbol>,
    },
}

impl BoundaryChange {
    /// The change of `kind`, which carries `related` exactly when it is a rename or a spin-off.
    pub fn new(kind: BoundaryKind, related: Option<Symbol>) -> Result<Self, BoundaryChangeRefusal> {
        match (kind, related) {
            (BoundaryKind::Renamed, Some(to)) => Ok(Self::Renamed { to }),
            (BoundaryKind::SpunOff, Some(company)) => Ok(Self::SpunOff { company }),
            (BoundaryKind::RightsDistributed, None) => Ok(Self::RightsDistributed),
            (BoundaryKind::UnitSeparated, None) => Ok(Self::UnitSeparated),
            (BoundaryKind::Reorganized, None) => Ok(Self::Reorganized),
            (
                kind @ (BoundaryKind::Renamed
                | BoundaryKind::SpunOff
                | BoundaryKind::RightsDistributed
                | BoundaryKind::UnitSeparated
                | BoundaryKind::Reorganized),
                related,
            ) => Err(BoundaryChangeRefusal::Related { kind, related }),
        }
    }

    pub fn kind(&self) -> BoundaryKind {
        BoundaryKind::from(self)
    }

    /// The symbol a rename continues under or a spin-off created.
    pub fn related(&self) -> Option<&Symbol> {
        match self {
            Self::Renamed { to } => Some(to),
            Self::SpunOff { company } => Some(company),
            Self::RightsDistributed | Self::UnitSeparated | Self::Reorganized => None,
        }
    }
}

/// A date a symbol's series may not be read across.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SeriesBoundary {
    id: ActionId,
    symbol: Symbol,
    on: SessionDate,
    /// When the vendor processed the action, which is what a refresh window is matched against.
    processed_on: SessionDate,
    change: BoundaryChange,
}

/// Why a series boundary was refused.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SeriesBoundaryRefusal {
    /// A rename onto the symbol it already had, which ends nothing.
    RenamedToItself { symbol: Symbol },
}

impl SeriesBoundary {
    pub fn new(
        id: ActionId,
        symbol: Symbol,
        on: SessionDate,
        processed_on: SessionDate,
        change: BoundaryChange,
    ) -> Result<Self, SeriesBoundaryRefusal> {
        match &change {
            BoundaryChange::Renamed { to } if *to == symbol => {
                Err(SeriesBoundaryRefusal::RenamedToItself { symbol })
            }
            BoundaryChange::Renamed { .. }
            | BoundaryChange::SpunOff { .. }
            | BoundaryChange::RightsDistributed
            | BoundaryChange::UnitSeparated
            | BoundaryChange::Reorganized => Ok(Self {
                id,
                symbol,
                on,
                processed_on,
                change,
            }),
        }
    }

    pub fn id(&self) -> &ActionId {
        &self.id
    }

    pub fn symbol(&self) -> &Symbol {
        &self.symbol
    }

    pub fn on(&self) -> SessionDate {
        self.on
    }

    pub fn processed_on(&self) -> SessionDate {
        self.processed_on
    }

    pub fn change(&self) -> &BoundaryChange {
        &self.change
    }
}

/// The boundaries after a refresh of `[first, last]` by processing date: inside the window the fetch is authoritative,
/// so a withdrawn action goes, and outside it the earlier snapshot stands; an action held twice keeps the fetched one.
pub fn refresh_boundaries(
    previous: &[SeriesBoundary],
    fetched: &[SeriesBoundary],
    (first, last): (SessionDate, SessionDate),
) -> Vec<SeriesBoundary> {
    let outside = previous
        .iter()
        .filter(|boundary| !(first..=last).contains(&boundary.processed_on));
    let mut by_id: BTreeMap<ActionId, SeriesBoundary> = BTreeMap::new();
    for boundary in outside.chain(fetched) {
        by_id.insert(boundary.id.clone(), boundary.clone());
    }
    by_id.into_values().collect()
}

#[cfg(test)]
mod tests {
    use chrono::NaiveDate;
    use proptest::prelude::*;
    use strum::IntoEnumIterator;

    use super::*;

    fn day(day_of_year: u32) -> SessionDate {
        SessionDate::from_date(NaiveDate::from_yo_opt(2026, day_of_year).unwrap())
    }

    fn symbol(raw: &str) -> Symbol {
        Symbol::new(raw).unwrap()
    }

    #[test]
    fn test_a_split_ratio_holds_fractional_sides_and_refuses_an_empty_one() {
        let ratio = SplitRatio::from_floats(10.0, 11.0).unwrap();
        assert_eq!(
            (ratio.from().units(), ratio.to().units()),
            (10_000_000, 11_000_000)
        );
        // CFRLF's stock dividend on 2026-09-09, as Massive published it.
        let stock_dividend = SplitRatio::from_floats(1.0, 1.039_270_687_237_026_5).unwrap();
        assert_eq!(stock_dividend.to().units(), 1_039_271);
        assert_eq!(
            SplitRatio::from_floats(0.0, 1.0),
            Err(SplitRatioRefusal::Zero)
        );
        assert!(matches!(
            SplitRatio::from_floats(-1.0, 1.0),
            Err(SplitRatioRefusal::Shares(_))
        ));
    }

    #[test]
    fn test_only_a_rename_or_spin_off_names_a_related_symbol() {
        let names: Vec<String> = BoundaryKind::iter().map(|kind| kind.to_string()).collect();
        assert_eq!(
            names,
            [
                "renamed",
                "spun_off",
                "rights_distributed",
                "unit_separated",
                "reorganized"
            ]
        );
        for kind in BoundaryKind::iter() {
            assert_eq!(kind.to_string().parse::<BoundaryKind>(), Ok(kind));
        }
        let renamed = BoundaryChange::new(BoundaryKind::Renamed, Some(symbol("ETHZ"))).unwrap();
        assert_eq!(renamed.kind(), BoundaryKind::Renamed);
        assert_eq!(renamed.related(), Some(&symbol("ETHZ")));
        assert!(BoundaryChange::new(BoundaryKind::SpunOff, None).is_err());
        assert!(BoundaryChange::new(BoundaryKind::Reorganized, Some(symbol("A"))).is_err());
        assert_eq!(
            SeriesBoundary::new(
                ActionId::new("x").unwrap(),
                symbol("ATNF"),
                day(230),
                day(230),
                BoundaryChange::Renamed { to: symbol("ATNF") },
            ),
            Err(SeriesBoundaryRefusal::RenamedToItself {
                symbol: symbol("ATNF")
            })
        );
        assert_eq!(ActionId::new(""), Err(ActionIdRefusal::Empty));
    }

    fn boundary(id: &str, processed: u32, kind: usize) -> SeriesBoundary {
        let change = match kind % 3 {
            0 => BoundaryChange::Reorganized,
            1 => BoundaryChange::UnitSeparated,
            _ => BoundaryChange::Renamed { to: symbol("NEW") },
        };
        SeriesBoundary::new(
            ActionId::new(id).unwrap(),
            symbol("OLD"),
            day(processed),
            day(processed),
            change,
        )
        .unwrap()
    }

    #[test]
    fn test_a_refresh_replaces_its_window_and_keeps_what_lies_outside() {
        let previous = [
            boundary("kept", 10, 0),
            boundary("withdrawn", 100, 0),
            boundary("revised", 110, 0),
        ];
        let fetched = [boundary("revised", 110, 1), boundary("new", 120, 2)];
        let refreshed = refresh_boundaries(&previous, &fetched, (day(90), day(150)));
        let ids: Vec<&str> = refreshed
            .iter()
            .map(|boundary| boundary.id().as_str())
            .collect();
        assert_eq!(ids, ["kept", "new", "revised"]);
        assert_eq!(refreshed[2].change(), &BoundaryChange::UnitSeparated);
    }

    fn boundaries() -> impl Strategy<Value = Vec<SeriesBoundary>> {
        prop::collection::vec((0_u8..12, 1_u32..300, 0_usize..3), 0..20).prop_map(|rows| {
            rows.into_iter()
                .map(|(id, processed, kind)| boundary(&id.to_string(), processed, kind))
                .collect()
        })
    }

    proptest! {
        #[test]
        fn test_a_refresh_is_idempotent_and_starts_from_what_it_fetched(
            previous in boundaries(),
            fetched in boundaries(),
            first in 1_u32..300,
            length in 0_u32..120,
        ) {
            let window = (day(first), day((first + length).min(365)));
            let once = refresh_boundaries(&previous, &fetched, window);
            prop_assert_eq!(refresh_boundaries(&once, &fetched, window), once.clone());
            let fresh = refresh_boundaries(&[], &fetched, window);
            let mut fetched_ids: Vec<&ActionId> = fetched.iter().map(SeriesBoundary::id).collect();
            fetched_ids.sort();
            fetched_ids.dedup();
            prop_assert_eq!(fresh.iter().map(SeriesBoundary::id).collect::<Vec<_>>(), fetched_ids);
            for boundary in &fetched {
                prop_assert!(once.iter().any(|held| held.id() == boundary.id()));
            }
        }
    }
}
