//! Alpaca's corporate actions read as series boundaries: the renames, spin-offs, rights distributions, unit
//! separations and reorganizations a symbol's price series may not be read across.

use chrono::NaiveDate;
use serde::Deserialize;

use super::super::retry::{FetchError, send, with_retries};
use super::super::{RefusedRow, RowRefusal};
use super::{Alpaca, paginate};
use crate::common::market::Symbol;
use crate::common::market::corporate_actions::{ActionId, BoundaryChange, SeriesBoundary};
use crate::common::monoid::{Monoid, concatenate};
use crate::common::time::{SessionDate, SessionRange};

const CORPORATE_ACTIONS_URL: &str = "https://data.alpaca.markets/v1/corporate-actions";

/// The action types that bound a series. Splits come from Massive and rescale a series rather than end it, and a
/// merger or delisting ends the symbol's bars without being told.
const BOUNDARY_TYPES: &str = "name_change,spin_off,rights_distribution,unit_split,reorganization";

const PAGE_LIMIT: &str = "1000";

/// The boundaries Alpaca processed over a window, with every row that did not become one.
#[derive(Debug, Clone, PartialEq)]
pub struct SeriesBoundaries {
    boundaries: Vec<SeriesBoundary>,
    refused: Vec<RefusedRow>,
}

impl SeriesBoundaries {
    pub fn boundaries(&self) -> &[SeriesBoundary] {
        &self.boundaries
    }

    pub fn refused(&self) -> &[RefusedRow] {
        &self.refused
    }
}

/// Pages and windows concatenate in the order they were read.
impl Monoid for SeriesBoundaries {
    fn empty() -> Self {
        Self {
            boundaries: Vec::new(),
            refused: Vec::new(),
        }
    }

    fn combine(mut self, other: Self) -> Self {
        self.boundaries.extend(other.boundaries);
        self.refused.extend(other.refused);
        self
    }
}

impl Alpaca {
    /// The series boundaries Alpaca processed over `range`.
    pub async fn series_boundaries(
        &self,
        range: SessionRange,
    ) -> Result<SeriesBoundaries, FetchError> {
        let (start, end) = (range.first().to_string(), range.last().to_string());
        let (start, end) = (&start, &end);
        let pages = paginate(|page_token| async move {
            with_retries(|| {
                let mut query = vec![
                    ("start", start.as_str()),
                    ("end", end.as_str()),
                    ("types", BOUNDARY_TYPES),
                    ("limit", PAGE_LIMIT),
                ];
                if let Some(token) = page_token.as_deref() {
                    query.push(("page_token", token));
                }
                send(
                    self.credentials
                        .sign(self.http_client.get(CORPORATE_ACTIONS_URL))
                        .query(&query),
                )
            })
            .await
        })
        .await?;
        let read = pages
            .iter()
            .map(|page| parse_boundaries_page(page))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(concatenate(read))
    }
}

#[derive(Deserialize)]
struct CorporateActionsPage {
    /// Required, so a response without it fails rather than reading as a window with no actions, which would withdraw
    /// every boundary the window held.
    corporate_actions: Categories,
}

/// Each category is absent from a page that has none of it.
#[derive(Deserialize)]
struct Categories {
    #[serde(default)]
    name_changes: Vec<NameChange>,
    #[serde(default)]
    spin_offs: Vec<SpinOff>,
    #[serde(default)]
    rights_distributions: Vec<RightsDistribution>,
    #[serde(default)]
    unit_splits: Vec<UnitSplit>,
    #[serde(default)]
    reorganizations: Vec<Reorganization>,
}

#[derive(Deserialize)]
struct NameChange {
    id: String,
    old_symbol: String,
    new_symbol: String,
    process_date: Option<NaiveDate>,
}

#[derive(Deserialize)]
struct SpinOff {
    id: String,
    source_symbol: String,
    new_symbol: String,
    ex_date: Option<NaiveDate>,
    process_date: Option<NaiveDate>,
}

#[derive(Deserialize)]
struct RightsDistribution {
    id: String,
    source_symbol: String,
    ex_date: Option<NaiveDate>,
    process_date: Option<NaiveDate>,
}

#[derive(Deserialize)]
struct UnitSplit {
    id: String,
    old_symbol: String,
    effective_date: Option<NaiveDate>,
    process_date: Option<NaiveDate>,
}

#[derive(Deserialize)]
struct Reorganization {
    id: String,
    symbol: String,
    effective_date: Option<NaiveDate>,
    process_date: Option<NaiveDate>,
}

/// One action as the categories share it: whose series it ends, on what date, and what follows.
struct Action {
    id: String,
    symbol: String,
    on: Option<NaiveDate>,
    processed_on: Option<NaiveDate>,
    /// The change, already refused where its related symbol is malformed.
    change: Result<BoundaryChange, RowRefusal>,
}

/// One page's boundaries. The feed fills symbol fields with CUSIP placeholders and reports renames that change no
/// symbol, so most refusals are routine.
fn parse_boundaries_page(body: &[u8]) -> Result<SeriesBoundaries, FetchError> {
    let page: CorporateActionsPage =
        serde_json::from_slice(body).map_err(|error| FetchError::Malformed {
            reason: error.to_string(),
        })?;
    let categories = page.corporate_actions;
    let actions = categories
        .name_changes
        .into_iter()
        .map(|row| Action {
            id: row.id,
            symbol: row.old_symbol,
            on: row.process_date,
            processed_on: row.process_date,
            change: related(&row.new_symbol).map(|to| BoundaryChange::Renamed { to }),
        })
        .chain(categories.spin_offs.into_iter().map(|row| Action {
            id: row.id,
            symbol: row.source_symbol,
            on: row.ex_date,
            processed_on: row.process_date,
            change: related(&row.new_symbol).map(|company| BoundaryChange::SpunOff { company }),
        }))
        .chain(
            categories
                .rights_distributions
                .into_iter()
                .map(|row| Action {
                    id: row.id,
                    symbol: row.source_symbol,
                    on: row.ex_date,
                    processed_on: row.process_date,
                    change: Ok(BoundaryChange::RightsDistributed),
                }),
        )
        .chain(categories.unit_splits.into_iter().map(|row| Action {
            id: row.id,
            symbol: row.old_symbol,
            on: row.effective_date,
            processed_on: row.process_date,
            change: Ok(BoundaryChange::UnitSeparated),
        }))
        .chain(categories.reorganizations.into_iter().map(|row| Action {
            id: row.id,
            symbol: row.symbol,
            on: row.effective_date,
            processed_on: row.process_date,
            change: Ok(BoundaryChange::Reorganized),
        }));
    let mut read = SeriesBoundaries::empty();
    for action in actions {
        let ticker = action.symbol.clone();
        match boundary(action) {
            Ok(boundary) => read.boundaries.push(boundary),
            Err(cause) => read.refused.push(RefusedRow { ticker, cause }),
        }
    }
    Ok(read)
}

fn related(raw: &str) -> Result<Symbol, RowRefusal> {
    Symbol::new(raw).map_err(RowRefusal::Symbol)
}

fn boundary(action: Action) -> Result<SeriesBoundary, RowRefusal> {
    let symbol = Symbol::new(&action.symbol).map_err(RowRefusal::Symbol)?;
    let change = action.change?;
    let (Some(on), Some(processed_on)) = (action.on, action.processed_on) else {
        return Err(RowRefusal::Undated);
    };
    SeriesBoundary::new(
        ActionId::new(&action.id).map_err(RowRefusal::ActionId)?,
        symbol,
        SessionDate::from_date(on),
        SessionDate::from_date(processed_on),
        change,
    )
    .map_err(RowRefusal::Boundary)
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;
    use crate::common::monoid::laws;

    /// Rows of each category Alpaca answered for 2025-10-07 to 2026-10-07: a rename and one that changes no symbol, a
    /// spin-off, a rights distribution, a unit separation, and a reorganization beside one filed under a CUSIP.
    const PAGE: &str = r#"{"corporate_actions": {"name_changes": [{"id": "d28956f4-5474-489c-ae4e-b66e735b0e0b", "new_cusip": "G0R38G104", "new_symbol": "XRPN", "old_cusip": "G0R38G104", "old_symbol": "AACI", "process_date": "2025-10-30"}, {"id": "ff96483e-fe02-4e1c-bc80-e4b9b11a1235", "new_cusip": "44053A424", "new_symbol": "AESR", "old_cusip": "90214Q733", "old_symbol": "AESR", "process_date": "2026-07-02"}], "spin_offs": [{"ex_date": "2026-04-20", "id": "fefc53e6-1dc4-4df2-a823-fac0a217c8a1", "new_cusip": "337185102", "new_rate": 1, "new_symbol": "TRAX", "payable_date": "2026-04-20", "process_date": "2026-04-20", "record_date": "2026-04-06", "source_cusip": "032724106", "source_rate": 1, "source_symbol": "ANAB"}], "rights_distributions": [{"ex_date": "2026-02-10", "expiration_date": "2026-02-27", "id": "ff51bac9-4bf4-42e2-a8e2-93fe930ee1bc", "new_cusip": "009RGT010", "new_symbol": "009RGT010", "payable_date": "2026-02-19", "process_date": "2026-02-19", "rate": 1, "record_date": "2026-02-10", "source_cusip": "00901B303", "source_symbol": "AIM"}], "unit_splits": [{"alternate_cusip": "G0679A118", "alternate_rate": 0.1667, "alternate_symbol": "ACAAW", "effective_date": "2026-04-10", "id": "2a2f13a0-84a3-45d5-9392-04422569eb4f", "new_cusip": "G0679A100", "new_rate": 1, "new_symbol": "ACAA", "old_cusip": "G0679A126", "old_rate": 1, "old_symbol": "ACAAU", "process_date": "2026-04-10"}], "reorganizations": [{"cusip": "75944B106", "effective_date": "2026-07-23", "id": "cdcd1e0c-a057-474a-ad42-0638f8a27c04", "payable_date": "2026-07-24", "process_date": "2026-07-24", "stock_movements": [{"cusip": "G4860C107", "new_rate": 1, "source_rate": 1, "symbol": "BIOT"}], "symbol": "ACQC"}, {"cash_rate": 0.1, "cusip": "004ESC018", "effective_date": "2026-05-06", "id": "ced7f6a2-24d2-4268-b36d-621cbe0ed30d", "payable_date": "2026-05-11", "process_date": "2026-05-11", "symbol": "004ESC018"}]}, "next_page_token": null}"#;

    #[test]
    fn test_each_category_becomes_the_boundary_it_describes() {
        let read = parse_boundaries_page(PAGE.as_bytes()).unwrap();
        let boundaries: Vec<String> = read
            .boundaries()
            .iter()
            .map(|boundary| {
                format!(
                    "{} {} {} {} {:?}",
                    boundary.symbol().as_str(),
                    boundary.on(),
                    boundary.processed_on(),
                    boundary.change().kind(),
                    boundary.change().related().map(Symbol::as_str),
                )
            })
            .collect();
        assert_eq!(
            boundaries,
            [
                "AACI 2025-10-30 2025-10-30 renamed Some(\"XRPN\")",
                "ANAB 2026-04-20 2026-04-20 spun_off Some(\"TRAX\")",
                "AIM 2026-02-10 2026-02-19 rights_distributed None",
                "ACAAU 2026-04-10 2026-04-10 unit_separated None",
                "ACQC 2026-07-23 2026-07-24 reorganized None",
            ]
        );
        let refused: Vec<(&str, &str)> = read
            .refused()
            .iter()
            .map(|row| (row.ticker(), <&str>::from(row.cause().kind())))
            .collect();
        assert_eq!(refused, [("AESR", "boundary"), ("004ESC018", "symbol")]);
    }

    #[test]
    fn test_an_action_with_no_date_is_refused_and_an_empty_page_reads_as_none() {
        let undated = r#"{"corporate_actions": {"reorganizations": [{"id": "x", "symbol": "ACQC", "process_date": "2026-07-24"}]}}"#;
        let read = parse_boundaries_page(undated.as_bytes()).unwrap();
        assert!(read.boundaries().is_empty());
        assert_eq!(read.refused()[0].cause(), &RowRefusal::Undated);
        assert_eq!(
            parse_boundaries_page(br#"{"corporate_actions": {}, "next_page_token": null}"#),
            Ok(SeriesBoundaries::empty())
        );
        assert!(matches!(
            parse_boundaries_page(br#"{"next_page_token": null}"#),
            Err(FetchError::Malformed { .. })
        ));
    }

    /// Fragments cut from one page's boundaries and refusals, so the law test does not lean on `combine`.
    fn any_fragment() -> impl Strategy<Value = SeriesBoundaries> {
        let read = parse_boundaries_page(PAGE.as_bytes()).unwrap();
        (
            prop::sample::subsequence(read.boundaries, 0..=5),
            prop::sample::subsequence(read.refused, 0..=2),
        )
            .prop_map(|(boundaries, refused)| SeriesBoundaries {
                boundaries,
                refused,
            })
    }

    proptest! {
        #[test]
        fn property_boundary_pages_concatenate_as_a_monoid(
            first in any_fragment(),
            second in any_fragment(),
            third in any_fragment(),
        ) {
            laws::check_ordered(first, second, third)?;
        }
    }
}
