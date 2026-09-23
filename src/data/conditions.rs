//! Which trades count, and under whose rule.
//!
//! Massive spells a condition by identifier and Alpaca by SIP character; both resolve here first.
//! The rows are published and loaded; the house rule below is ours and stays compiled in.

use crate::common::types::{Tape, TradeConditions};

/// One row of the provider's sale-condition reference.
///
/// The three `updates_*` flags are the provider's, copied rather than interpreted. The characters
/// are the same condition as each SIP spells it, which is what lets an Alpaca trade reach this table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SaleCondition {
    pub identifier: u32,
    pub name: String,
    pub updates_volume: bool,
    pub updates_high_low: bool,
    pub updates_open_close: bool,
    pub consolidated_tape_association: Option<u8>,
    pub unlisted_trading_privileges: Option<u8>,
    pub trade_data_dissemination: Option<u8>,
}

impl SaleCondition {
    /// The character this condition is spelled with on `tape`, if that SIP publishes it at all.
    fn character_on(&self, tape: Tape) -> Option<u8> {
        match tape {
            Tape::ConsolidatedTapeAssociation => self.consolidated_tape_association,
            Tape::UnlistedTradingPrivileges => self.unlisted_trading_privileges,
            Tape::TradeDataDissemination => self.trade_data_dissemination,
        }
    }
}

/// What the archive is entitled to do with a trade, and who said so.
///
/// `Ambiguous` is a real answer rather than a failure: a SIP character can name two conditions that
/// disagree, and the honest response is to say so instead of picking the convenient one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Eligibility {
    Eligible,
    Ineligible,
    Ambiguous,
}

/// A vendor row the archive's own numbers depend on, checked when a table is loaded.
///
/// The identifier is the vendor's namespace and these dependencies are ours, so each carries the
/// name it was written against: a renumbering would otherwise leave the rules pointing at different
/// conditions without a word.
struct Anchor {
    identifier: u32,
    name: &'static str,
    updates_volume: bool,
    /// Whether the house rule excludes this condition's price — ours, not the provider's.
    not_a_market_price: bool,
}

/// The five rows the archive cannot be wrong about, and what it believes of each.
///
/// Two dependencies, checked the same way. **The house rule** excludes 2 and 10: both are
/// volume-eligible and belong in VWAP, but an average-price trade reports a session average and a
/// derivatively priced one is computed off another instrument, so differencing either against the
/// prevailing quote measures the convention rather than the cost. **The auction prints** are what
/// the volume rule exists to exclude — measured 2026-08-21, 246 of them carried 14.1% of the
/// session's dollar volume, so a vendor flipping one to volume-eligible would move every VWAP in the
/// archive silently. A refused night is healed by the next; a wrongly folded one is not.
const ANCHORS: [Anchor; 5] = [
    Anchor {
        identifier: 2,
        name: "Average Price Trade",
        updates_volume: true,
        not_a_market_price: true,
    },
    Anchor {
        identifier: 10,
        name: "Derivatively Priced",
        updates_volume: true,
        not_a_market_price: true,
    },
    Anchor {
        identifier: 15,
        name: "Market Center Official Close",
        updates_volume: false,
        not_a_market_price: false,
    },
    Anchor {
        identifier: 16,
        name: "Market Center Official Open",
        updates_volume: false,
        not_a_market_price: false,
    },
    Anchor {
        identifier: 38,
        name: "Corrected Consolidated Close (per listing market)",
        updates_volume: false,
        not_a_market_price: false,
    },
];

/// Why a published conditions table was refused.
///
/// Each variant carries what produced the refusal, because a table is rejected while nobody is
/// watching and the message is the whole record of what was wrong with it.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ConditionsError {
    #[error("the table holds no conditions, which would report every trade as unresolved")]
    Empty,
    #[error("identifier {identifier} appears more than once, so a lookup answers with whichever row came first")]
    Duplicated { identifier: u32 },
    #[error("the archive expects {identifier} to be \"{expected}\", and the table {found}")]
    Anchor {
        identifier: u32,
        expected: &'static str,
        found: String,
    },
    #[error("condition {identifier} claims the unspellable byte on {tape}, so a token no table can spell would acquire its eligibility")]
    ClaimsTheSentinel { identifier: u32, tape: &'static str },
}

/// The provider's sale-condition rows, loaded rather than compiled in.
///
/// A value in scope is proof that identifiers are unique — which [`by_identifier`]'s scan needs and
/// nothing downstream re-checks — and that the house rule still names the conditions it was written
/// against.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConditionsTable {
    as_of: chrono::NaiveDate,
    conditions: Vec<SaleCondition>,
}

impl ConditionsTable {
    /// Builds a table from published rows, refusing anything the lookups could answer unsoundly.
    pub fn new(
        as_of: chrono::NaiveDate,
        mut conditions: Vec<SaleCondition>,
    ) -> Result<Self, ConditionsError> {
        // Sorted here rather than demanded of the caller: the published order is the provider's, and
        // the invariant the lookup needs is uniqueness, which sorting cannot fake.
        conditions.sort_by_key(|condition| condition.identifier);
        if conditions.is_empty() {
            return Err(ConditionsError::Empty);
        }
        for pair in conditions.windows(2) {
            if pair[0].identifier == pair[1].identifier {
                return Err(ConditionsError::Duplicated {
                    identifier: pair[0].identifier,
                });
            }
        }
        for anchor in &ANCHORS {
            let found = match conditions
                .iter()
                .find(|condition| condition.identifier == anchor.identifier)
            {
                None => "does not carry it".to_string(),
                Some(condition) if condition.name != anchor.name => {
                    format!("calls it \"{}\"", condition.name)
                }
                Some(condition) if condition.updates_volume != anchor.updates_volume => format!(
                    "makes it {}volume-eligible",
                    if condition.updates_volume { "" } else { "in" }
                ),
                Some(_) => continue,
            };
            return Err(ConditionsError::Anchor {
                identifier: anchor.identifier,
                expected: anchor.name,
                found,
            });
        }
        // A tape claiming the sentinel would resolve a token no table can spell to a real condition,
        // and a malformed print would quietly acquire that condition's eligibility.
        for condition in &conditions {
            for (column, tape) in TAPE_COLUMNS {
                if condition.character_on(tape) == Some(TradeConditions::UNSPELLABLE) {
                    return Err(ConditionsError::ClaimsTheSentinel {
                        identifier: condition.identifier,
                        tape: column,
                    });
                }
            }
        }
        Ok(Self { as_of, conditions })
    }

    /// The date the provider's reference endpoint was read into the partition this table holds.
    pub fn as_of(&self) -> chrono::NaiveDate {
        self.as_of
    }

    /// The rows this table holds, so a caller can walk them without a second copy to disagree with.
    pub fn conditions(&self) -> &[SaleCondition] {
        &self.conditions
    }
}

/// Column names of the published table, written once so the writer and the reader cannot disagree.
const TAPE_COLUMNS: [(&str, Tape); 3] = [
    (
        "consolidated_tape_association",
        Tape::ConsolidatedTapeAssociation,
    ),
    (
        "unlisted_trading_privileges",
        Tape::UnlistedTradingPrivileges,
    ),
    ("trade_data_dissemination", Tape::TradeDataDissemination),
];

impl ConditionsTable {
    /// The published rows, in the shape the dataset stores.
    ///
    /// A tape character is stored as the one-character string the SIP actually prints rather than as
    /// its byte, so the published table reads as the provider's own reference rather than as a
    /// column of numbers nobody can check by eye.
    pub fn to_dataframe(&self) -> Result<polars::prelude::DataFrame, polars::prelude::PolarsError> {
        use polars::prelude::*;

        let spelling = |condition: &SaleCondition, tape: Tape| {
            condition
                .character_on(tape)
                .map(|character| String::from_utf8_lossy(&[character]).into_owned())
        };

        let mut columns = vec![
            Column::new(
                "identifier".into(),
                self.conditions
                    .iter()
                    .map(|condition| condition.identifier)
                    .collect::<Vec<u32>>(),
            ),
            Column::new(
                "name".into(),
                self.conditions
                    .iter()
                    .map(|condition| condition.name.as_str())
                    .collect::<Vec<&str>>(),
            ),
        ];
        for (name, flag) in [
            ("updates_volume", 0usize),
            ("updates_high_low", 1),
            ("updates_open_close", 2),
        ] {
            columns.push(Column::new(
                name.into(),
                self.conditions
                    .iter()
                    .map(|condition| match flag {
                        0 => condition.updates_volume,
                        1 => condition.updates_high_low,
                        _ => condition.updates_open_close,
                    })
                    .collect::<Vec<bool>>(),
            ));
        }
        for (name, tape) in TAPE_COLUMNS {
            columns.push(Column::new(
                name.into(),
                self.conditions
                    .iter()
                    .map(|condition| spelling(condition, tape))
                    .collect::<Vec<Option<String>>>(),
            ));
        }
        DataFrame::new(columns)
    }

    /// Reads a published frame back, refusing a spelling that is not one character.
    ///
    /// A SIP prints exactly one byte per condition, so a longer string is a corrupt row rather than
    /// a wider spelling, and truncating it would invent a condition the tape never carried.
    pub fn from_dataframe(
        as_of: chrono::NaiveDate,
        frame: &polars::prelude::DataFrame,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        use polars::prelude::*;

        let identifiers = frame.column("identifier")?.cast(&DataType::UInt32)?;
        let identifiers = identifiers.u32()?;
        let names = frame.column("name")?.str()?;
        let volume = frame.column("updates_volume")?.bool()?;
        let high_low = frame.column("updates_high_low")?.bool()?;
        let open_close = frame.column("updates_open_close")?.bool()?;

        let mut spellings = Vec::with_capacity(TAPE_COLUMNS.len());
        for (name, _) in TAPE_COLUMNS {
            spellings.push(frame.column(name)?.str()?.clone());
        }

        let mut conditions = Vec::with_capacity(frame.height());
        for index in 0..frame.height() {
            let (Some(identifier), Some(name), Some(volume), Some(high_low), Some(open_close)) = (
                identifiers.get(index),
                names.get(index),
                volume.get(index),
                high_low.get(index),
                open_close.get(index),
            ) else {
                return Err(
                    format!("row {index} of the conditions table has a null column").into(),
                );
            };
            let mut characters = [None, None, None];
            for (position, spelling) in spellings.iter().enumerate() {
                characters[position] = match spelling.get(index) {
                    None => None,
                    Some(text) if text.len() == 1 => Some(text.as_bytes()[0]),
                    Some(text) => {
                        return Err(format!(
                            "condition {identifier} is spelled \"{text}\" on {}, which is not one character",
                            TAPE_COLUMNS[position].0
                        )
                        .into())
                    }
                };
            }
            conditions.push(SaleCondition {
                identifier,
                name: name.to_string(),
                updates_volume: volume,
                updates_high_low: high_low,
                updates_open_close: open_close,
                consolidated_tape_association: characters[0],
                unlisted_trading_privileges: characters[1],
                trade_data_dissemination: characters[2],
            });
        }

        Ok(Self::new(as_of, conditions)?)
    }
}

/// The condition with this identifier, as Massive spells it.
pub fn by_identifier(table: &ConditionsTable, identifier: u32) -> Option<&SaleCondition> {
    table
        .conditions
        .iter()
        .find(|condition| condition.identifier == identifier)
}

/// Every condition `character` could name on `tape`, as Alpaca spells it.
///
/// A slice rather than an option because the mapping is not injective: CTA `I` is both an odd lot
/// and a CAP election, and UTP `V` is both a stock option and a contingent trade.
pub fn by_character(table: &ConditionsTable, character: u8, tape: Tape) -> Vec<&SaleCondition> {
    table
        .conditions
        .iter()
        .filter(|condition| condition.character_on(tape) == Some(character))
        .collect()
}

/// Whether a set of identifiers leaves the trade eligible for consolidated volume.
///
/// Ineligible wins over eligible: one disqualifying condition disqualifies the print, however many
/// ordinary ones sit beside it.
pub fn volume_eligibility(table: &ConditionsTable, identifiers: &[u32]) -> Eligibility {
    let mut unresolved = false;
    for identifier in identifiers {
        match by_identifier(table, *identifier) {
            Some(condition) if !condition.updates_volume => return Eligibility::Ineligible,
            Some(_) => {}
            // Non-disqualifying by decision: an unknown code is far likelier to be a namespace this
            // table does not cover than a volume rule the provider forgot to publish.
            None => unresolved = true,
        }
    }
    if unresolved {
        Eligibility::Ambiguous
    } else {
        Eligibility::Eligible
    }
}

/// The marker a SIP spells "no condition applies" with, which is not a condition.
///
/// Tape-dependent like every other spelling here: CTA writes a space and UTP an at-sign, and the
/// provider's reference table has a row for neither. Counting it unknown reports nearly every print
/// on the tape as unreadable, so both spellings must be recognized rather than one.
fn regular_sale_on(tape: Tape) -> u8 {
    match tape {
        Tape::ConsolidatedTapeAssociation => b' ',
        // Alpaca reaches neither of these with a `D`, so FINRA arrives spelled as its listing
        // venue's tape rather than its own; the at-sign is UTP's and is what a `C` row carries.
        Tape::UnlistedTradingPrivileges | Tape::TradeDataDissemination => b'@',
    }
}

/// The volume rule, asked of however the provider spelled the conditions.
///
/// The dispatch lives here rather than at the fold, so a caller cannot read Alpaca's characters
/// against the identifier table by reaching for the wrong function.
pub fn volume_eligibility_of(table: &ConditionsTable, conditions: &TradeConditions) -> Eligibility {
    match conditions {
        TradeConditions::Identified(identifiers) => volume_eligibility(table, identifiers),
        TradeConditions::Spelled { characters, tape } => {
            volume_eligibility_from_characters(table, characters, *tape)
        }
    }
}

/// The market-price question, asked of however the provider spelled the conditions.
pub fn carries_a_market_price_of(table: &ConditionsTable, conditions: &TradeConditions) -> bool {
    match conditions {
        TradeConditions::Identified(identifiers) => carries_a_market_price(identifiers),
        TradeConditions::Spelled { characters, tape } => {
            carries_a_market_price_from_characters(table, characters, *tape)
        }
    }
}

/// The same question asked of Alpaca's characters, which is answerable only when they agree.
///
/// Every colliding character pair agrees on `updates_volume` today, so this returns [`Eligibility::
/// Ambiguous`] for a genuinely new collision rather than for any that exists now — the test below
/// is what keeps that true.
pub fn volume_eligibility_from_characters(
    table: &ConditionsTable,
    characters: &[u8],
    tape: Tape,
) -> Eligibility {
    let mut unresolved = false;
    for character in characters {
        if *character == regular_sale_on(tape) {
            continue;
        }
        let candidates = by_character(table, *character, tape);
        if candidates.is_empty() {
            unresolved = true;
            continue;
        }
        let mut verdicts = candidates.iter().map(|condition| condition.updates_volume);
        let first = verdicts.next().unwrap_or(true);
        if !verdicts.all(|verdict| verdict == first) {
            return Eligibility::Ambiguous;
        }
        if !first {
            return Eligibility::Ineligible;
        }
    }
    if unresolved {
        Eligibility::Ambiguous
    } else {
        Eligibility::Eligible
    }
}

/// Whether the trade's price can be differenced against the quote standing when it printed.
///
/// The house rule. Volume eligibility is asked separately and answers a different question — a print
/// can belong in the day's volume and still be useless for measuring what a trade cost.
pub fn carries_a_market_price(identifiers: &[u32]) -> bool {
    !identifiers
        .iter()
        .any(|identifier| excluded_by_the_house_rule(*identifier))
}

/// The same house rule against Alpaca's characters.
///
/// Both codes are collision-free on every tape, so unlike the high-low rules this needs no ambiguous
/// arm; the test below is what holds the provider to that.
pub fn carries_a_market_price_from_characters(
    table: &ConditionsTable,
    characters: &[u8],
    tape: Tape,
) -> bool {
    !characters.iter().any(|character| {
        by_character(table, *character, tape)
            .iter()
            .any(|condition| excluded_by_the_house_rule(condition.identifier))
    })
}

/// Whether the house rule names this identifier, whichever spelling reached it.
fn excluded_by_the_house_rule(identifier: u32) -> bool {
    ANCHORS
        .iter()
        .any(|anchor| anchor.not_a_market_price && anchor.identifier == identifier)
}

/// Fixtures for tests that need a loaded table, kept beside the rules they exercise.
///
/// Three modules fold trades under test and three private copies of this would drift apart.
#[cfg(test)]
pub(crate) mod fixture {
    use super::*;

    pub(crate) fn date() -> chrono::NaiveDate {
        chrono::NaiveDate::from_ymd_opt(2026, 8, 31).expect("a real date")
    }

    pub(crate) fn row(
        identifier: u32,
        name: &str,
        updates_volume: bool,
        spellings: &[(Tape, u8)],
    ) -> SaleCondition {
        let character = |wanted: Tape| {
            spellings
                .iter()
                .find(|(tape, _)| *tape == wanted)
                .map(|(_, character)| *character)
        };
        SaleCondition {
            identifier,
            name: name.to_string(),
            updates_volume,
            updates_high_low: updates_volume,
            updates_open_close: updates_volume,
            consolidated_tape_association: character(Tape::ConsolidatedTapeAssociation),
            unlisted_trading_privileges: character(Tape::UnlistedTradingPrivileges),
            trade_data_dissemination: character(Tape::TradeDataDissemination),
        }
    }

    /// Rows enough to exercise every rule, and the five the constructor insists on.
    ///
    /// Deliberately **not** a copy of the provider's table. What Massive actually publishes is
    /// checked against the published object by `tools/fetch-trade-conditions --check` and, for the
    /// five rows the archive's numbers depend on, by [`ConditionsTable::new`] on every load. A
    /// fixture asserting the vendor's content would only prove it agrees with itself.
    pub(crate) fn rows() -> Vec<SaleCondition> {
        use Tape::*;
        vec![
            row(
                2,
                "Average Price Trade",
                true,
                &[
                    (ConsolidatedTapeAssociation, b'B'),
                    (UnlistedTradingPrivileges, b'W'),
                    (TradeDataDissemination, b'W'),
                ],
            ),
            row(
                5,
                "CAP Election",
                true,
                &[(ConsolidatedTapeAssociation, b'I')],
            ),
            row(
                10,
                "Derivatively Priced",
                true,
                &[
                    (ConsolidatedTapeAssociation, b'4'),
                    (UnlistedTradingPrivileges, b'4'),
                ],
            ),
            row(
                14,
                "Intermarket Sweep",
                true,
                &[(ConsolidatedTapeAssociation, b'F')],
            ),
            row(
                15,
                "Market Center Official Close",
                false,
                &[(ConsolidatedTapeAssociation, b'M')],
            ),
            row(
                16,
                "Market Center Official Open",
                false,
                &[(ConsolidatedTapeAssociation, b'Q')],
            ),
            row(
                37,
                "Odd Lot Trade",
                true,
                &[(ConsolidatedTapeAssociation, b'I')],
            ),
            row(
                38,
                "Corrected Consolidated Close (per listing market)",
                false,
                &[],
            ),
        ]
    }

    pub(crate) fn table() -> ConditionsTable {
        ConditionsTable::new(date(), rows()).expect("the fixture must satisfy every anchor")
    }
}

#[cfg(test)]
mod tests {
    use super::fixture::*;
    use super::*;

    /// An empty table answers every print with the fallback, which reads like an unreadable tape.
    #[test]
    fn test_a_table_holding_no_conditions_is_refused() {
        assert_eq!(
            ConditionsTable::new(date(), Vec::new()),
            Err(ConditionsError::Empty)
        );
    }

    /// `by_identifier` scans, so a duplicate answers with whichever row happens to come first.
    #[test]
    fn test_a_duplicated_identifier_is_refused() {
        let mut duplicated = rows();
        duplicated.push(row(37, "Odd Lot Trade", false, &[]));
        assert_eq!(
            ConditionsTable::new(date(), duplicated),
            Err(ConditionsError::Duplicated { identifier: 37 })
        );
    }

    /// The house rule names identifiers, and the identifiers are the vendor's to renumber.
    #[test]
    fn test_an_anchor_the_provider_renamed_is_refused() {
        let renamed: Vec<SaleCondition> = rows()
            .into_iter()
            .map(|condition| match condition.identifier {
                10 => row(10, "Something Else Entirely", true, &[]),
                _ => condition,
            })
            .collect();
        assert_eq!(
            ConditionsTable::new(date(), renamed),
            Err(ConditionsError::Anchor {
                identifier: 10,
                expected: "Derivatively Priced",
                found: "calls it \"Something Else Entirely\"".to_string(),
            })
        );
    }

    /// The auction prints are 14.1% of a session's dollar volume, so this cannot change quietly.
    ///
    /// Measured 2026-08-21 over 246 prints. A vendor flipping one to volume-eligible would move
    /// every VWAP in the archive, and the fold cannot be undone — so it is refused on arrival.
    #[test]
    fn test_an_auction_print_turned_volume_eligible_is_refused() {
        let flipped: Vec<SaleCondition> = rows()
            .into_iter()
            .map(|condition| match condition.identifier {
                15 => row(
                    15,
                    "Market Center Official Close",
                    true,
                    &[(Tape::ConsolidatedTapeAssociation, b'M')],
                ),
                _ => condition,
            })
            .collect();
        assert_eq!(
            ConditionsTable::new(date(), flipped),
            Err(ConditionsError::Anchor {
                identifier: 15,
                expected: "Market Center Official Close",
                found: "makes it volume-eligible".to_string(),
            })
        );
    }

    /// The transport substitutes the sentinel for a token no table can spell.
    ///
    /// A row claiming that byte would resolve a malformed print to a real condition, and the print
    /// would silently acquire its eligibility.
    #[test]
    fn test_a_row_claiming_the_unspellable_byte_is_refused() {
        let mut claiming = rows();
        claiming.push(row(
            99,
            "Impostor",
            true,
            &[(
                Tape::UnlistedTradingPrivileges,
                TradeConditions::UNSPELLABLE,
            )],
        ));
        assert_eq!(
            ConditionsTable::new(date(), claiming),
            Err(ConditionsError::ClaimsTheSentinel {
                identifier: 99,
                tape: "unlisted_trading_privileges",
            })
        );
    }

    /// Both spellings of one condition reach the same row.
    #[test]
    fn test_a_condition_resolves_from_either_provider_spelling() {
        let table = table();
        let odd_lot = by_identifier(&table, 37).expect("the fixture carries it");
        assert_eq!(odd_lot.name, "Odd Lot Trade");
        assert!(
            by_character(&table, b'I', Tape::ConsolidatedTapeAssociation).contains(&odd_lot),
            "the character reaches the row the identifier does"
        );
    }

    /// One disqualifying code outweighs any number of ordinary ones beside it.
    #[test]
    fn test_ineligibility_wins_over_the_conditions_it_sits_beside() {
        let table = table();
        assert_eq!(volume_eligibility(&table, &[]), Eligibility::Eligible);
        assert_eq!(volume_eligibility(&table, &[37]), Eligibility::Eligible);
        assert_eq!(
            volume_eligibility(&table, &[14, 2, 37]),
            Eligibility::Eligible
        );
        assert_eq!(
            volume_eligibility(&table, &[16, 37]),
            Eligibility::Ineligible
        );
    }

    /// A code this table does not carry is reported, never silently treated as ordinary.
    ///
    /// The trades file mixes namespaces — 41 is a trade-through exemption rather than a sale
    /// condition — so an unresolved code is the common case and must stay visible.
    #[test]
    fn test_an_unresolved_code_is_ambiguous_rather_than_eligible() {
        let table = table();
        assert_eq!(volume_eligibility(&table, &[9_999]), Eligibility::Ambiguous);
        assert_eq!(
            volume_eligibility(&table, &[37, 9_999]),
            Eligibility::Ambiguous
        );
        // Still ineligible: a code we cannot read does not rescue one we can.
        assert_eq!(
            volume_eligibility(&table, &[15, 9_999]),
            Eligibility::Ineligible
        );
    }

    /// The sentinel resolves as unknown on every tape, which is the whole point of it.
    #[test]
    fn test_the_unspellable_sentinel_is_ambiguous_on_every_tape() {
        let table = table();
        for tape in Tape::ALL {
            assert!(
                by_character(&table, TradeConditions::UNSPELLABLE, tape).is_empty(),
                "no row may claim the sentinel on {tape:?}"
            );
            assert_eq!(
                volume_eligibility_from_characters(&table, &[TradeConditions::UNSPELLABLE], tape),
                Eligibility::Ambiguous,
                "an unspellable token must stay visible on {tape:?}"
            );
        }
        // A token we cannot read does not rescue one we can.
        assert_eq!(
            volume_eligibility_from_characters(
                &table,
                &[b'M', TradeConditions::UNSPELLABLE],
                Tape::ConsolidatedTapeAssociation
            ),
            Eligibility::Ineligible
        );
    }

    /// A character naming two conditions is answerable only when they agree.
    ///
    /// The fixture's CTA `I` names two rows that agree, so it resolves; a disagreeing pair is what
    /// `Ambiguous` exists for, and the archive would otherwise pick whichever came first.
    #[test]
    fn test_a_colliding_character_resolves_only_when_its_rows_agree() {
        let table = table();
        assert_eq!(
            by_character(&table, b'I', Tape::ConsolidatedTapeAssociation).len(),
            2,
            "the fixture's collision must really collide"
        );
        assert_eq!(
            volume_eligibility_from_characters(&table, b"I", Tape::ConsolidatedTapeAssociation),
            Eligibility::Eligible,
            "two rows that agree answer"
        );

        let mut disagreeing = rows();
        disagreeing.push(row(
            99,
            "Contrary",
            false,
            &[(Tape::ConsolidatedTapeAssociation, b'I')],
        ));
        let table = ConditionsTable::new(date(), disagreeing).expect("anchors are untouched");
        assert_eq!(
            volume_eligibility_from_characters(&table, b"I", Tape::ConsolidatedTapeAssociation),
            Eligibility::Ambiguous,
            "a collision that disagrees must not be resolved by row order"
        );
    }

    /// The house rule keeps odd lots and drops the two conventions that are not market prices.
    #[test]
    fn test_the_house_spread_rule_drops_only_prices_that_are_not_market_prices() {
        assert!(carries_a_market_price(&[]));
        assert!(
            carries_a_market_price(&[37]),
            "odd lots are real executions"
        );
        assert!(carries_a_market_price(&[14, 41]));
        assert!(
            !carries_a_market_price(&[2]),
            "an average price is not a quote"
        );
        assert!(!carries_a_market_price(&[10, 37, 41]));

        // And through Alpaca's spelling, which has to resolve the character first.
        let table = table();
        assert!(!carries_a_market_price_from_characters(
            &table,
            b"B",
            Tape::ConsolidatedTapeAssociation
        ));
        assert!(carries_a_market_price_from_characters(
            &table,
            b"I",
            Tape::ConsolidatedTapeAssociation
        ));
    }

    /// The marker a SIP spells "no condition applies" with is not a condition.
    #[test]
    fn test_the_regular_sale_marker_is_read_on_each_tape_that_spells_it() {
        let table = table();
        assert_eq!(
            volume_eligibility_from_characters(&table, b" ", Tape::ConsolidatedTapeAssociation),
            Eligibility::Eligible,
            "CTA spells an ordinary print with a space"
        );
        assert_eq!(
            volume_eligibility_from_characters(&table, b"@", Tape::UnlistedTradingPrivileges),
            Eligibility::Eligible,
            "UTP spells it with an at-sign"
        );

        // Beside a real condition it still resolves, which is the common shape on the tape.
        assert_eq!(
            volume_eligibility_from_characters(&table, b" F", Tape::ConsolidatedTapeAssociation),
            volume_eligibility_from_characters(&table, b"F", Tape::ConsolidatedTapeAssociation),
            "the marker contributes nothing beyond itself"
        );
    }

    /// Each provider's spelling is read against its own half of the table.
    #[test]
    fn test_each_spelling_is_read_against_its_own_table() {
        let table = table();
        let spelled = TradeConditions::Spelled {
            characters: vec![b'M'],
            tape: Tape::ConsolidatedTapeAssociation,
        };
        assert_eq!(
            volume_eligibility_of(&table, &spelled),
            Eligibility::Ineligible
        );

        // The same byte read as an identifier, which is what reading it against the wrong table
        // would do. A different answer, which is what makes the assertion above load-bearing.
        assert_eq!(
            volume_eligibility(&table, &[u32::from(b'M')]),
            Eligibility::Ambiguous
        );

        assert_eq!(
            volume_eligibility_of(&table, &TradeConditions::Identified(vec![15])),
            Eligibility::Ineligible,
            "identifier 15 is the same condition, spelled the other way"
        );
    }

    /// The two spellings of the same condition reach the same verdict.
    #[test]
    fn test_the_two_provider_spellings_agree_on_eligibility() {
        let table = table();
        let tape = Tape::ConsolidatedTapeAssociation;
        for (identifiers, characters, expected) in [
            (vec![37u32], b"I".to_vec(), Eligibility::Eligible),
            (vec![16], b"Q".to_vec(), Eligibility::Ineligible),
            (vec![15], b"M".to_vec(), Eligibility::Ineligible),
            (vec![14], b"F".to_vec(), Eligibility::Eligible),
        ] {
            assert_eq!(
                volume_eligibility(&table, &identifiers),
                expected,
                "{identifiers:?} must be {expected:?}"
            );
            assert_eq!(
                volume_eligibility_from_characters(&table, &characters, tape),
                expected,
                "{characters:?} must be {expected:?}"
            );
        }
    }

    /// Every row survives the shape the dataset stores it in, including an absent spelling.
    ///
    /// The property the published table is trusted on: a column silently dropped or a `None` read
    /// back as a character would change an eligibility verdict with nothing to show for it.
    #[test]
    fn test_a_table_round_trips_through_the_dataset_shape() {
        let table = table();
        let frame = table.to_dataframe().expect("the frame must build");
        assert_eq!(frame.height(), rows().len(), "every row must be written");
        let read = ConditionsTable::from_dataframe(date(), &frame).expect("it must read back");
        assert_eq!(read, table, "the round trip must return what it was given");

        // The absent spelling specifically: 38 is on no tape, and a null read as a byte would put
        // it on one.
        let corrected = by_identifier(&read, 38).expect("the row survived");
        assert_eq!(corrected.consolidated_tape_association, None);
        assert_eq!(corrected.unlisted_trading_privileges, None);
        assert_eq!(corrected.trade_data_dissemination, None);
    }

    /// A SIP prints one byte, so a longer spelling is a corrupt row rather than a wider character.
    #[test]
    fn test_a_spelling_that_is_not_one_character_is_refused() {
        use polars::prelude::*;

        let table = table();
        let mut frame = table.to_dataframe().expect("the frame must build");
        let mut spellings: Vec<Option<&str>> = vec![None; frame.height()];
        spellings[0] = Some("BB");
        frame
            .with_column(Column::new(
                "consolidated_tape_association".into(),
                spellings,
            ))
            .expect("the column must replace");

        let error = ConditionsTable::from_dataframe(date(), &frame)
            .expect_err("a two-character spelling must be refused");
        assert!(
            error.to_string().contains("not one character"),
            "the refusal must say what was wrong: {error}"
        );
    }

    /// Alpaca's tape letters and Massive's numeric markers name the same SIPs.
    #[test]
    fn test_alpacas_tape_letters_name_the_same_sips_as_the_numeric_markers() {
        assert_eq!(
            Tape::from_letter(b'A'),
            Some(Tape::ConsolidatedTapeAssociation)
        );
        assert_eq!(
            Tape::from_letter(b'B'),
            Some(Tape::ConsolidatedTapeAssociation)
        );
        assert_eq!(
            Tape::from_letter(b'C'),
            Some(Tape::UnlistedTradingPrivileges)
        );
        // No letter names FINRA: a TRF print is disseminated on its listing venue's tape.
        assert_eq!(Tape::from_letter(b'D'), None);
        assert_eq!(Tape::from_letter(b'Q'), None);

        // The two spellings agree about which SIP they mean, which is what lets one table serve
        // both providers.
        assert_eq!(Tape::from_letter(b'A'), Tape::from_marker(1));
        assert_eq!(Tape::from_letter(b'C'), Tape::from_marker(3));
    }
}
