//! Which trades count, and under whose rule.
//!
//! Massive spells a condition by identifier and Alpaca by SIP character; both resolve here first.

use crate::common::types::Tape;
use crate::data::conditions_table::SALE_CONDITIONS;

/// One row of the provider's sale-condition reference.
///
/// The three `updates_*` flags are the provider's, copied rather than interpreted. The characters
/// are the same condition as each SIP spells it, which is what lets an Alpaca trade reach this table.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SaleCondition {
    pub identifier: u32,
    pub name: &'static str,
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

/// Conditions whose price is not a market price at the instant they printed.
///
/// Ours, not the provider's — both are volume-eligible and belong in VWAP. An average-price trade
/// reports a session average and a derivatively priced one is computed off another instrument, so
/// differencing either against the prevailing quote measures the convention rather than the cost.
const NOT_A_MARKET_PRICE: [u32; 2] = [2, 10];

/// The condition with this identifier, as Massive spells it.
pub fn by_identifier(identifier: u32) -> Option<&'static SaleCondition> {
    SALE_CONDITIONS
        .iter()
        .find(|condition| condition.identifier == identifier)
}

/// Every condition `character` could name on `tape`, as Alpaca spells it.
///
/// A slice rather than an option because the mapping is not injective: CTA `I` is both an odd lot
/// and a CAP election, and UTP `V` is both a stock option and a contingent trade.
pub fn by_character(character: u8, tape: Tape) -> Vec<&'static SaleCondition> {
    SALE_CONDITIONS
        .iter()
        .filter(|condition| condition.character_on(tape) == Some(character))
        .collect()
}

/// Whether a set of identifiers leaves the trade eligible for consolidated volume.
///
/// Ineligible wins over eligible: one disqualifying condition disqualifies the print, however many
/// ordinary ones sit beside it.
pub fn volume_eligibility(identifiers: &[u32]) -> Eligibility {
    let mut unresolved = false;
    for identifier in identifiers {
        match by_identifier(*identifier) {
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

/// The same question asked of Alpaca's characters, which is answerable only when they agree.
///
/// Every colliding character pair agrees on `updates_volume` today, so this returns [`Eligibility::
/// Ambiguous`] for a genuinely new collision rather than for any that exists now — the test below
/// is what keeps that true.
pub fn volume_eligibility_from_characters(characters: &[u8], tape: Tape) -> Eligibility {
    let mut unresolved = false;
    for character in characters {
        let candidates = by_character(*character, tape);
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
        .any(|identifier| NOT_A_MARKET_PRICE.contains(identifier))
}

/// The same house rule against Alpaca's characters.
///
/// Both codes are collision-free on every tape, so unlike the high-low rules this needs no ambiguous
/// arm; the test below is what holds the provider to that.
pub fn carries_a_market_price_from_characters(characters: &[u8], tape: Tape) -> bool {
    !characters.iter().any(|character| {
        by_character(*character, tape)
            .iter()
            .any(|condition| NOT_A_MARKET_PRICE.contains(&condition.identifier))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::conditions_table::FETCHED_ON;

    /// Both spellings of one condition reach the same row.
    ///
    /// Pinned to the literal `37`/`b'I'` rather than to a lookup, because a test that resolves the
    /// character through the same table it is checking would pass against an empty one.
    #[test]
    fn test_a_condition_resolves_from_either_provider_spelling() {
        let odd_lot = by_identifier(37).expect("the table carries the odd lot condition");
        assert_eq!(odd_lot.name, "Odd Lot Trade");
        assert!(odd_lot.updates_volume, "an odd lot is real volume");
        assert!(
            !odd_lot.updates_high_low,
            "an odd lot does not set the high"
        );

        let from_character = by_character(b'I', Tape::ConsolidatedTapeAssociation);
        assert!(
            from_character.contains(&odd_lot),
            "CTA spells the odd lot condition I"
        );
    }

    /// The auction prints are the whole reason the volume rule exists.
    ///
    /// Measured 2026-08-21: 246 trades carrying these codes were 14.1% of the session's dollar
    /// volume, so treating them as ordinary would move every VWAP in the archive by double digits.
    #[test]
    fn test_the_official_open_and_close_are_not_consolidated_volume() {
        for identifier in [15, 16, 38] {
            let condition = by_identifier(identifier).expect("a published condition");
            assert!(
                !condition.updates_volume,
                "{} ({}) must stay out of consolidated volume",
                identifier, condition.name
            );
        }
        assert_eq!(volume_eligibility(&[15]), Eligibility::Ineligible);
        assert_eq!(volume_eligibility(&[37, 41, 15]), Eligibility::Ineligible);
    }

    /// One disqualifying code outweighs any number of ordinary ones beside it.
    #[test]
    fn test_ineligibility_wins_over_the_conditions_it_sits_beside() {
        assert_eq!(volume_eligibility(&[]), Eligibility::Eligible);
        assert_eq!(volume_eligibility(&[37]), Eligibility::Eligible);
        assert_eq!(volume_eligibility(&[14, 12, 37]), Eligibility::Eligible);
        assert_eq!(volume_eligibility(&[16, 37]), Eligibility::Ineligible);
    }

    /// A code this table does not carry is reported, never silently treated as ordinary.
    ///
    /// The trades file mixes namespaces — 41 is a trade-through exemption rather than a sale
    /// condition — so an unresolved code is the common case and must stay visible.
    #[test]
    fn test_an_unresolved_code_is_ambiguous_rather_than_eligible() {
        assert_eq!(volume_eligibility(&[9_999]), Eligibility::Ambiguous);
        assert_eq!(volume_eligibility(&[37, 9_999]), Eligibility::Ambiguous);
        // Still ineligible: a code we cannot read does not rescue one we can.
        assert_eq!(volume_eligibility(&[15, 9_999]), Eligibility::Ineligible);
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
    }

    /// The provider's character mapping collides, and the archive depends on it not mattering.
    ///
    /// Three collisions exist — CTA `I`, CTA `K`, UTP `V`. Every one agrees on `updates_volume`,
    /// which is the only rule read off a character. If the provider ever publishes a collision that
    /// disagrees, this fails and the resolution has to stop being character-based.
    #[test]
    fn test_every_colliding_character_agrees_on_the_volume_rule() {
        let mut collisions = 0;
        for tape in Tape::ALL {
            for character in 0u8..=255 {
                let candidates = by_character(character, tape);
                if candidates.len() < 2 {
                    continue;
                }
                collisions += 1;
                let verdicts: std::collections::BTreeSet<bool> =
                    candidates.iter().map(|row| row.updates_volume).collect();
                assert_eq!(
                    verdicts.len(),
                    1,
                    "{tape} {:?} names conditions that disagree on volume: {:?}",
                    character as char,
                    candidates
                );
            }
        }
        assert_eq!(collisions, 3, "CTA I, CTA K and UTP V, and nothing else");
    }

    /// The house rule is read off characters too, so its codes must name nothing else.
    ///
    /// Spelled out rather than iterated over `NOT_A_MARKET_PRICE`: a loop driven by the constant
    /// under test passes vacuously if the constant is emptied, which is the one edit that would
    /// silently retire the rule.
    #[test]
    fn test_the_house_rule_codes_are_collision_free_on_every_tape() {
        for (identifier, name, expected) in [
            (
                2,
                "Average Price Trade",
                vec![
                    (Tape::ConsolidatedTapeAssociation, b'B'),
                    (Tape::UnlistedTradingPrivileges, b'W'),
                    (Tape::TradeDataDissemination, b'W'),
                ],
            ),
            (
                10,
                "Derivatively Priced",
                vec![
                    (Tape::ConsolidatedTapeAssociation, b'4'),
                    (Tape::UnlistedTradingPrivileges, b'4'),
                ],
            ),
        ] {
            assert!(
                NOT_A_MARKET_PRICE.contains(&identifier),
                "{identifier} must still be excluded from the effective spread"
            );
            let condition = by_identifier(identifier).expect("a published condition");
            assert_eq!(condition.name, name);
            for (tape, character) in expected {
                assert_eq!(
                    condition.character_on(tape),
                    Some(character),
                    "{tape} spells {name} {:?}",
                    character as char
                );
                assert_eq!(
                    by_character(character, tape).len(),
                    1,
                    "{tape} {:?} must name only {name}",
                    character as char
                );
            }
        }
        assert_eq!(NOT_A_MARKET_PRICE.len(), 2, "exactly the two above");

        assert!(!carries_a_market_price_from_characters(
            b"B",
            Tape::ConsolidatedTapeAssociation
        ));
        assert!(!carries_a_market_price_from_characters(
            b"W",
            Tape::UnlistedTradingPrivileges
        ));
        assert!(carries_a_market_price_from_characters(
            b"I",
            Tape::ConsolidatedTapeAssociation
        ));
    }

    /// Both providers reach the same verdict on the same trade.
    ///
    /// The point of the whole module: a bulk fold reading Massive identifiers and a nightly fold
    /// reading Alpaca characters must not disagree about whether a print is volume.
    #[test]
    fn test_the_two_provider_spellings_agree_on_eligibility() {
        let tape = Tape::ConsolidatedTapeAssociation;
        for (identifiers, characters) in [
            (vec![37u32], b"I".to_vec()),
            (vec![16], b"Q".to_vec()),
            (vec![15], b"M".to_vec()),
            (vec![14], b"F".to_vec()),
        ] {
            assert_eq!(
                volume_eligibility(&identifiers),
                volume_eligibility_from_characters(&characters, tape),
                "{identifiers:?} and {characters:?} must agree"
            );
        }
    }

    /// The table is present and stamped, which is what makes the const reproducible.
    #[test]
    fn test_the_generated_table_is_populated_and_dated() {
        assert!(
            SALE_CONDITIONS.len() >= 40,
            "the provider published 40 sale conditions when this was written"
        );
        assert_eq!(FETCHED_ON.len(), 10, "an ISO date");
        // Strictly increasing, which is uniqueness and ordering in one pass. `dedup` would not do:
        // it drops only adjacent repeats, so an unsorted table with duplicates apart reads clean.
        assert!(
            SALE_CONDITIONS
                .windows(2)
                .all(|pair| pair[0].identifier < pair[1].identifier),
            "identifiers must ascend without repeating, which `by_identifier` assumes"
        );
    }
}
