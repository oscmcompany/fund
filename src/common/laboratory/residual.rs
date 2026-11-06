//! Returns net of the market and the industry: each name's return less the capitalization-weighted return of the other
//! names in its industry over the same interval, which removes the market's move along with the industry's.

use std::collections::BTreeMap;
use std::num::NonZeroUsize;

use super::READING_BOUND;
use crate::common::market::industry::Industry;
use crate::common::market::{Dollars, Symbol};

/// One name over one interval: its industry, its capitalization and its simple return.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Member {
    industry: Option<Industry>,
    capitalization: Option<Dollars>,
    simple_return: f64,
}

/// Why a member was refused.
#[derive(Debug, Clone, Copy, PartialEq, thiserror::Error)]
pub enum MemberRefusal {
    #[error("{simple_return} is not a finite return within the reading bound")]
    Unbounded { simple_return: f64 },
}

impl Member {
    pub fn new(
        industry: Option<Industry>,
        capitalization: Option<Dollars>,
        simple_return: f64,
    ) -> Result<Self, MemberRefusal> {
        match (-READING_BOUND..=READING_BOUND).contains(&simple_return) {
            true => Ok(Self {
                industry,
                capitalization,
                simple_return,
            }),
            false => Err(MemberRefusal::Unbounded { simple_return }),
        }
    }
}

/// Why a name has no residual.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum ResidualRefusal {
    #[error("the name has no industry")]
    NoIndustry,
    #[error(
        "{others} other names in the industry carry a capitalization, below the {minimum} required"
    )]
    ThinIndustry {
        others: usize,
        minimum: NonZeroUsize,
    },
}

/// The weights and weighted returns of one industry's capitalized names.
#[derive(Debug, Clone, Copy, Default)]
struct Totals {
    names: usize,
    weight: u128,
    weighted_return: f64,
}

/// Every member's residual, keyed as the members are.
/// Only a name with a positive capitalization counts toward an industry's average, but every name with an industry
/// is read against it.
pub fn residuals(
    members: &BTreeMap<Symbol, Member>,
    minimum_others: NonZeroUsize,
) -> BTreeMap<Symbol, Result<f64, ResidualRefusal>> {
    let weight = |member: &Member| {
        member
            .capitalization
            .map_or(0, |capitalization| u128::from(capitalization.millionths()))
    };
    let mut totals: BTreeMap<Industry, Totals> = BTreeMap::new();
    for member in members.values() {
        match (member.industry, weight(member)) {
            (Some(industry), weight) if weight > 0 => {
                let entry = totals.entry(industry).or_default();
                entry.names += 1;
                entry.weight += weight;
                entry.weighted_return += weight as f64 * member.simple_return;
            }
            (Some(_) | None, _) => {}
        }
    }
    members
        .iter()
        .map(|(symbol, member)| {
            let residual = match member.industry {
                None => Err(ResidualRefusal::NoIndustry),
                Some(industry) => {
                    let all = totals.get(&industry).copied().unwrap_or_default();
                    let own = weight(member);
                    let others = all.names - usize::from(own > 0);
                    match others >= minimum_others.get() {
                        true => {
                            let average = (all.weighted_return - own as f64 * member.simple_return)
                                / (all.weight - own) as f64;
                            Ok(member.simple_return - average)
                        }
                        false => Err(ResidualRefusal::ThinIndustry {
                            others,
                            minimum: minimum_others,
                        }),
                    }
                }
            };
            (symbol.clone(), residual)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;
    use strum::IntoEnumIterator;

    use super::*;

    const TOLERANCE: f64 = 1e-9;

    fn symbol(text: &str) -> Symbol {
        Symbol::new(text).unwrap()
    }

    fn member(industry: Option<Industry>, dollars: Option<u64>, simple_return: f64) -> Member {
        Member::new(
            industry,
            dollars.map(|dollars| Dollars::from_millionths(dollars * 1_000_000)),
            simple_return,
        )
        .unwrap()
    }

    fn one() -> NonZeroUsize {
        NonZeroUsize::new(1).unwrap()
    }

    fn measured(result: &Result<f64, ResidualRefusal>) -> f64 {
        *result.as_ref().unwrap()
    }

    #[test]
    fn a_pair_reads_each_against_the_other() {
        let members = BTreeMap::from([
            (
                symbol("AAA"),
                member(Some(Industry::Finance), Some(1), 0.03),
            ),
            (
                symbol("BBB"),
                member(Some(Industry::Finance), Some(9), 0.01),
            ),
        ]);
        let residuals = residuals(&members, one());
        assert!((measured(&residuals[&symbol("AAA")]) - 0.02).abs() < TOLERANCE);
        assert!((measured(&residuals[&symbol("BBB")]) + 0.02).abs() < TOLERANCE);
    }

    #[test]
    fn the_average_weights_by_capitalization_and_leaves_the_name_out() {
        let members = BTreeMap::from([
            (symbol("AAA"), member(Some(Industry::Energy), Some(1), 0.10)),
            (symbol("BBB"), member(Some(Industry::Energy), Some(3), 0.02)),
            (symbol("CCC"), member(Some(Industry::Energy), Some(1), 0.06)),
            (
                symbol("DDD"),
                member(Some(Industry::Utilities), Some(1), 0.50),
            ),
            (
                symbol("EEE"),
                member(Some(Industry::Utilities), Some(1), 0.50),
            ),
        ]);
        let residuals = residuals(&members, one());
        assert!((measured(&residuals[&symbol("AAA")]) - 0.07).abs() < TOLERANCE);
        assert!((measured(&residuals[&symbol("BBB")]) + 0.06).abs() < TOLERANCE);
        assert!(measured(&residuals[&symbol("DDD")]).abs() < TOLERANCE);
    }

    #[test]
    fn an_uncapitalized_name_is_read_but_moves_no_average() {
        let members = BTreeMap::from([
            (
                symbol("AAA"),
                member(Some(Industry::Finance), Some(1), 0.01),
            ),
            (
                symbol("BBB"),
                member(Some(Industry::Finance), Some(1), 0.03),
            ),
            (symbol("CCC"), member(Some(Industry::Finance), None, 0.50)),
            (
                symbol("DDD"),
                member(Some(Industry::Finance), Some(0), 0.90),
            ),
        ]);
        let residuals = residuals(&members, one());
        assert!((measured(&residuals[&symbol("AAA")]) + 0.02).abs() < TOLERANCE);
        assert!((measured(&residuals[&symbol("CCC")]) - 0.48).abs() < TOLERANCE);
        assert!((measured(&residuals[&symbol("DDD")]) - 0.88).abs() < TOLERANCE);
    }

    #[test]
    fn refusals_carry_their_cause_and_count() {
        let members = BTreeMap::from([
            (symbol("AAA"), member(None, Some(1), 0.01)),
            (
                symbol("BBB"),
                member(Some(Industry::Finance), Some(1), 0.01),
            ),
            (
                symbol("CCC"),
                member(Some(Industry::Finance), Some(1), 0.02),
            ),
            (symbol("DDD"), member(Some(Industry::Finance), None, 0.03)),
            (symbol("EEE"), member(Some(Industry::Energy), None, 0.03)),
        ]);
        let two = NonZeroUsize::new(2).unwrap();
        let residuals = residuals(&members, two);
        assert_eq!(
            residuals.keys().cloned().collect::<Vec<_>>(),
            ["AAA", "BBB", "CCC", "DDD", "EEE"].map(symbol).to_vec()
        );
        assert_eq!(residuals[&symbol("AAA")], Err(ResidualRefusal::NoIndustry));
        assert_eq!(
            residuals[&symbol("BBB")],
            Err(ResidualRefusal::ThinIndustry {
                others: 1,
                minimum: two
            })
        );
        assert!((measured(&residuals[&symbol("DDD")]) - 0.015).abs() < TOLERANCE);
        assert_eq!(
            residuals[&symbol("EEE")],
            Err(ResidualRefusal::ThinIndustry {
                others: 0,
                minimum: two
            })
        );
    }

    #[test]
    fn a_return_past_the_bound_is_refused() {
        assert!(Member::new(None, None, f64::NAN).is_err());
        assert!(Member::new(None, None, f64::INFINITY).is_err());
        assert_eq!(
            Member::new(None, None, 1e51),
            Err(MemberRefusal::Unbounded {
                simple_return: 1e51
            })
        );
        assert!(Member::new(None, None, -1e50).is_ok());
    }

    fn cross_section() -> impl Strategy<Value = BTreeMap<Symbol, Member>> {
        let industries: Vec<Industry> = Industry::iter().collect();
        proptest::collection::vec(
            (
                proptest::sample::select(industries),
                proptest::option::weighted(0.9, 1_u64..1_000_000),
                -0.2_f64..0.2,
            ),
            1..40,
        )
        .prop_map(|rows| {
            rows.into_iter()
                .enumerate()
                .map(|(index, (industry, dollars, simple_return))| {
                    (
                        symbol(&format!(
                            "A{}{}",
                            char::from(b'A' + (index / 26) as u8),
                            char::from(b'A' + (index % 26) as u8)
                        )),
                        member(Some(industry), dollars, simple_return),
                    )
                })
                .collect()
        })
    }

    fn agree(
        left: &BTreeMap<Symbol, Result<f64, ResidualRefusal>>,
        right: &BTreeMap<Symbol, Result<f64, ResidualRefusal>>,
    ) -> bool {
        left.keys().eq(right.keys())
            && left.values().zip(right.values()).all(|pair| match pair {
                (Ok(left), Ok(right)) => (left - right).abs() < TOLERANCE,
                (Err(left), Err(right)) => left == right,
                (Ok(_), Err(_)) | (Err(_), Ok(_)) => false,
            })
    }

    fn shifted(
        members: &BTreeMap<Symbol, Member>,
        shift: impl Fn(&Symbol, &Member) -> f64,
    ) -> BTreeMap<Symbol, Member> {
        members
            .iter()
            .map(|(symbol, member)| {
                (
                    symbol.clone(),
                    Member {
                        simple_return: member.simple_return + shift(symbol, member),
                        ..*member
                    },
                )
            })
            .collect()
    }

    proptest! {
        #[test]
        fn a_market_move_leaves_every_residual_unchanged(
            members in cross_section(),
            move_size in -0.1_f64..0.1,
        ) {
            let before = residuals(&members, one());
            let after = residuals(&shifted(&members, |_, _| move_size), one());
            prop_assert_eq!(before.keys().collect::<Vec<_>>(), members.keys().collect::<Vec<_>>());
            prop_assert!(agree(&before, &after));
        }

        #[test]
        fn an_industry_move_leaves_every_residual_unchanged(
            members in cross_section(),
            moved in proptest::sample::select(Industry::iter().collect::<Vec<_>>()),
            move_size in -0.1_f64..0.1,
        ) {
            let before = residuals(&members, one());
            let after = residuals(
                &shifted(&members, |_, member| match member.industry == Some(moved) {
                    true => move_size,
                    false => 0.0,
                }),
                one(),
            );
            prop_assert!(agree(&before, &after));
        }

        #[test]
        fn a_name_of_its_own_moves_its_residual_by_the_same_amount(
            members in cross_section(),
            shock in 0.01_f64..0.1,
        ) {
            let target = members.keys().next().unwrap().clone();
            let before = residuals(&members, one());
            let after = residuals(
                &shifted(&members, |symbol, _| match *symbol == target {
                    true => shock,
                    false => 0.0,
                }),
                one(),
            );
            match (&before[&target], &after[&target]) {
                (Ok(before), Ok(after)) => prop_assert!((after - before - shock).abs() < TOLERANCE),
                (Err(before), Err(after)) => prop_assert_eq!(before, after),
                (Ok(_), Err(_)) | (Err(_), Ok(_)) => prop_assert!(false),
            }
        }
    }
}
