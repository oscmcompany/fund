//! Constraints on a target, each a set of allowed targets. Combining two intersects them and "anything goes" changes
//! nothing; the combine is associative, commutative and idempotent, so constraints fold from any source in any order.

use std::cmp::Ordering;

use crate::common::book::ValuationRefusal;
use crate::common::market::{DollarVolume, Price, Symbol};
use crate::common::monoid::Monoid;
use crate::common::risk::DollarLimit;
use crate::common::strategy::Target;

/// One form a solver can read; every form admits the empty target, so no intersection of them excludes flat.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Constraint {
    /// Nothing held.
    Flat,
    /// Each holding worth at most the limit.
    PerName(DollarLimit),
    /// The holdings together worth at most the limit.
    Gross(DollarLimit),
}

/// The intersection of constraints, each form kept at its tightest.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Constraints {
    flat: bool,
    per_name: Bound,
    gross: Bound,
}

/// A dollar ceiling, or none.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Bound {
    Unbounded,
    AtMost(DollarLimit),
}

impl Bound {
    fn tighter(self, other: Self) -> Self {
        match (self, other) {
            (Self::Unbounded, bound) | (bound, Self::Unbounded) => bound,
            (Self::AtMost(first), Self::AtMost(second)) => {
                match first.units().cmp(&second.units()) {
                    Ordering::Less | Ordering::Equal => Self::AtMost(first),
                    Ordering::Greater => Self::AtMost(second),
                }
            }
        }
    }

    fn admits(self, worth: DollarVolume) -> bool {
        match self {
            Self::Unbounded => true,
            Self::AtMost(limit) => worth.units() <= limit.units(),
        }
    }
}

impl From<Constraint> for Constraints {
    fn from(constraint: Constraint) -> Self {
        let anything = Self::empty();
        match constraint {
            Constraint::Flat => Self {
                flat: true,
                ..anything
            },
            Constraint::PerName(limit) => Self {
                per_name: Bound::AtMost(limit),
                ..anything
            },
            Constraint::Gross(limit) => Self {
                gross: Bound::AtMost(limit),
                ..anything
            },
        }
    }
}

impl Monoid for Constraints {
    /// Anything goes.
    fn empty() -> Self {
        Self {
            flat: false,
            per_name: Bound::Unbounded,
            gross: Bound::Unbounded,
        }
    }

    /// The targets both admit.
    fn combine(self, other: Self) -> Self {
        Self {
            flat: self.flat || other.flat,
            per_name: self.per_name.tighter(other.per_name),
            gross: self.gross.tighter(other.gross),
        }
    }
}

impl Constraints {
    /// Whether `target` is allowed, refused when a dollar bound must price a holding that has no price.
    pub fn admits(
        &self,
        target: &Target,
        price: impl Fn(&Symbol) -> Option<Price>,
    ) -> Result<bool, ValuationRefusal> {
        if self.flat {
            return Ok(target.holdings().is_empty());
        }
        if (self.per_name, self.gross) == (Bound::Unbounded, Bound::Unbounded) {
            return Ok(true);
        }
        let mut gross = DollarVolume::default();
        let mut each_admitted = true;
        for (symbol, shares) in target.holdings() {
            let price = price(symbol).ok_or_else(|| ValuationRefusal::Unpriced {
                symbol: symbol.clone(),
            })?;
            let worth = DollarVolume::of(price, *shares);
            each_admitted &= self.per_name.admits(worth);
            gross = gross.plus(worth);
        }
        Ok(each_admitted && self.gross.admits(gross))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use proptest::prelude::*;

    use super::*;
    use crate::common::book::Cash;
    use crate::common::market::Shares;
    use crate::common::monoid::{concatenate, laws};

    const DOLLAR: i128 = 1_000_000_000_000;

    fn symbol(raw: &str) -> Symbol {
        Symbol::new(raw).unwrap()
    }

    fn limit(dollars: i128) -> DollarLimit {
        DollarLimit::new(Cash::from_units(dollars * DOLLAR)).unwrap()
    }

    fn target(wanted: &[(&str, u64)]) -> Target {
        Target::new(
            wanted
                .iter()
                .map(|(raw, units)| (symbol(raw), Shares::from_units(*units)))
                .collect(),
        )
    }

    /// Every symbol at $10.
    fn ten_dollars(_: &Symbol) -> Option<Price> {
        Some(Price::from_ticks(10_000_000).unwrap())
    }

    fn arbitrary_bound() -> impl Strategy<Value = Bound> {
        prop_oneof![
            Just(Bound::Unbounded),
            (1_i128..=50).prop_map(|dollars| Bound::AtMost(limit(dollars))),
        ]
    }

    /// Built field by field rather than by `combine`, so a broken combine cannot also break the inputs.
    fn arbitrary_constraints() -> impl Strategy<Value = Constraints> {
        (any::<bool>(), arbitrary_bound(), arbitrary_bound()).prop_map(|(flat, per_name, gross)| {
            Constraints {
                flat,
                per_name,
                gross,
            }
        })
    }

    /// Holdings in three names of up to five shares each, the bounds above reaching both sides of every limit.
    fn arbitrary_target() -> impl Strategy<Value = Target> {
        prop::collection::btree_map(
            prop::sample::select(vec!["A", "B", "C"]),
            0_u64..=5_000_000,
            0..=3,
        )
        .prop_map(|holdings: BTreeMap<&str, u64>| {
            Target::new(
                holdings
                    .into_iter()
                    .map(|(raw, units)| (symbol(raw), Shares::from_units(units)))
                    .collect(),
            )
        })
    }

    #[test]
    fn test_per_name_admits_up_to_its_limit_in_each_name() {
        let constraints = Constraints::from(Constraint::PerName(limit(20)));
        let at = target(&[("A", 2_000_000), ("B", 2_000_000)]);
        let over = target(&[("A", 2_000_000), ("B", 2_000_001)]);
        assert_eq!(constraints.admits(&at, ten_dollars), Ok(true));
        assert_eq!(constraints.admits(&over, ten_dollars), Ok(false));
    }

    #[test]
    fn test_gross_admits_up_to_its_limit_across_names() {
        let constraints = Constraints::from(Constraint::Gross(limit(40)));
        let at = target(&[("A", 2_000_000), ("B", 2_000_000)]);
        let over = target(&[("A", 2_000_000), ("B", 2_000_001)]);
        assert_eq!(constraints.admits(&at, ten_dollars), Ok(true));
        assert_eq!(constraints.admits(&over, ten_dollars), Ok(false));
    }

    #[test]
    fn test_flat_admits_only_the_empty_target() {
        let constraints = Constraints::from(Constraint::Flat);
        assert_eq!(constraints.admits(&Target::default(), |_| None), Ok(true));
        assert_eq!(
            constraints.admits(&target(&[("A", 1)]), ten_dollars),
            Ok(false)
        );
    }

    #[test]
    fn test_combining_with_flat_admits_only_the_empty_target() {
        let constraints = concatenate([
            Constraints::from(Constraint::PerName(limit(20))),
            Constraints::from(Constraint::Flat),
        ]);
        assert_eq!(
            constraints.admits(&target(&[("A", 1)]), ten_dollars),
            Ok(false)
        );
    }

    #[test]
    fn test_combining_keeps_the_tighter_of_two_limits() {
        let constraints = concatenate([
            Constraints::from(Constraint::Gross(limit(40))),
            Constraints::empty(),
            Constraints::from(Constraint::Gross(limit(30))),
        ]);
        let between = target(&[("A", 3_500_000)]);
        assert_eq!(constraints.admits(&between, ten_dollars), Ok(false));
    }

    #[test]
    fn test_a_dollar_bound_refuses_an_unpriced_holding() {
        let constraints = Constraints::from(Constraint::Gross(limit(40)));
        assert_eq!(
            constraints.admits(&target(&[("A", 1)]), |_| None),
            Err(ValuationRefusal::Unpriced {
                symbol: symbol("A")
            })
        );
        assert_eq!(
            Constraints::empty().admits(&target(&[("A", 1)]), |_| None),
            Ok(true)
        );
    }

    proptest! {
        #[test]
        fn property_constraints_are_a_commutative_monoid(
            first in arbitrary_constraints(),
            second in arbitrary_constraints(),
            third in arbitrary_constraints(),
        ) {
            laws::check(first, second, third)?;
        }

        #[test]
        fn property_combining_is_idempotent(constraints in arbitrary_constraints()) {
            prop_assert_eq!(constraints.combine(constraints), constraints);
        }

        #[test]
        fn property_combining_admits_exactly_what_both_admit(
            first in arbitrary_constraints(),
            second in arbitrary_constraints(),
            target in arbitrary_target(),
        ) {
            let both = first.combine(second).admits(&target, ten_dollars)?;
            let each = first.admits(&target, ten_dollars)? && second.admits(&target, ten_dollars)?;
            prop_assert_eq!(both, each);
        }

        #[test]
        fn property_adding_a_constraint_never_admits_a_refused_target(
            first in arbitrary_constraints(),
            second in arbitrary_constraints(),
            target in arbitrary_target(),
        ) {
            let smaller = first.admits(&target, ten_dollars)?;
            let larger = first.combine(second).admits(&target, ten_dollars)?;
            prop_assert!(smaller || !larger);
        }

        #[test]
        fn property_every_constraint_admits_flat(constraints in arbitrary_constraints()) {
            prop_assert_eq!(constraints.admits(&Target::default(), |_| None), Ok(true));
        }
    }
}
