//! Aggregates as monoids: an empty value and an associative combine, so fragments merge in any grouping.

/// An aggregate whose `combine` is associative and for which `empty` changes nothing it is combined with.
pub trait Monoid: Sized {
    fn empty() -> Self;

    fn combine(self, other: Self) -> Self;
}

/// Folds any number of fragments, returning `empty` for none.
pub fn concatenate<M: Monoid>(values: impl IntoIterator<Item = M>) -> M {
    values.into_iter().fold(M::empty(), M::combine)
}

#[cfg(test)]
pub(crate) mod laws {
    use std::fmt::Debug;

    use proptest::prelude::*;

    use super::{Monoid, concatenate};

    /// Identity on both sides, associativity and commutativity, which every aggregate here claims.
    pub(crate) fn check<M: Monoid + Clone + PartialEq + Debug>(
        first: M,
        second: M,
        third: M,
    ) -> Result<(), TestCaseError> {
        check_ordered(first.clone(), second.clone(), third)?;
        prop_assert_eq!(first.clone().combine(second.clone()), second.combine(first));
        Ok(())
    }

    /// Identity on both sides and associativity, which a concatenation claims without commuting.
    pub(crate) fn check_ordered<M: Monoid + Clone + PartialEq + Debug>(
        first: M,
        second: M,
        third: M,
    ) -> Result<(), TestCaseError> {
        prop_assert_eq!(M::empty().combine(first.clone()), first.clone());
        prop_assert_eq!(first.clone().combine(M::empty()), first.clone());
        prop_assert_eq!(
            first.clone().combine(second.clone()).combine(third.clone()),
            first.combine(second.combine(third))
        );
        Ok(())
    }

    /// A shuffled list of fragments concatenates to the same aggregate as the original order.
    pub(crate) fn check_any_order<M: Monoid + Clone + PartialEq + Debug>(
        ordered: Vec<M>,
        shuffled: Vec<M>,
    ) -> Result<(), TestCaseError> {
        prop_assert_eq!(concatenate(ordered), concatenate(shuffled));
        Ok(())
    }
}
