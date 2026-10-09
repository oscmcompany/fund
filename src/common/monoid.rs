//! Aggregates as monoids: an empty value and an associative combine, so fragments merge in any grouping.

use std::collections::BTreeMap;
use std::num::NonZeroU64;

/// An aggregate whose `combine` is associative and for which `empty` changes nothing it is combined with.
pub trait Monoid: Sized {
    fn empty() -> Self;

    fn combine(self, other: Self) -> Self;
}

/// An associative combine with no empty value, such as one bar's sums, which exist only once something was seen.
pub trait Semigroup: Sized {
    fn combine(self, other: Self) -> Self;
}

/// How many times each key was seen; a key never seen is absent, so no count is zero.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(transparent, bound(deserialize = "Key: Ord + serde::Deserialize<'de>"))]
pub struct Tally<Key: Ord>(BTreeMap<Key, NonZeroU64>);

impl<Key: Ord> Tally<Key> {
    /// One sighting of `key`.
    pub fn of(key: Key) -> Self {
        Self(BTreeMap::from([(key, NonZeroU64::MIN)]))
    }

    /// Counts one more sighting of `key`, as combining with `Tally::of(key)` would.
    pub fn add(&mut self, key: Key) {
        self.0
            .entry(key)
            .and_modify(|count| {
                *count = count
                    .checked_add(1)
                    .expect("a tally counts fewer than u64::MAX sightings");
            })
            .or_insert(NonZeroU64::MIN);
    }

    pub fn counts(&self) -> &BTreeMap<Key, NonZeroU64> {
        &self.0
    }

    pub fn total(&self) -> u64 {
        self.0.values().map(|count| count.get()).sum()
    }
}

impl<Key: Ord> Monoid for Tally<Key> {
    fn empty() -> Self {
        Self(BTreeMap::new())
    }

    fn combine(mut self, other: Self) -> Self {
        for (key, count) in other.0 {
            let summed = match self.0.remove(&key) {
                Some(held) => held
                    .checked_add(count.get())
                    .expect("a tally counts fewer than u64::MAX sightings"),
                None => count,
            };
            self.0.insert(key, summed);
        }
        self
    }
}

impl<Key: Ord> Default for Tally<Key> {
    fn default() -> Self {
        Self::empty()
    }
}

/// `key=count` pairs in key order, comma separated, for a log line.
impl<Key: Ord + std::fmt::Display> std::fmt::Display for Tally<Key> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for (index, (key, count)) in self.0.iter().enumerate() {
            let separator = if index == 0 { "" } else { ", " };
            write!(formatter, "{separator}{key}={count}")?;
        }
        Ok(())
    }
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

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    /// Built from counts directly rather than through `combine`, so the law test does not lean on what it checks.
    fn any_tally() -> impl Strategy<Value = Tally<u8>> {
        prop::collection::btree_map(0_u8..6, 1_u64..1_000, 0..6).prop_map(|counts| {
            Tally(
                counts
                    .into_iter()
                    .map(|(key, count)| (key, NonZeroU64::new(count).unwrap()))
                    .collect(),
            )
        })
    }

    #[test]
    fn test_a_tally_counts_each_key_and_shows_them_in_order() {
        let tally = concatenate(["price", "duplicate", "price"].map(Tally::of));
        assert_eq!(tally.to_string(), "duplicate=1, price=2");
        assert_eq!(tally.total(), 3);
        assert_eq!(
            serde_json::to_string(&tally).unwrap(),
            r#"{"duplicate":1,"price":2}"#
        );
        assert_eq!(Tally::<&str>::empty().to_string(), "");
    }

    /// A zero count would make a tally unequal to the one that never saw the key, so it does not read.
    #[test]
    fn test_a_tally_refuses_a_zero_count() {
        assert!(serde_json::from_str::<Tally<String>>(r#"{"price":0}"#).is_err());
    }

    proptest! {
        #[test]
        fn property_a_tally_is_a_commutative_monoid(
            first in any_tally(),
            second in any_tally(),
            third in any_tally(),
        ) {
            laws::check(first, second, third)?;
        }

        /// Adding in place agrees with combining with a one-sighting tally, so `add` stays inside the monoid.
        #[test]
        fn property_adding_a_key_equals_combining_with_its_tally(tally in any_tally(), key in 0_u8..8) {
            let mut added = tally.clone();
            added.add(key);
            prop_assert_eq!(added, tally.combine(Tally::of(key)));
        }

        #[test]
        fn property_a_tally_reads_back_as_itself(tally in any_tally()) {
            let text = serde_json::to_string(&tally).unwrap();
            prop_assert_eq!(serde_json::from_str::<Tally<u8>>(&text).unwrap(), tally);
        }

        /// Counting in any order gives the same tally, and its total is the number of sightings.
        #[test]
        fn property_a_tally_totals_its_sightings(keys in prop::collection::vec(0_u8..6, 0..20)) {
            let mut reversed = keys.clone();
            reversed.reverse();
            let tally = concatenate(keys.iter().copied().map(Tally::of));
            prop_assert_eq!(tally.total(), keys.len() as u64);
            laws::check_any_order(keys.into_iter().map(Tally::of).collect(), reversed.into_iter().map(Tally::of).collect())?;
        }
    }
}
