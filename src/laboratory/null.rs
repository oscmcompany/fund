//! The permutation null: one seeded shuffle that every study's control draws from.
//!
//! A shuffle keeps both marginals and breaks the association, so what survives it is the instrument's own bias.

use rand::{rngs::StdRng, seq::SliceRandom, SeedableRng};

/// A reproducible shuffle, fixed by its seed so a null can be drawn again exactly.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Permutation {
    seed: u64,
}

impl Permutation {
    pub const fn new(seed: u64) -> Self {
        Self { seed }
    }

    pub fn seed(self) -> u64 {
        self.seed
    }

    /// Shuffles `items` once, the same way on every call.
    pub fn shuffle<T>(self, items: &mut [T]) {
        items.shuffle(&mut StdRng::seed_from_u64(self.seed));
    }

    /// Shuffles `items` for one stream, such as a session, so each stream draws its own order.
    pub fn shuffle_stream<T>(self, items: &mut [T], stream: u64) {
        items.shuffle(&mut StdRng::seed_from_u64(mix(self.seed, stream)));
    }
}

/// Stirs two values into one seed.
///
/// The splitmix64 finalizer, so a run and a stream combine without colliding the way an exclusive
/// or does: seed 7 at stream 1 would otherwise draw the same order as seed 6 at stream 0.
pub(crate) fn mix(seed: u64, stream: u64) -> u64 {
    let mut value = seed
        .wrapping_mul(0x9E37_79B9_7F4A_7C15)
        .wrapping_add(stream);
    value = (value ^ (value >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    value ^ (value >> 31)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_a_permutation_is_reproducible_and_keeps_every_item() {
        let original: Vec<u32> = (0..50).collect();
        let (mut first, mut second) = (original.clone(), original.clone());
        Permutation::new(0x5EED).shuffle(&mut first);
        Permutation::new(0x5EED).shuffle(&mut second);
        assert_eq!(first, second);
        assert_ne!(first, original);
        let mut sorted = first.clone();
        sorted.sort_unstable();
        assert_eq!(sorted, original);
    }

    /// The exclusive-or this replaces drew one order for seed 7 at stream 1 and seed 6 at stream 0.
    #[test]
    fn test_streams_do_not_collide_across_seeds() {
        let original: Vec<u32> = (0..50).collect();
        let (mut left, mut right) = (original.clone(), original.clone());
        Permutation::new(7).shuffle_stream(&mut left, 1);
        Permutation::new(6).shuffle_stream(&mut right, 0);
        assert_ne!(left, right);
        assert_ne!(mix(7, 1), mix(6, 0));
    }
}
