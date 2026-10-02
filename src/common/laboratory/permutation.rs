//! A seeded generator and shuffle that never change, so a null drawn from a seed can be drawn again exactly.

/// SplitMix64: one 64-bit state, advanced by a fixed odd constant and finalized; the sequence for a seed is part of
/// the record, so neither the constants nor the finalizer may change.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Generator {
    state: u64,
}

impl Generator {
    pub fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    pub fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut value = self.state;
        value = (value ^ (value >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        value = (value ^ (value >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        value ^ (value >> 31)
    }

    /// A uniform draw from `0..bound`, rejecting the low remainder so no value is favored.
    pub fn below(&mut self, bound: u64) -> u64 {
        assert!(bound > 0, "a draw needs at least one value");
        let threshold = bound.wrapping_neg() % bound;
        loop {
            let value = self.next_u64();
            if value >= threshold {
                return value % bound;
            }
        }
    }

    pub fn coin(&mut self) -> bool {
        self.next_u64() >> 63 == 1
    }

    /// Fisher–Yates, from the back.
    pub fn shuffle<T>(&mut self, items: &mut [T]) {
        for index in (1..items.len()).rev() {
            let chosen = self.below(index as u64 + 1) as usize;
            items.swap(index, chosen);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The published SplitMix64 outputs from a zero state.
    #[test]
    fn test_the_sequence_matches_the_reference_and_never_changes() {
        let mut generator = Generator::new(0);
        let drawn: Vec<u64> = (0..4).map(|_| generator.next_u64()).collect();
        assert_eq!(
            drawn,
            [
                0xE220_A839_7B1D_CDAF,
                0x6E78_9E6A_A1B9_65F4,
                0x06C4_5D18_8009_454F,
                0xF88B_B8A8_724C_81EC
            ]
        );
        let mut items: Vec<u32> = (0..10).collect();
        Generator::new(0x5EED).shuffle(&mut items);
        assert_eq!(items, [9, 1, 8, 6, 0, 7, 4, 3, 5, 2]);
    }

    #[test]
    fn test_a_shuffle_is_reproducible_and_keeps_every_item() {
        let original: Vec<u32> = (0..50).collect();
        let (mut first, mut second) = (original.clone(), original.clone());
        Generator::new(7).shuffle(&mut first);
        Generator::new(7).shuffle(&mut second);
        assert_eq!(first, second);
        assert_ne!(first, original);
        let mut sorted = first.clone();
        sorted.sort_unstable();
        assert_eq!(sorted, original);
    }

    /// Every position of a three-item shuffle is equally likely, within four standard errors over 60,000 draws.
    #[test]
    fn test_every_arrangement_is_equally_likely() {
        let mut generator = Generator::new(11);
        let mut counts = std::collections::BTreeMap::new();
        for _ in 0..60_000 {
            let mut items = [0, 1, 2];
            generator.shuffle(&mut items);
            *counts.entry(items).or_insert(0u32) += 1;
        }
        assert_eq!(counts.len(), 6);
        let standard_error = (60_000.0 * (1.0 / 6.0) * (5.0 / 6.0f64)).sqrt();
        for (arrangement, count) in counts {
            assert!(
                (f64::from(count) - 10_000.0).abs() < 4.0 * standard_error,
                "{arrangement:?}: {count}"
            );
        }
        let heads = (0..10_000).filter(|_| generator.coin()).count();
        assert!((heads as f64 - 5_000.0).abs() < 4.0 * 50.0, "{heads}");
    }
}
