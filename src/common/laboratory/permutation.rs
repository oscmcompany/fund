//! A seeded generator and shuffle that never change, so a null drawn from a seed can be drawn again exactly, and the
//! shuffled null that permutes values only among rows sharing a block.

use std::collections::BTreeMap;
use std::num::NonZeroU64;

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
    fn below(&mut self, bound: NonZeroU64) -> u64 {
        let bound = bound.get();
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
            let chosen = self.below(NonZeroU64::MIN.saturating_add(index as u64)) as usize;
            items.swap(index, chosen);
        }
    }
}

/// Each row's value after permuting the values among the rows that share its block, one generator seeded `seed`
/// shuffling the blocks in key order; a value never leaves its block, so whatever the block holds in common survives
/// and only the pairing within it is broken.
pub fn shuffled_within<Key: Ord, Value: Clone>(rows: &[(Key, Value)], seed: u64) -> Vec<Value> {
    let mut blocks: BTreeMap<&Key, Vec<usize>> = BTreeMap::new();
    for (index, (key, _)) in rows.iter().enumerate() {
        blocks.entry(key).or_default().push(index);
    }
    let mut generator = Generator::new(seed);
    let mut shuffled: Vec<Value> = rows.iter().map(|(_, value)| value.clone()).collect();
    for indices in blocks.values() {
        let mut sources = indices.clone();
        generator.shuffle(&mut sources);
        for (target, source) in indices.iter().zip(sources) {
            shuffled[*target] = rows[source].1.clone();
        }
    }
    shuffled
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

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

    #[test]
    fn test_a_shuffle_within_blocks_keeps_each_value_in_its_block() {
        let rows = [
            ("a", 1),
            ("b", 10),
            ("a", 2),
            ("a", 3),
            ("c", 100),
            ("b", 20),
        ];
        let shuffled = shuffled_within(&rows, 3);
        assert_eq!(shuffled, shuffled_within(&rows, 3));
        assert_eq!(shuffled[4], 100);
        let mut a: Vec<i32> = [0, 2, 3].map(|index| shuffled[index]).to_vec();
        a.sort_unstable();
        assert_eq!(a, [1, 2, 3]);
        let differs = (0..20).any(|seed| shuffled_within(&rows, seed) != shuffled);
        assert!(differs);
    }

    /// Outcomes planted at correlation about 0.8 with the forecast in each of 20 blocks; shuffling the forecasts
    /// within blocks leaves a correlation within four standard errors of zero.
    #[test]
    fn test_the_shuffled_null_destroys_a_planted_signal() {
        let mut generator = Generator::new(41);
        let mut uniform = || (generator.next_u64() >> 11) as f64 / (1u64 << 53) as f64 - 0.5;
        let rows: Vec<(u32, f64, f64)> = (0..4_000)
            .map(|index| {
                let forecast = uniform();
                (index % 20, forecast, forecast + 0.8 * uniform())
            })
            .collect();
        let correlation = |forecasts: &[f64]| {
            let count = forecasts.len() as f64;
            let outcomes: Vec<f64> = rows.iter().map(|row| row.2).collect();
            let mean = |values: &[f64]| values.iter().sum::<f64>() / count;
            let (forecast_mean, outcome_mean) = (mean(forecasts), mean(&outcomes));
            let covariance: f64 = forecasts
                .iter()
                .zip(&outcomes)
                .map(|(forecast, outcome)| (forecast - forecast_mean) * (outcome - outcome_mean))
                .sum();
            let spread = |values: &[f64], center: f64| {
                values
                    .iter()
                    .map(|value| (value - center).powi(2))
                    .sum::<f64>()
                    .sqrt()
            };
            covariance / (spread(forecasts, forecast_mean) * spread(&outcomes, outcome_mean))
        };
        let forecasts: Vec<f64> = rows.iter().map(|row| row.1).collect();
        let planted = correlation(&forecasts);
        assert!(planted > 0.5, "{planted}");
        let keyed: Vec<(u32, f64)> = rows.iter().map(|row| (row.0, row.1)).collect();
        let null = correlation(&shuffled_within(&keyed, 5));
        assert!(null.abs() < 4.0 / 4_000f64.sqrt(), "{null}");
    }

    proptest! {
        /// Each block holds the same values after the shuffle as before it.
        #[test]
        fn test_a_shuffle_within_blocks_permutes_each_block(
            rows in prop::collection::vec((0u8..5, any::<i32>()), 0..60),
            seed in any::<u64>(),
        ) {
            let shuffled = shuffled_within(&rows, seed);
            prop_assert_eq!(shuffled.len(), rows.len());
            for block in 0u8..5 {
                let values = |values: &mut dyn Iterator<Item = i32>| {
                    let mut values: Vec<i32> = values.collect();
                    values.sort_unstable();
                    values
                };
                let before = values(&mut rows.iter().filter(|row| row.0 == block).map(|row| row.1));
                let after = values(&mut rows.iter().zip(&shuffled).filter(|(row, _)| row.0 == block).map(|(_, value)| *value));
                prop_assert_eq!(before, after);
            }
        }
    }
}
