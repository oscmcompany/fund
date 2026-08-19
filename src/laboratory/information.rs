//! Bits a feature carries about the target, measured against a shuffled null.
//!
//! Catches non-linear association a rank correlation misses, before any architecture is committed to.

use polars::prelude::*;
use rand::seq::SliceRandom;
use rand::{rngs::StdRng, SeedableRng};
use serde::Serialize;

use crate::models::tide::data::{session_ranks, TARGET_COLUMN};
use crate::models::tide::TideError;

/// Bins each side is cut into. Ten matches the decile language the rest of the laboratory uses.
pub const DEFAULT_BINS: usize = 10;

/// Names a cross-section needs before a hundred-cell table means anything.
pub const MINIMUM_CROSS_SECTION: usize = 100;

/// What one session's cross-section says about a feature.
///
/// Both figures are reported because their difference is the answer and their gap is the reason:
/// `null_bits` is the bias a hundred-cell table carries at this sample size, and it is not small.
#[derive(Debug, Clone, Copy, PartialEq, Serialize)]
pub struct SessionInformation {
    pub bits: f64,
    pub null_bits: f64,
}

impl SessionInformation {
    /// Bits above what an unrelated feature scores on the same cross-section.
    pub fn excess(&self) -> f64 {
        self.bits - self.null_bits
    }
}

/// Cuts values into `bins` roughly equal-count groups, by value rather than by position.
///
/// Equal values always land in one bin, so a constant feature yields a single bin and no
/// information. Splitting ties by sort position would instead manufacture groups whose boundaries
/// are wherever the rows happened to be ordered.
pub fn quantile_bins(values: &[f64], bins: usize) -> Option<Vec<usize>> {
    if bins == 0 || values.is_empty() || !values.iter().all(|value| value.is_finite()) {
        return None;
    }
    let mut sorted = values.to_vec();
    sorted.sort_by(|left, right| left.partial_cmp(right).expect("finite values compare"));

    Some(
        values
            .iter()
            .map(|value| {
                let position = sorted.partition_point(|other| other < value);
                (position * bins / sorted.len()).min(bins - 1)
            })
            .collect(),
    )
}

/// Numbers each distinct value, for a feature whose values name groups rather than order them.
///
/// A sector has no low and high, so cutting it into quantiles would read its alphabetical position
/// as a magnitude.
pub fn category_bins(values: &[&str]) -> Vec<usize> {
    let mut numbering: std::collections::HashMap<&str, usize> = std::collections::HashMap::new();
    values
        .iter()
        .map(|value| {
            let next = numbering.len();
            *numbering.entry(value).or_insert(next)
        })
        .collect()
}

/// Mutual information between two binned variables, in bits.
///
/// Zero when the two are independent, and at most the entropy of the coarser side. The estimate is
/// biased upward at finite sample size, which is what [`measure_session`]'s null exists to remove.
pub fn mutual_information(feature: &[usize], target: &[usize]) -> Option<f64> {
    if feature.len() != target.len() || feature.is_empty() {
        return None;
    }
    let total = feature.len() as f64;

    let mut joint: std::collections::HashMap<(usize, usize), f64> =
        std::collections::HashMap::new();
    let mut feature_counts: std::collections::HashMap<usize, f64> =
        std::collections::HashMap::new();
    let mut target_counts: std::collections::HashMap<usize, f64> = std::collections::HashMap::new();
    for (left, right) in feature.iter().zip(target) {
        *joint.entry((*left, *right)).or_default() += 1.0;
        *feature_counts.entry(*left).or_default() += 1.0;
        *target_counts.entry(*right).or_default() += 1.0;
    }

    let bits = joint
        .into_iter()
        .map(|((left, right), count)| {
            let independent = feature_counts[&left] * target_counts[&right];
            (count / total) * ((count * total) / independent).log2()
        })
        .sum();
    Some(bits)
}

/// Measures one session's cross-section, and the same cross-section with the feature shuffled.
///
/// Shuffling breaks the association while leaving both marginals alone, so what it scores is
/// exactly the bias — reporting the raw figure without subtracting it is how this technique
/// produces confident nonsense.
pub fn measure_session(
    feature: &[f64],
    target: &[f64],
    bins: usize,
    seed: u64,
) -> Option<SessionInformation> {
    if feature.len() != target.len() || feature.len() < MINIMUM_CROSS_SECTION {
        return None;
    }
    let feature_bins = quantile_bins(feature, bins)?;
    let target_bins = quantile_bins(target, bins)?;
    let bits = mutual_information(&feature_bins, &target_bins)?;

    let mut shuffled = feature_bins;
    shuffled.shuffle(&mut StdRng::seed_from_u64(seed));
    let null_bits = mutual_information(&shuffled, &target_bins)?;

    Some(SessionInformation { bits, null_bits })
}

/// One session's cross-section: a feature read before the session, and what it forecasts.
pub type Paired = std::collections::BTreeMap<i64, (Vec<f64>, Vec<f64>)>;

/// Which part of the next session's return is being asked about.
///
/// Mutual information counts any dependence, so a feature can say a great deal about how far a name
/// will move while saying nothing about which way — and only the second is directionally tradeable.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    /// The signed return, which is what a directional book acts on.
    Signed,
    /// Its magnitude, which is where a volatility relationship shows up.
    Magnitude,
    /// Its sign alone, which is the part a directional book can act on and nothing else.
    Direction,
}

/// Pairs each name's feature at one session with its own return in the next.
///
/// Keyed by the session being forecast, and only where that session immediately follows the one the
/// feature was read in — a pair spanning a gap would call a multi-session move a forecast. `binned`
/// receives the feature column already numbered when the feature names groups rather than ordering
/// them.
pub fn pair_with_next_session(
    frame: &DataFrame,
    feature: &str,
    outcome: Outcome,
    binned: impl Fn(&DataFrame) -> Result<Vec<f64>, TideError>,
) -> Result<Paired, TideError> {
    let sorted = frame.sort(
        ["ticker", "timestamp"],
        SortMultipleOptions::default().with_maintain_order(true),
    )?;
    let ranks = session_ranks(&sorted)?;

    let tickers: Vec<&str> = sorted
        .column("ticker")?
        .str()?
        .into_no_null_iter()
        .collect();
    let timestamps: Vec<i64> = sorted
        .column("timestamp")?
        .i64()?
        .into_no_null_iter()
        .collect();
    let features = binned(&sorted)?;
    let targets: Vec<f64> = sorted
        .column(TARGET_COLUMN)?
        .cast(&DataType::Float64)?
        .f64()?
        .into_no_null_iter()
        .map(|value| match outcome {
            Outcome::Signed => value,
            Outcome::Magnitude => value.abs(),
            Outcome::Direction => value.signum(),
        })
        .collect();

    if [
        tickers.len(),
        timestamps.len(),
        features.len(),
        targets.len(),
    ]
    .iter()
    .any(|length| *length != sorted.height())
    {
        return Err(TideError::Data(format!(
            "column `{feature}` or its neighbours hold nulls the pairing cannot align"
        )));
    }

    let mut paired = Paired::new();
    for index in 0..sorted.height().saturating_sub(1) {
        let next = index + 1;
        let follows = tickers[index] == tickers[next]
            && ranks
                .get(&timestamps[index])
                .zip(ranks.get(&timestamps[next]))
                .is_some_and(|(current, following)| *following == current + 1);
        if !follows {
            continue;
        }
        let entry = paired.entry(timestamps[next]).or_default();
        entry.0.push(features[index]);
        entry.1.push(targets[next]);
    }

    Ok(paired)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A cross-section large enough to clear the floor, with the target a stated function of the
    /// feature so the answer is known in advance.
    fn paired(names: usize, map: impl Fn(usize) -> f64) -> (Vec<f64>, Vec<f64>) {
        (
            (0..names).map(|index| index as f64).collect(),
            (0..names).map(map).collect(),
        )
    }

    /// A feature that determines the target carries the full entropy of ten equal bins, which is
    /// log2(10). Nothing can score higher, so this is the scale everything else is read against.
    #[test]
    fn test_a_perfect_association_carries_the_whole_entropy() {
        let (feature, target) = paired(1000, |index| index as f64);
        let measured = measure_session(&feature, &target, DEFAULT_BINS, 1).unwrap();

        assert!(
            (measured.bits - 10.0_f64.log2()).abs() < 1e-9,
            "expected log2(10) bits, got {}",
            measured.bits
        );
        // The shuffled copy of a perfect association is worth the bias and nothing more.
        assert!(measured.null_bits < 0.1, "{measured:?}");
    }

    /// Reversing the target is still a perfect association: mutual information counts shared
    /// structure and has no sign, which is what makes it catch relationships a correlation misses.
    #[test]
    fn test_a_reversed_association_is_worth_as_much_as_a_forward_one() {
        let (feature, forward) = paired(1000, |index| index as f64);
        let reversed: Vec<f64> = forward.iter().map(|value| -value).collect();

        let straight = measure_session(&feature, &forward, DEFAULT_BINS, 1).unwrap();
        let backward = measure_session(&feature, &reversed, DEFAULT_BINS, 1).unwrap();
        assert!((straight.bits - backward.bits).abs() < 1e-9);
    }

    /// A relationship no correlation would see. The target is a fold of the feature, so the two are
    /// close to uncorrelated and yet one determines the other.
    #[test]
    fn test_a_folded_relationship_is_visible_where_correlation_is_not() {
        let names = 1000;
        let feature: Vec<f64> = (0..names).map(|index| index as f64).collect();
        let target: Vec<f64> = feature
            .iter()
            .map(|value| (value - names as f64 / 2.0).abs())
            .collect();

        let measured = measure_session(&feature, &target, DEFAULT_BINS, 1).unwrap();
        assert!(
            measured.excess() > 1.0,
            "a fold carries real information: {measured:?}"
        );

        let correlation = crate::laboratory::metrics::information_coefficient(&feature, &target)
            .expect("both sides vary");
        assert!(
            correlation.abs() < 0.05,
            "and a rank correlation barely sees it: {correlation}"
        );
    }

    /// The bias is the whole reason the null is subtracted, and it is not small: a hundred-cell
    /// table over a thousand names scores real bits on two unrelated variables.
    #[test]
    fn test_an_unrelated_feature_scores_bits_it_has_not_earned() {
        let names = 1000;
        let feature: Vec<f64> = (0..names)
            .map(|index| ((index * 7919) % names) as f64)
            .collect();
        let target: Vec<f64> = (0..names)
            .map(|index| ((index * 104_729) % names) as f64)
            .collect();

        let measured = measure_session(&feature, &target, DEFAULT_BINS, 1).unwrap();
        assert!(
            measured.bits > 0.0,
            "the raw estimate is biased upward: {measured:?}"
        );
        assert!(
            measured.excess().abs() < 0.2,
            "and the null removes almost all of it: {measured:?}"
        );
    }

    /// A constant feature has one bin, not ten. Splitting its ties by sort position would invent
    /// groups whose boundaries are wherever the rows happened to be ordered — the same error as
    /// reading a decile spread off a tie-broken sort.
    #[test]
    fn test_a_constant_feature_yields_one_bin_and_no_information() {
        let feature = vec![0.5_f64; 1000];
        let target: Vec<f64> = (0..1000).map(|index| index as f64).collect();

        let bins = quantile_bins(&feature, DEFAULT_BINS).unwrap();
        assert!(bins.iter().all(|bin| *bin == 0));

        let measured = measure_session(&feature, &target, DEFAULT_BINS, 1).unwrap();
        assert!(measured.bits.abs() < 1e-12, "{measured:?}");
        assert!(measured.excess().abs() < 1e-12, "{measured:?}");
    }

    /// Equal values share a bin even when they straddle what would otherwise be a boundary.
    #[test]
    fn test_ties_are_not_split_across_a_boundary() {
        let values = vec![1.0, 1.0, 1.0, 1.0, 1.0, 2.0, 2.0, 2.0, 2.0, 2.0];
        let bins = quantile_bins(&values, DEFAULT_BINS).unwrap();

        assert_eq!(
            bins[..5]
                .iter()
                .collect::<std::collections::HashSet<_>>()
                .len(),
            1
        );
        assert_eq!(
            bins[5..]
                .iter()
                .collect::<std::collections::HashSet<_>>()
                .len(),
            1
        );
        assert_ne!(bins[0], bins[5]);
    }

    const DAY: i64 = 86_400_000;

    /// Two names over four sessions, with the third session missing for BBB so its pair would span
    /// a gap. Feature and target are distinct columns so a pairing off by one is visible.
    fn gapped_frame() -> DataFrame {
        let rows: Vec<(&str, i64, f64, f64)> = vec![
            ("AAA", 0, 10.0, 0.01),
            ("AAA", DAY, 20.0, 0.02),
            ("AAA", 2 * DAY, 30.0, 0.03),
            ("AAA", 3 * DAY, 40.0, 0.04),
            ("BBB", 0, 50.0, 0.05),
            ("BBB", DAY, 60.0, 0.06),
            // No session two for BBB, so its next row is two sessions on.
            ("BBB", 3 * DAY, 70.0, 0.07),
        ];
        DataFrame::new(vec![
            Column::new(
                "ticker".into(),
                rows.iter().map(|row| row.0).collect::<Vec<_>>(),
            ),
            Column::new(
                "timestamp".into(),
                rows.iter().map(|row| row.1).collect::<Vec<_>>(),
            ),
            Column::new(
                "close_price".into(),
                rows.iter().map(|row| row.2).collect::<Vec<_>>(),
            ),
            Column::new(
                TARGET_COLUMN.into(),
                rows.iter().map(|row| row.3).collect::<Vec<_>>(),
            ),
        ])
        .unwrap()
    }

    fn continuous(column: &'static str) -> impl Fn(&DataFrame) -> Result<Vec<f64>, TideError> {
        move |frame: &DataFrame| {
            Ok(frame
                .column(column)?
                .cast(&DataType::Float64)?
                .f64()?
                .into_no_null_iter()
                .collect())
        }
    }

    /// The feature is read before the session it forecasts, never in it. Pairing a session with
    /// itself would score the model against an outcome it was given as input.
    #[test]
    fn test_a_feature_is_paired_with_the_following_session() {
        let paired = pair_with_next_session(
            &gapped_frame(),
            "close_price",
            Outcome::Signed,
            continuous("close_price"),
        )
        .unwrap();

        // Session one is forecast by session zero: AAA's 10.0 against its own next return, and
        // BBB's 50.0 against its own.
        let (features, targets) = &paired[&DAY];
        assert_eq!(features, &vec![10.0, 50.0]);
        assert_eq!(targets, &vec![0.02, 0.06]);
    }

    /// BBB does not trade in session two, so its session-one row and its session-three row are not
    /// adjacent. Pairing them would call a two-session move a one-session forecast.
    #[test]
    fn test_a_pair_is_not_formed_across_a_session_gap() {
        let paired = pair_with_next_session(
            &gapped_frame(),
            "close_price",
            Outcome::Signed,
            continuous("close_price"),
        )
        .unwrap();

        assert_eq!(paired[&(2 * DAY)].0, vec![20.0], "AAA alone");
        assert_eq!(
            paired[&(3 * DAY)].0,
            vec![30.0],
            "BBB's jump over session two is refused"
        );
    }

    #[test]
    fn test_categories_are_numbered_rather_than_ordered() {
        let bins = category_bins(&["ENERGY", "HEALTH", "ENERGY", "UTILITIES"]);
        assert_eq!(bins[0], bins[2], "one sector is one bin");
        assert_ne!(bins[0], bins[1]);
        assert_eq!(
            bins.iter().collect::<std::collections::HashSet<_>>().len(),
            3
        );
    }

    /// The market's own move is a constant across a session, and the bins are cut by rank, so it
    /// is already absent from every figure here. Measured before this was understood: an outcome
    /// that demeaned the target returned bit-identical numbers, because it could not do otherwise.
    #[test]
    fn test_the_sessions_own_move_is_already_out_of_the_measurement() {
        let (feature, target) = paired(1000, |index| ((index * 37) % 1000) as f64);
        let shifted: Vec<f64> = target.iter().map(|value| value + 12.5).collect();

        let plain = measure_session(&feature, &target, DEFAULT_BINS, 1).unwrap();
        let moved = measure_session(&feature, &shifted, DEFAULT_BINS, 1).unwrap();
        // Equal to floating point rather than bitwise: the bins are identical, and what is left is
        // the last place of a sum over a hundred cells.
        assert!(
            (plain.bits - moved.bits).abs() < 1e-12,
            "{plain:?} {moved:?}"
        );
        assert!((plain.excess() - moved.excess()).abs() < 1e-12);
    }

    #[test]
    fn test_a_cross_section_too_thin_to_fill_the_table_is_refused() {
        let (feature, target) = paired(MINIMUM_CROSS_SECTION - 1, |index| index as f64);
        assert_eq!(measure_session(&feature, &target, DEFAULT_BINS, 1), None);
    }

    #[test]
    fn test_mismatched_or_unusable_input_is_refused() {
        assert_eq!(mutual_information(&[0, 1], &[0]), None);
        assert_eq!(mutual_information(&[], &[]), None);
        assert_eq!(quantile_bins(&[1.0, f64::NAN], DEFAULT_BINS), None);
        assert_eq!(quantile_bins(&[1.0, 2.0], 0), None);
    }
}
