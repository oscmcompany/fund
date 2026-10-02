//! From a series to evidence: `summarize` folds readings into a `Summary`, a monoid so partitions merge in any order,
//! and an `Estimate` carries the mean with the error it is judged by; the permutation shares judge it without one.

use std::num::NonZeroU32;

use crate::common::laboratory::haircut::{DegreesOfFreedom, Haircut};
use crate::common::laboratory::permutation::Generator;
use crate::common::laboratory::series::{Series, SeriesRefusal};
use crate::common::time::SessionDate;

/// The measured count, mean and summed squared deviations, with the unmeasured counted beside them.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Summary {
    measured: u64,
    undefined: u64,
    /// Zero, and meaningless, while nothing is measured.
    mean: f64,
    squared_deviations: f64,
}

impl Summary {
    pub const EMPTY: Self = Self {
        measured: 0,
        undefined: 0,
        mean: 0.0,
        squared_deviations: 0.0,
    };

    fn of(reading: Option<f64>) -> Self {
        match reading {
            Some(value) => Self {
                measured: 1,
                mean: value,
                ..Self::EMPTY
            },
            None => Self {
                undefined: 1,
                ..Self::EMPTY
            },
        }
    }

    /// Chan's pairwise update: associative up to rounding, and exact against `EMPTY`.
    pub fn combine(self, other: Self) -> Self {
        let undefined = self.undefined + other.undefined;
        match (self.measured, other.measured) {
            (0, _) => Self { undefined, ..other },
            (_, 0) => Self { undefined, ..self },
            (left, right) => {
                let measured = left + right;
                let delta = other.mean - self.mean;
                let share = right as f64 / measured as f64;
                Self {
                    measured,
                    undefined,
                    mean: self.mean + delta * share,
                    squared_deviations: self.squared_deviations
                        + other.squared_deviations
                        + delta * delta * left as f64 * share,
                }
            }
        }
    }

    pub fn measured(self) -> u64 {
        self.measured
    }

    pub fn undefined(self) -> u64 {
        self.undefined
    }
}

pub fn summarize(series: &Series) -> Summary {
    series
        .readings()
        .values()
        .map(|reading| Summary::of(*reading))
        .fold(Summary::EMPTY, Summary::combine)
}

/// A standard error that is positive, with the degrees of freedom it was estimated on.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct StandardError {
    value: f64,
    degrees_of_freedom: DegreesOfFreedom,
}

/// A mean with its sample; `error` is `None` where the readings never varied, so there is nothing to judge it by.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Estimate {
    mean: f64,
    sessions: u64,
    undefined: u64,
    error: Option<StandardError>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum EstimateRefusal {
    Series(SeriesRefusal),
    /// A matched comparison where only one arm read `session`.
    Unmatched {
        session: SessionDate,
    },
    /// Below two measured sessions there is no spread to take an error from.
    TooFewSessions {
        measured: u64,
    },
}

impl std::fmt::Display for EstimateRefusal {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Series(refusal) => write!(formatter, "{refusal}"),
            Self::Unmatched { session } => {
                write!(
                    formatter,
                    "only one arm read {session}, so it cannot be matched"
                )
            }
            Self::TooFewSessions { measured } => {
                write!(
                    formatter,
                    "{measured} measured sessions cannot carry an error"
                )
            }
        }
    }
}

impl std::error::Error for EstimateRefusal {}

impl TryFrom<Summary> for Estimate {
    type Error = EstimateRefusal;

    fn try_from(summary: Summary) -> Result<Self, Self::Error> {
        if summary.measured < 2 {
            return Err(EstimateRefusal::TooFewSessions {
                measured: summary.measured,
            });
        }
        let count = summary.measured as f64;
        let error = (summary.squared_deviations > 0.0).then(|| StandardError {
            value: (summary.squared_deviations / (count - 1.0) / count).sqrt(),
            degrees_of_freedom: DegreesOfFreedom::new(count - 1.0)
                .expect("two or more sessions leave at least one degree of freedom"),
        });
        Ok(Self {
            mean: summary.mean,
            sessions: summary.measured,
            undefined: summary.undefined,
            error,
        })
    }
}

impl Estimate {
    pub fn mean(self) -> f64 {
        self.mean
    }

    /// Measured sessions; a matched estimate counts sessions, a Welch one both arms' together.
    pub fn sessions(self) -> u64 {
        self.sessions
    }

    pub fn undefined(self) -> u64 {
        self.undefined
    }

    pub fn standard_error(self) -> Option<f64> {
        self.error.map(|error| error.value)
    }

    pub fn degrees_of_freedom(self) -> Option<DegreesOfFreedom> {
        self.error.map(|error| error.degrees_of_freedom)
    }

    /// The mean in standard errors.
    pub fn t(self) -> Option<f64> {
        self.error.map(|error| self.mean / error.value)
    }

    pub fn clears(self, haircut: Haircut) -> Option<bool> {
        self.error
            .map(|error| haircut.clears(self.mean / error.value, error.degrees_of_freedom))
    }
}

/// The arm a study changes; wrapping each arm makes an argument order the compiler checks.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Treatment<T>(pub T);

/// The arm a study holds fixed, differing from the treatment in exactly one respect.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Control<T>(pub T);

/// Treatment less control per session, refused where only one arm read a session so nothing is dropped silently.
pub fn matched(
    Treatment(treatment): Treatment<&Series>,
    Control(control): Control<&Series>,
) -> Result<Series, EstimateRefusal> {
    let unmatched = treatment
        .readings()
        .keys()
        .find(|session| !control.readings().contains_key(session))
        .or_else(|| {
            control
                .readings()
                .keys()
                .find(|session| !treatment.readings().contains_key(session))
        });
    match unmatched {
        Some(session) => Err(EstimateRefusal::Unmatched { session: *session }),
        None => treatment
            .zip_with(control, |treatment, control| treatment - control)
            .map_err(EstimateRefusal::Series),
    }
}

/// The matched difference's mean, so variation both arms share cancels before the error is taken.
pub fn paired(
    treatment: Treatment<&Series>,
    control: Control<&Series>,
) -> Result<Estimate, EstimateRefusal> {
    Estimate::try_from(summarize(&matched(treatment, control)?))
}

/// Treatment less control over arms that share no session: errors in quadrature, with Welch–Satterthwaite degrees
/// of freedom because the arms' variances need not agree. The arms' disjointness is the caller's to ensure.
pub fn welch(
    Treatment(treatment): Treatment<Estimate>,
    Control(control): Control<Estimate>,
) -> Estimate {
    let error = match (treatment.error, control.error) {
        (Some(treatment), Some(control)) => {
            let (treatment_variance, control_variance) =
                (treatment.value.powi(2), control.value.powi(2));
            Some(StandardError {
                value: (treatment_variance + control_variance).sqrt(),
                degrees_of_freedom: DegreesOfFreedom::new(
                    (treatment_variance + control_variance).powi(2)
                        / (treatment_variance.powi(2) / treatment.degrees_of_freedom.value()
                            + control_variance.powi(2) / control.degrees_of_freedom.value()),
                )
                .expect("Welch freedom lies between the smaller arm's and the sum of both"),
            })
        }
        // A side that never varied adds no variance, and the formula reduces to the other side's.
        (Some(error), None) | (None, Some(error)) => Some(error),
        (None, None) => None,
    };
    Estimate {
        mean: treatment.mean - control.mean,
        sessions: treatment.sessions + control.sessions,
        undefined: treatment.undefined + control.undefined,
        error,
    }
}

/// The share of sign flips of `differences` whose sum is at least as far from zero as the reading's, counting the
/// reading itself so it is never zero; `None` below two measured differences.
pub fn flipped_share(differences: &Series, seed: u64, permutations: NonZeroU32) -> Option<f64> {
    let measured: Vec<f64> = differences.readings().values().flatten().copied().collect();
    if measured.len() < 2 {
        return None;
    }
    let mut generator = Generator::new(seed);
    // Summed in session order, so a relabeling that reproduces the reading, or negates it, ties it exactly.
    let observed = measured.iter().sum::<f64>().abs();
    let count = (0..permutations.get())
        .filter(|_| {
            let flipped: f64 = measured
                .iter()
                .map(|difference| match generator.coin() {
                    true => -difference,
                    false => *difference,
                })
                .sum();
            flipped.abs() >= observed
        })
        .count();
    Some(share(count, permutations))
}

/// The share of reshufflings of both arms' readings, into groups of their own sizes, whose means lie at least as far
/// apart as the arms' do; `None` below two measured readings in either arm.
pub fn shuffled_share(
    Treatment(treatment): Treatment<&Series>,
    Control(control): Control<&Series>,
    seed: u64,
    permutations: NonZeroU32,
) -> Option<f64> {
    let measured =
        |series: &Series| -> Vec<f64> { series.readings().values().flatten().copied().collect() };
    let (treatment, control) = (measured(treatment), measured(control));
    if treatment.len() < 2 || control.len() < 2 {
        return None;
    }
    let treatment_sessions = treatment.len();
    // Each group summed in sorted order, so the same readings give the same gap whatever order the shuffle left.
    let mean = |group: &[f64]| {
        let mut sorted = group.to_vec();
        sorted.sort_by(f64::total_cmp);
        sorted.iter().sum::<f64>() / sorted.len() as f64
    };
    let gap = |pooled: &[f64]| {
        let (left, right) = pooled.split_at(treatment_sessions);
        (mean(left) - mean(right)).abs()
    };
    let mut pooled = [treatment, control].concat();
    let observed = gap(&pooled);
    let mut generator = Generator::new(seed);
    let count = (0..permutations.get())
        .filter(|_| {
            generator.shuffle(&mut pooled);
            gap(&pooled) >= observed
        })
        .count();
    Some(share(count, permutations))
}

fn share(at_least_as_extreme: usize, permutations: NonZeroU32) -> f64 {
    (at_least_as_extreme as f64 + 1.0) / (f64::from(permutations.get()) + 1.0)
}

#[cfg(test)]
mod tests {
    use chrono::NaiveDate;
    use proptest::prelude::*;

    use super::*;

    fn session(day: i64) -> SessionDate {
        SessionDate::from_date(NaiveDate::from_ymd_opt(2026, 3, 2).unwrap()).plus_calendar_days(day)
    }

    fn series(first_day: i64, readings: &[Option<f64>]) -> Series {
        Series::new(
            readings
                .iter()
                .enumerate()
                .map(|(day, reading)| (session(first_day + day as i64), *reading)),
        )
        .unwrap()
    }

    fn measured(first_day: i64, readings: &[f64]) -> Series {
        series(
            first_day,
            &readings.iter().copied().map(Some).collect::<Vec<_>>(),
        )
    }

    fn estimate(readings: &[f64]) -> Estimate {
        Estimate::try_from(summarize(&measured(0, readings))).unwrap()
    }

    fn haircut(tests: u32) -> Haircut {
        Haircut::new(NonZeroU32::new(tests).unwrap())
    }

    fn permutations(count: u32) -> NonZeroU32 {
        NonZeroU32::new(count).unwrap()
    }

    fn close(left: f64, right: f64) -> bool {
        (left - right).abs() <= 1e-9 * left.abs().max(right.abs()).max(1.0)
    }

    fn normal(generator: &mut Generator) -> f64 {
        let uniform = |generator: &mut Generator| {
            ((generator.next_u64() >> 11) as f64 + 0.5) / (1u64 << 53) as f64
        };
        let (radius, angle) = (uniform(generator), uniform(generator));
        (-2.0 * radius.ln()).sqrt() * (2.0 * std::f64::consts::PI * angle).cos()
    }

    #[test]
    fn test_a_matched_difference_removes_the_variation_both_arms_share() {
        let paired = paired(
            Treatment(&measured(0, &[11.0, -19.0, 32.0, -8.0])),
            Control(&measured(0, &[10.0, -20.0, 30.0, -10.0])),
        )
        .unwrap();
        // Differences 1, 1, 2, 2: standard deviation 1/√3, over √4.
        assert_eq!(paired.mean(), 1.5);
        assert!(close(
            paired.standard_error().unwrap(),
            1.0 / 3f64.sqrt() / 2.0
        ));
        assert_eq!(
            paired.degrees_of_freedom().map(DegreesOfFreedom::value),
            Some(3.0)
        );
        assert_eq!((paired.sessions(), paired.undefined()), (4, 0));
    }

    /// Treatment 1, 3 (squared error 1, one degree of freedom) against control 0, 0, 3 (squared error 1, two).
    #[test]
    fn test_disjoint_arms_add_their_errors_in_quadrature_with_welch_freedom() {
        let difference = welch(
            Treatment(estimate(&[1.0, 3.0])),
            Control(estimate(&[0.0, 0.0, 3.0])),
        );
        assert_eq!(difference.mean(), 1.0);
        assert!(close(difference.standard_error().unwrap(), 2f64.sqrt()));
        assert!(close(
            difference.degrees_of_freedom().unwrap().value(),
            4.0 / 1.5
        ));
        assert_eq!(difference.sessions(), 5);
    }

    #[test]
    fn test_a_side_that_never_varied_leaves_the_other_sides_error() {
        let varied = estimate(&[1.0, 3.0, 8.0]);
        let flat = estimate(&[2.0, 2.0]);
        assert_eq!(
            welch(Treatment(varied), Control(flat)).standard_error(),
            varied.standard_error()
        );
        assert_eq!(
            welch(Treatment(flat), Control(varied)).degrees_of_freedom(),
            varied.degrees_of_freedom()
        );
        assert_eq!(welch(Treatment(flat), Control(flat)).standard_error(), None);
    }

    #[test]
    fn test_an_unmeasured_session_is_counted_rather_than_zeroed() {
        let paired = paired(
            Treatment(&series(0, &[Some(2.0), None, Some(4.0), Some(6.0)])),
            Control(&series(0, &[Some(1.0), Some(1.0), None, Some(1.0)])),
        )
        .unwrap();
        assert_eq!(
            (paired.sessions(), paired.undefined(), paired.mean()),
            (2, 2, 3.0)
        );
    }

    #[test]
    fn test_a_session_only_one_arm_read_is_refused() {
        assert_eq!(
            paired(
                Treatment(&measured(0, &[1.0, 2.0, 3.0])),
                Control(&measured(1, &[1.0, 2.0, 3.0]))
            ),
            Err(EstimateRefusal::Unmatched {
                session: session(0)
            })
        );
        assert_eq!(
            paired(
                Treatment(&measured(0, &[1.0, 2.0])),
                Control(&measured(0, &[1.0, 2.0, 3.0]))
            ),
            Err(EstimateRefusal::Unmatched {
                session: session(2)
            })
        );
    }

    #[test]
    fn test_a_difference_that_never_varied_has_no_error_to_judge() {
        let flat = paired(
            Treatment(&measured(0, &[2.0, 3.0])),
            Control(&measured(0, &[1.0, 2.0])),
        )
        .unwrap();
        assert_eq!(
            (flat.mean(), flat.t(), flat.clears(haircut(1))),
            (1.0, None, None)
        );
        assert_eq!(
            Estimate::try_from(summarize(&series(0, &[Some(1.0), None]))),
            Err(EstimateRefusal::TooFewSessions { measured: 1 })
        );
    }

    /// At 31 degrees of freedom one test needs 2.04 and two need 2.36, so 2.2 tells a family of one from two.
    #[test]
    fn test_a_reading_is_judged_against_its_count_of_tests() {
        let mean = 2.2 / 31f64.sqrt();
        let readings: Vec<f64> = (0..32)
            .map(|day| mean + if day % 2 == 0 { 1.0 } else { -1.0 })
            .collect();
        let estimate = estimate(&readings);
        assert!(close(estimate.t().unwrap(), 2.2));
        assert_eq!(
            (estimate.clears(haircut(1)), estimate.clears(haircut(2))),
            (Some(true), Some(false))
        );
    }

    /// 2.5 standard errors clears the normal's 1.96 but not Student's 12.7 at one degree of freedom.
    #[test]
    fn test_a_small_sample_is_judged_at_its_own_degrees_of_freedom() {
        let small = welch(
            Treatment(estimate(&[1.5, 3.5])),
            Control(estimate(&[0.0, 0.0])),
        );
        assert!(close(small.t().unwrap(), 2.5));
        assert_eq!(small.clears(haircut(1)), Some(false));
    }

    #[test]
    fn test_readings_at_the_bound_estimate_finite() {
        let (high, low) = (
            measured(0, &[1e50, -1e50, 1e50]),
            measured(0, &[-1e50, 1e50, -1e50]),
        );
        let paired = paired(Treatment(&high), Control(&low));
        assert!(matches!(
            paired,
            Err(EstimateRefusal::Series(SeriesRefusal::OutOfRange { .. }))
        ));
        let disjoint = welch(
            Treatment(Estimate::try_from(summarize(&high)).unwrap()),
            Control(Estimate::try_from(summarize(&low)).unwrap()),
        );
        assert!(disjoint.standard_error().unwrap().is_finite());
        assert!(disjoint.degrees_of_freedom().is_some());
    }

    /// Twenty positive differences: only the two all-same-sign relabelings reach them, about one in half a million.
    #[test]
    fn test_a_consistent_effect_is_rare_under_its_null() {
        let differences = measured(0, &(1..=20).map(f64::from).collect::<Vec<_>>());
        assert_eq!(
            flipped_share(&differences, 1, permutations(199)),
            Some(1.0 / 200.0)
        );
        assert_eq!(
            flipped_share(&series(0, &[Some(1.0), None]), 1, permutations(199)),
            None
        );
    }

    /// Three equal differences tie the reading exactly on two of eight relabelings, and a tie counts.
    #[test]
    fn test_a_relabeling_that_ties_the_reading_counts_as_extreme() {
        let share = flipped_share(&measured(0, &[1.0, 1.0, 1.0]), 1, permutations(199)).unwrap();
        assert!((0.15..=0.36).contains(&share), "{share}");
    }

    /// A one-dollar gap between readings near a trillion is reached by 2 of the 6 two-by-two splits, not all of them.
    #[test]
    fn test_a_small_gap_between_large_readings_is_not_tied_by_every_split() {
        let share = shuffled_share(
            Treatment(&measured(0, &[1e12 + 1.0, 1e12 + 1.0])),
            Control(&measured(10, &[1e12, 1e12])),
            5,
            permutations(599),
        )
        .unwrap();
        assert!((0.25..=0.42).contains(&share), "{share}");
    }

    /// Readings whose plain sum depends on their order (±1e17 absorbs the small ones) still give one gap per set: the
    /// reading's gap is zero, so every relabeling is at least as extreme.
    #[test]
    fn test_the_same_readings_give_the_same_gap_in_any_order() {
        let share = shuffled_share(
            Treatment(&measured(0, &[0.1, 1e17, 0.7, -1e17, 0.2])),
            Control(&measured(10, &[0.0; 5])),
            1,
            permutations(199),
        );
        assert_eq!(share, Some(1.0));
    }

    #[test]
    fn test_disjoint_groups_are_reshuffled_into_their_own_sizes() {
        // Only the one split of eleven readings into five and six that reproduces the groups is as extreme.
        let separated = shuffled_share(
            Treatment(&measured(0, &[10.0, 11.0, 12.0, 13.0, 14.0])),
            Control(&measured(20, &[0.0, 1.0, 2.0, 3.0, 4.0, 5.0])),
            1,
            permutations(199),
        )
        .unwrap();
        assert!(separated <= 2.0 / 200.0, "{separated}");
        let interleaved = shuffled_share(
            Treatment(&measured(0, &[0.0, 2.0, 4.0, 6.0])),
            Control(&measured(20, &[1.0, 3.0, 5.0, 7.0])),
            1,
            permutations(199),
        )
        .unwrap();
        assert!(interleaved > 0.5, "{interleaved}");
        assert_eq!(
            shuffled_share(
                Treatment(&measured(0, &[1.0, 2.0])),
                Control(&measured(9, &[1.0])),
                1,
                permutations(9)
            ),
            None
        );
    }

    #[test]
    fn test_a_seed_redraws_the_same_null() {
        let mut generator = Generator::new(3);
        let differences = measured(
            0,
            &(0..30)
                .map(|_| 0.3 + normal(&mut generator))
                .collect::<Vec<_>>(),
        );
        let draw = |seed| flipped_share(&differences, seed, permutations(999));
        assert_eq!(draw(1), draw(1));
        assert_ne!(draw(1), draw(2));
    }

    /// Families of ten noise estimates clear their haircut at no more than the promised 5%, at thirty sessions and at
    /// three, where a normal cutoff would clear far more often; a real effect clears nearly always.
    #[test]
    fn test_noise_families_clear_no_more_often_than_the_family_wise_rate() {
        let mut generator = Generator::new(2026);
        let mut noise = |sessions: usize, effect: f64| {
            estimate(
                &(0..sessions)
                    .map(|_| effect + normal(&mut generator))
                    .collect::<Vec<_>>(),
            )
        };
        for sessions in [30, 3] {
            let families = 300;
            let clearing = (0..families)
                .filter(|_| (0..10).any(|_| noise(sessions, 0.0).clears(haircut(10)) == Some(true)))
                .count();
            let rate = clearing as f64 / f64::from(families);
            assert!(
                (0.01..=0.083).contains(&rate),
                "{sessions} sessions: {clearing} of {families}"
            );
        }
        let found = (0..100)
            .filter(|_| noise(30, 1.0).clears(haircut(1)) == Some(true))
            .count();
        assert!(found >= 95, "{found} of 100");
    }

    /// Under noise a p-value is uniform, so the permutation share lands below 5% and below a half about as often.
    #[test]
    fn test_the_permutation_share_is_uniform_under_noise() {
        let mut generator = Generator::new(17);
        let shares: Vec<f64> = (0..400)
            .map(|seed| {
                let differences = measured(
                    0,
                    &(0..20).map(|_| normal(&mut generator)).collect::<Vec<_>>(),
                );
                flipped_share(&differences, seed, permutations(99)).unwrap()
            })
            .collect();
        let below =
            |line: f64| shares.iter().filter(|share| **share <= line).count() as f64 / 400.0;
        assert!((0.017..=0.083).contains(&below(0.05)), "{}", below(0.05));
        assert!((0.43..=0.57).contains(&below(0.5)), "{}", below(0.5));
    }

    fn readings() -> impl Strategy<Value = Vec<Option<f64>>> {
        prop::collection::vec(prop::option::of(-1000.0..1000.0f64), 0..30)
    }

    fn agree(left: Summary, right: Summary) -> bool {
        (left.measured, left.undefined) == (right.measured, right.undefined)
            && close(left.mean, right.mean)
            && (left.squared_deviations - right.squared_deviations).abs()
                <= 1e-6 * left.squared_deviations.max(1.0)
    }

    proptest! {
        #[test]
        fn test_empty_is_the_identity_of_combine(readings in readings()) {
            let summary = summarize(&series(0, &readings));
            prop_assert_eq!(Summary::EMPTY.combine(summary), summary);
            prop_assert_eq!(summary.combine(Summary::EMPTY), summary);
        }

        #[test]
        fn test_combine_is_associative(first in readings(), second in readings(), third in readings()) {
            let (first, second, third) = (
                summarize(&series(0, &first)),
                summarize(&series(0, &second)),
                summarize(&series(0, &third)),
            );
            prop_assert!(agree(
                first.combine(second).combine(third),
                first.combine(second.combine(third))
            ));
        }

        /// Summarizing joined partitions is combining their summaries, so a dataset read in pieces summarizes whole.
        #[test]
        fn test_summarize_carries_concatenation_to_combine(first in readings(), second in readings()) {
            let (first, second) = (series(0, &first), series(100, &second));
            prop_assert!(agree(
                summarize(&first.concatenate(&second).unwrap()),
                summarize(&first).combine(summarize(&second))
            ));
        }

        /// The composed pipeline, matched then summarized then estimated, equals the two-pass textbook formula.
        #[test]
        fn test_paired_equals_its_direct_computation(rows in prop::collection::vec((-100.0..100.0f64, -100.0..100.0f64), 2..40)) {
            let estimate = paired(
                Treatment(&measured(0, &rows.iter().map(|(treatment, _)| *treatment).collect::<Vec<_>>())),
                Control(&measured(0, &rows.iter().map(|(_, control)| *control).collect::<Vec<_>>())),
            )
            .unwrap();
            let differences: Vec<f64> = rows.iter().map(|(treatment, control)| treatment - control).collect();
            let count = differences.len() as f64;
            let mean = differences.iter().sum::<f64>() / count;
            let variance = differences.iter().map(|value| (value - mean).powi(2)).sum::<f64>() / (count - 1.0);
            prop_assert!(close(estimate.mean(), mean));
            prop_assert!((estimate.standard_error().unwrap_or(0.0) - (variance / count).sqrt()).abs() < 1e-9);
        }

        /// Adding the same per-session shock to both matched arms moves neither the difference nor its error.
        #[test]
        fn test_a_shared_shock_cancels_in_a_matched_difference(rows in prop::collection::vec((-100.0..100.0f64, -100.0..100.0f64, -1000.0..1000.0f64), 2..40)) {
            let read = |shocked: bool| {
                let arm = |pick: fn(&(f64, f64, f64)) -> f64| {
                    measured(0, &rows.iter().map(|row| pick(row) + if shocked { row.2 } else { 0.0 }).collect::<Vec<_>>())
                };
                paired(Treatment(&arm(|row| row.0)), Control(&arm(|row| row.1))).unwrap()
            };
            let (plain, shocked) = (read(false), read(true));
            prop_assert!((plain.mean() - shocked.mean()).abs() < 1e-6);
            match (plain.standard_error(), shocked.standard_error()) {
                (Some(left), Some(right)) => prop_assert!((left - right).abs() < 1e-4 * left.max(1.0)),
                (left, right) => prop_assert_eq!(left.is_some(), right.is_some()),
            }
        }

        /// Swapping disjoint arms negates the difference and keeps its error and its degrees of freedom.
        #[test]
        fn test_swapping_disjoint_arms_negates_the_difference(treatment in prop::collection::vec(-100.0..100.0f64, 2..40), control in prop::collection::vec(-100.0..100.0f64, 2..40)) {
            let (treatment, control) = (estimate(&treatment), estimate(&control));
            let forward = welch(Treatment(treatment), Control(control));
            // The relabeling is the point: the old control is named the treatment.
            let backward = welch(Treatment(control), Control(treatment));
            prop_assert_eq!(forward.mean(), -backward.mean());
            prop_assert_eq!(forward.standard_error(), backward.standard_error());
            prop_assert_eq!(forward.degrees_of_freedom(), backward.degrees_of_freedom());
        }
    }
}
