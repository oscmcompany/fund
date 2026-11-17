//! What a model arm recovered of a mechanism planted at a stated strength: its reading of the planted market less its
//! reading of the null market that planted nothing, session by session.

use std::num::NonZeroU32;

use crate::common::cost::BasisPoints;
use crate::common::laboratory::estimate::{
    Control, Estimate, EstimateRefusal, Treatment, paired, summarize,
};
use crate::common::laboratory::experiment::{ExperimentRefusal, Metric, Outputs};
use crate::common::laboratory::haircut::Haircut;
use crate::common::laboratory::series::Series;
use crate::common::laboratory::verdict::{Checks, NullReading};

/// A reading a recovery journals, by the name it is journaled under.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, strum::Display, strum::IntoStaticStr, strum::EnumIter,
)]
#[strum(serialize_all = "snake_case")]
pub enum RecoveryOutput {
    StatedStrengthBasisPoints,
    PlantedEvents,
    PlantedReadingBasisPoints,
    NullReadingBasisPoints,
    RecoveredStrengthBasisPoints,
    RecoveredShare,
}

/// Which way an arm's reading moves when its mechanism is planted, the stated strength being a magnitude.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    strum::Display,
    strum::EnumString,
    strum::IntoStaticStr,
    strum::EnumIter,
)]
#[strum(serialize_all = "snake_case")]
pub enum Direction {
    Rises,
    Falls,
}

impl Direction {
    fn sign(self) -> f64 {
        match self {
            Self::Rises => 1.0,
            Self::Falls => -1.0,
        }
    }
}

/// Which try of an arm against the synthetic market this is, earlier tries included, so each retry raises the bar.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Attempt(NonZeroU32);

impl Attempt {
    pub const FIRST: Self = Self(NonZeroU32::MIN);

    pub fn new(attempt: NonZeroU32) -> Self {
        Self(attempt)
    }

    pub fn get(self) -> NonZeroU32 {
        self.0
    }
}

/// Why a recovery does not admit its arm, with the numbers that refused it.
#[derive(Debug, Clone, Copy, PartialEq, thiserror::Error)]
pub enum Shortfall {
    #[error("the recovered strength never varied, so it has no error to clear a bar by")]
    NoError,
    #[error("the arm recovered {recovered} against its expected direction")]
    WrongDirection { recovered: f64 },
    #[error(
        "the recovery is {standard_errors} standard errors from zero, short of the {required} its attempt needs"
    )]
    BelowBar { standard_errors: f64, required: f64 },
    /// Detected but not measured: the expected reading lies outside a single test's interval around the recovered one.
    #[error("the expected {expected} lies outside the recovered interval from {lower} to {upper}")]
    Inaccurate {
        expected: f64,
        lower: f64,
        upper: f64,
    },
}

/// The stated strength, the arm's direction and attempt, how many times the mechanism was planted, the arm's reading
/// of each market, and their paired difference.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Recovery {
    stated: BasisPoints,
    direction: Direction,
    attempt: Attempt,
    planted_events: u64,
    planted: Estimate,
    null: Estimate,
    recovered: Estimate,
}

impl Recovery {
    /// Refused where the two markets' readings do not cover the same sessions or fewer than two were measured.
    pub fn new(
        stated: BasisPoints,
        direction: Direction,
        attempt: Attempt,
        planted_events: u64,
        Treatment(planted): Treatment<&Series>,
        Control(null): Control<&Series>,
    ) -> Result<Self, EstimateRefusal> {
        Ok(Self {
            stated,
            direction,
            attempt,
            planted_events,
            planted: Estimate::from_summary(summarize(planted))?,
            null: Estimate::from_summary(summarize(null))?,
            recovered: paired(Treatment(planted), Control(null))?,
        })
    }

    pub fn stated(&self) -> BasisPoints {
        self.stated
    }

    pub fn direction(&self) -> Direction {
        self.direction
    }

    pub fn attempt(&self) -> Attempt {
        self.attempt
    }

    /// The reading a perfect arm would recover: the stated strength in the arm's direction.
    pub fn expected(&self) -> f64 {
        self.direction.sign() * self.stated.value()
    }

    pub fn planted_events(&self) -> u64 {
        self.planted_events
    }

    pub fn planted(&self) -> Estimate {
        self.planted
    }

    /// The arm's own reading where nothing was planted, which is the machinery's bias.
    pub fn null(&self) -> Estimate {
        self.null
    }

    pub fn recovered(&self) -> Estimate {
        self.recovered
    }

    /// The recovered strength over the expected one, `None` for a stated strength of zero.
    pub fn share(&self) -> Option<f64> {
        (self.stated.value() > 0.0).then(|| self.recovered.mean() / self.expected())
    }

    /// Admits the arm where the recovered strength moves in its direction, clears the bar of a family as large as its
    /// attempt, and holds the expected reading within a single test's interval around it.
    pub fn admits(&self) -> Result<(), Shortfall> {
        let Some(standard_errors) = self.recovered.t() else {
            return Err(Shortfall::NoError);
        };
        let mean = self.recovered.mean();
        if mean * self.direction.sign() <= 0.0 {
            return Err(Shortfall::WrongDirection { recovered: mean });
        }
        let freedom = self.recovered.degrees_of_freedom();
        let required = Haircut::new(self.attempt.get()).required_standard_errors(freedom);
        if standard_errors.abs() < required {
            return Err(Shortfall::BelowBar {
                standard_errors: standard_errors.abs(),
                required,
            });
        }
        // A single test's width, so a retry raises the bar above without widening what counts as accurate.
        let half_width = Haircut::new(NonZeroU32::MIN).required_standard_errors(freedom)
            * self.recovered.standard_error();
        let (lower, upper, expected) = (mean - half_width, mean + half_width, self.expected());
        match (lower..=upper).contains(&expected) {
            true => Ok(()),
            false => Err(Shortfall::Inaccurate {
                expected,
                lower,
                upper,
            }),
        }
    }

    /// Every reading by its `RecoveryOutput` name, with the sessions read, those either market left unmeasured, and
    /// the null market's reading as the checks.
    pub fn outputs(&self) -> Result<Outputs, ExperimentRefusal> {
        let name = |output: RecoveryOutput| -> &'static str { output.into() };
        let mut outputs = Outputs::default()
            .estimate(
                name(RecoveryOutput::PlantedReadingBasisPoints),
                self.planted,
            )?
            .estimate(name(RecoveryOutput::NullReadingBasisPoints), self.null)?
            .estimate(
                name(RecoveryOutput::RecoveredStrengthBasisPoints),
                self.recovered,
            )?
            .metric(
                name(RecoveryOutput::StatedStrengthBasisPoints),
                self.stated.value(),
            )?
            .metric(
                name(RecoveryOutput::PlantedEvents),
                self.planted_events as f64,
            )?;
        if let Some(share) = self.share() {
            outputs = outputs.metric(name(RecoveryOutput::RecoveredShare), share)?;
        }
        let null = Metric::new(self.null.mean()).expect("an estimate's mean is finite");
        outputs.checks(Checks::new(
            self.recovered.sessions() + self.recovered.undefined(),
            self.recovered.undefined(),
            NullReading::Measured(null),
        )?)
    }
}

#[cfg(test)]
mod tests {
    use chrono::NaiveDate;
    use proptest::prelude::*;

    use super::*;
    use crate::common::time::SessionDate;

    fn series(readings: &[Option<f64>]) -> Series {
        let first = SessionDate::from_date(NaiveDate::from_ymd_opt(2026, 3, 2).unwrap());
        Series::new(
            readings
                .iter()
                .enumerate()
                .map(|(day, reading)| (first.plus_calendar_days(day as i64), *reading)),
        )
        .unwrap()
    }

    fn attempted(
        stated: f64,
        direction: Direction,
        attempt: u32,
        planted: &[Option<f64>],
        null: &[Option<f64>],
    ) -> Recovery {
        Recovery::new(
            BasisPoints::new(stated).unwrap(),
            direction,
            Attempt::new(NonZeroU32::new(attempt).unwrap()),
            7,
            Treatment(&series(planted)),
            Control(&series(null)),
        )
        .unwrap()
    }

    fn recovery(stated: f64, planted: &[Option<f64>], null: &[Option<f64>]) -> Recovery {
        attempted(stated, Direction::Rises, 1, planted, null)
    }

    /// The null's readings shifted by `differences`, so the recovered strength is exactly their mean.
    fn shifted(null: &[Option<f64>], differences: &[f64]) -> Vec<Option<f64>> {
        null.iter()
            .zip(differences)
            .map(|(reading, difference)| reading.map(|value| value + difference))
            .collect()
    }

    const NULL: [Option<f64>; 5] = [Some(2.0), Some(3.0), Some(1.0), Some(2.5), Some(1.5)];

    #[test]
    fn test_a_reading_above_the_null_by_more_than_its_error_recovers() {
        let planted = [Some(21.0), Some(23.0), Some(20.0), Some(22.0), Some(21.5)];
        let recovered = recovery(20.0, &planted, &NULL);
        assert_eq!(recovered.admits(), Ok(()));
        assert!((recovered.recovered().mean() - 19.5).abs() < 1e-9);
        assert!((recovered.share().unwrap() - 0.975).abs() < 1e-9);
        assert!((recovered.null().mean() - 2.0).abs() < 1e-9);
    }

    #[test]
    fn test_a_reading_no_different_from_the_null_recovers_nothing() {
        let same = recovery(20.0, &NULL, &NULL);
        assert_eq!(same.recovered().standard_error(), 0.0);
        assert_eq!(same.admits(), Err(Shortfall::NoError));
        let noisy = [Some(3.0), Some(2.0), Some(1.5), Some(2.0), Some(2.0)];
        let within_error = recovery(20.0, &noisy, &NULL);
        assert!(within_error.recovered().mean() > 0.0);
        assert!(matches!(
            within_error.admits(),
            Err(Shortfall::BelowBar { .. })
        ));
        let below = [Some(0.5), Some(1.0), Some(-0.5), Some(1.0), Some(0.0)];
        match recovery(20.0, &below, &NULL).admits() {
            Err(Shortfall::WrongDirection { recovered }) => {
                assert!((recovered + 1.6).abs() < 1e-9, "{recovered}");
            }
            other => panic!("{other:?}"),
        }
    }

    /// Detection is not measurement: a tenth of the plant clears zero by far, but the plant lies outside its interval.
    #[test]
    fn test_an_arm_recovering_a_tenth_of_the_plant_is_refused_and_one_recovering_it_passes() {
        let tenth = shifted(&NULL, &[3.5, 4.5, 4.0, 3.75, 4.25]);
        match recovery(40.0, &tenth, &NULL).admits() {
            Err(Shortfall::Inaccurate {
                expected,
                lower,
                upper,
            }) => {
                assert_eq!(expected, 40.0);
                assert!(lower > 3.0 && upper < 5.0, "{lower} to {upper}");
            }
            other => panic!("{other:?}"),
        }
        let accurate = shifted(&NULL, &[39.5, 40.5, 40.0, 39.75, 40.25]);
        assert_eq!(recovery(40.0, &accurate, &NULL).admits(), Ok(()));
        let overshot = shifted(&NULL, &[79.5, 80.5, 80.0, 79.75, 80.25]);
        assert!(matches!(
            recovery(40.0, &overshot, &NULL).admits(),
            Err(Shortfall::Inaccurate { .. })
        ));
    }

    /// The same reading clears a first attempt's bar of 2.776 at four degrees of freedom but not a tenth's of 5.598.
    #[test]
    fn test_a_later_attempt_needs_a_larger_reading() {
        let planted = shifted(&NULL, &[10.0, 2.0, 18.0, 6.0, 14.0]);
        let first = attempted(10.0, Direction::Rises, 1, &planted, &NULL);
        assert!((first.recovered().t().unwrap() - 10.0 / 8f64.sqrt()).abs() < 1e-9);
        assert_eq!(first.admits(), Ok(()));
        match attempted(10.0, Direction::Rises, 10, &planted, &NULL).admits() {
            Err(Shortfall::BelowBar {
                standard_errors,
                required,
            }) => {
                assert!((standard_errors - 10.0 / 8f64.sqrt()).abs() < 1e-9);
                assert!((required - 5.597_568).abs() < 1e-5, "{required}");
            }
            other => panic!("{other:?}"),
        }
        let larger = shifted(&NULL, &[20.0, 12.0, 28.0, 16.0, 24.0]);
        assert_eq!(
            attempted(20.0, Direction::Rises, 10, &larger, &NULL).admits(),
            Ok(())
        );
    }

    #[test]
    fn test_an_arm_expecting_its_reading_to_fall_is_admitted_only_on_a_fall() {
        let fall = shifted(&NULL, &[-19.5, -20.5, -20.0, -19.75, -20.25]);
        let falling = attempted(20.0, Direction::Falls, 1, &fall, &NULL);
        assert_eq!(falling.expected(), -20.0);
        assert_eq!(falling.admits(), Ok(()));
        assert!((falling.share().unwrap() - 1.0).abs() < 1e-9);
        assert!(matches!(
            attempted(20.0, Direction::Rises, 1, &fall, &NULL).admits(),
            Err(Shortfall::WrongDirection { .. })
        ));
        let rise = shifted(&NULL, &[19.5, 20.5, 20.0, 19.75, 20.25]);
        assert!(matches!(
            attempted(20.0, Direction::Falls, 1, &rise, &NULL).admits(),
            Err(Shortfall::WrongDirection { .. })
        ));
        let noisy_fall = shifted(&NULL, &[-10.0, -2.0, -18.0, -6.0, -14.0]);
        match attempted(10.0, Direction::Falls, 10, &noisy_fall, &NULL).admits() {
            Err(Shortfall::BelowBar {
                standard_errors, ..
            }) => assert!((standard_errors - 10.0 / 8f64.sqrt()).abs() < 1e-9),
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn test_a_direction_reads_back_from_its_name() {
        assert_eq!(Direction::Rises.to_string(), "rises");
        assert_eq!(Direction::Falls.to_string(), "falls");
        for direction in <Direction as strum::IntoEnumIterator>::iter() {
            assert_eq!(direction.to_string().parse(), Ok(direction));
        }
    }

    #[test]
    fn test_a_stated_strength_of_zero_has_no_share() {
        let null = [Some(2.0), Some(3.0)];
        assert_eq!(recovery(0.0, &null, &null).share(), None);
    }

    #[test]
    fn test_the_outputs_name_every_reading_and_the_checks_count_the_unmeasured() {
        let null = [Some(2.0), None, Some(1.0), Some(2.5)];
        let planted = [Some(21.0), Some(23.0), None, Some(22.0)];
        let outputs = recovery(20.0, &planted, &null).outputs().unwrap();
        let journaled = crate::common::laboratory::experiment::ExperimentRan::new(
            crate::common::laboratory::experiment::Label::new("recovery").unwrap(),
            crate::common::laboratory::experiment::Machine::new(
                "host",
                "aarch64",
                "macos",
                NonZeroU32::MIN,
            )
            .unwrap(),
            Default::default(),
            Vec::new(),
            outputs,
            crate::common::laboratory::experiment::Elapsed::from_milliseconds(0),
            crate::common::laboratory::experiment::SourceDigest::new(&"0".repeat(64)).unwrap(),
        );
        let estimates: Vec<&str> = journaled
            .estimates()
            .keys()
            .map(|name| name.as_str())
            .collect();
        assert_eq!(
            estimates,
            [
                "null_reading_basis_points",
                "planted_reading_basis_points",
                "recovered_strength_basis_points"
            ]
        );
        let metrics: Vec<&str> = journaled
            .metrics()
            .keys()
            .map(|name| name.as_str())
            .collect();
        assert_eq!(
            metrics,
            [
                "planted_events",
                "recovered_share",
                "stated_strength_basis_points"
            ]
        );
        assert_eq!(
            journaled.metrics()["planted_events"],
            Metric::new(7.0).unwrap()
        );
        let checks = journaled.checks().unwrap();
        assert_eq!((checks.population(), checks.undefined()), (4, 2));
        match checks.null() {
            NullReading::Measured(metric) => {
                assert!((metric.value() - 5.5 / 3.0).abs() < 1e-9, "{metric:?}");
            }
            NullReading::Unmeasured { cause } => panic!("{cause:?}"),
        }
    }

    proptest! {
        /// Shifting every planted reading by a constant recovers exactly that constant, whatever the null read.
        #[test]
        fn test_a_constant_shift_is_recovered_exactly(
            null in prop::collection::vec(-100.0..100.0f64, 2..20),
            shift in -50.0..50.0f64,
        ) {
            let null: Vec<Option<f64>> = null.into_iter().map(Some).collect();
            let planted: Vec<Option<f64>> = null.iter().map(|reading| reading.map(|value| value + shift)).collect();
            let recovered = recovery(10.0, &planted, &null).recovered();
            prop_assert!((recovered.mean() - shift).abs() < 1e-9);
            prop_assert!(recovered.standard_error() < 1e-9);
        }

        /// A later attempt never admits what an earlier one refused, and mirroring every difference along with the
        /// direction leaves the verdict unchanged.
        #[test]
        fn test_admission_is_monotone_in_the_attempt_and_symmetric_in_the_direction(
            center in -60.0..60.0f64,
            noise in prop::collection::vec(-5.0..5.0f64, 3..12),
            offset in -5.0..5.0f64,
            attempt in 1..40u32,
        ) {
            // Centred differences and a stated strength near their mean, so both verdicts are reached.
            let differences: Vec<f64> = noise.iter().map(|value| center + value).collect();
            let stated = (center.abs() + offset).max(0.0);
            let null: Vec<Option<f64>> = differences.iter().map(|_| Some(0.0)).collect();
            let mirrored: Vec<f64> = differences.iter().map(|difference| -difference).collect();
            let rises = attempted(stated, Direction::Rises, attempt, &shifted(&null, &differences), &null);
            let later = attempted(stated, Direction::Rises, attempt + 1, &shifted(&null, &differences), &null);
            let falls = attempted(stated, Direction::Falls, attempt, &shifted(&null, &mirrored), &null);
            prop_assert!(rises.admits().is_ok() || later.admits().is_err());
            prop_assert_eq!(rises.admits().is_ok(), falls.admits().is_ok());
        }
    }
}
