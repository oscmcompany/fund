//! What a model arm recovered of a mechanism planted at a stated strength: its reading of the planted market less its
//! reading of the null market that planted nothing, session by session.

use std::num::NonZeroU32;

use crate::common::laboratory::cost::BasisPoints;
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

/// The stated strength, how many times it was planted, the arm's reading of each market, and their paired difference.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Recovery {
    stated: BasisPoints,
    planted_events: u64,
    planted: Estimate,
    null: Estimate,
    recovered: Estimate,
}

impl Recovery {
    /// Refused where the two markets' readings do not cover the same sessions or fewer than two were measured.
    pub fn new(
        stated: BasisPoints,
        planted_events: u64,
        Treatment(planted): Treatment<&Series>,
        Control(null): Control<&Series>,
    ) -> Result<Self, EstimateRefusal> {
        Ok(Self {
            stated,
            planted_events,
            planted: Estimate::from_summary(summarize(planted))?,
            null: Estimate::from_summary(summarize(null))?,
            recovered: paired(Treatment(planted), Control(null))?,
        })
    }

    pub fn stated(&self) -> BasisPoints {
        self.stated
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

    /// The recovered strength over the stated one, `None` for a stated strength of zero.
    pub fn share(&self) -> Option<f64> {
        (self.stated.value() > 0.0).then(|| self.recovered.mean() / self.stated.value())
    }

    /// Whether the recovered strength is positive and clears a single test's bar; a reading that never varied has no
    /// error to clear it by, so it recovers nothing.
    pub fn recovers(&self) -> bool {
        self.recovered.mean() > 0.0
            && self.recovered.clears(Haircut::new(NonZeroU32::MIN)) == Some(true)
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

    fn recovery(stated: f64, planted: &[Option<f64>], null: &[Option<f64>]) -> Recovery {
        Recovery::new(
            BasisPoints::new(stated).unwrap(),
            7,
            Treatment(&series(planted)),
            Control(&series(null)),
        )
        .unwrap()
    }

    #[test]
    fn test_a_reading_above_the_null_by_more_than_its_error_recovers() {
        let null = [Some(2.0), Some(3.0), Some(1.0), Some(2.5), Some(1.5)];
        let planted = [Some(21.0), Some(23.0), Some(20.0), Some(22.0), Some(21.5)];
        let recovered = recovery(20.0, &planted, &null);
        assert!(recovered.recovers());
        assert!((recovered.recovered().mean() - 19.5).abs() < 1e-9);
        assert!((recovered.share().unwrap() - 0.975).abs() < 1e-9);
        assert!((recovered.null().mean() - 2.0).abs() < 1e-9);
    }

    #[test]
    fn test_a_reading_no_different_from_the_null_recovers_nothing() {
        let null = [Some(2.0), Some(3.0), Some(1.0), Some(2.5), Some(1.5)];
        let same = recovery(20.0, &null, &null);
        assert_eq!(same.recovered().standard_error(), 0.0);
        assert!(!same.recovers());
        let noisy = [Some(3.0), Some(2.0), Some(1.5), Some(2.0), Some(2.0)];
        let within_error = recovery(20.0, &noisy, &null);
        assert!(within_error.recovered().mean() > 0.0);
        assert!(!within_error.recovers());
        let below = [Some(0.5), Some(1.0), Some(-0.5), Some(1.0), Some(0.0)];
        assert!(!recovery(20.0, &below, &null).recovers());
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
    }
}
