//! Re-runs a comparison under small changes to its incidental inputs, a later first session or another quoted spread,
//! and journals every change's paired difference in one experiment, so a reading whose sign turns shows as unstable.

use std::num::NonZeroUsize;

use crate::common::cost::BasisPoints;
use crate::common::laboratory::estimate::Estimate;
use crate::common::laboratory::experiment::{ExperimentRefusal, Outputs};
use crate::common::market::record::BarInterval;
use crate::common::replay::{Controls, FillModel};
use crate::common::strategy::Strategy;
use crate::laboratory::Study;
use crate::laboratory::compare::{Arm, ArmSide, DIFFERENCE, replay_arms};
use crate::laboratory::dataset::Dataset;
use crate::laboratory::replay::{Opening, ReplayStudyError, beside, settings};

/// One small change to a comparison's incidental inputs, applied to both arms alike.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Perturbation {
    /// The dataset's first `sessions` sessions are left out of both replays.
    LaterStart { sessions: NonZeroUsize },
    /// Both arms cross at this quoted spread in place of the comparison's own.
    QuotedSpread(BasisPoints),
}

impl std::fmt::Display for Perturbation {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::LaterStart { sessions } => write!(formatter, "later_start_{sessions}_sessions"),
            Self::QuotedSpread(spread) => write!(formatter, "quoted_spread_{spread}"),
        }
    }
}

/// The comparison's own paired difference and the difference under each perturbation, in the order declared.
#[derive(Debug)]
pub struct Sensitivity {
    unperturbed: Estimate,
    perturbed: Vec<(Perturbation, Estimate)>,
}

impl Sensitivity {
    pub fn unperturbed(&self) -> Estimate {
        self.unperturbed
    }

    pub fn perturbed(&self) -> &[(Perturbation, Estimate)] {
        &self.perturbed
    }

    /// How many perturbed means fall strictly on the other side of zero from the unperturbed mean.
    pub fn sign_turns(&self) -> usize {
        self.perturbed
            .iter()
            .filter(|(_, estimate)| estimate.mean() * self.unperturbed.mean() < 0.0)
            .count()
    }
}

/// Compares both arms as `compare` does, then again under each of `perturbations`, and journals one experiment
/// holding every paired difference and the count of sign turns.
#[expect(
    clippy::too_many_arguments,
    reason = "each is one condition both arms share, named at the call so none is mistaken for another"
)]
pub fn sensitivity<C: Strategy + Clone, B: Strategy + Clone>(
    study: &mut Study,
    dataset: &Dataset,
    candidate: &Arm<C>,
    baseline: &Arm<B>,
    fill_model: FillModel,
    decision: BarInterval,
    opening: Opening,
    controls: Controls,
    perturbations: &[Perturbation],
) -> Result<Sensitivity, ReplayStudyError> {
    let declared: Vec<String> = perturbations.iter().map(ToString::to_string).collect();
    // A repeat would only be refused as a duplicate output once every replay had run.
    if let Some((index, _)) = perturbations
        .iter()
        .enumerate()
        .find(|(index, perturbation)| perturbations[..*index].contains(perturbation))
    {
        return Err(ReplayStudyError::Experiment(ExperimentRefusal::Duplicate {
            name: format!("{DIFFERENCE}_{}", declared[index]),
        }));
    }
    // A run identical to the unperturbed one cannot turn, so it would only pad the count of stable readings.
    if perturbations.contains(&Perturbation::QuotedSpread(fill_model.quoted_spread())) {
        return Err(ReplayStudyError::UnperturbedSpread {
            quoted_spread: fill_model.quoted_spread(),
        });
    }
    let parameters = beside(
        [
            (
                ArmSide::Candidate.into(),
                candidate.name().as_str().to_string(),
            ),
            (
                ArmSide::Baseline.into(),
                baseline.name().as_str().to_string(),
            ),
            ("perturbations", declared.join(",")),
        ],
        settings(fill_model, decision, &controls, opening),
    )?;
    let difference = |fill_model: FillModel, skipped: usize| {
        replay_arms(
            dataset,
            candidate.strategy().clone(),
            baseline.strategy().clone(),
            fill_model,
            decision,
            &controls,
            opening,
            skipped,
        )
        .map(|(comparison, _, _)| comparison.difference())
    };
    let unperturbed = difference(fill_model, 0)?;
    let perturbed = perturbations
        .iter()
        .map(|perturbation| {
            let estimate = match perturbation {
                Perturbation::LaterStart { sessions } => difference(fill_model, sessions.get()),
                Perturbation::QuotedSpread(spread) => difference(
                    FillModel::new(fill_model.style(), *spread).map_err(ReplayStudyError::Cost)?,
                    0,
                ),
            }?;
            Ok((*perturbation, estimate))
        })
        .collect::<Result<Vec<_>, ReplayStudyError>>()?;
    let sensitivity = Sensitivity {
        unperturbed,
        perturbed,
    };
    let mut outputs = Outputs::default()
        .estimate(DIFFERENCE, unperturbed)
        .map_err(ReplayStudyError::Experiment)?;
    for (perturbation, estimate) in &sensitivity.perturbed {
        outputs = outputs
            .estimate(format!("{DIFFERENCE}_{perturbation}"), *estimate)
            .map_err(ReplayStudyError::Experiment)?;
    }
    let outputs = outputs
        .metric("sign_turns", sensitivity.sign_turns() as f64)
        .map_err(ReplayStudyError::Experiment)?;
    study
        .experiment(parameters, &[dataset.lineage()], outputs)
        .map_err(ReplayStudyError::Study)?;
    Ok(sensitivity)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::laboratory::experiment::{Label, Parameters};
    use crate::laboratory::compare::tests::{Flat, OneShare, fill_model, journaled, opening};
    use crate::laboratory::dataset::tests::dataset;
    use crate::laboratory::source::Source;

    fn study(directory: &std::path::Path) -> Study {
        Study::open(
            Label::new("sensitivity check").unwrap(),
            directory,
            Source::new("test.rs", ""),
        )
        .unwrap()
    }

    fn spread(value: f64) -> Perturbation {
        Perturbation::QuotedSpread(BasisPoints::new(value).unwrap())
    }

    fn later(sessions: usize) -> Perturbation {
        Perturbation::LaterStart {
            sessions: NonZeroUsize::new(sessions).unwrap(),
        }
    }

    /// One share bought at session 1's $10 open and marked at its $11 close gains 10% less the crossing, half the
    /// quoted spread: 0.995% at 10bp, 0.985% at 30bp, and -0.5% at 3000bp, each over three sessions against flat.
    #[test]
    fn test_each_perturbation_is_journaled_beside_the_unperturbed_difference() {
        let directory =
            std::env::temp_dir().join(format!("fund-sensitivity-{}", uuid::Uuid::new_v4()));
        let mut study = study(&directory);
        let dataset = dataset(study.run_id());
        let sensitivity = sensitivity(
            &mut study,
            &dataset,
            &Arm::new(OneShare, "one_share", &Parameters::default()).unwrap(),
            &Arm::new(Flat, "flat", &Parameters::default()).unwrap(),
            fill_model(),
            BarInterval::OneDay,
            opening(),
            Controls::Unrestrained,
            &[spread(30.0), spread(3000.0)],
        )
        .unwrap();
        let ran = journaled(&directory);
        std::fs::remove_dir_all(&directory).unwrap();
        let estimates: Vec<_> = ran
            .estimates()
            .iter()
            .map(|(name, estimate)| (name.as_str(), estimate.mean(), estimate.sessions()))
            .collect();
        assert_eq!(estimates.len(), 3);
        for ((name, mean, sessions), expected) in estimates.iter().zip([
            ("session_return_difference", 0.00995 / 3.0),
            (
                "session_return_difference_quoted_spread_3000bp",
                -0.005 / 3.0,
            ),
            (
                "session_return_difference_quoted_spread_30bp",
                0.00985 / 3.0,
            ),
        ]) {
            assert_eq!(*name, expected.0);
            assert!((mean - expected.1).abs() < 1e-15, "{name} mean {mean}");
            assert_eq!(*sessions, 3, "{name}");
        }
        let turns: Vec<_> = ran
            .metrics()
            .iter()
            .map(|(name, value)| (name.as_str(), value.value()))
            .collect();
        assert_eq!(turns, [("sign_turns", 1.0)]);
        assert_eq!(sensitivity.sign_turns(), 1);
        let perturbations = ran
            .parameters()
            .settings()
            .iter()
            .find(|(name, _)| name.as_str() == "perturbations")
            .map(|(_, value)| value.as_str());
        assert_eq!(
            perturbations,
            Some("quoted_spread_30bp,quoted_spread_3000bp")
        );
    }

    /// Leaving out session 0 moves the decision to session 1's first bar, which fills at its second bar's $10 open:
    /// the same 0.995% gain, now one session in two rather than one in three.
    #[test]
    fn test_a_later_start_replays_only_the_sessions_after_it() {
        let directory =
            std::env::temp_dir().join(format!("fund-sensitivity-{}", uuid::Uuid::new_v4()));
        let mut study = study(&directory);
        let dataset = dataset(study.run_id());
        let sensitivity = sensitivity(
            &mut study,
            &dataset,
            &Arm::new(OneShare, "one_share", &Parameters::default()).unwrap(),
            &Arm::new(Flat, "flat", &Parameters::default()).unwrap(),
            fill_model(),
            BarInterval::OneDay,
            opening(),
            Controls::Unrestrained,
            &[later(1)],
        )
        .unwrap();
        std::fs::remove_dir_all(&directory).unwrap();
        let [(perturbation, estimate)] = sensitivity.perturbed() else {
            panic!("one perturbation: {:?}", sensitivity.perturbed());
        };
        assert_eq!(*perturbation, later(1));
        assert_eq!(estimate.sessions(), 2);
        assert!(
            (estimate.mean() - 0.00995 / 2.0).abs() < 1e-15,
            "{}",
            estimate.mean()
        );
    }

    /// A strategy against itself differs by exactly nothing under every perturbation, so nothing turns.
    #[test]
    fn test_a_strategy_against_itself_turns_nowhere() {
        let directory =
            std::env::temp_dir().join(format!("fund-sensitivity-{}", uuid::Uuid::new_v4()));
        let mut study = study(&directory);
        let dataset = dataset(study.run_id());
        let arm = Arm::new(OneShare, "one_share", &Parameters::default()).unwrap();
        let sensitivity = sensitivity(
            &mut study,
            &dataset,
            &arm,
            &arm,
            fill_model(),
            BarInterval::OneDay,
            opening(),
            Controls::Unrestrained,
            &[later(1), spread(3000.0)],
        )
        .unwrap();
        std::fs::remove_dir_all(&directory).unwrap();
        let means: Vec<_> = std::iter::once(sensitivity.unperturbed())
            .chain(
                sensitivity
                    .perturbed()
                    .iter()
                    .map(|(_, estimate)| *estimate),
            )
            .map(|estimate| (estimate.mean(), estimate.standard_error()))
            .collect();
        assert_eq!(means, [(0.0, 0.0), (0.0, 0.0), (0.0, 0.0)]);
        assert_eq!(sensitivity.sign_turns(), 0);
    }

    /// A perturbation declared twice is refused before anything replays, so a start past every session never runs.
    #[test]
    fn test_a_repeated_perturbation_is_refused() {
        let directory =
            std::env::temp_dir().join(format!("fund-sensitivity-{}", uuid::Uuid::new_v4()));
        let mut study = study(&directory);
        let dataset = dataset(study.run_id());
        let refused = sensitivity(
            &mut study,
            &dataset,
            &Arm::new(OneShare, "one_share", &Parameters::default()).unwrap(),
            &Arm::new(Flat, "flat", &Parameters::default()).unwrap(),
            fill_model(),
            BarInterval::OneDay,
            opening(),
            Controls::Unrestrained,
            &[later(3), spread(30.0), later(3)],
        );
        let _ = std::fs::remove_dir_all(&directory);
        let Err(ReplayStudyError::Experiment(ExperimentRefusal::Duplicate { name })) = refused
        else {
            panic!("not refused as a duplicate: {refused:?}");
        };
        assert_eq!(name, "session_return_difference_later_start_3_sessions");
    }

    /// Signed zeros are one spread, so declaring both is refused as a repeat under the zero spread's name.
    #[test]
    fn test_signed_zero_spreads_are_one_perturbation() {
        let directory =
            std::env::temp_dir().join(format!("fund-sensitivity-{}", uuid::Uuid::new_v4()));
        let mut study = study(&directory);
        let dataset = dataset(study.run_id());
        let refused = sensitivity(
            &mut study,
            &dataset,
            &Arm::new(OneShare, "one_share", &Parameters::default()).unwrap(),
            &Arm::new(Flat, "flat", &Parameters::default()).unwrap(),
            fill_model(),
            BarInterval::OneDay,
            opening(),
            Controls::Unrestrained,
            &[later(3), spread(0.0), spread(-0.0)],
        );
        let _ = std::fs::remove_dir_all(&directory);
        let Err(ReplayStudyError::Experiment(ExperimentRefusal::Duplicate { name })) = refused
        else {
            panic!("not refused as a duplicate: {refused:?}");
        };
        assert_eq!(name, "session_return_difference_quoted_spread_0bp");
    }

    /// A spread equal to the comparison's own is refused before anything replays, so a start past every session never
    /// runs.
    #[test]
    fn test_the_comparisons_own_spread_is_refused() {
        let directory =
            std::env::temp_dir().join(format!("fund-sensitivity-{}", uuid::Uuid::new_v4()));
        let mut study = study(&directory);
        let dataset = dataset(study.run_id());
        let refused = sensitivity(
            &mut study,
            &dataset,
            &Arm::new(OneShare, "one_share", &Parameters::default()).unwrap(),
            &Arm::new(Flat, "flat", &Parameters::default()).unwrap(),
            fill_model(),
            BarInterval::OneDay,
            opening(),
            Controls::Unrestrained,
            &[later(3), spread(10.0)],
        );
        let _ = std::fs::remove_dir_all(&directory);
        let Err(ReplayStudyError::UnperturbedSpread { quoted_spread }) = refused else {
            panic!("not refused as unperturbed: {refused:?}");
        };
        assert_eq!(quoted_spread.value(), 10.0);
    }

    /// A later start past every session leaves nothing to decide on, and is refused rather than read as no effect.
    #[test]
    fn test_a_later_start_past_every_session_is_refused() {
        let directory =
            std::env::temp_dir().join(format!("fund-sensitivity-{}", uuid::Uuid::new_v4()));
        let mut study = study(&directory);
        let dataset = dataset(study.run_id());
        let refused = sensitivity(
            &mut study,
            &dataset,
            &Arm::new(OneShare, "one_share", &Parameters::default()).unwrap(),
            &Arm::new(Flat, "flat", &Parameters::default()).unwrap(),
            fill_model(),
            BarInterval::OneDay,
            opening(),
            Controls::Unrestrained,
            &[later(3)],
        );
        let _ = std::fs::remove_dir_all(&directory);
        assert!(matches!(
            refused,
            Err(ReplayStudyError::NoDecisionBars {
                decision: BarInterval::OneDay
            })
        ));
    }
}
