//! An ensemble of trees whose score is the sum of theirs, and the booster that fits one by descending a loss.

use std::num::NonZeroUsize;

use crate::common::laboratory::permutation::Generator;
use crate::common::learning::boosting::bins::Bins;
use crate::common::learning::boosting::histogram::{DerivativeError, Statistics};
use crate::common::learning::boosting::tree::{Grower, Node, Tree};
use crate::common::learning::boosting::{Fraction, Penalty};
use crate::common::learning::loss::{Label, Loss, Prediction, Score};
use crate::common::learning::matrix::{Input, Matrix, Sample};
use crate::common::monoid::{Fold, Monoid, Sequence};

/// Trees in the order they were fit; combining concatenates, and a combination scores the sum of the parts' scores.
#[derive(Debug, Clone, PartialEq, Default, serde::Serialize, serde::Deserialize)]
#[serde(transparent)]
pub struct Ensemble(Vec<Tree>);

impl Ensemble {
    pub fn trees(&self) -> &[Tree] {
        &self.0
    }

    pub fn score(&self, row: &[f64]) -> Score {
        self.score_from(Score::default(), row)
    }

    /// Adds each tree's score to `base` in fit order, which rounds as the fit's running scores did.
    pub fn score_from(&self, base: Score, row: &[f64]) -> Score {
        self.0
            .iter()
            .fold(base, |score, tree| score + tree.score(row))
    }
}

impl Monoid for Ensemble {
    fn empty() -> Self {
        Self(Vec::new())
    }

    fn combine(mut self, other: Self) -> Self {
        self.0.extend(other.0);
        self
    }
}

/// The free choices of a boosted fit, each typed so that any combination is valid.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Settings {
    pub leaves: NonZeroUsize,
    /// Shrinks every leaf, so each tree corrects only part of what remains.
    pub learning_rate: Fraction,
    pub minimum_rows_per_leaf: NonZeroUsize,
    /// The share of inputs each tree may split on, drawn afresh per tree.
    pub input_fraction: Fraction,
    /// The share of rows each tree is grown from, drawn afresh per tree.
    pub row_fraction: Fraction,
    pub penalty: Penalty,
    pub seed: u64,
}

/// When a fit stops adding trees.
#[derive(Debug, Clone, Copy)]
pub enum Stopping<'a> {
    /// Up to `rounds`, ending once `validation` loss has not improved for `patience` rounds and keeping its best.
    Early {
        validation: &'a Sample<'a>,
        rounds: usize,
        patience: NonZeroUsize,
    },
    /// Exactly `rounds` trees, reading no validation rows.
    Fixed { rounds: usize },
}

#[derive(Debug, Clone, Copy, PartialEq, thiserror::Error)]
pub enum FitError {
    #[error("a fit needs training rows")]
    NoTrainingRows,
    #[error("early stopping needs validation rows")]
    NoValidationRows,
    #[error("training rows are {train} wide and validation rows {validation}")]
    Width { train: usize, validation: usize },
    #[error("the derivative at row {row} is out of range")]
    Derivative {
        row: usize,
        #[source]
        error: DerivativeError,
    },
    #[error("training labels are read under {train:?} and validation labels under {validation:?}")]
    Loss { train: Loss, validation: Loss },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("the booster reads rows {expected} wide, and these are {actual}")]
pub struct WidthError {
    pub expected: usize,
    pub actual: usize,
}

/// A fitted model: the loss it descended, the constant it starts from, and the trees that correct it.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(try_from = "Parts", into = "Parts")]
pub struct Booster {
    loss: Loss,
    width: usize,
    base: Score,
    ensemble: Ensemble,
}

impl Booster {
    /// Adds one tree per round until `stopping` ends the fit.
    pub fn fit(
        train: &Sample,
        stopping: Stopping,
        settings: &Settings,
        fold: &impl Fold,
    ) -> Result<Self, FitError> {
        let (matrix, labels, loss) = (train.matrix(), train.labels(), train.loss());
        if matrix.rows() == 0 {
            return Err(FitError::NoTrainingRows);
        }
        let base = loss.base(labels);
        let (rounds, mut watch) = match stopping {
            Stopping::Early {
                validation,
                rounds,
                patience,
            } => {
                if loss != validation.loss() {
                    return Err(FitError::Loss {
                        train: loss,
                        validation: validation.loss(),
                    });
                }
                if validation.matrix().rows() == 0 {
                    return Err(FitError::NoValidationRows);
                }
                if matrix.width() != validation.matrix().width() {
                    return Err(FitError::Width {
                        train: matrix.width(),
                        validation: validation.matrix().width(),
                    });
                }
                let scores = vec![base; validation.matrix().rows()];
                let best = (mean_loss(loss, &scores, validation.labels()), 0);
                (
                    rounds,
                    Some(Watch {
                        validation,
                        patience,
                        scores,
                        best,
                    }),
                )
            }
            Stopping::Fixed { rounds } => (rounds, None),
        };
        let bins = Bins::fit(&matrix);
        let codes = bins.quantize(&matrix);
        let mut scores = vec![base; matrix.rows()];
        let mut generator = Generator::new(settings.seed);
        let mut trees = Vec::new();
        for round in 1..=rounds {
            let statistics: Vec<Statistics> = scores
                .iter()
                .zip(labels)
                .enumerate()
                .map(|(row, (&score, &label))| {
                    Statistics::of(loss.derivatives(score, label))
                        .map_err(|error| FitError::Derivative { row, error })
                })
                .collect::<Result<_, _>>()?;
            let rows = draw(&mut generator, matrix.rows(), settings.row_fraction);
            let inputs: Vec<Input> = draw(&mut generator, matrix.width(), settings.input_fraction)
                .into_iter()
                .map(Input::new)
                .collect();
            let grower = Grower {
                bins: &bins,
                codes: &codes,
                statistics: &statistics,
                leaves: settings.leaves,
                minimum_rows_per_leaf: settings.minimum_rows_per_leaf,
                penalty: settings.penalty,
            };
            let tree = grower.grow(rows, &inputs, fold, |rows, statistics| {
                let step = loss
                    .renewed(rows.iter().map(|&row| (scores[row], labels[row])))
                    .unwrap_or_else(|| {
                        Score::new(
                            -statistics.gradient()
                                / (statistics.hessian() + settings.penalty.get()),
                        )
                    });
                Score::new(step.get() * settings.learning_rate.get())
            });
            add_scores(&mut scores, &tree, &matrix, fold);
            let stop = match &mut watch {
                Some(watch) => watch.stops_after(&tree, round, loss, fold),
                None => false,
            };
            trees.push(tree);
            if stop {
                break;
            }
        }
        if let Some(watch) = watch {
            trees.truncate(watch.best.1);
        }
        Ok(Self {
            loss,
            width: matrix.width(),
            base,
            ensemble: Ensemble(trees),
        })
    }

    /// Each row's prediction on the label's scale: a probability for `Logistic`.
    pub fn predict(&self, matrix: &Matrix) -> Result<Vec<Prediction>, WidthError> {
        if matrix.width() != self.width {
            return Err(WidthError {
                expected: self.width,
                actual: matrix.width(),
            });
        }
        Ok((0..matrix.rows())
            .map(|row| {
                self.loss
                    .response(self.ensemble.score_from(self.base, matrix.row(row)))
            })
            .collect())
    }

    /// The trees kept, which early stopping may leave below the rounds asked for.
    pub fn rounds(&self) -> usize {
        self.ensemble.trees().len()
    }

    /// Each input's summed split gain across every tree.
    pub fn importance(&self) -> Vec<f64> {
        let mut importance = vec![0.0; self.width];
        for node in self.ensemble.trees().iter().flat_map(Tree::nodes) {
            match node {
                Node::Split { input, gain, .. } => importance[input.index()] += gain,
                Node::Leaf { .. } => {}
            }
        }
        importance
    }
}

/// A booster as stored, checked on the way in so no split reads past the width.
#[derive(serde::Serialize, serde::Deserialize)]
struct Parts {
    loss: Loss,
    width: usize,
    base: Score,
    ensemble: Ensemble,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("a split reads input {} of rows {width} wide", input.index())]
pub struct InputError {
    pub input: Input,
    pub width: usize,
}

impl TryFrom<Parts> for Booster {
    type Error = InputError;

    fn try_from(
        Parts {
            loss,
            width,
            base,
            ensemble,
        }: Parts,
    ) -> Result<Self, Self::Error> {
        for node in ensemble.trees().iter().flat_map(Tree::nodes) {
            match node {
                Node::Split { input, .. } if input.index() >= width => {
                    return Err(InputError {
                        input: *input,
                        width,
                    });
                }
                Node::Split { .. } | Node::Leaf { .. } => {}
            }
        }
        Ok(Self {
            loss,
            width,
            base,
            ensemble,
        })
    }
}

impl From<Booster> for Parts {
    fn from(
        Booster {
            loss,
            width,
            base,
            ensemble,
        }: Booster,
    ) -> Self {
        Self {
            loss,
            width,
            base,
            ensemble,
        }
    }
}

fn add_scores(scores: &mut [Score], tree: &Tree, matrix: &Matrix, fold: &impl Fold) {
    let Sequence(added) = fold.fold(matrix.rows(), |range| {
        Sequence(range.map(|row| tree.score(matrix.row(row))).collect())
    });
    for (score, added) in scores.iter_mut().zip(added) {
        *score = *score + added;
    }
}

fn mean_loss(loss: Loss, scores: &[Score], labels: &[Label]) -> f64 {
    scores
        .iter()
        .zip(labels)
        .map(|(&score, &label)| loss.value(score, label))
        .sum::<f64>()
        / labels.len() as f64
}

/// Early stopping's running state: the validation scores and the best loss with the round that reached it.
struct Watch<'a> {
    validation: &'a Sample<'a>,
    patience: NonZeroUsize,
    scores: Vec<Score>,
    best: (f64, usize),
}

impl Watch<'_> {
    /// Adds `tree` to the validation scores and says whether `patience` rounds have now passed without a better loss.
    fn stops_after(&mut self, tree: &Tree, round: usize, loss: Loss, fold: &impl Fold) -> bool {
        add_scores(&mut self.scores, tree, &self.validation.matrix(), fold);
        let current = mean_loss(loss, &self.scores, self.validation.labels());
        if current < self.best.0 {
            self.best = (current, round);
        }
        round - self.best.1 >= self.patience.get()
    }
}

/// The sorted indices of a uniform draw of `fraction` of `population`, drawing nothing when it keeps everything.
fn draw(generator: &mut Generator, population: usize, fraction: Fraction) -> Vec<usize> {
    let mut indices: Vec<usize> = (0..population).collect();
    let kept = fraction.of(population);
    if kept < population {
        generator.shuffle(&mut indices);
        indices.truncate(kept);
        indices.sort_unstable();
    }
    indices
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;
    use crate::common::learning::loss::Level;
    use crate::common::monoid::{Sequential, laws};

    fn settings() -> Settings {
        Settings {
            leaves: NonZeroUsize::new(4).unwrap(),
            learning_rate: Fraction::new(0.1).unwrap(),
            minimum_rows_per_leaf: NonZeroUsize::new(5).unwrap(),
            input_fraction: Fraction::new(1.0).unwrap(),
            row_fraction: Fraction::new(0.8).unwrap(),
            penalty: Penalty::new(1.0).unwrap(),
            seed: 7,
        }
    }

    fn fit(
        loss: Loss,
        values: &[f64],
        width: usize,
        labels: &[f64],
        settings: &Settings,
        rounds: usize,
    ) -> Result<Booster, FitError> {
        let matrix = Matrix::new(values, NonZeroUsize::new(width).unwrap()).unwrap();
        let sample = Sample::new(matrix, labels, loss).unwrap();
        Booster::fit(&sample, Stopping::Fixed { rounds }, settings, &Sequential)
    }

    fn early<'a>(validation: &'a Sample<'a>) -> Stopping<'a> {
        Stopping::Early {
            validation,
            rounds: 200,
            patience: NonZeroUsize::new(20).unwrap(),
        }
    }

    fn predicted(booster: &Booster, matrix: &Matrix) -> Vec<f64> {
        booster
            .predict(matrix)
            .unwrap()
            .into_iter()
            .map(Prediction::get)
            .collect()
    }

    fn mean_squared_error(predictions: &[f64], labels: &[f64]) -> f64 {
        predictions
            .iter()
            .zip(labels)
            .map(|(prediction, label)| (prediction - label).powi(2))
            .sum::<f64>()
            / labels.len() as f64
    }

    /// Two inputs, the first carrying a step in the label and the second pure noise, with noise in the label.
    fn stepped(rows: usize, seed: u64) -> (Vec<f64>, Vec<f64>) {
        let mut generator = Generator::new(seed);
        let mut uniform = || (generator.next_u64() >> 11) as f64 / (1_u64 << 53) as f64 - 0.5;
        let mut values = Vec::with_capacity(rows * 2);
        let mut labels = Vec::with_capacity(rows);
        for _ in 0..rows {
            let (signal, noise) = (uniform(), uniform());
            values.extend([signal, noise]);
            labels.push(if signal > 0.0 { 1.0 } else { -1.0 } + 0.2 * uniform());
        }
        (values, labels)
    }

    /// One leaf per tree, with dyadic values so every sum of scores is exact.
    fn any_ensemble() -> impl Strategy<Value = Ensemble> {
        prop::collection::vec(-64_i32..64, 0..5).prop_map(|values| {
            Ensemble(
                values
                    .into_iter()
                    .map(|value| Tree::leaf(Score::new(f64::from(value) / 8.0)))
                    .collect(),
            )
        })
    }

    #[test]
    fn test_one_round_of_full_shrinkage_recovers_a_step() {
        let values: Vec<f64> = (1..=8).map(f64::from).collect();
        let labels = [0.0, 0.0, 0.0, 0.0, 10.0, 10.0, 10.0, 10.0];
        let settings = Settings {
            leaves: NonZeroUsize::new(2).unwrap(),
            learning_rate: Fraction::new(1.0).unwrap(),
            minimum_rows_per_leaf: NonZeroUsize::MIN,
            row_fraction: Fraction::new(1.0).unwrap(),
            penalty: Penalty::new(1e-9).unwrap(),
            ..settings()
        };
        let booster = fit(Loss::SquaredError, &values, 1, &labels, &settings, 1).unwrap();
        let matrix = Matrix::new(&values, NonZeroUsize::MIN).unwrap();
        let predictions = predicted(&booster, &matrix);
        for (prediction, label) in predictions.iter().zip(labels) {
            assert!(
                (prediction - label).abs() < 1e-6,
                "{prediction} against {label}"
            );
        }
        assert_eq!(booster.rounds(), 1);
        assert!((booster.importance()[0] - 200.0).abs() < 1e-6);
    }

    /// Shrinkage takes half the Newton step, so one round lands halfway from the mean to each side of the step.
    #[test]
    fn test_the_learning_rate_shrinks_each_leaf() {
        let values: Vec<f64> = (1..=8).map(f64::from).collect();
        let labels = [0.0, 0.0, 0.0, 0.0, 10.0, 10.0, 10.0, 10.0];
        let settings = Settings {
            leaves: NonZeroUsize::new(2).unwrap(),
            learning_rate: Fraction::new(0.5).unwrap(),
            minimum_rows_per_leaf: NonZeroUsize::MIN,
            row_fraction: Fraction::new(1.0).unwrap(),
            penalty: Penalty::new(1e-9).unwrap(),
            ..settings()
        };
        let booster = fit(Loss::SquaredError, &values, 1, &labels, &settings, 1).unwrap();
        let predictions = predicted(&booster, &Matrix::new(&values, NonZeroUsize::MIN).unwrap());
        for (prediction, expected) in predictions
            .iter()
            .zip([2.5, 2.5, 2.5, 2.5, 7.5, 7.5, 7.5, 7.5])
        {
            assert!(
                (prediction - expected).abs() < 1e-6,
                "{prediction} against {expected}"
            );
        }
    }

    /// The penalty joins each side's hessian, so a split's gain is 2 · 20² / (4 + 2) rather than the unpenalized 200.
    #[test]
    fn test_the_penalty_discounts_a_split_gain() {
        let values: Vec<f64> = (1..=8).map(f64::from).collect();
        let labels = [0.0, 0.0, 0.0, 0.0, 10.0, 10.0, 10.0, 10.0];
        let settings = Settings {
            leaves: NonZeroUsize::new(2).unwrap(),
            minimum_rows_per_leaf: NonZeroUsize::MIN,
            row_fraction: Fraction::new(1.0).unwrap(),
            penalty: Penalty::new(2.0).unwrap(),
            ..settings()
        };
        let booster = fit(Loss::SquaredError, &values, 1, &labels, &settings, 1).unwrap();
        assert!(
            (booster.importance()[0] - 800.0 / 6.0).abs() < 1e-6,
            "{:?}",
            booster.importance()
        );
    }

    #[test]
    fn test_a_draw_that_keeps_everything_consumes_no_randomness() {
        let mut generator = Generator::new(9);
        assert_eq!(
            draw(&mut generator, 5, Fraction::new(1.0).unwrap()),
            [0, 1, 2, 3, 4]
        );
        assert_eq!(generator, Generator::new(9));
        let drawn = draw(&mut generator, 5, Fraction::new(0.4).unwrap());
        assert_eq!(drawn.len(), 2);
        assert!(drawn.windows(2).all(|pair| pair[0] < pair[1]));
        assert_ne!(generator, Generator::new(9));
    }

    #[test]
    fn test_constant_labels_predict_the_constant_exactly() {
        let values: Vec<f64> = (0..40).map(f64::from).collect();
        for loss in [
            Loss::SquaredError,
            Loss::Quantile(Level::try_from(0.9).unwrap()),
        ] {
            let booster = fit(loss, &values, 1, &[2.5; 40], &settings(), 200).unwrap();
            let matrix = Matrix::new(&values, NonZeroUsize::MIN).unwrap();
            assert!(
                predicted(&booster, &matrix)
                    .iter()
                    .all(|&prediction| prediction == 2.5),
                "{loss:?}"
            );
            assert_eq!(booster.importance(), [0.0]);
        }
    }

    #[test]
    fn test_zero_rounds_predict_the_base_on_the_response_scale() {
        let values = [0.0, 1.0, 2.0, 3.0];
        let booster = fit(
            Loss::Logistic,
            &values,
            1,
            &[0.0, 0.0, 0.0, 1.0],
            &settings(),
            0,
        )
        .unwrap();
        let matrix = Matrix::new(&values, NonZeroUsize::MIN).unwrap();
        assert!(
            predicted(&booster, &matrix)
                .iter()
                .all(|prediction| (prediction - 0.25).abs() < 1e-12)
        );
        assert_eq!(booster.rounds(), 0);
    }

    #[test]
    fn test_a_fit_learns_the_step_and_credits_the_input_that_carries_it() {
        let (values, labels) = stepped(2_000, 3);
        let booster = fit(Loss::SquaredError, &values, 2, &labels, &settings(), 200).unwrap();
        let (validation_values, validation_labels) = stepped(1_000, 4);
        let matrix = Matrix::new(&validation_values, NonZeroUsize::new(2).unwrap()).unwrap();
        let predictions = predicted(&booster, &matrix);
        assert!(mean_squared_error(&predictions, &validation_labels) < 0.1);
        let importance = booster.importance();
        assert!(importance[0] > 20.0 * importance[1], "{importance:?}");
        let classifier = fit(
            Loss::Logistic,
            &values,
            2,
            &labels
                .iter()
                .map(|&label| f64::from(u8::from(label > 0.0)))
                .collect::<Vec<_>>(),
            &settings(),
            200,
        )
        .unwrap();
        let probabilities = predicted(&classifier, &matrix);
        assert!(
            probabilities
                .iter()
                .all(|&probability| probability > 0.0 && probability < 1.0)
        );
        let wrong = probabilities
            .iter()
            .zip(&validation_labels)
            .filter(|&(&probability, &label)| (probability > 0.5) != (label > 0.0))
            .count();
        assert!(wrong < 30, "{wrong} of 1000 misclassified");
    }

    /// The same pipeline on labels that cannot be predicted: validation loss barely moves off the base, and early
    /// stopping keeps few rounds.
    #[test]
    fn test_a_fit_on_shuffled_labels_finds_nothing() {
        let (values, mut labels) = stepped(2_000, 5);
        Generator::new(6).shuffle(&mut labels);
        let (train_values, validation_values) = values.split_at(2_400);
        let (train_labels, validation_labels) = labels.split_at(1_200);
        let width = NonZeroUsize::new(2).unwrap();
        let loss = Loss::SquaredError;
        let train = Sample::new(
            Matrix::new(train_values, width).unwrap(),
            train_labels,
            loss,
        )
        .unwrap();
        let validation = Sample::new(
            Matrix::new(validation_values, width).unwrap(),
            validation_labels,
            loss,
        )
        .unwrap();
        let booster = Booster::fit(&train, early(&validation), &settings(), &Sequential).unwrap();
        let base = mean_squared_error(&[loss.base(train.labels()).get(); 800], validation_labels);
        let fitted = mean_squared_error(
            &predicted(&booster, &validation.matrix()),
            validation_labels,
        );
        assert!(fitted > 0.99 * base, "{fitted} against a base of {base}");
        assert!(booster.rounds() < 20, "{} rounds", booster.rounds());
    }

    /// A walk-forward refit relies on this: the trees early stopping keeps are the ones a fixed fit of that many grows.
    #[test]
    fn test_an_early_fit_equals_a_fixed_fit_of_the_rounds_it_kept() {
        let (values, mut labels) = stepped(2_000, 11);
        let mut generator = Generator::new(12);
        for label in &mut labels {
            *label += 6.0 * ((generator.next_u64() >> 11) as f64 / (1_u64 << 53) as f64 - 0.5);
        }
        let (train_values, validation_values) = values.split_at(2_400);
        let (train_labels, validation_labels) = labels.split_at(1_200);
        let width = NonZeroUsize::new(2).unwrap();
        let loss = Loss::SquaredError;
        let train = Sample::new(
            Matrix::new(train_values, width).unwrap(),
            train_labels,
            loss,
        )
        .unwrap();
        let validation = Sample::new(
            Matrix::new(validation_values, width).unwrap(),
            validation_labels,
            loss,
        )
        .unwrap();
        let stopped = Booster::fit(&train, early(&validation), &settings(), &Sequential).unwrap();
        let rounds = stopped.rounds();
        assert!(rounds > 0 && rounds < 200, "{rounds} rounds");
        let validation_loss = |rounds| {
            let booster =
                Booster::fit(&train, Stopping::Fixed { rounds }, &settings(), &Sequential).unwrap();
            mean_squared_error(
                &predicted(&booster, &validation.matrix()),
                validation_labels,
            )
        };
        let losses: Vec<f64> = (0..=rounds + 20).map(validation_loss).collect();
        let best = (0..losses.len())
            .min_by(|&a, &b| losses[a].total_cmp(&losses[b]))
            .unwrap();
        assert_eq!(best, rounds, "{losses:?}");
        let fixed =
            Booster::fit(&train, Stopping::Fixed { rounds }, &settings(), &Sequential).unwrap();
        assert_eq!(fixed, stopped);
        let longer = Booster::fit(
            &train,
            Stopping::Fixed { rounds: 200 },
            &settings(),
            &Sequential,
        )
        .unwrap();
        assert_eq!(longer.rounds(), 200);
    }

    #[test]
    fn test_a_fit_refuses_mismatched_or_unusable_samples() {
        let values = [0.0, 1.0, 2.0, 3.0];
        let matrix = Matrix::new(&values, NonZeroUsize::MIN).unwrap();
        let [squared, logistic] = [Loss::SquaredError, Loss::Logistic]
            .map(|loss| Sample::new(matrix, &[0.0, 1.0, 1.0, 0.0], loss).unwrap());
        assert_eq!(
            Booster::fit(&squared, early(&logistic), &settings(), &Sequential),
            Err(FitError::Loss {
                train: Loss::SquaredError,
                validation: Loss::Logistic
            })
        );
        assert_eq!(
            fit(
                Loss::SquaredError,
                &values[..3],
                1,
                &[0.0, 0.0, 1.2e16],
                &settings(),
                200
            ),
            Err(FitError::Derivative {
                row: 2,
                error: DerivativeError { value: -8e15 }
            })
        );
        assert_eq!(
            fit(Loss::SquaredError, &[], 1, &[], &settings(), 200),
            Err(FitError::NoTrainingRows)
        );
        let empty = Sample::new(
            Matrix::new(&[], NonZeroUsize::MIN).unwrap(),
            &[],
            Loss::SquaredError,
        )
        .unwrap();
        assert_eq!(
            Booster::fit(&squared, early(&empty), &settings(), &Sequential),
            Err(FitError::NoValidationRows)
        );
        let train = Sample::new(matrix, &[0.0; 4], Loss::SquaredError).unwrap();
        let validation = Sample::new(
            Matrix::new(&values, NonZeroUsize::new(2).unwrap()).unwrap(),
            &[0.0; 2],
            Loss::SquaredError,
        )
        .unwrap();
        assert_eq!(
            Booster::fit(&train, early(&validation), &settings(), &Sequential),
            Err(FitError::Width {
                train: 1,
                validation: 2
            })
        );
        let booster = fit(Loss::SquaredError, &values, 1, &[0.0; 4], &settings(), 200).unwrap();
        assert_eq!(
            booster.predict(&validation.matrix()),
            Err(WidthError {
                expected: 1,
                actual: 2
            })
        );
    }

    #[test]
    fn test_a_booster_reads_back_as_itself_and_refuses_a_split_past_its_width() {
        let (values, labels) = stepped(300, 8);
        let booster = fit(
            Loss::Quantile(Level::try_from(0.1).unwrap()),
            &values,
            2,
            &labels,
            &settings(),
            200,
        )
        .unwrap();
        assert!(booster.rounds() > 0);
        let text = serde_json::to_string(&booster).unwrap();
        let read = serde_json::from_str::<Booster>(&text).unwrap();
        assert_eq!(read, booster);
        let matrix = Matrix::new(&values, NonZeroUsize::new(2).unwrap()).unwrap();
        assert_eq!(read.predict(&matrix), booster.predict(&matrix));
        let stored = r#"{"loss":"SquaredError","width":1,"base":0.5,"ensemble":[[{"Split":{"input":0,"threshold":1.5,"missing_left":true,"gain":2.0,"left":1,"right":2}},{"Leaf":{"value":-0.25}},{"Leaf":{"value":0.25}}]]}"#;
        let read = serde_json::from_str::<Booster>(stored).unwrap();
        assert_eq!(serde_json::to_string(&read).unwrap(), stored);
        let matrix = Matrix::new(&[1.0, 2.0], NonZeroUsize::MIN).unwrap();
        assert_eq!(predicted(&read, &matrix), [0.25, 0.75]);
        let narrowed = text.replacen(r#""width":2"#, r#""width":1"#, 1);
        assert!(
            serde_json::from_str::<Booster>(&narrowed)
                .unwrap_err()
                .to_string()
                .contains("of rows 1 wide")
        );
    }

    /// A split below every finite value stores a threshold of -inf, which JSON cannot hold as a number.
    #[test]
    fn test_a_split_at_negative_infinity_survives_a_round_trip() {
        let values = [f64::NEG_INFINITY, f64::NEG_INFINITY, 1.0, 1.0];
        let settings = Settings {
            leaves: NonZeroUsize::new(2).unwrap(),
            learning_rate: Fraction::new(1.0).unwrap(),
            minimum_rows_per_leaf: NonZeroUsize::MIN,
            row_fraction: Fraction::new(1.0).unwrap(),
            penalty: Penalty::new(1e-9).unwrap(),
            ..settings()
        };
        let booster = fit(
            Loss::SquaredError,
            &values,
            1,
            &[0.0, 0.0, 10.0, 10.0],
            &settings,
            1,
        )
        .unwrap();
        let read: Booster =
            serde_json::from_str(&serde_json::to_string(&booster).unwrap()).unwrap();
        let matrix = Matrix::new(&values, NonZeroUsize::MIN).unwrap();
        for predictions in [predicted(&booster, &matrix), predicted(&read, &matrix)] {
            let rounded: Vec<f64> = predictions.iter().map(|value| value.round()).collect();
            assert_eq!(rounded, [0.0, 0.0, 10.0, 10.0]);
        }
    }

    /// Small corrections to a large base round differently summed among themselves first, so prediction adds each
    /// tree to the base in the order the fit did.
    #[test]
    fn test_a_prediction_adds_trees_to_the_base_in_fit_order() {
        let values: Vec<f64> = (1..=8).map(f64::from).collect();
        let labels = [
            1e16 - 16.0,
            1e16 - 16.0,
            1e16 - 16.0,
            1e16 - 16.0,
            1e16 + 16.0,
            1e16 + 16.0,
            1e16 + 16.0,
            1e16 + 16.0,
        ];
        let settings = Settings {
            leaves: NonZeroUsize::new(2).unwrap(),
            minimum_rows_per_leaf: NonZeroUsize::MIN,
            row_fraction: Fraction::new(1.0).unwrap(),
            ..settings()
        };
        let booster = fit(Loss::SquaredError, &values, 1, &labels, &settings, 2).unwrap();
        let offsets: Vec<f64> =
            predicted(&booster, &Matrix::new(&values, NonZeroUsize::MIN).unwrap())
                .iter()
                .map(|prediction| prediction - 1e16)
                .collect();
        assert_eq!(booster.rounds(), 2);
        assert_eq!(offsets, [-4.0, -4.0, -4.0, -4.0, 4.0, 4.0, 4.0, 4.0]);
    }

    proptest! {
        #[test]
        fn property_an_ensemble_is_a_monoid_and_its_score_a_homomorphism(
            first in any_ensemble(),
            second in any_ensemble(),
            third in any_ensemble(),
        ) {
            prop_assert_eq!(first.clone().combine(second.clone()).score(&[]), first.score(&[]) + second.score(&[]));
            prop_assert_eq!(Ensemble::empty().score(&[]), Score::new(0.0));
            laws::check_ordered(first, second, third)?;
        }

        /// The fit reads only the order of each input, so an exact increasing map of one leaves every prediction.
        #[test]
        fn property_an_increasing_transform_of_an_input_leaves_the_fit_unchanged(seed in 0_u64..1_000) {
            let (values, labels) = stepped(200, seed);
            let transformed: Vec<f64> = values.chunks(2).flat_map(|row| [row[0] * 4.0 + 1.0, row[1]]).collect();
            let settings = settings();
            let width = NonZeroUsize::new(2).unwrap();
            let original = fit(Loss::SquaredError, &values, 2, &labels, &settings, 20).unwrap();
            let mapped = fit(Loss::SquaredError, &transformed, 2, &labels, &settings, 20).unwrap();
            prop_assert_eq!(
                original.predict(&Matrix::new(&values, width).unwrap()).unwrap(),
                mapped.predict(&Matrix::new(&transformed, width).unwrap()).unwrap()
            );
        }
    }
}
