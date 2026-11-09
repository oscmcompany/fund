//! Losses as functions of a score and a label, with the derivatives every learner descends along.

/// Keeps the logistic base finite when every label agrees.
const LOGISTIC_CLAMP: f64 = 1e-15;

/// A quantile level strictly between zero and one.
#[derive(Debug, Clone, Copy, PartialEq, PartialOrd, serde::Serialize, serde::Deserialize)]
#[serde(try_from = "f64", into = "f64")]
pub struct Level(f64);

#[derive(Debug, Clone, Copy, PartialEq, thiserror::Error)]
#[error("a quantile level lies strictly between 0 and 1, and {0} does not")]
pub struct LevelError(pub f64);

impl TryFrom<f64> for Level {
    type Error = LevelError;

    fn try_from(value: f64) -> Result<Self, Self::Error> {
        if value > 0.0 && value < 1.0 {
            Ok(Self(value))
        } else {
            Err(LevelError(value))
        }
    }
}

impl From<Level> for f64 {
    fn from(level: Level) -> Self {
        level.0
    }
}

/// A model's raw output, on the scale a loss descends along: log-odds for `Logistic`, the label's scale otherwise.
#[derive(
    Debug, Clone, Copy, PartialEq, PartialOrd, Default, serde::Serialize, serde::Deserialize,
)]
#[serde(transparent)]
pub struct Score(f64);

impl Score {
    pub fn new(value: f64) -> Self {
        Self(value)
    }

    pub fn get(self) -> f64 {
        self.0
    }
}

impl std::ops::Add for Score {
    type Output = Self;

    fn add(self, other: Self) -> Self {
        Self(self.0 + other.0)
    }
}

/// A score mapped onto the label's scale by `Loss::response`: a probability for `Logistic`.
#[derive(Debug, Clone, Copy, PartialEq, PartialOrd)]
pub struct Prediction(f64);

impl Prediction {
    pub fn get(self) -> f64 {
        self.0
    }
}

/// A finite target that the loss it was read under accepts, made only by `Loss::label`.
#[derive(Debug, Clone, Copy, PartialEq, PartialOrd)]
pub struct Label(f64);

impl Label {
    pub fn get(self) -> f64 {
        self.0
    }
}

/// The first and second derivatives of a loss with respect to the score at one row.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Derivatives {
    pub gradient: f64,
    pub hessian: f64,
}

/// A label that is not finite, or for `Logistic` is neither 0 nor 1.
#[derive(Debug, Clone, Copy, PartialEq, thiserror::Error)]
#[error("{loss:?} cannot read a label of {value}")]
pub struct LabelError {
    pub loss: Loss,
    pub value: f64,
}

/// What a fit minimizes.
#[derive(Debug, Clone, Copy, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum Loss {
    /// Half the squared error, so the gradient is the residual itself.
    SquaredError,
    /// The pinball loss, whose minimizer is the level's quantile.
    Quantile(Level),
    Logistic,
}

impl Loss {
    pub fn label(self, value: f64) -> Result<Label, LabelError> {
        let readable = match self {
            Loss::SquaredError | Loss::Quantile(_) => value.is_finite(),
            Loss::Logistic => value == 0.0 || value == 1.0,
        };
        match readable {
            true => Ok(Label(value)),
            false => Err(LabelError { loss: self, value }),
        }
    }

    pub fn value(self, Score(score): Score, Label(label): Label) -> f64 {
        match self {
            Loss::SquaredError => (score - label).powi(2) / 2.0,
            Loss::Quantile(Level(level)) => {
                let residual = label - score;
                residual * if residual >= 0.0 { level } else { level - 1.0 }
            }
            // ln(1 + e^s) - y s, with the softplus written so neither tail overflows.
            Loss::Logistic => score.max(0.0) + (-score.abs()).exp().ln_1p() - label * score,
        }
    }

    /// The gradient and hessian of the loss at `score`; the pinball loss has no curvature, so its hessian is one by
    /// convention and `renewed` sets its leaves.
    pub fn derivatives(self, Score(score): Score, Label(label): Label) -> Derivatives {
        match self {
            Loss::SquaredError => Derivatives {
                gradient: score - label,
                hessian: 1.0,
            },
            Loss::Quantile(Level(level)) => Derivatives {
                gradient: if label > score { -level } else { 1.0 - level },
                hessian: 1.0,
            },
            Loss::Logistic => {
                let probability = sigmoid(score);
                Derivatives {
                    gradient: probability - label,
                    hessian: probability * (1.0 - probability),
                }
            }
        }
    }

    pub fn response(self, Score(score): Score) -> Prediction {
        match self {
            Loss::SquaredError | Loss::Quantile(_) => Prediction(score),
            Loss::Logistic => Prediction(sigmoid(score)),
        }
    }

    /// The constant score that minimizes the loss over `labels`, or zero when there are none.
    pub fn base(self, labels: &[Label]) -> Score {
        if labels.is_empty() {
            return Score(0.0);
        }
        let mean = labels.iter().map(|label| label.0).sum::<f64>() / labels.len() as f64;
        match self {
            Loss::SquaredError => Score(mean),
            Loss::Quantile(level) => Score(quantile(
                &mut labels.iter().map(|label| label.0).collect::<Vec<_>>(),
                level,
            )),
            Loss::Logistic => {
                let clamped = mean.clamp(LOGISTIC_CLAMP, 1.0 - LOGISTIC_CLAMP);
                Score((clamped / (1.0 - clamped)).ln())
            }
        }
    }

    /// The score increment that minimizes the loss over `rows`, for a loss whose Newton step says nothing, or `None`
    /// when the Newton step stands or there are no rows.
    pub fn renewed(self, rows: impl IntoIterator<Item = (Score, Label)>) -> Option<Score> {
        match self {
            Loss::SquaredError | Loss::Logistic => None,
            Loss::Quantile(level) => {
                let mut residuals: Vec<f64> = rows
                    .into_iter()
                    .map(|(Score(score), Label(label))| label - score)
                    .collect();
                (!residuals.is_empty()).then(|| Score(quantile(&mut residuals, level)))
            }
        }
    }
}

fn sigmoid(value: f64) -> f64 {
    1.0 / (1.0 + (-value).exp())
}

/// The smallest value with at least `level` of `values` at or below it, an order statistic that minimizes the
/// pinball loss; `values` must not be empty and its order is not kept.
fn quantile(values: &mut [f64], Level(level): Level) -> f64 {
    let rank = ((level * values.len() as f64).ceil() as usize).clamp(1, values.len());
    *values.select_nth_unstable_by(rank - 1, f64::total_cmp).1
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    fn level(value: f64) -> Level {
        Level::try_from(value).unwrap()
    }

    fn labels(loss: Loss, values: &[f64]) -> Vec<Label> {
        values
            .iter()
            .map(|&value| loss.label(value).unwrap())
            .collect()
    }

    fn base(loss: Loss, values: &[f64]) -> f64 {
        loss.base(&labels(loss, values)).get()
    }

    #[test]
    fn test_a_level_lies_strictly_inside_the_unit_interval_and_reads_back() {
        for value in [0.0, 1.0, -0.5, f64::NAN] {
            assert!(Level::try_from(value).is_err(), "{value}");
        }
        let loss = Loss::Quantile(level(0.1));
        let text = serde_json::to_string(&loss).unwrap();
        assert_eq!(text, r#"{"Quantile":0.1}"#);
        assert_eq!(serde_json::from_str::<Loss>(&text).unwrap(), loss);
        assert!(serde_json::from_str::<Loss>(r#"{"Quantile":1.0}"#).is_err());
    }

    #[test]
    fn test_each_base_is_its_loss_minimizer() {
        assert_eq!(base(Loss::SquaredError, &[1.0, 2.0, 6.0]), 3.0);
        assert_eq!(
            base(Loss::Quantile(level(0.1)), &[5.0, 1.0, 3.0, 2.0, 4.0]),
            1.0
        );
        assert_eq!(base(Loss::Quantile(level(0.1)), &[0.0, 10.0]), 0.0);
        assert_eq!(base(Loss::Quantile(level(0.5)), &[4.0, 1.0, 3.0, 2.0]), 2.0);
        assert_eq!(
            base(Loss::Quantile(level(0.25)), &[4.0, 1.0, 3.0, 2.0]),
            1.0
        );
        assert!((base(Loss::Logistic, &[0.0, 0.0, 0.0, 1.0]) - (1.0_f64 / 3.0).ln()).abs() < 1e-12);
        assert!(base(Loss::Logistic, &[1.0, 1.0]).is_finite());
        assert_eq!(base(Loss::SquaredError, &[]), 0.0);
        assert_eq!(
            base(Loss::Quantile(level(0.9)), &[5.0, 1.0, 3.0, 2.0, 4.0]),
            5.0
        );
    }

    #[test]
    fn test_only_the_quantile_loss_renews_its_leaves() {
        let median = Loss::Quantile(level(0.5));
        let rows = [(2.0, 5.0), (1.0, 0.0), (0.0, 1.0)]
            .map(|(score, label)| (Score::new(score), median.label(label).unwrap()));
        assert_eq!(median.renewed(rows), Some(Score::new(1.0)));
        assert_eq!(median.renewed([]), None);
        let row = [(Score::new(0.0), Loss::Logistic.label(1.0).unwrap())];
        assert_eq!(Loss::SquaredError.renewed(row), None);
        assert_eq!(Loss::Logistic.renewed(row), None);
    }

    #[test]
    fn test_each_loss_reads_only_the_labels_it_accepts() {
        assert_eq!(
            Loss::Logistic.label(0.5),
            Err(LabelError {
                loss: Loss::Logistic,
                value: 0.5
            })
        );
        assert_eq!(Loss::Logistic.label(1.0).map(Label::get), Ok(1.0));
        assert_eq!(Loss::SquaredError.label(0.5).map(Label::get), Ok(0.5));
        for value in [f64::INFINITY, f64::NAN] {
            assert!(Loss::Quantile(level(0.5)).label(value).is_err(), "{value}");
            assert!(Loss::SquaredError.label(value).is_err(), "{value}");
        }
    }

    #[test]
    fn test_the_logistic_value_holds_in_both_tails() {
        let [zero, one] = [0.0, 1.0].map(|value| Loss::Logistic.label(value).unwrap());
        assert!((Loss::Logistic.value(Score::new(0.0), one) - 2.0_f64.ln()).abs() < 1e-12);
        assert!((Loss::Logistic.value(Score::new(800.0), zero) - 800.0).abs() < 1e-9);
        assert!(Loss::Logistic.value(Score::new(-800.0), zero) < 1e-300);
        assert_eq!(Loss::Logistic.response(Score::new(0.0)).get(), 0.5);
        assert_eq!(
            Loss::Logistic.derivatives(Score::new(0.0), one),
            Derivatives {
                gradient: -0.5,
                hessian: 0.25
            }
        );
        let Derivatives { gradient, hessian } =
            Loss::Logistic.derivatives(Score::new(3.0_f64.ln()), zero);
        assert!((gradient - 0.75).abs() < 1e-12 && (hessian - 0.1875).abs() < 1e-12);
    }

    proptest! {
        /// The gradient is the slope of the value, measured by a central difference away from the pinball's kink.
        #[test]
        fn property_each_gradient_is_the_slope_of_its_value(
            score in -5.0..5.0_f64,
            label in -5.0..5.0_f64,
            binary in any::<bool>(),
            quantile_level in 0.05..0.95_f64,
        ) {
            prop_assume!((score - label).abs() > 1e-3);
            let step = 1e-6;
            let losses = [
                (Loss::SquaredError, label),
                (Loss::Quantile(level(quantile_level)), label),
                (Loss::Logistic, f64::from(u8::from(binary))),
            ];
            for (loss, label) in losses {
                let label = loss.label(label).unwrap();
                let value = |shifted: f64| loss.value(Score::new(shifted), label);
                let slope = (value(score + step) - value(score - step)) / (2.0 * step);
                let derivatives = loss.derivatives(Score::new(score), label);
                prop_assert!((slope - derivatives.gradient).abs() < 1e-5, "{loss:?}: {slope} against {derivatives:?}");
                prop_assert!(derivatives.hessian >= 0.0);
            }
        }

        /// The pinball loss is piecewise linear with kinks at the labels, so no label can beat its minimizer.
        #[test]
        fn property_the_quantile_base_minimizes_the_pinball_loss(
            integers in prop::collection::vec(-20_i32..20, 1..30),
            quantile_level in 0.01..0.99_f64,
        ) {
            let loss = Loss::Quantile(level(quantile_level));
            let values: Vec<f64> = integers.into_iter().map(f64::from).collect();
            let labels = labels(loss, &values);
            let total = |score: Score| labels.iter().map(|&label| loss.value(score, label)).sum::<f64>();
            let best = total(loss.base(&labels));
            for &candidate in &values {
                prop_assert!(best <= total(Score::new(candidate)) + 1e-9, "{best} at {candidate}");
            }
        }

        #[test]
        fn property_a_probability_stays_inside_the_unit_interval(score in -30.0..30.0_f64) {
            let probability = Loss::Logistic.response(Score::new(score)).get();
            prop_assert!(probability > 0.0 && probability < 1.0);
        }
    }
}
