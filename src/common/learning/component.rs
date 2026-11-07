//! A model as fitted components wired output to input, so a wrong wiring fails to compile, any component can be
//! switched to the arm it replaces, and every application reports each component's diagnostics by name.

use std::convert::Infallible;

use crate::common::laboratory::experiment::{ExperimentRefusal, Name, Outputs};
use crate::common::learning::boosting::ensemble::{Booster, WidthError};
use crate::common::learning::loss::Prediction;
use crate::common::learning::matrix::Matrix;
use crate::common::learning::ridge::Ridge;

/// One fitted step of a model: it maps its input to its output, recording what it observed in `diagnostics`.
pub trait Component<Input> {
    type Output;
    type Refusal;

    fn apply(
        &self,
        input: Input,
        diagnostics: &mut Diagnostics,
    ) -> Result<Self::Output, Self::Refusal>;
}

/// The readings components record as they apply, in order, each under its components' names joined by dots.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Diagnostics(Vec<(String, f64)>);

impl Diagnostics {
    pub fn record(&mut self, name: &str, value: f64) {
        self.0.push((name.to_string(), value));
    }

    pub fn readings(&self) -> &[(String, f64)] {
        &self.0
    }

    /// Adds every reading to `outputs` as a metric; two readings under one name, as two unnamed components of the
    /// same kind give, are refused as a duplicate.
    pub fn journal(self, outputs: Outputs) -> Result<Outputs, ExperimentRefusal> {
        self.0
            .into_iter()
            .try_fold(outputs, |outputs, (name, value)| {
                outputs.metric(name, value)
            })
    }
}

/// A component whose readings are recorded under `name`.
#[derive(Debug, Clone, PartialEq)]
pub struct Named<C> {
    pub name: Name,
    pub component: C,
}

impl<Input, C: Component<Input>> Component<Input> for Named<C> {
    type Output = C::Output;
    type Refusal = C::Refusal;

    fn apply(&self, input: Input, diagnostics: &mut Diagnostics) -> Result<C::Output, C::Refusal> {
        let mut own = Diagnostics::default();
        let output = self.component.apply(input, &mut own);
        for (name, value) in own.0 {
            diagnostics.record(&format!("{}.{name}", self.name.as_str()), value);
        }
        output
    }
}

/// `first`'s output fed to `second`, which compiles only when `second` reads what `first` writes.
#[derive(Debug, Clone, PartialEq)]
pub struct Then<First, Second> {
    pub first: First,
    pub second: Second,
}

/// Which of two wired components refused.
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum ThenRefusal<First, Second> {
    #[error("the first component refused: {0}")]
    First(First),
    #[error("the second component refused: {0}")]
    Second(Second),
}

impl<Input, First, Second> Component<Input> for Then<First, Second>
where
    First: Component<Input>,
    Second: Component<First::Output>,
{
    type Output = Second::Output;
    type Refusal = ThenRefusal<First::Refusal, Second::Refusal>;

    fn apply(
        &self,
        input: Input,
        diagnostics: &mut Diagnostics,
    ) -> Result<Self::Output, Self::Refusal> {
        let middle = self
            .first
            .apply(input, diagnostics)
            .map_err(ThenRefusal::First)?;
        self.second
            .apply(middle, diagnostics)
            .map_err(ThenRefusal::Second)
    }
}

/// Whether a switch applies its component or the arm it replaces.
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
pub enum Setting {
    On,
    Off,
}

/// A component with its off switch: `off` is the arm it replaces, reading and writing the same types.
#[derive(Debug, Clone, PartialEq)]
pub struct Switch<On, Off> {
    pub setting: Setting,
    pub on: On,
    pub off: Off,
}

/// Which arm of a switch refused.
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum SwitchRefusal<On, Off> {
    #[error("the component refused: {0}")]
    On(On),
    #[error("the arm it replaces refused: {0}")]
    Off(Off),
}

impl<Input, On, Off> Component<Input> for Switch<On, Off>
where
    On: Component<Input>,
    Off: Component<Input, Output = On::Output>,
{
    type Output = On::Output;
    type Refusal = SwitchRefusal<On::Refusal, Off::Refusal>;

    /// Records `switched_on` as one or zero, so the journal says which arm ran.
    fn apply(
        &self,
        input: Input,
        diagnostics: &mut Diagnostics,
    ) -> Result<Self::Output, Self::Refusal> {
        match self.setting {
            Setting::On => {
                diagnostics.record("switched_on", 1.0);
                self.on.apply(input, diagnostics).map_err(SwitchRefusal::On)
            }
            Setting::Off => {
                diagnostics.record("switched_on", 0.0);
                self.off
                    .apply(input, diagnostics)
                    .map_err(SwitchRefusal::Off)
            }
        }
    }
}

/// The component that passes its input through, the arm a step that keeps its input's type replaces.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Identity;

impl<Input> Component<Input> for Identity {
    type Output = Input;
    type Refusal = Infallible;

    fn apply(&self, input: Input, _: &mut Diagnostics) -> Result<Input, Infallible> {
        Ok(input)
    }
}

/// Records the rows read, how many predicted no number (a missing input to a linear model), and the mean of the rest.
fn record_predictions(predictions: &[Prediction], diagnostics: &mut Diagnostics) {
    let defined: Vec<f64> = predictions
        .iter()
        .map(|prediction| prediction.get())
        .filter(|value| value.is_finite())
        .collect();
    diagnostics.record("rows", predictions.len() as f64);
    diagnostics.record(
        "undefined_predictions",
        (predictions.len() - defined.len()) as f64,
    );
    if !defined.is_empty() {
        diagnostics.record(
            "mean_prediction",
            defined.iter().sum::<f64>() / defined.len() as f64,
        );
    }
}

impl<'a> Component<Matrix<'a>> for Ridge {
    type Output = Vec<Prediction>;
    type Refusal = WidthError;

    fn apply(
        &self,
        input: Matrix<'a>,
        diagnostics: &mut Diagnostics,
    ) -> Result<Vec<Prediction>, WidthError> {
        let predictions = self.predict(&input)?;
        let norm = self
            .coefficients()
            .iter()
            .map(|coefficient| coefficient.powi(2))
            .sum::<f64>()
            .sqrt();
        diagnostics.record("coefficient_norm", norm);
        record_predictions(&predictions, diagnostics);
        Ok(predictions)
    }
}

impl<'a> Component<Matrix<'a>> for Booster {
    type Output = Vec<Prediction>;
    type Refusal = WidthError;

    fn apply(
        &self,
        input: Matrix<'a>,
        diagnostics: &mut Diagnostics,
    ) -> Result<Vec<Prediction>, WidthError> {
        let predictions = self.predict(&input)?;
        diagnostics.record("rounds", self.rounds() as f64);
        record_predictions(&predictions, diagnostics);
        Ok(predictions)
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;

    use super::*;
    use crate::common::learning::boosting::ensemble::{Settings, Stopping};
    use crate::common::learning::boosting::{Fraction, Penalty};
    use crate::common::learning::loss::Loss;
    use crate::common::learning::matrix::Sample;
    use crate::common::monoid::Sequential;

    /// Scales each prediction, a stand-in for a step that reads a model's output.
    struct Scale(f64);

    impl Component<Vec<Prediction>> for Scale {
        type Output = Vec<f64>;
        type Refusal = Infallible;

        fn apply(
            &self,
            input: Vec<Prediction>,
            diagnostics: &mut Diagnostics,
        ) -> Result<Vec<f64>, Infallible> {
            diagnostics.record("factor", self.0);
            Ok(input
                .iter()
                .map(|prediction| prediction.get() * self.0)
                .collect())
        }
    }

    /// Reads predictions as plain values, the arm `Scale` replaces.
    struct Unscaled;

    impl Component<Vec<Prediction>> for Unscaled {
        type Output = Vec<f64>;
        type Refusal = Infallible;

        fn apply(
            &self,
            input: Vec<Prediction>,
            _: &mut Diagnostics,
        ) -> Result<Vec<f64>, Infallible> {
            Ok(input.iter().map(|prediction| prediction.get()).collect())
        }
    }

    fn name(text: &str) -> Name {
        Name::new(text).unwrap()
    }

    /// Fits `label = 2x` on x = 1, 2, 3 with a negligible penalty.
    fn ridge() -> Ridge {
        let matrix = Matrix::new(&[1.0, 2.0, 3.0], NonZeroUsize::MIN).unwrap();
        let sample = Sample::new(matrix, &[2.0, 4.0, 6.0], Loss::SquaredError).unwrap();
        Ridge::fit(&sample, Penalty::new(1e-12).unwrap()).unwrap()
    }

    fn model(setting: Setting) -> Then<Named<Ridge>, Named<Switch<Scale, Unscaled>>> {
        Then {
            first: Named {
                name: name("ridge"),
                component: ridge(),
            },
            second: Named {
                name: name("scale"),
                component: Switch {
                    setting,
                    on: Scale(10.0),
                    off: Unscaled,
                },
            },
        }
    }

    #[test]
    fn test_wired_components_feed_each_other_and_name_their_readings() {
        let values = [1.0, 4.0];
        let matrix = Matrix::new(&values, NonZeroUsize::MIN).unwrap();
        let mut diagnostics = Diagnostics::default();
        let output = model(Setting::On).apply(matrix, &mut diagnostics).unwrap();
        assert_eq!(output.len(), 2);
        assert!(
            (output[0] - 20.0).abs() < 1e-6 && (output[1] - 80.0).abs() < 1e-6,
            "{output:?}"
        );
        let names: Vec<&str> = diagnostics
            .readings()
            .iter()
            .map(|(name, _)| name.as_str())
            .collect();
        assert_eq!(
            names,
            [
                "ridge.coefficient_norm",
                "ridge.rows",
                "ridge.undefined_predictions",
                "ridge.mean_prediction",
                "scale.switched_on",
                "scale.factor"
            ]
        );
        let reading = |wanted: &str| {
            diagnostics
                .readings()
                .iter()
                .find(|(name, _)| name == wanted)
                .map(|(_, value)| *value)
        };
        assert_eq!(reading("ridge.rows"), Some(2.0));
        assert_eq!(reading("scale.switched_on"), Some(1.0));
        assert!((reading("ridge.mean_prediction").unwrap() - 5.0).abs() < 1e-6);
    }

    #[test]
    fn test_a_missing_input_counts_as_undefined_and_still_journals() {
        let values = [1.0, f64::NAN, 3.0];
        let matrix = Matrix::new(&values, NonZeroUsize::MIN).unwrap();
        let mut diagnostics = Diagnostics::default();
        ridge().apply(matrix, &mut diagnostics).unwrap();
        let readings: Vec<(&str, f64)> = diagnostics
            .readings()
            .iter()
            .filter(|(name, _)| name != "coefficient_norm")
            .map(|(name, value)| (name.as_str(), (value * 1e6).round() / 1e6))
            .collect();
        assert_eq!(
            readings,
            [
                ("rows", 3.0),
                ("undefined_predictions", 1.0),
                ("mean_prediction", 4.0)
            ]
        );
        assert!(diagnostics.journal(Outputs::default()).is_ok());
    }

    #[test]
    fn test_switching_a_component_off_runs_the_arm_it_replaces() {
        let values = [1.0, 4.0];
        let matrix = Matrix::new(&values, NonZeroUsize::MIN).unwrap();
        let mut diagnostics = Diagnostics::default();
        let output = model(Setting::Off).apply(matrix, &mut diagnostics).unwrap();
        assert!(
            (output[0] - 2.0).abs() < 1e-6 && (output[1] - 8.0).abs() < 1e-6,
            "{output:?}"
        );
        let names: Vec<&str> = diagnostics
            .readings()
            .iter()
            .map(|(name, _)| name.as_str())
            .collect();
        assert_eq!(names.last(), Some(&"scale.switched_on"));
        assert_eq!(diagnostics.readings().last().unwrap().1, 0.0);
    }

    #[test]
    fn test_a_refusal_says_which_component_refused() {
        let values = [1.0, 4.0];
        let wide = Matrix::new(&values, NonZeroUsize::new(2).unwrap()).unwrap();
        let refused = model(Setting::On).apply(wide, &mut Diagnostics::default());
        assert_eq!(
            refused,
            Err(ThenRefusal::First(WidthError {
                expected: 1,
                actual: 2
            }))
        );
    }

    #[test]
    fn test_a_booster_reports_its_rounds_and_predictions() {
        let values: Vec<f64> = (0..20).map(f64::from).collect();
        let labels: Vec<f64> = values.iter().map(|value| 3.0 * value).collect();
        let matrix = Matrix::new(&values, NonZeroUsize::MIN).unwrap();
        let sample = Sample::new(matrix, &labels, Loss::SquaredError).unwrap();
        let settings = Settings {
            leaves: NonZeroUsize::new(4).unwrap(),
            learning_rate: Fraction::new(0.5).unwrap(),
            minimum_rows_per_leaf: NonZeroUsize::new(2).unwrap(),
            input_fraction: Fraction::new(1.0).unwrap(),
            row_fraction: Fraction::new(1.0).unwrap(),
            penalty: Penalty::new(1.0).unwrap(),
            seed: 7,
        };
        let booster = Booster::fit(
            &sample,
            Stopping::Fixed { rounds: 3 },
            &settings,
            &Sequential,
        )
        .unwrap();
        let mut diagnostics = Diagnostics::default();
        let predictions = booster.apply(matrix, &mut diagnostics).unwrap();
        let mean = predictions
            .iter()
            .map(|prediction| prediction.get())
            .sum::<f64>()
            / 20.0;
        assert_eq!(
            diagnostics.readings(),
            [
                ("rounds".to_string(), 3.0),
                ("rows".to_string(), 20.0),
                ("undefined_predictions".to_string(), 0.0),
                ("mean_prediction".to_string(), mean),
            ]
        );
    }

    #[test]
    fn test_the_identity_passes_its_input_and_records_nothing() {
        let mut diagnostics = Diagnostics::default();
        assert_eq!(Identity.apply(vec![1, 2], &mut diagnostics), Ok(vec![1, 2]));
        assert!(diagnostics.readings().is_empty());
    }

    #[test]
    fn test_diagnostics_journal_as_metrics_and_refuse_a_name_twice() {
        let mut diagnostics = Diagnostics::default();
        diagnostics.record("ridge.rows", 3.0);
        diagnostics.record("booster.rows", 4.0);
        let expected = Outputs::default()
            .metric("booster.rows", 4.0)
            .and_then(|outputs| outputs.metric("ridge.rows", 3.0))
            .unwrap();
        assert_eq!(
            diagnostics.clone().journal(Outputs::default()),
            Ok(expected)
        );
        diagnostics.record("ridge.rows", 5.0);
        assert_eq!(
            diagnostics.journal(Outputs::default()),
            Err(ExperimentRefusal::Duplicate {
                name: "ridge.rows".to_string()
            })
        );
    }
}
