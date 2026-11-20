//! The execution pair: the chance an order resting at the touch fills, and what a fill earns per share by the window's
//! end, each a booster over the order's row, whose product is the expected value of resting.

use std::num::NonZeroUsize;

use crate::common::book::Side;
use crate::common::cost::{PerShare, PerShareRefusal};
use crate::common::laboratory::calibration::by_forecast_decile;
use crate::common::laboratory::score::{
    Forecast, Outcome, Pair, area_under_curve, linear_correlation,
};
use crate::common::learning::boosting::ensemble::{
    Booster, FitError, Settings, Stopping, WidthError,
};
use crate::common::learning::component::{Component, Diagnostics};
use crate::common::learning::loss::Loss;
use crate::common::learning::matrix::{Matrix, Sample, ShapeError};
use crate::common::learning::registry::{
    Inputs, InputsRefusal, ModelName, Weights, WeightsRefusal,
};
use crate::common::market::Price;
use crate::common::monoid::Fold;

/// The event table's inputs whose direction is the side's, so a buy and a sell read the book each its own way.
pub const SIDE_SIGNED_INPUTS: [&str; 6] = [
    "quote_imbalance",
    "microprice_offset",
    "order_flow_seconds_5",
    "trade_sign_events_10",
    "sweep_events_10",
    "spy_lead_seconds_5",
];

/// An order row's inputs: the event row's columns, then `side`, then each side-signed input times the side.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderInputs {
    inputs: Inputs,
    columns: usize,
    signed: [usize; SIDE_SIGNED_INPUTS.len()],
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum OrderInputsRefusal {
    #[error("the columns lack the side-signed input `{name}`")]
    Missing { name: &'static str },
    #[error(transparent)]
    Names(#[from] InputsRefusal),
}

/// The order inputs over `columns`, refused unless every side-signed input is among them and no name repeats.
pub fn order_inputs(columns: &Inputs) -> Result<OrderInputs, OrderInputsRefusal> {
    let names = columns.names();
    let mut signed = [0; SIDE_SIGNED_INPUTS.len()];
    for (position, name) in signed.iter_mut().zip(SIDE_SIGNED_INPUTS) {
        *position = names
            .iter()
            .position(|column| column == name)
            .ok_or(OrderInputsRefusal::Missing { name })?;
    }
    let added = std::iter::once("side".to_string()).chain(
        SIDE_SIGNED_INPUTS
            .iter()
            .map(|name| format!("side_signed_{name}")),
    );
    Ok(OrderInputs {
        inputs: Inputs::try_from(names.iter().cloned().chain(added).collect::<Vec<_>>())?,
        columns: names.len(),
        signed,
    })
}

/// One for a buy and minus one for a sell.
fn sign(side: Side) -> f64 {
    match side {
        Side::Buy => 1.0,
        Side::Sell => -1.0,
    }
}

impl OrderInputs {
    pub fn inputs(&self) -> &Inputs {
        &self.inputs
    }

    /// Appends to `values` the order row of `row`, an event row of the columns, resting on `side`.
    pub fn extend(&self, row: &[f64], side: Side, values: &mut Vec<f64>) -> Result<(), WidthError> {
        if row.len() != self.columns {
            return Err(WidthError {
                expected: self.columns,
                actual: row.len(),
            });
        }
        values.extend_from_slice(row);
        values.push(sign(side));
        values.extend(self.signed.iter().map(|column| sign(side) * row[*column]));
        Ok(())
    }
}

/// The chance a resting order fills, between zero and one inclusive.
#[derive(Debug, Clone, Copy, PartialEq, PartialOrd)]
pub struct FillProbability(f64);

#[derive(Debug, Clone, Copy, PartialEq, thiserror::Error)]
#[error("{value} is not a probability")]
pub struct FillProbabilityRefusal {
    pub value: f64,
}

impl FillProbability {
    pub fn new(value: f64) -> Result<Self, FillProbabilityRefusal> {
        match (0.0..=1.0).contains(&value) {
            true => Ok(Self(value + 0.0)),
            false => Err(FillProbabilityRefusal { value }),
        }
    }

    pub fn value(self) -> f64 {
        self.0
    }
}

/// What became of a resting order: unfilled, or filled with its markout, the gain per share to its side from the
/// entry to the mid at the window's end.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderOutcome {
    Unfilled,
    Filled { markout: PerShare },
}

impl OrderOutcome {
    /// A `side` fill at `entry` marked to the mid of `window_bid` and `window_ask`, a half-millionth rounded toward zero.
    pub fn filled(
        side: Side,
        entry: Price,
        window_bid: Price,
        window_ask: Price,
    ) -> Result<Self, PerShareRefusal> {
        let doubled = window_bid.ticks() + window_ask.ticks() - 2 * entry.ticks();
        let gained = match side {
            Side::Buy => doubled,
            Side::Sell => -doubled,
        };
        Ok(Self::Filled {
            markout: PerShare::from_millionths(gained / 2)?,
        })
    }
}

/// Order rows beside what became of each, as many outcomes as rows.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Orders<'a> {
    matrix: Matrix<'a>,
    outcomes: &'a [OrderOutcome],
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("{outcomes} outcomes for {rows} order rows")]
pub struct OrdersRefusal {
    pub rows: usize,
    pub outcomes: usize,
}

impl<'a> Orders<'a> {
    pub fn new(matrix: Matrix<'a>, outcomes: &'a [OrderOutcome]) -> Result<Self, OrdersRefusal> {
        match matrix.rows() == outcomes.len() {
            true => Ok(Self { matrix, outcomes }),
            false => Err(OrdersRefusal {
                rows: matrix.rows(),
                outcomes: outcomes.len(),
            }),
        }
    }

    pub fn matrix(&self) -> Matrix<'a> {
        self.matrix
    }

    pub fn outcomes(&self) -> &'a [OrderOutcome] {
        self.outcomes
    }
}

/// A booster refused for having descended another loss than its forecast reads.
#[derive(Debug, Clone, Copy, PartialEq, thiserror::Error)]
#[error("the forecast reads a {expected:?} booster, and this one descended {fitted:?}")]
pub struct LossRefusal {
    pub expected: Loss,
    pub fitted: Loss,
}

/// Why a forecast was refused: rows of the wrong width, or a row whose prediction is not a probability or a markout.
#[derive(Debug, Clone, Copy, PartialEq, thiserror::Error)]
pub enum ExecutionRefusal {
    #[error(transparent)]
    Width(#[from] WidthError),
    #[error("row {row} predicts a fill probability of {value}")]
    Probability { row: usize, value: f64 },
    #[error("row {row} predicts a markout of {value} millionths per share")]
    Markout { row: usize, value: f64 },
}

/// `booster` if it descended `expected`.
fn descended(booster: Booster, expected: Loss) -> Result<Booster, LossRefusal> {
    match booster.loss() == expected {
        true => Ok(booster),
        false => Err(LossRefusal {
            expected,
            fitted: booster.loss(),
        }),
    }
}

/// The chance each order fills, a logistic booster registered as `execution_fill`.
#[derive(Debug, Clone, PartialEq)]
pub struct FillForecast(Booster);

impl FillForecast {
    pub fn new(booster: Booster) -> Result<Self, LossRefusal> {
        descended(booster, Loss::Logistic).map(Self)
    }

    pub fn model_name() -> ModelName {
        ModelName::new("execution_fill").expect("a lowercase name is a model name")
    }

    pub fn weights(&self, inputs: &OrderInputs) -> Result<Weights<Booster>, WeightsRefusal> {
        Weights::new(inputs.inputs.clone(), self.0.clone())
    }
}

impl<'a> Component<Matrix<'a>> for FillForecast {
    type Output = Vec<FillProbability>;
    type Refusal = ExecutionRefusal;

    fn apply(
        &self,
        input: Matrix<'a>,
        diagnostics: &mut Diagnostics,
    ) -> Result<Vec<FillProbability>, ExecutionRefusal> {
        let predictions = self.0.apply(input, diagnostics)?;
        predictions
            .iter()
            .enumerate()
            .map(|(row, prediction)| {
                FillProbability::new(prediction.get()).map_err(|refusal| {
                    ExecutionRefusal::Probability {
                        row,
                        value: refusal.value,
                    }
                })
            })
            .collect()
    }
}

/// Each order's markout were it to fill, a squared-error booster over millionths per share fitted on fills alone and
/// registered as `execution_markout`.
#[derive(Debug, Clone, PartialEq)]
pub struct MarkoutForecast(Booster);

impl MarkoutForecast {
    pub fn new(booster: Booster) -> Result<Self, LossRefusal> {
        descended(booster, Loss::SquaredError).map(Self)
    }

    pub fn model_name() -> ModelName {
        ModelName::new("execution_markout").expect("a lowercase name is a model name")
    }

    pub fn weights(&self, inputs: &OrderInputs) -> Result<Weights<Booster>, WeightsRefusal> {
        Weights::new(inputs.inputs.clone(), self.0.clone())
    }
}

impl<'a> Component<Matrix<'a>> for MarkoutForecast {
    type Output = Vec<PerShare>;
    type Refusal = ExecutionRefusal;

    fn apply(
        &self,
        input: Matrix<'a>,
        diagnostics: &mut Diagnostics,
    ) -> Result<Vec<PerShare>, ExecutionRefusal> {
        let predictions = self.0.apply(input, diagnostics)?;
        predictions
            .iter()
            .enumerate()
            .map(|(row, prediction)| {
                let value = prediction.get().round();
                // The cast saturates, so a value beyond `i64` reaches the bound check rather than wrapping.
                match value.is_finite() {
                    true => PerShare::from_millionths(value as i64).ok(),
                    false => None,
                }
                .ok_or(ExecutionRefusal::Markout { row, value })
            })
            .collect()
    }
}

/// One order's fill probability beside its markout were it to fill.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct OrderForecast {
    fill: FillProbability,
    markout: PerShare,
}

impl OrderForecast {
    pub fn new(fill: FillProbability, markout: PerShare) -> Self {
        Self { fill, markout }
    }

    pub fn fill(self) -> FillProbability {
        self.fill
    }

    pub fn markout(self) -> PerShare {
        self.markout
    }

    /// The probability times the markout, rounded to the nearest millionth per share.
    pub fn expected_resting_value(self) -> PerShare {
        let value = (self.fill.0 * self.markout.millionths() as f64).round() as i64;
        PerShare::from_millionths(value).expect("a probability shrinks a markout toward zero")
    }
}

/// The fill and markout forecasts over rows of one width.
#[derive(Debug, Clone, PartialEq)]
pub struct ExecutionPair {
    fill: FillForecast,
    markout: MarkoutForecast,
}

impl ExecutionPair {
    pub fn new(fill: FillForecast, markout: MarkoutForecast) -> Result<Self, WidthError> {
        match fill.0.width() == markout.0.width() {
            true => Ok(Self { fill, markout }),
            false => Err(WidthError {
                expected: fill.0.width(),
                actual: markout.0.width(),
            }),
        }
    }

    pub fn fill(&self) -> &FillForecast {
        &self.fill
    }

    pub fn markout(&self) -> &MarkoutForecast {
        &self.markout
    }
}

/// Records `own`'s readings in `diagnostics` under `prefix`.
fn prefixed(prefix: &str, own: Diagnostics, diagnostics: &mut Diagnostics) {
    for (name, value) in own.readings() {
        diagnostics.record(&format!("{prefix}.{name}"), *value);
    }
}

impl<'a> Component<Matrix<'a>> for ExecutionPair {
    type Output = Vec<OrderForecast>;
    type Refusal = ExecutionRefusal;

    fn apply(
        &self,
        input: Matrix<'a>,
        diagnostics: &mut Diagnostics,
    ) -> Result<Vec<OrderForecast>, ExecutionRefusal> {
        let (mut fill, mut markout) = (Diagnostics::default(), Diagnostics::default());
        let probabilities = self.fill.apply(input, &mut fill)?;
        let markouts = self.markout.apply(input, &mut markout)?;
        prefixed("fill", fill, diagnostics);
        prefixed("markout", markout, diagnostics);
        Ok(probabilities
            .into_iter()
            .zip(markouts)
            .map(|(fill, markout)| OrderForecast::new(fill, markout))
            .collect())
    }
}

/// Applied to orders with their outcomes, it also records the fill ranking's area under the curve, the mean forecast
/// and fill rate by decile of forecast, and the correlation of forecast with realized markout over fills; a reading
/// with no defined value is left out.
impl<'a> Component<Orders<'a>> for ExecutionPair {
    type Output = Vec<OrderForecast>;
    type Refusal = ExecutionRefusal;

    fn apply(
        &self,
        input: Orders<'a>,
        diagnostics: &mut Diagnostics,
    ) -> Result<Vec<OrderForecast>, ExecutionRefusal> {
        let forecasts = self.apply(input.matrix, diagnostics)?;
        let mut ranked = Vec::with_capacity(forecasts.len());
        let (mut calibrated, mut marked) = (Vec::new(), Vec::new());
        for (forecast, outcome) in forecasts.iter().zip(input.outcomes) {
            let probability = Forecast::new(forecast.fill.0).expect("a probability is finite");
            let filled = match outcome {
                OrderOutcome::Unfilled => false,
                OrderOutcome::Filled { markout } => {
                    marked.push(Pair::new(
                        Forecast::new(forecast.markout.millionths() as f64)
                            .expect("an integer is finite"),
                        Outcome::new(markout.millionths() as f64).expect("an integer is finite"),
                    ));
                    true
                }
            };
            ranked.push((probability, filled));
            calibrated.push(Pair::new(
                probability,
                Outcome::new(f64::from(u8::from(filled))).expect("zero and one are finite"),
            ));
        }
        diagnostics.record("orders", ranked.len() as f64);
        diagnostics.record("fills", marked.len() as f64);
        if let Ok(area) = area_under_curve(&ranked) {
            diagnostics.record("area_under_curve", area);
        }
        let deciles = by_forecast_decile(&calibrated);
        for (decile, means) in (1..)
            .zip(deciles)
            .filter_map(|(decile, means)| Some((decile, means?)))
        {
            diagnostics.record(
                &format!("calibration_decile_{decile}_predicted"),
                means.forecast,
            );
            diagnostics.record(
                &format!("calibration_decile_{decile}_realized"),
                means.outcome,
            );
        }
        if let Ok(correlation) = linear_correlation(&marked) {
            diagnostics.record("markout_correlation", correlation);
        }
        Ok(forecasts)
    }
}

/// How the pair's boosters fit: the free choices of each, and up to `rounds` trees, stopping once validation loss
/// has not improved for `patience` rounds.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ExecutionSettings {
    pub boosting: Settings,
    pub rounds: usize,
    pub patience: NonZeroUsize,
}

/// Which booster's fit failed, or a fill-only matrix the orders could not shape.
#[derive(Debug, Clone, Copy, PartialEq, thiserror::Error)]
pub enum ExecutionFitRefusal {
    #[error("the fill forecast's fit failed: {0}")]
    Fill(FitError),
    #[error("the markout forecast's fit failed: {0}")]
    Markout(FitError),
    #[error(transparent)]
    Shape(#[from] ShapeError),
}

/// The fill labels of every order and the markout rows and labels of the filled ones.
fn labels(orders: &Orders) -> (Vec<f64>, Vec<f64>, Vec<f64>) {
    let (mut filled, mut rows, mut markouts) = (Vec::new(), Vec::new(), Vec::new());
    for (row, outcome) in input_rows(orders).zip(orders.outcomes) {
        match outcome {
            OrderOutcome::Unfilled => filled.push(0.0),
            OrderOutcome::Filled { markout } => {
                filled.push(1.0);
                rows.extend_from_slice(row);
                markouts.push(markout.millionths() as f64);
            }
        }
    }
    (filled, rows, markouts)
}

fn input_rows<'a>(orders: &Orders<'a>) -> impl Iterator<Item = &'a [f64]> {
    let matrix = orders.matrix;
    (0..matrix.rows()).map(move |row| matrix.row(row))
}

/// The pair fit on `train`, each booster early-stopped on `validation`: the fill forecast over every order, and the
/// markout forecast over the filled ones.
pub fn fit_execution_pair(
    train: &Orders,
    validation: &Orders,
    settings: &ExecutionSettings,
    fold: &impl Fold,
) -> Result<ExecutionPair, ExecutionFitRefusal> {
    let width =
        NonZeroUsize::new(train.matrix.width()).expect("a matrix is at least one input wide");
    let validation_width =
        NonZeroUsize::new(validation.matrix.width()).expect("a matrix is at least one input wide");
    let (
        (train_filled, train_rows, train_markouts),
        (validation_filled, validation_rows, validation_markouts),
    ) = (labels(train), labels(validation));
    let fit = |train: &Sample, held_out: &Sample| {
        let stopping = Stopping::Early {
            validation: held_out,
            rounds: settings.rounds,
            patience: settings.patience,
        };
        Booster::fit(train, stopping, &settings.boosting, fold)
    };
    let fill = fit(
        &Sample::new(train.matrix, &train_filled, Loss::Logistic)?,
        &Sample::new(validation.matrix, &validation_filled, Loss::Logistic)?,
    )
    .map_err(ExecutionFitRefusal::Fill)?;
    let markout = fit(
        &Sample::new(
            Matrix::new(&train_rows, width)?,
            &train_markouts,
            Loss::SquaredError,
        )?,
        &Sample::new(
            Matrix::new(&validation_rows, validation_width)?,
            &validation_markouts,
            Loss::SquaredError,
        )?,
    )
    .map_err(ExecutionFitRefusal::Markout)?;
    Ok(ExecutionPair {
        fill: FillForecast(fill),
        markout: MarkoutForecast(markout),
    })
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use proptest::prelude::*;

    use super::*;
    use crate::common::laboratory::permutation::Generator;
    use crate::common::learning::boosting::{Fraction, Penalty};
    use crate::common::monoid::Sequential;

    /// The six side-signed inputs, then the spread a fill reads and a column of noise.
    fn columns() -> Inputs {
        let mut names: Vec<&str> = SIDE_SIGNED_INPUTS.to_vec();
        names.extend(["spread_basis_points", "noise"]);
        Inputs::new(&names).unwrap()
    }

    fn settings() -> ExecutionSettings {
        ExecutionSettings {
            boosting: Settings {
                leaves: NonZeroUsize::new(8).unwrap(),
                learning_rate: Fraction::new(0.2).unwrap(),
                minimum_rows_per_leaf: NonZeroUsize::new(20).unwrap(),
                input_fraction: Fraction::new(1.0).unwrap(),
                row_fraction: Fraction::new(1.0).unwrap(),
                penalty: Penalty::new(1.0).unwrap(),
                seed: 11,
            },
            rounds: 200,
            patience: NonZeroUsize::new(20).unwrap(),
        }
    }

    /// A uniform draw from minus one to one.
    fn uniform(generator: &mut Generator) -> f64 {
        2.0 * (generator.next_u64() >> 11) as f64 / (1_u64 << 53) as f64 - 1.0
    }

    /// `events` synthetic event rows, each rested on both sides: an order fills with chance rising in the spread
    /// column alone, and a fill's markout is twenty thousand millionths times its side-signed microprice offset, plus
    /// noise.
    fn planted(events: usize, seed: u64) -> (Vec<f64>, Vec<OrderOutcome>) {
        let order_inputs = order_inputs(&columns()).unwrap();
        let mut generator = Generator::new(seed);
        let (mut values, mut outcomes) = (Vec::new(), Vec::new());
        for _ in 0..events {
            let row: Vec<f64> = (0..8).map(|_| uniform(&mut generator)).collect();
            for side in [Side::Buy, Side::Sell] {
                order_inputs.extend(&row, side, &mut values).unwrap();
                let chance = 1.0 / (1.0 + (-4.0 * row[6]).exp());
                let filled = (uniform(&mut generator) + 1.0) / 2.0 < chance;
                let markout = 20_000.0 * sign(side) * row[1] + 2_000.0 * uniform(&mut generator);
                outcomes.push(match filled {
                    true => OrderOutcome::Filled {
                        markout: PerShare::from_millionths(markout.round() as i64).unwrap(),
                    },
                    false => OrderOutcome::Unfilled,
                });
            }
        }
        (values, outcomes)
    }

    fn orders<'a>(values: &'a [f64], outcomes: &'a [OrderOutcome]) -> Orders<'a> {
        let width = NonZeroUsize::new(15).unwrap();
        Orders::new(Matrix::new(values, width).unwrap(), outcomes).unwrap()
    }

    /// Fits the pair on planted train and validation orders, with the fill outcomes shuffled when `shuffled`, and
    /// applies it to planted test orders, returning its readings.
    fn readings(shuffled: bool) -> Vec<(String, f64)> {
        let (train_values, mut train_outcomes) = planted(1500, 1);
        let (validation_values, mut validation_outcomes) = planted(500, 2);
        let (test_values, test_outcomes) = planted(1000, 3);
        if shuffled {
            let mut generator = Generator::new(4);
            generator.shuffle(&mut train_outcomes);
            generator.shuffle(&mut validation_outcomes);
        }
        let pair = fit_execution_pair(
            &orders(&train_values, &train_outcomes),
            &orders(&validation_values, &validation_outcomes),
            &settings(),
            &Sequential,
        )
        .unwrap();
        let mut diagnostics = Diagnostics::default();
        let forecasts = pair
            .apply(orders(&test_values, &test_outcomes), &mut diagnostics)
            .unwrap();
        assert_eq!(forecasts.len(), 2000);
        diagnostics.readings().to_vec()
    }

    fn reading(readings: &[(String, f64)], name: &str) -> f64 {
        readings
            .iter()
            .find(|(read, _)| read == name)
            .unwrap_or_else(|| panic!("no reading {name}"))
            .1
    }

    /// Fill planted in one input ranks held-out orders well above chance, the markout forecast tracks the planted
    /// markout, and the same fit on shuffled fill outcomes ranks at chance, within four standard errors of a half.
    #[test]
    fn test_a_planted_fill_is_ranked_and_a_shuffled_one_is_not() {
        let planted = readings(false);
        let mut expected: BTreeSet<String> = [
            "fill.rounds",
            "fill.rows",
            "fill.undefined_predictions",
            "fill.mean_prediction",
            "markout.rounds",
            "markout.rows",
            "markout.undefined_predictions",
            "markout.mean_prediction",
            "orders",
            "fills",
            "area_under_curve",
            "markout_correlation",
        ]
        .into_iter()
        .map(str::to_string)
        .collect();
        for decile in 1..=10 {
            expected.insert(format!("calibration_decile_{decile}_predicted"));
            expected.insert(format!("calibration_decile_{decile}_realized"));
        }
        let names: BTreeSet<String> = planted.iter().map(|(name, _)| name.clone()).collect();
        assert_eq!(names, expected);
        assert_eq!(reading(&planted, "orders"), 2000.0);
        assert!(reading(&planted, "area_under_curve") > 0.75, "{planted:?}");
        assert!(
            reading(&planted, "markout_correlation") > 0.8,
            "{planted:?}"
        );
        let (lowest, highest) = (
            reading(&planted, "calibration_decile_1_realized"),
            reading(&planted, "calibration_decile_10_realized"),
        );
        assert!(lowest < 0.25 && highest > 0.75, "{lowest} {highest}");
        let null = reading(&readings(true), "area_under_curve");
        assert!((null - 0.5).abs() < 4.0 * 0.013, "{null}");
    }

    /// Each model's weights carry the order inputs and decode only against them, under the pair's two model names.
    #[test]
    fn test_each_forecast_is_its_own_registry_entry() {
        let (values, outcomes) = planted(300, 5);
        let train = orders(&values, &outcomes);
        let pair = fit_execution_pair(&train, &train, &settings(), &Sequential).unwrap();
        let inputs = order_inputs(&columns()).unwrap();
        for (weights, name, expected) in [
            (
                pair.fill().weights(&inputs).unwrap(),
                FillForecast::model_name(),
                "execution_fill",
            ),
            (
                pair.markout().weights(&inputs).unwrap(),
                MarkoutForecast::model_name(),
                "execution_markout",
            ),
        ] {
            let decoded = Weights::<Booster>::decode(&weights.encode(), inputs.inputs()).unwrap();
            assert_eq!(decoded, weights);
            assert_eq!(decoded.inputs().width(), 15);
            assert!(Weights::<Booster>::decode(&weights.encode(), &columns()).is_err());
            assert_eq!(String::from(name), expected);
        }
        let fill = pair.fill().0.clone();
        assert_eq!(
            MarkoutForecast::new(fill),
            Err(LossRefusal {
                expected: Loss::SquaredError,
                fitted: Loss::Logistic,
            })
        );
    }

    /// The order inputs append `side` and the six signed inputs, refusing columns that lack one or already name one.
    #[test]
    fn test_order_inputs_name_the_side_and_its_signed_inputs() {
        let inputs = order_inputs(&columns()).unwrap();
        assert_eq!(
            inputs.inputs().names()[7..],
            [
                "noise",
                "side",
                "side_signed_quote_imbalance",
                "side_signed_microprice_offset",
                "side_signed_order_flow_seconds_5",
                "side_signed_trade_sign_events_10",
                "side_signed_sweep_events_10",
                "side_signed_spy_lead_seconds_5",
            ]
        );
        assert_eq!(
            order_inputs(&Inputs::new(&["quote_imbalance"]).unwrap()),
            Err(OrderInputsRefusal::Missing {
                name: "microprice_offset"
            })
        );
        let mut named: Vec<&str> = SIDE_SIGNED_INPUTS.to_vec();
        named.push("side");
        assert_eq!(
            order_inputs(&Inputs::new(&named).unwrap()),
            Err(OrderInputsRefusal::Names(InputsRefusal::Repeated {
                name: "side".to_string(),
                first: 6,
                second: 7,
            }))
        );
        let mut values = Vec::new();
        assert_eq!(
            inputs.extend(&[0.0; 7], Side::Buy, &mut values),
            Err(WidthError {
                expected: 8,
                actual: 7
            })
        );
    }

    /// A fill's markout is the side's gain to the window's mid, and the expected value of resting shrinks it by the
    /// chance of the fill.
    #[test]
    fn test_markouts_and_resting_value_match_hand_arithmetic() {
        let price = |dollars| Price::from_dollars(dollars).unwrap();
        let (entry, bid, ask) = (price(10.0), price(10.01), price(10.03));
        let markout = |side| match OrderOutcome::filled(side, entry, bid, ask).unwrap() {
            OrderOutcome::Filled { markout } => markout.millionths(),
            OrderOutcome::Unfilled => panic!("a fill is filled"),
        };
        assert_eq!((markout(Side::Buy), markout(Side::Sell)), (20_000, -20_000));
        let value = |probability, millionths| {
            OrderForecast::new(
                FillProbability::new(probability).unwrap(),
                PerShare::from_millionths(millionths).unwrap(),
            )
            .expected_resting_value()
            .millionths()
        };
        assert_eq!(value(0.25, 20_000), 5_000);
        assert_eq!(value(1.0 / 3.0, -10), -3);
        assert_eq!(value(0.0, -10), 0);
        assert!(FillProbability::new(1.5).is_err() && FillProbability::new(f64::NAN).is_err());
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(16))]

        /// Swapping the side keeps the event row and negates the side and every signed input.
        #[test]
        fn test_swapping_the_side_negates_only_the_signed_inputs(
            row in prop::collection::vec(-1e6_f64..1e6, 8),
        ) {
            let inputs = order_inputs(&columns()).unwrap();
            let (mut buy, mut sell) = (Vec::new(), Vec::new());
            inputs.extend(&row, Side::Buy, &mut buy).unwrap();
            inputs.extend(&row, Side::Sell, &mut sell).unwrap();
            prop_assert_eq!(buy.len(), inputs.inputs().width());
            prop_assert_eq!(&buy[..8], &row[..]);
            prop_assert_eq!(&sell[..8], &row[..]);
            for (index, (buy, sell)) in buy[8..].iter().zip(&sell[8..]).enumerate() {
                prop_assert_eq!(*buy, -*sell);
                let expected = match index {
                    0 => 1.0,
                    signed => row[signed - 1],
                };
                prop_assert_eq!(*buy, expected);
            }
        }

        /// A fill forecast's every probability lies in the unit interval, and a markout's sign flips with the side.
        #[test]
        fn test_probabilities_are_probabilities_and_markouts_flip_with_the_side(
            seed in 0_u64..1000,
            entry in 1_000_i64..1_000_000,
            spread in 0_i64..10_000,
        ) {
            let (values, outcomes) = planted(60, seed);
            let train = orders(&values, &outcomes);
            let pair = fit_execution_pair(&train, &train, &settings(), &Sequential).unwrap();
            let probabilities = pair.fill().apply(train.matrix(), &mut Diagnostics::default()).unwrap();
            prop_assert!(probabilities.iter().all(|probability| (0.0..=1.0).contains(&probability.value())));
            let price = |millionths| Price::from_ticks(millionths).unwrap();
            let (bid, ask) = (price(entry), price(entry + spread));
            prop_assert_eq!(
                OrderOutcome::filled(Side::Buy, price(entry), bid, ask).unwrap(),
                OrderOutcome::Filled {
                    markout: PerShare::from_millionths(spread / 2).unwrap()
                }
            );
            prop_assert_eq!(
                OrderOutcome::filled(Side::Sell, price(entry), bid, ask).unwrap(),
                OrderOutcome::Filled {
                    markout: PerShare::from_millionths(-(spread / 2)).unwrap()
                }
            );
        }
    }
}
