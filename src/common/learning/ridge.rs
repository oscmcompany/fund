//! Ridge regression: least squares with an L2 penalty on every coefficient but the intercept, solved in closed form.

use crate::common::learning::boosting::Penalty;
use crate::common::learning::boosting::ensemble::WidthError;
use crate::common::learning::loss::{Loss, Prediction, Score};
use crate::common::learning::matrix::{Matrix, Sample};

/// A fitted ridge regression: the intercept and one coefficient per input, read under squared error.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Ridge {
    intercept: f64,
    coefficients: Vec<f64>,
}

/// Why a ridge regression could not be fit.
#[derive(Debug, Clone, Copy, PartialEq, thiserror::Error)]
pub enum RidgeRefusal {
    #[error("a fit needs training rows")]
    NoRows,
    /// Ridge reads squared error only; another loss's labels mean something a linear fit does not minimize.
    #[error("ridge fits squared error, not {loss:?}")]
    Loss { loss: Loss },
    #[error("the value at row {row}, input {input} is missing")]
    Missing { row: usize, input: usize },
    #[error("the value at row {row}, input {input} is {value}")]
    Infinite {
        row: usize,
        input: usize,
        value: f64,
    },
    /// Rounding left the penalized system without a positive pivot, as near-collinear inputs at a tiny penalty can.
    #[error("the penalized system's pivot at input {input} is {pivot}")]
    Unsolvable { input: usize, pivot: f64 },
}

impl Ridge {
    /// Minimizes the squared error plus `penalty` times the squared coefficients; the intercept goes unpenalized, so
    /// the fit is the same whatever constant the labels are shifted by.
    pub fn fit(sample: &Sample, penalty: Penalty) -> Result<Self, RidgeRefusal> {
        let (matrix, labels) = (sample.matrix(), sample.labels());
        match sample.loss() {
            Loss::SquaredError => {}
            loss @ (Loss::Quantile(_) | Loss::Logistic) => return Err(RidgeRefusal::Loss { loss }),
        }
        let (rows, width) = (matrix.rows(), matrix.width());
        if rows == 0 {
            return Err(RidgeRefusal::NoRows);
        }
        for row in 0..rows {
            for (input, &value) in matrix.row(row).iter().enumerate() {
                match (value.is_nan(), value.is_infinite()) {
                    (true, _) => return Err(RidgeRefusal::Missing { row, input }),
                    (false, true) => return Err(RidgeRefusal::Infinite { row, input, value }),
                    (false, false) => {}
                }
            }
        }
        let count = rows as f64;
        let means: Vec<f64> = (0..width)
            .map(|input| (0..rows).map(|row| matrix.row(row)[input]).sum::<f64>() / count)
            .collect();
        let label_mean = labels.iter().map(|label| label.get()).sum::<f64>() / count;
        let mut gram = vec![0.0; width * width];
        let mut moments = vec![0.0; width];
        for (row, label) in labels.iter().enumerate() {
            let centered: Vec<f64> = matrix
                .row(row)
                .iter()
                .zip(&means)
                .map(|(value, mean)| value - mean)
                .collect();
            for (left, left_value) in centered.iter().enumerate() {
                moments[left] += left_value * (label.get() - label_mean);
                for (right, right_value) in centered.iter().enumerate() {
                    gram[left * width + right] += left_value * right_value;
                }
            }
        }
        for input in 0..width {
            gram[input * width + input] += penalty.get();
        }
        let coefficients = solve_positive_definite(gram, moments, width)?;
        let intercept = label_mean
            - coefficients
                .iter()
                .zip(&means)
                .map(|(coefficient, mean)| coefficient * mean)
                .sum::<f64>();
        Ok(Self {
            intercept,
            coefficients,
        })
    }

    pub fn intercept(&self) -> f64 {
        self.intercept
    }

    pub fn coefficients(&self) -> &[f64] {
        &self.coefficients
    }

    /// Each row's prediction; a missing value predicts NaN, since a linear model has no branch to send it down.
    pub fn predict(&self, matrix: &Matrix) -> Result<Vec<Prediction>, WidthError> {
        if matrix.width() != self.coefficients.len() {
            return Err(WidthError {
                expected: self.coefficients.len(),
                actual: matrix.width(),
            });
        }
        Ok((0..matrix.rows())
            .map(|row| {
                let score = self.intercept
                    + matrix
                        .row(row)
                        .iter()
                        .zip(&self.coefficients)
                        .map(|(value, coefficient)| value * coefficient)
                        .sum::<f64>();
                Loss::SquaredError.response(Score::new(score))
            })
            .collect())
    }
}

/// Solves `system · x = right` by Cholesky for a symmetric `system` stored row-major, refusing when a pivot is not
/// positive and finite or the solution overflows, since rounding can break the definiteness a penalty promises.
fn solve_positive_definite(
    mut system: Vec<f64>,
    mut right: Vec<f64>,
    width: usize,
) -> Result<Vec<f64>, RidgeRefusal> {
    for column in 0..width {
        let pivot = system[column * width + column]
            - (0..column)
                .map(|inner| system[column * width + inner].powi(2))
                .sum::<f64>();
        if !(pivot > 0.0 && pivot.is_finite()) {
            return Err(RidgeRefusal::Unsolvable {
                input: column,
                pivot,
            });
        }
        let diagonal = pivot.sqrt();
        system[column * width + column] = diagonal;
        for row in column + 1..width {
            let dot: f64 = (0..column)
                .map(|inner| system[row * width + inner] * system[column * width + inner])
                .sum();
            system[row * width + column] = (system[row * width + column] - dot) / diagonal;
        }
    }
    for row in 0..width {
        let dot: f64 = (0..row)
            .map(|inner| system[row * width + inner] * right[inner])
            .sum();
        right[row] = (right[row] - dot) / system[row * width + row];
    }
    for row in (0..width).rev() {
        let dot: f64 = (row + 1..width)
            .map(|inner| system[inner * width + row] * right[inner])
            .sum();
        right[row] = (right[row] - dot) / system[row * width + row];
    }
    match right.iter().position(|value| !value.is_finite()) {
        Some(input) => Err(RidgeRefusal::Unsolvable {
            input,
            pivot: system[input * width + input].powi(2),
        }),
        None => Ok(right),
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;

    use proptest::prelude::*;

    use super::*;
    use crate::common::learning::loss::Level;

    fn sample<'a>(values: &'a [f64], width: usize, labels: &[f64]) -> Sample<'a> {
        let matrix = Matrix::new(values, NonZeroUsize::new(width).unwrap()).unwrap();
        Sample::new(matrix, labels, Loss::SquaredError).unwrap()
    }

    #[test]
    fn test_a_small_penalty_recovers_an_exact_linear_relation() {
        // Three inputs, so the factorization subtracts an earlier column from an off-diagonal entry.
        let values = [
            1.0, 0.0, 2.0, 2.0, 1.0, 0.0, 3.0, 5.0, 1.0, 4.0, 2.0, 3.0, 0.0, 3.0, 4.0, 2.0, 2.0,
            5.0,
        ];
        let labels: Vec<f64> = values
            .chunks(3)
            .map(|row| 1.5 + 2.0 * row[0] - 0.5 * row[1] + 0.25 * row[2])
            .collect();
        let ridge = Ridge::fit(&sample(&values, 3, &labels), Penalty::new(1e-9).unwrap()).unwrap();
        assert_eq!(ridge.coefficients().len(), 3);
        assert!((ridge.intercept() - 1.5).abs() < 1e-6, "{ridge:?}");
        assert!((ridge.coefficients()[0] - 2.0).abs() < 1e-6, "{ridge:?}");
        assert!((ridge.coefficients()[1] + 0.5).abs() < 1e-6, "{ridge:?}");
        assert!((ridge.coefficients()[2] - 0.25).abs() < 1e-6, "{ridge:?}");
    }

    #[test]
    fn test_one_input_matches_the_closed_form_shrinkage() {
        // Centered inputs -1, 0, 1 and labels 0, 1, 4: slope = Σxy / (Σx² + λ) = 4 / (2 + 2) = 1, intercept = 5/3 - 2.
        let ridge = Ridge::fit(
            &sample(&[1.0, 2.0, 3.0], 1, &[0.0, 1.0, 4.0]),
            Penalty::new(2.0).unwrap(),
        )
        .unwrap();
        assert!((ridge.coefficients()[0] - 1.0).abs() < 1e-12);
        assert!((ridge.intercept() - (5.0 / 3.0 - 2.0)).abs() < 1e-12);
    }

    #[test]
    fn test_a_fit_refuses_what_it_cannot_read() {
        let matrix = Matrix::new(&[1.0, 2.0], NonZeroUsize::new(1).unwrap()).unwrap();
        let quantile = Sample::new(
            matrix,
            &[1.0, 2.0],
            Loss::Quantile(Level::try_from(0.5).unwrap()),
        )
        .unwrap();
        let penalty = Penalty::new(1.0).unwrap();
        assert!(matches!(
            Ridge::fit(&quantile, penalty),
            Err(RidgeRefusal::Loss { .. })
        ));
        assert_eq!(
            Ridge::fit(&sample(&[], 1, &[]), penalty),
            Err(RidgeRefusal::NoRows)
        );
        assert_eq!(
            Ridge::fit(&sample(&[1.0, f64::NAN, 2.0, 3.0], 2, &[1.0, 2.0]), penalty),
            Err(RidgeRefusal::Missing { row: 0, input: 1 })
        );
        assert_eq!(
            Ridge::fit(
                &sample(&[1.0, 2.0, f64::NEG_INFINITY, 3.0], 2, &[1.0, 2.0]),
                penalty
            ),
            Err(RidgeRefusal::Infinite {
                row: 1,
                input: 0,
                value: f64::NEG_INFINITY
            })
        );
        let huge = [1e200, -1e200];
        assert!(matches!(
            Ridge::fit(&sample(&huge, 1, &[1.0, 2.0]), penalty),
            Err(RidgeRefusal::Unsolvable { input: 0, .. })
        ));
    }

    #[test]
    fn test_predicting_rows_of_another_width_is_refused() {
        let ridge = Ridge::fit(
            &sample(&[1.0, 2.0, 3.0], 1, &[1.0, 2.0, 3.0]),
            Penalty::new(1.0).unwrap(),
        )
        .unwrap();
        let wide = Matrix::new(&[1.0, 2.0], NonZeroUsize::new(2).unwrap()).unwrap();
        assert_eq!(
            ridge.predict(&wide),
            Err(WidthError {
                expected: 1,
                actual: 2
            })
        );
    }

    proptest! {
        #[test]
        fn test_shifting_the_labels_moves_only_the_intercept(
            values in prop::collection::vec(-10.0..10.0f64, 6..30),
            shift in -100.0..100.0f64,
        ) {
            let rows = values.len() / 2;
            let values = &values[..rows * 2];
            let labels: Vec<f64> = values.chunks(2).map(|row| row[0] - row[1]).collect();
            let shifted: Vec<f64> = labels.iter().map(|label| label + shift).collect();
            let penalty = Penalty::new(0.5).unwrap();
            let base = Ridge::fit(&sample(values, 2, &labels), penalty).unwrap();
            let moved = Ridge::fit(&sample(values, 2, &shifted), penalty).unwrap();
            for (left, right) in base.coefficients().iter().zip(moved.coefficients()) {
                prop_assert!((left - right).abs() < 1e-9);
            }
            prop_assert!((moved.intercept() - base.intercept() - shift).abs() < 1e-9);
        }

        #[test]
        fn test_a_larger_penalty_never_lengthens_the_coefficients(
            values in prop::collection::vec(-10.0..10.0f64, 6..30),
            small in 0.01..10.0f64,
            factor in 1.0..100.0f64,
        ) {
            let rows = values.len() / 2;
            let values = &values[..rows * 2];
            let labels: Vec<f64> = values.chunks(2).map(|row| 3.0 * row[0] + row[1]).collect();
            let norm = |ridge: &Ridge| ridge.coefficients().iter().map(|value| value.powi(2)).sum::<f64>();
            let loose = Ridge::fit(&sample(values, 2, &labels), Penalty::new(small).unwrap()).unwrap();
            let tight = Ridge::fit(&sample(values, 2, &labels), Penalty::new(small * factor).unwrap()).unwrap();
            prop_assert!(norm(&tight) <= norm(&loose) * (1.0 + 1e-9) + 1e-12);
        }
    }
}
