//! Pure statistical math utilities for portfolio analysis.
//!
//! All functions operate on plain `&[f64]` slices so the logic stays
//! dependency-free and easy to unit-test in isolation.

/// Computes log returns from a price series: `ln(price[i+1] / price[i])`.
///
/// Returns an empty `Vec` when fewer than two prices are provided.
pub fn log_returns(prices: &[f64]) -> Vec<f64> {
    if prices.len() < 2 {
        return Vec::new();
    }
    prices
        .windows(2)
        .map(|window| (window[1] / window[0]).ln())
        .collect()
}

/// Computes the arithmetic mean of a slice.
///
/// Returns `0.0` for an empty slice.
pub fn mean(values: &[f64]) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    values.iter().sum::<f64>() / values.len() as f64
}

/// Computes the variance with the given degrees-of-freedom correction `ddof`.
///
/// Returns `0.0` when `values.len() <= ddof`.
pub fn variance(values: &[f64], ddof: usize) -> f64 {
    if values.len() <= ddof {
        return 0.0;
    }
    let m = mean(values);
    let sum_of_squares: f64 = values.iter().map(|value| (value - m).powi(2)).sum();
    sum_of_squares / (values.len() - ddof) as f64
}

/// Computes the standard deviation with the given degrees-of-freedom correction.
pub fn standard_deviation(values: &[f64], ddof: usize) -> f64 {
    variance(values, ddof).sqrt()
}

/// Computes the Pearson correlation coefficient.
///
/// Returns `0.0` when either slice is empty, the slices have different lengths,
/// or either standard deviation is zero.
pub fn pearson_correlation(x: &[f64], y: &[f64]) -> f64 {
    if x.len() != y.len() || x.is_empty() {
        return 0.0;
    }
    let x_mean = mean(x);
    let y_mean = mean(y);
    let covariance: f64 = x
        .iter()
        .zip(y.iter())
        .map(|(xi, yi)| (xi - x_mean) * (yi - y_mean))
        .sum::<f64>()
        / x.len() as f64;
    let x_standard_deviation = standard_deviation(x, 0);
    let y_standard_deviation = standard_deviation(y, 0);
    if x_standard_deviation < f64::EPSILON || y_standard_deviation < f64::EPSILON {
        return 0.0;
    }
    covariance / (x_standard_deviation * y_standard_deviation)
}

/// Computes the OLS regression slope of `y` on `x`.
///
/// Equivalent to `np.polyfit(x, y, 1)[0]`. Returns `0.0` when the variance of
/// `x` is zero or the slices differ in length.
pub fn ols_slope(x: &[f64], y: &[f64]) -> f64 {
    if x.len() != y.len() || x.is_empty() {
        return 0.0;
    }
    let x_mean = mean(x);
    let y_mean = mean(y);
    let numerator: f64 = x
        .iter()
        .zip(y.iter())
        .map(|(xi, yi)| (xi - x_mean) * (yi - y_mean))
        .sum();
    let denominator: f64 = x.iter().map(|xi| (xi - x_mean).powi(2)).sum();
    if denominator.abs() < f64::EPSILON {
        return 0.0;
    }
    numerator / denominator
}

/// Computes the z-score of the last element in `spread` relative to the full series.
///
/// Uses population standard deviation (`ddof=0`), matching `scipy.stats.zscore`.
/// Returns `0.0` when the spread has fewer than two elements or has zero standard
/// deviation.
pub fn z_score_last(spread: &[f64]) -> f64 {
    if spread.len() < 2 {
        return 0.0;
    }
    let m = mean(spread);
    let deviation = standard_deviation(spread, 0);
    if deviation.abs() < f64::EPSILON {
        return 0.0;
    }
    (spread[spread.len() - 1] - m) / deviation
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_log_returns_empty_prices() {
        assert!(log_returns(&[]).is_empty());
    }

    #[test]
    fn test_log_returns_single_price() {
        assert!(log_returns(&[100.0]).is_empty());
    }

    #[test]
    fn test_log_returns_two_prices() {
        let returns = log_returns(&[100.0, 110.0]);
        assert_eq!(returns.len(), 1);
        assert!((returns[0] - (110.0_f64 / 100.0).ln()).abs() < 1e-10);
    }

    #[test]
    fn test_log_returns_flat_prices() {
        let returns = log_returns(&[100.0, 100.0, 100.0]);
        assert_eq!(returns.len(), 2);
        for return_value in &returns {
            assert!(return_value.abs() < f64::EPSILON);
        }
    }

    #[test]
    fn test_mean_empty() {
        assert_eq!(mean(&[]), 0.0);
    }

    #[test]
    fn test_mean_single() {
        assert!((mean(&[5.0]) - 5.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_mean_multiple() {
        assert!((mean(&[1.0, 2.0, 3.0]) - 2.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_variance_empty() {
        assert_eq!(variance(&[], 1), 0.0);
    }

    #[test]
    fn test_variance_too_few_for_ddof() {
        // Only 1 value with ddof=1 → 0.0
        assert_eq!(variance(&[5.0], 1), 0.0);
    }

    #[test]
    fn test_variance_population() {
        // variance([2, 4, 4, 4, 5, 5, 7, 9], ddof=0) = 4.0
        let values = [2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0];
        assert!((variance(&values, 0) - 4.0).abs() < 1e-10);
    }

    #[test]
    fn test_variance_sample() {
        let values = [2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0];
        // sample variance (ddof=1) = 32/7 ≈ 4.571
        let expected = 32.0 / 7.0;
        assert!((variance(&values, 1) - expected).abs() < 1e-10);
    }

    #[test]
    fn test_standard_deviation_zero_variance() {
        assert!((standard_deviation(&[3.0, 3.0, 3.0], 0)).abs() < f64::EPSILON);
    }

    #[test]
    fn test_standard_deviation_positive() {
        // standard_deviation([1, 2, 3], ddof=0) = sqrt(2/3)
        let values = [1.0, 2.0, 3.0];
        let expected = (2.0_f64 / 3.0).sqrt();
        assert!((standard_deviation(&values, 0) - expected).abs() < 1e-10);
    }

    #[test]
    fn test_pearson_correlation_empty() {
        assert_eq!(pearson_correlation(&[], &[]), 0.0);
    }

    #[test]
    fn test_pearson_correlation_different_lengths() {
        assert_eq!(pearson_correlation(&[1.0, 2.0], &[1.0]), 0.0);
    }

    #[test]
    fn test_pearson_correlation_zero_standard_deviation() {
        assert_eq!(pearson_correlation(&[5.0, 5.0], &[1.0, 2.0]), 0.0);
    }

    #[test]
    fn test_pearson_correlation_perfect_positive() {
        let x = [1.0, 2.0, 3.0, 4.0];
        let y = [2.0, 4.0, 6.0, 8.0];
        assert!((pearson_correlation(&x, &y) - 1.0).abs() < 1e-10);
    }

    #[test]
    fn test_pearson_correlation_perfect_negative() {
        let x = [1.0, 2.0, 3.0, 4.0];
        let y = [4.0, 3.0, 2.0, 1.0];
        assert!((pearson_correlation(&x, &y) + 1.0).abs() < 1e-10);
    }

    #[test]
    fn test_ols_slope_empty() {
        assert_eq!(ols_slope(&[], &[]), 0.0);
    }

    #[test]
    fn test_ols_slope_different_lengths() {
        assert_eq!(ols_slope(&[1.0, 2.0], &[1.0]), 0.0);
    }

    #[test]
    fn test_ols_slope_zero_variance_in_x() {
        assert_eq!(ols_slope(&[5.0, 5.0], &[1.0, 2.0]), 0.0);
    }

    #[test]
    fn test_ols_slope_exact_linear_relationship() {
        // y = 2x + 1: slope should be 2.0
        let x = [1.0, 2.0, 3.0, 4.0, 5.0];
        let y: Vec<f64> = x.iter().map(|xi| 2.0 * xi + 1.0).collect();
        assert!((ols_slope(&x, &y) - 2.0).abs() < 1e-10);
    }

    #[test]
    fn test_ols_slope_inverse_relationship() {
        // y = -x: slope should be -1.0
        let x = [1.0, 2.0, 3.0];
        let y: Vec<f64> = x.iter().map(|xi| -xi).collect();
        assert!((ols_slope(&x, &y) + 1.0).abs() < 1e-10);
    }

    #[test]
    fn test_z_score_last_too_few_values() {
        assert_eq!(z_score_last(&[]), 0.0);
        assert_eq!(z_score_last(&[5.0]), 0.0);
    }

    #[test]
    fn test_z_score_last_zero_standard_deviation() {
        assert_eq!(z_score_last(&[3.0, 3.0, 3.0]), 0.0);
    }

    #[test]
    fn test_z_score_last_symmetric() {
        // Values centered at 0 with standard_deviation = 1.0
        let values = [-1.0, 0.0, 1.0];
        let result = z_score_last(&values);
        // mean=0, standard_deviation(ddof=0)=sqrt(2/3), z_score(1.0) = 1.0/sqrt(2/3)
        let expected = 1.0 / (2.0_f64 / 3.0).sqrt();
        assert!((result - expected).abs() < 1e-10);
    }

    #[test]
    fn test_z_score_last_mean_value() {
        // Last value equals the mean → z-score should be 0
        let values = [1.0, 3.0, 2.0]; // mean = 2.0, last = 2.0
        assert!(z_score_last(&values).abs() < 1e-10);
    }
}
