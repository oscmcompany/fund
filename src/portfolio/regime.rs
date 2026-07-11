//! Market regime classification from SPY price history.
//!
//! Computes annualized realized volatility and lag-1 autocorrelation of SPY log
//! returns over a trailing window, then classifies the regime as
//! `MeanReversion` (favourable for statistical arbitrage) or `Trending`
//! (momentum-driven; exposure is halved).

use crate::domain::primitives::Percent;
use crate::domain::signals::{Regime, RegimeResult};
use crate::portfolio::math::{log_returns, mean, pearson_correlation, standard_deviation};

/// Trailing window for regime classification (trading days).
pub const REGIME_WINDOW_DAYS: usize = 60;

/// Annualized volatility threshold above which the market is considered trending.
const REGIME_VOLATILITY_THRESHOLD: f64 = 0.20;

/// Lag-1 autocorrelation threshold; negative autocorrelation signals mean reversion.
const REGIME_AUTOCORRELATION_THRESHOLD: f64 = 0.0;

/// Trading days used to annualize daily volatility.
const TRADING_DAYS_PER_YEAR: f64 = 252.0;

/// Minimum number of log returns needed for a reliable regime signal.
const MINIMUM_RETURN_COUNT: usize = 3;

/// Classifies the current market regime from the trailing SPY close prices.
///
/// Uses up to the last `REGIME_WINDOW_DAYS + 1` prices to compute:
/// - Annualized realized volatility (sample std-dev of log returns × √252)
/// - Lag-1 autocorrelation of log returns
///
/// Returns a `Trending` regime with zero confidence when data is insufficient
/// or any price is non-positive.
pub fn classify_regime(spy_close_prices: &[f64]) -> RegimeResult {
    let trending_default = RegimeResult {
        state: Regime::Trending,
        confidence: Percent::new(0.0).expect("0.0 is always a valid Percent"),
    };

    let window = &spy_close_prices[spy_close_prices
        .len()
        .saturating_sub(REGIME_WINDOW_DAYS + 1)..];

    if window.iter().any(|&price| price <= 0.0) {
        return trending_default;
    }

    let returns = log_returns(window);
    if returns.len() < MINIMUM_RETURN_COUNT {
        return trending_default;
    }

    let realized_volatility = standard_deviation(&returns, 1) * TRADING_DAYS_PER_YEAR.sqrt();

    // Lag-1 autocorrelation: correlate returns[:-1] with returns[1:]
    let autocorrelation = pearson_correlation(&returns[..returns.len() - 1], &returns[1..]);

    let low_volatility = realized_volatility < REGIME_VOLATILITY_THRESHOLD;
    let mean_reverting_signal = autocorrelation < REGIME_AUTOCORRELATION_THRESHOLD;

    if low_volatility && mean_reverting_signal {
        let volatility_margin =
            (REGIME_VOLATILITY_THRESHOLD - realized_volatility) / REGIME_VOLATILITY_THRESHOLD;
        let autocorrelation_margin = (-autocorrelation).min(1.0);
        let raw_confidence = ((volatility_margin + autocorrelation_margin) / 2.0).clamp(0.0, 1.0);
        let confidence =
            Percent::new(raw_confidence).expect("clamped value is always a valid Percent");
        return RegimeResult {
            state: Regime::MeanReversion,
            confidence,
        };
    }

    let excess_volatility = ((realized_volatility - REGIME_VOLATILITY_THRESHOLD)
        / REGIME_VOLATILITY_THRESHOLD)
        .max(0.0);
    let excess_autocorrelation = (autocorrelation - REGIME_AUTOCORRELATION_THRESHOLD).max(0.0);
    let raw_confidence = ((excess_volatility + excess_autocorrelation) / 2.0).clamp(0.0, 1.0);
    let confidence = Percent::new(raw_confidence).expect("clamped value is always a valid Percent");
    RegimeResult {
        state: Regime::Trending,
        confidence,
    }
}

/// Returns the realized annualized volatility from the trailing window of SPY prices.
///
/// Used for drawdown and risk monitoring. Returns `None` when data is insufficient.
pub fn annualized_volatility(spy_close_prices: &[f64]) -> Option<f64> {
    let window = &spy_close_prices[spy_close_prices
        .len()
        .saturating_sub(REGIME_WINDOW_DAYS + 1)..];
    if window.iter().any(|&price| price <= 0.0) {
        return None;
    }
    let returns = log_returns(window);
    if returns.len() < MINIMUM_RETURN_COUNT {
        return None;
    }
    let mean_value = mean(&returns);
    if !mean_value.is_finite() {
        return None;
    }
    Some(standard_deviation(&returns, 1) * TRADING_DAYS_PER_YEAR.sqrt())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_classify_regime_empty_returns_trending() {
        let result = classify_regime(&[]);
        assert_eq!(result.state, Regime::Trending);
        assert_eq!(result.confidence.value(), 0.0);
    }

    #[test]
    fn test_classify_regime_zero_price_returns_trending() {
        let prices = [100.0, 0.0, 100.0];
        let result = classify_regime(&prices);
        assert_eq!(result.state, Regime::Trending);
        assert_eq!(result.confidence.value(), 0.0);
    }

    #[test]
    fn test_classify_regime_negative_price_returns_trending() {
        let prices = [100.0, -50.0, 100.0];
        let result = classify_regime(&prices);
        assert_eq!(result.state, Regime::Trending);
        assert_eq!(result.confidence.value(), 0.0);
    }

    #[test]
    fn test_classify_regime_insufficient_data_returns_trending() {
        // Need at least MINIMUM_RETURN_COUNT = 3 returns → 4 prices
        let result = classify_regime(&[100.0, 101.0, 102.0]);
        assert_eq!(result.state, Regime::Trending);
        assert_eq!(result.confidence.value(), 0.0);
    }

    #[test]
    fn test_classify_regime_stable_prices_classify_as_mean_reversion() {
        // Very small daily returns → low volatility
        // Alternating +/- returns → negative autocorrelation → mean reversion
        let mut prices = vec![100.0];
        for i in 0..70 {
            let last = *prices.last().unwrap();
            // Alternating small returns
            let sign = if i % 2 == 0 { 1.0 } else { -1.0 };
            prices.push(last * (1.0 + sign * 0.001));
        }
        let result = classify_regime(&prices);
        assert_eq!(result.state, Regime::MeanReversion);
        assert!(result.confidence.value() > 0.0);
    }

    #[test]
    fn test_classify_regime_high_volatility_trending() {
        // Alternating large shocks → non-trivial volatility well above REGIME_VOLATILITY_THRESHOLD
        let mut prices = vec![100.0];
        for index in 0..70 {
            let last = *prices.last().unwrap();
            let shock = if index % 2 == 0 { 0.08 } else { -0.06 };
            prices.push(last * (1.0 + shock));
        }
        let result = classify_regime(&prices);
        assert_eq!(result.state, Regime::Trending);
    }

    #[test]
    fn test_classify_regime_uses_trailing_window() {
        // Prepend many flat prices then add volatile prices; should use trailing window
        let mut prices = vec![100.0; 50]; // flat prefix
        let last = *prices.last().unwrap();
        for _ in 0..70 {
            prices.push(last * 1.05); // monotone large returns for trailing portion
        }
        let result = classify_regime(&prices);
        assert_eq!(result.state, Regime::Trending);
    }

    #[test]
    fn test_annualized_volatility_empty() {
        assert!(annualized_volatility(&[]).is_none());
    }

    #[test]
    fn test_annualized_volatility_non_positive_price() {
        assert!(annualized_volatility(&[100.0, 0.0, 100.0]).is_none());
    }

    #[test]
    fn test_annualized_volatility_returns_positive() {
        // Alternating small shocks → non-zero variance so annualized volatility is positive
        let mut prices = vec![100.0];
        for index in 0..70 {
            let last = *prices.last().unwrap();
            let shock = if index % 2 == 0 { 0.01 } else { -0.009 };
            prices.push(last * (1.0 + shock));
        }
        let result = annualized_volatility(&prices);
        assert!(result.is_some());
        assert!(result.unwrap() > 0.0);
    }
}
