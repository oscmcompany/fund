//! Market beta estimation via OLS regression of ticker log returns on SPY log returns.

use std::collections::HashMap;

use crate::portfolio_manager::math::{log_returns, ols_slope};

/// Trailing window for beta estimation (trading days).
pub const BETA_WINDOW_DAYS: usize = 60;

/// Minimum number of log returns needed to produce a reliable beta estimate.
const MINIMUM_RETURN_COUNT: usize = 2;

/// Computes the market beta of each ticker against SPY over the trailing window.
///
/// Market beta measures a stock's sensitivity to broad market moves; values near
/// `1.0` track the market closely, values above `1.0` amplify moves, and negative
/// values indicate counter-cyclical behaviour.
///
/// Tickers with fewer than `MINIMUM_RETURN_COUNT` returns, any non-positive close
/// prices, or where the aligned SPY/ticker return window is too short are silently
/// skipped and will be absent from the returned map.
///
/// `ticker_closes` maps ticker → ordered close prices (oldest to newest).
/// `spy_closes` is the ordered SPY close price series.
pub fn compute_market_betas(
    ticker_closes: &HashMap<String, Vec<f64>>,
    spy_closes: &[f64],
) -> HashMap<String, f64> {
    let spy_window = &spy_closes[spy_closes.len().saturating_sub(BETA_WINDOW_DAYS + 1)..];

    if spy_window.iter().any(|&price| price <= 0.0) {
        return HashMap::new();
    }

    let spy_returns = log_returns(spy_window);
    if spy_returns.len() < MINIMUM_RETURN_COUNT {
        return HashMap::new();
    }

    let mut betas = HashMap::new();

    for (ticker, closes) in ticker_closes {
        let ticker_window = &closes[closes.len().saturating_sub(BETA_WINDOW_DAYS + 1)..];

        if ticker_window.iter().any(|&price| price <= 0.0) {
            continue;
        }

        let ticker_returns = log_returns(ticker_window);
        if ticker_returns.len() < MINIMUM_RETURN_COUNT {
            continue;
        }

        // Align the two return series to the same length (take the shorter suffix).
        let count = spy_returns.len().min(ticker_returns.len());
        if count < MINIMUM_RETURN_COUNT {
            continue;
        }

        let spy_aligned = &spy_returns[spy_returns.len() - count..];
        let ticker_aligned = &ticker_returns[ticker_returns.len() - count..];

        let beta = ols_slope(spy_aligned, ticker_aligned);
        if beta.is_finite() {
            betas.insert(ticker.clone(), beta);
        }
    }

    betas
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_prices(count: usize, base: f64, daily_return: f64) -> Vec<f64> {
        let mut prices = vec![base];
        for _ in 1..count {
            let last = *prices.last().unwrap();
            prices.push(last * (1.0 + daily_return));
        }
        prices
    }

    /// Builds a price series with sinusoidally varying returns so that log returns
    /// have non-zero variance. Required for OLS slope tests to be well-defined.
    fn make_varying_prices(count: usize, base: f64, amplitude: f64) -> Vec<f64> {
        let mut prices = vec![base];
        for index in 1..count {
            let last = *prices.last().unwrap();
            let daily_return = amplitude * ((index as f64 * 0.4).sin());
            prices.push(last * (1.0 + daily_return));
        }
        prices
    }

    #[test]
    fn test_compute_market_betas_empty_spy_returns_empty() {
        let tickers = HashMap::new();
        assert!(compute_market_betas(&tickers, &[]).is_empty());
    }

    #[test]
    fn test_compute_market_betas_zero_spy_price_returns_empty() {
        let mut tickers = HashMap::new();
        tickers.insert("AAPL".to_string(), make_prices(70, 150.0, 0.001));
        assert!(compute_market_betas(&tickers, &[100.0, 0.0, 100.0]).is_empty());
    }

    #[test]
    fn test_compute_market_betas_insufficient_spy_returns_empty() {
        let mut tickers = HashMap::new();
        tickers.insert("AAPL".to_string(), make_prices(70, 150.0, 0.001));
        // Only one SPY price → zero returns
        assert!(compute_market_betas(&tickers, &[100.0]).is_empty());
    }

    #[test]
    fn test_compute_market_betas_skips_ticker_with_nonpositive_price() {
        let spy = make_prices(70, 400.0, 0.001);
        let mut tickers = HashMap::new();
        tickers.insert("BAD".to_string(), vec![100.0, 0.0, 100.0]);
        let result = compute_market_betas(&tickers, &spy);
        assert!(!result.contains_key("BAD"));
    }

    #[test]
    fn test_compute_market_betas_skips_ticker_with_too_few_prices() {
        let spy = make_prices(70, 400.0, 0.001);
        let mut tickers = HashMap::new();
        tickers.insert("TINY".to_string(), vec![100.0]); // only 1 price → 0 returns
        let result = compute_market_betas(&tickers, &spy);
        assert!(!result.contains_key("TINY"));
    }

    #[test]
    fn test_compute_market_betas_perfect_correlation_beta_one() {
        // Ticker moves identically to SPY → beta = 1.0 exactly.
        // Uses sinusoidal returns so log returns have non-zero variance.
        let spy = make_varying_prices(70, 400.0, 0.005);
        let mut tickers = HashMap::new();
        tickers.insert("SAME".to_string(), spy.clone());
        let result = compute_market_betas(&tickers, &spy);
        assert!(result.contains_key("SAME"));
        assert!((result["SAME"] - 1.0).abs() < 1e-6);
    }

    #[test]
    fn test_compute_market_betas_double_return_beta_two() {
        // Ticker log returns are exactly 2× SPY log returns → beta = 2.0.
        // Uses sinusoidal returns so log returns have non-zero variance.
        let spy = make_varying_prices(70, 400.0, 0.005);
        let spy_log_returns: Vec<f64> = spy
            .windows(2)
            .map(|window| (window[1] / window[0]).ln())
            .collect();

        // Build LEVERAGED by applying 2× log returns starting from 100.
        let mut leveraged = vec![100.0_f64];
        for &log_return in &spy_log_returns {
            let last = *leveraged.last().unwrap();
            leveraged.push(last * (2.0 * log_return).exp());
        }

        let mut tickers = HashMap::new();
        tickers.insert("LEVERAGED".to_string(), leveraged);
        let result = compute_market_betas(&tickers, &spy);
        assert!(result.contains_key("LEVERAGED"));
        assert!((result["LEVERAGED"] - 2.0).abs() < 1e-6);
    }

    #[test]
    fn test_compute_market_betas_multiple_tickers() {
        let spy = make_prices(70, 400.0, 0.003);
        let mut tickers = HashMap::new();
        tickers.insert("AAPL".to_string(), make_prices(70, 150.0, 0.004));
        tickers.insert("MSFT".to_string(), make_prices(70, 300.0, 0.002));
        let result = compute_market_betas(&tickers, &spy);
        assert!(result.contains_key("AAPL"));
        assert!(result.contains_key("MSFT"));
        for beta in result.values() {
            assert!(beta.is_finite());
        }
    }
}
