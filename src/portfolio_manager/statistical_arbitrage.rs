//! Statistical arbitrage pair selection via correlation and z-score screening.
//!
//! Builds a correlation matrix of ticker log returns over a trailing window,
//! identifies pairs whose correlation falls in the signal band, computes the
//! OLS spread z-score for each candidate pair, and returns the top
//! `TARGET_PAIR_COUNT` pairs by rank score using a greedy no-duplicate-ticker
//! selection.

use std::collections::HashMap;

use crate::portfolio_manager::consolidation::ConsolidatedSignal;
use crate::portfolio_manager::math::{log_returns, ols_slope, pearson_correlation, z_score_last};

/// Trailing window for correlation and z-score computation (trading days).
pub const CORRELATION_WINDOW_DAYS: usize = 60;

/// Minimum absolute Pearson correlation for a pair to be considered.
const CORRELATION_MINIMUM: f64 = 0.5;

/// Maximum absolute Pearson correlation; pairs above this are too similar.
const CORRELATION_MAXIMUM: f64 = 0.95;

/// Z-score magnitude required to enter a position.
const Z_SCORE_ENTRY_THRESHOLD: f64 = 2.0;

/// Minimum ensemble confidence for a ticker to be eligible.
const CONFIDENCE_THRESHOLD: f64 = 0.5;

/// Minimum number of eligible tickers needed to form any pair.
const MINIMUM_TICKER_COUNT: usize = 2;

/// Target number of pairs to return.
pub const TARGET_PAIR_COUNT: usize = 10;

/// A candidate long-short pair identified by the statistical arbitrage screener.
#[derive(Debug, Clone)]
pub struct CandidatePair {
    /// Human-readable identifier combining both tickers, e.g. `"AAPL-MSFT"`.
    pub pair_id: String,
    /// The leg to buy: the relatively cheap ticker.
    pub long_ticker: String,
    /// The leg to sell short: the relatively expensive ticker.
    pub short_ticker: String,
    /// Standard deviations the current spread has diverged from its historical mean.
    pub z_score: f64,
    /// OLS regression slope: shares of the short leg per share of the long leg.
    pub hedge_ratio: f64,
    /// Absolute difference in `ensemble_alpha` between the two legs.
    pub signal_strength: f64,
    /// Realized daily return volatility of the long leg.
    pub long_realized_volatility: f64,
    /// Realized daily return volatility of the short leg.
    pub short_realized_volatility: f64,
}

/// Selects up to `TARGET_PAIR_COUNT` statistical arbitrage pairs from the signals.
///
/// Filters signals by `ensemble_confidence >= CONFIDENCE_THRESHOLD` and
/// `realized_volatility > 0`. Builds a Pearson correlation matrix over the last
/// `CORRELATION_WINDOW_DAYS` of log returns. Pairs are screened by:
/// 1. Correlation in `[CORRELATION_MINIMUM, CORRELATION_MAXIMUM]`
/// 2. Z-score of the OLS spread >= `Z_SCORE_ENTRY_THRESHOLD`
///
/// Pairs are ranked by `|z_score| × signal_strength` and selected greedily
/// (no ticker appears in more than one pair). Returns an empty `Vec` when
/// insufficient data or fewer than `MINIMUM_TICKER_COUNT` eligible tickers.
pub fn select_pairs(
    signals: &[ConsolidatedSignal],
    historical_closes: &HashMap<String, Vec<f64>>,
) -> Vec<CandidatePair> {
    // Filter to confident tickers with valid volatility.
    let eligible: Vec<&ConsolidatedSignal> = signals
        .iter()
        .filter(|s| s.ensemble_confidence >= CONFIDENCE_THRESHOLD && s.realized_volatility > 0.0)
        .collect();

    if eligible.len() < MINIMUM_TICKER_COUNT {
        return Vec::new();
    }

    // Build per-ticker log returns over the correlation window.
    let mut ticker_returns: Vec<(String, Vec<f64>)> = Vec::new();
    for signal in &eligible {
        if let Some(closes) = historical_closes.get(&signal.ticker) {
            let window_closes: &[f64] = if closes.len() > CORRELATION_WINDOW_DAYS {
                &closes[closes.len() - CORRELATION_WINDOW_DAYS..]
            } else {
                closes
            };
            if window_closes.iter().any(|&p| p <= 0.0) {
                continue;
            }
            if window_closes.len() < CORRELATION_WINDOW_DAYS {
                continue;
            }
            let returns = log_returns(window_closes);
            if returns.is_empty() {
                continue;
            }
            let return_std: f64 =
                returns.iter().map(|r| r.powi(2)).sum::<f64>() / returns.len() as f64;
            if return_std < f64::EPSILON {
                continue;
            }
            ticker_returns.push((signal.ticker.clone(), returns));
        }
    }

    if ticker_returns.len() < MINIMUM_TICKER_COUNT {
        return Vec::new();
    }

    // Build a signals lookup for alpha and volatility access.
    let signals_lookup: HashMap<&str, &ConsolidatedSignal> =
        eligible.iter().map(|s| (s.ticker.as_str(), *s)).collect();

    // Candidate pair generation with correlation and z-score screening.
    let mut candidates: Vec<(CandidatePair, f64)> = Vec::new();

    for i in 0..ticker_returns.len() {
        for j in (i + 1)..ticker_returns.len() {
            let (ticker_a, returns_a) = &ticker_returns[i];
            let (ticker_b, returns_b) = &ticker_returns[j];

            let count = returns_a.len().min(returns_b.len());
            let returns_a_aligned = &returns_a[returns_a.len() - count..];
            let returns_b_aligned = &returns_b[returns_b.len() - count..];

            let correlation = pearson_correlation(returns_a_aligned, returns_b_aligned);
            if !(CORRELATION_MINIMUM..=CORRELATION_MAXIMUM).contains(&correlation.abs()) {
                continue;
            }

            // Retrieve log price series for spread computation.
            let closes_a = match historical_closes.get(ticker_a) {
                Some(closes) if closes.len() >= CORRELATION_WINDOW_DAYS => {
                    &closes[closes.len() - CORRELATION_WINDOW_DAYS..]
                }
                _ => continue,
            };
            let closes_b = match historical_closes.get(ticker_b) {
                Some(closes) if closes.len() >= CORRELATION_WINDOW_DAYS => {
                    &closes[closes.len() - CORRELATION_WINDOW_DAYS..]
                }
                _ => continue,
            };

            let log_prices_a: Vec<f64> = closes_a.iter().map(|p| p.ln()).collect();
            let log_prices_b: Vec<f64> = closes_b.iter().map(|p| p.ln()).collect();

            // Replicate Python: slope of log_prices_a on log_prices_b
            let hedge_ratio = ols_slope(&log_prices_b, &log_prices_a);
            if !hedge_ratio.is_finite() {
                continue;
            }

            let spread: Vec<f64> = log_prices_a
                .iter()
                .zip(log_prices_b.iter())
                .map(|(a, b)| a - hedge_ratio * b)
                .collect();

            let current_z_score = z_score_last(&spread);
            if !current_z_score.is_finite() {
                continue;
            }
            if current_z_score.abs() < Z_SCORE_ENTRY_THRESHOLD {
                continue;
            }

            // z > 0: A is expensive → short A, long B.
            let (long_ticker, short_ticker) = if current_z_score > 0.0 {
                (ticker_b.clone(), ticker_a.clone())
            } else {
                (ticker_a.clone(), ticker_b.clone())
            };

            let long_signal = match signals_lookup.get(long_ticker.as_str()) {
                Some(signal) => signal,
                None => continue,
            };
            let short_signal = match signals_lookup.get(short_ticker.as_str()) {
                Some(signal) => signal,
                None => continue,
            };

            let signal_strength = (long_signal.ensemble_alpha - short_signal.ensemble_alpha).abs();
            let rank_score = current_z_score.abs() * signal_strength;

            candidates.push((
                CandidatePair {
                    pair_id: format!("{long_ticker}-{short_ticker}"),
                    long_ticker,
                    short_ticker,
                    z_score: current_z_score.abs(),
                    hedge_ratio,
                    signal_strength,
                    long_realized_volatility: long_signal.realized_volatility,
                    short_realized_volatility: short_signal.realized_volatility,
                },
                rank_score,
            ));
        }
    }

    if candidates.is_empty() {
        return Vec::new();
    }

    // Sort by rank score descending.
    candidates.sort_by(|(_, score_a), (_, score_b)| {
        score_b
            .partial_cmp(score_a)
            .unwrap_or(std::cmp::Ordering::Equal)
    });

    // Greedy selection: each ticker appears in at most one pair.
    let mut used_tickers: std::collections::HashSet<String> = std::collections::HashSet::new();
    let mut selected: Vec<CandidatePair> = Vec::new();

    for (pair, _) in candidates {
        if used_tickers.contains(&pair.long_ticker) || used_tickers.contains(&pair.short_ticker) {
            continue;
        }
        used_tickers.insert(pair.long_ticker.clone());
        used_tickers.insert(pair.short_ticker.clone());
        selected.push(pair);
        if selected.len() >= TARGET_PAIR_COUNT {
            break;
        }
    }

    selected
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_signal(
        ticker: &str,
        alpha: f64,
        confidence: f64,
        volatility: f64,
    ) -> ConsolidatedSignal {
        ConsolidatedSignal {
            ticker: ticker.to_string(),
            ensemble_alpha: alpha,
            ensemble_confidence: confidence,
            realized_volatility: volatility,
            sector: "TECHNOLOGY".to_string(),
        }
    }

    fn make_correlated_prices(
        count: usize,
        base: f64,
        market_factor: &[f64],
        idiosyncratic: f64,
    ) -> Vec<f64> {
        let mut prices = vec![base];
        for (i, &market_return) in market_factor.iter().enumerate().take(count - 1) {
            let last = *prices.last().unwrap();
            let noise = idiosyncratic * ((i as f64).sin() * 0.001);
            prices.push(last * (1.0 + market_return + noise));
        }
        prices
    }

    #[test]
    fn test_select_pairs_empty_signals() {
        assert!(select_pairs(&[], &HashMap::new()).is_empty());
    }

    #[test]
    fn test_select_pairs_insufficient_eligible_signals() {
        // Only one signal above confidence threshold
        let signals = vec![make_signal("AAPL", 0.02, 0.8, 0.01)];
        assert!(select_pairs(&signals, &HashMap::new()).is_empty());
    }

    #[test]
    fn test_select_pairs_below_confidence_threshold_excluded() {
        let signals = vec![
            make_signal("AAPL", 0.02, 0.3, 0.01), // below threshold
            make_signal("MSFT", 0.01, 0.3, 0.01), // below threshold
        ];
        assert!(select_pairs(&signals, &HashMap::new()).is_empty());
    }

    #[test]
    fn test_select_pairs_missing_closes_excluded() {
        let signals = vec![
            make_signal("AAPL", 0.02, 0.8, 0.01),
            make_signal("MSFT", 0.01, 0.8, 0.01),
        ];
        // No closes provided → no pair selected
        assert!(select_pairs(&signals, &HashMap::new()).is_empty());
    }

    #[test]
    fn test_select_pairs_no_ticker_in_multiple_pairs() {
        // Build signals and correlated prices to generate at least one pair,
        // then verify the greedy constraint holds.
        let common_factor: Vec<f64> = (0..70).map(|i| 0.005 * ((i as f64 * 0.3).sin())).collect();

        let mut closes = HashMap::new();
        let tickers = ["AAPL", "MSFT", "GOOG", "AMZN", "META"];
        let signals: Vec<ConsolidatedSignal> = tickers
            .iter()
            .enumerate()
            .map(|(i, &ticker)| {
                let offset = i as f64 * 0.0001;
                let prices =
                    make_correlated_prices(71, 100.0 + i as f64 * 10.0, &common_factor, offset);
                closes.insert(ticker.to_string(), prices);
                make_signal(ticker, 0.01 * (i as f64 + 1.0), 0.9, 0.01)
            })
            .collect();

        let pairs = select_pairs(&signals, &closes);

        // Verify no ticker appears in more than one pair.
        let mut all_tickers = std::collections::HashSet::new();
        for pair in &pairs {
            assert!(
                !all_tickers.contains(&pair.long_ticker),
                "duplicate long ticker"
            );
            assert!(
                !all_tickers.contains(&pair.short_ticker),
                "duplicate short ticker"
            );
            all_tickers.insert(pair.long_ticker.clone());
            all_tickers.insert(pair.short_ticker.clone());
        }
    }

    #[test]
    fn test_select_pairs_z_score_magnitude_matches() {
        // Any returned pair must have z_score >= Z_SCORE_ENTRY_THRESHOLD.
        let common_factor: Vec<f64> = (0..70).map(|i| 0.003 * ((i as f64 * 0.25).sin())).collect();

        let mut closes = HashMap::new();
        let tickers = ["AAPL", "MSFT", "GOOG", "AMZN"];
        let signals: Vec<ConsolidatedSignal> = tickers
            .iter()
            .enumerate()
            .map(|(i, &ticker)| {
                let offset = i as f64 * 0.0002;
                let prices =
                    make_correlated_prices(71, 100.0 + i as f64 * 20.0, &common_factor, offset);
                closes.insert(ticker.to_string(), prices);
                make_signal(ticker, 0.01 * (i as f64 + 1.0), 0.9, 0.012)
            })
            .collect();

        let pairs = select_pairs(&signals, &closes);
        for pair in &pairs {
            assert!(pair.z_score >= Z_SCORE_ENTRY_THRESHOLD);
            assert!(pair.hedge_ratio.is_finite());
            assert!(pair.signal_strength >= 0.0);
        }
    }
}
