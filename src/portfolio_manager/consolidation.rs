//! Consolidation of ensemble model predictions into per-ticker trading signals.
//!
//! Blends TiDE quantile predictions into `ConsolidatedSignal` values that carry
//! the expected forward return, ensemble confidence, realized daily volatility,
//! and GICS sector for each ticker.

use std::collections::HashMap;

use crate::domain::predictions::EquityPrediction;
use crate::portfolio_manager::math::standard_deviation;

/// Trailing window for realized volatility (trading days of percentage returns).
const VOLATILITY_WINDOW_DAYS: usize = 20;

/// A per-ticker signal produced by blending the ensemble model predictions.
#[derive(Debug, Clone)]
pub struct ConsolidatedSignal {
    /// Normalized US equity ticker symbol.
    pub ticker: String,
    /// Mean expected forward return across all quantile predictions (quantile_50).
    pub ensemble_alpha: f64,
    /// Confidence in this signal, normalized to the most confident ticker.
    ///
    /// Derived from the quantile spread width: a narrow spread signals high model
    /// agreement. Range `[0.0, 1.0]`.
    pub ensemble_confidence: f64,
    /// Trailing daily return standard deviation over the last `VOLATILITY_WINDOW_DAYS` days.
    pub realized_volatility: f64,
    /// GICS sector string, or `"NOT AVAILABLE"` when sector data is absent.
    pub sector: String,
}

/// Blends the latest `EquityPrediction` rows into per-ticker signals.
///
/// For each ticker, takes the most recent prediction (by `timestamp`) and computes:
/// - `ensemble_alpha` = `quantile_50`
/// - `ensemble_confidence` = normalized inverse of the quantile spread width
/// - `realized_volatility` = sample std-dev of the last 20 daily pct-returns
///
/// Tickers without historical close prices or with zero/negative returns are
/// excluded. Tickers whose `realized_volatility` cannot be computed are dropped.
///
/// `historical_closes` maps ticker → ordered close prices (oldest to newest).
/// `equity_details` maps ticker → sector string.
pub fn consolidate_predictions(
    predictions: &[EquityPrediction],
    historical_closes: &HashMap<String, Vec<f64>>,
    equity_details: &HashMap<String, String>,
) -> Vec<ConsolidatedSignal> {
    if predictions.is_empty() {
        return Vec::new();
    }

    // Keep the latest prediction per ticker (highest timestamp).
    let mut latest_per_ticker: HashMap<String, &EquityPrediction> = HashMap::new();
    for prediction in predictions {
        let entry = latest_per_ticker
            .entry(prediction.ticker().to_string())
            .or_insert(prediction);
        if prediction.timestamp() > entry.timestamp() {
            *entry = prediction;
        }
    }

    // Compute raw_confidence = 1.0 / (1.0 + max(0, spread_width))
    let mut ticker_signals: Vec<(String, f64, f64)> = latest_per_ticker
        .iter()
        .map(|(ticker, prediction)| {
            let alpha = prediction.quantile_50();
            let spread_width = (prediction.quantile_90() - prediction.quantile_10()).max(0.0);
            let raw_confidence = 1.0 / (1.0 + spread_width);
            (ticker.clone(), alpha, raw_confidence)
        })
        .collect();

    if ticker_signals.is_empty() {
        return Vec::new();
    }

    // Normalize raw_confidence by the maximum so the most confident ticker gets 1.0.
    let maximum_raw_confidence = ticker_signals
        .iter()
        .map(|(_, _, confidence)| *confidence)
        .fold(f64::NEG_INFINITY, f64::max);
    let normalizer = if maximum_raw_confidence > 0.0 {
        maximum_raw_confidence
    } else {
        1.0
    };

    ticker_signals.iter_mut().for_each(|(_, _, confidence)| {
        *confidence /= normalizer;
    });

    // Build consolidated signals, dropping tickers without usable volatility.
    ticker_signals
        .into_iter()
        .filter_map(|(ticker, ensemble_alpha, ensemble_confidence)| {
            let realized_volatility = compute_realized_volatility(historical_closes, &ticker)?;
            let sector = equity_details
                .get(&ticker)
                .cloned()
                .unwrap_or_else(|| "NOT AVAILABLE".to_string());
            Some(ConsolidatedSignal {
                ticker,
                ensemble_alpha,
                ensemble_confidence,
                realized_volatility,
                sector,
            })
        })
        .collect()
}

/// Computes the realized daily return volatility for `ticker` from its close prices.
///
/// Returns `None` when fewer than two close prices are available, any close price is
/// non-positive, or the computed std-dev is not finite.
fn compute_realized_volatility(
    historical_closes: &HashMap<String, Vec<f64>>,
    ticker: &str,
) -> Option<f64> {
    let closes = historical_closes.get(ticker)?;
    if closes.len() < 2 {
        return None;
    }
    if closes.iter().any(|&close_price| close_price <= 0.0) {
        return None;
    }

    // Percentage daily returns: (price[i] - price[i-1]) / price[i-1]
    let all_returns: Vec<f64> = closes
        .windows(2)
        .map(|window| (window[1] - window[0]) / window[0])
        .collect();

    if all_returns.is_empty() {
        return None;
    }

    // Take the last VOLATILITY_WINDOW_DAYS returns.
    let window_returns: &[f64] = if all_returns.len() > VOLATILITY_WINDOW_DAYS {
        &all_returns[all_returns.len() - VOLATILITY_WINDOW_DAYS..]
    } else {
        &all_returns
    };

    let volatility = standard_deviation(window_returns, 1);
    if volatility.is_finite() && volatility > 0.0 {
        Some(volatility)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use uuid::Uuid;

    fn make_prediction(
        ticker: &str,
        quantile_10: f64,
        quantile_50: f64,
        quantile_90: f64,
    ) -> EquityPrediction {
        EquityPrediction::new(
            Uuid::new_v4(),
            "run-001".to_string(),
            ticker.to_string(),
            Utc::now(),
            quantile_10,
            quantile_50,
            quantile_90,
            Utc::now(),
        )
    }

    fn make_closes(count: usize, base: f64, daily_return: f64) -> Vec<f64> {
        let mut prices = vec![base];
        for _ in 1..count {
            let last = *prices.last().unwrap();
            prices.push(last * (1.0 + daily_return));
        }
        prices
    }

    #[test]
    fn test_consolidate_predictions_empty_returns_empty() {
        let result = consolidate_predictions(&[], &HashMap::new(), &HashMap::new());
        assert!(result.is_empty());
    }

    #[test]
    fn test_consolidate_predictions_drops_ticker_without_closes() {
        let predictions = vec![make_prediction("AAPL", -0.01, 0.02, 0.05)];
        let closes = HashMap::new(); // no closes for AAPL
        let details = HashMap::new();
        let result = consolidate_predictions(&predictions, &closes, &details);
        assert!(result.is_empty());
    }

    #[test]
    fn test_consolidate_predictions_single_ticker() {
        let predictions = vec![make_prediction("AAPL", -0.01, 0.02, 0.05)];
        let mut closes = HashMap::new();
        closes.insert("AAPL".to_string(), make_closes(25, 150.0, 0.001));
        let mut details = HashMap::new();
        details.insert("AAPL".to_string(), "TECHNOLOGY".to_string());

        let result = consolidate_predictions(&predictions, &closes, &details);
        assert_eq!(result.len(), 1);
        let signal = &result[0];
        assert_eq!(signal.ticker, "AAPL");
        assert!((signal.ensemble_alpha - 0.02).abs() < 1e-10);
        // Most confident ticker gets confidence = 1.0
        assert!((signal.ensemble_confidence - 1.0).abs() < 1e-10);
        assert!(signal.realized_volatility > 0.0);
        assert_eq!(signal.sector, "TECHNOLOGY");
    }

    #[test]
    fn test_consolidate_predictions_missing_sector_defaults_to_not_available() {
        let predictions = vec![make_prediction("AAPL", -0.01, 0.02, 0.05)];
        let mut closes = HashMap::new();
        closes.insert("AAPL".to_string(), make_closes(25, 150.0, 0.001));
        let details = HashMap::new(); // no sector

        let result = consolidate_predictions(&predictions, &closes, &details);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].sector, "NOT AVAILABLE");
    }

    #[test]
    fn test_consolidate_predictions_normalizes_confidence() {
        // Two predictions: one with narrow spread (high confidence), one wide.
        let mut closes = HashMap::new();
        closes.insert("AAPL".to_string(), make_closes(25, 150.0, 0.001));
        closes.insert("MSFT".to_string(), make_closes(25, 200.0, 0.001));
        let details = HashMap::new();

        let narrow_spread = make_prediction("AAPL", 0.019, 0.02, 0.021); // narrow → high confidence
        let wide_spread = make_prediction("MSFT", -0.10, 0.02, 0.30); // wide → low confidence

        let result = consolidate_predictions(&[narrow_spread, wide_spread], &closes, &details);
        assert_eq!(result.len(), 2);

        let aapl_signal = result.iter().find(|s| s.ticker == "AAPL").unwrap();
        let msft_signal = result.iter().find(|s| s.ticker == "MSFT").unwrap();

        // AAPL has narrower spread → higher confidence
        assert!(aapl_signal.ensemble_confidence > msft_signal.ensemble_confidence);
        // At least one signal has confidence = 1.0 (the most confident ticker)
        let maximum_confidence = result
            .iter()
            .map(|s| s.ensemble_confidence)
            .fold(f64::NEG_INFINITY, f64::max);
        assert!((maximum_confidence - 1.0).abs() < 1e-10);
    }

    #[test]
    fn test_consolidate_predictions_uses_latest_prediction_per_ticker() {
        use chrono::Duration;
        // Two predictions for AAPL: one older, one newer
        let older = EquityPrediction::new(
            Uuid::new_v4(),
            "run-001".to_string(),
            "AAPL".to_string(),
            Utc::now() - Duration::hours(1),
            -0.05,
            -0.01, // older has negative alpha
            0.02,
            Utc::now(),
        );
        let newer = EquityPrediction::new(
            Uuid::new_v4(),
            "run-001".to_string(),
            "AAPL".to_string(),
            Utc::now(),
            0.01,
            0.03, // newer has positive alpha
            0.06,
            Utc::now(),
        );

        let mut closes = HashMap::new();
        closes.insert("AAPL".to_string(), make_closes(25, 150.0, 0.001));
        let details = HashMap::new();

        let result = consolidate_predictions(&[older, newer], &closes, &details);
        assert_eq!(result.len(), 1);
        // Should use the newer prediction's quantile_50 = 0.03
        assert!((result[0].ensemble_alpha - 0.03).abs() < 1e-10);
    }

    #[test]
    fn test_compute_realized_volatility_insufficient_closes() {
        let mut closes = HashMap::new();
        closes.insert("AAPL".to_string(), vec![100.0]); // only one price
        assert!(compute_realized_volatility(&closes, "AAPL").is_none());
    }

    #[test]
    fn test_compute_realized_volatility_nonpositive_price_excluded() {
        let mut closes = HashMap::new();
        closes.insert("AAPL".to_string(), vec![100.0, 0.0, 100.0]);
        // Any non-positive price causes the entire series to be rejected.
        assert!(compute_realized_volatility(&closes, "AAPL").is_none());
    }

    #[test]
    fn test_compute_realized_volatility_flat_prices_returns_none() {
        let mut closes = HashMap::new();
        closes.insert("AAPL".to_string(), vec![100.0; 25]);
        // All returns are 0 → standard_deviation = 0 → should be filtered out
        assert!(compute_realized_volatility(&closes, "AAPL").is_none());
    }

    #[test]
    fn test_compute_realized_volatility_returns_positive() {
        let mut closes = HashMap::new();
        closes.insert("AAPL".to_string(), make_closes(30, 100.0, 0.01));
        let result = compute_realized_volatility(&closes, "AAPL");
        assert!(result.is_some());
        assert!(result.unwrap() > 0.0);
    }
}
