//! Regime classification and signal gating types.

use crate::domain::market::Ticker;
use crate::domain::primitives::Percent;
use serde::{Deserialize, Serialize};

/// Regime state controlling position exposure scaling.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Regime {
    /// Momentum-driven market. Position exposure is scaled to 0.5x.
    Trending,
    /// Mean-reverting market. Position exposure is scaled to 1.0x.
    MeanReversion,
}

impl Regime {
    /// Returns the position size exposure multiplier for this regime.
    ///
    /// `Trending` halves exposure to reduce risk during momentum markets.
    /// `MeanReversion` uses full exposure when the stat-arb signal is strong.
    pub fn exposure_factor(&self) -> f64 {
        match self {
            Regime::Trending => 0.5,
            Regime::MeanReversion => 1.0,
        }
    }
}

/// Output of the regime classifier.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegimeResult {
    pub state: Regime,
    /// Classifier confidence in the regime state, in `[0.0, 1.0]`.
    pub confidence: Percent,
}

/// Minimum regime confidence required for signal processing to proceed.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct ConfidenceFloor(pub Percent);

/// A single equity signal from the ensemble model.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Signal {
    pub ticker: Ticker,
    /// Ensemble confidence in this signal, in `[0.0, 1.0]`.
    pub confidence: Percent,
    /// Predicted price return for the next period.
    pub predicted_return: f64,
}

/// Error returned when `GatedSignals::new()` rejects the input.
#[derive(Debug, Clone, PartialEq)]
pub enum SignalGateError {
    /// Signals vector was empty.
    EmptySignals,
    /// Regime confidence is below the configured floor.
    BelowConfidenceFloor { confidence: f64, floor: f64 },
    /// All signals have identical predicted returns, producing a degenerate
    /// quantile spread where normalization would assign uniform confidence.
    DegenerateQuantileSpread,
    /// At least one signal has a non-finite (`NaN` or `±inf`) predicted return.
    NonFiniteReturn,
}

impl std::fmt::Display for SignalGateError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SignalGateError::EmptySignals => write!(formatter, "Signals vector is empty."),
            SignalGateError::BelowConfidenceFloor { confidence, floor } => write!(
                formatter,
                "Regime confidence {:.4} is below floor {:.4}.",
                confidence, floor
            ),
            SignalGateError::DegenerateQuantileSpread => write!(
                formatter,
                "All predicted returns are identical; quantile spread is degenerate."
            ),
            SignalGateError::NonFiniteReturn => write!(
                formatter,
                "One or more signals contain a non-finite predicted return."
            ),
        }
    }
}

impl std::error::Error for SignalGateError {}

/// Signals that have passed all gate checks.
///
/// Constructed only via `::new()`, which enforces:
/// - non-empty signal list
/// - regime confidence above the configured floor
/// - all `predicted_return` values are finite (no `NaN` or `±inf`)
/// - non-degenerate quantile spread across predicted returns
///
/// Fields are private; use the provided accessors to read values.
#[derive(Debug)]
pub struct GatedSignals {
    signals: Vec<Signal>,
    regime: Regime,
    confidence: Percent,
}

impl GatedSignals {
    /// Constructs `GatedSignals` after validating all gate invariants.
    ///
    /// Returns `Err` if signals is empty, regime confidence is below `floor`,
    /// any signal has a non-finite `predicted_return`, or all signals have
    /// identical `predicted_return` values.
    pub fn new(
        signals: Vec<Signal>,
        regime_result: RegimeResult,
        floor: ConfidenceFloor,
    ) -> Result<Self, SignalGateError> {
        if signals.is_empty() {
            return Err(SignalGateError::EmptySignals);
        }

        let regime_confidence = regime_result.confidence.value();
        let confidence_floor = floor.0.value();
        if regime_confidence < confidence_floor {
            return Err(SignalGateError::BelowConfidenceFloor {
                confidence: regime_confidence,
                floor: confidence_floor,
            });
        }

        if signals
            .iter()
            .any(|signal| !signal.predicted_return.is_finite())
        {
            return Err(SignalGateError::NonFiniteReturn);
        }

        let maximum_return = signals
            .iter()
            .map(|signal| signal.predicted_return)
            .fold(f64::NEG_INFINITY, f64::max);
        let minimum_return = signals
            .iter()
            .map(|signal| signal.predicted_return)
            .fold(f64::INFINITY, f64::min);

        if (maximum_return - minimum_return).abs() <= f64::EPSILON {
            return Err(SignalGateError::DegenerateQuantileSpread);
        }

        Ok(GatedSignals {
            signals,
            regime: regime_result.state,
            confidence: regime_result.confidence,
        })
    }

    /// Returns a slice of the validated signals.
    pub fn signals(&self) -> &[Signal] {
        &self.signals
    }

    /// Returns a reference to the regime state.
    pub fn regime(&self) -> &Regime {
        &self.regime
    }

    /// Returns the regime confidence.
    pub fn confidence(&self) -> Percent {
        self.confidence
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::primitives::Percent;

    fn make_regime_result(confidence: f64) -> RegimeResult {
        RegimeResult {
            state: Regime::MeanReversion,
            confidence: Percent::new(confidence).unwrap(),
        }
    }

    fn make_signals(returns: &[f64]) -> Vec<Signal> {
        returns
            .iter()
            .enumerate()
            .map(|(index, &predicted_return)| {
                let letter = (b'A' + (index % 26) as u8) as char;
                Signal {
                    ticker: Ticker::new(&format!("TST{letter}")).unwrap(),
                    confidence: Percent::new(0.8).unwrap(),
                    predicted_return,
                }
            })
            .collect()
    }

    #[test]
    fn test_regime_trending_exposure_factor() {
        assert_eq!(Regime::Trending.exposure_factor(), 0.5);
    }

    #[test]
    fn test_regime_mean_reversion_exposure_factor() {
        assert_eq!(Regime::MeanReversion.exposure_factor(), 1.0);
    }

    #[test]
    fn test_regime_clone_and_eq() {
        let regime = Regime::Trending;
        let cloned = regime.clone();
        assert_eq!(regime, cloned);
    }

    #[test]
    fn test_gated_signals_new_rejects_empty_signals() {
        let error = GatedSignals::new(
            vec![],
            make_regime_result(0.8),
            ConfidenceFloor(Percent::new(0.5).unwrap()),
        )
        .unwrap_err();
        assert_eq!(error, SignalGateError::EmptySignals);
    }

    #[test]
    fn test_gated_signals_new_rejects_below_confidence_floor() {
        let signals = make_signals(&[0.01, 0.02, 0.03]);
        let error = GatedSignals::new(
            signals,
            make_regime_result(0.3),
            ConfidenceFloor(Percent::new(0.5).unwrap()),
        )
        .unwrap_err();

        assert_eq!(
            error,
            SignalGateError::BelowConfidenceFloor {
                confidence: 0.3,
                floor: 0.5
            }
        );
    }

    #[test]
    fn test_gated_signals_new_rejects_degenerate_spread() {
        let signals = make_signals(&[0.01, 0.01, 0.01]);
        let error = GatedSignals::new(
            signals,
            make_regime_result(0.8),
            ConfidenceFloor(Percent::new(0.5).unwrap()),
        )
        .unwrap_err();
        assert_eq!(error, SignalGateError::DegenerateQuantileSpread);
    }

    #[test]
    fn test_gated_signals_new_rejects_all_zero_returns() {
        let signals = make_signals(&[0.0, 0.0, 0.0, 0.0, 0.0]);
        let error = GatedSignals::new(
            signals,
            make_regime_result(0.9),
            ConfidenceFloor(Percent::new(0.5).unwrap()),
        )
        .unwrap_err();
        assert_eq!(error, SignalGateError::DegenerateQuantileSpread);
    }

    #[test]
    fn test_gated_signals_new_accepts_valid_input() {
        let signals = make_signals(&[-0.02, 0.0, 0.01, 0.03, 0.05]);
        let gated = GatedSignals::new(
            signals,
            make_regime_result(0.8),
            ConfidenceFloor(Percent::new(0.5).unwrap()),
        )
        .unwrap();
        assert_eq!(gated.signals().len(), 5);
        assert_eq!(*gated.regime(), Regime::MeanReversion);
        assert_eq!(gated.confidence().value(), 0.8);
    }

    #[test]
    fn test_gated_signals_accepts_confidence_equal_to_floor() {
        let signals = make_signals(&[0.01, 0.02, 0.03]);
        let gated = GatedSignals::new(
            signals,
            make_regime_result(0.5),
            ConfidenceFloor(Percent::new(0.5).unwrap()),
        )
        .unwrap();
        assert_eq!(gated.confidence().value(), 0.5);
    }

    #[test]
    fn test_gated_signals_new_rejects_nan_return() {
        let signals = make_signals(&[0.01, f64::NAN, 0.03]);
        let error = GatedSignals::new(
            signals,
            make_regime_result(0.8),
            ConfidenceFloor(Percent::new(0.5).unwrap()),
        )
        .unwrap_err();
        assert_eq!(error, SignalGateError::NonFiniteReturn);
    }

    #[test]
    fn test_gated_signals_new_rejects_inf_return() {
        let signals = make_signals(&[0.01, f64::INFINITY, 0.03]);
        let error = GatedSignals::new(
            signals,
            make_regime_result(0.8),
            ConfidenceFloor(Percent::new(0.5).unwrap()),
        )
        .unwrap_err();
        assert_eq!(error, SignalGateError::NonFiniteReturn);
    }

    #[test]
    fn test_signal_gate_error_display() {
        let error = SignalGateError::EmptySignals;
        assert!(format!("{}", error).contains("empty"));

        let error = SignalGateError::BelowConfidenceFloor {
            confidence: 0.3,
            floor: 0.5,
        };
        assert!(format!("{}", error).contains("0.3000"));

        let error = SignalGateError::DegenerateQuantileSpread;
        assert!(format!("{}", error).contains("degenerate"));

        let error = SignalGateError::NonFiniteReturn;
        assert!(format!("{}", error).contains("non-finite"));
    }

    #[test]
    fn test_confidence_floor_construction() {
        let floor = ConfidenceFloor(Percent::new(0.6).unwrap());
        assert_eq!(floor.0.value(), 0.6);
    }

    #[test]
    fn test_regime_result_construction() {
        let result = RegimeResult {
            state: Regime::Trending,
            confidence: Percent::new(0.75).unwrap(),
        };
        assert_eq!(result.state, Regime::Trending);
        assert_eq!(result.confidence.value(), 0.75);
    }
}
