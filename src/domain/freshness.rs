//! Data freshness wrapper enforcing staleness checks at the type level.

use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};

/// Named staleness window for equity predictions.
///
/// Twenty hours covers the full trading day without gating valid same-day
/// afternoon rebalances. The window is measured from the prediction batch's
/// `created_at` timestamp.
pub const PREDICTIONS_STALENESS_WINDOW_HOURS: i64 = 20;

/// Named staleness window for quotes, whatever transport delivered them.
///
/// One window, applied identically to streamed and REST snapshot quotes. There
/// were two — sixty seconds for streamed, five minutes for snapshot — and they
/// fed the same decision: a single exit evaluation mixes both sources, so a
/// 61-second-old streamed quote was rejected as stale in the same pass where a
/// four-minute-old snapshot quote decided a stop-loss close. The transport a
/// price arrived over is not evidence about whether the price is still true.
///
/// Five minutes was chosen against a measured sample of the traded universe:
/// 64% of symbols had quoted within sixty seconds, 80% within five minutes, and
/// 86% within fifteen. The wider value is the right one to keep, because the
/// tighter one rejected precisely the quiet symbols the snapshot path exists to
/// cover — a symbol quiet on the stream is quiet in the snapshot for the same
/// reason, both being the same IEX feed. The marginal six points between five
/// and fifteen minutes come from names whose books are too wide to price against
/// anyway, so the book-quality gate rejects them regardless of quote age.
///
/// Widening the streamed window is safe only alongside
/// [`MAXIMUM_LEG_SKEW_SECONDS`](crate::portfolio::spread::MAXIMUM_LEG_SKEW_SECONDS),
/// which is what actually protects a spread: absolute age asks whether a price
/// is still true, leg skew asks whether two prices describe one moment. Age was
/// standing in for a property it does not check.
pub const QUOTE_STALENESS_WINDOW_SECONDS: i64 = 300;

/// Error returned when constructing a `StalenessWindow` with a non-positive duration.
#[derive(Debug, Clone, PartialEq)]
pub struct ZeroDurationError;

impl std::fmt::Display for ZeroDurationError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "Staleness window duration must be positive.")
    }
}

impl std::error::Error for ZeroDurationError {}

/// A validated positive duration representing the maximum age for fresh data.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct StalenessWindow(pub Duration);

impl StalenessWindow {
    /// Creates a new `StalenessWindow`, returning an error if `duration` is non-positive.
    pub fn new(duration: Duration) -> Result<Self, ZeroDurationError> {
        if duration <= Duration::zero() {
            return Err(ZeroDurationError);
        }
        Ok(StalenessWindow(duration))
    }

    /// Returns the staleness window for equity predictions (20 hours).
    pub fn predictions() -> Self {
        StalenessWindow::new(Duration::hours(PREDICTIONS_STALENESS_WINDOW_HOURS))
            .expect("PREDICTIONS_STALENESS_WINDOW_HOURS must be positive")
    }

    /// Returns the staleness window for quotes of any source (5 minutes).
    pub fn quotes() -> Self {
        StalenessWindow::new(Duration::seconds(QUOTE_STALENESS_WINDOW_SECONDS))
            .expect("QUOTE_STALENESS_WINDOW_SECONDS must be positive")
    }
}

/// A timestamped data wrapper that enforces a staleness check on access.
///
/// `get()` returns `None` if the data is older than `maximum_age`, forcing
/// callers to handle the stale-data case explicitly rather than silently
/// trading on yesterday's predictions.
#[derive(Debug, Clone)]
pub struct Fresh<T> {
    pub data: T,
    pub fetched_at: DateTime<Utc>,
    pub maximum_age: StalenessWindow,
}

impl<T> Fresh<T> {
    /// Creates a new `Fresh` wrapper with the current time as `fetched_at`.
    pub fn new(data: T, maximum_age: StalenessWindow) -> Self {
        Fresh {
            data,
            fetched_at: Utc::now(),
            maximum_age,
        }
    }

    /// Returns a reference to the wrapped data if it is still within the
    /// staleness window, or `None` if the data has expired.
    pub fn get(&self) -> Option<&T> {
        let age = Utc::now().signed_duration_since(self.fetched_at);
        if age <= self.maximum_age.0 {
            Some(&self.data)
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration;

    #[test]
    fn test_staleness_window_new_rejects_zero() {
        let error = StalenessWindow::new(Duration::zero()).unwrap_err();
        assert_eq!(error, ZeroDurationError);
    }

    #[test]
    fn test_staleness_window_new_accepts_positive() {
        let window = StalenessWindow::new(Duration::hours(1)).unwrap();
        assert_eq!(window.0, Duration::hours(1));
    }

    #[test]
    fn test_staleness_window_predictions_returns_twenty_hours() {
        let window = StalenessWindow::predictions();
        assert_eq!(
            window.0,
            Duration::hours(PREDICTIONS_STALENESS_WINDOW_HOURS)
        );
        assert_eq!(window.0, Duration::hours(20));
    }

    #[test]
    fn test_quote_window_is_source_independent() {
        // One window for streamed and snapshot quotes alike. Two windows fed the
        // same exit decision, so a pass could reject a 61-second streamed quote
        // and act on a four-minute snapshot quote in the same evaluation.
        let window = StalenessWindow::quotes();
        assert_eq!(window.0, Duration::seconds(300));
    }

    #[test]
    fn test_staleness_window_copy() {
        let window = StalenessWindow::predictions();
        let copy = window;
        assert_eq!(window, copy);
    }

    #[test]
    fn test_staleness_window_new_rejects_negative() {
        let error = StalenessWindow::new(Duration::hours(-1)).unwrap_err();
        assert_eq!(error, ZeroDurationError);
    }

    #[test]
    fn test_zero_duration_error_display() {
        let error = ZeroDurationError;
        let message = format!("{}", error);
        assert!(message.contains("positive"));
    }

    #[test]
    fn test_fresh_get_returns_data_when_within_window() {
        let window = StalenessWindow::new(Duration::hours(1)).unwrap();
        let fresh = Fresh::new("live data".to_string(), window);
        assert_eq!(fresh.get(), Some(&"live data".to_string()));
    }

    #[test]
    fn test_fresh_get_returns_none_when_stale() {
        let window = StalenessWindow::new(Duration::hours(1)).unwrap();
        let stale = Fresh {
            data: "stale predictions".to_string(),
            fetched_at: Utc::now() - Duration::hours(25),
            maximum_age: window,
        };
        assert!(stale.get().is_none());
    }

    #[test]
    fn test_fresh_get_returns_data_just_inside_window() {
        let window = StalenessWindow::new(Duration::hours(20)).unwrap();
        let fresh = Fresh {
            data: 42_u32,
            fetched_at: Utc::now() - Duration::hours(19),
            maximum_age: window,
        };
        assert_eq!(fresh.get(), Some(&42_u32));
    }

    #[test]
    fn test_fresh_clone() {
        let window = StalenessWindow::new(Duration::hours(1)).unwrap();
        let fresh = Fresh::new(100_u32, window);
        let cloned = fresh.clone();
        assert_eq!(cloned.data, 100_u32);
    }
}
