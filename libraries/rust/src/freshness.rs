//! Data freshness wrapper enforcing staleness checks at the type level.

use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};

/// Named staleness window for equity predictions.
///
/// Twenty hours covers the full trading day without gating valid same-day
/// afternoon rebalances. The window is measured from the prediction batch's
/// `created_at` timestamp.
pub const PREDICTIONS_STALENESS_WINDOW_HOURS: i64 = 20;

/// Error returned when constructing a `StalenessWindow` with a zero duration.
#[derive(Debug, Clone, PartialEq)]
pub struct ZeroDurationError;

impl std::fmt::Display for ZeroDurationError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "Staleness window duration must not be zero.")
    }
}

impl std::error::Error for ZeroDurationError {}

/// A validated non-zero duration representing the maximum age for fresh data.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct StalenessWindow(pub Duration);

impl StalenessWindow {
    /// Creates a new `StalenessWindow`, returning an error if `duration` is zero.
    pub fn new(duration: Duration) -> Result<Self, ZeroDurationError> {
        if duration.is_zero() {
            return Err(ZeroDurationError);
        }
        Ok(StalenessWindow(duration))
    }

    /// Returns the staleness window for equity predictions (20 hours).
    pub fn predictions() -> Self {
        StalenessWindow(Duration::hours(PREDICTIONS_STALENESS_WINDOW_HOURS))
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
    fn test_staleness_window_copy() {
        let window = StalenessWindow::predictions();
        let copy = window;
        assert_eq!(window, copy);
    }

    #[test]
    fn test_zero_duration_error_display() {
        let error = ZeroDurationError;
        let message = format!("{}", error);
        assert!(message.contains("zero"));
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
