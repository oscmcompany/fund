//! Shared application state for the portfolio_manager service.

use std::env;
use std::num::NonZeroU8;

use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;

use crate::common::alpaca::AlpacaCredentials;
use crate::domain::portfolio::{
    BetaTolerance, ConcentrationCap, Constraints, DrawdownThreshold, MinimumPairs,
};
use crate::domain::primitives::Percent;
use crate::domain::signals::ConfidenceFloor;
use crate::portfolio_manager::alpaca::AlpacaTradingClient;

/// Default drawdown threshold: halt trading when portfolio value drops 10% from peak.
const DEFAULT_DRAWDOWN_THRESHOLD: f64 = 0.10;

/// Default concentration cap: no single ticker may exceed 20% of gross notional.
const DEFAULT_CONCENTRATION_CAP: f64 = 0.20;

/// Minimum pairs required in a valid portfolio.
const DEFAULT_MINIMUM_PAIRS: u8 = 10;

/// Default beta tolerance: net portfolio beta must be within ±0.1 of zero.
const DEFAULT_BETA_TOLERANCE: f64 = 0.10;

/// Default confidence floor: signals below 50% confidence are excluded.
const DEFAULT_CONFIDENCE_FLOOR: f64 = 0.50;

/// Error returned when `AppState::from_env()` cannot read required configuration.
#[derive(Debug)]
pub struct ConfigError {
    message: String,
}

impl std::fmt::Display for ConfigError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}", self.message)
    }
}

impl std::error::Error for ConfigError {}

/// Shared state injected into every Axum handler via `axum::Extension`.
///
/// Constructed once at startup via `from_env()`. A value of this type proves
/// that the database pool, Alpaca credentials, and portfolio constraints are
/// all valid and ready to use.
#[derive(Clone)]
pub struct AppState {
    pool: PgPool,
    alpaca_client: AlpacaTradingClient,
    confidence_floor: ConfidenceFloor,
    constraints: Constraints,
}

impl AppState {
    /// Returns a reference to the database pool.
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// Returns a reference to the Alpaca trading client.
    pub fn alpaca_client(&self) -> &AlpacaTradingClient {
        &self.alpaca_client
    }

    /// Returns the confidence floor used to gate signals.
    pub fn confidence_floor(&self) -> ConfidenceFloor {
        self.confidence_floor
    }

    /// Returns a reference to the portfolio constraints.
    pub fn constraints(&self) -> &Constraints {
        &self.constraints
    }

    /// Constructs `AppState` by reading all required values from the environment.
    ///
    /// Required environment variables:
    /// - `DATABASE_URL`: PostgreSQL connection string
    /// - `ALPACA_KEY_ID`: Alpaca API key identifier
    /// - `ALPACA_SECRET`: Alpaca API secret key
    /// - `IS_PAPER` (optional): `"true"` for paper trading; defaults to `"true"` for safety
    ///
    /// Returns `Err` if any required variable is absent or the database pool
    /// cannot be created.
    pub async fn from_env() -> Result<Self, ConfigError> {
        let database_url = env::var("DATABASE_URL").map_err(|_| ConfigError {
            message: "DATABASE_URL environment variable is not set".to_string(),
        })?;

        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect(&database_url)
            .await
            .map_err(|error| ConfigError {
                message: format!("Failed to connect to PostgreSQL: {error}"),
            })?;

        let key_id = env::var("ALPACA_KEY_ID").map_err(|_| ConfigError {
            message: "ALPACA_KEY_ID environment variable is not set".to_string(),
        })?;
        let secret = env::var("ALPACA_SECRET").map_err(|_| ConfigError {
            message: "ALPACA_SECRET environment variable is not set".to_string(),
        })?;

        let credentials = AlpacaCredentials::new(key_id, secret)
            .map_err(|error| ConfigError { message: error })?;

        // Default to paper trading for safety; require explicit opt-in for live.
        let is_paper = env::var("IS_PAPER")
            .map(|value| !value.eq_ignore_ascii_case("false"))
            .unwrap_or(true);

        let alpaca_client = AlpacaTradingClient::new(credentials, is_paper);

        let drawdown_threshold = Percent::new(DEFAULT_DRAWDOWN_THRESHOLD)
            .map(DrawdownThreshold)
            .map_err(|_| ConfigError {
                message: "Invalid default drawdown threshold".to_string(),
            })?;

        let concentration_cap = Percent::new(DEFAULT_CONCENTRATION_CAP)
            .map(ConcentrationCap)
            .map_err(|_| ConfigError {
                message: "Invalid default concentration cap".to_string(),
            })?;

        let minimum_pairs = NonZeroU8::new(DEFAULT_MINIMUM_PAIRS)
            .map(MinimumPairs)
            .ok_or_else(|| ConfigError {
                message: "Minimum pairs must be non-zero".to_string(),
            })?;

        let confidence_floor = Percent::new(DEFAULT_CONFIDENCE_FLOOR)
            .map(ConfidenceFloor)
            .map_err(|_| ConfigError {
                message: "Invalid default confidence floor".to_string(),
            })?;

        let beta_tolerance = BetaTolerance::new(DEFAULT_BETA_TOLERANCE)
            .map_err(|error| ConfigError { message: error })?;

        Ok(Self {
            pool,
            alpaca_client,
            confidence_floor,
            constraints: Constraints::new(
                drawdown_threshold,
                concentration_cap,
                minimum_pairs,
                beta_tolerance,
            ),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_error_display() {
        let error = ConfigError {
            message: "DATABASE_URL is not set".to_string(),
        };
        assert!(format!("{error}").contains("DATABASE_URL"));
    }

    #[test]
    fn test_config_error_is_error_trait() {
        let error = ConfigError {
            message: "test".to_string(),
        };
        let _boxed: Box<dyn std::error::Error> = Box::new(error);
    }

    #[test]
    fn test_from_env_fails_without_database_url() {
        // Remove DATABASE_URL from environment to trigger the expected error.
        // SAFETY: single-threaded test; env mutations don't race.
        unsafe { env::remove_var("DATABASE_URL") };
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            let result = AppState::from_env().await;
            assert!(result.is_err());
            assert!(result.err().unwrap().to_string().contains("DATABASE_URL"));
        });
    }

    #[test]
    fn test_from_env_fails_without_alpaca_key_id() {
        unsafe {
            env::set_var("DATABASE_URL", "postgresql://localhost:5432/nonexistent");
            env::remove_var("ALPACA_KEY_ID");
        }
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            let result = AppState::from_env().await;
            // Fails either at DB connect or at ALPACA_KEY_ID — both are errors.
            assert!(result.is_err());
        });
        unsafe {
            env::remove_var("DATABASE_URL");
        }
    }
}
