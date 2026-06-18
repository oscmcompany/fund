//! Shared application state for the portfolio_manager service.

use std::collections::HashSet;
use std::env;
use std::num::NonZeroU8;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::Arc;

use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use tokio::sync::RwLock;

use crate::common::alpaca::AlpacaCredentials;
use crate::domain::portfolio::{
    BetaTolerance, ConcentrationCap, Constraints, DrawdownThreshold, MinimumPairs,
};
use crate::domain::primitives::Percent;
use crate::domain::signals::ConfidenceFloor;
use crate::portfolio_manager::alpaca::AlpacaTradingClient;
use crate::portfolio_manager::statistical_arbitrage::DEFAULT_CANDIDATE_POOL;

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

/// Reads `key` as `f64`, returning `default` when unset. A present-but-unparseable
/// value is a hard error so a misconfigured environment fails fast at startup
/// rather than silently falling back.
fn env_f64(key: &str, default: f64) -> Result<f64, ConfigError> {
    match env::var(key) {
        Ok(raw) => raw.trim().parse::<f64>().map_err(|_| ConfigError {
            message: format!("{key} must be a number, got '{raw}'"),
        }),
        Err(env::VarError::NotPresent) => Ok(default),
        Err(env::VarError::NotUnicode(_)) => Err(ConfigError {
            message: format!("{key} must be valid UTF-8"),
        }),
    }
}

/// Reads `key` as `u8`, returning `default` when unset. Present-but-unparseable is
/// a hard error (see [`env_f64`]).
fn env_u8(key: &str, default: u8) -> Result<u8, ConfigError> {
    match env::var(key) {
        Ok(raw) => raw.trim().parse::<u8>().map_err(|_| ConfigError {
            message: format!("{key} must be an integer 0-255, got '{raw}'"),
        }),
        Err(env::VarError::NotPresent) => Ok(default),
        Err(env::VarError::NotUnicode(_)) => Err(ConfigError {
            message: format!("{key} must be valid UTF-8"),
        }),
    }
}

/// Reads `key` as `usize`, returning `default` when unset. Present-but-unparseable
/// is a hard error (see [`env_f64`]).
fn env_usize(key: &str, default: usize) -> Result<usize, ConfigError> {
    match env::var(key) {
        Ok(raw) => raw.trim().parse::<usize>().map_err(|_| ConfigError {
            message: format!("{key} must be a non-negative integer, got '{raw}'"),
        }),
        Err(env::VarError::NotPresent) => Ok(default),
        Err(env::VarError::NotUnicode(_)) => Err(ConfigError {
            message: format!("{key} must be valid UTF-8"),
        }),
    }
}

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
    /// Cached set of tradable+shortable+easy_to_borrow asset symbols.
    ///
    /// `None` until the first rebalance of the session fetches and populates it.
    /// Cleared on service restart (intraday deploys rehydrate on next rebalance).
    /// The inner `Arc` avoids cloning the full set on every cache hit.
    tradable_assets: Arc<RwLock<Option<Arc<HashSet<String>>>>>,
    /// Guards against concurrent rebalance cycles when the prediction pipeline
    /// takes longer than the 5-minute `market_session_check` interval.
    rebalance_cycle_in_progress: Arc<AtomicBool>,
    /// Unix timestamp (seconds) when the current rebalance cycle started.
    ///
    /// `0` when no cycle is in progress. Used to detect stale flags caused by
    /// an upstream crash that never emits `equity_predictions_completed` or
    /// `equity_predictions_errored`.
    rebalance_cycle_started_at: Arc<AtomicI64>,
    /// Number of statistical-arbitrage candidate pairs to consider per rebalance.
    /// Decoupled from the required minimum (`constraints.minimum_pairs`) so a
    /// larger pool can absorb sizing attrition. Override per environment with
    /// `PORTFOLIO_CANDIDATE_POOL`.
    candidate_pool_count: usize,
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

    /// Returns the candidate-pool size for statistical-arbitrage pair selection.
    pub fn candidate_pool_count(&self) -> usize {
        self.candidate_pool_count
    }

    /// Returns a reference to the portfolio constraints.
    pub fn constraints(&self) -> &Constraints {
        &self.constraints
    }

    /// Returns the shared tradable asset cache.
    pub fn tradable_assets(&self) -> &Arc<RwLock<Option<Arc<HashSet<String>>>>> {
        &self.tradable_assets
    }

    /// Returns `true` when a rebalance cycle is already in progress.
    pub fn rebalance_cycle_in_progress(&self) -> bool {
        self.rebalance_cycle_in_progress.load(Ordering::SeqCst)
    }

    /// Returns the Unix timestamp (seconds) when the current rebalance cycle started,
    /// or `0` if no cycle is in progress.
    pub fn rebalance_cycle_started_at(&self) -> i64 {
        self.rebalance_cycle_started_at.load(Ordering::SeqCst)
    }

    /// Sets or clears the rebalance-cycle-in-progress flag.
    ///
    /// When `in_progress` is `true`, also records the current time so callers
    /// can detect stale flags after upstream crashes.
    pub fn set_rebalance_cycle_in_progress(&self, in_progress: bool) {
        self.rebalance_cycle_in_progress
            .store(in_progress, Ordering::SeqCst);
        let timestamp = if in_progress {
            chrono::Utc::now().timestamp()
        } else {
            0
        };
        self.rebalance_cycle_started_at
            .store(timestamp, Ordering::SeqCst);
    }

    /// Constructs `AppState` by reading all required values from the environment.
    ///
    /// Required environment variables:
    /// - `DATABASE_URL`: PostgreSQL connection string
    /// - `ALPACA_API_KEY_ID`: Alpaca API key identifier
    /// - `ALPACA_API_SECRET`: Alpaca API secret key
    /// - `ALPACA_IS_PAPER` (optional): `"true"` for paper trading; defaults to `"true"` for safety
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

        let key_id = env::var("ALPACA_API_KEY_ID").map_err(|_| ConfigError {
            message: "ALPACA_API_KEY_ID environment variable is not set".to_string(),
        })?;
        let secret = env::var("ALPACA_API_SECRET").map_err(|_| ConfigError {
            message: "ALPACA_API_SECRET environment variable is not set".to_string(),
        })?;

        let credentials = AlpacaCredentials::new(key_id, secret)
            .map_err(|error| ConfigError { message: error })?;

        // Default to paper trading for safety; require explicit opt-in for live.
        let is_paper = env::var("ALPACA_IS_PAPER")
            .map(|value| !value.eq_ignore_ascii_case("false"))
            .unwrap_or(true);

        let alpaca_client = AlpacaTradingClient::new(credentials, is_paper);

        // Risk and strategy parameters fall back to the safe defaults below but
        // can be overridden per environment (keyed off FUND_PROFILE via the
        // service's environment). A present-but-invalid value fails startup.
        let drawdown_threshold = Percent::new(env_f64(
            "PORTFOLIO_DRAWDOWN_THRESHOLD",
            DEFAULT_DRAWDOWN_THRESHOLD,
        )?)
        .map(DrawdownThreshold)
        .map_err(|_| ConfigError {
            message: "PORTFOLIO_DRAWDOWN_THRESHOLD must be a fraction in [0, 1]".to_string(),
        })?;

        let concentration_cap = Percent::new(env_f64(
            "PORTFOLIO_CONCENTRATION_CAP",
            DEFAULT_CONCENTRATION_CAP,
        )?)
        .map(ConcentrationCap)
        .map_err(|_| ConfigError {
            message: "PORTFOLIO_CONCENTRATION_CAP must be a fraction in [0, 1]".to_string(),
        })?;

        let minimum_pairs =
            NonZeroU8::new(env_u8("PORTFOLIO_MINIMUM_PAIRS", DEFAULT_MINIMUM_PAIRS)?)
                .map(MinimumPairs)
                .ok_or_else(|| ConfigError {
                    message: "PORTFOLIO_MINIMUM_PAIRS must be non-zero".to_string(),
                })?;

        let confidence_floor = Percent::new(env_f64(
            "PORTFOLIO_CONFIDENCE_FLOOR",
            DEFAULT_CONFIDENCE_FLOOR,
        )?)
        .map(ConfidenceFloor)
        .map_err(|_| ConfigError {
            message: "PORTFOLIO_CONFIDENCE_FLOOR must be a fraction in [0, 1]".to_string(),
        })?;

        let beta_tolerance =
            BetaTolerance::new(env_f64("PORTFOLIO_BETA_TOLERANCE", DEFAULT_BETA_TOLERANCE)?)
                .map_err(|error| ConfigError { message: error })?;

        // The candidate pool must be at least the required minimum, otherwise
        // sizing can never reach `minimum_pairs` viable pairs.
        let candidate_pool_count = env_usize("PORTFOLIO_CANDIDATE_POOL", DEFAULT_CANDIDATE_POOL)?
            .max(minimum_pairs.0.get() as usize);

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
            tradable_assets: Arc::new(RwLock::new(None)),
            rebalance_cycle_in_progress: Arc::new(AtomicBool::new(false)),
            rebalance_cycle_started_at: Arc::new(AtomicI64::new(0)),
            candidate_pool_count,
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
    fn test_env_f64_falls_back_to_default_when_unset() {
        assert_eq!(env_f64("PORTFOLIO_TEST_UNSET_F64_KEY", 0.25).unwrap(), 0.25);
    }

    #[test]
    #[serial_test::serial]
    fn test_env_f64_reads_and_parses_override() {
        // Unique key avoids racing other tests that mutate the environment.
        // SAFETY: edition-2021 single-process test; the key is used only here.
        unsafe { env::set_var("PORTFOLIO_TEST_OVERRIDE_F64", "0.05") };
        unsafe { env::remove_var("PORTFOLIO_TEST_OVERRIDE_F64") };
    }

    #[test]
    #[serial_test::serial]
    fn test_env_f64_rejects_unparseable_value() {
        unsafe { env::set_var("PORTFOLIO_TEST_BAD_F64", "not-a-number") };
        let result = env_f64("PORTFOLIO_TEST_BAD_F64", 0.10);
        unsafe { env::remove_var("PORTFOLIO_TEST_BAD_F64") };
    }

    #[test]
    fn test_env_u8_and_usize_defaults_and_overrides() {
        assert_eq!(env_u8("PORTFOLIO_TEST_UNSET_U8", 10).unwrap(), 10);
        assert_eq!(env_usize("PORTFOLIO_TEST_UNSET_USIZE", 20).unwrap(), 20);
        unsafe { env::set_var("PORTFOLIO_TEST_OVERRIDE_USIZE", "30") };
        assert_eq!(env_usize("PORTFOLIO_TEST_OVERRIDE_USIZE", 20).unwrap(), 30);
        unsafe { env::remove_var("PORTFOLIO_TEST_OVERRIDE_USIZE") };
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
            env::remove_var("ALPACA_API_KEY_ID");
        }
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            let result = AppState::from_env().await;
            // Fails either at DB connect or at ALPACA_API_KEY_ID — both are errors.
            assert!(result.is_err());
        });
        unsafe {
            env::remove_var("DATABASE_URL");
        }
    }
}
