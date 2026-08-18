//! PostgreSQL connection handling.
//!
//! Connecting is fallible at startup rather than optional at runtime: every command arrives
//! through the database, so there is no degraded mode to fall back to.

use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use tracing::info;

/// Maximum pooled connections.
///
/// One process with one event consumer, a handful of concurrent queries inside a single evaluation
/// pass, and a separate listener connection. Five is comfortable headroom; the previous default of
/// ten existed for three services sharing a database.
const MAXIMUM_CONNECTIONS: u32 = 5;

/// How long to wait for a free connection before giving up.
///
/// Explicit rather than left at sqlx's 30-second default. A handler that cannot get a connection is
/// blocked on something that is not going to resolve inside a five-minute evaluation window, and a
/// prompt error that lands in the `_errored` payload is more useful than a long stall that pushes
/// the pass past its next firing.
const ACQUIRE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

/// Errors that prevent the service from reaching PostgreSQL.
#[derive(Debug, thiserror::Error)]
pub enum ConnectionError {
    #[error("DATABASE_URL environment variable must be set")]
    MissingDatabaseUrl,
    #[error("failed to connect to PostgreSQL: {0}")]
    Connect(#[from] sqlx::Error),
}

/// Connects to PostgreSQL using `DATABASE_URL`.
pub async fn connect_pool() -> Result<PgPool, ConnectionError> {
    let database_url =
        std::env::var("DATABASE_URL").map_err(|_| ConnectionError::MissingDatabaseUrl)?;
    let pool = PgPoolOptions::new()
        .max_connections(MAXIMUM_CONNECTIONS)
        .acquire_timeout(ACQUIRE_TIMEOUT)
        .connect(&database_url)
        .await?;
    info!(
        maximum_connections = MAXIMUM_CONNECTIONS,
        acquire_timeout_seconds = ACQUIRE_TIMEOUT.as_secs(),
        "Connected to PostgreSQL"
    );
    Ok(pool)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// RAII guard that restores a single environment variable on drop.
    ///
    /// Guarantees cleanup even when the test body panics. Tests using this guard must be marked
    /// `#[serial_test::serial]` to prevent concurrent environment access.
    struct EnvironmentVariableGuard {
        key: &'static str,
        previous: Option<String>,
    }

    impl EnvironmentVariableGuard {
        fn save(key: &'static str) -> Self {
            Self {
                key,
                previous: std::env::var(key).ok(),
            }
        }
    }

    impl Drop for EnvironmentVariableGuard {
        fn drop(&mut self) {
            // SAFETY: protected by #[serial_test::serial] — no concurrent environment access.
            unsafe {
                match self.previous.as_ref() {
                    Some(value) => std::env::set_var(self.key, value),
                    None => std::env::remove_var(self.key),
                }
            }
        }
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn test_connect_pool_reports_missing_database_url() {
        let _guard = EnvironmentVariableGuard::save("DATABASE_URL");
        // SAFETY: single-process test; environment mutation is serialized by #[serial].
        unsafe { std::env::remove_var("DATABASE_URL") };

        let error = connect_pool().await.expect_err("unset URL must fail");

        assert!(matches!(error, ConnectionError::MissingDatabaseUrl));
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn test_connect_pool_reports_unreachable_host() {
        let _guard = EnvironmentVariableGuard::save("DATABASE_URL");
        // SAFETY: single-process test; environment mutation is serialized by #[serial].
        // The .invalid TLD (RFC 2606) never resolves, so this always fails to connect.
        unsafe {
            std::env::set_var(
                "DATABASE_URL",
                "postgresql://user:pass@db-host.invalid:5432/nonexistent",
            )
        };

        let error = connect_pool()
            .await
            .expect_err("unreachable host must fail");

        assert!(matches!(error, ConnectionError::Connect(_)));
    }
}
