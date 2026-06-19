//! PostgreSQL connection handling shared by all services.

use sqlx::PgPool;
use tracing::{info, warn};

/// Connect to PostgreSQL when `DATABASE_URL` is set.
///
/// Returns the optional pool together with whether `DATABASE_URL` was configured
/// at all — the pool can be `None` even when configured, if the connection
/// attempt failed. Services that do not distinguish the two cases can ignore the
/// boolean.
pub async fn connect_optional_pool() -> (Option<PgPool>, bool) {
    match std::env::var("DATABASE_URL") {
        Ok(database_url) => match PgPool::connect(&database_url).await {
            Ok(pool) => {
                info!("Connected to PostgreSQL");
                (Some(pool), true)
            }
            Err(error) => {
                warn!("Failed to connect to PostgreSQL: {}", error);
                (None, true)
            }
        },
        Err(_) => {
            info!("DATABASE_URL not set, PostgreSQL disabled");
            (None, false)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_runtime() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
    }

    #[test]
    #[serial_test::serial]
    fn test_connect_optional_pool_returns_false_when_database_url_unset() {
        // Remove DATABASE_URL so the not-configured branch is exercised.
        let previous = std::env::var("DATABASE_URL").ok();
        // SAFETY: single-process test; env mutation is serialized by #[serial].
        unsafe { std::env::remove_var("DATABASE_URL") };

        let (pool, configured) = make_runtime().block_on(connect_optional_pool());

        // Restore whatever value existed before this test.
        if let Some(value) = previous {
            unsafe { std::env::set_var("DATABASE_URL", value) };
        }

        assert!(pool.is_none());
        assert!(!configured);
    }

    #[test]
    #[serial_test::serial]
    fn test_connect_optional_pool_returns_true_when_database_url_set_but_unreachable() {
        // With a syntactically valid but unreachable URL the function must return
        // (None, true): the URL was configured, but the connection failed.
        let previous = std::env::var("DATABASE_URL").ok();
        // SAFETY: single-process test; env mutation is serialized by #[serial].
        unsafe {
            std::env::set_var(
                "DATABASE_URL",
                "postgresql://user:pass@127.0.0.1:19999/nonexistent",
            )
        };

        let (pool, configured) = make_runtime().block_on(connect_optional_pool());

        if let Some(value) = previous {
            unsafe { std::env::set_var("DATABASE_URL", value) };
        } else {
            unsafe { std::env::remove_var("DATABASE_URL") };
        }

        // The URL was present, so configured == true regardless of whether the
        // connection succeeded.
        assert!(configured);
        // A refused connection at a local port that is almost certainly not
        // listening returns None.
        assert!(pool.is_none());
    }
}
