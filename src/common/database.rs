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
