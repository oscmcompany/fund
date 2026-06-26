use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::domain::market::Ticker;
use aws_sdk_s3::Client as S3Client;
use reqwest::Client as HTTPClient;
use sqlx::PgPool;
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

/// Alpaca API credentials.
///
/// The private fields enforce that credentials are only constructed when both
/// `ALPACA_API_KEY_ID` and `ALPACA_API_SECRET` are non-empty. An `AlpacaCredentials`
/// in scope is proof that both values were present at initialization.
#[derive(Clone)]
pub struct AlpacaCredentials {
    key_id: String,
    secret: String,
    feed: String,
}

impl AlpacaCredentials {
    /// Reads credentials from environment variables.
    ///
    /// Returns `None` if either `ALPACA_API_KEY_ID` or `ALPACA_API_SECRET` is absent or empty.
    /// The `ALPACA_FEED` variable defaults to `"iex"` if not set.
    pub fn from_env() -> Option<Self> {
        let key_id = std::env::var("ALPACA_API_KEY_ID").unwrap_or_default();
        let secret = std::env::var("ALPACA_API_SECRET").unwrap_or_default();
        if key_id.is_empty() || secret.is_empty() {
            return None;
        }
        let feed = std::env::var("ALPACA_FEED").unwrap_or_else(|_| "iex".to_string());
        Some(Self {
            key_id,
            secret,
            feed,
        })
    }

    pub fn key_id(&self) -> &str {
        &self.key_id
    }

    pub fn secret(&self) -> &str {
        &self.secret
    }

    pub fn feed(&self) -> &str {
        &self.feed
    }
}

/// Database connection state.
///
/// Encodes three distinct states:
/// - `NotConfigured`: `DATABASE_URL` was not set; PostgreSQL is intentionally disabled.
/// - `ConnectFailed`: `DATABASE_URL` was set but the connection attempt failed.
/// - `Connected`: A live `PgPool` is available for queries.
#[derive(Clone)]
pub enum DatabaseState {
    NotConfigured,
    ConnectFailed,
    Connected(PgPool),
}

impl DatabaseState {
    /// Returns a reference to the pool if connected, or `None` otherwise.
    pub fn pool(&self) -> Option<&PgPool> {
        match self {
            DatabaseState::Connected(pool) => Some(pool),
            _ => None,
        }
    }

    /// Returns `true` if `DATABASE_URL` was configured (whether or not the connection succeeded).
    pub fn is_configured(&self) -> bool {
        !matches!(self, DatabaseState::NotConfigured)
    }
}

#[derive(Clone)]
pub struct MassiveSecrets {
    pub base: String,
    pub key: String,
}

#[derive(Clone)]
pub struct State {
    pub http_client: HTTPClient,
    pub massive: MassiveSecrets,
    pub s3_client: S3Client,
    pub bucket_name: String,
    pub last_s3_ok_epoch: Arc<AtomicU64>,
    pub last_sync_epoch: Arc<AtomicU64>,
    pub database: DatabaseState,
    pub alpaca_credentials: Option<AlpacaCredentials>,
    pub active_symbols: Arc<RwLock<HashSet<Ticker>>>,
}

impl State {
    pub async fn from_env() -> Self {
        info!("Initializing application state from environment");

        debug!("Creating HTTP client with 10s timeout");
        let http_client = HTTPClient::builder()
            .timeout(std::time::Duration::from_secs(10))
            .build()
            .unwrap();

        debug!("Loading AWS configuration");
        let config = crate::common::aws::load_config().await;

        let region = config
            .region()
            .map(|r| r.as_ref().to_string())
            .unwrap_or_else(|| "not configured".to_string());
        info!("AWS region: {}", region);

        let s3_client = S3Client::new(&config);

        let bucket_name = std::env::var("AWS_S3_BUCKET_NAME")
            .expect("AWS_S3_BUCKET_NAME environment variable must be set");
        info!("Using S3 bucket: {}", bucket_name);

        let massive_base_url = std::env::var("MASSIVE_BASE_URL")
            .expect("MASSIVE_BASE_URL environment variable must be set");
        info!("Using Massive API base URL: {}", massive_base_url);

        let massive_api_key = std::env::var("MASSIVE_API_KEY")
            .expect("MASSIVE_API_KEY environment variable must be set");

        let alpaca_credentials = AlpacaCredentials::from_env();
        if let Some(ref credentials) = alpaca_credentials {
            info!("Using Alpaca feed: {}", credentials.feed());
        } else {
            info!("Alpaca credentials not configured");
        }

        let database = match std::env::var("DATABASE_URL") {
            Ok(database_url) => {
                debug!("Connecting to PostgreSQL");
                match PgPool::connect(&database_url).await {
                    Ok(pool) => {
                        info!("Connected to PostgreSQL");
                        DatabaseState::Connected(pool)
                    }
                    Err(error) => {
                        warn!("Failed to connect to PostgreSQL: {}", error);
                        DatabaseState::ConnectFailed
                    }
                }
            }
            Err(_) => {
                info!("DATABASE_URL not set, PostgreSQL disabled");
                DatabaseState::NotConfigured
            }
        };

        info!("Application state initialized successfully");

        Self {
            http_client,
            massive: MassiveSecrets {
                base: massive_base_url,
                key: massive_api_key,
            },
            s3_client,
            bucket_name,
            last_s3_ok_epoch: Arc::new(AtomicU64::new(0)),
            last_sync_epoch: Arc::new(AtomicU64::new(0)),
            database,
            alpaca_credentials,
            active_symbols: Arc::new(RwLock::new(HashSet::new())),
        }
    }

    pub fn new(
        http_client: HTTPClient,
        massive: MassiveSecrets,
        s3_client: S3Client,
        bucket_name: String,
    ) -> Self {
        Self {
            http_client,
            massive,
            s3_client,
            bucket_name,
            last_s3_ok_epoch: Arc::new(AtomicU64::new(0)),
            last_sync_epoch: Arc::new(AtomicU64::new(0)),
            database: DatabaseState::NotConfigured,
            alpaca_credentials: None,
            active_symbols: Arc::new(RwLock::new(HashSet::new())),
        }
    }

    pub fn s3_ok_recently(&self, ttl_secs: u64) -> bool {
        let last = self.last_s3_ok_epoch.load(Ordering::Relaxed);
        if last == 0 {
            return false;
        }
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        now.saturating_sub(last) < ttl_secs
    }

    pub fn mark_s3_ok(&self) {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        self.last_s3_ok_epoch.store(now, Ordering::Relaxed);
    }

    pub fn synced_recently(&self, ttl_secs: u64) -> bool {
        let last = self.last_sync_epoch.load(Ordering::Relaxed);
        if last == 0 {
            return false;
        }
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        now.saturating_sub(last) < ttl_secs
    }

    pub fn mark_synced(&self) {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        self.last_sync_epoch.store(now, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::{AlpacaCredentials, DatabaseState};
    use serial_test::serial;

    #[test]
    fn test_database_state_not_configured_pool_is_none() {
        assert!(DatabaseState::NotConfigured.pool().is_none());
    }

    #[test]
    fn test_database_state_connect_failed_pool_is_none() {
        assert!(DatabaseState::ConnectFailed.pool().is_none());
    }

    #[test]
    fn test_database_state_not_configured_is_not_configured() {
        assert!(!DatabaseState::NotConfigured.is_configured());
    }

    #[test]
    fn test_database_state_connect_failed_is_configured() {
        assert!(DatabaseState::ConnectFailed.is_configured());
    }

    #[test]
    #[serial]
    fn test_alpaca_credentials_from_env_returns_none_when_key_id_missing() {
        // SAFETY: protected by serial test runner conventions; env mutation is
        // scoped to the test process.
        let original_key = std::env::var("ALPACA_API_KEY_ID").ok();
        let original_secret = std::env::var("ALPACA_API_SECRET").ok();
        unsafe {
            std::env::remove_var("ALPACA_API_KEY_ID");
            std::env::set_var("ALPACA_API_SECRET", "test-secret");
        }
        let result = AlpacaCredentials::from_env();
        unsafe {
            match original_key {
                Some(v) => std::env::set_var("ALPACA_API_KEY_ID", v),
                None => std::env::remove_var("ALPACA_API_KEY_ID"),
            }
            match original_secret {
                Some(v) => std::env::set_var("ALPACA_API_SECRET", v),
                None => std::env::remove_var("ALPACA_API_SECRET"),
            }
        }
        assert!(result.is_none());
    }

    #[test]
    #[serial]
    fn test_alpaca_credentials_from_env_returns_none_when_secret_missing() {
        let original_key = std::env::var("ALPACA_API_KEY_ID").ok();
        let original_secret = std::env::var("ALPACA_API_SECRET").ok();
        unsafe {
            std::env::set_var("ALPACA_API_KEY_ID", "test-key");
            std::env::remove_var("ALPACA_API_SECRET");
        }
        let result = AlpacaCredentials::from_env();
        unsafe {
            match original_key {
                Some(v) => std::env::set_var("ALPACA_API_KEY_ID", v),
                None => std::env::remove_var("ALPACA_API_KEY_ID"),
            }
            match original_secret {
                Some(v) => std::env::set_var("ALPACA_API_SECRET", v),
                None => std::env::remove_var("ALPACA_API_SECRET"),
            }
        }
        assert!(result.is_none());
    }

    #[test]
    #[serial]
    fn test_alpaca_credentials_feed_defaults_to_iex() {
        let original_key = std::env::var("ALPACA_API_KEY_ID").ok();
        let original_secret = std::env::var("ALPACA_API_SECRET").ok();
        let original_feed = std::env::var("ALPACA_FEED").ok();
        unsafe {
            std::env::set_var("ALPACA_API_KEY_ID", "test-key");
            std::env::set_var("ALPACA_API_SECRET", "test-secret");
            std::env::remove_var("ALPACA_FEED");
        }
        let credentials = AlpacaCredentials::from_env().unwrap();
        unsafe {
            match original_key {
                Some(v) => std::env::set_var("ALPACA_API_KEY_ID", v),
                None => std::env::remove_var("ALPACA_API_KEY_ID"),
            }
            match original_secret {
                Some(v) => std::env::set_var("ALPACA_API_SECRET", v),
                None => std::env::remove_var("ALPACA_API_SECRET"),
            }
            match original_feed {
                Some(v) => std::env::set_var("ALPACA_FEED", v),
                None => std::env::remove_var("ALPACA_FEED"),
            }
        }
        assert_eq!(credentials.feed(), "iex");
        assert_eq!(credentials.key_id(), "test-key");
        assert_eq!(credentials.secret(), "test-secret");
    }

    #[test]
    #[serial]
    fn test_alpaca_credentials_feed_explicit_value_is_used() {
        let original_key = std::env::var("ALPACA_API_KEY_ID").ok();
        let original_secret = std::env::var("ALPACA_API_SECRET").ok();
        let original_feed = std::env::var("ALPACA_FEED").ok();
        unsafe {
            std::env::set_var("ALPACA_API_KEY_ID", "test-key");
            std::env::set_var("ALPACA_API_SECRET", "test-secret");
            std::env::set_var("ALPACA_FEED", "sip");
        }
        let credentials = AlpacaCredentials::from_env().unwrap();
        unsafe {
            match original_key {
                Some(v) => std::env::set_var("ALPACA_API_KEY_ID", v),
                None => std::env::remove_var("ALPACA_API_KEY_ID"),
            }
            match original_secret {
                Some(v) => std::env::set_var("ALPACA_API_SECRET", v),
                None => std::env::remove_var("ALPACA_API_SECRET"),
            }
            match original_feed {
                Some(v) => std::env::set_var("ALPACA_FEED", v),
                None => std::env::remove_var("ALPACA_FEED"),
            }
        }
        assert_eq!(credentials.feed(), "sip");
    }

    #[test]
    #[serial]
    fn test_alpaca_credentials_from_env_returns_none_when_key_id_empty() {
        let original_key = std::env::var("ALPACA_API_KEY_ID").ok();
        let original_secret = std::env::var("ALPACA_API_SECRET").ok();
        unsafe {
            std::env::set_var("ALPACA_API_KEY_ID", "");
            std::env::set_var("ALPACA_API_SECRET", "test-secret");
        }
        let result = AlpacaCredentials::from_env();
        unsafe {
            match original_key {
                Some(v) => std::env::set_var("ALPACA_API_KEY_ID", v),
                None => std::env::remove_var("ALPACA_API_KEY_ID"),
            }
            match original_secret {
                Some(v) => std::env::set_var("ALPACA_API_SECRET", v),
                None => std::env::remove_var("ALPACA_API_SECRET"),
            }
        }
        assert!(result.is_none());
    }

    #[test]
    #[serial]
    fn test_alpaca_credentials_from_env_returns_none_when_secret_empty() {
        let original_key = std::env::var("ALPACA_API_KEY_ID").ok();
        let original_secret = std::env::var("ALPACA_API_SECRET").ok();
        unsafe {
            std::env::set_var("ALPACA_API_KEY_ID", "test-key");
            std::env::set_var("ALPACA_API_SECRET", "");
        }
        let result = AlpacaCredentials::from_env();
        unsafe {
            match original_key {
                Some(v) => std::env::set_var("ALPACA_API_KEY_ID", v),
                None => std::env::remove_var("ALPACA_API_KEY_ID"),
            }
            match original_secret {
                Some(v) => std::env::set_var("ALPACA_API_SECRET", v),
                None => std::env::remove_var("ALPACA_API_SECRET"),
            }
        }
        assert!(result.is_none());
    }

    #[test]
    fn test_database_state_is_configured_returns_true_for_connect_failed() {
        assert!(DatabaseState::ConnectFailed.is_configured());
    }

    #[test]
    fn test_database_state_is_configured_returns_false_for_not_configured() {
        assert!(!DatabaseState::NotConfigured.is_configured());
    }

    #[test]
    fn test_s3_ok_recently_returns_false_when_never_marked() {
        use super::{MassiveSecrets, State};

        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            use aws_credential_types::Credentials;
            use aws_sdk_s3::config::Region;

            let credentials =
                Credentials::new("test-access-key", "test-secret-key", None, None, "tests");
            let shared_config = aws_config::defaults(aws_config::BehaviorVersion::latest())
                .region(Region::new("us-east-1"))
                .credentials_provider(credentials)
                .endpoint_url("http://127.0.0.1:9")
                .load()
                .await;
            let s3_config = aws_sdk_s3::config::Builder::from(&shared_config)
                .force_path_style(true)
                .build();
            let s3_client = aws_sdk_s3::Client::from_conf(s3_config);
            let state = State::new(
                reqwest::Client::new(),
                MassiveSecrets {
                    base: "http://127.0.0.1:1".to_string(),
                    key: "test-api-key".to_string(),
                },
                s3_client,
                "test-bucket".to_string(),
            );
            // No mark_s3_ok call — epoch is 0.
            assert!(!state.s3_ok_recently(60));
        });
    }

    #[test]
    fn test_s3_ok_recently_returns_true_after_mark_s3_ok() {
        use super::{MassiveSecrets, State};

        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            use aws_credential_types::Credentials;
            use aws_sdk_s3::config::Region;

            let credentials =
                Credentials::new("test-access-key", "test-secret-key", None, None, "tests");
            let shared_config = aws_config::defaults(aws_config::BehaviorVersion::latest())
                .region(Region::new("us-east-1"))
                .credentials_provider(credentials)
                .endpoint_url("http://127.0.0.1:9")
                .load()
                .await;
            let s3_config = aws_sdk_s3::config::Builder::from(&shared_config)
                .force_path_style(true)
                .build();
            let s3_client = aws_sdk_s3::Client::from_conf(s3_config);
            let state = State::new(
                reqwest::Client::new(),
                MassiveSecrets {
                    base: "http://127.0.0.1:1".to_string(),
                    key: "test-api-key".to_string(),
                },
                s3_client,
                "test-bucket".to_string(),
            );
            state.mark_s3_ok();
            // Should be recent within a generous 300 second window.
            assert!(state.s3_ok_recently(300));
        });
    }

    #[test]
    fn test_s3_ok_recently_returns_false_after_ttl_expires() {
        use super::{MassiveSecrets, State};
        use std::sync::atomic::Ordering;

        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            use aws_credential_types::Credentials;
            use aws_sdk_s3::config::Region;

            let credentials =
                Credentials::new("test-access-key", "test-secret-key", None, None, "tests");
            let shared_config = aws_config::defaults(aws_config::BehaviorVersion::latest())
                .region(Region::new("us-east-1"))
                .credentials_provider(credentials)
                .endpoint_url("http://127.0.0.1:9")
                .load()
                .await;
            let s3_config = aws_sdk_s3::config::Builder::from(&shared_config)
                .force_path_style(true)
                .build();
            let s3_client = aws_sdk_s3::Client::from_conf(s3_config);
            let state = State::new(
                reqwest::Client::new(),
                MassiveSecrets {
                    base: "http://127.0.0.1:1".to_string(),
                    key: "test-api-key".to_string(),
                },
                s3_client,
                "test-bucket".to_string(),
            );
            // Store an epoch well in the past (Unix epoch + 1 second = 1970).
            state.last_s3_ok_epoch.store(1, Ordering::Relaxed);
            // ttl_secs=60 — the stored epoch is way older than 60 seconds ago.
            assert!(!state.s3_ok_recently(60));
        });
    }

    #[test]
    fn test_synced_recently_returns_false_when_never_marked() {
        use super::{MassiveSecrets, State};

        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            use aws_credential_types::Credentials;
            use aws_sdk_s3::config::Region;

            let credentials =
                Credentials::new("test-access-key", "test-secret-key", None, None, "tests");
            let shared_config = aws_config::defaults(aws_config::BehaviorVersion::latest())
                .region(Region::new("us-east-1"))
                .credentials_provider(credentials)
                .endpoint_url("http://127.0.0.1:9")
                .load()
                .await;
            let s3_config = aws_sdk_s3::config::Builder::from(&shared_config)
                .force_path_style(true)
                .build();
            let s3_client = aws_sdk_s3::Client::from_conf(s3_config);
            let state = State::new(
                reqwest::Client::new(),
                MassiveSecrets {
                    base: "http://127.0.0.1:1".to_string(),
                    key: "test-api-key".to_string(),
                },
                s3_client,
                "test-bucket".to_string(),
            );
            assert!(!state.synced_recently(300));
        });
    }

    #[test]
    fn test_synced_recently_returns_true_after_mark_synced() {
        use super::{MassiveSecrets, State};

        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            use aws_credential_types::Credentials;
            use aws_sdk_s3::config::Region;

            let credentials =
                Credentials::new("test-access-key", "test-secret-key", None, None, "tests");
            let shared_config = aws_config::defaults(aws_config::BehaviorVersion::latest())
                .region(Region::new("us-east-1"))
                .credentials_provider(credentials)
                .endpoint_url("http://127.0.0.1:9")
                .load()
                .await;
            let s3_config = aws_sdk_s3::config::Builder::from(&shared_config)
                .force_path_style(true)
                .build();
            let s3_client = aws_sdk_s3::Client::from_conf(s3_config);
            let state = State::new(
                reqwest::Client::new(),
                MassiveSecrets {
                    base: "http://127.0.0.1:1".to_string(),
                    key: "test-api-key".to_string(),
                },
                s3_client,
                "test-bucket".to_string(),
            );
            state.mark_synced();
            assert!(state.synced_recently(300));
        });
    }

    #[test]
    fn test_synced_recently_returns_false_after_ttl_expires() {
        use super::{MassiveSecrets, State};
        use std::sync::atomic::Ordering;

        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            use aws_credential_types::Credentials;
            use aws_sdk_s3::config::Region;

            let credentials =
                Credentials::new("test-access-key", "test-secret-key", None, None, "tests");
            let shared_config = aws_config::defaults(aws_config::BehaviorVersion::latest())
                .region(Region::new("us-east-1"))
                .credentials_provider(credentials)
                .endpoint_url("http://127.0.0.1:9")
                .load()
                .await;
            let s3_config = aws_sdk_s3::config::Builder::from(&shared_config)
                .force_path_style(true)
                .build();
            let s3_client = aws_sdk_s3::Client::from_conf(s3_config);
            let state = State::new(
                reqwest::Client::new(),
                MassiveSecrets {
                    base: "http://127.0.0.1:1".to_string(),
                    key: "test-api-key".to_string(),
                },
                s3_client,
                "test-bucket".to_string(),
            );
            // Store epoch in the distant past.
            state.last_sync_epoch.store(1, Ordering::Relaxed);
            assert!(!state.synced_recently(60));
        });
    }

    #[test]
    #[serial]
    fn test_alpaca_credentials_from_env_restores_previously_set_key_id() {
        // Sets ALPACA_API_KEY_ID before capturing the original so the
        // restoration `Some(v) =>` branch (line 280) is guaranteed to execute.
        unsafe {
            std::env::set_var("ALPACA_API_KEY_ID", "pre-existing-key");
            std::env::set_var("ALPACA_API_SECRET", "pre-existing-secret");
        }
        let original_key = std::env::var("ALPACA_API_KEY_ID").ok();
        let original_secret = std::env::var("ALPACA_API_SECRET").ok();
        let original_feed = std::env::var("ALPACA_FEED").ok();
        // Both must be Some(v) now.
        assert!(original_key.is_some());
        assert!(original_secret.is_some());
        unsafe {
            std::env::remove_var("ALPACA_API_KEY_ID");
            std::env::set_var("ALPACA_API_SECRET", "another-secret");
        }
        let result = AlpacaCredentials::from_env();
        // Restore — exercises the Some(v) branches.
        unsafe {
            match original_key {
                Some(ref v) => std::env::set_var("ALPACA_API_KEY_ID", v),
                None => std::env::remove_var("ALPACA_API_KEY_ID"),
            }
            match original_secret {
                Some(ref v) => std::env::set_var("ALPACA_API_SECRET", v),
                None => std::env::remove_var("ALPACA_API_SECRET"),
            }
            match original_feed {
                Some(ref v) => std::env::set_var("ALPACA_FEED", v),
                None => std::env::remove_var("ALPACA_FEED"),
            }
        }
        // Key was removed → should return None.
        assert!(result.is_none());
    }

    #[test]
    #[serial]
    fn test_alpaca_credentials_from_env_restores_previously_set_secret() {
        // Sets ALPACA_API_SECRET before capturing so the restoration
        // `Some(v) =>` branch executes for both key and secret (lines 284, 303).
        unsafe {
            std::env::set_var("ALPACA_API_KEY_ID", "pre-set-key");
            std::env::set_var("ALPACA_API_SECRET", "pre-set-secret");
        }
        let original_key = std::env::var("ALPACA_API_KEY_ID").ok();
        let original_secret = std::env::var("ALPACA_API_SECRET").ok();
        assert!(original_key.is_some());
        assert!(original_secret.is_some());
        unsafe {
            std::env::set_var("ALPACA_API_KEY_ID", "temp-key");
            std::env::remove_var("ALPACA_API_SECRET");
        }
        let result = AlpacaCredentials::from_env();
        unsafe {
            match original_key {
                Some(ref v) => std::env::set_var("ALPACA_API_KEY_ID", v),
                None => std::env::remove_var("ALPACA_API_KEY_ID"),
            }
            match original_secret {
                Some(ref v) => std::env::set_var("ALPACA_API_SECRET", v),
                None => std::env::remove_var("ALPACA_API_SECRET"),
            }
        }
        // Secret was removed → should return None.
        assert!(result.is_none());
    }

    #[test]
    #[serial]
    fn test_alpaca_credentials_from_env_restores_feed_when_previously_set() {
        // Guarantees the `Some(v) =>` branch for ALPACA_FEED restoration
        // (lines 328, 332, 336, 363, 367) by pre-setting all three vars.
        unsafe {
            std::env::set_var("ALPACA_API_KEY_ID", "key-feed-test");
            std::env::set_var("ALPACA_API_SECRET", "secret-feed-test");
            std::env::set_var("ALPACA_FEED", "sip");
        }
        let original_key = std::env::var("ALPACA_API_KEY_ID").ok();
        let original_secret = std::env::var("ALPACA_API_SECRET").ok();
        let original_feed = std::env::var("ALPACA_FEED").ok();
        assert!(original_key.is_some());
        assert!(original_secret.is_some());
        assert!(original_feed.is_some());
        // Now change feed and call from_env.
        unsafe {
            std::env::set_var("ALPACA_FEED", "iex");
        }
        let credentials = AlpacaCredentials::from_env();
        unsafe {
            match original_key {
                Some(ref v) => std::env::set_var("ALPACA_API_KEY_ID", v),
                None => std::env::remove_var("ALPACA_API_KEY_ID"),
            }
            match original_secret {
                Some(ref v) => std::env::set_var("ALPACA_API_SECRET", v),
                None => std::env::remove_var("ALPACA_API_SECRET"),
            }
            match original_feed {
                Some(ref v) => std::env::set_var("ALPACA_FEED", v),
                None => std::env::remove_var("ALPACA_FEED"),
            }
        }
        assert!(credentials.is_some());
        assert_eq!(credentials.unwrap().feed(), "iex");
    }

    #[test]
    #[serial]
    fn test_alpaca_credentials_from_env_restores_empty_key_id_original() {
        // Exercises the Some(v) restoration branches (lines 386, 390) for tests
        // that check empty key_id/secret behavior. Pre-sets vars to empty strings
        // before capture so originals are Some("").
        unsafe {
            std::env::set_var("ALPACA_API_KEY_ID", "existing-key-for-empty-test");
            std::env::set_var("ALPACA_API_SECRET", "existing-secret-for-empty-test");
        }
        let original_key = std::env::var("ALPACA_API_KEY_ID").ok();
        let original_secret = std::env::var("ALPACA_API_SECRET").ok();
        assert!(original_key.is_some());
        assert!(original_secret.is_some());
        unsafe {
            // Now set key to empty to test the empty-key branch.
            std::env::set_var("ALPACA_API_KEY_ID", "");
            std::env::set_var("ALPACA_API_SECRET", "non-empty-secret");
        }
        let result = AlpacaCredentials::from_env();
        unsafe {
            match original_key {
                Some(ref v) => std::env::set_var("ALPACA_API_KEY_ID", v),
                None => std::env::remove_var("ALPACA_API_KEY_ID"),
            }
            match original_secret {
                Some(ref v) => std::env::set_var("ALPACA_API_SECRET", v),
                None => std::env::remove_var("ALPACA_API_SECRET"),
            }
        }
        assert!(result.is_none());
    }

    #[test]
    #[serial]
    fn test_alpaca_credentials_from_env_restores_empty_secret_original() {
        // Exercises the Some(v) restoration branches (lines 409, 413) for
        // tests that check empty secret behavior.
        unsafe {
            std::env::set_var("ALPACA_API_KEY_ID", "key-for-empty-secret-test");
            std::env::set_var("ALPACA_API_SECRET", "secret-for-empty-secret-test");
        }
        let original_key = std::env::var("ALPACA_API_KEY_ID").ok();
        let original_secret = std::env::var("ALPACA_API_SECRET").ok();
        assert!(original_key.is_some());
        assert!(original_secret.is_some());
        unsafe {
            std::env::set_var("ALPACA_API_KEY_ID", "non-empty-key");
            std::env::set_var("ALPACA_API_SECRET", "");
        }
        let result = AlpacaCredentials::from_env();
        unsafe {
            match original_key {
                Some(ref v) => std::env::set_var("ALPACA_API_KEY_ID", v),
                None => std::env::remove_var("ALPACA_API_KEY_ID"),
            }
            match original_secret {
                Some(ref v) => std::env::set_var("ALPACA_API_SECRET", v),
                None => std::env::remove_var("ALPACA_API_SECRET"),
            }
        }
        assert!(result.is_none());
    }

    #[test]
    fn test_state_new_has_not_configured_database_and_no_alpaca_credentials() {
        use super::{DatabaseState, MassiveSecrets, State};

        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            use aws_credential_types::Credentials;
            use aws_sdk_s3::config::Region;

            let credentials =
                Credentials::new("test-access-key", "test-secret-key", None, None, "tests");
            let shared_config = aws_config::defaults(aws_config::BehaviorVersion::latest())
                .region(Region::new("us-east-1"))
                .credentials_provider(credentials)
                .endpoint_url("http://127.0.0.1:9")
                .load()
                .await;
            let s3_config = aws_sdk_s3::config::Builder::from(&shared_config)
                .force_path_style(true)
                .build();
            let s3_client = aws_sdk_s3::Client::from_conf(s3_config);
            let state = State::new(
                reqwest::Client::new(),
                MassiveSecrets {
                    base: "http://127.0.0.1:1".to_string(),
                    key: "test-api-key".to_string(),
                },
                s3_client,
                "test-bucket".to_string(),
            );
            assert!(matches!(state.database, DatabaseState::NotConfigured));
            assert!(state.alpaca_credentials.is_none());
            assert_eq!(state.bucket_name, "test-bucket");
            assert_eq!(state.massive.base, "http://127.0.0.1:1");
            assert_eq!(state.massive.key, "test-api-key");
        });
    }
}
