use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use aws_sdk_s3::Client as S3Client;
use reqwest::Client as HTTPClient;
use sqlx::PgPool;
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

use crate::database::set_bucket_guc;

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
    pub pool: Option<PgPool>,
    pub database_url_configured: bool,
    pub alpaca_key_id: String,
    pub alpaca_secret: String,
    pub alpaca_feed: String,
    pub active_symbols: Arc<RwLock<HashSet<String>>>,
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
        let config = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;

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

        let alpaca_key_id = std::env::var("ALPACA_KEY_ID").unwrap_or_default();
        let alpaca_secret = std::env::var("ALPACA_SECRET").unwrap_or_default();
        let alpaca_feed = std::env::var("ALPACA_FEED").unwrap_or_else(|_| "iex".to_string());
        info!("Using Alpaca feed: {}", alpaca_feed);

        let (pool, database_url_configured) = match std::env::var("DATABASE_URL") {
            Ok(database_url) => {
                debug!("Connecting to PostgreSQL");
                match PgPool::connect(&database_url).await {
                    Ok(pool) => {
                        info!("Connected to PostgreSQL");
                        if let Err(error) = set_bucket_guc(&pool, &bucket_name).await {
                            warn!("Failed to set app.bucket_name database GUC: {}", error);
                        }
                        (Some(pool), true)
                    }
                    Err(error) => {
                        warn!("Failed to connect to PostgreSQL: {}", error);
                        (None, true)
                    }
                }
            }
            Err(_) => {
                info!("DATABASE_URL not set, PostgreSQL disabled");
                (None, false)
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
            pool,
            database_url_configured,
            alpaca_key_id,
            alpaca_secret,
            alpaca_feed,
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
            pool: None,
            database_url_configured: false,
            alpaca_key_id: String::new(),
            alpaca_secret: String::new(),
            alpaca_feed: "iex".to_string(),
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
