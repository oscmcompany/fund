use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use aws_sdk_s3::Client as S3Client;
use reqwest::Client as HTTPClient;
use tracing::{debug, info};

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

        let bucket_name = std::env::var("AWS_S3_DATA_BUCKET_NAME")
            .expect("AWS_S3_DATA_BUCKET_NAME environment variable must be set");
        info!("Using S3 bucket: {}", bucket_name);

        let massive_base_url = std::env::var("MASSIVE_BASE_URL")
            .expect("MASSIVE_BASE_URL environment variable must be set");
        info!("Using Massive API base URL: {}", massive_base_url);

        let massive_api_key = std::env::var("MASSIVE_API_KEY")
            .expect("MASSIVE_API_KEY environment variable must be set");

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
}
