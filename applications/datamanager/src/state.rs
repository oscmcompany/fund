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

        let bucket_name =
            std::env::var("AWS_S3_DATA_BUCKET_NAME").unwrap_or_else(|_| "fund-data".to_string());
        info!("Using S3 bucket: {}", bucket_name);

        let massive_base_url = std::env::var("MASSIVE_BASE_URL")
            .unwrap_or_else(|_| "https://api.massive.io".to_string());
        info!("Using Massive API base URL: {}", massive_base_url);

        let massive_api_key = std::env::var("MASSIVE_API_KEY").unwrap_or_else(|_| String::new());

        info!("Application state initialized successfully");

        Self {
            http_client,
            massive: MassiveSecrets {
                base: massive_base_url,
                key: massive_api_key,
            },
            s3_client,
            bucket_name,
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
        }
    }
}
