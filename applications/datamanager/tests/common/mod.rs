use aws_credential_types::Credentials;
use aws_sdk_s3::{config::Region, primitives::ByteStream, Client as S3Client};
use axum::Router;
use std::{net::SocketAddr, sync::OnceLock, time::Duration};
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::localstack::LocalStack;
use tokio::{net::TcpListener, sync::oneshot, task::JoinHandle};

const TEST_BUCKET: &str = "test-bucket";
const TEST_ACCESS_KEY: &str = "test";
const TEST_SECRET_KEY: &str = "test";
const TEST_REGION: &str = "us-east-1";

static LOCALSTACK_ENDPOINT: OnceLock<String> = OnceLock::new();
static TRACING_INIT: std::sync::Once = std::sync::Once::new();

pub struct EnvironmentVariableGuard {
    name: String,
    original_value: Option<String>,
}

impl EnvironmentVariableGuard {
    pub fn set(name: &str, value: &str) -> Self {
        let original_value = std::env::var(name).ok();
        unsafe {
            std::env::set_var(name, value);
        }

        Self {
            name: name.to_string(),
            original_value,
        }
    }

    pub fn remove(name: &str) -> Self {
        let original_value = std::env::var(name).ok();
        unsafe {
            std::env::remove_var(name);
        }

        Self {
            name: name.to_string(),
            original_value,
        }
    }
}

impl Drop for EnvironmentVariableGuard {
    fn drop(&mut self) {
        match self.original_value.as_ref() {
            Some(value) => unsafe {
                std::env::set_var(&self.name, value);
            },
            None => unsafe {
                std::env::remove_var(&self.name);
            },
        }
    }
}

pub struct DuckDbEnvironmentGuard {
    _guards: Vec<EnvironmentVariableGuard>,
}

impl DuckDbEnvironmentGuard {
    pub fn new(endpoint_host_port: &str) -> Self {
        let guards = vec![
            EnvironmentVariableGuard::set("AWS_REGION", TEST_REGION),
            EnvironmentVariableGuard::set("AWS_ACCESS_KEY_ID", TEST_ACCESS_KEY),
            EnvironmentVariableGuard::set("AWS_SECRET_ACCESS_KEY", TEST_SECRET_KEY),
            EnvironmentVariableGuard::set("AWS_EC2_METADATA_DISABLED", "true"),
            EnvironmentVariableGuard::set("DUCKDB_S3_ENDPOINT", endpoint_host_port),
            EnvironmentVariableGuard::set("DUCKDB_S3_USE_SSL", "false"),
        ];
        Self { _guards: guards }
    }
}

pub fn initialize_test_tracing() {
    TRACING_INIT.call_once(|| {
        let _ = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::TRACE)
            .with_test_writer()
            .try_init();
    });
}

pub async fn get_localstack_endpoint() -> String {
    if let Some(endpoint) = LOCALSTACK_ENDPOINT.get() {
        return endpoint.clone();
    }

    let container = LocalStack::default()
        .start()
        .await
        .expect("Failed to start LocalStack container — is Docker running?");

    let host = container.get_host().await.unwrap();
    let port = container.get_host_port_ipv4(4566).await.unwrap();
    let endpoint = format!("http://{}:{}", host, port);

    // INTENTIONAL LEAK: Container is leaked to keep it alive for entire test run.
    //
    // Rationale:
    // - Tests use #[serial] for sequential execution within this process
    // - All tests share the same LocalStack container for performance
    // - Container cleanup happens automatically when process exits
    // - Alternative (proper Drop cleanup) requires complex lifetime management
    //   across static OnceLock, creating more complexity than benefit
    //
    // Trade-off: Small memory leak during test execution vs architectural complexity
    // Impact: Container memory is reclaimed when test process terminates
    Box::leak(Box::new(container));

    let _ = LOCALSTACK_ENDPOINT.set(endpoint.clone());
    endpoint
}

pub async fn create_test_s3_client(endpoint_url: &str) -> S3Client {
    let credentials = Credentials::new(TEST_ACCESS_KEY, TEST_SECRET_KEY, None, None, "tests");

    let shared_config = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .region(Region::new(TEST_REGION))
        .credentials_provider(credentials)
        .endpoint_url(endpoint_url)
        .load()
        .await;

    let s3_config = aws_sdk_s3::config::Builder::from(&shared_config)
        .force_path_style(true)
        .build();

    S3Client::from_conf(s3_config)
}

/// Start LocalStack, create the test bucket, clean it, configure DuckDB env vars,
/// and return the endpoint URL and a ready-to-use S3 client.
pub async fn setup_test_bucket() -> (String, S3Client) {
    initialize_test_tracing();

    let endpoint = get_localstack_endpoint().await;
    let s3_client = create_test_s3_client(&endpoint).await;

    // Create bucket (ignore AlreadyExists / BucketAlreadyOwnedByYou)
    let _ = s3_client.create_bucket().bucket(TEST_BUCKET).send().await;

    clean_bucket(&s3_client).await;

    let host_port = endpoint
        .strip_prefix("http://")
        .unwrap_or(&endpoint)
        .to_string();
    set_duckdb_aws_environment(&host_port);

    (endpoint, s3_client)
}

pub async fn clean_bucket(s3_client: &S3Client) {
    let mut continuation_token: Option<String> = None;

    loop {
        let mut request = s3_client.list_objects_v2().bucket(TEST_BUCKET);
        if let Some(token) = &continuation_token {
            request = request.continuation_token(token);
        }

        let output = match request.send().await {
            Ok(output) => output,
            Err(_) => break,
        };

        let contents = output.contents();
        for object in contents {
            if let Some(key) = object.key() {
                let _ = s3_client
                    .delete_object()
                    .bucket(TEST_BUCKET)
                    .key(key)
                    .send()
                    .await;
            }
        }

        if output.is_truncated() == Some(true) {
            continuation_token = output.next_continuation_token().map(|s| s.to_string());
        } else {
            break;
        }
    }
}

pub fn set_duckdb_aws_environment(endpoint_host_port: &str) {
    unsafe {
        std::env::set_var("AWS_REGION", TEST_REGION);
        std::env::set_var("AWS_ACCESS_KEY_ID", TEST_ACCESS_KEY);
        std::env::set_var("AWS_SECRET_ACCESS_KEY", TEST_SECRET_KEY);
        std::env::set_var("AWS_EC2_METADATA_DISABLED", "true");
        std::env::set_var("DUCKDB_S3_ENDPOINT", endpoint_host_port);
        std::env::set_var("DUCKDB_S3_USE_SSL", "false");
    }
}

pub async fn put_test_object(s3_client: &S3Client, key: &str, bytes: Vec<u8>) {
    s3_client
        .put_object()
        .bucket(TEST_BUCKET)
        .key(key)
        .body(ByteStream::from(bytes))
        .send()
        .await
        .expect("Failed to put test object");
}

pub fn test_bucket_name() -> String {
    TEST_BUCKET.to_string()
}

pub struct SpawnedAppServer {
    pub base_url: String,
    shutdown_sender: Option<oneshot::Sender<()>>,
    server_handle: Option<JoinHandle<()>>,
}

impl SpawnedAppServer {
    pub async fn start(app: Router) -> Self {
        initialize_test_tracing();

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let local_address = listener.local_addr().unwrap();
        let base_url = format!("http://{}", local_address);

        let (shutdown_sender, shutdown_receiver) = oneshot::channel::<()>();

        let server_handle = tokio::spawn(async move {
            let server = axum::serve(listener, app);
            tokio::select! {
                _ = server => {}
                _ = shutdown_receiver => {}
            }
        });

        wait_for_server_start(local_address).await;

        Self {
            base_url,
            shutdown_sender: Some(shutdown_sender),
            server_handle: Some(server_handle),
        }
    }

    pub fn url(&self, path: &str) -> String {
        if path.starts_with('/') {
            format!("{}{}", self.base_url, path)
        } else {
            format!("{}/{}", self.base_url, path)
        }
    }
}

impl Drop for SpawnedAppServer {
    fn drop(&mut self) {
        if let Some(shutdown_sender) = self.shutdown_sender.take() {
            let _ = shutdown_sender.send(());
        }

        if let Some(server_handle) = self.server_handle.take() {
            server_handle.abort();
        }
    }
}

async fn wait_for_server_start(address: SocketAddr) {
    for _ in 0..50 {
        if tokio::net::TcpStream::connect(address).await.is_ok() {
            return;
        }

        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    panic!("Server did not start listening on {}", address);
}
