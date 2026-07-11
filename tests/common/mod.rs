#![allow(dead_code)]

use aws_credential_types::Credentials;
use aws_sdk_s3::{config::Region, primitives::ByteStream, Client as S3Client};
use std::{sync::OnceLock, time::Duration};
use testcontainers::{runners::AsyncRunner, ContainerAsync};
use testcontainers_modules::localstack::LocalStack;

const TEST_BUCKET: &str = "test-bucket";
const TEST_ACCESS_KEY: &str = "test";
const TEST_SECRET_KEY: &str = "test";
const TEST_REGION: &str = "us-east-1";

static LOCALSTACK_ENDPOINT: OnceLock<String> = OnceLock::new();
static LOCALSTACK_CONTAINER: OnceLock<&'static ContainerAsync<LocalStack>> = OnceLock::new();
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

    // Give LocalStack additional time to fully initialize services
    tokio::time::sleep(Duration::from_secs(5)).await;

    let host = container.get_host().await.unwrap();
    let port = {
        let mut attempts = 0u32;
        loop {
            match container.get_host_port_ipv4(4566).await {
                Ok(port) => break port,
                Err(_) if attempts < 10 => {
                    attempts += 1;
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }
                Err(error) => panic!(
                    "LocalStack port 4566 not available after retries: {}",
                    error
                ),
            }
        }
    };
    let endpoint = format!("http://{}:{}", host, port);

    // INTENTIONAL LEAK: Container is leaked to keep it alive for entire test run.
    //
    // Rationale:
    // - Tests use #[serial] for sequential execution within this process
    // - All tests share the same LocalStack container for performance
    // - Container cleanup happens automatically when process exits
    // - Storing container reference prevents testcontainers from losing port mapping
    //
    // Trade-off: Small memory leak during test execution vs architectural complexity
    // Impact: Container memory is reclaimed when test process terminates
    let leaked_container: &'static ContainerAsync<LocalStack> = Box::leak(Box::new(container));
    let _ = LOCALSTACK_CONTAINER.set(leaked_container);
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

/// Start LocalStack, create the test bucket, clean it, and return the endpoint URL
/// and a ready-to-use S3 client.
pub async fn setup_test_bucket() -> (String, S3Client) {
    initialize_test_tracing();

    let endpoint = get_localstack_endpoint().await;
    let s3_client = create_test_s3_client(&endpoint).await;

    // Create bucket (ignore AlreadyExists / BucketAlreadyOwnedByYou)
    let _ = s3_client.create_bucket().bucket(TEST_BUCKET).send().await;

    clean_bucket(&s3_client).await;

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
