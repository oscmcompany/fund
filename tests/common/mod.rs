#![allow(dead_code)]

use aws_credential_types::Credentials;
use aws_sdk_s3::{config::Region, primitives::ByteStream, Client as S3Client};
use chrono::{DateTime, Duration, Utc};
use sqlx::PgPool;
use std::{sync::OnceLock, time::Duration as StdDuration};
use testcontainers::{runners::AsyncRunner, ContainerAsync};
use testcontainers_modules::localstack::LocalStack;
use testcontainers_modules::postgres::Postgres;
use uuid::Uuid;

// ---------------------------------------------------------------------------
// S3 / LocalStack
// ---------------------------------------------------------------------------

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
    tokio::time::sleep(StdDuration::from_secs(5)).await;

    let host = container.get_host().await.unwrap();
    let port = {
        let mut attempts = 0u32;
        loop {
            match container.get_host_port_ipv4(4566).await {
                Ok(port) => break port,
                Err(_) if attempts < 10 => {
                    attempts += 1;
                    tokio::time::sleep(StdDuration::from_millis(500)).await;
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

// ---------------------------------------------------------------------------
// PostgreSQL (testcontainers)
// ---------------------------------------------------------------------------

const SCHEMA_SQL: &str = include_str!("../../schema.sql");

static PG_POOL: tokio::sync::OnceCell<PgPool> = tokio::sync::OnceCell::const_new();
static PG_CONTAINER: OnceLock<&'static ContainerAsync<Postgres>> = OnceLock::new();

/// Strips lines that require pg_cron or TimescaleDB (unavailable in vanilla Postgres).
fn filter_schema_for_test(schema: &str) -> String {
    let mut inside_cron_block = false;
    schema
        .lines()
        .filter(|line| {
            let trimmed = line.trim().to_lowercase();
            if trimmed.starts_with("do $do$") {
                inside_cron_block = true;
            }
            if inside_cron_block {
                if trimmed.starts_with("$do$;") {
                    inside_cron_block = false;
                }
                return false;
            }
            !trimmed.starts_with("create extension if not exists pg_cron")
                && !trimmed.starts_with("create extension if not exists timescaledb")
                && !trimmed.starts_with("select cron.schedule")
                && !trimmed.starts_with("select create_hypertable")
                && !trimmed.starts_with("select add_retention_policy")
        })
        .collect::<Vec<_>>()
        .join("\n")
}

/// Returns a connection pool to a shared testcontainers Postgres instance.
///
/// The first call starts the container, applies the filtered schema, and leaks
/// the container handle so it lives for the entire test-binary process. Subsequent
/// calls return a clone of the same pool, avoiding redundant connections.
pub async fn get_pg_pool() -> PgPool {
    PG_POOL
        .get_or_init(|| async {
            let container = Postgres::default()
                .start()
                .await
                .expect("Failed to start PostgreSQL container — is Docker running?");

            let host = container.get_host().await.unwrap();
            let port = container.get_host_port_ipv4(5432).await.unwrap();
            let url = format!("postgresql://postgres:postgres@{}:{}/postgres", host, port);

            let connect_deadline = tokio::time::Instant::now() + StdDuration::from_secs(30);
            let pool = loop {
                match PgPool::connect(&url).await {
                    Ok(pool) => break pool,
                    Err(error) => {
                        if tokio::time::Instant::now() >= connect_deadline {
                            panic!("Failed to connect to PostgreSQL within timeout: {error}");
                        }
                        tokio::time::sleep(StdDuration::from_millis(250)).await;
                    }
                }
            };

            let filtered_schema = filter_schema_for_test(SCHEMA_SQL);
            sqlx::raw_sql(&filtered_schema)
                .execute(&pool)
                .await
                .unwrap();

            let leaked: &'static ContainerAsync<Postgres> = Box::leak(Box::new(container));
            let _ = PG_CONTAINER.set(leaked);

            pool
        })
        .await
        .clone()
}

/// Truncates all portfolio-related tables in dependency order.
///
/// Uses `TRUNCATE ... CASCADE` on the root table to handle foreign key
/// constraints in a single statement, then truncates leaf tables individually.
pub async fn clean_portfolio_tables(pool: &PgPool) {
    sqlx::raw_sql(
        "TRUNCATE equity_rebalance_sessions CASCADE; \
         TRUNCATE equity_portfolio_snapshots; \
         TRUNCATE equity_reconciliation_events; \
         TRUNCATE event_consumer_offsets; \
         DELETE FROM events;",
    )
    .execute(pool)
    .await
    .expect("Failed to clean portfolio tables");
}

// ---------------------------------------------------------------------------
// Seed helpers
// ---------------------------------------------------------------------------

/// Inserts equity prediction rows directly via SQL.
///
/// Each prediction is inserted with the given `correlation_id` and `created_at`
/// timestamp, allowing tests to control staleness checks.
pub async fn seed_equity_predictions(
    pool: &PgPool,
    correlation_id: Uuid,
    model_run_id: &str,
    tickers: &[&str],
    created_at: DateTime<Utc>,
) {
    let timestamp = created_at;
    for ticker in tickers {
        sqlx::query(
            "INSERT INTO equity_predictions \
             (correlation_id, model_run_id, ticker, timestamp, quantile_10, quantile_50, quantile_90, created_at) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8) \
             ON CONFLICT (ticker, timestamp) DO UPDATE SET \
                 correlation_id = EXCLUDED.correlation_id, \
                 quantile_10 = EXCLUDED.quantile_10, \
                 quantile_50 = EXCLUDED.quantile_50, \
                 quantile_90 = EXCLUDED.quantile_90, \
                 created_at = EXCLUDED.created_at",
        )
        .bind(correlation_id)
        .bind(model_run_id)
        .bind(*ticker)
        .bind(timestamp)
        .bind(-0.01_f64)
        .bind(0.005_f64)
        .bind(0.02_f64)
        .bind(created_at)
        .execute(pool)
        .await
        .expect("Failed to seed equity prediction");
    }
}

/// Inserts daily equity bar rows for the given tickers over a date range.
///
/// Generates one bar per ticker per day with synthetic prices based on a
/// deterministic walk seeded by the ticker name. This provides non-constant
/// returns suitable for correlation and beta calculations.
pub async fn seed_equity_bars(pool: &PgPool, tickers: &[&str], days: i64) {
    let now = Utc::now();
    for ticker in tickers {
        let base_price = 100.0 + (*ticker).len() as f64 * 10.0;
        for day in (0..days).rev() {
            let timestamp = now - Duration::days(day);
            // Sinusoidal variation so log returns are non-zero and non-constant
            let factor = 1.0 + 0.02 * (day as f64 * 0.5).sin();
            let close = base_price * factor;
            let open = close * 0.998;
            let high = close * 1.005;
            let low = close * 0.995;

            sqlx::query(
                "INSERT INTO equity_bars \
                 (ticker, timestamp, open_price, high_price, low_price, close_price, volume, inserted_at) \
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8) \
                 ON CONFLICT (ticker, timestamp) DO UPDATE SET \
                     close_price = EXCLUDED.close_price",
            )
            .bind(*ticker)
            .bind(timestamp)
            .bind(open)
            .bind(high)
            .bind(low)
            .bind(close)
            .bind(1_000_000_i64)
            .bind(timestamp)
            .execute(pool)
            .await
            .expect("Failed to seed equity bar");
        }
    }
}

/// Inserts equity detail rows (ticker + sector) for the given tickers.
pub async fn seed_equity_details(pool: &PgPool, ticker_sectors: &[(&str, &str)]) {
    for (ticker, sector) in ticker_sectors {
        sqlx::query(
            "INSERT INTO equity_details (ticker, sector) \
             VALUES ($1, $2) \
             ON CONFLICT (ticker) DO UPDATE SET sector = EXCLUDED.sector",
        )
        .bind(*ticker)
        .bind(*sector)
        .execute(pool)
        .await
        .expect("Failed to seed equity detail");
    }
}
