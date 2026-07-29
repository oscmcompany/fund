#![allow(dead_code)]

use aws_credential_types::Credentials;
use aws_sdk_s3::{config::Region, primitives::ByteStream, Client as S3Client};
use chrono::{DateTime, Duration, Utc};
use sqlx::PgPool;
use uuid::Uuid;

// ---------------------------------------------------------------------------
// S3 (devenv-managed SeaweedFS)
// ---------------------------------------------------------------------------

const TEST_BUCKET: &str = "test-bucket";
const TEST_REGION: &str = "us-east-1";

static TRACING_INIT: std::sync::Once = std::sync::Once::new();

/// Returns the S3-compatible endpoint, defaulting to the devenv object store.
///
/// The suite previously started a LocalStack container through testcontainers,
/// which required a Docker daemon that neither the devenv shell nor CI has. It
/// never needed Docker as such — only an HTTP endpoint speaking S3 — so it now
/// points at a SeaweedFS process devenv runs directly.
fn s3_endpoint() -> String {
    std::env::var("TEST_S3_ENDPOINT").unwrap_or_else(|_| "http://127.0.0.1:8333".to_string())
}

fn s3_access_key() -> String {
    std::env::var("TEST_S3_ACCESS_KEY").unwrap_or_else(|_| "fundtest".to_string())
}

fn s3_secret_key() -> String {
    std::env::var("TEST_S3_SECRET_KEY").unwrap_or_else(|_| "fundtestsecret".to_string())
}

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

pub async fn create_test_s3_client(endpoint_url: &str) -> S3Client {
    let credentials = Credentials::new(s3_access_key(), s3_secret_key(), None, None, "tests");

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

/// Creates the test bucket, empties it, and returns the endpoint and a client.
///
/// Reachability is asserted explicitly. `create_bucket` tolerates an
/// already-exists error and `clean_bucket` swallows listing failures, so an
/// absent object store would otherwise surface much later as a confusing
/// assertion failure rather than as "the service is not running".
pub async fn setup_test_bucket() -> (String, S3Client) {
    initialize_test_tracing();

    let endpoint = s3_endpoint();
    let s3_client = create_test_s3_client(&endpoint).await;

    match s3_client.list_buckets().send().await {
        Ok(_) => {}
        Err(error) => panic!(
            "S3-compatible endpoint at {endpoint} is unreachable: {error}\n\
             Start it with `devenv --profile test up object-store --detach`."
        ),
    }

    // Tolerates AlreadyExists / BucketAlreadyOwnedByYou across repeated runs.
    let _ = s3_client.create_bucket().bucket(TEST_BUCKET).send().await;

    // MinIO persists between runs, unlike the container that was recreated each
    // time, so emptying the bucket is now load-bearing rather than tidiness.
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
// PostgreSQL (devenv-managed)
// ---------------------------------------------------------------------------

const SCHEMA_SQL: &str = include_str!("../../schema.sql");

/// Database the integration tests own outright.
///
/// Deliberately not the development database: these tests truncate tables, and
/// pointing them at `fund` would destroy local data on every run.
const TEST_DATABASE: &str = "fund_test";

/// Marks that the test database exists and carries the schema.
///
/// Stores `()` rather than the pool on purpose. A `PgPool` is bound to the
/// tokio runtime that created it, and every `#[tokio::test]` builds its own
/// runtime; caching a pool here meant the second test to run inherited a handle
/// whose background reaper had already died with the first test's runtime, and
/// every acquire from it failed with `PoolTimedOut`. Caching only the setup
/// keeps the expensive work once-per-binary while leaving each test to own a
/// pool tied to its own runtime.
static SCHEMA_READY: tokio::sync::OnceCell<()> = tokio::sync::OnceCell::const_new();

/// Connections per test pool.
///
/// Small because each test builds its own pool and the server allows 100 in
/// total; the tests are `#[serial]` and none needs concurrency within a pool.
const TEST_POOL_MAX_CONNECTIONS: u32 = 4;

/// Returns the base connection URL, minus the database name.
///
/// Honours `TEST_DATABASE_URL_BASE` so CI can point at a different host without
/// editing this file; defaults to the devenv-managed local Postgres.
fn database_url_base() -> String {
    std::env::var("TEST_DATABASE_URL_BASE")
        .unwrap_or_else(|_| "postgresql://localhost:5432".to_string())
}

/// Strips the parts of `schema.sql` that cannot be applied to a second database.
///
/// Only pg_cron is removed, and not because it is unavailable in general: the
/// extension is restricted by `cron.database_name` to a single database, so
/// `CREATE EXTENSION` fails anywhere else. That covers the extension itself, the
/// `DO` blocks that schedule jobs, and `cron.schedule_in_timezone`.
///
/// TimescaleDB is deliberately *not* stripped. It was, back when these tests ran
/// against a vanilla Postgres container, which meant every assertion ran against
/// a schema with no hypertables and no retention policies — measurably not the
/// schema being shipped. The devenv Postgres carries the extension, so the real
/// one applies.
fn filter_schema_for_test(schema: &str) -> String {
    let mut inside_do_block = false;
    let mut inside_cron_function = false;
    schema
        .lines()
        .filter(|line| {
            let trimmed = line.trim().to_lowercase();

            if trimmed.starts_with("do $do$") {
                inside_do_block = true;
            }
            if inside_do_block {
                if trimmed.starts_with("$do$;") {
                    inside_do_block = false;
                }
                return false;
            }

            if trimmed.starts_with("create or replace function cron.") {
                inside_cron_function = true;
            }
            if inside_cron_function {
                if trimmed == "$$;" {
                    inside_cron_function = false;
                }
                return false;
            }

            !trimmed.starts_with("create extension if not exists pg_cron")
                && !trimmed.starts_with("select cron.schedule")
        })
        .collect::<Vec<_>>()
        .join("\n")
}

/// Returns a connection pool to the shared test database.
///
/// Creates the database if absent and applies the schema on first use, then
/// hands back clones of the same pool. Uses the devenv-managed Postgres rather
/// than a container, so the suite needs no Docker daemon and runs against a
/// server carrying the same extensions as production.
pub async fn get_pg_pool() -> PgPool {
    let base = database_url_base();

    SCHEMA_READY
        .get_or_init(|| async {
            // Connect to the maintenance database to create the test one. This
            // cannot run through a pool aimed at a database that may not exist.
            let admin = PgPool::connect(&format!("{base}/postgres"))
                .await
                .expect("Failed to connect to Postgres — is `devenv up` running?");

            let exists: Option<i32> =
                sqlx::query_scalar("SELECT 1 FROM pg_database WHERE datname = $1")
                    .bind(TEST_DATABASE)
                    .fetch_optional(&admin)
                    .await
                    .expect("Failed to query for the test database");
            if exists.is_none() {
                // CREATE DATABASE cannot be parameterised or run in a
                // transaction, and TEST_DATABASE is a compile-time constant.
                sqlx::raw_sql(&format!("CREATE DATABASE {TEST_DATABASE}"))
                    .execute(&admin)
                    .await
                    .expect("Failed to create the test database");
            }
            admin.close().await;

            let setup = PgPool::connect(&format!("{base}/{TEST_DATABASE}"))
                .await
                .expect("Failed to connect to the test database");
            sqlx::raw_sql(&filter_schema_for_test(SCHEMA_SQL))
                .execute(&setup)
                .await
                .expect("Failed to apply schema.sql to the test database");
            setup.close().await;
        })
        .await;

    sqlx::postgres::PgPoolOptions::new()
        .max_connections(TEST_POOL_MAX_CONNECTIONS)
        .connect(&format!("{base}/{TEST_DATABASE}"))
        .await
        .expect("Failed to connect to the test database")
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
