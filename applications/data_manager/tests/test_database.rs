mod common;

use chrono::Utc;
use data_manager::data::{EquityBar, Ticker};
use data_manager::database::{
    claim_pending_job, complete_job, fail_job, insert_equity_bars, set_bucket_guc,
};
use serial_test::serial;
use sqlx::PgPool;
use std::sync::OnceLock;
use std::time::Duration;
use testcontainers::{runners::AsyncRunner, ContainerAsync};
use testcontainers_modules::postgres::Postgres;

static PG_URL: OnceLock<String> = OnceLock::new();
static PG_CONTAINER: OnceLock<&'static ContainerAsync<Postgres>> = OnceLock::new();

const SCHEMA_SQL: &str = include_str!("../../../schema.sql");

/// Lines from schema.sql that require pg_cron or TimescaleDB (not available in vanilla Postgres).
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

async fn get_pg_pool() -> PgPool {
    if let Some(url) = PG_URL.get() {
        return PgPool::connect(url).await.unwrap();
    }

    let container = Postgres::default()
        .start()
        .await
        .expect("Failed to start PostgreSQL container — is Docker running?");

    let host = container.get_host().await.unwrap();
    let port = container.get_host_port_ipv4(5432).await.unwrap();
    let url = format!("postgresql://postgres:postgres@{}:{}/postgres", host, port);

    let connect_deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    let pool = loop {
        match PgPool::connect(&url).await {
            Ok(pool) => break pool,
            Err(error) => {
                if tokio::time::Instant::now() >= connect_deadline {
                    panic!("Failed to connect to PostgreSQL within timeout: {error}");
                }
                tokio::time::sleep(Duration::from_millis(250)).await;
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
    let _ = PG_URL.set(url);

    pool
}

fn sample_bars() -> Vec<EquityBar> {
    let now = Utc::now();
    vec![
        EquityBar {
            ticker: Ticker::new("AAPL").unwrap(),
            timestamp: now,
            open_price: 150.0,
            high_price: 155.0,
            low_price: 149.0,
            close_price: 153.0,
            volume: 1_000_000,
            volume_weighted_average_price: Some(152.0),
            transactions: Some(50_000),
            inserted_at: now,
        },
        EquityBar {
            ticker: Ticker::new("MSFT").unwrap(),
            timestamp: now,
            open_price: 350.0,
            high_price: 355.0,
            low_price: 349.0,
            close_price: 353.0,
            volume: 500_000,
            volume_weighted_average_price: Some(352.0),
            transactions: Some(25_000),
            inserted_at: now,
        },
    ]
}

async fn clean_tables(pool: &PgPool) {
    sqlx::raw_sql("DELETE FROM equity_bars; DELETE FROM scheduled_jobs;")
        .execute(pool)
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn test_insert_and_query_equity_bars() {
    let pool = get_pg_pool().await;
    clean_tables(&pool).await;

    let bars = sample_bars();
    let rows = insert_equity_bars(&pool, &bars).await.unwrap();
    assert_eq!(rows, 2);

    let result: Vec<EquityBar> = sqlx::query_as(
        "SELECT ticker, timestamp, open_price, high_price, low_price, close_price, \
         volume, volume_weighted_average_price, transactions, inserted_at \
         FROM equity_bars \
         WHERE timestamp >= now() - interval '1 day'",
    )
    .fetch_all(&pool)
    .await
    .unwrap();

    assert_eq!(result.len(), 2);

    let tickers: Vec<&str> = result.iter().map(|b| b.ticker.as_str()).collect();
    assert!(tickers.contains(&"AAPL"));
    assert!(tickers.contains(&"MSFT"));

    let aapl = result.iter().find(|b| b.ticker == "AAPL").unwrap();
    assert_eq!(aapl.open_price, 150.0);
    assert_eq!(aapl.close_price, 153.0);
    assert_eq!(aapl.volume, 1_000_000);
    assert_eq!(aapl.transactions, Some(50_000));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn test_query_with_ticker_filter() {
    let pool = get_pg_pool().await;
    clean_tables(&pool).await;

    let bars = sample_bars();
    insert_equity_bars(&pool, &bars).await.unwrap();

    let result: Vec<EquityBar> = sqlx::query_as(
        "SELECT ticker, timestamp, open_price, high_price, low_price, close_price, \
         volume, volume_weighted_average_price, transactions, inserted_at \
         FROM equity_bars \
         WHERE ticker = 'AAPL'",
    )
    .fetch_all(&pool)
    .await
    .unwrap();

    assert_eq!(result.len(), 1);
    assert_eq!(result[0].ticker, "AAPL");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn test_query_old_bars_excluded() {
    let pool = get_pg_pool().await;
    clean_tables(&pool).await;

    let old_timestamp = Utc::now() - chrono::Duration::days(15);
    let old_bars = vec![EquityBar {
        ticker: Ticker::new("OLD").unwrap(),
        timestamp: old_timestamp,
        open_price: 100.0,
        high_price: 105.0,
        low_price: 99.0,
        close_price: 102.0,
        volume: 10_000,
        volume_weighted_average_price: Some(101.0),
        transactions: Some(500),
        inserted_at: old_timestamp,
    }];

    insert_equity_bars(&pool, &old_bars).await.unwrap();

    let result: Vec<EquityBar> = sqlx::query_as(
        "SELECT ticker, timestamp, open_price, high_price, low_price, close_price, \
         volume, volume_weighted_average_price, transactions, inserted_at \
         FROM equity_bars \
         WHERE timestamp >= now() - interval '7 days'",
    )
    .fetch_all(&pool)
    .await
    .unwrap();

    assert!(
        result.is_empty(),
        "Bars from 15 days ago should not appear in 7-day query"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn test_upsert_updates_existing_bar() {
    let pool = get_pg_pool().await;
    clean_tables(&pool).await;

    let bars = sample_bars();
    insert_equity_bars(&pool, &bars).await.unwrap();

    let mut updated = bars.clone();
    updated[0].close_price = 160.0;
    insert_equity_bars(&pool, &updated).await.unwrap();

    let result: Vec<EquityBar> = sqlx::query_as(
        "SELECT ticker, timestamp, open_price, high_price, low_price, close_price, \
         volume, volume_weighted_average_price, transactions, inserted_at \
         FROM equity_bars",
    )
    .fetch_all(&pool)
    .await
    .unwrap();

    let aapl = result.iter().find(|b| b.ticker == "AAPL").unwrap();
    assert_eq!(
        aapl.close_price, 160.0,
        "Upsert should have updated close_price"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn test_claim_pending_job() {
    let pool = get_pg_pool().await;
    clean_tables(&pool).await;

    sqlx::raw_sql(
        "INSERT INTO scheduled_jobs (job_name, scheduled_at, status) \
         VALUES ('equity-bar-sync', now(), 'pending')",
    )
    .execute(&pool)
    .await
    .unwrap();

    let job_id = claim_pending_job(&pool, "equity-bar-sync").await.unwrap();
    assert!(job_id.is_some(), "Should claim the pending job");

    let second = claim_pending_job(&pool, "equity-bar-sync").await.unwrap();
    assert!(
        second.is_none(),
        "No more pending jobs after first was claimed"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn test_claim_ignores_future_jobs() {
    let pool = get_pg_pool().await;
    clean_tables(&pool).await;

    sqlx::raw_sql(
        "INSERT INTO scheduled_jobs (job_name, scheduled_at, status) \
         VALUES ('equity-bar-sync', now() + interval '1 hour', 'pending')",
    )
    .execute(&pool)
    .await
    .unwrap();

    let job_id = claim_pending_job(&pool, "equity-bar-sync").await.unwrap();
    assert!(
        job_id.is_none(),
        "Should not claim a job scheduled in the future"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn test_complete_job() {
    let pool = get_pg_pool().await;
    clean_tables(&pool).await;

    sqlx::raw_sql(
        "INSERT INTO scheduled_jobs (job_name, scheduled_at, status) \
         VALUES ('equity-bar-sync', now(), 'pending')",
    )
    .execute(&pool)
    .await
    .unwrap();

    let job_id = claim_pending_job(&pool, "equity-bar-sync")
        .await
        .unwrap()
        .unwrap();

    complete_job(&pool, job_id, "s3_key: test/key.parquet")
        .await
        .unwrap();

    let row: (String,) = sqlx::query_as("SELECT status FROM scheduled_jobs WHERE id = $1")
        .bind(job_id)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(row.0, "completed");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn test_set_bucket_guc_persists() {
    let pool = get_pg_pool().await;

    set_bucket_guc(&pool, "fund-test-data").await.unwrap();

    // ALTER DATABASE persists the GUC in pg_db_role_setting for all future connections.
    // Verify the setting appears there with the expected value.
    let settings: Vec<String> = sqlx::query_scalar(
        "SELECT unnest(setconfig) FROM pg_db_role_setting \
         WHERE setdatabase = (SELECT oid FROM pg_database WHERE datname = current_database()) \
         AND setrole = 0",
    )
    .fetch_all(&pool)
    .await
    .unwrap();

    assert!(
        settings.iter().any(
            |setting| setting.contains("app.bucket_name") && setting.contains("fund-test-data")
        ),
        "Expected app.bucket_name GUC to be persisted, got: {:?}",
        settings
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn test_fail_job() {
    let pool = get_pg_pool().await;
    clean_tables(&pool).await;

    sqlx::raw_sql(
        "INSERT INTO scheduled_jobs (job_name, scheduled_at, status) \
         VALUES ('equity-bar-sync', now(), 'pending')",
    )
    .execute(&pool)
    .await
    .unwrap();

    let job_id = claim_pending_job(&pool, "equity-bar-sync")
        .await
        .unwrap()
        .unwrap();

    fail_job(&pool, job_id, "connection timeout").await.unwrap();

    let row: (String, Option<String>) =
        sqlx::query_as("SELECT status, result FROM scheduled_jobs WHERE id = $1")
            .bind(job_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(row.0, "failed");
    assert_eq!(row.1.as_deref(), Some("connection timeout"));
}
