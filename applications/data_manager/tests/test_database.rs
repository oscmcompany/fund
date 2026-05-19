mod common;

use data_manager::data::EquityBar;
use data_manager::database::{
    claim_pending_job, complete_job, fail_job, insert_equity_bars, query_recent_equity_bars,
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

/// Lines from schema.sql that require pg_cron (not available in vanilla Postgres).
fn filter_schema_for_test(schema: &str) -> String {
    schema
        .lines()
        .filter(|line| {
            let trimmed = line.trim().to_lowercase();
            !trimmed.starts_with("create extension if not exists pg_cron")
                && !trimmed.starts_with("select cron.schedule")
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

    tokio::time::sleep(Duration::from_secs(2)).await;

    let host = container.get_host().await.unwrap();
    let port = container.get_host_port_ipv4(5432).await.unwrap();
    let url = format!("postgresql://postgres:postgres@{}:{}/postgres", host, port);

    let pool = PgPool::connect(&url).await.unwrap();

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
    let now_millis = chrono::Utc::now().timestamp_millis();
    vec![
        EquityBar {
            ticker: "AAPL".to_string(),
            timestamp: now_millis,
            open_price: Some(150.0),
            high_price: Some(155.0),
            low_price: Some(149.0),
            close_price: Some(153.0),
            volume: Some(1_000_000),
            volume_weighted_average_price: Some(152.0),
            transactions: Some(50_000),
        },
        EquityBar {
            ticker: "MSFT".to_string(),
            timestamp: now_millis,
            open_price: Some(350.0),
            high_price: Some(355.0),
            low_price: Some(349.0),
            close_price: Some(353.0),
            volume: Some(500_000),
            volume_weighted_average_price: Some(352.0),
            transactions: Some(25_000),
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

    let result = query_recent_equity_bars(&pool, None, 1).await.unwrap();
    assert_eq!(result.len(), 2);

    let tickers: Vec<&str> = result.iter().map(|b| b.ticker.as_str()).collect();
    assert!(tickers.contains(&"AAPL"));
    assert!(tickers.contains(&"MSFT"));

    let aapl = result.iter().find(|b| b.ticker == "AAPL").unwrap();
    assert_eq!(aapl.open_price, Some(150.0));
    assert_eq!(aapl.close_price, Some(153.0));
    assert_eq!(aapl.volume, Some(1_000_000));
    assert_eq!(aapl.transactions, Some(50_000));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn test_query_with_ticker_filter() {
    let pool = get_pg_pool().await;
    clean_tables(&pool).await;

    let bars = sample_bars();
    insert_equity_bars(&pool, &bars).await.unwrap();

    let tickers = vec!["AAPL".to_string()];
    let result = query_recent_equity_bars(&pool, Some(&tickers), 1)
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

    let old_timestamp = chrono::Utc::now().timestamp_millis() - (15 * 24 * 60 * 60 * 1000);
    let old_bars = vec![EquityBar {
        ticker: "OLD".to_string(),
        timestamp: old_timestamp,
        open_price: Some(100.0),
        high_price: Some(105.0),
        low_price: Some(99.0),
        close_price: Some(102.0),
        volume: Some(10_000),
        volume_weighted_average_price: Some(101.0),
        transactions: Some(500),
    }];

    insert_equity_bars(&pool, &old_bars).await.unwrap();

    let result = query_recent_equity_bars(&pool, None, 7).await.unwrap();
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
    updated[0].close_price = Some(160.0);
    insert_equity_bars(&pool, &updated).await.unwrap();

    let result = query_recent_equity_bars(&pool, None, 1).await.unwrap();
    let aapl = result.iter().find(|b| b.ticker == "AAPL").unwrap();
    assert_eq!(
        aapl.close_price,
        Some(160.0),
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
