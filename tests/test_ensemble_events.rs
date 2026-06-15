//! Integration tests for the ensemble service's event-system SQL: consumer
//! offsets, event catch-up lookup, model_runs lineage upsert, and prediction
//! inserts — run against a real Postgres via testcontainers.

mod common;

use fund::common::events::{
    emit_event, get_consumer_offset, latest_event_after, update_consumer_offset, EventType,
};
use fund::ensemble_manager::database::{insert_predictions, upsert_model_run, ModelRunRecord};
use serial_test::serial;
use sqlx::PgPool;
use std::sync::OnceLock;
use std::time::Duration;
use testcontainers::{runners::AsyncRunner, ContainerAsync};
use testcontainers_modules::postgres::Postgres;
use uuid::Uuid;

static PG_URL: OnceLock<String> = OnceLock::new();
static PG_CONTAINER: OnceLock<&'static ContainerAsync<Postgres>> = OnceLock::new();

const SCHEMA_SQL: &str = include_str!("../schema.sql");

/// Strip lines that need pg_cron or TimescaleDB (unavailable in vanilla Postgres).
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

#[tokio::test]
#[serial]
async fn test_consumer_offset_round_trip() {
    let pool = get_pg_pool().await;
    let consumer = "ensemble-offset-test";

    assert_eq!(get_consumer_offset(&pool, consumer).await.unwrap(), 0);

    update_consumer_offset(&pool, consumer, 5).await.unwrap();
    assert_eq!(get_consumer_offset(&pool, consumer).await.unwrap(), 5);

    // GREATEST guards against moving the offset backwards.
    update_consumer_offset(&pool, consumer, 3).await.unwrap();
    assert_eq!(get_consumer_offset(&pool, consumer).await.unwrap(), 5);

    update_consumer_offset(&pool, consumer, 9).await.unwrap();
    assert_eq!(get_consumer_offset(&pool, consumer).await.unwrap(), 9);
}

#[tokio::test]
#[serial]
async fn test_latest_event_after_matches_only_requested_type() {
    let pool = get_pg_pool().await;

    let before = latest_event_after(&pool, EventType::EquityPredictionsRequested, 0)
        .await
        .unwrap()
        .unwrap_or(0);

    emit_event(
        &pool,
        EventType::EquityPredictionsRequested,
        &serde_json::json!({}),
    )
    .await
    .unwrap();

    let found = latest_event_after(&pool, EventType::EquityPredictionsRequested, before)
        .await
        .unwrap();
    assert!(found.is_some());
    let requested_id = found.unwrap();
    assert!(requested_id > before);

    // A later event of a different type must not be returned.
    emit_event(&pool, EventType::MarketSessionCheck, &serde_json::json!({}))
        .await
        .unwrap();
    assert_eq!(
        latest_event_after(&pool, EventType::EquityPredictionsRequested, requested_id)
            .await
            .unwrap(),
        None
    );
}

#[tokio::test]
#[serial]
async fn test_upsert_model_run_inserts_then_updates() {
    let pool = get_pg_pool().await;

    let metadata = serde_json::json!({
        "lookback_days": 1200,
        "start_date": "2023-02-25",
        "end_date": "2026-06-09",
        "train_samples": 100,
        "validation_samples": 20,
        "metrics": {"crps": 0.0059, "directional_accuracy": 0.617, "quantile_coverage": 0.719},
        "drift": {"status": "no_drift", "message": "No drift detected", "baseline_crps": 0.0056, "prior_runs": 7}
    });
    let record = ModelRunRecord::from_metadata(
        "run-events-test",
        "models/tide/run-events-test/output/model.tar.gz",
        &metadata,
    );
    upsert_model_run(&pool, &record).await.unwrap();

    let (crps, lookback, status, drift_status): (Option<f64>, Option<i32>, String, Option<String>) =
        sqlx::query_as(
            "SELECT continuous_ranked_probability_score, lookback_days, status, drift_status \
             FROM model_runs WHERE run_id = $1",
        )
        .bind("run-events-test")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(status, "completed");
    assert_eq!(lookback, Some(1200));
    assert!((crps.unwrap() - 0.0059).abs() < 1e-9);
    assert_eq!(drift_status.as_deref(), Some("no_drift"));

    // Upsert again with the same run_id updates in place (no duplicate row).
    let updated = ModelRunRecord::from_metadata(
        "run-events-test",
        "models/tide/run-events-test/output/model.tar.gz",
        &serde_json::json!({"metrics": {"crps": 0.01}}),
    );
    upsert_model_run(&pool, &updated).await.unwrap();

    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM model_runs WHERE run_id = $1")
        .bind("run-events-test")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(count, 1);
}

#[tokio::test]
#[serial]
async fn test_insert_predictions_writes_rows() {
    let pool = get_pg_pool().await;

    let predictions = vec![serde_json::json!({
        "ticker": "PREDTEST",
        "timestamp": 1_735_689_600_000_i64,
        "quantile_10": -0.01,
        "quantile_50": 0.0,
        "quantile_90": 0.02,
    })];

    let rows = insert_predictions(&pool, &predictions, Uuid::new_v4(), "run-events-test")
        .await
        .unwrap();
    assert_eq!(rows, 1);

    let count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM equity_predictions WHERE ticker = 'PREDTEST'")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(count, 1);
}
