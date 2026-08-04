//! Shared fixtures for the integration suite.
//!
//! Everything here runs against the devenv-managed PostgreSQL, applying the real `schema.sql` to a
//! database the suite owns outright. That matters more than it sounds: these tests exist precisely
//! to exercise what the unit tests cannot — the hypertables, the CHECK constraints, the notify
//! trigger, and the queries as PostgreSQL actually plans them.

#![allow(dead_code)]

use chrono::{DateTime, Utc};
use fund::data::calendar::SessionDate;
use sqlx::PgPool;
use uuid::Uuid;

const SCHEMA_SQL: &str = include_str!("../../schema.sql");

/// Prefix for the databases the integration suite owns outright.
///
/// Deliberately not the development database: these tests delete from every table, and pointing
/// them at `fund` would destroy local data on every run.
///
/// One database *per test binary*, not one shared between them. `#[serial]` only serializes tests
/// within a process, and cargo runs test binaries concurrently — so a shared database would have
/// two binaries deleting from the same tables at the same time, with the flakes landing wherever
/// the timing fell.
const TEST_DATABASE_PREFIX: &str = "fund_test";

/// Connections per test pool.
///
/// Small because each test builds its own pool and the server allows a hundred in total.
const TEST_POOL_MAX_CONNECTIONS: u32 = 4;

/// Databases this process has already created and populated.
///
/// Holds names rather than pools, and that is load-bearing. A `PgPool` is bound to the tokio runtime
/// that created it, and every `#[tokio::test]` builds its own runtime; caching a pool here meant the
/// second test to run inherited a handle whose background reaper had died with the first test's
/// runtime, and every acquire from it failed with `PoolTimedOut`. This is the trap recorded in
/// `rust_test_pitfalls`.
///
/// A set rather than a single `OnceCell`, so the readiness is keyed by database. A `OnceCell` is
/// initialized once per *process*: a binary calling `test_pool` with two suffixes would create the
/// first database, then skip the initializer for the second and connect to a database that was
/// never created — surfacing as a bare connection error rather than as fixture misuse.
static PREPARED_DATABASES: tokio::sync::Mutex<Option<std::collections::HashSet<String>>> =
    tokio::sync::Mutex::const_new(None);

fn database_url_base() -> String {
    std::env::var("TEST_DATABASE_URL_BASE")
        .unwrap_or_else(|_| "postgresql://localhost:5432".to_string())
}

/// Strips the parts of `schema.sql` that cannot be applied to a second database.
///
/// Only pg_cron is removed, and not because it is unavailable in general: the extension is
/// restricted by `cron.database_name` to a single database, so `CREATE EXTENSION` fails anywhere
/// else. That covers the extension, the `DO` blocks that schedule jobs, and any function defined in
/// the `cron` schema.
///
/// TimescaleDB is deliberately *not* stripped. It was, back when these tests ran against a vanilla
/// Postgres container, which meant every assertion ran against a schema with no hypertables and no
/// retention policies — measurably not the schema being shipped.
///
/// `DO` blocks are buffered rather than dropped on sight: the idiom is shared between pg_cron
/// scheduling, which this database cannot run, and plain DDL such as the `events_notify` trigger,
/// which it very much needs. Dropping every block took the trigger with it, leaving NOTIFY silent
/// here and the listener untestable.
fn filter_schema_for_test(schema: &str) -> String {
    let mut kept: Vec<&str> = Vec::new();
    let mut do_block: Vec<&str> = Vec::new();
    let mut inside_do_block = false;
    let mut inside_cron_function = false;

    for line in schema.lines() {
        let trimmed = line.trim().to_lowercase();

        if inside_do_block {
            do_block.push(line);
            if trimmed.starts_with("$do$;") {
                inside_do_block = false;
                let mentions_cron = do_block
                    .iter()
                    .any(|blocked| blocked.to_lowercase().contains("cron."));
                if !mentions_cron {
                    kept.append(&mut do_block);
                }
                do_block.clear();
            }
            continue;
        }
        if trimmed.starts_with("do $do$") {
            inside_do_block = true;
            do_block.push(line);
            continue;
        }

        if trimmed.starts_with("create or replace function cron.") {
            inside_cron_function = true;
        }
        if inside_cron_function {
            if trimmed == "$$;" {
                inside_cron_function = false;
            }
            continue;
        }

        if trimmed.starts_with("create extension if not exists pg_cron")
            || trimmed.starts_with("select cron.schedule")
        {
            continue;
        }
        kept.push(line);
    }

    // A block that never terminated means every line after it was buffered and dropped, and the
    // suite would then apply a truncated schema — surfacing much later as a confusing missing-column
    // error. Fail here, where the cause is legible.
    assert!(
        !inside_do_block,
        "schema.sql has an unterminated DO block; the test filter expects `$do$;` on its own line"
    );
    assert!(
        !inside_cron_function,
        "schema.sql has an unterminated cron function; the test filter expects `$$;` on its own line"
    );

    kept.join("\n")
}

/// Returns a pool to this binary's test database, recreating it on first use.
///
/// `suffix` must be unique per test binary and must be a plain identifier — it is interpolated into
/// `CREATE DATABASE`, which PostgreSQL will not accept as a bound parameter.
///
/// The database is **dropped and recreated**, not reused. `CREATE TABLE IF NOT EXISTS` is a no-op
/// against a table that already exists with a different shape, so a test database left over from an
/// earlier schema silently keeps its old columns and fails much later with a confusing error about
/// a missing column in an index. That is the same property the production cutover has to respect;
/// here it costs nothing to simply start clean.
pub async fn test_pool(suffix: &str) -> PgPool {
    assert!(
        suffix
            .chars()
            .all(|character| character.is_ascii_lowercase() || character == '_'),
        "the database suffix is interpolated into DDL and must be a plain identifier"
    );
    let base = database_url_base();
    let database = format!("{TEST_DATABASE_PREFIX}_{suffix}");

    // The lock is held across the setup so two concurrent callers for the same database cannot both
    // decide it needs creating. It is released before the pool below is built.
    {
        let mut guard = PREPARED_DATABASES.lock().await;
        let prepared = guard.get_or_insert_with(std::collections::HashSet::new);

        if prepared.insert(database.clone()) {
            let admin = PgPool::connect(&format!("{base}/postgres"))
                .await
                .expect("Failed to connect to Postgres — is `devenv up` running?");

            // `WITH (FORCE)` because a pool from an earlier run in the same session can still hold
            // a connection, and `DROP DATABASE` refuses while anything is attached.
            sqlx::raw_sql(&format!("DROP DATABASE IF EXISTS {database} WITH (FORCE)"))
                .execute(&admin)
                .await
                .expect("Failed to drop the previous test database");
            sqlx::raw_sql(&format!("CREATE DATABASE {database}"))
                .execute(&admin)
                .await
                .expect("Failed to create the test database");
            admin.close().await;

            let setup = PgPool::connect(&format!("{base}/{database}"))
                .await
                .expect("Failed to connect to the test database");
            sqlx::raw_sql(&filter_schema_for_test(SCHEMA_SQL))
                .execute(&setup)
                .await
                .expect("Failed to apply schema.sql to the test database");
            setup.close().await;
        }
    }

    sqlx::postgres::PgPoolOptions::new()
        .max_connections(TEST_POOL_MAX_CONNECTIONS)
        .connect(&format!("{base}/{database}"))
        .await
        .expect("Failed to connect to the test database")
}

/// Empties every table the suite writes to.
///
/// `DELETE` rather than `TRUNCATE` on the hypertables: TimescaleDB permits both, but `TRUNCATE` on
/// a hypertable takes a lock that a concurrently-running test in the same binary will wait on.
pub async fn reset_tables(pool: &PgPool) {
    sqlx::raw_sql(
        "DELETE FROM events; \
         DELETE FROM equity_predictions; \
         DELETE FROM equity_pairs; \
         DELETE FROM equity_bars; \
         DELETE FROM equity_details; \
         DELETE FROM account_activities; \
         DELETE FROM account_snapshots;",
    )
    .execute(pool)
    .await
    .expect("Failed to reset the test tables");
}

/// Inserts daily bars for each ticker over `sessions` consecutive **calendar** days ending today.
///
/// Calendar days, not trading sessions: the loop applies no weekday filter, so the series includes
/// weekends. The synthetic prices still satisfy the correlation property the screen needs, so this
/// only matters to a reader sizing a window that must align with real sessions.
///
/// The two legs are driven by a shared factor plus an idiosyncratic one, so their log returns
/// correlate around 0.8 — inside the screen's `[0.5, 0.95]` band — and the spread has real
/// dispersion. A fixture whose legs correlate at 1.0 is rejected by the screen and yields zero
/// pairs, which makes every test built on it pass while asserting nothing. That is the trap
/// recorded in `statistical_arbitrage_test_fixtures`.
pub async fn seed_correlated_bars(pool: &PgPool, tickers: &[&str], sessions: i64) {
    let today = SessionDate::at(Utc::now());

    for (index, ticker) in tickers.iter().enumerate() {
        let mut price = 100.0 + index as f64 * 20.0;
        let mut session_date = today.plus_calendar_days(-(sessions - 1));

        for step in 0..sessions {
            let common = 0.012 * (step as f64 * 0.7).sin();
            let idiosyncratic = 0.012 * (step as f64 * 1.9 + index as f64).sin();
            price *= (0.8 * common + 0.6 * idiosyncratic).exp();

            let timestamp = session_date.midnight();

            sqlx::query(
                "INSERT INTO equity_bars \
                 (ticker, bar_interval, timestamp, open_price, high_price, low_price, \
                  close_price, volume) \
                 VALUES ($1, 'one_day', $2, $3, $4, $5, $6, $7) \
                 ON CONFLICT (ticker, bar_interval, timestamp) DO UPDATE SET \
                     close_price = EXCLUDED.close_price",
            )
            .bind(*ticker)
            .bind(timestamp)
            .bind(price * 0.998)
            .bind(price * 1.005)
            .bind(price * 0.995)
            .bind(price)
            .bind(5_000_000_i64)
            .execute(pool)
            .await
            .expect("Failed to seed an equity bar");

            session_date = session_date.plus_calendar_days(1);
        }
    }
}

/// Inserts one daily bar for a ticker at a specific date, for gap and alignment tests.
pub async fn seed_bar(pool: &PgPool, ticker: &str, date: SessionDate, close: f64) {
    let timestamp = date.midnight();
    sqlx::query(
        "INSERT INTO equity_bars \
         (ticker, bar_interval, timestamp, open_price, high_price, low_price, close_price, volume) \
         VALUES ($1, 'one_day', $2, $3, $3, $3, $3, 5000000) \
         ON CONFLICT (ticker, bar_interval, timestamp) DO UPDATE SET close_price = EXCLUDED.close_price",
    )
    .bind(ticker)
    .bind(timestamp)
    .bind(close)
    .execute(pool)
    .await
    .expect("Failed to seed an equity bar");
}

/// Inserts ticker metadata.
pub async fn seed_details(pool: &PgPool, ticker_sectors: &[(&str, &str)]) {
    for (ticker, sector) in ticker_sectors {
        sqlx::query(
            "INSERT INTO equity_details (ticker, sector, industry) VALUES ($1, $2, $2) \
             ON CONFLICT (ticker) DO UPDATE SET sector = EXCLUDED.sector",
        )
        .bind(*ticker)
        .bind(*sector)
        .execute(pool)
        .await
        .expect("Failed to seed an equity detail");
    }
}

/// Inserts one prediction per ticker at `timestamp`, all sharing one `correlation_id`.
///
/// One identifier for the whole batch, not one per row. `insert_predictions` takes a single
/// `correlation_id` and applies it to every prediction in the call, which is what makes the column
/// identify a batch at all — a fixture giving each ticker its own would produce a table state the
/// writer cannot produce, and any query grouping by batch would see one row per group.
pub async fn seed_predictions(
    pool: &PgPool,
    model_run_id: &str,
    tickers_and_medians: &[(&str, f64)],
    timestamp: DateTime<Utc>,
) {
    let correlation_id = Uuid::new_v4();
    for (ticker, median) in tickers_and_medians {
        sqlx::query(
            "INSERT INTO equity_predictions \
             (correlation_id, model_run_id, ticker, timestamp, quantile_10, quantile_50, quantile_90) \
             VALUES ($1, $2, $3, $4, $5, $6, $7) \
             ON CONFLICT (ticker, timestamp) DO UPDATE SET \
                 quantile_10 = EXCLUDED.quantile_10, \
                 quantile_50 = EXCLUDED.quantile_50, \
                 quantile_90 = EXCLUDED.quantile_90",
        )
        .bind(correlation_id)
        .bind(model_run_id)
        .bind(*ticker)
        .bind(timestamp)
        // A narrow interval, so `confidence` clears the screen's floor comfortably.
        .bind(median - 0.02)
        .bind(*median)
        .bind(median + 0.02)
        .execute(pool)
        .await
        .expect("Failed to seed a prediction");
    }
}
