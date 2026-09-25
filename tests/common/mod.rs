//! Shared fixtures for the integration suite, against the devenv-managed PostgreSQL with the real
//! `schema.sql` applied to a database the suite owns outright — which is what makes the
//! hypertables, the CHECK constraints, and the notify trigger reachable at all.

#![allow(dead_code)]

use chrono::{DateTime, TimeZone, Utc};
use chrono_tz::America::New_York;
use sqlx::PgPool;

const SCHEMA_SQL: &str = include_str!("../../schema.sql");

/// Prefix for the databases the integration suite owns outright.
///
/// Deliberately not the development database: these tests delete from every table, so pointing them
/// at `fund` would destroy local data. One database *per test binary*, because `#[serial]` only
/// serializes within a process while cargo runs test binaries concurrently, and a shared database
/// would have two of them deleting from the same tables at once.
const TEST_DATABASE_PREFIX: &str = "fund_test";

/// Connections per test pool.
///
/// Small because each test builds its own pool and the server allows a hundred in total.
const TEST_POOL_MAX_CONNECTIONS: u32 = 4;

/// Databases this process has already created and populated.
///
/// Holds names rather than pools: a `PgPool` is bound to the tokio runtime that created it and every
/// `#[tokio::test]` builds its own, so a cached pool leaves the next test acquiring against a dead
/// reaper and failing with `PoolTimedOut`. A set rather than a single `OnceCell`, which initializes
/// once per *process* and would let a second suffix connect to a database that was never created.
static PREPARED_DATABASES: tokio::sync::Mutex<Option<std::collections::HashSet<String>>> =
    tokio::sync::Mutex::const_new(None);

fn database_url_base() -> String {
    std::env::var("TEST_DATABASE_URL_BASE")
        .unwrap_or_else(|_| "postgresql://localhost:5432".to_string())
}

/// Strips the parts of `schema.sql` that cannot be applied to a second database.
///
/// Only pg_cron, because `cron.database_name` restricts the extension to a single database — that
/// covers the extension, the `DO` blocks that schedule jobs, and anything in the `cron` schema.
/// TimescaleDB is deliberately *not* stripped, since a schema with no hypertables and no retention
/// policies is not the schema being shipped. `DO` blocks are buffered rather than dropped on sight,
/// because the idiom is shared with plain DDL such as the `events_notify` trigger, and dropping
/// every block leaves NOTIFY silent here and the listener untestable.
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
/// `suffix` must be unique per test binary and a plain identifier, since it is interpolated into
/// `CREATE DATABASE`, which PostgreSQL will not accept as a bound parameter. The database is
/// **dropped and recreated**, not reused: `CREATE TABLE IF NOT EXISTS` is a no-op against a table
/// that already exists with a different shape, so a stale one keeps its old columns and fails much
/// later with a confusing error about a missing column in an index.
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
         DELETE FROM equity_pairs; \
         DELETE FROM equity_bars; \
         DELETE FROM account_activities; \
         DELETE FROM account_snapshots;",
    )
    .execute(pool)
    .await
    .expect("Failed to reset the test tables");
}

/// The instant a daily bar for `date` actually carries: 16:00 Eastern, the regular-session close.
///
/// Not `SessionDate::midnight`, which sits on the edge of `bounds()` rather than inside it and is a
/// shape ingestion never produces. Resolved through the zone rather than by adding sixteen hours,
/// because the two differ on the days the clocks move and `seed_correlated_bars` walks onto them.
pub fn session_close(date: SessionDate) -> DateTime<Utc> {
    let local_close = date
        .date()
        .and_hms_opt(16, 0, 0)
        .expect("16:00 is a valid wall-clock time");
    New_York
        .from_local_datetime(&local_close)
        .earliest()
        .map(|zoned| zoned.with_timezone(&Utc))
        .expect("16:00 Eastern exists on every date")
}

/// Inserts daily bars for each ticker over `sessions` consecutive **calendar** days ending today.
///
/// Calendar days, not trading sessions: the loop applies no weekday filter, so the series includes
/// weekends. The two legs are a shared factor plus an idiosyncratic one, so their log returns
/// correlate around 0.8, inside the screen's `[0.5, 0.95]` band — legs correlating at 1.0 are
/// rejected by the screen and yield zero pairs, which makes every test built on them pass while
/// asserting nothing.
pub async fn seed_correlated_bars(pool: &PgPool, tickers: &[&str], sessions: i64) {
    let today = SessionDate::at(Utc::now());

    for (index, ticker) in tickers.iter().enumerate() {
        let mut price = 100.0 + index as f64 * 20.0;
        let mut session_date = today.plus_calendar_days(-(sessions - 1));

        for step in 0..sessions {
            let common = 0.012 * (step as f64 * 0.7).sin();
            let idiosyncratic = 0.012 * (step as f64 * 1.9 + index as f64).sin();
            price *= (0.8 * common + 0.6 * idiosyncratic).exp();

            let timestamp = session_close(session_date);

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
    seed_bar_with_volume(pool, ticker, date, close, 5_000_000).await;
}

/// [`seed_bar`] with the share count spelled out, for the liquidity aggregates that read it.
pub async fn seed_bar_with_volume(
    pool: &PgPool,
    ticker: &str,
    date: SessionDate,
    close: f64,
    volume: i64,
) {
    let timestamp = session_close(date);
    sqlx::query(
        "INSERT INTO equity_bars \
         (ticker, bar_interval, timestamp, open_price, high_price, low_price, close_price, volume) \
         VALUES ($1, 'one_day', $2, $3, $3, $3, $3, $4) \
         ON CONFLICT (ticker, bar_interval, timestamp) \
         DO UPDATE SET open_price = EXCLUDED.open_price, high_price = EXCLUDED.high_price, \
                       low_price = EXCLUDED.low_price, close_price = EXCLUDED.close_price, \
                       volume = EXCLUDED.volume",
    )
    .bind(ticker)
    .bind(timestamp)
    .bind(close)
    .bind(volume)
    .execute(pool)
    .await
    .expect("Failed to seed an equity bar");
}

use fund::common::types::SessionDate;
