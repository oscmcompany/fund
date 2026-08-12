//! The dashboard's queries, run against the real schema.
//!
//! This target exists because the dashboard is the one part of the tree with no compile-time check
//! on its SQL. Everything else uses `sqlx::query!`, which fails the build on a mistyped column;
//! the dashboard uses raw `sqlx::query` so it adds no entries to the offline cache, and the price
//! of that is that a wrong column name is a runtime error on a page nobody is watching when it
//! breaks. Running every query once against a real database is what buys the guarantee back.
//!
//! The rows are written through the production writers wherever one exists — `record_open`,
//! `record_close`, `store_snapshot` — rather than by hand. A dashboard test that seeds its own rows
//! with its own INSERT proves the query parses, not that it reads what the service writes.

mod common;

use chrono::{Duration, Utc};
use fund::common::alpaca::AccountSnapshot;
use fund::common::events::{self, Command, EventType, Outcome};
use fund::common::types::{PairID, SessionDate, Ticker};
use fund::dashboard::database::fetch_dashboard_data;

use fund::portfolio::account;
use fund::portfolio::pairs::{self, CloseReason, PairEntry};
use rust_decimal::Decimal;
use serde_json::json;
use serial_test::serial;
use sqlx::PgPool;
use std::str::FromStr;

fn ticker(raw: &str) -> Ticker {
    Ticker::new(raw).expect("test ticker must be valid")
}

fn decimal(value: &str) -> Decimal {
    Decimal::from_str(value).expect("test decimal must be valid")
}

fn entry(long: &str, short: &str) -> PairEntry {
    PairEntry::new(
        PairID::new(ticker(long), ticker(short)),
        1.05,
        2.4,
        0.03,
        Some("run-dashboard".to_string()),
    )
    .expect("test entry must be valid")
}

async fn fresh_pool() -> PgPool {
    let pool = common::test_pool("dashboard").await;
    common::reset_tables(&pool).await;
    pool
}

async fn store_equity(pool: &PgPool, session_date: SessionDate, equity: &str) {
    let snapshot = AccountSnapshot::new(
        decimal(equity),
        decimal("50000"),
        decimal("200000"),
        decimal("40000"),
        decimal("-40000"),
    );
    account::store_snapshot(pool, session_date, &snapshot)
        .await
        .expect("Failed to store an account snapshot");
}

/// The other kind of row: a session rebuilt by `backfill_account_snapshots`.
async fn store_reconstructed_equity(pool: &PgPool, session_date: SessionDate, equity: &str) {
    account::store_equity_snapshot(pool, session_date, decimal(equity))
        .await
        .expect("Failed to store a reconstructed account snapshot");
}

/// Every query, against an empty database.
///
/// The case a raw query is most likely to be wrong in and least likely to be exercised in: a fresh
/// deployment, where an aggregate over no rows must still return a row rather than nothing.
#[tokio::test]
#[serial]
async fn test_every_query_runs_against_an_empty_database() {
    let pool = fresh_pool().await;

    let data = fetch_dashboard_data(&pool)
        .await
        .expect("every dashboard query must run against an empty database");

    assert!(data.account_snapshot_history.is_empty());
    assert!(data.open_pairs.is_empty());
    assert!(data.closed_pairs.is_empty());
    assert!(data.predictions.is_empty());
    assert!(data.recent_events.is_empty());
    assert_eq!(data.prediction_model_run_id, None);
    // `MAX(...)` over no rows is a single NULL row, not an empty result. A query written to expect
    // no rows would fail here rather than on the empty page it was tested on.
    assert_eq!(data.latest_bars_inserted_at, None);
    assert_eq!(data.closed_summary.total_closed, 0);
}

#[tokio::test]
#[serial]
async fn test_the_dashboard_reads_what_the_service_writes() {
    let pool = fresh_pool().await;
    let now = Utc::now();

    // An open pair and a closed one, both through the production writers.
    pairs::record_open(&pool, &entry("AAPL", "MSFT"), now - Duration::hours(3))
        .await
        .expect("Failed to record an open pair");

    let closed_id = pairs::record_open(&pool, &entry("GOOG", "AMZN"), now - Duration::hours(9))
        .await
        .expect("Failed to record the pair that will close");
    assert!(pairs::record_close(
        &pool,
        closed_id,
        CloseReason::Convergence,
        now - Duration::hours(4)
    )
    .await
    .expect("Failed to close the pair"));
    assert!(
        pairs::record_realized_profit_and_loss(&pool, closed_id, decimal("412.75"))
            .await
            .expect("Failed to attribute realized profit and loss")
    );

    store_equity(&pool, SessionDate::at(now - Duration::days(1)), "1000000").await;
    store_equity(&pool, SessionDate::at(now), "1010000").await;

    common::seed_bar(&pool, "AAPL", SessionDate::at(now), 190.0).await;
    common::seed_predictions(
        &pool,
        "run-dashboard",
        &[("AAPL", 0.01), ("MSFT", -0.004)],
        now,
    )
    .await;

    events::emit(
        &pool,
        EventType::new(Command::PortfolioEvaluation, Outcome::Completed),
        json!({"pairs_opened": 1}),
    )
    .await
    .expect("Failed to emit an event");

    let data = fetch_dashboard_data(&pool)
        .await
        .expect("every dashboard query must run against a populated database");

    assert_eq!(data.open_pairs.len(), 1);
    let open = &data.open_pairs[0];
    assert_eq!(open.pair_id.as_str(), "AAPL-MSFT");
    assert_eq!(open.long_ticker.as_str(), "AAPL");
    assert_eq!(open.short_ticker.as_str(), "MSFT");
    assert_eq!(open.entry_z_score, 2.4);
    assert_eq!(open.hedge_ratio, 1.05);

    assert_eq!(data.closed_pairs.len(), 1);
    let closed = &data.closed_pairs[0];
    assert_eq!(closed.pair_id.as_str(), "GOOG-AMZN");
    assert_eq!(closed.close_reason, CloseReason::Convergence);
    assert_eq!(closed.realized_profit_and_loss, Some(decimal("412.75")));
    assert!((closed.holding_hours() - 5.0).abs() < 0.01);

    assert_eq!(data.closed_summary.total_closed, 1);
    assert_eq!(data.closed_summary.wins, 1);
    assert_eq!(data.closed_summary.signal_exit_share, Some(100.0));

    assert_eq!(data.account_snapshot_history.len(), 2);
    assert_eq!(data.account_snapshot_history[0].equity, decimal("1000000"));
    assert_eq!(data.account_snapshot_history[1].equity, decimal("1010000"));
    assert_eq!(data.period_returns.one_day, Some(1.0));

    assert_eq!(data.predictions.len(), 2);
    // Ranked by median forecast, so the positive one leads.
    assert_eq!(data.predictions[0].ticker.as_str(), "AAPL");
    assert_eq!(
        data.prediction_model_run_id.as_deref(),
        Some("run-dashboard")
    );

    assert!(data.latest_bars_inserted_at.is_some());
    assert_eq!(data.recent_events.len(), 1);
    assert_eq!(
        data.recent_events[0].event_type,
        "portfolio_evaluation_completed"
    );
    assert_eq!(data.recent_events[0].payload["pairs_opened"], json!(1));
}

/// The prediction query selects one batch by its shared `correlation_id`, so a second run must
/// replace the first on the page rather than interleave with it.
#[tokio::test]
#[serial]
async fn test_only_the_most_recent_prediction_batch_is_shown() {
    let pool = fresh_pool().await;
    let now = Utc::now();

    common::seed_predictions(
        &pool,
        "run-yesterday",
        &[("AAPL", 0.02), ("MSFT", 0.01), ("GOOG", 0.005)],
        now - Duration::days(1),
    )
    .await;
    common::seed_predictions(&pool, "run-today", &[("AAPL", 0.03), ("MSFT", 0.02)], now).await;

    let data = fetch_dashboard_data(&pool)
        .await
        .expect("the prediction query must run");

    assert_eq!(data.predictions.len(), 2, "expected only today's batch");
    assert_eq!(data.prediction_model_run_id.as_deref(), Some("run-today"));
}

/// A closed pair with no attributed profit and loss is still a close. The query filters on
/// `closed_at` and `close_reason`, not on the money, and the summary must count it.
#[tokio::test]
#[serial]
async fn test_a_closed_pair_without_attribution_is_still_read() {
    let pool = fresh_pool().await;
    let now = Utc::now();

    let id = pairs::record_open(&pool, &entry("AAPL", "MSFT"), now - Duration::hours(2))
        .await
        .expect("Failed to record the pair");
    pairs::record_close(&pool, id, CloseReason::EndOfDay, now - Duration::hours(1))
        .await
        .expect("Failed to close the pair");

    let data = fetch_dashboard_data(&pool)
        .await
        .expect("the closed-pair query must run");

    assert_eq!(data.closed_pairs.len(), 1);
    assert_eq!(data.closed_pairs[0].realized_profit_and_loss, None);
    assert_eq!(data.closed_summary.total_closed, 1);
    assert_eq!(data.closed_summary.wins, 0);
    assert_eq!(data.closed_summary.losses, 0);
    // No decided trade, so no win rate — as distinct from a win rate of zero.
    assert_eq!(data.closed_summary.win_rate, None);
    // The exit was the pre-close fail-safe, not the strategy's opinion.
    assert_eq!(data.closed_summary.signal_exit_share, Some(0.0));
}

/// An open pair is not a closed one and vice versa. Both queries filter on `status`, and getting
/// either predicate wrong would show a flat book as fully invested or the reverse.
#[tokio::test]
#[serial]
async fn test_open_and_closed_pairs_do_not_appear_in_each_others_sections() {
    let pool = fresh_pool().await;
    let now = Utc::now();

    pairs::record_open(&pool, &entry("AAPL", "MSFT"), now - Duration::hours(2))
        .await
        .expect("Failed to record the open pair");
    let closed_id = pairs::record_open(&pool, &entry("GOOG", "AMZN"), now - Duration::hours(5))
        .await
        .expect("Failed to record the pair that will close");
    pairs::record_close(
        &pool,
        closed_id,
        CloseReason::StopLoss,
        now - Duration::hours(1),
    )
    .await
    .expect("Failed to close the pair");

    let data = fetch_dashboard_data(&pool)
        .await
        .expect("both pair queries must run");

    let open_identifiers: Vec<&str> = data
        .open_pairs
        .iter()
        .map(|pair| pair.pair_id.as_str())
        .collect();
    let closed_identifiers: Vec<&str> = data
        .closed_pairs
        .iter()
        .map(|pair| pair.pair_id.as_str())
        .collect();

    assert_eq!(open_identifiers, vec!["AAPL-MSFT"]);
    assert_eq!(closed_identifiers, vec!["GOOG-AMZN"]);
}

/// The whole flow guard, end to end against PostgreSQL.
///
/// The unit tests hand `compute_period_returns` a list of sessions directly, which proves the
/// arithmetic and nothing about how that list is obtained. This exercises the parts only a real
/// database can: the `activity_type = ANY($1)` array bind, the range predicate, and the Eastern
/// mapping from a stored `transaction_time` back to the session the guard compares against.
#[tokio::test]
#[serial]
async fn test_a_recorded_transfer_withholds_the_returns_it_invalidates() {
    use fund::common::alpaca::AccountActivity;

    let pool = fresh_pool().await;
    let today = SessionDate::at(Utc::now());
    let yesterday = today.plus_calendar_days(-1);

    // A flat book that doubles only because capital arrived: the exact shape the guard exists for.
    store_equity(&pool, yesterday, "20000").await;
    store_equity(&pool, today, "30000").await;

    let baseline = fetch_dashboard_data(&pool)
        .await
        .expect("the dashboard must read a database with no transfers");
    assert_eq!(
        baseline.period_returns.one_day,
        Some(50.0),
        "with no transfer recorded this reads as a 50% gain, which is the bug"
    );

    // Stamped at Eastern midnight, the way a dated transfer arrives from Alpaca.
    let transfer = |id: &str, session: SessionDate| {
        AccountActivity::new(
            id.to_string(),
            "CSD".to_string(),
            session.midnight(),
            None,
            None,
            None,
            None,
            Some(decimal("10000")),
            None,
        )
    };

    // A transfer far outside the displayed history must not blank the page. It is excluded by the
    // query's range bound and would be ignored by the guard regardless, so this pins the pair of
    // them together: an old contribution cannot withhold today's return forever.
    let ancient = transfer("deposit-ancient", today.plus_calendar_days(-400));
    account::store_activities(&pool, std::slice::from_ref(&ancient))
        .await
        .expect("Failed to store the out-of-range transfer");
    let unaffected = fetch_dashboard_data(&pool)
        .await
        .expect("the dashboard must read a database with an out-of-range transfer");
    assert_eq!(unaffected.period_returns.one_day, Some(50.0));

    let deposit = transfer("deposit-1", today);
    account::store_activities(&pool, std::slice::from_ref(&deposit))
        .await
        .expect("Failed to store the transfer");

    let guarded = fetch_dashboard_data(&pool)
        .await
        .expect("the dashboard must read a database containing a transfer");
    assert_eq!(guarded.period_returns.one_day, None);
    assert_eq!(guarded.period_returns.since_inception, None);
}

/// A reconstructed session must survive the whole read path.
///
/// `try_get` errors rather than defaults on NULL, so one backfilled row would fail *every* query on
/// the page, not just its own cells. The returns still publish, being derived from equity.
#[tokio::test]
#[serial]
async fn test_a_reconstructed_session_reads_back_without_its_balances() {
    let pool = fresh_pool().await;
    let today = SessionDate::at(Utc::now());
    let yesterday = today.plus_calendar_days(-1);

    store_equity(&pool, yesterday, "20000").await;
    store_reconstructed_equity(&pool, today, "22000").await;

    let data = fetch_dashboard_data(&pool)
        .await
        .expect("the dashboard must read a database containing a reconstructed session");

    let latest = data
        .account_snapshot_history
        .last()
        .expect("the reconstructed session must be in the history");
    assert_eq!(latest.session_date, today);
    assert_eq!(latest.equity, decimal("22000"));
    // Directly, because the exposures below short-circuit on `?` and prove only that one side is
    // absent.
    assert_eq!(latest.cash, None);
    assert_eq!(latest.buying_power, None);
    assert_eq!(latest.long_market_value, None);
    assert_eq!(latest.short_market_value, None);
    assert_eq!(
        latest.gross_exposure(),
        None,
        "an unknown book is not a flat one"
    );
    assert_eq!(latest.net_exposure(), None);

    assert_eq!(
        data.period_returns.one_day,
        Some(10.0),
        "equity is present, so the return it implies must still publish"
    );
}
