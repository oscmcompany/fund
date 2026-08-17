//! Schema-level integration tests: the constraints, the trigger, and the queries as PostgreSQL
//! actually runs them.
//!
//! Everything here is something a unit test cannot reach. The CHECK constraints only exist in the
//! database; the notify trigger only fires in the database; the alignment guarantee in
//! `load_aligned_closes` depends on how the query is planned, not on how the Rust reads.
//!
//! Session windows are always built with `SessionDate::at(now).bounds()`, never from
//! `now.date_naive()`. The second is the UTC calendar date, which between 20:00 Eastern and
//! midnight names *tomorrow* — so the window starts four hours in the future and excludes the rows
//! the test just seeded. It is the trading day these queries are bounded by, and a test that
//! assumes the two coincide fails for four hours every evening.

mod common;

use chrono::{Duration, NaiveDate, Utc};
use fund::common::alpaca::{ActivityType, OrderSide};
use fund::common::events::{self, Command, EventType, Outcome};
use fund::common::types::{BarInterval, PairID, SessionDate, Ticker};
use fund::data::adjust::SplitTable;
use fund::data::bars;
use fund::data::truncate::BoundaryTable;

use fund::common::types::CloseReason;
use fund::models::tide::predict;
use fund::portfolio::account;
use fund::portfolio::pairs::{self, PairEntry};
use rust_decimal::Decimal;
use serial_test::serial;
use sqlx::PgPool;
use uuid::Uuid;

fn ticker(raw: &str) -> Ticker {
    Ticker::new(raw).expect("test ticker must be valid")
}

fn pair(long: &str, short: &str) -> PairID {
    PairID::new(ticker(long), ticker(short))
}

fn entry(long: &str, short: &str) -> PairEntry {
    PairEntry::new(
        pair(long, short),
        1.05,
        2.4,
        0.03,
        Some("run-1".to_string()),
    )
    .expect("test entry must be valid")
}

async fn fresh_pool() -> PgPool {
    let pool = common::test_pool("database").await;
    common::reset_tables(&pool).await;
    pool
}

#[tokio::test]
#[serial]
async fn test_a_pair_opens_loads_and_closes() {
    let pool = fresh_pool().await;
    let now = Utc::now();

    let id = pairs::record_open(&pool, &entry("AAAA", "BBBB"), now)
        .await
        .expect("the open must persist");

    let open = pairs::load_open_pairs(&pool).await.expect("must load");
    assert_eq!(open.len(), 1);
    assert_eq!(open[0].id(), id);
    assert_eq!(open[0].pair_id(), &pair("AAAA", "BBBB"));
    assert!((open[0].hedge_ratio() - 1.05).abs() < 1e-12);

    assert!(
        pairs::record_close(&pool, id, CloseReason::Convergence, now)
            .await
            .expect("the close must persist")
    );
    assert!(pairs::load_open_pairs(&pool).await.unwrap().is_empty());
}

/// The predicate on `status = 'open'` is what makes closing idempotent. Without it a replayed
/// liquidation would overwrite a convergence exit with `end_of_day`, and the record of why the
/// trade ended would be lost to a retry.
#[tokio::test]
#[serial]
async fn test_closing_an_already_closed_pair_changes_nothing() {
    let pool = fresh_pool().await;
    let now = Utc::now();
    let id = pairs::record_open(&pool, &entry("AAAA", "BBBB"), now)
        .await
        .unwrap();

    assert!(
        pairs::record_close(&pool, id, CloseReason::Convergence, now)
            .await
            .unwrap()
    );
    assert!(!pairs::record_close(&pool, id, CloseReason::EndOfDay, now)
        .await
        .unwrap());

    let (start, end) = SessionDate::at(now).bounds();
    let closed = pairs::load_closed_between(&pool, start, end).await.unwrap();
    assert_eq!(closed.len(), 1);
    assert_eq!(closed[0].close_reason(), CloseReason::Convergence);
}

/// The closure-consistency constraint lives in the database because three columns have to agree and
/// the handler that writes them is not the only thing that could write them. Without it the table
/// accepts a closed pair with a null `closed_at`, which the descending index then sorts first — so
/// the least complete row presents as the most recent close.
#[tokio::test]
#[serial]
async fn test_the_database_refuses_an_inconsistent_closure() {
    let pool = fresh_pool().await;
    let id = pairs::record_open(&pool, &entry("AAAA", "BBBB"), Utc::now())
        .await
        .unwrap();

    let result = sqlx::query("UPDATE equity_pairs SET status = 'closed' WHERE id = $1")
        .bind(id)
        .execute(&pool)
        .await;
    assert!(
        result.is_err(),
        "closing without a timestamp and reason must violate the constraint"
    );
}

/// Every reason the Rust enum can produce must satisfy the CHECK constraint. A variant whose stored
/// spelling drifted would fail at the close, in a handler, at 15:45.
#[tokio::test]
#[serial]
async fn test_every_close_reason_is_accepted_by_the_constraint() {
    let pool = fresh_pool().await;
    // Letters only: `Ticker::new` rejects digits, so an indexed name would fail validation long
    // before it reached the constraint this test is about.
    let legs = [
        ("AAAA", "BBBB"),
        ("CCCC", "DDDD"),
        ("EEEE", "FFFF"),
        ("GGGG", "HHHH"),
    ];
    assert_eq!(legs.len(), CloseReason::ALL.len());

    for (reason, (long, short)) in CloseReason::ALL.into_iter().zip(legs) {
        let entry = PairEntry::new(pair(long, short), 1.0, 2.0, 0.01, None).unwrap();
        let id = pairs::record_open(&pool, &entry, Utc::now()).await.unwrap();
        assert!(
            pairs::record_close(&pool, id, reason, Utc::now())
                .await
                .expect("every reason must satisfy the CHECK constraint"),
            "{reason} must be storable"
        );
    }
}

#[tokio::test]
#[serial]
async fn test_realized_profit_and_loss_is_written_only_onto_a_closed_pair() {
    let pool = fresh_pool().await;
    let now = Utc::now();
    let id = pairs::record_open(&pool, &entry("AAAA", "BBBB"), now)
        .await
        .unwrap();

    assert!(
        !pairs::record_realized_profit_and_loss(&pool, id, Decimal::from(150))
            .await
            .unwrap(),
        "an open pair has no realized result to record"
    );

    pairs::record_close(&pool, id, CloseReason::Convergence, now)
        .await
        .unwrap();
    assert!(
        pairs::record_realized_profit_and_loss(&pool, id, Decimal::from(150))
            .await
            .unwrap()
    );
}

/// Alpaca's activity identifier is the primary key, which is what makes the post-close sync
/// idempotent by construction: a re-run conflicts on every row and changes nothing.
#[tokio::test]
#[serial]
async fn test_activities_are_idempotent_on_alpacas_identifier() {
    use fund::common::alpaca::AccountActivity;

    let pool = fresh_pool().await;
    let activity = AccountActivity::new(
        "activity-1".to_string(),
        ActivityType::Fill,
        Utc::now(),
        Some(ticker("AAAA")),
        Some(OrderSide::Buy),
        Some(Decimal::from(10)),
        Some(Decimal::from(150)),
        None,
        Some("order-1".to_string()),
    );

    assert_eq!(
        account::store_activities(&pool, std::slice::from_ref(&activity))
            .await
            .unwrap(),
        vec![activity.activity_id().to_string()],
        "the first store reports the row it inserted"
    );
    assert_eq!(
        account::store_activities(&pool, std::slice::from_ref(&activity))
            .await
            .unwrap(),
        Vec::<String>::new(),
        "a re-run must conflict rather than duplicate, and report nothing new"
    );

    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM account_activities")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(count, 1);
}

#[tokio::test]
#[serial]
async fn test_an_account_snapshot_overwrites_the_same_session() {
    use fund::common::alpaca::AccountSnapshot;

    let pool = fresh_pool().await;
    let date = SessionDate::from_date(NaiveDate::from_ymd_opt(2026, 7, 30).unwrap());
    let snapshot = |equity: i64| {
        AccountSnapshot::new(
            Decimal::from(equity),
            Decimal::from(equity),
            Decimal::from(equity * 2),
            Decimal::ZERO,
            Decimal::ZERO,
        )
    };

    account::store_snapshot(&pool, date, &snapshot(100_000))
        .await
        .unwrap();
    account::store_snapshot(&pool, date, &snapshot(101_000))
        .await
        .unwrap();

    assert_eq!(
        account::load_equity_for(&pool, date).await.unwrap(),
        Some(Decimal::from(101_000))
    );
}

/// A missing snapshot is `None`, not an error and not a zero. The drawdown gate reads it as "no
/// reference" and skips the check; a zero would read as a total loss and halt the strategy.
#[tokio::test]
#[serial]
async fn test_a_session_with_no_snapshot_reads_as_none() {
    let pool = fresh_pool().await;
    let date = SessionDate::from_date(NaiveDate::from_ymd_opt(2026, 7, 30).unwrap());
    assert_eq!(account::load_equity_for(&pool, date).await.unwrap(), None);
}

/// Every balance column for a session, in schema order.
///
/// All four together because nothing enforces all-or-nothing: a writer that filled three would
/// satisfy any single-column assertion.
type Balances = (
    Option<Decimal>,
    Option<Decimal>,
    Option<Decimal>,
    Option<Decimal>,
);

async fn balances_for(pool: &PgPool, session_date: SessionDate) -> Balances {
    sqlx::query_as(
        "SELECT cash, buying_power, long_market_value, short_market_value
         FROM account_snapshots WHERE session_date = $1",
    )
    .bind(session_date.date())
    .fetch_one(pool)
    .await
    .expect("the snapshot row must exist")
}

/// A reconstructed session stores equity and leaves every balance NULL — the insert the old
/// `NOT NULL` schema rejected outright.
#[tokio::test]
#[serial]
async fn test_a_reconstructed_snapshot_stores_equity_without_balances() {
    let pool = fresh_pool().await;
    let date = SessionDate::from_date(NaiveDate::from_ymd_opt(2026, 7, 30).unwrap());

    let written = account::store_equity_snapshot(&pool, date, Decimal::from(20_559))
        .await
        .unwrap();

    assert_eq!(written, 1);
    assert_eq!(
        account::load_equity_for(&pool, date).await.unwrap(),
        Some(Decimal::from(20_559))
    );

    assert_eq!(
        balances_for(&pool, date).await,
        (None, None, None, None),
        "portfolio history supplies no balances, so none may be invented"
    );
}

/// `store_snapshot` upserts and `store_equity_snapshot` does not, so a backfill re-run over an
/// already-synced range cannot strip the balances off a complete row.
#[tokio::test]
#[serial]
async fn test_a_backfill_never_downgrades_a_complete_snapshot() {
    use fund::common::alpaca::AccountSnapshot;

    let pool = fresh_pool().await;
    let date = SessionDate::from_date(NaiveDate::from_ymd_opt(2026, 7, 30).unwrap());
    let complete = AccountSnapshot::new(
        Decimal::from(100_000),
        Decimal::from(50_000),
        Decimal::from(200_000),
        Decimal::from(40_000),
        Decimal::from(-40_000),
    );
    account::store_snapshot(&pool, date, &complete)
        .await
        .unwrap();

    let written = account::store_equity_snapshot(&pool, date, Decimal::from(999))
        .await
        .unwrap();

    assert_eq!(written, 0, "the existing row must be left alone");
    assert_eq!(
        account::load_equity_for(&pool, date).await.unwrap(),
        Some(Decimal::from(100_000)),
        "equity must not be replaced by the backfilled value"
    );

    assert_eq!(
        balances_for(&pool, date).await,
        (
            Some(Decimal::from(50_000)),
            Some(Decimal::from(200_000)),
            Some(Decimal::from(40_000)),
            Some(Decimal::from(-40_000)),
        ),
        "every balance must survive, not merely the first one checked"
    );
}

/// Position `i` in every returned series must be the same session. Two series of equal length over
/// different dates produce a correlation between different days, and nothing downstream carries
/// enough information to notice.
#[tokio::test]
#[serial]
async fn test_aligned_closes_drops_a_ticker_with_a_gap() {
    let pool = fresh_pool().await;
    let today = SessionDate::at(Utc::now());

    // Negative: the window this loader serves is trailing history, so the fixture must run
    // backwards from today. Seeding forwards puts the whole window in the future, and the
    // alignment assertion then proves nothing about the historical lower bound.
    for offset in 0..5 {
        let date = today.plus_calendar_days(-offset);
        common::seed_bar(&pool, "AAAA", date, 100.0 + offset as f64).await;
        // BBBB is missing one session in the middle of the window.
        if offset != 2 {
            common::seed_bar(&pool, "BBBB", date, 50.0 + offset as f64).await;
        }
    }

    let closes = bars::load_aligned_closes(
        &pool,
        BarInterval::OneDay,
        5,
        &SplitTable::default(),
        &BoundaryTable::default(),
        SessionDate::at(Utc::now()),
    )
    .await
    .expect("the load must succeed");

    assert!(closes.contains_key(&ticker("AAAA")));
    assert!(
        !closes.contains_key(&ticker("BBBB")),
        "a ticker missing a session must be dropped, not silently shortened"
    );
    assert_eq!(closes[&ticker("AAAA")].len(), 5);
}

/// The interval is part of the primary key, so a daily and a one-minute bar for the same ticker and
/// instant coexist. A query that ignored the interval would mix sampling rates into one series.
#[tokio::test]
#[serial]
async fn test_aligned_closes_reads_only_the_requested_interval() {
    let pool = fresh_pool().await;
    let today = SessionDate::at(Utc::now());
    common::seed_bar(&pool, "AAAA", today, 100.0).await;

    // Inside the session, like the daily bar above and unlike Eastern midnight — the interval is
    // what the loader must discriminate on here, not the hour.
    let timestamp = common::session_close(today);
    sqlx::query(
        "INSERT INTO equity_bars \
         (ticker, bar_interval, timestamp, open_price, high_price, low_price, close_price, volume) \
         VALUES ('AAAA', 'one_minute', $1, 9, 9, 9, 9, 100)",
    )
    .bind(timestamp)
    .execute(&pool)
    .await
    .unwrap();

    let closes = bars::load_aligned_closes(
        &pool,
        BarInterval::OneDay,
        1,
        &SplitTable::default(),
        &BoundaryTable::default(),
        SessionDate::at(Utc::now()),
    )
    .await
    .unwrap();
    assert_eq!(closes[&ticker("AAAA")], vec![100.0]);
}

/// The pass reads only the current session's predictions. Reading yesterday's when this morning's
/// inference failed would present a stale prediction as current, and nothing downstream carries the
/// timestamp far enough to notice.
#[tokio::test]
#[serial]
async fn test_predictions_are_bounded_to_the_session_window() {
    let pool = fresh_pool().await;
    let now = Utc::now();
    let (start, end) = SessionDate::at(now).bounds();

    common::seed_predictions(&pool, "run-today", &[("AAAA", 0.03)], now).await;
    common::seed_predictions(
        &pool,
        "run-yesterday",
        &[("BBBB", 0.04)],
        start - Duration::hours(2),
    )
    .await;

    let loaded = predict::load_predictions_between(&pool, start, end)
        .await
        .unwrap();
    assert_eq!(loaded.len(), 1);
    assert_eq!(loaded[0].ticker().as_str(), "AAAA");
    assert_eq!(loaded[0].model_run_id(), "run-today");
}

/// One row per ticker, the newest. A re-run leaves two predictions for the same symbol, and feeding
/// both into the screen would let one ticker appear on both legs of the same pair.
#[tokio::test]
#[serial]
async fn test_predictions_return_the_newest_row_per_ticker() {
    let pool = fresh_pool().await;
    let now = Utc::now();
    let (start, end) = SessionDate::at(now).bounds();

    common::seed_predictions(
        &pool,
        "run-early",
        &[("AAAA", 0.01)],
        now - Duration::hours(2),
    )
    .await;
    common::seed_predictions(
        &pool,
        "run-late",
        &[("AAAA", 0.05)],
        now - Duration::minutes(5),
    )
    .await;

    let loaded = predict::load_predictions_between(&pool, start, end)
        .await
        .unwrap();
    assert_eq!(loaded.len(), 1);
    assert_eq!(loaded[0].model_run_id(), "run-late");
}

#[tokio::test]
#[serial]
async fn test_a_request_with_no_terminal_outcome_is_recovered() {
    let pool = fresh_pool().await;

    events::emit(
        &pool,
        EventType::new(Command::AccountSync, Outcome::Requested),
        serde_json::json!({}),
    )
    .await
    .unwrap();

    let recovered = events::recover_missed_commands(&pool).await.unwrap();
    assert_eq!(recovered, vec![Command::AccountSync]);

    events::emit_completed(&pool, Command::AccountSync, serde_json::json!({"rows": 1}))
        .await
        .unwrap();
    assert!(events::recover_missed_commands(&pool)
        .await
        .unwrap()
        .is_empty());
}

/// A failed command is finished, not outstanding. Replaying an errored command at startup would
/// re-run whatever failed, on every restart, until it stopped failing.
#[tokio::test]
#[serial]
async fn test_an_errored_command_is_not_recovered() {
    let pool = fresh_pool().await;
    events::emit(
        &pool,
        EventType::new(Command::MarketDataSync, Outcome::Requested),
        serde_json::json!({}),
    )
    .await
    .unwrap();
    events::emit_errored(&pool, Command::MarketDataSync, "Alpaca returned 500")
        .await
        .unwrap();

    assert!(events::recover_missed_commands(&pool)
        .await
        .unwrap()
        .is_empty());
}

/// Only the five-minute evaluation is skipped, and only because a fresher firing is minutes away.
/// A missed liquidation in particular must be replayed: the alternative is positions held overnight.
#[tokio::test]
#[serial]
async fn test_an_unfinished_evaluation_is_skipped_but_a_liquidation_is_not() {
    let pool = fresh_pool().await;
    for command in [Command::PortfolioEvaluation, Command::PortfolioLiquidation] {
        events::emit(
            &pool,
            EventType::new(command, Outcome::Requested),
            serde_json::json!({}),
        )
        .await
        .unwrap();
    }

    let recovered = events::recover_missed_commands(&pool).await.unwrap();
    assert_eq!(recovered, vec![Command::PortfolioLiquidation]);
}

/// The trigger's size guard is the reason completion payloads can carry a growing summary at all.
/// `pg_notify` rejects anything at or past 8000 bytes with an error that would abort the insert,
/// so a large payload must round-trip through the row instead.
#[tokio::test]
#[serial]
async fn test_an_oversized_payload_still_inserts_and_arrives_truncated() {
    let pool = fresh_pool().await;
    let mut listener = sqlx::postgres::PgListener::connect_with(&pool)
        .await
        .expect("the listener must connect");
    listener.listen("events").await.expect("must subscribe");

    let oversized = serde_json::json!({ "filler": "x".repeat(9_000) });
    events::emit(
        &pool,
        EventType::new(Command::DatabaseExport, Outcome::Completed),
        oversized.clone(),
    )
    .await
    .expect("an oversized payload must still insert");

    let received = tokio::time::timeout(std::time::Duration::from_secs(5), listener.recv())
        .await
        .expect("a notification must arrive")
        .expect("the listener must not error");

    let notification =
        events::Notification::parse(received.payload()).expect("the body must parse");
    assert!(
        notification.payload_truncated,
        "an oversized payload must be flagged rather than dropped silently"
    );

    let full = events::fetch_payload(&pool, notification.event_id)
        .await
        .expect("the row must carry the whole payload");
    assert_eq!(full, oversized);
}

/// The small case has to keep working, or the truncation guard would have quietly turned every
/// notification into a database round trip.
#[tokio::test]
#[serial]
async fn test_a_small_payload_arrives_whole() {
    let pool = fresh_pool().await;
    let mut listener = sqlx::postgres::PgListener::connect_with(&pool)
        .await
        .unwrap();
    listener.listen("events").await.unwrap();

    let payload = serde_json::json!({ "pairs_opened": ["AAAA-BBBB"] });
    events::emit(
        &pool,
        EventType::new(Command::PortfolioEvaluation, Outcome::Completed),
        payload.clone(),
    )
    .await
    .unwrap();

    let received = tokio::time::timeout(std::time::Duration::from_secs(5), listener.recv())
        .await
        .unwrap()
        .unwrap();
    let notification = events::Notification::parse(received.payload()).unwrap();

    assert!(!notification.payload_truncated);
    assert_eq!(notification.payload, payload);
}

/// Every event type the Rust vocabulary can emit must be storable. The table has no CHECK on
/// `event_type`, so this is really a test that the round trip through `Notification::parse` holds
/// for all eighteen — a name that did not parse back is a name no listener could dispatch.
#[tokio::test]
#[serial]
async fn test_every_event_type_round_trips_through_the_trigger() {
    let pool = fresh_pool().await;
    let mut listener = sqlx::postgres::PgListener::connect_with(&pool)
        .await
        .unwrap();
    listener.listen("events").await.unwrap();

    for command in Command::ALL {
        for outcome in [Outcome::Requested, Outcome::Completed, Outcome::Errored] {
            let event_type = EventType::new(command, outcome);
            events::emit(&pool, event_type, serde_json::json!({}))
                .await
                .unwrap();

            let received = tokio::time::timeout(std::time::Duration::from_secs(5), listener.recv())
                .await
                .unwrap()
                .unwrap();
            let notification = events::Notification::parse(received.payload())
                .unwrap_or_else(|| panic!("{event_type} must parse back"));
            assert_eq!(notification.event_type, event_type);
        }
    }
}

#[tokio::test]
#[serial]
async fn test_sectors_load_as_a_lookup_map() {
    let pool = fresh_pool().await;
    common::seed_details(&pool, &[("AAAA", "Technology"), ("BBBB", "Utilities")]).await;

    let sectors = fund::data::details::load_sectors(&pool).await.unwrap();
    assert_eq!(sectors.len(), 2);
    assert_eq!(sectors[&ticker("AAAA")], "Technology");
}

/// A pair identifier is stored as text and parsed back on read. A leg whose symbol contains a dot
/// must survive the round trip, because splitting on every dash would turn `BRK.B-MSFT` into three
/// fragments and drop the pair on load.
#[tokio::test]
#[serial]
async fn test_a_dotted_ticker_survives_the_pair_round_trip() {
    let pool = fresh_pool().await;
    let entry = PairEntry::new(pair("BRK.B", "MSFT"), 1.0, 2.5, 0.01, None).unwrap();
    let id = pairs::record_open(&pool, &entry, Utc::now()).await.unwrap();

    let open = pairs::load_open_pairs(&pool).await.unwrap();
    assert_eq!(open.len(), 1);
    assert_eq!(open[0].id(), id);
    assert_eq!(open[0].long_ticker().as_str(), "BRK.B");
    assert_eq!(open[0].short_ticker().as_str(), "MSFT");
}

/// A row whose identifier no longer parses must not take the readable pairs down with it. The pass
/// cannot act on it — there is no way to know which symbols to close — but it can still act on
/// everything else.
#[tokio::test]
#[serial]
async fn test_an_unparsable_pair_row_is_dropped_without_failing_the_load() {
    let pool = fresh_pool().await;
    pairs::record_open(&pool, &entry("AAAA", "BBBB"), Utc::now())
        .await
        .unwrap();

    sqlx::query(
        "INSERT INTO equity_pairs \
         (id, pair_id, long_ticker, short_ticker, hedge_ratio, entry_z_score, signal_strength, \
          status, opened_at) \
         VALUES ($1, 'not a pair identifier', 'X', 'Y', 1, 2, 0.1, 'open', now())",
    )
    .bind(Uuid::new_v4())
    .execute(&pool)
    .await
    .unwrap();

    let open = pairs::load_open_pairs(&pool).await.expect("must not fail");
    assert_eq!(open.len(), 1);
    assert_eq!(open[0].pair_id(), &pair("AAAA", "BBBB"));
}

/// The stored closes are raw, so the loader is what makes them comparable. A caller adjusting
/// afterwards could not: the returned map has no dates to key a factor on.
#[tokio::test]
#[serial]
async fn test_aligned_closes_are_restated_onto_todays_share_basis() {
    let pool = fresh_pool().await;
    let today = SessionDate::at(Utc::now());

    // A two-for-one executing in the middle of the window: the two sessions before it are on the
    // old basis and the two from it onwards are already on the new one.
    let execution_date = today.plus_calendar_days(-2);
    for offset in 0..5 {
        common::seed_bar(&pool, "AAAA", today.plus_calendar_days(-offset), 100.0).await;
    }

    let splits = SplitTable::from_dataframe(
        &fund::data::splits::splits_to_dataframe(
            &[fund::common::types::EquitySplit::new(
                "E1".to_string(),
                ticker("AAAA"),
                execution_date,
                1.0,
                2.0,
            )
            .unwrap()],
            Utc::now(),
        )
        .unwrap(),
    )
    .unwrap();

    let closes = bars::load_aligned_closes(
        &pool,
        BarInterval::OneDay,
        5,
        &splits,
        &BoundaryTable::default(),
        today,
    )
    .await
    .expect("the load must succeed");

    // Ordered oldest first, so the two halved entries lead.
    assert_eq!(
        closes[&ticker("AAAA")],
        vec![50.0, 50.0, 100.0, 100.0, 100.0],
        "sessions before the execution date are halved and the rest are left alone"
    );
}

/// The same guarantee on the frame the model reads, which is a different loader and a different
/// shape reaching the same rows.
#[tokio::test]
#[serial]
async fn test_the_bar_frame_is_restated_onto_todays_share_basis() {
    let pool = fresh_pool().await;
    let today = SessionDate::at(Utc::now());
    common::seed_bar(&pool, "AAAA", today.plus_calendar_days(-3), 100.0).await;

    let splits = SplitTable::from_dataframe(
        &fund::data::splits::splits_to_dataframe(
            &[fund::common::types::EquitySplit::new(
                "E1".to_string(),
                ticker("AAAA"),
                today.plus_calendar_days(-1),
                1.0,
                4.0,
            )
            .unwrap()],
            Utc::now(),
        )
        .unwrap(),
    )
    .unwrap();

    let frame = bars::load_bars_dataframe(
        &pool,
        BarInterval::OneDay,
        10,
        &splits,
        &BoundaryTable::default(),
        today,
    )
    .await
    .expect("the load must succeed");

    assert_eq!(
        frame.column("close_price").unwrap().f64().unwrap().get(0),
        Some(25.0),
        "a four-for-one quarters every price before it"
    );
}

/// The window a loader reads and the basis it restates onto have to be the same day. Both bounds
/// used to come off the wall clock while the factor came off `as_of`, so a replay would have loaded
/// today's sessions and adjusted them to a past date.
#[tokio::test]
#[serial]
async fn test_the_loaded_window_follows_as_of_rather_than_the_clock() {
    let pool = fresh_pool().await;
    let today = SessionDate::at(Utc::now());
    let as_of = today.plus_calendar_days(-10);

    // Inside the requested window, on the first session outside it, and well past it. Only the
    // first belongs to an `as_of` ten days back.
    common::seed_bar(&pool, "AAAA", as_of.plus_calendar_days(-1), 10.0).await;
    common::seed_bar(&pool, "AAAA", as_of.plus_calendar_days(1), 55.0).await;
    common::seed_bar(&pool, "AAAA", today, 99.0).await;

    let frame = bars::load_bars_dataframe(
        &pool,
        BarInterval::OneDay,
        5,
        &SplitTable::default(),
        &BoundaryTable::default(),
        as_of,
    )
    .await
    .expect("the load must succeed");

    assert_eq!(
        frame.height(),
        1,
        "a session after `as_of` is outside the window it asked for"
    );
    assert_eq!(
        frame.column("close_price").unwrap().f64().unwrap().get(0),
        Some(10.0)
    );

    let closes = bars::load_aligned_closes(
        &pool,
        BarInterval::OneDay,
        5,
        &SplitTable::default(),
        &BoundaryTable::default(),
        as_of,
    )
    .await
    .expect("the load must succeed");

    assert_eq!(
        closes[&ticker("AAAA")],
        vec![10.0],
        "the aligned window is bounded above by `as_of` too, not by the latest session stored"
    );
}

/// `None` in the third position is a boundary that stops a series; `Some` renames it onto that
/// symbol, which is what lets the successor inherit the history.
fn boundary_table(rows: &[(&str, SessionDate, Option<&str>)]) -> BoundaryTable {
    use polars::prelude::*;
    let tickers: Vec<String> = rows
        .iter()
        .map(|(ticker, _, _)| ticker.to_string())
        .collect();
    let dates: Vec<String> = rows
        .iter()
        .map(|(_, date, _)| date.date().format("%Y-%m-%d").to_string())
        .collect();
    let reasons: Vec<String> = rows
        .iter()
        .map(|(_, _, to)| match to {
            Some(_) => "renamed".to_string(),
            None => "spun_off".to_string(),
        })
        .collect();
    let related: Vec<Option<String>> = rows
        .iter()
        .map(|(_, _, to)| to.map(str::to_string))
        .collect();
    let frame = DataFrame::new(vec![
        Column::new("ticker".into(), tickers),
        Column::new("date".into(), dates),
        Column::new("reason".into(), reasons),
        Column::new("related_ticker".into(), related),
    ])
    .expect("a frame must build");
    BoundaryTable::from_dataframe(&frame).expect("the table must build")
}

/// The other half of a rename. `AAAA` stops trading and `BBBB` continues it, so neither symbol has
/// a full window on its own and the screen would see two unusable series where there is one company.
#[tokio::test]
#[serial]
async fn test_a_renamed_company_keeps_its_history_under_the_new_symbol() {
    let pool = fresh_pool().await;
    let today = SessionDate::at(Utc::now());
    let renamed_on = today.plus_calendar_days(-3);

    for offset in 4..=6 {
        common::seed_bar(&pool, "AAAA", today.plus_calendar_days(-offset), 10.0).await;
    }
    for offset in 1..=3 {
        common::seed_bar(&pool, "BBBB", today.plus_calendar_days(-offset), 20.0).await;
    }

    let boundaries = boundary_table(&[("AAAA", renamed_on, Some("BBBB"))]);
    let closes = bars::load_aligned_closes(
        &pool,
        BarInterval::OneDay,
        6,
        &SplitTable::default(),
        &boundaries,
        today,
    )
    .await
    .expect("the load must succeed");

    assert_eq!(
        closes.get(&ticker("BBBB")).map(Vec::len),
        Some(6),
        "the successor carries both runs, so it fills the window"
    );
    assert_eq!(
        closes[&ticker("BBBB")],
        vec![10.0, 10.0, 10.0, 20.0, 20.0, 20.0],
        "and they arrive in session order, not in the order the query returned them"
    );
    assert!(
        !closes.contains_key(&ticker("AAAA")),
        "the symbol that stopped trading is not a candidate of its own"
    );
}

/// Both ways a ticker leaves the aligned set, exercised together: a hole in its history, and a
/// boundary that takes every session it has.
///
/// The two are reported as separate counts, which this cannot assert — they are log fields, not
/// return values — so it covers the exclusions themselves rather than the tally.
#[tokio::test]
#[serial]
async fn test_a_gap_and_a_boundary_both_exclude_a_ticker() {
    let pool = fresh_pool().await;
    let today = SessionDate::at(Utc::now());

    // Complete: three consecutive sessions, no boundary.
    for offset in 1..=3 {
        common::seed_bar(&pool, "GOOD", today.plus_calendar_days(-offset), 10.0).await;
    }
    // A hole in the middle, no boundary.
    common::seed_bar(&pool, "GAPY", today.plus_calendar_days(-1), 10.0).await;
    common::seed_bar(&pool, "GAPY", today.plus_calendar_days(-3), 10.0).await;
    // Every session precedes its boundary, so nothing of it survives.
    for offset in 1..=3 {
        common::seed_bar(&pool, "BNDY", today.plus_calendar_days(-offset), 10.0).await;
    }

    let boundaries = boundary_table(&[("BNDY", today, None)]);
    let closes = bars::load_aligned_closes(
        &pool,
        BarInterval::OneDay,
        3,
        &SplitTable::default(),
        &boundaries,
        today,
    )
    .await
    .expect("the load must succeed");

    assert_eq!(
        closes.keys().collect::<Vec<_>>(),
        vec![&ticker("GOOD")],
        "only the complete unbounded series survives"
    );
}

/// A predecessor and its successor can trade at the same time — `BK` and `BNY` share about two
/// dozen sessions — and on those the symbol still trading is the one whose price counts.
#[tokio::test]
#[serial]
async fn test_an_overlapping_session_keeps_the_successors_own_price() {
    let pool = fresh_pool().await;
    let today = SessionDate::at(Utc::now());
    let renamed_on = today.plus_calendar_days(-2);
    let shared = today.plus_calendar_days(-3);

    common::seed_bar(&pool, "AAAA", today.plus_calendar_days(-4), 10.0).await;
    common::seed_bar(&pool, "AAAA", shared, 11.0).await;
    common::seed_bar(&pool, "BBBB", shared, 99.0).await;
    common::seed_bar(&pool, "BBBB", today.plus_calendar_days(-2), 20.0).await;

    let boundaries = boundary_table(&[("AAAA", renamed_on, Some("BBBB"))]);
    let closes = bars::load_aligned_closes(
        &pool,
        BarInterval::OneDay,
        3,
        &SplitTable::default(),
        &boundaries,
        today,
    )
    .await
    .expect("the load must succeed");

    assert_eq!(
        closes[&ticker("BBBB")],
        vec![10.0, 99.0, 20.0],
        "the shared session takes BBBB's own close, not the one inherited from AAAA"
    );
}

/// The case the boundary table exists for, in the shape it really occurred. `RNA` was Avidity
/// Biosciences until 2026-02-26 and Atrium Therapeutics after it, so a window spanning that date
/// fits a hedge ratio across two companies' prices and reports nothing unusual.
#[tokio::test]
#[serial]
async fn test_a_window_is_not_read_across_a_boundary() {
    let pool = fresh_pool().await;
    let today = SessionDate::at(Utc::now());
    let boundary = today.plus_calendar_days(-3);

    for offset in 1..=6 {
        common::seed_bar(
            &pool,
            "AAAA",
            today.plus_calendar_days(-offset),
            10.0 * offset as f64,
        )
        .await;
    }

    let boundaries = boundary_table(&[("AAAA", boundary, None)]);
    let frame = bars::load_bars_dataframe(
        &pool,
        BarInterval::OneDay,
        10,
        &SplitTable::default(),
        &boundaries,
        today,
    )
    .await
    .expect("the load must succeed");

    assert_eq!(
        frame.height(),
        3,
        "only the sessions from the boundary onward describe the company trading now"
    );

    let closes = bars::load_aligned_closes(
        &pool,
        BarInterval::OneDay,
        6,
        &SplitTable::default(),
        &boundaries,
        today,
    )
    .await
    .expect("the load must succeed");

    assert!(
        !closes.contains_key(&ticker("AAAA")),
        "a truncated series cannot fill a six-session window, so it is not a candidate"
    );
}

/// `SessionDate::bounds` is half-open, so a row landing exactly on its upper edge belongs to the
/// next session and must not load.
///
/// Stamped at midnight deliberately: real bars arrive sixteen hours inside the interval, where both
/// bound forms agree, so this guards the predicate rather than reproducing observed data.
#[tokio::test]
#[serial]
async fn test_a_bar_on_the_upper_bound_belongs_to_the_next_session() {
    let pool = fresh_pool().await;
    let as_of = SessionDate::at(Utc::now()).plus_calendar_days(-10);

    common::seed_bar(&pool, "AAAA", as_of.plus_calendar_days(-1), 10.0).await;

    sqlx::query(
        "INSERT INTO equity_bars \
         (ticker, bar_interval, timestamp, open_price, high_price, low_price, close_price, volume) \
         VALUES ('AAAA', 'one_day', $1, 55, 55, 55, 55, 5000000)",
    )
    .bind(as_of.plus_calendar_days(1).midnight())
    .execute(&pool)
    .await
    .expect("Failed to seed the boundary bar");

    let frame = bars::load_bars_dataframe(
        &pool,
        BarInterval::OneDay,
        5,
        &SplitTable::default(),
        &BoundaryTable::default(),
        as_of,
    )
    .await
    .expect("the load must succeed");

    assert_eq!(
        frame.height(),
        1,
        "the upper bound is exclusive, so the next session's opening instant is outside it"
    );

    let closes = bars::load_aligned_closes(
        &pool,
        BarInterval::OneDay,
        5,
        &SplitTable::default(),
        &BoundaryTable::default(),
        as_of,
    )
    .await
    .expect("the load must succeed");

    assert_eq!(
        closes[&ticker("AAAA")],
        vec![10.0],
        "both loaders have to agree about where the window stops"
    );
}
