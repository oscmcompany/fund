//! Cross-module flows against a real database and a mocked broker.
//!
//! These are the tests that exercise the paths a unit test cannot assemble: the evaluation pass end
//! to end, the pre-close liquidation, and the post-close account sync. Each one runs against the
//! real `schema.sql` and a `mockito` server standing in for Alpaca, so the assertions cover the
//! wiring between modules rather than any one module's arithmetic.

mod common;

use std::collections::HashMap;

use chrono::{Duration, NaiveTime, Utc};
use fund::common::alpaca::{
    AlpacaCredentials, CalendarDay, DataFeed, MarketDataClient, TradingClient,
};
use fund::common::types::{BarInterval, Ticker};
use fund::data::bars;
use fund::data::calendar::{SessionDate, TradingCalendar};
use fund::data::universe::{LiquidityRow, Universe};
use fund::portfolio::evaluate::{self, EvaluationContext};
use fund::portfolio::execute::ExecutionSettings;
use fund::portfolio::pairs::{self, CloseReason, PairEntry};
use fund::portfolio::size::SizingParameters;
use serial_test::serial;
use sqlx::PgPool;
use tokio_util::sync::CancellationToken;

const SESSIONS: i64 = 70;

fn ticker(raw: &str) -> Ticker {
    Ticker::new(raw).expect("test ticker must be valid")
}

fn credentials() -> AlpacaCredentials {
    AlpacaCredentials::new("key".to_string(), "secret".to_string()).unwrap()
}

fn settings() -> ExecutionSettings {
    ExecutionSettings::new(
        std::time::Duration::from_millis(200),
        std::time::Duration::from_millis(5),
    )
}

async fn fresh_pool() -> PgPool {
    let pool = common::test_pool("handlers").await;
    common::reset_tables(&pool).await;
    pool
}

/// A calendar whose session runs 09:30 to 16:00 today, so `minutes_until_close` answers something.
fn calendar_for_today() -> TradingCalendar {
    let today = SessionDate::at(Utc::now());
    let days = (0..5)
        .filter_map(|offset| {
            let date = today.plus_calendar_days(-offset);
            CalendarDay::new(
                date.date(),
                NaiveTime::from_hms_opt(9, 30, 0).unwrap(),
                NaiveTime::from_hms_opt(16, 0, 0).unwrap(),
            )
        })
        .collect();
    TradingCalendar::from_days(days)
}

/// A universe holding exactly the named tickers, every one of them shortable.
fn universe_of(tickers: &[&str]) -> Universe {
    use fund::common::alpaca::TradableAssets;
    use std::collections::HashSet;

    let symbols: HashSet<String> = tickers.iter().map(|raw| raw.to_string()).collect();
    let assets = TradableAssets::from_sets(symbols.clone(), symbols);
    let liquidity: Vec<LiquidityRow> = tickers
        .iter()
        .map(|raw| LiquidityRow::new(ticker(raw), 100.0, 5_000_000.0))
        .collect();
    Universe::build(&assets, &liquidity)
}

// ---------------------------------------------------------------------------
// The evaluation pass
// ---------------------------------------------------------------------------

/// The whole entry half, end to end: history from the database, prices and orders from Alpaca, the
/// pair recorded on the way out. This is the test that would catch a mis-wiring between the screen,
/// the sizing, the gate, and execution — each of which passes its own unit tests in isolation.
#[tokio::test]
#[serial]
async fn test_a_pass_opens_a_pair_and_records_it() {
    let pool = fresh_pool().await;
    let mut server = mockito::Server::new_async().await;

    common::seed_correlated_bars(&pool, &["AAAA", "BBBB"], SESSIONS).await;
    common::seed_details(&pool, &[("AAAA", "Technology"), ("BBBB", "Utilities")]).await;
    // The long leg is forecast to out-return the short, so the model agrees with the spread.
    common::seed_predictions(
        &pool,
        "run-1",
        &[("AAAA", 0.04), ("BBBB", -0.03)],
        Utc::now(),
    )
    .await;

    let close_history = bars::load_aligned_closes(&pool, BarInterval::OneDay, 60)
        .await
        .expect("history must load");
    assert_eq!(close_history.len(), 2, "both legs need aligned history");

    // Prices that push the spread well past the entry threshold. Which leg is stretched decides
    // which becomes the short, so both orderings are quoted and the screen picks.
    let snapshot_body = serde_json::json!({
        "AAAA": { "latestTrade": { "p": last_close(&close_history, "AAAA") } },
        "BBBB": { "latestTrade": { "p": last_close(&close_history, "BBBB") * 1.5 } },
    });

    let _snapshots = server
        .mock(
            "GET",
            mockito::Matcher::Regex(r"^/v2/stocks/snapshots".into()),
        )
        .with_status(200)
        .with_header("content-type", "application/json")
        .with_body(snapshot_body.to_string())
        .create_async()
        .await;
    let _account = server
        .mock("GET", "/v2/account")
        .with_status(200)
        .with_body(account_body(100_000))
        .create_async()
        .await;
    // Both legs, and exactly both. Without the count this test would pass if only one order were
    // ever submitted, which is precisely the mis-wiring it exists to catch.
    let submit = server
        .mock("POST", "/v2/orders")
        .with_status(200)
        .with_body(r#"{"id":"order-1","status":"accepted"}"#)
        .expect(2)
        .create_async()
        .await;
    let _confirm = server
        .mock("GET", "/v2/orders/order-1")
        .with_status(200)
        .with_body(
            r#"{"id":"order-1","status":"filled","filled_qty":"33","filled_avg_price":"150.00"}"#,
        )
        .create_async()
        .await;

    let trading = TradingClient::with_base_url(credentials(), server.url());
    let market_data = MarketDataClient::with_base_url(credentials(), server.url(), DataFeed::Iex);
    let calendar = calendar_for_today();
    let universe = universe_of(&["AAAA", "BBBB"]);

    let running = CancellationToken::new();
    let context = EvaluationContext {
        pool: &pool,
        trading: &trading,
        market_data: &market_data,
        calendar: &calendar,
        universe: &universe,
        close_history: &close_history,
        sizing: SizingParameters::default(),
        execution: settings(),
        shutdown: &running,
        now: session_instant(),
    };

    let summary = evaluate::run_pass(&context)
        .await
        .expect("the pass must run");

    assert_eq!(
        summary.entries_blocked, None,
        "nothing should have stopped the entry half"
    );
    assert_eq!(
        summary.candidates_screened, 1,
        "the fixture must screen one pair"
    );
    assert_eq!(summary.pairs_opened, vec!["AAAA-BBBB".to_string()]);
    assert_eq!(summary.model_run_id.as_deref(), Some("run-1"));

    let open = pairs::load_open_pairs(&pool).await.unwrap();
    assert_eq!(open.len(), 1);
    assert!(
        open[0].entry_z_score() > 0.0,
        "the stored entry score must be positive by the orientation invariant"
    );
    submit.assert_async().await;
}

/// The same fixture as above, with shutdown already requested: the pass must open nothing and say
/// so, rather than submitting orders it may not survive to record.
///
/// This is what makes the drain's timeout in `bin/fund.rs` a real bound. Opening a pair is two
/// broker legs at `FILL_TIMEOUT` each, so a pass that keeps working through its approved list
/// cannot be covered by any fixed timeout — the list's length is decided by the screen. Bounding
/// the *start* of new pairs is what caps the worst case at the one pair already in flight.
///
/// `.expect(0)` on the order mock is the assertion that matters. Without it this test would pass on
/// a summary that merely reported zero opens while orders went out anyway.
#[tokio::test]
#[serial]
async fn test_a_pass_opens_nothing_once_shutdown_is_requested() {
    let pool = fresh_pool().await;
    let mut server = mockito::Server::new_async().await;

    common::seed_correlated_bars(&pool, &["AAAA", "BBBB"], SESSIONS).await;
    common::seed_details(&pool, &[("AAAA", "Technology"), ("BBBB", "Utilities")]).await;
    common::seed_predictions(
        &pool,
        "run-1",
        &[("AAAA", 0.04), ("BBBB", -0.03)],
        Utc::now(),
    )
    .await;

    let close_history = bars::load_aligned_closes(&pool, BarInterval::OneDay, 60)
        .await
        .expect("history must load");

    let snapshot_body = serde_json::json!({
        "AAAA": { "latestTrade": { "p": last_close(&close_history, "AAAA") } },
        "BBBB": { "latestTrade": { "p": last_close(&close_history, "BBBB") * 1.5 } },
    });
    let _snapshots = server
        .mock(
            "GET",
            mockito::Matcher::Regex(r"^/v2/stocks/snapshots".into()),
        )
        .with_status(200)
        .with_header("content-type", "application/json")
        .with_body(snapshot_body.to_string())
        .create_async()
        .await;
    let _account = server
        .mock("GET", "/v2/account")
        .with_status(200)
        .with_body(account_body(100_000))
        .create_async()
        .await;
    let submit = server
        .mock("POST", "/v2/orders")
        .with_status(200)
        .with_body(r#"{"id":"order-1","status":"accepted"}"#)
        .expect(0)
        .create_async()
        .await;

    let trading = TradingClient::with_base_url(credentials(), server.url());
    let market_data = MarketDataClient::with_base_url(credentials(), server.url(), DataFeed::Iex);
    let calendar = calendar_for_today();
    let universe = universe_of(&["AAAA", "BBBB"]);

    let running = CancellationToken::new();
    running.cancel();

    let context = EvaluationContext {
        pool: &pool,
        trading: &trading,
        market_data: &market_data,
        calendar: &calendar,
        universe: &universe,
        close_history: &close_history,
        sizing: SizingParameters::default(),
        execution: settings(),
        shutdown: &running,
        now: session_instant(),
    };

    let summary = evaluate::run_pass(&context)
        .await
        .expect("the pass must still complete cleanly");

    // The pair cleared every check — it was approved and then not reached. That is the distinction
    // `entries_abandoned` exists to record, and it is why this is not `entries_refused`.
    assert_eq!(
        summary.candidates_screened, 1,
        "the screen still ran; only the opening stopped"
    );
    assert_eq!(summary.entries_blocked, None, "the gate did not block");
    assert!(summary.entries_refused.is_empty(), "nothing was refused");
    assert_eq!(summary.entries_abandoned, 1);
    assert!(summary.pairs_opened.is_empty());

    assert!(
        pairs::load_open_pairs(&pool).await.unwrap().is_empty(),
        "no pair may be recorded"
    );
    submit.assert_async().await;
}

/// Exits run before anything else and are never gated. A pass on a full book still has to close
/// what should close, which is the property that makes every early return in the entry half safe.
#[tokio::test]
#[serial]
async fn test_a_pass_closes_a_converged_pair_from_a_full_book() {
    let pool = fresh_pool().await;
    let mut server = mockito::Server::new_async().await;

    common::seed_correlated_bars(&pool, &["AAAA", "BBBB"], SESSIONS).await;
    let close_history = bars::load_aligned_closes(&pool, BarInterval::OneDay, 60)
        .await
        .unwrap();

    // Open the maximum number of pairs so the entry half is blocked on capacity, with the pair
    // under test priced at its own mean so it reads as converged.
    let entry = PairEntry::new(
        fund::common::types::PairID::new(ticker("AAAA"), ticker("BBBB")),
        hedge_ratio_for(&close_history),
        2.5,
        0.03,
        None,
    )
    .unwrap();
    pairs::record_open(&pool, &entry, Utc::now() - Duration::hours(1))
        .await
        .unwrap();
    for (long, short) in [("CCCC", "DDDD"), ("EEEE", "FFFF"), ("GGGG", "HHHH")] {
        let filler = PairEntry::new(
            fund::common::types::PairID::new(ticker(long), ticker(short)),
            1.0,
            2.5,
            0.01,
            None,
        )
        .unwrap();
        pairs::record_open(&pool, &filler, Utc::now())
            .await
            .unwrap();
    }

    // Priced at the last close of each leg: the spread sits at roughly its fitted mean, which is
    // at or below the convergence threshold.
    let snapshot_body = serde_json::json!({
        "AAAA": { "latestTrade": { "p": last_close(&close_history, "AAAA") } },
        "BBBB": { "latestTrade": { "p": mean_reverting_short_price(&close_history) } },
    });

    let _snapshots = server
        .mock(
            "GET",
            mockito::Matcher::Regex(r"^/v2/stocks/snapshots".into()),
        )
        .with_status(200)
        .with_body(snapshot_body.to_string())
        .create_async()
        .await;
    let _close = server
        .mock(
            "DELETE",
            mockito::Matcher::Regex(r"^/v2/positions/\w+".into()),
        )
        .with_status(200)
        .with_body("{}")
        .create_async()
        .await;
    let _account = server
        .mock("GET", "/v2/account")
        .with_status(200)
        .with_body(account_body(100_000))
        .create_async()
        .await;

    let trading = TradingClient::with_base_url(credentials(), server.url());
    let market_data = MarketDataClient::with_base_url(credentials(), server.url(), DataFeed::Iex);
    let calendar = calendar_for_today();
    let universe = universe_of(&["AAAA", "BBBB"]);
    let sizing = SizingParameters::new(4, 1.0).unwrap();

    let running = CancellationToken::new();
    let context = EvaluationContext {
        pool: &pool,
        trading: &trading,
        market_data: &market_data,
        calendar: &calendar,
        universe: &universe,
        close_history: &close_history,
        sizing,
        execution: settings(),
        shutdown: &running,
        now: session_instant(),
    };

    let summary = evaluate::run_pass(&context)
        .await
        .expect("the pass must run");

    // The book was at capacity when the pass began, which is the condition under test: the exit
    // half must run before, and independently of, anything the entry half decides.
    assert_eq!(summary.open_pairs_at_start, 4);
    assert_eq!(
        summary.pairs_closed.len(),
        1,
        "the converged pair must close even though the book started full"
    );
    assert_eq!(summary.pairs_closed[0].pair_id, "AAAA-BBBB");
    assert_eq!(summary.pairs_closed[0].reason, "convergence");
    assert!(summary.pairs_opened.is_empty());

    // Pin *why* nothing opened rather than only that nothing did. Closing the converged pair frees
    // a slot, so the entry half is not capacity-blocked by the time it runs — it runs and finds
    // nothing, because this test seeds no details and no predictions. Asserting both fields
    // distinguishes that from a gate refusal, which an empty `pairs_opened` alone cannot.
    assert_eq!(summary.entries_blocked, None);
    assert_eq!(summary.candidates_screened, 0);
}

/// A pair with no price this pass is held, not closed and not crashed. The pre-close liquidation
/// closes it regardless, so the worst case is holding until 15:45 rather than exiting on a signal.
#[tokio::test]
#[serial]
async fn test_a_pair_that_cannot_be_priced_is_held_and_counted() {
    let pool = fresh_pool().await;
    let mut server = mockito::Server::new_async().await;

    common::seed_correlated_bars(&pool, &["AAAA", "BBBB"], SESSIONS).await;
    let close_history = bars::load_aligned_closes(&pool, BarInterval::OneDay, 60)
        .await
        .unwrap();

    let entry = PairEntry::new(
        fund::common::types::PairID::new(ticker("AAAA"), ticker("BBBB")),
        1.0,
        2.5,
        0.03,
        None,
    )
    .unwrap();
    pairs::record_open(&pool, &entry, Utc::now()).await.unwrap();

    // Alpaca answers with no usable price for either leg.
    let _snapshots = server
        .mock(
            "GET",
            mockito::Matcher::Regex(r"^/v2/stocks/snapshots".into()),
        )
        .with_status(200)
        .with_body(r#"{"AAAA":{},"BBBB":{}}"#)
        .create_async()
        .await;
    let _account = server
        .mock("GET", "/v2/account")
        .with_status(200)
        .with_body(account_body(100_000))
        .create_async()
        .await;

    let trading = TradingClient::with_base_url(credentials(), server.url());
    let market_data = MarketDataClient::with_base_url(credentials(), server.url(), DataFeed::Iex);
    let calendar = calendar_for_today();
    let universe = universe_of(&["AAAA", "BBBB"]);

    let running = CancellationToken::new();
    let context = EvaluationContext {
        pool: &pool,
        trading: &trading,
        market_data: &market_data,
        calendar: &calendar,
        universe: &universe,
        close_history: &close_history,
        sizing: SizingParameters::default(),
        execution: settings(),
        shutdown: &running,
        now: session_instant(),
    };

    let summary = evaluate::run_pass(&context)
        .await
        .expect("the pass must survive");
    assert_eq!(summary.pairs_unpriced, 1);
    assert!(summary.pairs_closed.is_empty());
    assert_eq!(pairs::load_open_pairs(&pool).await.unwrap().len(), 1);
}

// ---------------------------------------------------------------------------
// Liquidation
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial]
async fn test_liquidation_flattens_the_book_and_marks_every_pair() {
    let pool = fresh_pool().await;
    let mut server = mockito::Server::new_async().await;

    for (long, short) in [("AAAA", "BBBB"), ("CCCC", "DDDD")] {
        let entry = PairEntry::new(
            fund::common::types::PairID::new(ticker(long), ticker(short)),
            1.0,
            2.5,
            0.01,
            None,
        )
        .unwrap();
        pairs::record_open(&pool, &entry, Utc::now()).await.unwrap();
    }

    let _bulk = server
        .mock("DELETE", "/v2/positions?cancel_orders=true")
        .with_status(207)
        .with_body(
            r#"[{"symbol":"AAAA","status":200,"body":{}},
                {"symbol":"BBBB","status":200,"body":{}},
                {"symbol":"CCCC","status":200,"body":{}},
                {"symbol":"DDDD","status":200,"body":{}}]"#,
        )
        .create_async()
        .await;

    let trading = TradingClient::with_base_url(credentials(), server.url());
    let summary = evaluate::run_liquidation(&pool, &trading, Utc::now())
        .await
        .expect("the liquidation must run");

    assert_eq!(summary.pairs_closed, 2);
    assert!(summary.pairs_still_open.is_empty());
    assert!(pairs::load_open_pairs(&pool).await.unwrap().is_empty());
}

/// A pair whose leg Alpaca refused to close stays open in the record. Marking it closed would leave
/// the application believing it holds nothing while the account holds a position overnight, and
/// nothing would look again until the next morning.
#[tokio::test]
#[serial]
async fn test_a_refused_leg_leaves_its_pair_open() {
    let pool = fresh_pool().await;
    let mut server = mockito::Server::new_async().await;

    for (long, short) in [("AAAA", "BBBB"), ("CCCC", "DDDD")] {
        let entry = PairEntry::new(
            fund::common::types::PairID::new(ticker(long), ticker(short)),
            1.0,
            2.5,
            0.01,
            None,
        )
        .unwrap();
        pairs::record_open(&pool, &entry, Utc::now()).await.unwrap();
    }

    let _bulk = server
        .mock("DELETE", "/v2/positions?cancel_orders=true")
        .with_status(207)
        .with_body(
            r#"[{"symbol":"AAAA","status":200,"body":{}},
                {"symbol":"BBBB","status":500,"body":{}},
                {"symbol":"CCCC","status":200,"body":{}},
                {"symbol":"DDDD","status":200,"body":{}}]"#,
        )
        .create_async()
        .await;

    let trading = TradingClient::with_base_url(credentials(), server.url());
    let summary = evaluate::run_liquidation(&pool, &trading, Utc::now())
        .await
        .unwrap();

    assert_eq!(summary.positions_refused, 1);
    assert_eq!(summary.pairs_closed, 1);
    assert_eq!(summary.pairs_still_open, vec!["AAAA-BBBB".to_string()]);

    let open = pairs::load_open_pairs(&pool).await.unwrap();
    assert_eq!(open.len(), 1);
    assert_eq!(open[0].long_ticker().as_str(), "AAAA");
}

// ---------------------------------------------------------------------------
// The post-close account sync
// ---------------------------------------------------------------------------

/// Balances stored, fills stored, and the round trip attributed back to the pair that made it.
#[tokio::test]
#[serial]
async fn test_the_account_sync_stores_and_attributes_a_session() {
    use fund::portfolio::account;

    let pool = fresh_pool().await;
    let mut server = mockito::Server::new_async().await;

    let session_date = SessionDate::at(Utc::now());
    let (start, _end) = session_date.bounds();
    let opened = start + Duration::hours(14);
    let closed = start + Duration::hours(18);

    let entry = PairEntry::new(
        fund::common::types::PairID::new(ticker("AAAA"), ticker("BBBB")),
        1.0,
        2.5,
        0.03,
        None,
    )
    .unwrap();
    let id = pairs::record_open(&pool, &entry, opened).await.unwrap();
    pairs::record_close(&pool, id, CloseReason::Convergence, closed)
        .await
        .unwrap();

    let _account = server
        .mock("GET", "/v2/account")
        .with_status(200)
        .with_body(account_body(102_000))
        .create_async()
        .await;
    let _activities = server
        .mock(
            "GET",
            mockito::Matcher::Regex(r"^/v2/account/activities/FILL".into()),
        )
        .with_status(200)
        .with_body(format!(
            r#"[{{"id":"a1","activity_type":"FILL","transaction_time":"{}",
                  "symbol":"AAAA","side":"buy","qty":"10","price":"100"}},
                {{"id":"a2","activity_type":"FILL","transaction_time":"{}",
                  "symbol":"AAAA","side":"sell","qty":"10","price":"110"}}]"#,
            (opened + Duration::minutes(1)).to_rfc3339(),
            (closed - Duration::minutes(1)).to_rfc3339(),
        ))
        .create_async()
        .await;

    let trading = TradingClient::with_base_url(credentials(), server.url());
    let summary = account::sync_account(&pool, &trading, session_date)
        .await
        .expect("the sync must run");

    assert_eq!(summary.activities_stored, 2);
    assert_eq!(summary.activities_unattributed, 0);
    assert_eq!(summary.pairs_attributed, 1);

    let realized: Option<rust_decimal::Decimal> =
        sqlx::query_scalar("SELECT realized_profit_and_loss FROM equity_pairs WHERE id = $1")
            .bind(id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(realized, Some(rust_decimal::Decimal::from(100)));

    assert_eq!(
        account::load_equity_for(&pool, session_date).await.unwrap(),
        Some(rust_decimal::Decimal::from(102_000))
    );
}

/// Re-running a sync must change nothing. Alpaca's activity identifier is the primary key, so the
/// second run conflicts on every row, and the attribution recomputes to the same figure.
#[tokio::test]
#[serial]
async fn test_the_account_sync_is_idempotent() {
    use fund::portfolio::account;

    let pool = fresh_pool().await;
    let mut server = mockito::Server::new_async().await;
    let session_date = SessionDate::at(Utc::now());
    let (start, _end) = session_date.bounds();

    let _account = server
        .mock("GET", "/v2/account")
        .with_status(200)
        .with_body(account_body(99_000))
        .create_async()
        .await;
    let _activities = server
        .mock(
            "GET",
            mockito::Matcher::Regex(r"^/v2/account/activities/FILL".into()),
        )
        .with_status(200)
        .with_body(format!(
            r#"[{{"id":"a1","activity_type":"FILL","transaction_time":"{}",
                  "symbol":"AAAA","side":"buy","qty":"10","price":"100"}}]"#,
            (start + Duration::hours(15)).to_rfc3339(),
        ))
        .create_async()
        .await;

    let trading = TradingClient::with_base_url(credentials(), server.url());
    let first = account::sync_account(&pool, &trading, session_date)
        .await
        .unwrap();
    let second = account::sync_account(&pool, &trading, session_date)
        .await
        .unwrap();

    assert_eq!(first.activities_stored, 1);
    assert_eq!(second.activities_stored, 0);

    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM account_activities")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(count, 1);

    let snapshots: i64 = sqlx::query_scalar("SELECT count(*) FROM account_snapshots")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(snapshots, 1, "the same session must not stack rows");
}

// ---------------------------------------------------------------------------
// Fixture helpers
// ---------------------------------------------------------------------------

fn account_body(equity: i64) -> String {
    format!(
        r#"{{"equity":"{equity}","cash":"{equity}",
             "buying_power":"{}","long_market_value":"0","short_market_value":"0"}}"#,
        equity * 2
    )
}

fn last_close(history: &HashMap<Ticker, Vec<f64>>, symbol: &str) -> f64 {
    *history[&ticker(symbol)]
        .last()
        .expect("the fixture history must be non-empty")
}

/// The hedge ratio the screen would fit for this fixture, so an open pair's stored ratio matches
/// what the exit path rebuilds from.
fn hedge_ratio_for(history: &HashMap<Ticker, Vec<f64>>) -> f64 {
    use fund::portfolio::screen::SpreadModel;
    SpreadModel::fit(&history[&ticker("AAAA")], &history[&ticker("BBBB")])
        .expect("the fixture must fit")
        .hedge_ratio()
}

/// A short-leg price that puts the spread at or below its fitted mean, so `exit_reason` reads
/// convergence.
fn mean_reverting_short_price(history: &HashMap<Ticker, Vec<f64>>) -> f64 {
    use fund::portfolio::screen::SpreadModel;
    let model = SpreadModel::fit(&history[&ticker("AAAA")], &history[&ticker("BBBB")]).unwrap();
    let long_price = last_close(history, "AAAA");

    // Walk down from the last close until the reading is at or below the convergence threshold.
    // Deriving it rather than hardcoding keeps the fixture honest if the series changes.
    let mut short_price = last_close(history, "BBBB");
    for _ in 0..200 {
        match model.z_score(long_price, short_price) {
            Some(z_score) if z_score <= 0.0 => return short_price,
            _ => short_price *= 0.99,
        }
    }
    panic!("could not construct a converged price for the fixture");
}

/// An instant inside the session, so the risk gate's hold-window check has room.
///
/// Built from the **Eastern** date, not `SessionDate::at(Utc::now())`. Between 20:00 Eastern and
/// midnight the two name different days, and a fixture anchored to the UTC date lands outside the
/// session it is meant to sit inside — which is a test that fails for four hours every evening and
/// passes every time anyone looks at it during the day.
///
/// `eastern_day_bounds` returns UTC instants, so the offset below is from Eastern midnight.
fn session_instant() -> chrono::DateTime<Utc> {
    let today = SessionDate::at(Utc::now());
    let (start, _) = today.bounds();
    start + Duration::hours(11) // 11:00 Eastern, mid-session
}
