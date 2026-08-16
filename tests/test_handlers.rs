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
use fund::common::session_log::SessionLog;
use fund::common::types::{BarInterval, SessionDate, Ticker};
use fund::data::adjust::SplitTable;
use fund::data::bars;
use fund::data::calendar::TradingCalendar;
use fund::data::truncate::BoundaryTable;
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

/// A session log in a directory of this test's own.
///
/// Every path under test writes one, so a shared directory would let one test's records be read as
/// another's — and these tests already share a database.
fn session_log(name: &str) -> SessionLog {
    use std::sync::atomic::{AtomicUsize, Ordering};
    static COUNTER: AtomicUsize = AtomicUsize::new(0);
    let unique = COUNTER.fetch_add(1, Ordering::Relaxed);
    let directory = std::env::temp_dir().join(format!(
        "fund-test-handlers-{name}-{}-{unique}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&directory);
    SessionLog::new(directory).expect("the log directory must be creatable")
}

/// Every record a log holds, in the order it was written.
fn recorded(log: &SessionLog) -> Vec<serde_json::Value> {
    let mut files: Vec<_> = std::fs::read_dir(log.directory())
        .expect("the log directory must be readable")
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .collect();
    files.sort();
    files
        .iter()
        .filter_map(|path| std::fs::read_to_string(path).ok())
        .flat_map(|contents| {
            contents
                .lines()
                .filter_map(|line| serde_json::from_str(line).ok())
                .collect::<Vec<serde_json::Value>>()
        })
        .collect()
}

/// The records of one type, preserving write order.
fn of_type<'a>(records: &'a [serde_json::Value], event_type: &str) -> Vec<&'a serde_json::Value> {
    records
        .iter()
        .filter(|record| record["event_type"] == event_type)
        .collect()
}

/// The five most recent weekday sessions ending at `session_date`.
///
/// Weekends are filtered rather than taken as consecutive calendar days, so
/// `previous_trading_day` answers what it would in production: Friday for a Monday session, not
/// Sunday. Without the filter a Monday would silently exercise a calendar that cannot exist, and the
/// gap test would pass while checking nothing real.
///
/// `is_weekend` rather than `is_trading_day` because this *builds* the calendar the latter consults;
/// it is the case that method's own documentation names, "bounding a fetch range before the calendar
/// is available". Holidays are not modelled — no test here turns on one.
fn calendar_ending_at(session_date: SessionDate) -> TradingCalendar {
    let days = (0..10)
        .map(|offset| session_date.plus_calendar_days(-offset))
        .filter(|date| !date.is_weekend())
        .take(5)
        .filter_map(|date| {
            CalendarDay::new(
                date.date(),
                NaiveTime::from_hms_opt(9, 30, 0).unwrap(),
                NaiveTime::from_hms_opt(16, 0, 0).unwrap(),
            )
        })
        .collect();
    TradingCalendar::from_days(days)
}

/// Answers the trailing-window request for dividends, interest, and fees with an empty list.
///
/// One request rather than one per type: the sync asks the plural endpoint for the whole family at
/// once. Anchored so it cannot also swallow the per-type requests the other mocks answer.
async fn mock_no_return_activities(server: &mut mockito::ServerGuard) {
    server
        .mock(
            "GET",
            mockito::Matcher::Regex(r"^/v2/account/activities(\?|$)".into()),
        )
        .with_status(200)
        .with_body("[]")
        .expect_at_least(1)
        .create_async()
        .await;
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

    let close_history = bars::load_aligned_closes(
        &pool,
        BarInterval::OneDay,
        60,
        &SplitTable::default(),
        &BoundaryTable::default(),
        SessionDate::at(Utc::now()),
    )
    .await
    .expect("history must load");
    assert_eq!(close_history.len(), 2, "both legs need aligned history");

    // Prices that push the spread past the entry threshold but not past the cap. Which leg is
    // stretched decides which becomes the short, so both orderings are quoted and the screen picks.
    // 1.2% rather than 50%: the seeded series is near-deterministic, so a 50% dislocation scores a
    // z in the hundreds, which the screen now refuses as a data-quality artifact.
    // `AAAA` is quoted as well as traded so the accepted-midpoint path is exercised and the
    // assertion on the recorded book is not vacuous. The book straddles the trade, so prices hold.
    let long_price = last_close(&close_history, "AAAA");
    let snapshot_body = serde_json::json!({
        "AAAA": {
            "latestTrade": { "t": session_instant().to_rfc3339(), "p": long_price },
            "latestQuote": {
                "t": session_instant().to_rfc3339(),
                "bp": long_price * 0.9995,
                "ap": long_price * 1.0005,
                "bs": 10,
                "as": 12,
            },
        },
        "BBBB": { "latestTrade": { "t": session_instant().to_rfc3339(), "p": last_close(&close_history, "BBBB") * 1.012 } },
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
    let session_log = session_log("test-a-pass-opens-a-pair-and-records-it");
    let dispatched_correlation_id = uuid::Uuid::new_v4();
    let context = EvaluationContext {
        prices_adjustable: true,
        pool: &pool,
        trading: &trading,
        market_data: &market_data,
        calendar: &calendar,
        universe: &universe,
        close_history: &close_history,
        sizing: SizingParameters::default(),
        execution: settings(),
        session_log: &session_log,
        correlation_id: dispatched_correlation_id,
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

    // The pass is only useful to a replay if the observation reached the disk. One evaluation
    // record, and one submission and one resolution per leg, all threaded by the pass's identifier.
    let records = recorded(&session_log);
    let passes = of_type(&records, "pass_evaluated");
    assert_eq!(passes.len(), 1, "one record per pass");
    let pass = &passes[0];
    let correlation_id = pass["correlation_id"]
        .as_str()
        .expect("a pass is correlated");
    assert_eq!(
        correlation_id,
        dispatched_correlation_id.to_string(),
        "the pass records the identifier the dispatcher supplied, not one of its own"
    );
    assert!(
        pass["payload"]["failure"].is_null(),
        "a pass that completed records no failure"
    );

    assert_eq!(pass["payload"]["candidates"].as_array().unwrap().len(), 1);
    let candidate = &pass["payload"]["candidates"][0];
    assert_eq!(candidate["decision"], "opened");
    // Sizing is recorded on the candidate, not only on the orders that went out, so a pair the risk
    // gate refuses is still answerable in the dimension that caused the refusal.
    assert!(
        candidate["long_notional"].is_number()
            && candidate["short_shares"].is_number()
            && candidate["gross_exposure"].is_number(),
        "a candidate that reached the sizer carries what it was sized to"
    );

    // Prices are their own records, one per fetch, sharing the pass's identifier.
    let priced = of_type(&records, "prices_observed");
    assert!(!priced.is_empty(), "the prices the pass decided on");
    let readings: Vec<&serde_json::Value> = priced
        .iter()
        .flat_map(|record| record["payload"]["readings"].as_array().unwrap())
        .collect();
    assert!(!readings.is_empty());
    assert!(
        readings
            .iter()
            .all(|reading| reading["price"].is_number() && reading["price_source"].is_string()),
        "a price without its source cannot be compared across passes"
    );
    // Asserted before the check below, which is otherwise vacuously true the moment the fixture
    // stops quoting anything.
    assert!(
        readings
            .iter()
            .any(|reading| reading["price_source"] == "quote_midpoint"),
        "the fixture must exercise the guard's accepting path"
    );
    assert!(
        readings.iter().all(|reading| {
            reading["price_source"] != "quote_midpoint"
                || (reading["bid_price"].is_number()
                    && reading["ask_price"].is_number()
                    && reading["quote_timestamp"].is_string())
        }),
        "a midpoint must carry the book it was taken from, or the guard cannot be tuned"
    );
    // Nothing refuses a trade for being old, so the record is the only place to notice a stale one.
    assert!(
        readings
            .iter()
            .any(|reading| reading["price_source"] == "last_trade"),
        "the fixture must exercise the fallback path"
    );
    assert!(
        readings.iter().all(|reading| {
            reading["price_source"] != "last_trade" || reading["trade_timestamp"].is_string()
        }),
        "a fallback price must carry when it printed, or its staleness cannot be judged"
    );
    assert!(
        priced
            .iter()
            .all(|record| record["payload"]["purpose"].is_string()),
        "each fetch names what it was for"
    );

    // What the model offered the screen, as rows rather than a count. `expected_return` and
    // `confidence` are derived from the stored quantiles, so `equity_predictions` alone cannot
    // reconstruct what the screen actually consumed.
    let screened = of_type(&records, "universe_screened");
    assert_eq!(screened.len(), 1, "one funnel per pass");
    let screen_inputs = screened[0]["payload"]["inputs"]
        .as_array()
        .expect("screen inputs are recorded");
    assert_eq!(
        screen_inputs.len(),
        2,
        "both predictions reached the screen"
    );
    assert!(screen_inputs
        .iter()
        .all(|input| input["expected_return"].is_number()
            && input["confidence"].is_number()
            && input["is_shortable"].is_boolean()));
    assert!(
        screened[0]["payload"]["excluded"]
            .as_array()
            .expect("the funnel is recorded")
            .is_empty(),
        "nothing was filtered out in this fixture"
    );

    let submitted = of_type(&records, "order_submitted");
    let resolved = of_type(&records, "order_resolved");
    assert_eq!(submitted.len(), 2, "one submission per leg");
    assert_eq!(resolved.len(), 2, "every submission is resolved");
    for record in submitted.iter().chain(resolved.iter()) {
        assert_eq!(
            record["correlation_id"], correlation_id,
            "orders thread back to the pass that decided them"
        );
    }
    // The submission is keyed by an identifier chosen before the request was sent, which is what
    // makes an order recoverable if the process dies between the write and Alpaca's response.
    assert_eq!(
        submitted[0]["payload"]["client_order_id"],
        resolved[0]["payload"]["client_order_id"]
    );
    assert_eq!(resolved[0]["payload"]["outcome"], "filled");

    // The pair itself, with the rationale nothing outside this application knows: which long was
    // paired with which short, on what hedge ratio, at what entry score.
    let opened = of_type(&records, "pair_opened");
    assert_eq!(opened.len(), 1, "the pair that opened is recorded");
    assert_eq!(opened[0]["payload"]["pair_id"], "AAAA-BBBB");
    assert_eq!(opened[0]["payload"]["model_run_id"], "run-1");
    assert!(
        opened[0]["payload"]["hedge_ratio"].is_number()
            && opened[0]["payload"]["entry_z_score"].is_number()
            && opened[0]["payload"]["signal_strength"].is_number()
    );
    // The identifier the close and the attribution will join on.
    assert_eq!(
        opened[0]["payload"]["pair_uuid"],
        open[0].id().to_string(),
        "the record names the row it wrote"
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

    let close_history = bars::load_aligned_closes(
        &pool,
        BarInterval::OneDay,
        60,
        &SplitTable::default(),
        &BoundaryTable::default(),
        SessionDate::at(Utc::now()),
    )
    .await
    .expect("history must load");

    let snapshot_body = serde_json::json!({
        "AAAA": { "latestTrade": { "t": session_instant().to_rfc3339(), "p": last_close(&close_history, "AAAA") } },
        "BBBB": { "latestTrade": { "t": session_instant().to_rfc3339(), "p": last_close(&close_history, "BBBB") * 1.012 } },
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

    let session_log = session_log("test-a-pass-opens-nothing-once-shutdown-is-requested");
    let context = EvaluationContext {
        prices_adjustable: true,
        pool: &pool,
        trading: &trading,
        market_data: &market_data,
        calendar: &calendar,
        universe: &universe,
        close_history: &close_history,
        sizing: SizingParameters::default(),
        execution: settings(),
        session_log: &session_log,
        correlation_id: uuid::Uuid::new_v4(),
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
    let close_history = bars::load_aligned_closes(
        &pool,
        BarInterval::OneDay,
        60,
        &SplitTable::default(),
        &BoundaryTable::default(),
        SessionDate::at(Utc::now()),
    )
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
        "AAAA": { "latestTrade": { "t": session_instant().to_rfc3339(), "p": last_close(&close_history, "AAAA") } },
        "BBBB": { "latestTrade": { "t": session_instant().to_rfc3339(), "p": mean_reverting_short_price(&close_history) } },
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
    let session_log = session_log("test-a-pass-closes-a-converged-pair-from-a-full-book");
    let context = EvaluationContext {
        prices_adjustable: true,
        pool: &pool,
        trading: &trading,
        market_data: &market_data,
        calendar: &calendar,
        universe: &universe,
        close_history: &close_history,
        sizing,
        execution: settings(),
        session_log: &session_log,
        correlation_id: uuid::Uuid::new_v4(),
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

    // The exit half is recorded to the same standard as the entry half. Both legs of the pair that
    // closed, each carrying the order that will settle it — without these, realized profit and loss
    // is attributable in one direction only.
    let records = recorded(&session_log);
    let closes = of_type(&records, "position_close_requested");
    assert_eq!(closes.len(), 2, "one record per leg of the closed pair");
    for record in &closes {
        assert_eq!(record["payload"]["pair_id"], "AAAA-BBBB");
        assert_eq!(record["payload"]["reason"], "pair_exit");
        assert_eq!(record["payload"]["accepted"], true);
    }

    // And the reading that measured every open pair says so, whether or not the pair closed.
    let observed = of_type(&records, "open_pairs_observed");
    assert_eq!(observed.len(), 1, "one book reading per pass");
    let measured = observed[0]["payload"]["readings"]
        .as_array()
        .expect("open pairs are recorded");
    assert_eq!(
        measured.len(),
        4,
        "every open pair is measured, not just the one that closed"
    );
    let closed = measured
        .iter()
        .find(|reading| reading["pair_id"] == "AAAA-BBBB")
        .expect("the converged pair is among them");
    assert_eq!(closed["decision"], "convergence");
    assert!(
        closed["z_score"].is_number() && closed["spread_mean"].is_number(),
        "the inputs that produced the exit decision are recorded with it"
    );
}

/// A pass that dies partway through still says what it had measured.
///
/// The book was priced and read before the account call failed, so discarding the observation
/// would leave the log silent about the one pass worth reading. The failure is named on the record
/// rather than left as an absence, which is indistinguishable from a crashed process.
#[tokio::test]
#[serial]
async fn test_a_failed_pass_records_what_it_had_already_observed() {
    let pool = fresh_pool().await;
    let mut server = mockito::Server::new_async().await;

    common::seed_correlated_bars(&pool, &["AAAA", "BBBB"], SESSIONS).await;
    let close_history = bars::load_aligned_closes(
        &pool,
        BarInterval::OneDay,
        60,
        &SplitTable::default(),
        &BoundaryTable::default(),
        SessionDate::at(Utc::now()),
    )
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

    let _snapshots = server
        .mock(
            "GET",
            mockito::Matcher::Regex(r"^/v2/stocks/snapshots".into()),
        )
        .with_status(200)
        .with_body(format!(
            r#"{{"AAAA":{{"latestTrade":{{"t":"{traded_at}","p":100.0}}}},"BBBB":{{"latestTrade":{{"t":"{traded_at}","p":50.0}}}}}}"#,
            traded_at = session_instant().to_rfc3339()
        ))
        .create_async()
        .await;
    // The exits are done; the entry half asks for the account and Alpaca is unreachable.
    let _account = server
        .mock("GET", "/v2/account")
        .with_status(500)
        .with_body("upstream is down")
        .create_async()
        .await;

    let trading = TradingClient::with_base_url(credentials(), server.url());
    let market_data = MarketDataClient::with_base_url(credentials(), server.url(), DataFeed::Iex);
    let calendar = calendar_for_today();
    let universe = universe_of(&["AAAA", "BBBB"]);

    let running = CancellationToken::new();
    let session_log = session_log("test-a-failed-pass-records-what-it-had-already-observed");
    let context = EvaluationContext {
        prices_adjustable: true,
        pool: &pool,
        trading: &trading,
        market_data: &market_data,
        calendar: &calendar,
        universe: &universe,
        close_history: &close_history,
        sizing: SizingParameters::default(),
        execution: settings(),
        session_log: &session_log,
        correlation_id: uuid::Uuid::new_v4(),
        shutdown: &running,
        now: session_instant(),
    };

    evaluate::run_pass(&context)
        .await
        .expect_err("the account call must fail the pass");

    let records = recorded(&session_log);
    let passes = of_type(&records, "pass_evaluated");
    assert_eq!(passes.len(), 1, "a failed pass is still recorded");
    assert!(
        passes[0]["payload"]["failure"]
            .as_str()
            .expect("the failure is named")
            .contains("Alpaca"),
        "the record says what ended the pass"
    );
    assert_eq!(
        passes[0]["payload"]["open_pairs_at_start"], 1,
        "what the pass had measured before the failure survives"
    );
    assert_eq!(
        of_type(&records, "open_pairs_observed").len(),
        1,
        "the book reading was written before the failure and is unaffected by it"
    );
    assert!(
        of_type(&records, "universe_screened").is_empty(),
        "the pass never reached the screen"
    );
}

/// A pair with no price this pass is held, not closed and not crashed. The pre-close liquidation
/// closes it regardless, so the worst case is holding until 15:45 rather than exiting on a signal.
#[tokio::test]
#[serial]
async fn test_a_pair_that_cannot_be_priced_is_held_and_counted() {
    let pool = fresh_pool().await;
    let mut server = mockito::Server::new_async().await;

    common::seed_correlated_bars(&pool, &["AAAA", "BBBB"], SESSIONS).await;
    let close_history = bars::load_aligned_closes(
        &pool,
        BarInterval::OneDay,
        60,
        &SplitTable::default(),
        &BoundaryTable::default(),
        SessionDate::at(Utc::now()),
    )
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
    let session_log = session_log("test-a-pair-that-cannot-be-priced-is-held-and-counted");
    let context = EvaluationContext {
        prices_adjustable: true,
        pool: &pool,
        trading: &trading,
        market_data: &market_data,
        calendar: &calendar,
        universe: &universe,
        close_history: &close_history,
        sizing: SizingParameters::default(),
        execution: settings(),
        session_log: &session_log,
        correlation_id: uuid::Uuid::new_v4(),
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

/// A book refused with no last trade behind it is where the guard has its largest effect: the
/// symbol goes unpriced and neither half of the pass can use it. The refused book has to reach the
/// record, or the limits cannot be judged against the readings that cost the most.
#[tokio::test]
#[serial]
async fn test_a_refused_book_with_no_trade_records_what_was_refused() {
    let pool = fresh_pool().await;
    let mut server = mockito::Server::new_async().await;

    common::seed_correlated_bars(&pool, &["AAAA", "BBBB"], SESSIONS).await;
    let close_history = bars::load_aligned_closes(
        &pool,
        BarInterval::OneDay,
        60,
        &SplitTable::default(),
        &BoundaryTable::default(),
        SessionDate::at(Utc::now()),
    )
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

    // A book far too wide to price, and no trade to fall back to.
    let snapshot_body = serde_json::json!({
        "AAAA": {
            "latestQuote": {
                "t": session_instant().to_rfc3339(),
                "bp": 90.0, "ap": 110.0, "bs": 10, "as": 12,
            },
        },
        "BBBB": {},
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
    let session_log = session_log("test-a-refused-book-with-no-trade-records-what-was-refused");
    let context = EvaluationContext {
        prices_adjustable: true,
        pool: &pool,
        trading: &trading,
        market_data: &market_data,
        calendar: &calendar,
        universe: &universe,
        close_history: &close_history,
        sizing: SizingParameters::default(),
        execution: settings(),
        session_log: &session_log,
        correlation_id: uuid::Uuid::new_v4(),
        shutdown: &running,
        now: session_instant(),
    };

    let summary = evaluate::run_pass(&context)
        .await
        .expect("the pass must survive");
    assert_eq!(summary.pairs_unpriced, 1, "a refused book leaves no price");

    let records = recorded(&session_log);
    let unavailable: Vec<&serde_json::Value> = of_type(&records, "prices_observed")
        .iter()
        .flat_map(|record| record["payload"]["unavailable"].as_array().unwrap())
        .collect();
    let refused = unavailable
        .iter()
        .find(|entry| entry["ticker"] == "AAAA")
        .expect("the refused symbol must be named");

    assert_eq!(refused["cause"], "quote_rejected");
    assert_eq!(refused["quote_rejection"], "wide_quote");
    assert_eq!(refused["bid_price"], 90.0);
    assert_eq!(refused["ask_price"], 110.0);
    assert!(refused["quote_timestamp"].is_string());

    let absent = unavailable
        .iter()
        .find(|entry| entry["ticker"] == "BBBB")
        .expect("a symbol with nothing at all is still named");
    assert_eq!(absent["cause"], "no_quote");
    assert!(
        absent["bid_price"].is_null(),
        "no book means no book, not a defaulted one"
    );
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
    let log = session_log("test-liquidation-flattens-the-book-and-marks-every-pair");
    let summary =
        evaluate::run_liquidation(&pool, &trading, &log, uuid::Uuid::new_v4(), Utc::now())
            .await
            .expect("the liquidation must run");

    assert_eq!(summary.pairs_closed, 2);
    assert!(summary.pairs_still_open.is_empty());
    assert!(pairs::load_open_pairs(&pool).await.unwrap().is_empty());

    // The last fail-safe before positions carry overnight says it ran, and says the book is flat.
    let records = recorded(&log);
    let attempted = of_type(&records, "liquidation_attempted");
    assert_eq!(attempted.len(), 1);
    assert!(attempted[0]["payload"]["failure"].is_null());
    assert_eq!(attempted[0]["payload"]["pairs_closed"], 2);

    // The table is mutated in place; the log needs its own record of the transition.
    let closed = of_type(&records, "pair_closed");
    assert_eq!(closed.len(), 2, "one record per pair marked closed");
    for record in &closed {
        assert_eq!(record["payload"]["reason"], "end_of_day");
        assert_eq!(
            record["payload"]["updated"], true,
            "a pair that was open when the liquidation reached it"
        );
    }
}

/// A liquidation that could not reach the broker still records that it was attempted.
///
/// This is the last thing standing between an open book and an overnight position. A run that
/// failed at the bulk close used to leave no trace of having happened at all, which reads exactly
/// like a liquidation that was never scheduled.
#[tokio::test]
#[serial]
async fn test_a_failed_liquidation_records_the_attempt() {
    let pool = fresh_pool().await;
    let mut server = mockito::Server::new_async().await;

    let _bulk = server
        .mock("DELETE", mockito::Matcher::Regex(r"^/v2/positions".into()))
        .with_status(500)
        .with_body("upstream is down")
        .create_async()
        .await;

    let trading = TradingClient::with_base_url(credentials(), server.url());
    let log = session_log("test-a-failed-liquidation-records-the-attempt");
    evaluate::run_liquidation(&pool, &trading, &log, uuid::Uuid::new_v4(), Utc::now())
        .await
        .expect_err("the bulk close must fail");

    let records = recorded(&log);
    let attempted = of_type(&records, "liquidation_attempted");
    assert_eq!(attempted.len(), 1, "the attempt is recorded even so");
    assert!(
        attempted[0]["payload"]["failure"]
            .as_str()
            .expect("the failure is named")
            .contains("Alpaca"),
        "the record says what stopped it"
    );
    assert_eq!(
        attempted[0]["payload"]["pairs_closed"], 0,
        "nothing was flattened"
    );
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
    let summary = evaluate::run_liquidation(
        &pool,
        &trading,
        &session_log("test-a-refused-leg-leaves-its-pair-open"),
        uuid::Uuid::new_v4(),
        Utc::now(),
    )
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

    // A fixed instant on the wrong side of UTC midnight: 00:30Z on 4 August is 20:30 on 3 August in
    // New York. The session is therefore 2026-08-03, and the activities request below asserts that
    // date — so a regression sending the UTC date instead of `session_date.date()` fails here
    // rather than only during the four hours a day when the two disagree.
    let session_date = SessionDate::at(
        chrono::DateTime::parse_from_rfc3339("2026-08-04T00:30:00Z")
            .unwrap()
            .with_timezone(&Utc),
    );
    assert_eq!(session_date.to_string(), "2026-08-03");

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
        // The path prefix alone would match a request carrying any date. Alpaca is asked for one
        // session, and which session that is comes from the Eastern derivation under test.
        .match_query(mockito::Matcher::UrlEncoded(
            "date".into(),
            "2026-08-03".into(),
        ))
        .with_status(200)
        .with_body(format!(
            r#"[{{"id":"a1","activity_type":"FILL","transaction_time":"{}",
                  "symbol":"AAAA","side":"buy","qty":"10","price":"100","order_id":"o1"}},
                {{"id":"a2","activity_type":"FILL","transaction_time":"{}",
                  "symbol":"AAAA","side":"sell","qty":"10","price":"110","order_id":"o2"}}]"#,
            (opened + Duration::minutes(1)).to_rfc3339(),
            (closed - Duration::minutes(1)).to_rfc3339(),
        ))
        .create_async()
        .await;

    mock_no_return_activities(&mut server).await;

    let trading = TradingClient::with_base_url(credentials(), server.url());
    let log = session_log("test-the-account-sync-stores-and-attributes-a-session");
    let summary = account::sync_account(
        &pool,
        &trading,
        &log,
        uuid::Uuid::new_v4(),
        &calendar_ending_at(session_date),
        session_date,
    )
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

    // Every activity Alpaca reported, as our own record. `net_amount` in particular reaches no
    // other store the application owns.
    let records = recorded(&log);
    let observed = of_type(&records, "activity_observed");
    assert_eq!(observed.len(), 2, "one record per activity, fills included");
    assert!(observed
        .iter()
        .all(|record| record["payload"]["activity_id"].is_string()
            && record["payload"]["activity_type"].is_string()));
    assert!(
        observed
            .iter()
            .all(|record| record["payload"]["order_id"].is_string()),
        "every fill joins back to the order that produced it"
    );

    // The attribution is the one derived value the log keeps, recorded beside the pair it landed
    // on so the stored figure can be checked against the fills it came from.
    let attributed = of_type(&records, "pair_attributed");
    assert_eq!(attributed.len(), 1);
    assert_eq!(attributed[0]["payload"]["pair_uuid"], id.to_string());
    assert_eq!(attributed[0]["payload"]["realized_profit_and_loss"], 100.0);
    assert_eq!(attributed[0]["payload"]["updated"], true);
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

    mock_no_return_activities(&mut server).await;

    let trading = TradingClient::with_base_url(credentials(), server.url());
    let calendar = calendar_ending_at(session_date);
    let log = session_log("test-the-account-sync-is-idempotent-0");
    let first = account::sync_account(
        &pool,
        &trading,
        &log,
        uuid::Uuid::new_v4(),
        &calendar,
        session_date,
    )
    .await
    .unwrap();
    let log = session_log("test-the-account-sync-is-idempotent-1");
    let second = account::sync_account(
        &pool,
        &trading,
        &log,
        uuid::Uuid::new_v4(),
        &calendar,
        session_date,
    )
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

/// A deposit has to survive the round trip with its amount, because that amount is the only record
/// of how much arrived — and it must not be mistaken for a fill along the way. `attribute` counts
/// anything without a ticker as unattributed, so a transfer reaching it would raise a false alarm
/// on every session that received capital.
///
/// `CSD` is the type a live bank deposit books as, and the one branch the paper account cannot
/// exercise: paper funds by journal, so `CSD` has no coverage anywhere but here.
#[tokio::test]
#[serial]
async fn test_the_account_sync_stores_transfers_without_attributing_them() {
    use fund::portfolio::account;

    const DEPOSIT_ACTIVITY_TYPE: &str = "CSD";

    let pool = fresh_pool().await;
    let mut server = mockito::Server::new_async().await;
    let session_date = SessionDate::at(session_instant());

    let _account = server
        .mock("GET", "/v2/account")
        .with_status(200)
        .with_body(account_body(30_000))
        .create_async()
        .await;
    for activity_type in account::synced_activity_types() {
        server
            .mock(
                "GET",
                mockito::Matcher::Regex(format!("^/v2/account/activities/{activity_type}")),
            )
            .with_status(200)
            .with_body("[]")
            .create_async()
            .await;
    }
    // On the trailing window, which is the only request that can reach it: a transfer is dated and
    // created the morning after, so this session's own `date=` query returns the previous one's.
    let _deposit = server
        .mock("GET", "/v2/account/activities")
        .match_query(mockito::Matcher::AllOf(vec![
            // Pinned to literals, not to the production constants: building the expectation from
            // the list under test would move both sides together and assert nothing.
            mockito::Matcher::UrlEncoded(
                "activity_types".into(),
                "DIV,DIVCGL,DIVCGS,DIVFEE,DIVFT,DIVNRA,DIVROC,DIVTW,DIVTXEX,CGD,INT,INTNRA,INTTW,\
                 FEE,CSD,CSW,JNLC"
                    .into(),
            ),
            mockito::Matcher::UrlEncoded(
                "after".into(),
                (session_date.date() - Duration::days(7)).to_string(),
            ),
        ]))
        .with_status(200)
        .with_body(format!(
            r#"[{{"id":"deposit-1","activity_type":"{DEPOSIT_ACTIVITY_TYPE}","date":"{}",
                  "net_amount":"10000.00","status":"executed"}}]"#,
            session_date.date()
        ))
        .expect_at_least(1)
        .create_async()
        .await;

    let trading = TradingClient::with_base_url(credentials(), server.url());
    let summary = account::sync_account(
        &pool,
        &trading,
        &session_log("test-the-account-sync-stores-transfers-without-attributing-them"),
        uuid::Uuid::new_v4(),
        &calendar_ending_at(session_date),
        session_date,
    )
    .await
    .expect("the sync must run");

    assert_eq!(summary.activities_stored, 1);
    assert_eq!(
        summary.activities_unattributed, 0,
        "a transfer is not a fill and must never reach attribution"
    );

    let (activity_type, net_amount, stored_time): (
        String,
        rust_decimal::Decimal,
        chrono::DateTime<Utc>,
    ) = sqlx::query_as(
        "SELECT activity_type, net_amount, transaction_time
             FROM account_activities WHERE id = 'deposit-1'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();

    assert_eq!(activity_type, "CSD");
    assert_eq!(net_amount, rust_decimal::Decimal::new(1000000, 2));
    assert_eq!(
        SessionDate::at(stored_time),
        session_date,
        "a dated transfer must land in the session Alpaca dated it, not the one before"
    );
}

/// Nothing else notices a failed sync: the hole it leaves is silent until a return spanning it is
/// asked for, which may be months later.
#[tokio::test]
#[serial]
async fn test_the_account_sync_reports_a_missing_previous_session() {
    use fund::portfolio::account;

    let pool = fresh_pool().await;
    let mut server = mockito::Server::new_async().await;
    let session_date = SessionDate::at(session_instant());
    let calendar = calendar_ending_at(session_date);
    let previous = calendar
        .previous_trading_day(session_date)
        .expect("the fixture calendar reaches back");

    let _account = server
        .mock("GET", "/v2/account")
        .with_status(200)
        .with_body(account_body(100_000))
        .create_async()
        .await;
    for activity_type in account::synced_activity_types() {
        server
            .mock(
                "GET",
                mockito::Matcher::Regex(format!("^/v2/account/activities/{activity_type}")),
            )
            .with_status(200)
            .with_body("[]")
            .create_async()
            .await;
    }
    mock_no_return_activities(&mut server).await;

    let trading = TradingClient::with_base_url(credentials(), server.url());
    let log = session_log("test-the-account-sync-reports-a-missing-previous-session-2");
    let with_gap = account::sync_account(
        &pool,
        &trading,
        &log,
        uuid::Uuid::new_v4(),
        &calendar,
        session_date,
    )
    .await
    .expect("the sync must run");
    assert_eq!(
        with_gap.previous_session_gap,
        Some(previous),
        "the previous session has no snapshot and the sync must say so"
    );

    // Fill the hole and the same sync stops reporting it.
    account::sync_account(
        &pool,
        &trading,
        &log,
        uuid::Uuid::new_v4(),
        &calendar,
        previous,
    )
    .await
    .expect("the sync must run");
    let repaired = account::sync_account(
        &pool,
        &trading,
        &log,
        uuid::Uuid::new_v4(),
        &calendar,
        session_date,
    )
    .await
    .expect("the sync must run");
    assert_eq!(repaired.previous_session_gap, None);
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
/// Built from the **Eastern** date via `SessionDate::at`, not from `Utc::now().date_naive()`.
/// Between 20:00 Eastern and midnight the two name different days, and a fixture anchored to the
/// UTC date lands outside the session it is meant to sit inside — which is a test that fails for
/// four hours every evening and passes every time anyone looks at it during the day.
///
/// `SessionDate::bounds` returns UTC instants, so the offset below is from Eastern midnight.
fn session_instant() -> chrono::DateTime<Utc> {
    let today = SessionDate::at(Utc::now());
    let (start, _) = today.bounds();
    start + Duration::hours(11) // 11:00 Eastern, mid-session
}

/// A fee for an earlier session, which is the only way one can ever arrive.
///
/// Alpaca does not create a session's fees until roughly 00:15 UTC the following day, hours after
/// the 16:15 Eastern sync, so the trailing window is what picks them up. This asserts the fee is
/// stored under its own session, counted as a cost, and never reaches attribution — it has no
/// symbol, so no pair could claim it.
#[tokio::test]
#[serial]
async fn test_a_fee_from_an_earlier_session_is_stored_as_a_cost() {
    use fund::portfolio::account;

    let pool = fresh_pool().await;
    let mut server = mockito::Server::new_async().await;
    let session_date = SessionDate::at(session_instant());
    let earlier = session_date.plus_calendar_days(-1);

    let _account = server
        .mock("GET", "/v2/account")
        .with_status(200)
        .with_body(account_body(30_000))
        .create_async()
        .await;
    for activity_type in account::synced_activity_types() {
        server
            .mock(
                "GET",
                mockito::Matcher::Regex(format!("^/v2/account/activities/{activity_type}")),
            )
            .with_status(200)
            .with_body("[]")
            .create_async()
            .await;
    }
    let _fees = server
        .mock(
            "GET",
            mockito::Matcher::Regex(r"^/v2/account/activities(\?|$)".into()),
        )
        .with_status(200)
        .with_body(format!(
            r#"[{{"id":"fee-taf","activity_type":"FEE","activity_sub_type":"TAF","date":"{}",
                  "net_amount":"-0.07","status":"executed"}},
                {{"id":"fee-reg","activity_type":"FEE","activity_sub_type":"REG","date":"{}",
                  "net_amount":"-0.31","status":"executed"}}]"#,
            earlier.date(),
            earlier.date()
        ))
        .expect_at_least(1)
        .create_async()
        .await;

    let trading = TradingClient::with_base_url(credentials(), server.url());
    let log = session_log("test-a-fee-from-an-earlier-session-is-stored-as-a-cost");
    let summary = account::sync_account(
        &pool,
        &trading,
        &log,
        uuid::Uuid::new_v4(),
        &calendar_ending_at(session_date),
        session_date,
    )
    .await
    .expect("the sync must run");

    assert_eq!(summary.return_activities_stored, 2);
    assert_eq!(
        summary.return_activities_net,
        rust_decimal::Decimal::new(-38, 2),
        "the fee family nets to a drag on the session it fell in"
    );
    assert_eq!(
        summary.activities_unattributed, 0,
        "a fee has no symbol and must never reach attribution"
    );

    let stored_time: chrono::DateTime<Utc> =
        sqlx::query_scalar("SELECT transaction_time FROM account_activities WHERE id = 'fee-taf'")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(
        SessionDate::at(stored_time),
        earlier,
        "the fee belongs to the session that incurred it, not the one that fetched it"
    );

    // The window re-reads the same fees for another six days. Without the insert reporting which
    // rows were new, each re-read would append another observation for a fee already recorded.
    account::sync_account(
        &pool,
        &trading,
        &log,
        uuid::Uuid::new_v4(),
        &calendar_ending_at(session_date),
        session_date,
    )
    .await
    .expect("the second sync must run");

    let observed = recorded(&log);
    let fees: Vec<&serde_json::Value> = of_type(&observed, "activity_observed")
        .into_iter()
        .filter(|record| record["payload"]["activity_type"] == "FEE")
        .collect();
    assert_eq!(
        fees.len(),
        2,
        "an overlapping window must not record a fee it already recorded"
    );
}
