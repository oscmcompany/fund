//! The event table's statements run by a real `duckdb` over synthetic ticks laid out as scratch holds them, so the
//! draw, the read delay and the session's bounds are read off the rows written rather than off the SQL text.

use std::num::NonZeroU16;
use std::path::Path;
use std::process::Command;

use chrono::{NaiveDate, NaiveTime};
use fund::common::laboratory::event_table::{
    Draws, EntryWindow, EventClock, EventTable, ReadDelay, TickProvider,
};
use fund::common::laboratory::universe::{Universe, UniverseSize, universe};
use fund::common::market::aggregate::TradeTotals;
use fund::common::market::record::BarInterval;
use fund::common::market::security_details::{SecurityDetails, SecurityType};
use fund::common::market::trade_bars::{TradeBar, TradeSums};
use fund::common::market::{DollarVolume, Price, Shares, Symbol, TradeCount};
use fund::common::time::SessionDate;
use fund::common::time::calendar::TradingSession;

fn date(month: u32, day: u32) -> SessionDate {
    SessionDate::from_date(NaiveDate::from_ymd_opt(2025, month, day).unwrap())
}

/// AAA and BBB, two common stocks traded in May, as 2025-06-16's universe.
fn named() -> Universe {
    let bars = ["AAA", "BBB"].map(|raw| {
        let shares = Shares::whole(100).unwrap();
        TradeBar::new(
            Symbol::new(raw).unwrap(),
            BarInterval::OneDay,
            date(5, 2).regular_close(),
            TradeSums::new(
                TradeTotals::new(
                    TradeCount::new(1),
                    shares,
                    DollarVolume::of(Price::from_dollars(10.0).unwrap(), shares),
                ),
                None,
                None,
            ),
        )
        .unwrap()
    });
    let details = ["AAA", "BBB"].map(|raw| {
        SecurityDetails::new(
            Symbol::new(raw).unwrap(),
            Some(SecurityType::CommonStock),
            None,
            None,
            None,
            None,
            None,
            None,
        )
    });
    universe(
        date(6, 16),
        &bars,
        date(6, 13),
        &details,
        |_, _| true,
        UniverseSize::new(NonZeroU16::new(10).unwrap()),
    )
    .unwrap()
}

/// Quotes every 2 seconds and prints every 3 for AAA, BBB, SPY and QQQ over 2025-06-16's regular hours, the mid rising
/// a tenth of a cent a quote, and daily closes for March to May with June's month not yet written, all under
/// `directory` as scratch and the archive lay them out.
fn fixture(directory: &Path) -> String {
    let quotes = directory.join("scratch/kind=quotes/provider=massive/session=2025-06-16");
    let trades = directory.join("scratch/kind=trades/provider=massive/session=2025-06-16");
    for folder in [&quotes, &trades, &directory.join("table")] {
        std::fs::create_dir_all(folder).unwrap();
    }
    let mut script = format!(
        "SET TimeZone='UTC';
SET VARIABLE scratch = '{scratch}';
SET VARIABLE parsed = '{parsed}';
CREATE MACRO eligible(conditions, correction) AS true;
CREATE TEMP TABLE names AS SELECT * FROM (VALUES ('AAA'), ('BBB'), ('SPY'), ('QQQ')) AS names(ticker);
COPY (SELECT ticker, TIMESTAMP_NS '2025-06-16 13:30:00' + to_seconds(2 * step) AS sip_timestamp,
        100 + 0.001 * step - 0.01 AS bid_price, 100 + 0.001 * step + 0.01 AS ask_price,
        100.0 AS bid_size, 200.0 AS ask_size
    FROM names, range(11700) AS steps(step)) TO '{quotes}/data.parquet';
COPY (SELECT ticker, TIMESTAMP_NS '2025-06-16 13:30:01' + to_seconds(3 * step) AS sip_timestamp,
        100 + 0.0015 * step AS price, 100.0 AS size, '' AS conditions, NULL::VARCHAR AS correction,
        CASE WHEN step % 2 = 0 THEN '4' ELSE '11' END AS exchange
    FROM names, range(7800) AS steps(step)) TO '{trades}/data.parquet';
",
        scratch = directory.join("scratch").display(),
        parsed = directory.join("parsed").display(),
        quotes = quotes.display(),
        trades = trades.display(),
    );
    for month in 3..=5 {
        let folder = directory.join(format!(
            "parsed/trades/provider=massive/interval=one_day/year=2025/month={month:02}/day=10"
        ));
        std::fs::create_dir_all(&folder).unwrap();
        script.push_str(&format!(
            "COPY (SELECT ticker AS symbol, TIMESTAMP '2025-{month:02}-02 20:00:00' + to_days(day::INTEGER) AS closed_at,
        100.0 + day AS close FROM names, range(5) AS days(day)) TO '{}/data.parquet';\n",
            folder.display()
        ));
    }
    script
}

/// Runs `table`'s statements over the fixture, then `query`, and returns duckdb's output.
fn run(table: EventTable, query: &str) -> (String, String) {
    let directory = std::env::temp_dir().join(format!("fund-event-table-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&directory).unwrap();
    let at = |hour, minute| NaiveTime::from_hms_opt(hour, minute, 0).unwrap();
    let statements = table
        .statements(
            &TradingSession::new(date(6, 16), at(9, 30), at(16, 0)).unwrap(),
            &named(),
        )
        .unwrap();
    let script = format!("{}{statements}{query}", fixture(&directory));
    let output = Command::new("duckdb")
        .current_dir(&directory)
        .args(["-csv", "-noheader", "-c", &script])
        .output()
        .expect("duckdb is on the path, as devenv provides it");
    std::fs::remove_dir_all(&directory).unwrap();
    (
        String::from_utf8_lossy(&output.stdout).into_owned(),
        String::from_utf8_lossy(&output.stderr).into_owned(),
    )
}

/// Runs `clock`'s statements over the fixture, drawing from minutes 100 to 110, and returns duckdb's output: per kind
/// the rows, names, read delays seen at the quote, whether every one-minute target rose and whether every draw lies
/// in the window; then the universe's count and how many of the checked columns exist.
fn built(clock: EventClock) -> (String, String) {
    let table = EventTable::new(
        TickProvider::Massive,
        clock,
        ReadDelay::from_milliseconds(500),
        Draws::new(NonZeroU16::new(3).unwrap()),
        EntryWindow::new(100, 110).unwrap(),
    );
    run(
        table,
        "
SELECT kind, count(*), count(DISTINCT ticker), min(screen_quote_age), max(screen_quote_age), bool_and(raw_1 > 0),
    min(minutes_since_open) >= 100, max(minutes_since_open) < 110.01
FROM 'table/rows_massive_2025-06-16.parquet' GROUP BY kind;
SELECT count(*) FROM 'table/universe_2025-06-16.parquet';
SELECT count(*) FROM (DESCRIBE SELECT * FROM 'table/rows_massive_2025-06-16.parquet')
WHERE column_name LIKE 'forward_buy%' OR column_name = 'screen_trailing_spread';
",
    )
}

/// Three quote updates per name, each read half a second after it, so the quote in force at the read is the drawn
/// one; the universe file holds both names, and the screen's columns are present while the grid's resting labels are
/// not.
#[test]
fn test_the_quote_clock_reads_each_drawn_update_after_the_delay() {
    let (stdout, stderr) = built(EventClock::QuoteUpdates);
    assert_eq!(
        stdout, "quote,6,2,0.5,0.5,true,true,true\n2\n1\n",
        "{stderr}"
    );
}

/// The print clock draws prints instead, read against the quote in force half a second after each.
#[test]
fn test_the_print_clock_draws_prints() {
    let (stdout, stderr) = built(EventClock::Prints);
    let lines: Vec<&str> = stdout.lines().collect();
    assert_eq!(lines.len(), 3, "{stdout}{stderr}");
    let fields: Vec<&str> = lines[0].split(',').collect();
    assert_eq!(fields[..3], ["print", "6", "2"], "{stdout}");
    assert_eq!(fields[5..], ["true", "true", "true"], "{stdout}");
    assert_eq!(lines[1..], ["2", "1"], "{stdout}");
}

/// A window ending at the close draws only events whose delayed read lands by the last second, so the update at
/// 19:59:58 UTC, read 1.5 seconds later, is left out and each name keeps its other 29 updates of the last minute, every
/// one with a quote in force.
#[test]
fn test_a_delayed_read_past_the_close_is_not_drawn() {
    let table = EventTable::new(
        TickProvider::Massive,
        EventClock::QuoteUpdates,
        ReadDelay::from_milliseconds(1500),
        Draws::new(NonZeroU16::new(100).unwrap()),
        EntryWindow::new(389, 390).unwrap(),
    );
    let (stdout, stderr) = run(
        table,
        "
SELECT count(*), count(screen_bid), max(arrival), min(minutes_to_close) > 0
FROM 'table/rows_massive_2025-06-16.parquet';
",
    );
    assert_eq!(stdout, "58,58,2025-06-16 19:59:57.5,true\n", "{stderr}");
}
