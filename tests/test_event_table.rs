//! The event table's statements run by a real `duckdb` over synthetic ticks laid out as scratch and the archive hold
//! them, so the draw, the read delay and the session's bounds are read off the rows written rather than off the SQL text.

use std::num::NonZeroU16;
use std::path::Path;
use std::process::Command;

use chrono::{NaiveDate, NaiveTime};
use fund::common::journal::RunId;
use fund::common::laboratory::event_table::{
    DRAW_ORDER_MACROS, Draws, EntryWindow, EventClock, EventTable, ReadDelay, draw_order, draw_seed,
};
use fund::common::laboratory::universe::{Universe, UniverseSize, universe};
use fund::common::market::aggregate::TradeTotals;
use fund::common::market::record::BarInterval;
use fund::common::market::security_details::{SecurityDetails, SecurityType};
use fund::common::market::trade_bars::{TradeBar, TradeSums};
use fund::common::market::{DollarVolume, Price, Shares, Symbol, TradeCount};
use fund::common::storage::{Key, Provider, ReferenceKey, ReferenceTable, TickProvider, TradesKey};
use fund::common::time::calendar::{TradingCalendar, TradingSession};
use fund::common::time::{SessionDate, SessionRange};
use fund::storage::provenance::{Provenance, Subscription};
use fund::storage::reference::{decode_details_snapshot, encode_security_details};
use proptest::prelude::*;
use uuid::Uuid;

fn date(month: u32, day: u32) -> SessionDate {
    SessionDate::from_date(NaiveDate::from_ymd_opt(2025, month, day).unwrap())
}

/// Every weekday from March to June 2025, trading 09:30 to 16:00 Eastern.
fn calendar() -> TradingCalendar {
    let at = |hour, minute| NaiveTime::from_hms_opt(hour, minute, 0).unwrap();
    let range = SessionRange::new(date(3, 1), date(6, 30)).unwrap();
    let sessions =
        std::iter::successors(Some(range.first()), |day| Some(day.plus_calendar_days(1)))
            .take_while(|day| *day <= range.last())
            .filter(|day| !day.is_weekend())
            .map(|day| TradingSession::new(day, at(9, 30), at(16, 0)).unwrap())
            .collect();
    TradingCalendar::new(sessions, range).unwrap()
}

/// AAA and BBB, two common stocks traded in May, as 2025-06-16's universe, described by a snapshot read back from
/// the archive's own file for 2025-06-13.
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
    let key = ReferenceKey::new(
        Provider::Massive,
        ReferenceTable::SecurityDetails,
        date(6, 13),
    );
    let provenance = Provenance::new(
        Subscription::StocksAdvanced,
        "2025-06-13T22:00:00Z".parse().unwrap(),
        RunId::new(Uuid::from_u128(1)),
        None,
    );
    let written = encode_security_details(&key, &details, &provenance).unwrap();
    let (snapshot, _) = decode_details_snapshot(&key, written).unwrap();
    universe(
        date(6, 16),
        &bars,
        &snapshot,
        |_, _| true,
        UniverseSize::new(NonZeroU16::new(10).unwrap()),
    )
    .unwrap()
}

/// Massive's daily bars of every weekday from March to the Friday before 2025-06-16.
fn daily_bars() -> Vec<TradesKey> {
    calendar()
        .trading_days_in_range(SessionRange::new(date(3, 3), date(6, 13)).unwrap())
        .into_iter()
        .map(|day| TradesKey::new(Provider::Massive, BarInterval::OneDay, day))
        .collect()
}

/// Quotes every 2 seconds and prints every 3, from 13:30:00 and 13:30:01 UTC, for AAA, BBB, SPY and QQQ over
/// 2025-06-16's regular hours, the mid rising a tenth of a cent a quote, and a daily close for each of `daily_bars`,
/// all under `directory` as scratch and the archive lay them out.
fn fixture(directory: &Path) -> String {
    let quotes = directory.join("scratch/kind=quotes/provider=massive/session=2025-06-16");
    let trades = directory.join("scratch/kind=trades/provider=massive/session=2025-06-16");
    for folder in [&quotes, &trades, &directory.join("table")] {
        std::fs::create_dir_all(folder).unwrap();
    }
    let mut script = format!(
        "SET TimeZone='UTC';
SET VARIABLE scratch = '{scratch}';
SET VARIABLE archive = '{archive}';
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
        archive = directory.display(),
        quotes = quotes.display(),
        trades = trades.display(),
    );
    for (index, key) in daily_bars().into_iter().enumerate() {
        let file = directory.join(Key::from(key).path());
        std::fs::create_dir_all(file.parent().unwrap()).unwrap();
        script.push_str(&format!(
            "COPY (SELECT ticker AS symbol, TIMESTAMPTZ '{} 20:00:00+00' AS closed_at, 100.0 + {index} AS close FROM names)
    TO '{}';\n",
            key.session(),
            file.display()
        ));
    }
    script
}

/// Runs `table`'s statements over the fixture, then `query`, and returns duckdb's output.
fn run(table: EventTable, query: &str) -> (String, String) {
    let directory = std::env::temp_dir().join(format!("fund-event-table-{}", Uuid::new_v4()));
    std::fs::create_dir_all(&directory).unwrap();
    let statements = table
        .statements(&calendar(), &named(), &daily_bars())
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
/// in the window; then the universe's count, how many of the checked columns exist, and the beta's day count.
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
SELECT round(min(previous_return), 4), max(beta_days) FROM 'table/rows_massive_2025-06-16.parquet';
",
    )
}

/// Three quote updates per name, each read half a second after it, so the quote in force at the read is the drawn
/// one; the universe file holds both names, the screen's columns are present while the grid's resting labels are
/// not, and the daily features read the Friday's return, 174 over 173, and sixty returns.
#[test]
fn test_the_quote_clock_reads_each_drawn_update_after_the_delay() {
    let (stdout, stderr) = built(EventClock::QuoteUpdates);
    assert_eq!(
        stdout, "quote,6,2,0.5,0.5,true,true,true\n2\n1\n57.8035,60\n",
        "{stderr}"
    );
}

/// The print clock draws prints instead, read against the quote in force half a second after each.
#[test]
fn test_the_print_clock_draws_prints() {
    let (stdout, stderr) = built(EventClock::Prints);
    let lines: Vec<&str> = stdout.lines().collect();
    assert_eq!(lines.len(), 4, "{stdout}{stderr}");
    let fields: Vec<&str> = lines[0].split(',').collect();
    assert_eq!(fields[..3], ["print", "6", "2"], "{stdout}");
    assert_eq!(fields[5..], ["true", "true", "true"], "{stdout}");
    assert_eq!(lines[1..], ["2", "1", "57.8035,60"], "{stdout}");
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

/// The SQL `draw_order` the table sorts by agrees with the Rust one on every pair drawn, the full 32-bit seed and
/// event numbers past 32 bits included.
#[test]
fn test_the_sql_draw_order_agrees_with_the_rust_one() {
    let pairs: Vec<(u32, u64)> = (0..200_u64)
        .map(|index| {
            let seed = u32::try_from(index * 21_474_836 % 4_294_967_296).unwrap();
            (seed, index * index * 1_000_003 + index)
        })
        .chain([(u32::MAX, u64::from(u32::MAX)), (0, 5_000_000_000)])
        .collect();
    let values = pairs
        .iter()
        .map(|(seed, event_number)| format!("({seed}, {event_number})"))
        .collect::<Vec<_>>()
        .join(", ");
    let script = format!(
        "{DRAW_ORDER_MACROS}SELECT draw_order(seed, event_number) FROM (VALUES {values}) AS pairs(seed, event_number);"
    );
    let output = Command::new("duckdb")
        .args(["-csv", "-noheader", "-c", &script])
        .output()
        .expect("duckdb is on the path, as devenv provides it");
    let read: Vec<String> = String::from_utf8_lossy(&output.stdout)
        .lines()
        .map(str::to_string)
        .collect();
    let expected: Vec<String> = pairs
        .iter()
        .map(|(seed, event_number)| draw_order(*seed, *event_number).to_string())
        .collect();
    assert_eq!(
        read.len(),
        202,
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(read, expected);
}

/// The event numbers, counted from one, of a name's events every `spacing` seconds from `first` seconds after the
/// open that fall in `window` and are read `delay` milliseconds later by one second before the 16:00 close.
fn eligible(first: u32, spacing: u32, count: u32, window: (u16, u16), delay: u32) -> Vec<u64> {
    let last_read = 23_399_000_u64;
    (0..count)
        .filter(|step| {
            let milliseconds = u64::from(first + spacing * step) * 1000;
            milliseconds >= u64::from(window.0) * 60_000
                && milliseconds < u64::from(window.1) * 60_000
                && milliseconds + u64::from(delay) <= last_read
        })
        .map(|step| u64::from(step) + 1)
        .collect()
}

/// The `draws` of `events` lowest in `raw`'s draw order, in event order, as the SQL lists them.
fn drawn(raw: &str, clock: EventClock, events: &[u64], draws: u16) -> String {
    let seed = draw_seed(&Symbol::new(raw).unwrap(), date(6, 16), clock);
    let mut ordered = events.to_vec();
    ordered.sort_by_key(|event_number| (draw_order(seed, *event_number), *event_number));
    ordered.truncate(usize::from(draws));
    ordered.sort_unstable();
    ordered
        .iter()
        .map(u64::to_string)
        .collect::<Vec<_>>()
        .join(" ")
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(12))]

    /// Whatever the window, delay and draws, each name keeps at most `draws` of its eligible events, those its seed
    /// orders first; every arrival lies the delay after its event inside the window, and no feature reads past its
    /// arrival or the close.
    #[test]
    fn test_every_draw_reads_its_event_after_the_delay_and_before_the_close(
        start in prop_oneof![0..390_u16, 385..390_u16],
        length in 1..30_u16,
        delay in 0..5000_u32,
        draws in 1..40_u16,
        prints in any::<bool>(),
    ) {
        let end = (start + length).min(390);
        let (clock, kind, first, spacing, count) = match prints {
            true => (EventClock::Prints, "print", 1, 3, 7800),
            false => (EventClock::QuoteUpdates, "quote", 0, 2, 11_700),
        };
        let table = EventTable::new(
            TickProvider::Massive,
            clock,
            ReadDelay::from_milliseconds(delay),
            Draws::new(NonZeroU16::new(draws).unwrap()),
            EntryWindow::new(start, end).unwrap(),
        );
        let open = "TIMESTAMP '2025-06-16 13:30:00'";
        let (stdout, stderr) = run(
            table,
            &format!(
                "
SELECT names.ticker, coalesce(string_agg(key::VARCHAR, ' ' ORDER BY key), ''),
    coalesce(bool_and(datediff('millisecond', {open} + to_seconds({first} + {spacing} * (key - 1)), arrival) = {delay}), true),
    coalesce(bool_and(kind = '{kind}' AND arrival <= TIMESTAMP '2025-06-16 19:59:59'
        AND arrival - INTERVAL {delay} MILLISECOND >= {open} + INTERVAL {start} MINUTE
        AND arrival - INTERVAL {delay} MILLISECOND < {open} + INTERVAL {end} MINUTE
        AND screen_quote_age >= 0 AND seconds_since_mid_change >= 0 AND minutes_to_close > 0), true)
FROM (VALUES ('AAA'), ('BBB')) AS names(ticker)
LEFT JOIN 'table/rows_massive_2025-06-16.parquet' rows USING (ticker)
GROUP BY ALL ORDER BY names.ticker;
"
            ),
        );
        let events = eligible(first, spacing, count, (start, end), delay);
        prop_assert_eq!(
            stdout,
            format!(
                "AAA,{},true,true\nBBB,{},true,true\n",
                drawn("AAA", clock, &events, draws),
                drawn("BBB", clock, &events, draws)
            ),
            "{}",
            stderr
        );
    }
}

/// The property's count agrees with windows counted by hand: the last minute's 29 quote updates read by the last
/// second, and the first minute's 20 prints.
#[test]
fn test_the_eligible_count_matches_a_window_counted_by_hand() {
    assert_eq!(eligible(0, 2, 11_700, (389, 390), 1500).len(), 29);
    assert_eq!(
        eligible(1, 3, 7800, (0, 1), 0),
        (1..=20).collect::<Vec<u64>>()
    );
}
