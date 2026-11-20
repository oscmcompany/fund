//! The event table's statements run by a real `duckdb` over synthetic ticks laid out as scratch and the archive hold
//! them, so the draw, the read delay, the session's bounds and what a print may set are read off the rows written.

use std::collections::BTreeMap;
use std::num::{NonZeroU16, NonZeroU32};
use std::path::Path;
use std::process::Command;

use chrono::{NaiveDate, NaiveTime};
use fund::common::book::Side;
use fund::common::cost::FillBound;
use fund::common::journal::RunId;
use fund::common::laboratory::event_table::{
    DRAW_ORDER_MACROS, Draws, EntryWindow, EventClock, EventTable, EventTableRefusal,
    ForwardHorizons, ReadDelay, RestingWindow, draw_order, draw_seed,
};
use fund::common::laboratory::universe::{Universe, UniverseSize, universe};
use fund::common::market::aggregate::TradeTotals;
use fund::common::market::record::{BarInterval, Partition};
use fund::common::market::security_details::{SecurityDetails, SecurityType};
use fund::common::market::trade_bars::{
    Condition, ConditionCode, ConditionLetter, ConditionStatus, HighLow, OpenClose, TradeBar,
    TradeConditions, TradeSums, UpdateRules,
};
use fund::common::market::{DollarVolume, Price, Shares, StampedPrice, Symbol, TradeCount};
use fund::common::storage::{Key, Provider, ReferenceKey, ReferenceTable, TickProvider, TradesKey};
use fund::common::time::calendar::{TradingCalendar, TradingSession};
use fund::common::time::{SessionDate, SessionRange};
use fund::storage::provenance::{Provenance, Subscription};
use fund::storage::reference::{
    decode_details_snapshot, encode_conditions, encode_security_details,
};
use fund::storage::trade_bars;
use proptest::prelude::*;
use strum::IntoEnumIterator;
use uuid::Uuid;

fn date(month: u32, day: u32) -> SessionDate {
    SessionDate::from_date(NaiveDate::from_ymd_opt(2025, month, day).unwrap())
}

/// The conditions snapshot every run reads, taken the Friday before the session.
const CONDITIONS_AS_OF: (u32, u32) = (6, 13);

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

fn provenance(subscription: Subscription) -> Provenance {
    Provenance::new(
        subscription,
        "2025-06-13T22:00:00Z".parse().unwrap(),
        RunId::new(Uuid::from_u128(1)),
        None,
    )
}

fn subscription(provider: TickProvider) -> Subscription {
    match provider {
        TickProvider::Massive => Subscription::StocksAdvanced,
        TickProvider::Alpaca => Subscription::AlgoTraderPlus,
    }
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
    let written =
        encode_security_details(&key, &details, &provenance(Subscription::StocksAdvanced)).unwrap();
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

/// `provider`'s daily bars of every weekday from its first to the Friday before 2025-06-16: Massive's from March,
/// Alpaca's from mid-April.
fn daily_bars(provider: TickProvider) -> Vec<TradesKey> {
    let first = match provider {
        TickProvider::Massive => date(3, 3),
        TickProvider::Alpaca => date(4, 14),
    };
    calendar()
        .trading_days_in_range(SessionRange::new(first, date(6, 13)).unwrap())
        .into_iter()
        .map(|day| TradesKey::new(provider.provider(), BarInterval::OneDay, day))
        .collect()
}

/// The days Alpaca's daily files lack SPY, one inside the lookback and the session before.
const SPY_ABSENT: [(u32, u32); 2] = [(5, 14), (6, 13)];

/// Each of `daily_bars(provider)` as the archive's writer encodes it, every name closing at 100 plus the day's index
/// at 16:00 Eastern, and Alpaca's without SPY on `SPY_ABSENT`.
fn write_daily_bars(directory: &Path, provider: TickProvider) {
    for (index, key) in daily_bars(provider).into_iter().enumerate() {
        let day = key.session();
        let absent = SPY_ABSENT.map(|(month, day)| date(month, day));
        let bars: Vec<TradeBar> = ["AAA", "BBB", "SPY", "QQQ"]
            .into_iter()
            .filter(|raw| match provider {
                TickProvider::Massive => true,
                TickProvider::Alpaca => !(*raw == "SPY" && absent.contains(&day)),
            })
            .map(|raw| {
                let close = Price::from_dollars(100.0 + index as f64).unwrap();
                let shares = Shares::whole(1000).unwrap();
                let stamped = StampedPrice::new(day.regular_close(), close);
                TradeBar::new(
                    Symbol::new(raw).unwrap(),
                    BarInterval::OneDay,
                    day.regular_close(),
                    TradeSums::new(
                        TradeTotals::new(
                            TradeCount::new(10),
                            shares,
                            DollarVolume::of(close, shares),
                        ),
                        Some(OpenClose::new(stamped, stamped).unwrap()),
                        Some(HighLow::new(close, close).unwrap()),
                    ),
                )
                .unwrap()
            })
            .collect();
        let written = trade_bars::encode(
            &key,
            &Partition::try_from(bars).unwrap(),
            &provenance(subscription(provider)),
        )
        .unwrap();
        let file = directory.join(Key::from(key).path());
        std::fs::create_dir_all(file.parent().unwrap()).unwrap();
        std::fs::write(file, written).unwrap();
    }
}

/// A conditions snapshot as the archive's writer encodes it, modeled on Massive's: F a sweep that sets prices, I an
/// odd lot that does not beside a retired I that did, T an extended-hours print, X a current letter whose retired twin
/// disagrees, and Z two current conditions that disagree.
fn write_conditions(directory: &Path) {
    let rules = |volume, high_low, open_close| UpdateRules {
        volume,
        high_low,
        open_close,
    };
    let letter = |spelled: &str| Some(ConditionLetter::new(spelled).unwrap());
    let conditions: BTreeMap<ConditionCode, Condition> = [
        (
            14,
            rules(true, true, true),
            letter("F"),
            letter("F"),
            ConditionStatus::Current,
        ),
        (
            37,
            rules(true, false, false),
            letter("I"),
            letter("I"),
            ConditionStatus::Current,
        ),
        (
            6,
            rules(true, true, true),
            letter("I"),
            None,
            ConditionStatus::Retired,
        ),
        (
            12,
            rules(true, false, false),
            letter("T"),
            letter("T"),
            ConditionStatus::Current,
        ),
        (
            9,
            rules(true, true, true),
            letter("X"),
            letter("X"),
            ConditionStatus::Current,
        ),
        (
            61,
            rules(true, false, false),
            letter("X"),
            None,
            ConditionStatus::Retired,
        ),
        (
            32,
            rules(true, true, false),
            letter("Z"),
            letter("Z"),
            ConditionStatus::Current,
        ),
        (
            60,
            rules(true, true, true),
            letter("Z"),
            None,
            ConditionStatus::Current,
        ),
    ]
    .into_iter()
    .map(|(code, rules, consolidated, unlisted, status)| {
        (
            ConditionCode::new(code),
            Condition::new(rules, consolidated, unlisted, status),
        )
    })
    .collect();
    let (month, day) = CONDITIONS_AS_OF;
    let key = ReferenceKey::new(
        Provider::Massive,
        ReferenceTable::Conditions,
        date(month, day),
    );
    let written = encode_conditions(
        &key,
        &TradeConditions::new(conditions),
        &provenance(Subscription::StocksAdvanced),
    )
    .unwrap();
    let file = directory.join(Key::from(key).path());
    std::fs::create_dir_all(file.parent().unwrap()).unwrap();
    std::fs::write(file, written).unwrap();
}

/// Whether AAA also prints the probes of what may set the high, at 13:50 UTC, or `DIPS` at 13:40 UTC, either shifting
/// its later print numbers, or BBB quotes nothing before 13:35 UTC.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Probes {
    Without,
    With,
    Dips,
    Unquoted,
}

/// AAA's prints below the grid's, as milliseconds after the open, price in ten-thousandths of a dollar, and whether
/// it may fill: an odd lot that may not, a withdrawn print, a print through every bid, and one at the bid in force
/// for the two seconds before it, so whatever the delay one drawn row rests at that bid.
const DIPS: [(i64, i64, Dip); 4] = [
    (600_500, 990_000, Dip::Ineligible),
    (620_500, 990_000, Dip::Withdrawn),
    (640_500, 990_000, Dip::Eligible),
    (651_999, 1_003_150, Dip::Eligible),
];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Dip {
    Eligible,
    Ineligible,
    Withdrawn,
}

/// Quotes every 2 seconds and prints every 3, from 13:30:00 and 13:30:01 UTC, for AAA, BBB, SPY and QQQ over
/// 2025-06-16's regular hours in `provider`'s cleaned spelling, the mid rising a tenth of a cent a quote, with the
/// conditions snapshot and `provider`'s daily bars, all under `directory` as scratch and the archive lay them out.
fn fixture(directory: &Path, provider: TickProvider, probes: Probes) -> String {
    let quotes = directory.join(format!(
        "scratch/kind=quotes/provider={provider}/session=2025-06-16"
    ));
    let trades = directory.join(format!(
        "scratch/kind=trades/provider={provider}/session=2025-06-16"
    ));
    for folder in [&quotes, &trades, &directory.join("table")] {
        std::fs::create_dir_all(folder).unwrap();
    }
    write_conditions(directory);
    write_daily_bars(directory, provider);
    // Each probe: seconds after 13:50:00, price, and how the provider spells its conditions and correction.
    let (quote_columns, grid_trade_columns, probe_trades) = match provider {
        TickProvider::Massive => (
            "TIMESTAMP_NS '2025-06-16 13:30:00' + to_seconds(2 * step) AS sip_timestamp,
        100 + 0.001 * step - 0.01 AS bid_price, 100 + 0.001 * step + 0.01 AS ask_price,
        100.0 AS bid_size, 200.0 AS ask_size",
            "TIMESTAMP_NS '2025-06-16 13:30:01' + to_seconds(3 * step) AS sip_timestamp,
        100 + 0.0015 * step AS price, 100.0 AS size, '' AS conditions, NULL::VARCHAR AS correction,
        CASE WHEN step % 2 = 0 THEN '4' ELSE '11' END AS exchange",
            "SELECT 'AAA', TIMESTAMP_NS '2025-06-16 13:50:00' + to_seconds(seconds), price, 100.0, conditions, correction, '11'
    FROM (VALUES (0.5, 130, '14', NULL), (1.5, 135, '6', NULL), (2.5, 150, '37', NULL), (3.5, 160, '14', '1'),
        (4.5, 170, '14,99', NULL), (5.5, 180, '', '10')) AS probes(seconds, price, conditions, correction)",
        ),
        TickProvider::Alpaca => (
            "TIMESTAMP_NS '2025-06-16 13:30:00' + to_seconds(2 * step) AS timestamp,
        (100 + 0.001 * step - 0.01)::DECIMAL(18, 6) AS bid_price, 100::DECIMAL(20, 6) AS bid_size,
        (100 + 0.001 * step + 0.01)::DECIMAL(18, 6) AS ask_price, 200::DECIMAL(20, 6) AS ask_size,
        'Q' AS bid_exchange, 'P' AS ask_exchange, ['R'] AS conditions, 'A' AS tape",
            "TIMESTAMP_NS '2025-06-16 13:30:01' + to_seconds(3 * step) AS timestamp,
        (100 + 0.0015 * step)::DECIMAL(18, 6) AS price, 100::DECIMAL(20, 6) AS size,
        CASE step % 3 WHEN 0 THEN [' '] WHEN 1 THEN [' ', 'F'] ELSE ['@'] END AS conditions,
        CASE WHEN step % 2 = 0 THEN 'D' ELSE 'N' END AS exchange,
        CASE step % 3 WHEN 0 THEN 'A' WHEN 1 THEN 'B' ELSE 'C' END AS tape,
        step::UBIGINT AS id, NULL::VARCHAR AS \"update\"",
            "SELECT 'AAA', TIMESTAMP_NS '2025-06-16 13:50:00' + to_seconds(seconds), price::DECIMAL(18, 6),
        100::DECIMAL(20, 6), conditions, 'N', tape, (100000 + price)::UBIGINT, \"update\"
    FROM (VALUES (0.5, 130, [' ', 'X'], 'A', NULL), (1.5, 135, [' '], 'B', 'incorrect'), (2.5, 140, []::VARCHAR[], 'C', NULL),
        (3.5, 150, [' ', 'I'], 'A', NULL), (4.5, 160, [' '], 'B', 'canceled'), (5.5, 170, [' ', 'Z'], 'A', NULL),
        (6.5, 175, ['@', 'T'], 'C', NULL), (7.5, 180, [' ', 'F'], 'B', 'corrected'), (8.5, 145, ['@', 'Q'], 'C', NULL),
        (9.5, 190, [' ', NULL], 'A', NULL))
        AS probes(seconds, price, conditions, tape, \"update\")",
        ),
    };
    let dips = DIPS
        .iter()
        .enumerate()
        .map(|(index, (milliseconds, price, dip))| {
            let (time, price) = (
                format!("TIMESTAMP_NS '2025-06-16 13:30:00' + INTERVAL {milliseconds} MILLISECOND"),
                format!("{}.{:04}", price / 10_000, price % 10_000),
            );
            match (provider, dip) {
                (TickProvider::Massive, Dip::Eligible) => format!("('AAA', {time}, {price}, 100.0, '', NULL, '11')"),
                (TickProvider::Massive, Dip::Ineligible) => format!("('AAA', {time}, {price}, 100.0, '37', NULL, '11')"),
                (TickProvider::Massive, Dip::Withdrawn) => format!("('AAA', {time}, {price}, 100.0, '14', '1', '11')"),
                (TickProvider::Alpaca, Dip::Eligible) => format!(
                    "('AAA', {time}, {price}::DECIMAL(18, 6), 100::DECIMAL(20, 6), [' '], 'N', 'A', {}::UBIGINT, NULL)",
                    200_000 + index
                ),
                (TickProvider::Alpaca, Dip::Ineligible) => format!(
                    "('AAA', {time}, {price}::DECIMAL(18, 6), 100::DECIMAL(20, 6), [' ', 'I'], 'N', 'A', {}::UBIGINT, NULL)",
                    200_000 + index
                ),
                (TickProvider::Alpaca, Dip::Withdrawn) => format!(
                    "('AAA', {time}, {price}::DECIMAL(18, 6), 100::DECIMAL(20, 6), [' '], 'N', 'B', {}::UBIGINT, 'canceled')",
                    200_000 + index
                ),
            }
        })
        .collect::<Vec<_>>()
        .join(", ");
    let probe_trades = match probes {
        Probes::Without => String::new(),
        Probes::With => format!("UNION ALL {probe_trades}"),
        Probes::Dips => format!("UNION ALL SELECT * FROM (VALUES {dips})"),
        Probes::Unquoted => String::new(),
    };
    let unquoted = match probes {
        Probes::Without | Probes::With | Probes::Dips => "",
        Probes::Unquoted => "WHERE NOT (ticker = 'BBB' AND step < 150)",
    };
    format!(
        "SET TimeZone='UTC';
SET VARIABLE scratch = '{scratch}';
SET VARIABLE archive = '{archive}';
CREATE TEMP TABLE names AS SELECT * FROM (VALUES ('AAA'), ('BBB'), ('SPY'), ('QQQ')) AS names(ticker);
COPY (SELECT ticker, {quote_columns} FROM names, range(11700) AS steps(step) {unquoted}) TO '{quotes}/data.parquet';
COPY (SELECT ticker, {grid_trade_columns} FROM names, range(7800) AS steps(step) {probe_trades})
    TO '{trades}/data.parquet';
",
        scratch = directory.join("scratch").display(),
        archive = directory.display(),
        quotes = quotes.display(),
        trades = trades.display(),
    )
}

/// Runs `table`'s statements over `provider`'s fixture, then `query`, and returns duckdb's output.
fn run(table: EventTable, provider: TickProvider, probes: Probes, query: &str) -> (String, String) {
    let directory = std::env::temp_dir().join(format!("fund-event-table-{}", Uuid::new_v4()));
    std::fs::create_dir_all(&directory).unwrap();
    let (month, day) = CONDITIONS_AS_OF;
    let statements = table
        .statements(
            &calendar(),
            &named(),
            &daily_bars(provider),
            date(month, day),
        )
        .unwrap();
    let script = format!(
        "{}{statements}{query}",
        fixture(&directory, provider, probes)
    );
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

/// Runs `clock`'s statements over the Massive fixture, drawing from minutes 100 to 110, and returns duckdb's output:
/// per kind the rows, names, read delays seen at the quote, whether every one-minute target rose and whether every
/// draw lies in the window; then the universe's count, how many of the checked columns exist, and the daily features.
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
        TickProvider::Massive,
        Probes::Without,
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
/// one; the universe file holds both names, the screen's column and the buy side's six resting labels are present,
/// and the daily features read the Friday's return, 174 over 173, and sixty returns.
#[test]
fn test_the_quote_clock_reads_each_drawn_update_after_the_delay() {
    let (stdout, stderr) = built(EventClock::QuoteUpdates);
    assert_eq!(
        stdout, "quote,6,2,0.5,0.5,true,true,true\n2\n7\n57.8035,60\n",
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
    assert_eq!(lines[1..], ["2", "7", "57.8035,60"], "{stdout}");
}

/// Alpaca's ticks and daily bars, in Alpaca's spelling and with no Massive file on disk, build the table: the
/// lookback runs from Alpaca's first bar, 45 closes, and the Friday's return, 144 over 143, is read although SPY's
/// close of that day is absent; beta reads only the 41 days whose returns span the same two closes as SPY's, so SPY's
/// hole on 2025-05-14 drops that day and the next.
#[test]
fn test_alpaca_ticks_and_daily_bars_build_the_table() {
    let table = EventTable::new(
        TickProvider::Alpaca,
        EventClock::QuoteUpdates,
        ReadDelay::from_milliseconds(500),
        Draws::new(NonZeroU16::new(3).unwrap()),
        EntryWindow::new(100, 110).unwrap(),
    );
    let (stdout, stderr) = run(
        table,
        TickProvider::Alpaca,
        Probes::Without,
        "
SELECT kind, count(*), count(DISTINCT ticker), min(screen_quote_age), max(screen_quote_age), bool_and(raw_1 > 0),
    round(min(previous_return), 4), round(max(previous_return), 4), min(beta_days), max(beta_days),
    count(daily_volatility_20), round(min(beta), 6), round(max(beta), 6)
FROM 'table/rows_alpaca_2025-06-16.parquet' GROUP BY kind;
",
    );
    assert_eq!(
        stdout, "quote,6,2,0.5,0.5,true,69.9301,69.9301,41,41,6,1.0,1.0\n",
        "{stderr}"
    );
}

/// Alpaca's letters decide per tape what may set the high: a regular sale with no letter or a current letter whose
/// retired twin disagrees sets it, as does the record replacing a correction; an odd lot, an ambiguous, unknown or
/// missing letter and an extended-hours print do not; a corrected or canceled print is left out entirely.
#[test]
fn test_alpaca_letters_decide_what_sets_the_high() {
    let table = EventTable::new(
        TickProvider::Alpaca,
        EventClock::Prints,
        ReadDelay::from_milliseconds(500),
        Draws::new(NonZeroU16::new(3).unwrap()),
        EntryWindow::new(100, 110).unwrap(),
    );
    let (stdout, stderr) = run(
        table,
        TickProvider::Alpaca,
        Probes::With,
        "
SELECT string_agg(price::INTEGER || ':' || eligible, ' ' ORDER BY timestamp) FROM signed
WHERE ticker = 'AAA' AND price >= 120;
SELECT count(*) FILTER (WHERE ticker = 'AAA'), count(*) FILTER (WHERE ticker = 'BBB') FROM signed;
SELECT max(session_high) FROM trade_events WHERE ticker = 'AAA';
",
    );
    assert_eq!(
        stdout,
        "130:true 135:true 140:true 150:false 170:false 175:false 145:false 190:false\n7808,7800\n140.0\n",
        "{stderr}"
    );
}

/// Massive's codes decide the same: a sweep or a retired code that set prices sets it, an odd lot or an unknown code
/// does not, and a print its correction withdraws is left out.
#[test]
fn test_massive_codes_decide_what_sets_the_high() {
    let table = EventTable::new(
        TickProvider::Massive,
        EventClock::Prints,
        ReadDelay::from_milliseconds(500),
        Draws::new(NonZeroU16::new(3).unwrap()),
        EntryWindow::new(100, 110).unwrap(),
    );
    let (stdout, stderr) = run(
        table,
        TickProvider::Massive,
        Probes::With,
        "
SELECT string_agg(price::INTEGER || ':' || eligible, ' ' ORDER BY timestamp) FROM signed
WHERE ticker = 'AAA' AND price >= 120;
SELECT count(*) FILTER (WHERE ticker = 'AAA'), count(*) FILTER (WHERE ticker = 'BBB') FROM signed;
SELECT max(session_high) FROM trade_events WHERE ticker = 'AAA';
",
    );
    assert_eq!(
        stdout, "130:true 135:true 150:false 170:false\n7804,7800\n135.0\n",
        "{stderr}"
    );
}

/// An Alpaca table is refused when Alpaca's bars of the session before are absent, though Massive's are held.
#[test]
fn test_alpaca_refuses_a_session_without_its_daily_bars() {
    let table = EventTable::new(
        TickProvider::Alpaca,
        EventClock::QuoteUpdates,
        ReadDelay::from_milliseconds(500),
        Draws::new(NonZeroU16::new(3).unwrap()),
        EntryWindow::new(100, 110).unwrap(),
    );
    let held: Vec<TradesKey> = daily_bars(TickProvider::Alpaca)
        .into_iter()
        .filter(|key| key.session() != date(6, 13))
        .chain(daily_bars(TickProvider::Massive))
        .collect();
    assert_eq!(
        table.statements(&calendar(), &named(), &held, date(6, 13)),
        Err(EventTableRefusal::DailyBarsAbsent {
            provider: TickProvider::Alpaca,
            absent: date(6, 13),
        })
    );
}

/// A print read before its name's first quote rests at no touch, so each side's fill label, seconds and entry price
/// are unknown, though the window's end, from 13:34 UTC on, finds a quote; AAA, quoted throughout, fills every sell
/// and no buy, and an unfilled buy enters at the window's ask.
#[test]
fn test_an_order_with_no_quote_at_arrival_has_unknown_resting_labels() {
    let table = EventTable::new(
        TickProvider::Massive,
        EventClock::Prints,
        ReadDelay::from_milliseconds(500),
        Draws::new(NonZeroU16::new(100).unwrap()),
        EntryWindow::new(0, 5).unwrap(),
    );
    let (stdout, stderr) = run(
        table,
        TickProvider::Massive,
        Probes::Unquoted,
        "
SELECT ticker, count(*), max(arrival) < TIMESTAMP '2025-06-16 13:35:00',
    count(COLUMNS('forward_(buy|sell)_.*')), count(forward_bid_at_window)
FROM 'table/rows_massive_2025-06-16.parquet' GROUP BY ticker ORDER BY ticker;
",
    );
    assert_eq!(
        stdout,
        "AAA,100,true,100,0,100,100,0,100,100,100,100,100,100,100,100\n\
         BBB,100,true,0,0,0,0,0,0,0,0,0,0,0,0,20\n",
        "{stderr}"
    );
}

/// A window ending at the close draws only events whose delayed read lands by the last second, so the update at
/// 19:59:58 UTC, read 1.5 seconds later, is left out and each name keeps its other 29 updates of the last minute, every
/// one with a quote in force and none with resting labels, as a sixty-second rest would outlast the last read.
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
        TickProvider::Massive,
        Probes::Without,
        "
SELECT count(*), count(screen_bid), max(arrival), min(minutes_to_close) > 0,
    count(forward_sell_filled_optimistic), count(forward_ask_at_window)
FROM 'table/rows_massive_2025-06-16.parquet';
",
    );
    assert_eq!(stdout, "58,58,2025-06-16 19:59:57.5,true,0,0\n", "{stderr}");
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
            TickProvider::Massive,
            Probes::Without,
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

/// The fixture's bid and ask in force `milliseconds` after the open, in ten-thousandths of a dollar.
fn touch_at(milliseconds: i64) -> (i64, i64) {
    let quote = (milliseconds / 2000).min(11_699);
    (1_000_000 + 10 * quote - 100, 1_000_000 + 10 * quote + 100)
}

/// AAA's or BBB's prints that may fill, as milliseconds after the open and price in ten-thousandths of a dollar.
fn fillable_prints(raw: &str) -> Vec<(i64, i64)> {
    let dips = DIPS
        .iter()
        .filter(|(_, _, dip)| raw == "AAA" && *dip == Dip::Eligible)
        .map(|(milliseconds, price, _)| (*milliseconds, *price));
    (0..7800)
        .map(|print| (1000 + 3000 * print, 1_000_000 + 15 * print))
        .chain(dips)
        .collect()
}

/// A row's resting labels as the fixture implies them, each side then each bound in the table's order, decided by
/// `FillBound::fills` rather than the SQL: filled, seconds to fill and entry price, then the window's bid and ask.
fn resting_labels(raw: &str, arrival: i64, window_seconds: i64) -> Vec<String> {
    let end = arrival + 1000 * window_seconds;
    let ((bid, ask), (window_bid, window_ask)) = (touch_at(arrival), touch_at(end));
    let price = |ticks: i64| Price::from_ticks(100 * ticks).unwrap();
    let dollars = |ticks: i64| format!("{}", ticks as f64 / 10_000.0);
    let mut prints: Vec<(i64, i64)> = fillable_prints(raw)
        .into_iter()
        .filter(|(milliseconds, _)| *milliseconds > arrival && *milliseconds <= end)
        .collect();
    prints.sort_unstable();
    let mut labels = Vec::new();
    for side in Side::iter() {
        let (limit, cross) = match side {
            Side::Buy => (bid, window_ask),
            Side::Sell => (ask, window_bid),
        };
        for bound in FillBound::iter() {
            match prints
                .iter()
                .find(|(_, print)| bound.fills(side, price(limit), price(*print)))
            {
                Some((milliseconds, _)) => labels.extend([
                    "true".to_string(),
                    format!("{}", (milliseconds - arrival) as f64 / 1000.0),
                    dollars(limit),
                ]),
                None => labels.extend(["false".to_string(), "NULL".to_string(), dollars(cross)]),
            }
        }
    }
    labels.extend([dollars(window_bid), dollars(window_ask)]);
    labels
}

/// The forward return a horizon of `seconds` reads, in basis points of the mid at `arrival`.
fn forward_return(arrival: i64, seconds: i64) -> f64 {
    let mid = |milliseconds: i64| {
        let (bid, ask) = touch_at(milliseconds);
        (bid as f64 / 10_000.0 + ask as f64 / 10_000.0) / 2.0
    };
    10_000.0 * (mid(arrival + 1000 * seconds) / mid(arrival) - 1.0)
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(12))]

    /// Every quote update of minutes nine to eleven, read after any delay, carries the resting labels the fixture
    /// implies for any window, past a print at its arrival, an odd lot and a withdrawn print, and fills at the bid
    /// only under the optimistic bound; a horizon of any seconds reads the mid that far ahead.
    #[test]
    fn test_resting_labels_and_horizons_agree_with_the_fixture(
        delay in prop_oneof![Just(1000_u32), Just(500), 0..3000_u32],
        window_seconds in 1..90_u32,
        horizon in 1..600_u32,
    ) {
        let table = EventTable::new(
            TickProvider::Massive,
            EventClock::QuoteUpdates,
            ReadDelay::from_milliseconds(delay),
            Draws::new(NonZeroU16::new(100).unwrap()),
            EntryWindow::new(9, 11).unwrap(),
        )
        .with_resting_window(RestingWindow::from_seconds(NonZeroU32::new(window_seconds).unwrap()))
        .with_horizons(ForwardHorizons::from_seconds([NonZeroU32::new(horizon).unwrap()].into()));
        let suffix = match horizon % 60 {
            0 => format!("{}", horizon / 60),
            _ => format!("seconds_{horizon}"),
        };
        let (stdout, stderr) = run(
            table,
            TickProvider::Massive,
            Probes::Dips,
            &format!(
                "
SELECT ticker, epoch_ms(arrival) - epoch_ms(TIMESTAMP '2025-06-16 13:30:00'),
    COLUMNS('forward_(buy|sell)_.*|forward_(bid|ask)_at_window'), raw_{suffix}
FROM 'table/rows_massive_2025-06-16.parquet' ORDER BY ticker, key;
"
            ),
        );
        let rows: Vec<Vec<&str>> = stdout.lines().map(|line| line.split(',').collect()).collect();
        prop_assert_eq!(rows.len(), 120, "{}{}", stdout, stderr);
        let mut filled = [0_usize; 4];
        for row in &rows {
            let arrival: i64 = row[1].parse().unwrap();
            let expected = resting_labels(row[0], arrival, i64::from(window_seconds));
            prop_assert_eq!(row.len(), 2 + expected.len() + 1, "{:?}", row);
            for (column, (read, implied)) in row[2..row.len() - 1].iter().zip(&expected).enumerate() {
                match (read.parse::<f64>(), implied.parse::<f64>()) {
                    (Ok(read), Ok(implied)) => {
                        prop_assert!((read - implied).abs() < 1e-9, "column {} of {:?}: {:?}", column, row, expected);
                    }
                    (Ok(_) | Err(_), Ok(_) | Err(_)) => {
                        prop_assert_eq!(read, implied, "column {} of {:?}", column, row);
                    }
                }
            }
            for (count, column) in filled.iter_mut().zip([0, 3, 6, 9]) {
                *count += usize::from(expected[column] == "true");
            }
            let read: f64 = row[row.len() - 1].parse().unwrap();
            let implied = forward_return(arrival, i64::from(horizon));
            prop_assert!((read - implied).abs() < 1e-6, "{} against {}", read, implied);
        }
        // Rising prints reach every sell's ask within 25 seconds, so a longer window must see fills.
        prop_assert!(window_seconds < 30 || filled[2] == 120, "{:?}", filled);
        // The print at the bid fills the buy resting there under the optimistic bound alone.
        prop_assert!(window_seconds < 2 || filled[0] > filled[1], "{:?}", filled);
    }
}

/// The resting oracle agrees with windows read by hand: AAA's buy resting at 13:40:30's bid of 100.305 is filled
/// through its limit at 13:40:40.5 by the dip to 99, and BBB's, with no dip, crosses to the ask of 100.355 at 13:41:30.
#[test]
fn test_the_resting_oracle_matches_a_window_read_by_hand() {
    let labels = resting_labels("AAA", 630_000, 60);
    assert_eq!(
        labels[..6],
        ["true", "10.5", "100.305", "true", "10.5", "100.305"]
    );
    let labels = resting_labels("BBB", 630_000, 60);
    assert_eq!(labels[..3], ["false", "NULL", "100.355"]);
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
