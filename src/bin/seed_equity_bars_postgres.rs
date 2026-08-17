//! Backfills daily bars into an empty database over an arbitrary date range.
//!
//! Bootstrap tooling. The service's `market_data_sync` closes a three-session gap, which is right
//! for a populated database and wrong for an empty one: the screen needs sixty sessions of aligned
//! closes and the model forty, so a fresh deployment cannot trade until something has fetched
//! several months at once.
//!
//! Usage: `seed_equity_bars_postgres <start YYYY-MM-DD> [end YYYY-MM-DD]`
//! The end date defaults to today (US/Eastern) when omitted.
//!
//! Massive is the only source, because its grouped endpoint takes a **date** rather than a symbol
//! list. Asking Alpaca means asking for its *current* tradable set, so every symbol delisted since
//! the start date would be missing from its own history.

use chrono::{NaiveDate, Utc};
use tracing::{error, info, warn};

use fund::common::database::connect_pool;
use fund::common::log::init_tracing;
use fund::common::massive::MassiveClient;
use fund::common::types::SessionDate;
use fund::data::bars;

const USAGE: &str = "Usage: seed_equity_bars_postgres <start YYYY-MM-DD> [end YYYY-MM-DD]";

/// Calendar days fetched before the rows are written and the buffer released.
///
/// A grouped response is the whole market — on the order of ten thousand rows per session — so a
/// year fetched before the first write would hold roughly two and a half million bars in memory and
/// lose all of them to one failure. Thirty days is about twenty-one sessions, a couple of hundred
/// thousand rows, and a bounded amount of work to repeat.
const CHUNK_DAYS: i64 = 30;

/// Inclusive date range, validated on construction.
#[derive(Debug, PartialEq, Eq)]
struct SeedRange {
    start: SessionDate,
    end: SessionDate,
}

impl SeedRange {
    /// Rejects an inverted range, so a `SeedRange` in scope is proof the window is orderable.
    fn new(start: SessionDate, end: SessionDate) -> Result<Self, String> {
        if start > end {
            return Err(format!(
                "Invalid range: start date {start} must be on or before end date {end}"
            ));
        }
        Ok(Self { start, end })
    }

    /// Splits into consecutive inclusive windows of at most [`CHUNK_DAYS`].
    ///
    /// Windows abut rather than overlap: each begins the day after the previous one ended. The bars
    /// upsert is idempotent, so an overlap would be harmless — it would just refetch.
    fn chunks(&self) -> Vec<SeedRange> {
        let mut chunks = Vec::new();
        let mut window_start = self.start;
        while window_start <= self.end {
            let window_end = window_start
                .plus_calendar_days(CHUNK_DAYS - 1)
                .min(self.end);
            chunks.push(SeedRange {
                start: window_start,
                end: window_end,
            });
            window_start = window_end.plus_calendar_days(1);
        }
        chunks
    }

    /// Every calendar day in the window.
    ///
    /// Calendar days, not trading sessions: this binary has no calendar — it exists for the case
    /// where the database is empty, and the published calendar is one of the things that is not
    /// there yet. A weekend costs one request and answers with nothing, which is cheap enough that
    /// filtering is not worth a dependency on data the caller may not have.
    fn dates(&self) -> Vec<SessionDate> {
        let mut dates = Vec::new();
        let mut date = self.start;
        while date <= self.end {
            dates.push(date);
            date = date.plus_calendar_days(1);
        }
        dates
    }
}

fn parse_date(value: &str) -> Result<SessionDate, String> {
    NaiveDate::parse_from_str(value, "%Y-%m-%d")
        .map(SessionDate::from_date)
        .map_err(|error| format!("Invalid date '{value}': expected YYYY-MM-DD ({error})"))
}

/// Parses the positional date arguments, defaulting the end date to today in Eastern.
///
/// `today` is a parameter rather than read from the clock here, because a function that reads the
/// wall clock cannot be tested across the hours where the Eastern date and the UTC date disagree.
fn parse_arguments(arguments: &[String], today: SessionDate) -> Result<SeedRange, String> {
    match arguments {
        [] => Err(format!("Start date is required\n{USAGE}")),
        [start] => SeedRange::new(parse_date(start)?, today),
        [start, end] => SeedRange::new(parse_date(start)?, parse_date(end)?),
        _ => Err(format!("Too many arguments\n{USAGE}")),
    }
}

/// What a run did, and whether any part of it silently did nothing.
#[derive(Debug, Default, PartialEq, Eq)]
struct SeedSummary {
    rows_stored: u64,
    /// Sessions the fetch could not retrieve. A gap in the history, not a failure to store.
    dates_failed: usize,
    /// Windows whose store failed after a successful fetch.
    chunks_failed: usize,
}

impl SeedSummary {
    /// A run that stepped over anything is incomplete, so it must not look successful.
    ///
    /// Both counters gate the exit code. A seed whose fetch quietly skipped eleven sessions leaves
    /// exactly the kind of hole that surfaces later as a correlation computed across a gap, and
    /// the operator's only signal that the range needs re-running is this exit code.
    fn exit_code(&self) -> i32 {
        if self.chunks_failed > 0 || self.dates_failed > 0 {
            1
        } else {
            0
        }
    }
}

#[tokio::main]
async fn main() {
    fund::common::crypto::install_default_crypto_provider();
    let tracing_guard = init_tracing(
        "seed-equity-bars-postgres.log",
        Some("info"),
        "seed-equity-bars-postgres",
    );

    let arguments: Vec<String> = std::env::args().skip(1).collect();
    let range = match parse_arguments(&arguments, SessionDate::at(Utc::now())) {
        Ok(range) => range,
        Err(message) => {
            eprintln!("{message}");
            drop(tracing_guard);
            std::process::exit(2);
        }
    };

    let code = match run(&range).await {
        Ok(summary) => {
            info!(
                rows = summary.rows_stored,
                dates_failed = summary.dates_failed,
                chunks_failed = summary.chunks_failed,
                "Equity bars seeded"
            );
            summary.exit_code()
        }
        Err(error) => {
            error!(%error, "Seeding equity bars failed");
            eprintln!("Seeding equity bars failed: {error}");
            1
        }
    };

    // `std::process::exit` runs no destructors, so the non-blocking appender's guard would never
    // drop and its buffered lines would be lost — exactly when the failure log matters.
    drop(tracing_guard);
    std::process::exit(code);
}

async fn run(range: &SeedRange) -> Result<SeedSummary, Box<dyn std::error::Error>> {
    let client = MassiveClient::from_env()?;
    let pool = connect_pool().await?;

    // No symbol list, and no universe. The grouped endpoint is asked for a date and answers with
    // every stock that traded on it, which is exactly what a bootstrap wants: the liquidity screen
    // downstream selects from what is stored, so storing a pre-filtered subset would decide the
    // universe here rather than there.
    let chunks = range.chunks();
    info!(
        start = %range.start,
        end = %range.end,
        chunks = chunks.len(),
        "Seeding equity bars from Massive"
    );

    let mut summary = SeedSummary::default();
    for chunk in &chunks {
        match seed_chunk(&client, &pool, chunk).await {
            Ok(chunk_summary) => {
                summary.rows_stored += chunk_summary.rows_stored;
                summary.dates_failed += chunk_summary.dates_failed;
            }
            Err(error) => {
                // A store failure, as distinct from a fetch failure — `fetch_daily_bars` already
                // steps over the dates it could not retrieve. Stepped over for the same reason:
                // a seed spans months, aborting costs every window after this one, and the upsert
                // makes re-running the whole range cheap.
                summary.chunks_failed += 1;
                error!(
                    start = %chunk.start,
                    end = %chunk.end,
                    %error,
                    "Chunk failed, continuing"
                );
            }
        }
    }

    Ok(summary)
}

async fn seed_chunk(
    client: &MassiveClient,
    pool: &sqlx::PgPool,
    chunk: &SeedRange,
) -> Result<SeedSummary, Box<dyn std::error::Error>> {
    let dates = chunk.dates();
    let fetched = bars::fetch_daily_bars(client, &dates).await;

    if fetched.bars.is_empty() {
        // Expected for a window that is entirely weekend or holiday, so not an error — but a
        // silent zero over a window that should hold sessions is worth being able to see.
        warn!(start = %chunk.start, end = %chunk.end, "Chunk returned no bars");
        return Ok(SeedSummary {
            rows_stored: 0,
            dates_failed: fetched.dates_failed.len(),
            chunks_failed: 0,
        });
    }

    let stored = bars::store_bars(pool, &fetched.bars).await?;
    info!(
        start = %chunk.start,
        end = %chunk.end,
        fetched = fetched.bars.len(),
        dates_failed = fetched.dates_failed.len(),
        stored,
        "Chunk seeded"
    );
    Ok(SeedSummary {
        rows_stored: stored,
        dates_failed: fetched.dates_failed.len(),
        chunks_failed: 0,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn date(value: &str) -> SessionDate {
        SessionDate::from_date(
            NaiveDate::parse_from_str(value, "%Y-%m-%d").expect("a valid test date"),
        )
    }

    #[test]
    fn test_a_missing_start_date_is_rejected() {
        let error = parse_arguments(&[], date("2026-07-31")).expect_err("no start date");
        assert!(error.contains("Start date is required"), "{error}");
    }

    #[test]
    fn test_an_omitted_end_date_defaults_to_today() {
        let range = parse_arguments(&["2026-01-05".to_string()], date("2026-07-31"))
            .expect("a valid single-argument range");
        assert_eq!(range.start, date("2026-01-05"));
        assert_eq!(range.end, date("2026-07-31"));
    }

    #[test]
    fn test_an_inverted_range_is_rejected() {
        let error = parse_arguments(
            &["2026-07-31".to_string(), "2026-01-05".to_string()],
            date("2026-07-31"),
        )
        .expect_err("an inverted range");
        assert!(error.contains("must be on or before"), "{error}");
    }

    #[test]
    fn test_an_unparseable_date_is_rejected() {
        let error = parse_arguments(&["not-a-date".to_string()], date("2026-07-31"))
            .expect_err("an unparseable date");
        assert!(error.contains("expected YYYY-MM-DD"), "{error}");
    }

    #[test]
    fn test_extra_arguments_are_rejected() {
        let arguments = vec![
            "2026-01-05".to_string(),
            "2026-01-06".to_string(),
            "2026-01-07".to_string(),
        ];
        let error =
            parse_arguments(&arguments, date("2026-07-31")).expect_err("too many arguments");
        assert!(error.contains("Too many arguments"), "{error}");
    }

    #[test]
    fn test_a_range_shorter_than_one_chunk_is_a_single_window() {
        let range = SeedRange::new(date("2026-01-05"), date("2026-01-09")).expect("a valid range");
        let chunks = range.chunks();
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].start, date("2026-01-05"));
        assert_eq!(chunks[0].end, date("2026-01-09"));
    }

    /// The windows must abut exactly: a one-day gap between them would drop a session, and the
    /// upsert would never tell anyone, because a bar that was never fetched cannot conflict.
    #[test]
    fn test_chunks_abut_and_cover_the_whole_range_without_gaps() {
        let range = SeedRange::new(date("2026-01-01"), date("2026-04-15")).expect("a valid range");
        let chunks = range.chunks();

        assert!(chunks.len() > 1, "expected the range to split");
        assert_eq!(chunks[0].start, date("2026-01-01"));
        assert_eq!(
            chunks.last().expect("a final chunk").end,
            date("2026-04-15")
        );

        for window in chunks.windows(2) {
            assert_eq!(
                window[1].start,
                window[0].end.plus_calendar_days(1),
                "chunks must abut without a gap or an overlap"
            );
        }

        for chunk in &chunks {
            let span = (chunk.end.date() - chunk.start.date()).num_days() + 1;
            assert!(span <= CHUNK_DAYS, "chunk of {span} days exceeds the bound");
        }
    }

    #[test]
    fn test_a_single_day_range_yields_one_window() {
        let range = SeedRange::new(date("2026-01-05"), date("2026-01-05")).expect("a valid range");
        assert_eq!(range.chunks().len(), 1);
    }

    /// A single-day range must still produce that day, or a one-session top-up fetches nothing.
    #[test]
    fn test_a_single_day_range_yields_that_one_date() {
        let range = SeedRange::new(date("2026-01-05"), date("2026-01-05")).expect("a valid range");
        assert_eq!(range.dates(), vec![date("2026-01-05")]);
    }

    /// Every calendar day, weekends included. The binary has no calendar to filter with, and a
    /// non-session simply answers with nothing.
    #[test]
    fn test_dates_covers_every_calendar_day_in_the_window() {
        let range = SeedRange::new(date("2026-01-01"), date("2026-01-10")).expect("a valid range");
        let dates = range.dates();
        assert_eq!(dates.len(), 10);
        assert_eq!(dates[0], date("2026-01-01"));
        assert_eq!(dates[9], date("2026-01-10"));
        for window in dates.windows(2) {
            assert_eq!(window[1], window[0].plus_calendar_days(1));
        }
    }

    /// Both counters gate the exit code. A fetch that skipped sessions leaves a hole in the history
    /// that nothing else reports, so it must not exit zero any more than a failed store does.
    #[test]
    fn test_any_skipped_work_makes_the_run_fail() {
        let clean = SeedSummary {
            rows_stored: 100,
            dates_failed: 0,
            chunks_failed: 0,
        };
        let store_failed = SeedSummary {
            chunks_failed: 1,
            ..clean
        };
        let fetch_skipped = SeedSummary {
            dates_failed: 1,
            ..clean
        };
        assert_eq!(clean.exit_code(), 0);
        assert_eq!(store_failed.exit_code(), 1);
        assert_eq!(fetch_skipped.exit_code(), 1);
    }
}
