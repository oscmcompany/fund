//! Backfills daily bars into an empty database over an arbitrary date range.
//!
//! Bootstrap tooling. The running service's `market_data_sync` closes a three-session gap, which is
//! the right window for a database that is already populated and the wrong one for a database that
//! is not: the screen needs sixty sessions of aligned closes and the model seventy, so a fresh
//! deployment cannot trade until something has fetched several months at once.
//!
//! Usage: `seed_equity_bars <start YYYY-MM-DD> [end YYYY-MM-DD]`
//! The end date defaults to today (US/Eastern) when omitted.
//!
//! The previous version took `--source <massive|s3>` and `--target <s3|postgresql|all>`. Both are
//! gone with the topology that justified them: Massive is no longer a data provider, and the
//! trainer now fetches and archives its own S3 parquet rather than reading what a seed run left
//! behind. What remains is the one path that was ever load-bearing — Alpaca into PostgreSQL.

use chrono::{Duration, NaiveDate, Utc};
use std::collections::HashSet;
use tracing::{error, info, warn};

use fund::common::alpaca::{AlpacaCredentials, MarketDataClient, TradingClient};
use fund::common::database::connect_pool;
use fund::common::observability::init_tracing;
use fund::common::types::BarInterval;
use fund::data::bars;
use fund::data::calendar::eastern_date;
use fund::data::details;

const USAGE: &str = "Usage: seed_equity_bars <start YYYY-MM-DD> [end YYYY-MM-DD]";

/// Calendar days fetched and stored per round trip.
///
/// The bars endpoint pages internally, so a single call spanning a year would succeed — and would
/// hold every row of it in memory before the first one reached PostgreSQL, then lose all of it to
/// one timeout. Chunking bounds the memory and makes a failure cost one window instead of the run.
const CHUNK_DAYS: i64 = 30;

/// Inclusive date range, validated on construction.
#[derive(Debug, PartialEq, Eq)]
struct SeedRange {
    start: NaiveDate,
    end: NaiveDate,
}

impl SeedRange {
    /// Rejects an inverted range, so a `SeedRange` in scope is proof the window is orderable.
    fn new(start: NaiveDate, end: NaiveDate) -> Result<Self, String> {
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
            let window_end = (window_start + Duration::days(CHUNK_DAYS - 1)).min(self.end);
            chunks.push(SeedRange {
                start: window_start,
                end: window_end,
            });
            window_start = window_end + Duration::days(1);
        }
        chunks
    }
}

fn parse_date(value: &str) -> Result<NaiveDate, String> {
    NaiveDate::parse_from_str(value, "%Y-%m-%d")
        .map_err(|error| format!("Invalid date '{value}': expected YYYY-MM-DD ({error})"))
}

/// Parses the positional date arguments, defaulting the end date to today in Eastern.
///
/// `today` is a parameter rather than read from the clock here, because a function that reads the
/// wall clock cannot be tested across the hours where the Eastern date and the UTC date disagree.
fn parse_arguments(arguments: &[String], today: NaiveDate) -> Result<SeedRange, String> {
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
    chunks_failed: usize,
}

impl SeedSummary {
    /// A run that stepped over a failed window is incomplete, so it must not look successful.
    fn exit_code(&self) -> i32 {
        if self.chunks_failed > 0 {
            1
        } else {
            0
        }
    }
}

#[tokio::main]
async fn main() {
    fund::common::crypto::install_default_crypto_provider();
    let tracing_guard = init_tracing("seed-equity-bars.log", Some("info"), "seed-equity-bars");

    let arguments: Vec<String> = std::env::args().skip(1).collect();
    let range = match parse_arguments(&arguments, eastern_date(Utc::now())) {
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
    let credentials = AlpacaCredentials::from_env()?;
    let trading = TradingClient::from_env(credentials.clone());
    let market_data = MarketDataClient::from_env(credentials);
    let pool = connect_pool().await?;

    // Alpaca's tradable set intersected with the embedded ticker list, which is the same universe
    // the trainer fetches. Deliberately not the application's liquidity-filtered universe: that one
    // is computed *from* `equity_bars`, and this binary exists for the case where that table is
    // empty.
    let assets = trading.fetch_tradable_assets().await?;
    let known: HashSet<String> = details::parse_embedded_details()?
        .into_iter()
        .map(|detail| detail.ticker().as_str().to_string())
        .collect();
    let symbols: Vec<String> = assets
        .tradable_symbols()
        .into_iter()
        .filter(|symbol| known.contains(symbol))
        .collect();

    if symbols.is_empty() {
        return Err("No tradable symbols intersect the embedded ticker list".into());
    }

    let chunks = range.chunks();
    info!(
        symbols = symbols.len(),
        start = %range.start,
        end = %range.end,
        chunks = chunks.len(),
        "Seeding equity bars from Alpaca"
    );

    let mut summary = SeedSummary::default();
    for chunk in &chunks {
        match seed_chunk(&market_data, &pool, &symbols, chunk).await {
            Ok(rows) => summary.rows_stored += rows,
            Err(error) => {
                // Stepped over rather than propagated. A seed spans months, and one failed window
                // costs the sessions inside it; aborting costs every window after it too, and the
                // upsert makes a rerun of the whole range cheap enough that partial progress is
                // worth keeping.
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
    market_data: &MarketDataClient,
    pool: &sqlx::PgPool,
    symbols: &[String],
    chunk: &SeedRange,
) -> Result<u64, Box<dyn std::error::Error>> {
    let fetched = bars::fetch_bars(
        market_data,
        symbols,
        BarInterval::OneDay,
        chunk.start,
        chunk.end,
    )
    .await?;

    if fetched.is_empty() {
        // Expected for a window that is entirely weekend or holiday, so not an error — but a
        // silent zero over a window that should hold sessions is worth being able to see.
        warn!(start = %chunk.start, end = %chunk.end, "Chunk returned no bars");
        return Ok(0);
    }

    let stored = bars::store_bars(pool, &fetched).await?;
    info!(
        start = %chunk.start,
        end = %chunk.end,
        fetched = fetched.len(),
        stored,
        "Chunk seeded"
    );
    Ok(stored)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn date(value: &str) -> NaiveDate {
        NaiveDate::parse_from_str(value, "%Y-%m-%d").expect("a valid test date")
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
                window[0].end + Duration::days(1),
                "chunks must abut without a gap or an overlap"
            );
        }

        for chunk in &chunks {
            let span = (chunk.end - chunk.start).num_days() + 1;
            assert!(span <= CHUNK_DAYS, "chunk of {span} days exceeds the bound");
        }
    }

    #[test]
    fn test_a_single_day_range_yields_one_window() {
        let range = SeedRange::new(date("2026-01-05"), date("2026-01-05")).expect("a valid range");
        assert_eq!(range.chunks().len(), 1);
    }

    #[test]
    fn test_a_failed_chunk_makes_the_run_fail() {
        let clean = SeedSummary {
            rows_stored: 100,
            chunks_failed: 0,
        };
        let partial = SeedSummary {
            rows_stored: 100,
            chunks_failed: 1,
        };
        assert_eq!(clean.exit_code(), 0);
        assert_eq!(partial.exit_code(), 1);
    }
}
