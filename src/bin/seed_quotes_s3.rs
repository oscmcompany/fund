//! Folds the quoted book from Alpaca into the S3 spread archive, session by session.
//!
//! Reads and writes `data/equity/quotes/interval=<cadence>/`. No database is touched.

use std::collections::BTreeSet;

use chrono::NaiveDate;
use tracing::{error, info};

use fund::common::alpaca::{AlpacaCredentials, DataFeed, MarketDataClient, TradingClient};
use fund::common::log::init_tracing;
use fund::common::types::{LiquidityFloor, QuoteSummary, SessionDate, Ticker};
use fund::data::archive;
use fund::data::calendar::TradingCalendar;
use fund::data::quotes;

const USAGE: &str = "Usage: seed_quotes_s3 START_DATE END_DATE [STRIDE [SYMBOLS]]\n\
                     Dates are Eastern calendar dates, inclusive: YYYY-MM-DD.\n\
                     STRIDE samples every Nth trading session from START_DATE (default 1).\n\
                     A stride that is a multiple of 5 samples one weekday forever; 21 does not.\n\
                     SYMBOLS is a comma-separated list, and naming it requires naming STRIDE too.\n\
                     Naming symbols measures rather than archives: it prints what those names read\n\
                     and writes nothing, because a partition holding only them would read as a\n\
                     complete session to the next pass.";

/// Sessions between samples unless told otherwise, which is every session.
const DEFAULT_STRIDE: usize = 1;

/// What the run does, which the `SYMBOLS` argument selects.
#[derive(Debug, PartialEq, Eq)]
enum Mode {
    /// Fold the screened universe for every sampled session and write both cadences.
    Archive,
    /// Fold only these names and print what they read, touching no partition.
    Measure(BTreeSet<Ticker>),
}

struct Parameters {
    start: SessionDate,
    end: SessionDate,
    stride: usize,
    mode: Mode,
}

impl Parameters {
    fn parse(arguments: &[String]) -> Result<Self, String> {
        let (start, end, stride, symbols) = match arguments {
            [start, end] => (start, end, None, None),
            [start, end, stride] => (start, end, Some(stride), None),
            // Positional after the stride, so measuring named symbols means stating the stride
            // rather than having it inferred from an argument that could be either.
            [start, end, stride, symbols] => (start, end, Some(stride), Some(symbols)),
            _ => {
                return Err(format!(
                    "Expected two dates, an optional stride and an optional symbol list\n{USAGE}"
                ))
            }
        };

        let start = session(start, "START_DATE")?;
        let end = session(end, "END_DATE")?;
        if start > end {
            return Err(format!("START_DATE must not be after END_DATE\n{USAGE}"));
        }

        let stride = match stride.map(|raw| raw.trim()) {
            None => DEFAULT_STRIDE,
            Some(raw) => raw
                .parse::<usize>()
                .ok()
                .filter(|stride| *stride > 0)
                .ok_or_else(|| {
                    format!("STRIDE must be a positive whole number, got {raw:?}\n{USAGE}")
                })?,
        };

        let mode = match symbols.map(|raw| raw.trim()) {
            None => Mode::Archive,
            Some(raw) => Mode::Measure(parse_symbols(raw)?),
        };

        Ok(Self {
            start,
            end,
            stride,
            mode,
        })
    }
}

/// Parses the comma-separated symbol list into validated tickers.
///
/// Refuses every unusable component, an empty one included, rather than skipping it: a list that
/// silently loses a name measures fewer than it reports.
fn parse_symbols(raw: &str) -> Result<BTreeSet<Ticker>, String> {
    let mut symbols = BTreeSet::new();
    for candidate in raw.split(',').map(str::trim) {
        let ticker = Ticker::new(candidate).ok_or_else(|| {
            format!("SYMBOLS contains an unusable ticker: {candidate:?}\n{USAGE}")
        })?;
        symbols.insert(ticker);
    }
    Ok(symbols)
}

/// Parses an Eastern calendar date, which is what a session is.
fn session(raw: &str, name: &str) -> Result<SessionDate, String> {
    NaiveDate::parse_from_str(raw.trim(), "%Y-%m-%d")
        .map(SessionDate::from_date)
        .map_err(|_| format!("{name} must be YYYY-MM-DD, got {raw:?}\n{USAGE}"))
}

/// Every `stride`-th published session in the window, anchored at the oldest.
///
/// Anchored at the start rather than the end so re-running the same window samples the same
/// sessions: a sample that shifts under a longer window cannot be extended without refetching what
/// is already archived.
fn sample(
    calendar: &TradingCalendar,
    start: SessionDate,
    end: SessionDate,
    stride: usize,
) -> Vec<SessionDate> {
    calendar
        .trading_days_in_range(start, end)
        .into_iter()
        .step_by(stride)
        .collect()
}

#[tokio::main]
async fn main() {
    fund::common::crypto::install_default_crypto_provider();
    let tracing_guard = init_tracing("seed-quotes-s3.log", Some("info"), "seed-quotes-s3");

    let arguments: Vec<String> = std::env::args().skip(1).collect();
    let parameters = match Parameters::parse(&arguments) {
        Ok(parameters) => parameters,
        Err(message) => {
            eprintln!("{message}");
            drop(tracing_guard);
            std::process::exit(2);
        }
    };

    let code = match run(&parameters).await {
        // `None` when nothing was archived, which is what a measurement run always reports.
        Ok(None) => 0,
        Ok(Some(summary)) => {
            println!(
                "requested {} sessions, wrote {}, {} without data, {} failed, {} summaries, {} symbols missing, {} quotes folded",
                summary.sessions_requested,
                summary.sessions_written,
                summary.sessions_without_data,
                summary.sessions_failed.len(),
                summary.summaries_written,
                summary.symbols_failed,
                summary.quotes_folded
            );
            // Non-zero on an incomplete pass, because the partition it wrote reads as complete to
            // everything downstream and the exit code is the only signal automation sees.
            if summary.symbols_failed > 0 || !summary.sessions_failed.is_empty() {
                eprintln!("Incomplete: re-run the affected sessions to fill the missing symbols");
                1
            } else {
                0
            }
        }
        Err(error) => {
            error!(%error, "Seeding the quote archive failed");
            eprintln!("Seeding the quote archive failed: {error}");
            1
        }
    };

    drop(tracing_guard);
    std::process::exit(code);
}

/// Folds the sampled sessions, or measures the named symbols across them.
async fn run(
    parameters: &Parameters,
) -> Result<Option<archive::QuoteArchiveSummary>, Box<dyn std::error::Error>> {
    let credentials = AlpacaCredentials::from_env()?;
    // SIP is pinned, not read from `ALPACA_DATA_FEED`: IEX's best bid and offer is not the national
    // one, so an environment variable could put two incomparable series under one key.
    let market_data = MarketDataClient::new(credentials.clone(), DataFeed::Sip);
    let days = TradingClient::from_env(credentials)
        .fetch_calendar(parameters.start.date(), parameters.end.date())
        .await?;
    let calendar = TradingCalendar::from_days(days);
    let sampled = sample(
        &calendar,
        parameters.start,
        parameters.end,
        parameters.stride,
    );

    info!(
        start = %parameters.start,
        end = %parameters.end,
        stride = parameters.stride,
        published = calendar.len(),
        sampled = sampled.len(),
        "Sampled the sessions to fold"
    );

    match &parameters.mode {
        Mode::Measure(symbols) => {
            measure(&market_data, &calendar, &sampled, symbols).await;
            Ok(None)
        }
        Mode::Archive => {
            let bucket = std::env::var("AWS_S3_BUCKET_NAME")
                .map_err(|_| "AWS_S3_BUCKET_NAME must be set (the equity-bar data bucket)")?;
            let s3_client = fund::common::aws::s3_client().await;
            Ok(Some(
                archive::archive_quote_sessions(
                    &s3_client,
                    &market_data,
                    &calendar,
                    &bucket,
                    &sampled,
                    LiquidityFloor::CURRENT,
                )
                .await?,
            ))
        }
    }
}

/// Folds the named symbols and prints their session figures, writing nothing.
///
/// Sequential on purpose: this is for reading a handful of numbers off real data, and a run whose
/// symbols interleave is harder to compare against a reference than one that is slower.
async fn measure(
    market_data: &MarketDataClient,
    calendar: &TradingCalendar,
    sampled: &[SessionDate],
    symbols: &BTreeSet<Ticker>,
) {
    println!(
        "{:<8}{:<12}{:>10}{:>10}{:>10}{:>10}{:>10}{:>10}{:>12}",
        "ticker",
        "session",
        "mean_bp",
        "median_bp",
        "p90_bp",
        "first_bp",
        "min_bp",
        "quotes",
        "covered_s"
    );
    for session in sampled {
        let Some((open, close)) = quotes::trading_hours(calendar, *session) else {
            println!("{session}: not a published session");
            continue;
        };
        for ticker in symbols {
            match quotes::fold_session(market_data, ticker, *session, open, close).await {
                Ok((summaries, fetch)) => {
                    print_session_row(ticker, *session, &summaries, fetch.received)
                }
                // Padded through `as_str`/`to_string`, because both Display impls delegate to an
                // inner type that ignores the width and would run the two columns together.
                Err(error) => println!(
                    "{:<8}{:<12} failed: {error}",
                    ticker.as_str(),
                    session.to_string()
                ),
            }
        }
    }
}

/// Prints one fold: its session row, plus the two intraday buckets worth reading beside it.
///
/// A session mean hides the shape the five-minute cadence exists to expose — AAPL quotes four times
/// wider at the open than at midday. `first` is the earliest bucket that carried a book, which is
/// the opening one only for a name quoting from 09:30; `min` is the tightest anywhere in the day.
fn print_session_row(
    ticker: &Ticker,
    session: SessionDate,
    summaries: &[QuoteSummary],
    quotes_folded: usize,
) {
    let Some((row, buckets)) = summaries.split_last() else {
        println!(
            "{:<8}{:<12} no quotes",
            ticker.as_str(),
            session.to_string()
        );
        return;
    };
    let basis_points = |summary: &QuoteSummary| summary.quoted_spread_basis_points_mean().value();
    let opening = buckets.first().map(basis_points).unwrap_or(f64::NAN);
    let tightest = buckets
        .iter()
        .map(basis_points)
        .fold(f64::NAN, |narrowest, bucket| bucket.min(narrowest));
    println!(
        "{:<8}{:<12}{:>10.2}{:>10.2}{:>10.2}{:>10.2}{:>10.2}{:>10}{:>12.0}",
        ticker.as_str(),
        session.to_string(),
        basis_points(row),
        row.quoted_spread_basis_points_median().value(),
        row.quoted_spread_basis_points_ninetieth_percentile()
            .value(),
        opening,
        tightest,
        quotes_folded,
        row.covered_seconds()
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    fn arguments(values: &[&str]) -> Vec<String> {
        values.iter().map(|value| value.to_string()).collect()
    }

    #[test]
    fn test_defaults_to_every_session_and_the_screened_universe() {
        let parameters = Parameters::parse(&arguments(&["2026-08-03", "2026-08-21"]))
            .expect("two dates are enough");
        assert_eq!(parameters.stride, 1);
        assert_eq!(parameters.mode, Mode::Archive);
    }

    #[test]
    fn test_naming_symbols_measures_rather_than_archives() {
        let parameters = Parameters::parse(&arguments(&[
            "2026-08-03",
            "2026-08-21",
            "21",
            " AAPL , cboe ",
        ]))
        .expect("a stride and two symbols");
        assert_eq!(parameters.stride, 21);
        let Mode::Measure(symbols) = parameters.mode else {
            panic!("naming symbols must not archive");
        };
        assert_eq!(symbols.len(), 2, "lowercase is normalized, not rejected");
        assert!(symbols.contains(&Ticker::new("CBOE").unwrap()));
    }

    #[test]
    fn test_a_stride_that_samples_nothing_is_refused() {
        assert!(Parameters::parse(&arguments(&["2026-08-03", "2026-08-21", "0"])).is_err());
        assert!(Parameters::parse(&arguments(&["2026-08-03", "2026-08-21", "-1"])).is_err());
        assert!(Parameters::parse(&arguments(&["2026-08-03", "2026-08-21", "many"])).is_err());
    }

    #[test]
    fn test_an_unusable_symbol_refuses_the_whole_list() {
        assert!(
            Parameters::parse(&arguments(&["2026-08-03", "2026-08-21", "1", "AAPL,,CBOE"]))
                .is_err(),
            "an empty component is a typo, not a separator"
        );
        assert!(Parameters::parse(&arguments(&[
            "2026-08-03",
            "2026-08-21",
            "1",
            "AAPL,TOOLONGNAME"
        ]))
        .is_err());
    }

    #[test]
    fn test_dates_must_be_eastern_calendar_dates_in_order() {
        assert!(Parameters::parse(&arguments(&["2026-08-21", "2026-08-03"])).is_err());
        assert!(Parameters::parse(&arguments(&["2026-08-21T00:00:00Z", "2026-08-21"])).is_err());
        assert!(Parameters::parse(&arguments(&["2026-08-21"])).is_err());
    }
}
