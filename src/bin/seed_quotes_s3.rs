//! Folds the quoted book from Alpaca into the S3 spread archive, session by session.
//!
//! Reads and writes `data/equity/quotes/interval=<cadence>/`. No database is touched.

use std::collections::BTreeSet;

use chrono::NaiveDate;
use tracing::{error, info};

use fund::common::alpaca::{AlpacaCredentials, DataFeed, MarketDataClient, TradingClient};
use fund::common::log::init_tracing;
use fund::common::types::{LiquidityFloor, QuoteSummary, SessionDate, Ticker};
use fund::data::archive::{self, NameSelection, Scope, SessionSelection};
use fund::data::calendar::TradingCalendar;
use fund::data::quotes;

const USAGE: &str = "Usage: seed_quotes_s3 START_DATE END_DATE [STRIDE [SYMBOLS [MODE]]]\n\
                     Dates are Eastern calendar dates, inclusive: YYYY-MM-DD.\n\
                     STRIDE samples every Nth trading session from START_DATE (default 1).\n\
                     A stride that is a multiple of 5 samples one weekday forever; 21 does not.\n\
                     SYMBOLS is a comma-separated list, and naming it requires naming STRIDE too.\n\
                     SYMBOLS may instead be the reserved lowercase word `all`, matched exactly so\n\
                     an uppercase name of the same spelling is still a ticker:\n\
                       all      every name the daily archive holds, for every sampled session it\n\
                                can describe, widening one already summarized rather than\n\
                                skipping it; a session the daily archive is missing is left alone\n\
                     MODE applies only with a symbol list, and is one of two reserved lowercase\n\
                     words, matched exactly on the same terms as `all`:\n\
                       measure  print what those names read and write nothing (the default)\n\
                       repair   fold them into the sessions that already have a partition\n\
                     A repair never creates a partition: one holding only the named symbols would\n\
                     read as a complete session to every later pass. `all` may, because what it\n\
                     writes is the whole market.";

/// Sessions between samples unless told otherwise, which is every session.
const DEFAULT_STRIDE: usize = 1;

/// What the run does, which the `SYMBOLS` and `MODE` arguments select.
///
/// `Measure` is the default for a named symbol list rather than `Repair`, because the safe reading
/// of "I named some symbols" is "show me these", and writing is the irreversible half.
#[derive(Debug, PartialEq, Eq)]
enum Mode {
    /// Fold the screened universe for every sampled session and write both cadences.
    Archive,
    /// Fold every name the daily archive holds, widening sessions already summarized.
    WholeMarket,
    /// Fold only these names and print what they read, touching no partition.
    Measure(BTreeSet<Ticker>),
    /// Fold only these names into the sampled sessions that already have a partition.
    Repair(BTreeSet<Ticker>),
}

struct Parameters {
    start: SessionDate,
    end: SessionDate,
    stride: usize,
    mode: Mode,
}

impl Parameters {
    fn parse(arguments: &[String]) -> Result<Self, String> {
        // `named` pairs the symbol list with its mode, because a mode without symbols is not a
        // state the arguments can express and should not be a branch anyone has to read.
        let (start, end, stride, named) = match arguments {
            [start, end] => (start, end, None, None),
            [start, end, stride] => (start, end, Some(stride), None),
            // Positional after the stride, so measuring named symbols means stating the stride
            // rather than having it inferred from an argument that could be either.
            [start, end, stride, symbols] => (start, end, Some(stride), Some((symbols, None))),
            [start, end, stride, symbols, mode] => {
                (start, end, Some(stride), Some((symbols, Some(mode))))
            }
            _ => {
                return Err(format!(
                    "Expected two dates, an optional stride, symbol list and mode\n{USAGE}"
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

        let mode = match named {
            None => Mode::Archive,
            // Matched before the symbol parser, and exactly: `ALL` is a valid ticker shape, so
            // order is what separates the reserved word from a name spelled the same way.
            Some((symbols, None)) if symbols.trim() == "all" => Mode::WholeMarket,
            Some((symbols, Some(mode))) if symbols.trim() == "all" => {
                return Err(format!(
                    "MODE applies to a symbol list, not to `all`, got {:?}\n{USAGE}",
                    mode.trim()
                ))
            }
            Some((symbols, mode)) => {
                let symbols = parse_symbols(symbols)?;
                // Trimmed before matching, so a padded mode word is not read as an unknown one.
                match mode.map(|raw| raw.trim()) {
                    None | Some("measure") => Mode::Measure(symbols),
                    Some("repair") => Mode::Repair(symbols),
                    Some(other) => {
                        return Err(format!(
                            "MODE must be measure or repair, got {other:?}\n{USAGE}"
                        ))
                    }
                }
            }
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
            //
            // `sessions_without_data` counts here where it would not on the bar path: the sample
            // is drawn from the published calendar, so a session that answers with nothing is a
            // missing daily partition or an empty fold, never a holiday.
            if summary.symbols_failed > 0
                || !summary.sessions_failed.is_empty()
                || summary.sessions_without_data > 0
            {
                eprintln!(
                    "Incomplete: re-run the affected sessions with the missing symbols and `repair`"
                );
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

    let scope = match &parameters.mode {
        Mode::Measure(symbols) => {
            measure(&market_data, &calendar, &sampled, symbols).await;
            return Ok(None);
        }
        Mode::Archive => Scope::new(
            NameSelection::Screened(LiquidityFloor::CURRENT),
            SessionSelection::Absent,
        )?,
        Mode::WholeMarket => Scope::new(NameSelection::WholeMarket, SessionSelection::Every)?,
        Mode::Repair(symbols) => Scope::new(
            NameSelection::Named(symbols.clone()),
            SessionSelection::Present,
        )?,
    };

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
            &scope,
        )
        .await?,
    ))
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

    /// Writing is the irreversible half, so it must be asked for by name. A run that meant to
    /// measure and silently repaired instead would rewrite partitions across a whole window.
    #[test]
    fn test_repairing_must_be_asked_for_explicitly() {
        let measured = Parameters::parse(&arguments(&["2026-08-03", "2026-08-21", "21", "AAPL"]))
            .expect("symbols without a mode parse");
        assert!(
            matches!(measured.mode, Mode::Measure(_)),
            "the safe default"
        );

        let explicit = Parameters::parse(&arguments(&[
            "2026-08-03",
            "2026-08-21",
            "21",
            "AAPL",
            " repair ",
        ]))
        .expect("a trimmed mode word parses");
        assert!(matches!(explicit.mode, Mode::Repair(_)));

        assert!(
            Parameters::parse(&arguments(&[
                "2026-08-03",
                "2026-08-21",
                "21",
                "AAPL",
                "Repair"
            ]))
            .is_err(),
            "the words are lowercase, so an uppercase one is a typo rather than a mode"
        );
        assert!(Parameters::parse(&arguments(&[
            "2026-08-03",
            "2026-08-21",
            "21",
            "AAPL",
            "write"
        ]))
        .is_err());
    }

    /// A mode names what to do with a symbol list, so the two travel together and "a mode with no
    /// symbols" is not a state the arguments can express. What is left to refuse is a sixth one.
    /// The word is what widens the archive past its own screen, and `ALL` is a valid ticker shape,
    /// so the lowercase form has to be matched exactly and before the symbol parser sees it.
    #[test]
    fn test_all_is_a_reserved_word_and_its_uppercase_is_a_ticker() {
        let widening = Parameters::parse(&arguments(&["2026-08-17", "2026-08-21", "1", " all "]))
            .expect("a trimmed reserved word parses");
        assert_eq!(widening.mode, Mode::WholeMarket);

        let named = Parameters::parse(&arguments(&["2026-08-17", "2026-08-21", "1", "ALL"]))
            .expect("uppercase stays a ticker");
        let Mode::Measure(symbols) = named.mode else {
            panic!("an uppercase name must not widen the archive");
        };
        assert!(symbols.contains(&Ticker::new("ALL").unwrap()));
    }

    /// A mode says what to do with named symbols, and `all` is not a list of them.
    #[test]
    fn test_a_mode_alongside_all_is_refused() {
        assert!(Parameters::parse(&arguments(&[
            "2026-08-17",
            "2026-08-21",
            "1",
            "all",
            "repair"
        ]))
        .is_err());
    }

    #[test]
    fn test_arguments_past_the_mode_are_refused() {
        assert!(Parameters::parse(&arguments(&[
            "2026-08-03",
            "2026-08-21",
            "21",
            "AAPL",
            "repair",
            "extra"
        ]))
        .is_err());
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
