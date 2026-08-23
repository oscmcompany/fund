//! Backfills the intraday bar archive from Massive, one cadence at a time.
//!
//! Reads and repairs `data/equity/bars/interval=<cadence>/`. No database is touched.

use std::collections::BTreeSet;

use chrono::NaiveDate;
use tracing::{error, info};

use fund::common::log::init_tracing;
use fund::common::massive::MassiveClient;
use fund::common::types::{BarInterval, LiquidityFloor, SessionDate, Ticker};
use fund::data::archive::{self, IntradayScope};

const USAGE: &str = "Usage: seed_intraday_bars_s3 START_DATE END_DATE [CADENCE [SYMBOLS]]\n\
                     Dates are Eastern calendar dates, inclusive: YYYY-MM-DD.\n\
                     CADENCE is five_minute (default) or one_minute.\n\
                     SYMBOLS is a comma-separated list, and naming it requires naming CADENCE too.\n\
                     Supplying it fetches only those names and requests every session in the\n\
                     window rather than only the absent ones.\n\
                     SYMBOLS may instead be one of three reserved lowercase words, matched\n\
                     exactly so an uppercase name of the same spelling is still a ticker:\n\
                       scan    report which names each partition lacks, and write nothing\n\
                       repair  scan, then fetch the names the scan reported\n\
                       all     every name the daily archive holds, every session in the window";

/// What the archive is filled with unless told otherwise.
///
/// Five minutes, because coverage across the whole universe puts the cliff between one minute and
/// five: 95.8% of names clear 99% of regular-session buckets at five, against 33.2% at one.
const DEFAULT_CADENCE: BarInterval = BarInterval::FiveMinute;

/// What the run does, which the `SYMBOLS` argument selects.
///
/// `Scan` writes nothing, so it is the safe way to size a repair before one rewrites a partition
/// per session across the window.
#[derive(Debug, PartialEq, Eq)]
enum Mode {
    /// Fetch, per the scope: the absent sessions, or an explicit set of names across all of them.
    Seed(IntradayScope),
    /// Report per-session coverage and stop.
    Scan,
    /// Report per-session coverage, then fetch whatever names it reported missing.
    Repair,
}

struct Parameters {
    start: SessionDate,
    end: SessionDate,
    interval: BarInterval,
    mode: Mode,
}

impl Parameters {
    fn parse(arguments: &[String]) -> Result<Self, String> {
        let (start, end, cadence, symbols) = match arguments {
            [start, end] => (start, end, None, None),
            [start, end, cadence] => (start, end, Some(cadence), None),
            // Positional after the cadence, so repairing named symbols means stating the cadence
            // rather than having it inferred from an argument that could be either.
            [start, end, cadence, symbols] => (start, end, Some(cadence), Some(symbols)),
            _ => {
                return Err(format!(
                    "Expected two dates, an optional cadence and an optional symbol list\n{USAGE}"
                ))
            }
        };

        let start = session(start, "START_DATE")?;
        let end = session(end, "END_DATE")?;
        if start > end {
            return Err(format!("START_DATE must not be after END_DATE\n{USAGE}"));
        }

        // Refused rather than redirected: the aggregates route stamps a daily bar sixteen hours
        // from where the grouped route does, so it would not line up with the archive beside it.
        let interval = match cadence.map(String::as_str) {
            None => DEFAULT_CADENCE,
            Some("five_minute") => BarInterval::FiveMinute,
            Some("one_minute") => BarInterval::OneMinute,
            Some(other) => {
                return Err(format!(
                    "CADENCE must be five_minute or one_minute, got {other:?}\n{USAGE}"
                ))
            }
        };

        // Trimmed and matched before the symbol parser. Both words are valid ticker shapes, so
        // order separates them -- and untrimmed, " scan " would parse as a symbol and fetch nothing.
        let mode = match symbols.map(|raw| raw.trim()) {
            None => Mode::Seed(IntradayScope::MissingSessions),
            Some("scan") => Mode::Scan,
            Some("repair") => Mode::Repair,
            Some("all") => Mode::Seed(IntradayScope::WholeMarket),
            Some(raw) => Mode::Seed(IntradayScope::Symbols(parse_symbols(raw)?)),
        };

        Ok(Self {
            start,
            end,
            interval,
            mode,
        })
    }
}

/// Parses the comma-separated symbol list into validated tickers.
///
/// Refuses every unusable component, an empty one included, rather than skipping it: a list that
/// silently loses a name produces a partial repair the run then reports as success. `split` always
/// yields at least one component, so an empty argument is refused here too.
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

#[tokio::main]
async fn main() {
    fund::common::crypto::install_default_crypto_provider();
    let tracing_guard = init_tracing(
        "seed-intraday-bars-s3.log",
        Some("info"),
        "seed-intraday-bars-s3",
    );

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
        // `None` when no fetching pass ran, which is a scan or a repair that found nothing. Printing
        // the summary anyway reported every counter as zero and read as a failed run.
        Ok(None) => 0,
        Ok(Some(summary)) => {
            println!(
                "requested {} sessions, wrote {}, {} without data, {} failed, {} bars, {} symbols missing",
                summary.sessions_requested,
                summary.sessions_written,
                summary.sessions_without_data,
                summary.sessions_failed.len(),
                summary.bars_written,
                summary.symbols_failed
            );
            0
        }
        Err(error) => {
            error!(%error, "Seeding the intraday bar archive failed");
            eprintln!("Seeding the intraday bar archive failed: {error}");
            1
        }
    };

    drop(tracing_guard);
    std::process::exit(code);
}

/// Repairs the intraday archive over the requested window.
///
/// Without `SYMBOLS` only the sessions the bucket is missing are fetched, so re-running over a
/// repaired range costs one listing and nothing else. With it every session in the window is
/// requested, because a partition missing one name is indistinguishable from a complete one.
async fn run(
    parameters: &Parameters,
) -> Result<Option<archive::ArchiveSummary>, Box<dyn std::error::Error>> {
    let bucket = std::env::var("AWS_S3_BUCKET_NAME")
        .map_err(|_| "AWS_S3_BUCKET_NAME must be set (the equity-bar data bucket)")?;
    let s3_client = fund::common::aws::s3_client().await;

    // Built before the scan, which in repair mode is thousands of reads: an unset credential should
    // fail in the first second rather than after it. A scan writes nothing and never needs one.
    let massive = match parameters.mode {
        Mode::Scan => None,
        Mode::Seed(_) | Mode::Repair => Some(MassiveClient::from_env()?),
    };

    let scope = match &parameters.mode {
        Mode::Seed(scope) => scope.clone(),
        Mode::Scan | Mode::Repair => {
            let scan = archive::scan_intraday_symbols(
                &s3_client,
                &bucket,
                parameters.interval,
                parameters.start,
                parameters.end,
                LiquidityFloor::CURRENT,
            )
            .await?;
            report(&scan);
            if parameters.mode == Mode::Scan {
                return Ok(None);
            }
            let missing = scan.missing_symbols();
            if missing.is_empty() {
                println!("Nothing to repair.");
                return Ok(None);
            }
            IntradayScope::Symbols(missing)
        }
    };

    let massive = massive.expect("every mode that fetches builds a client above");
    let symbols = match &scope {
        IntradayScope::MissingSessions => "every name, absent sessions only".to_string(),
        IntradayScope::WholeMarket => "every name, every session".to_string(),
        IntradayScope::Symbols(symbols) => symbols
            .iter()
            .map(Ticker::as_str)
            .collect::<Vec<_>>()
            .join(","),
    };
    info!(
        bucket,
        start = %parameters.start,
        end = %parameters.end,
        interval = %parameters.interval,
        symbols,
        "Seeding the intraday bar archive from Massive"
    );

    Ok(Some(
        archive::archive_intraday_sessions(
            &s3_client,
            &massive,
            &bucket,
            parameters.interval,
            parameters.start,
            parameters.end,
            &scope,
        )
        .await?,
    ))
}

/// Missing names printed per session before the line is truncated.
///
/// A session short over a thousand names produced a multi-kilobyte line that buried the counts
/// above it. The full count is always printed; the union at the end is the actionable list.
const NAMES_SHOWN_PER_SESSION: usize = 12;

/// Prints the scan: the counts, then every session that is short a name.
///
/// Per-session as well as the union, because the two answer different questions — the union is what
/// the repair takes, while one session short fifty names and fifty sessions short one are the same
/// union and very different faults.
fn report(scan: &archive::SymbolScan) {
    let counts = scan.counts();
    println!(
        "scanned {} sessions: {} complete, {} partial, {} absent, {} undescribed",
        counts.complete + counts.partial + counts.absent + counts.undescribed,
        counts.complete,
        counts.partial,
        counts.absent,
        counts.undescribed
    );

    for (session, coverage) in scan.coverage() {
        match coverage {
            archive::SessionCoverage::Partial(missing) => {
                let shown: Vec<&str> = missing
                    .iter()
                    .take(NAMES_SHOWN_PER_SESSION)
                    .map(Ticker::as_str)
                    .collect();
                let elided = missing.len().saturating_sub(shown.len());
                let suffix = if elided > 0 {
                    format!(" and {elided} more")
                } else {
                    String::new()
                };
                println!(
                    "  {session}: short {} — {}{suffix}",
                    missing.len(),
                    shown.join(",")
                );
            }
            archive::SessionCoverage::Absent => println!("  {session}: no partition"),
            archive::SessionCoverage::Undescribed => {
                println!("  {session}: no daily partition to screen against")
            }
            archive::SessionCoverage::Complete => {}
        }
    }

    if !scan.failed().is_empty() {
        // Loud, because a repair driven by this scan repairs only what the readable sessions named.
        println!(
            "WARNING: {} sessions could not be read; this picture is incomplete",
            scan.failed().len()
        );
    }

    let missing = scan.missing_symbols();
    println!(
        "{} distinct names missing from at least one session",
        missing.len()
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    fn arguments(values: &[&str]) -> Vec<String> {
        values.iter().map(|value| value.to_string()).collect()
    }

    #[test]
    fn test_the_cadence_defaults_to_five_minutes() {
        let parameters = Parameters::parse(&arguments(&["2026-08-01", "2026-08-20"])).unwrap();
        assert_eq!(parameters.interval, BarInterval::FiveMinute);
        assert_eq!(parameters.start, SessionDate::from_date(date(2026, 8, 1)));
        assert_eq!(parameters.end, SessionDate::from_date(date(2026, 8, 20)));

        let parameters =
            Parameters::parse(&arguments(&["2026-08-01", "2026-08-20", "one_minute"])).unwrap();
        assert_eq!(parameters.interval, BarInterval::OneMinute);

        // The documented spelling, passed explicitly. Without this a typo in that arm sends a user
        // who followed the usage text to the "CADENCE must be" error and the default test still passes.
        let parameters =
            Parameters::parse(&arguments(&["2026-08-01", "2026-08-20", "five_minute"])).unwrap();
        assert_eq!(parameters.interval, BarInterval::FiveMinute);
    }

    fn date(year: i32, month: u32, day: u32) -> NaiveDate {
        NaiveDate::from_ymd_opt(year, month, day).expect("a valid test date")
    }

    /// A daily cadence would come from the aggregates route, which stamps a daily bar sixteen hours
    /// from where the grouped route stamps it — so a backfill taken here would not line up with the
    /// archive it landed beside.
    #[test]
    fn test_a_daily_cadence_is_refused() {
        assert!(Parameters::parse(&arguments(&["2026-08-01", "2026-08-20", "one_day"])).is_err());
        assert!(Parameters::parse(&arguments(&["2026-08-01", "2026-08-20", "1Day"])).is_err());
    }

    #[test]
    fn test_an_unusable_window_is_refused() {
        assert!(Parameters::parse(&arguments(&["2026-08-20", "2026-08-01"])).is_err());
        assert!(Parameters::parse(&arguments(&["2026-13-01", "2026-08-20"])).is_err());
        assert!(Parameters::parse(&arguments(&["not-a-date", "2026-08-20"])).is_err());
        assert!(Parameters::parse(&arguments(&["2026-08-01"])).is_err());
        assert!(Parameters::parse(&[]).is_err());
    }

    /// A one-day window is a window, not an error: it is how a single missing session is repaired.
    #[test]
    fn test_a_single_session_window_is_allowed() {
        let parameters = Parameters::parse(&arguments(&["2026-08-20", "2026-08-20"])).unwrap();
        assert_eq!(parameters.start, parameters.end);
    }

    #[test]
    fn test_omitting_symbols_scans_for_missing_sessions() {
        let parameters = Parameters::parse(&arguments(&["2026-08-01", "2026-08-20"])).unwrap();
        assert!(matches!(
            parameters.mode,
            Mode::Seed(IntradayScope::MissingSessions)
        ));

        let parameters =
            Parameters::parse(&arguments(&["2026-08-01", "2026-08-20", "five_minute"])).unwrap();
        assert!(matches!(
            parameters.mode,
            Mode::Seed(IntradayScope::MissingSessions)
        ));
    }

    /// Both words are valid ticker shapes, so only the order of the match separates them from a
    /// one-name repair list. A `scan` parsed as a ticker would fetch a symbol that does not exist
    /// and report a clean run.
    #[test]
    fn test_the_two_mode_words_are_modes_and_not_tickers() {
        let scan = Parameters::parse(&arguments(&[
            "2026-08-01",
            "2026-08-20",
            "five_minute",
            "scan",
        ]))
        .unwrap();
        assert_eq!(scan.mode, Mode::Scan);

        let repair = Parameters::parse(&arguments(&[
            "2026-08-01",
            "2026-08-20",
            "five_minute",
            "repair",
        ]))
        .unwrap();
        assert_eq!(repair.mode, Mode::Repair);
    }

    /// `parse_symbols` trims and `Ticker::new` uppercases, so an untrimmed mode word parsed as a
    /// symbol list becomes `Ticker("SCAN")` — a name that does not exist, fetched, written as
    /// nothing, and reported as a clean pass.
    #[test]
    fn test_a_mode_word_with_surrounding_whitespace_is_still_a_mode() {
        let scan = Parameters::parse(&arguments(&[
            "2026-08-01",
            "2026-08-20",
            "five_minute",
            "  scan  ",
        ]))
        .unwrap();
        assert_eq!(scan.mode, Mode::Scan);

        let repair = Parameters::parse(&arguments(&[
            "2026-08-01",
            "2026-08-20",
            "five_minute",
            " repair\t",
        ]))
        .unwrap();
        assert_eq!(repair.mode, Mode::Repair);
    }

    /// The words are exact, so a name that merely contains one is still a ticker. `SCAN` is a
    /// plausible symbol and must not silently become a mode.
    #[test]
    fn test_a_ticker_resembling_a_mode_word_is_still_a_ticker() {
        let parameters = Parameters::parse(&arguments(&[
            "2026-08-01",
            "2026-08-20",
            "five_minute",
            "SCAN",
        ]))
        .unwrap();

        let Mode::Seed(IntradayScope::Symbols(symbols)) = parameters.mode else {
            panic!("an uppercase name must parse as a symbol list");
        };
        let named: Vec<&str> = symbols.iter().map(Ticker::as_str).collect();
        assert_eq!(named, vec!["SCAN"]);
    }

    #[test]
    fn test_a_symbol_list_is_parsed_and_trimmed() {
        let parameters = Parameters::parse(&arguments(&[
            "2026-08-01",
            "2026-08-20",
            "five_minute",
            " CBOE , CME,ICE ",
        ]))
        .unwrap();

        let Mode::Seed(IntradayScope::Symbols(symbols)) = parameters.mode else {
            panic!("a symbol list must produce a symbol scope");
        };
        let named: Vec<&str> = symbols.iter().map(Ticker::as_str).collect();
        assert_eq!(named, vec!["CBOE", "CME", "ICE"]);
    }

    /// Refused rather than skipped. A typo silently narrowing the universe looks exactly like a name
    /// the vendor has no data for, and the run would report success having fetched less than asked.
    #[test]
    fn test_an_unusable_symbol_list_is_refused() {
        // The typo that matters: an operator who meant three names gets two, and a partial repair
        // reports success.
        assert!(Parameters::parse(&arguments(&[
            "2026-08-01",
            "2026-08-20",
            "five_minute",
            "CBOE,,ICE"
        ]))
        .is_err());
        assert!(Parameters::parse(&arguments(&[
            "2026-08-01",
            "2026-08-20",
            "five_minute",
            "CBOE,"
        ]))
        .is_err());
        assert!(
            Parameters::parse(&arguments(&["2026-08-01", "2026-08-20", "five_minute", ""]))
                .is_err()
        );
        assert!(Parameters::parse(&arguments(&[
            "2026-08-01",
            "2026-08-20",
            "five_minute",
            "  ,  "
        ]))
        .is_err());
        assert!(Parameters::parse(&arguments(&[
            "2026-08-01",
            "2026-08-20",
            "five_minute",
            "CBOE",
            "extra"
        ]))
        .is_err());
    }
}
