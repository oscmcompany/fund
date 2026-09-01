//! Does a dislocated pair converge inside the session, and does it beat the spread it pays?
//!
//! Screened pairs against an unscreened control, both measured on intraday volume-weighted prices.

use chrono::NaiveDate;
use tracing::{error, info};

use fund::common::alpaca::{AlpacaCredentials, TradingClient};
use fund::common::log::init_tracing;
use fund::common::types::{BarInterval, SessionDate};
use fund::laboratory::convergence::{curves_of, sample_universe, Closes, Curve, Selection};
use fund::laboratory::intraday::{self, SessionHours};
use fund::laboratory::intraday_convergence::{self, IntradayEntry};
use fund::laboratory::{dataset, intraday_convergence as measure};

use std::collections::BTreeMap;

use chrono::Timelike;

const USAGE: &str =
    "Usage: laboratory_intraday_convergence END_SESSION [LOOKBACK_DAYS] [UNIVERSE]\n\
                     END_SESSION is an Eastern calendar date: YYYY-MM-DD.";

/// Calendar days of archive to measure over by default.
const DEFAULT_LOOKBACK_DAYS: i64 = 90;

/// Names sampled from the universe by default.
///
/// Pairs grow with the square, so the whole intraday universe is millions of pairs per session.
/// Two hundred names is ~19,900 pairs, which is what #1091 measured the daily version over.
const DEFAULT_UNIVERSE: usize = 200;

/// Fixed, so two runs over one archive draw the same sample and differ only where the data does.
const SAMPLE_SEED: u64 = 0x5EED;

/// Round-trip cost in z-score units is not knowable without the fitted sigma, so the cost is
/// reported in basis points beside the result rather than folded into it.
///
/// Measured by Roll's estimator over the same archive: 9 to 13 basis points.
const EFFECTIVE_SPREAD_BASIS_POINTS: f64 = 10.0;

struct Parameters {
    session: SessionDate,
    lookback_days: i64,
    universe: usize,
}

impl Parameters {
    fn parse(arguments: &[String]) -> Result<Self, String> {
        let (session, lookback, universe) = match arguments {
            [session] => (session, DEFAULT_LOOKBACK_DAYS, DEFAULT_UNIVERSE),
            [session, lookback] => (
                session,
                positive(lookback, "LOOKBACK_DAYS")?,
                DEFAULT_UNIVERSE,
            ),
            [session, lookback, universe] => (
                session,
                positive(lookback, "LOOKBACK_DAYS")?,
                usize::try_from(positive(universe, "UNIVERSE")?).map_err(|_| {
                    format!("UNIVERSE is larger than this platform can index\n{USAGE}")
                })?,
            ),
            _ => return Err(format!("Expected an end session\n{USAGE}")),
        };
        let session = NaiveDate::parse_from_str(session.trim(), "%Y-%m-%d")
            .map(SessionDate::from_date)
            .map_err(|_| format!("END_SESSION must be YYYY-MM-DD\n{USAGE}"))?;
        if universe < 2 {
            return Err(format!("UNIVERSE must name at least two tickers\n{USAGE}"));
        }
        Ok(Self {
            session,
            lookback_days: lookback,
            universe,
        })
    }
}

fn positive(raw: &str, name: &str) -> Result<i64, String> {
    match raw.trim().parse::<i64>() {
        Ok(value) if value > 0 => Ok(value),
        _ => Err(format!("{name} must be a positive number\n{USAGE}")),
    }
}

#[tokio::main]
async fn main() {
    fund::common::crypto::install_default_crypto_provider();
    let tracing_guard = init_tracing(
        "laboratory-intraday-convergence.log",
        Some("info"),
        "laboratory-intraday-convergence",
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
        Ok(()) => 0,
        Err(error) => {
            error!(%error, "The intraday convergence measurement failed");
            eprintln!("The intraday convergence measurement failed: {error}");
            1
        }
    };
    drop(tracing_guard);
    std::process::exit(code);
}

async fn run(parameters: &Parameters) -> Result<(), Box<dyn std::error::Error>> {
    let bucket = std::env::var("AWS_S3_ARCHIVE_BUCKET_NAME")
        .map_err(|_| "AWS_S3_ARCHIVE_BUCKET_NAME must be set (the shared data/** archive)")?;
    let s3_client = fund::common::aws::s3_client().await;

    // The models are fitted on daily closes, as production fits them, so the daily window must
    // cover the correlation window ahead of the first session judged as well as the window itself.
    let daily = dataset::returns(
        &s3_client,
        &bucket,
        parameters.lookback_days + 150,
        parameters.session,
    )
    .await?;
    let closes = Closes::from_frame(&daily.returns)?;
    let universe: Vec<&str> = sample_universe(&closes, parameters.universe, SAMPLE_SEED);
    info!(
        sessions = closes.sessions(),
        universe = universe.len(),
        "Read the daily closes the models are fitted on"
    );

    let intraday_bars = dataset::intraday(
        &s3_client,
        &bucket,
        BarInterval::FiveMinute,
        parameters.lookback_days,
        parameters.session,
    )
    .await?;
    let hours = session_hours(parameters).await?;
    let vwaps = intraday::session_vwaps(&intraday_bars.bars, BarInterval::FiveMinute, &hours)?;
    info!(sessions = vwaps.len(), "Read the intraday prices");

    // Sessions are joined by their own date rather than by position: the intraday window and the
    // daily window cover different spans, so an index into one means nothing in the other.
    let daily_index: BTreeMap<SessionDate, usize> = (0..closes.sessions())
        .filter_map(|index| {
            let stamp = closes.session_at(index)?;
            let instant = chrono::DateTime::from_timestamp_millis(stamp)?;
            Some((SessionDate::at(instant), index))
        })
        .collect();

    for selection in [Selection::Screened, Selection::Unscreened] {
        let mut entries: Vec<IntradayEntry> = Vec::new();
        for (session, prices) in &vwaps {
            let Some(daily_session) = daily_index.get(session).copied() else {
                continue;
            };
            entries.extend(measure::entries_in_session(
                &closes,
                prices,
                &universe,
                *session,
                daily_session,
                selection,
            ));
        }
        report(selection, &entries);
    }
    Ok(())
}

/// The exchange's published hours for every session in the window.
async fn session_hours(
    parameters: &Parameters,
) -> Result<BTreeMap<SessionDate, SessionHours>, Box<dyn std::error::Error>> {
    let client = TradingClient::from_env(AlpacaCredentials::from_env()?);
    let start = parameters.session.date() - chrono::Duration::days(parameters.lookback_days);
    let days = client
        .fetch_calendar(start, parameters.session.date())
        .await?;

    let mut hours = BTreeMap::new();
    for day in days {
        let open = day.session_open().hour() * 60 + day.session_open().minute();
        let close = day.session_close().hour() * 60 + day.session_close().minute();
        if let Some(published) = SessionHours::new(open, close) {
            hours.insert(SessionDate::from_date(day.session_date()), published);
        }
    }
    Ok(hours)
}

/// Prints one cohort's curve and what it is worth.
fn report(selection: Selection, entries: &[IntradayEntry]) {
    println!("\n=== {} ===", selection.as_str());
    if entries.is_empty() {
        println!("no entries");
        return;
    }
    let states: Vec<_> = entries.iter().map(IntradayEntry::state).collect();
    let curves = curves_of(&states);
    let mean_z = intraday_convergence::mean_entry_z_score(entries).unwrap_or(0.0);

    println!("entries {}, mean entry z {mean_z:.3}", entries.len());
    println!("  horizon  entries  converged  stopped  open");
    for Curve {
        horizon,
        converged,
        stopped,
        open,
        entries: standing,
    } in &curves
    {
        println!("  {horizon:>7}  {standing:>7}  {converged:>9.4}  {stopped:>7.4}  {open:>5.4}");
    }

    // The statistic the horizon can answer. Full convergence asks a daily-sigma dislocation to
    // close inside a hundred minutes, so the binary resolution above reads zero whatever the pairs
    // do; drift measures how far the spread actually travelled.
    match measure::drift(entries) {
        Some(reading) => {
            let ratio = if reading.standard_error > 0.0 {
                reading.mean / reading.standard_error
            } else {
                0.0
            };
            println!(
                "\n  drift from entry to the session end: {:+.5} sigma  se {:.5}  {ratio:+.2} standard \
                 errors  over {} sessions ({} entries)",
                reading.mean, reading.standard_error, reading.sessions, reading.entries
            );
            println!(
                "  share moving toward the mean: {:.4}  (a coin is 0.5000)",
                reading.share_converging
            );
        }
        None => println!("\n  drift not measurable"),
    }

    if let Some(final_curve) = curves.last() {
        let expected = final_curve.converged * mean_z
            - final_curve.stopped * fund::portfolio::screen::STOP_LOSS_WIDENING;
        println!(
            "\n  expected value at horizon {}: {expected:+.4} sigma per entry",
            final_curve.horizon
        );
        println!(
            "  round-trip cost is about {EFFECTIVE_SPREAD_BASIS_POINTS:.0} basis points; a sigma \
             is worth that only if the fitted spread is wider than it"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn arguments(values: &[&str]) -> Vec<String> {
        values.iter().map(|value| value.to_string()).collect()
    }

    #[test]
    fn test_the_defaults_and_overrides_parse() {
        let parameters = Parameters::parse(&arguments(&["2026-08-20"])).unwrap();
        assert_eq!(parameters.lookback_days, 90);
        assert_eq!(parameters.universe, 200);

        let parameters = Parameters::parse(&arguments(&["2026-08-20", "30", "50"])).unwrap();
        assert_eq!(parameters.lookback_days, 30);
        assert_eq!(parameters.universe, 50);
    }

    /// One ticker makes no pairs, so a universe of one would measure nothing and report it as a
    /// clean null rather than as a refusal.
    #[test]
    fn test_an_unusable_window_is_refused() {
        assert!(Parameters::parse(&arguments(&["2026-08-20", "30", "1"])).is_err());
        assert!(Parameters::parse(&arguments(&["2026-08-20", "0"])).is_err());
        assert!(Parameters::parse(&arguments(&["not-a-date"])).is_err());
        assert!(Parameters::parse(&[]).is_err());
    }
}
