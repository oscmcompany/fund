//! Does a dislocated pair converge inside the session, and does it beat the spread it pays?
//!
//! Screened pairs against an unscreened control, both measured on intraday volume-weighted prices.

use chrono::NaiveDate;
use tracing::{error, info};

use fund::common::alpaca::{AlpacaCredentials, TradingClient};
use fund::common::log::init_tracing;
use fund::common::types::{BarInterval, BasisPoints, SessionDate};
use fund::laboratory::convergence::{
    curves_of, sample_universe, state_at, Closes, Curve, Selection,
};
use fund::laboratory::cost::{CostModel, FillStyle, RoundTrip};
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
/// Two hundred names is ~19,900 pairs, matching what the daily measurement runs over.
const DEFAULT_UNIVERSE: usize = 200;

/// Fixed, so two runs over one archive draw the same sample and differ only where the data does.
const SAMPLE_SEED: u64 = 0x5EED;

/// The spread this run prices its hurdle against, until a per-name reading is loaded beside the
/// pairs.
///
/// A placeholder rather than a measurement: archived spreads run from 0.26bp to 18bp, so one figure
/// understates the tight names and overstates the wide ones. Cost in z-score units needs the fitted
/// sigma, so it is reported in basis points beside the result rather than folded into it.
const PLACEHOLDER_QUOTED_SPREAD_BASIS_POINTS: f64 = 10.0;

/// What this study assumes about reaching the book: both legs crossed, in and out.
const COST_MODEL: CostModel = CostModel::new(FillStyle::Aggressive, RoundTrip::PAIR);

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
    let bucket = fund::common::aws::archive_bucket()?;
    let s3_client = fund::common::aws::s3_client().await;

    // The models are fitted on daily closes, as production fits them, so the daily window must
    // cover the correlation window ahead of the first session judged as well as the window itself.
    let daily = dataset::returns(
        &s3_client,
        &bucket,
        parameters.lookback_days + 150,
        parameters.session,
        dataset::RESEARCH_SCREEN,
        dataset::Microstructure::Omitted,
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
        dataset::RESEARCH_SCREEN,
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
    let states: Vec<_> = entries
        .iter()
        .map(|entry| (entry.session, entry.resolution, entry.observed))
        .collect();
    let curves = curves_of(&states);

    match intraday_convergence::mean_entry_z_score(entries) {
        Some(mean_z_score) => println!("entries {}, mean entry z {mean_z_score:.3}", entries.len()),
        None => println!("entries {}, mean entry z not measurable", entries.len()),
    }
    println!("  horizon  entries  sessions  converged      se  stopped  open");
    for Curve {
        horizon,
        converged,
        stopped,
        open,
        entries: standing,
        sessions,
        converged_standard_error,
    } in &curves
    {
        let error = converged_standard_error
            .map_or_else(|| "      -".to_string(), |value| format!("{value:>7.4}"));
        println!(
            "  {horizon:>7}  {standing:>7}  {sessions:>8}  {converged:>9.4}  {error}  \
             {stopped:>7.4}  {open:>5.4}"
        );
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
        // The shares count only the entries still standing at this horizon, so the z-score they are
        // multiplied by has to be taken over that same cohort and not over every entry opened.
        let standing = standing_at(entries, final_curve.horizon);
        match intraday_convergence::mean_entry_z_score(&standing) {
            Some(mean_z_score) => {
                let expected = final_curve.converged * mean_z_score
                    - final_curve.stopped * fund::portfolio::screen::STOP_LOSS_WIDENING;
                println!(
                    "\n  expected value at horizon {}: {expected:+.4} sigma per entry, over the \
                     {} entries standing there (mean entry z {mean_z_score:.3})",
                    final_curve.horizon,
                    standing.len()
                );
            }
            None => println!(
                "\n  expected value at horizon {}: no entry stands there to value",
                final_curve.horizon
            ),
        }
        // `expect` on a source literal rather than on a reading: the placeholder is fixed above,
        // so a failure here is an edit to this file and not anything the archive could produce.
        let quoted = BasisPoints::new(PLACEHOLDER_QUOTED_SPREAD_BASIS_POINTS)
            .expect("the placeholder spread must be a usable reading");
        match COST_MODEL.cost(quoted) {
            Ok(round_trip) => println!(
                "  pair round trip is about {round_trip} \
                 ({quoted} single-name quoted spread over {} names); a sigma is worth that only if \
                 the fitted spread is wider than it",
                COST_MODEL.round_trip().names()
            ),
            Err(refusal) => println!("  pair round trip is not priceable: {refusal}"),
        }
    }
}

/// The entries a horizon's shares are computed over, which is what those shares may be priced with.
fn standing_at(entries: &[IntradayEntry], horizon: usize) -> Vec<IntradayEntry> {
    entries
        .iter()
        .filter(|entry| state_at(entry.resolution, entry.observed, horizon).is_some())
        .cloned()
        .collect()
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

    use fund::laboratory::convergence::{Observed, Resolution};

    fn entry_with(long: &str, entry_z_score: f64, resolution: Resolution) -> IntradayEntry {
        IntradayEntry {
            session: SessionDate::from_date(NaiveDate::from_ymd_opt(2026, 6, 1).unwrap()),
            entry_bar: 0,
            long: long.to_string(),
            short: "SHORT".to_string(),
            entry_z_score,
            final_z_score: None,
            resolution,
            observed: Observed::default(),
        }
    }

    /// The shares at a horizon are over the entries still standing there, so the z-score they are
    /// priced with has to be over the same cohort. Taken over every entry opened, an entry never
    /// observed at that horizon lends its z-score to a share it is no part of.
    #[test]
    fn test_the_value_at_a_horizon_prices_only_the_entries_standing_there() {
        let entries = vec![
            entry_with("RESOLVED", 2.0, Resolution::Converged(2)),
            // Never priced at any horizon, so it stands nowhere.
            entry_with("TRUNCATED", 4.0, Resolution::Unresolved),
        ];

        let standing = standing_at(&entries, 20);

        assert_eq!(standing.len(), 1, "only one entry was seen that far");
        assert_eq!(standing[0].long, "RESOLVED");
        assert!(
            (intraday_convergence::mean_entry_z_score(&standing).unwrap() - 2.0).abs() < 1e-12,
            "over every entry opened it would have been 3.0"
        );
        assert!(
            standing_at(&entries, 1).is_empty(),
            "and a horizon before either was seen values nothing"
        );
    }

    /// This study prices a pair and not a single name, which is the study-level choice worth
    /// pinning here; the arithmetic itself belongs to `laboratory::cost` and is tested there.
    #[test]
    fn test_this_study_prices_a_pair_rather_than_a_single_name() {
        assert_eq!(COST_MODEL.round_trip().names(), 2);

        let hurdle = COST_MODEL
            .cost(BasisPoints::new(10.0).expect("ten basis points is a usable reading"))
            .expect("an aggressive pair fill is costable");

        assert!(
            (hurdle.value() - 20.0).abs() < 1e-12,
            "ten basis points a name is twenty for the pair, got {hurdle}"
        );
    }

    /// The placeholder has to survive its own validation, or `report` panics at run time.
    #[test]
    fn test_the_placeholder_spread_is_a_usable_reading() {
        assert!(BasisPoints::new(PLACEHOLDER_QUOTED_SPREAD_BASIS_POINTS).is_some());
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
