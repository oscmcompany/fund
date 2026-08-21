//! Measures what a five-minute bar predicts about the next one, and whether bounce explains it.
//!
//! Trains nothing, and measures inside a session so no reading is an overnight return.

use chrono::NaiveDate;
use std::collections::BTreeMap;

use chrono::Timelike;
use tracing::{error, info, warn};

use fund::common::alpaca::{AlpacaCredentials, TradingClient};
use fund::common::log::init_tracing;
use fund::common::types::{BarInterval, SessionDate};
use fund::laboratory::dataset;
use fund::laboratory::intraday::{self, BounceReading, SessionHours, SessionReturns};
use fund::laboratory::metrics;
use fund::laboratory::predictor::{
    evaluate, Momentum, Panel, Persistence, Predictor, RandomRanking,
};

const USAGE: &str = "Usage: laboratory_intraday_baselines END_SESSION [LOOKBACK_DAYS]\n\
                     END_SESSION is an Eastern calendar date: YYYY-MM-DD.";

/// Calendar days of intraday archive to measure over by default.
///
/// Short next to the daily baselines' 730, because a session carries ~78 bars per name rather than
/// one: ninety days is already millions of observations, and the whole archive is gigabytes.
const DEFAULT_LOOKBACK_DAYS: i64 = 90;

/// Bars the momentum baseline sums over.
///
/// Four bars is twenty minutes, chosen to sit above the one-bar horizon bounce contaminates and
/// below the horizon at which a session runs out of room.
const MOMENTUM_BARS: usize = 4;

/// Fixed, so two runs over one archive draw the same orderings and differ only where the data does.
const RANDOM_SEED: u64 = 0x5EED;

/// Bars to skip in the controls that separate real reversion from bid-ask bounce.
///
/// Two is the one that matters — bounce lives between *adjacent* closes — and three is carried so a
/// reading at two cannot be mistaken for the start of a decay it is not part of.
const SKIPS: [usize; 2] = [2, 3];

struct Parameters {
    session: SessionDate,
    lookback_days: i64,
}

impl Parameters {
    fn parse(arguments: &[String]) -> Result<Self, String> {
        let (session, lookback) = match arguments {
            [session] => (session, DEFAULT_LOOKBACK_DAYS),
            [session, lookback] => (
                session,
                lookback
                    .trim()
                    .parse::<i64>()
                    .map_err(|_| format!("LOOKBACK_DAYS must be a number\n{USAGE}"))?,
            ),
            _ => return Err(format!("Expected an end session\n{USAGE}")),
        };
        if lookback <= 0 {
            return Err(format!("LOOKBACK_DAYS must be positive\n{USAGE}"));
        }
        let session = NaiveDate::parse_from_str(session.trim(), "%Y-%m-%d")
            .map(SessionDate::from_date)
            .map_err(|_| format!("END_SESSION must be YYYY-MM-DD\n{USAGE}"))?;

        Ok(Self {
            session,
            lookback_days: lookback,
        })
    }
}

#[tokio::main]
async fn main() {
    fund::common::crypto::install_default_crypto_provider();
    let tracing_guard = init_tracing(
        "laboratory-intraday-baselines.log",
        Some("info"),
        "laboratory-intraday-baselines",
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
            error!(%error, "The intraday baselines failed");
            eprintln!("The intraday baselines failed: {error}");
            1
        }
    };
    drop(tracing_guard);
    std::process::exit(code);
}

async fn run(parameters: &Parameters) -> Result<(), Box<dyn std::error::Error>> {
    let bucket = std::env::var("AWS_S3_BUCKET_NAME")
        .map_err(|_| "AWS_S3_BUCKET_NAME must be set (the equity-bar data bucket)")?;
    let s3_client = fund::common::aws::s3_client().await;

    info!(
        bucket,
        session = %parameters.session,
        lookback_days = parameters.lookback_days,
        "Reading the intraday archive"
    );
    let dataset = dataset::intraday(
        &s3_client,
        &bucket,
        BarInterval::FiveMinute,
        parameters.lookback_days,
        parameters.session,
    )
    .await?;

    let hours = session_hours(parameters).await?;
    let sessions = intraday::session_returns(&dataset.bars, BarInterval::FiveMinute, &hours)?;
    if sessions.is_empty() {
        return Err("the window produced no intraday returns".into());
    }
    let names: usize = sessions.iter().map(SessionReturns::names).sum();
    let observations: usize = sessions.iter().map(SessionReturns::observations).sum();
    println!(
        "window: {} sessions, {} name-sessions, {} five-minute returns",
        sessions.len(),
        names,
        observations
    );

    report_bounce(&sessions);
    report_baselines(&sessions);
    Ok(())
}

/// The exchange's published hours for every session in the window.
///
/// From the calendar rather than assumed, because a half-day closes at 13:00 and a fixed 16:00
/// would read three hours of post-market prints as regular bars.
async fn session_hours(
    parameters: &Parameters,
) -> Result<BTreeMap<SessionDate, SessionHours>, Box<dyn std::error::Error>> {
    let client = TradingClient::from_env(AlpacaCredentials::from_env()?);
    let start = parameters.session.date() - chrono::Duration::days(parameters.lookback_days);
    let days = client
        .fetch_calendar(start, parameters.session.date())
        .await?;

    let mut hours = BTreeMap::new();
    let mut early = 0_usize;
    for day in days {
        let open = day.session_open().hour() * 60 + day.session_open().minute();
        let close = day.session_close().hour() * 60 + day.session_close().minute();
        let Some(published) = SessionHours::new(open, close) else {
            continue;
        };
        if published != SessionHours::regular() {
            early += 1;
        }
        hours.insert(SessionDate::from_date(day.session_date()), published);
    }
    info!(
        sessions = hours.len(),
        irregular = early,
        "Read the exchange calendar"
    );
    Ok(hours)
}

/// Prints the bounce reading, which decides whether anything below it can be believed.
fn report_bounce(sessions: &[SessionReturns]) {
    let Some(BounceReading {
        lag_one,
        lag_two,
        roll_spread,
        measured,
        share_negative,
    }) = intraday::bounce(sessions)
    else {
        println!("\nbounce: no name-session carried enough returns to measure");
        return;
    };

    println!("\nbid-ask bounce, over {measured} name-sessions");
    println!("  lag-1 autocorrelation  {lag_one:+.4}");
    match lag_two {
        Some(value) => println!("  lag-2 autocorrelation  {value:+.4}"),
        None => println!("  lag-2 autocorrelation  not estimable"),
    }
    println!("  share negative at lag-1 {share_negative:.4}");
    match roll_spread {
        Some(spread) => println!("  Roll effective spread   {:.4}%", spread * 100.0),
        None => println!("  Roll effective spread   not estimable"),
    }
}

/// Prints each baseline's information coefficient, pooled over sessions.
///
/// One panel per session and the coefficients averaged: a panel spanning the window would put the
/// overnight gap on the time axis, which is the mismatch this whole measurement exists to avoid.
fn report_baselines(sessions: &[SessionReturns]) {
    let mut predictors: Vec<(String, Box<dyn Predictor>)> = vec![
        ("persistence".to_string(), Box::new(Persistence)),
        (
            "momentum".to_string(),
            Box::new(Momentum {
                sessions: MOMENTUM_BARS,
            }),
        ),
        (
            "random".to_string(),
            Box::new(RandomRanking { seed: RANDOM_SEED }),
        ),
    ];
    // The skip-a-bar controls, which are what make the persistence row interpretable: bounce lives
    // between adjacent closes, so it should fade here while real reversion should not.
    for skip in SKIPS {
        if let Some(skipped) = intraday::SkippedPersistence::new(skip) {
            predictors.push((format!("persistence skip-{skip}"), Box::new(skipped)));
        }
    }

    println!("\nbaselines, one information coefficient per session");
    for (name, predictor) in &predictors {
        // One reading per session, not one per bar. Bars within a session overlap in the names and
        // history they read, and this module's own finding is that they are serially dependent, so
        // pooling them and dividing by the square root of their count overstates significance.
        let mut readings: Vec<Option<f64>> = Vec::new();
        let mut skipped = 0_usize;
        for session in sessions {
            let panel = intraday::panel_frame(session)
                .map_err(|error| error.to_string())
                .and_then(|frame| {
                    Panel::from_frame_of(&frame, intraday::INTRADAY_RETURN_COLUMN)
                        .map_err(|error| error.to_string())
                });
            match panel {
                Ok(panel) => readings.push(
                    evaluate(predictor.as_ref(), &panel)
                        .information_coefficient
                        .map(|distribution| distribution.mean),
                ),
                // Named rather than dropped: a silently skipped session leaves the row below
                // looking complete over a window it did not cover.
                Err(error) => {
                    skipped += 1;
                    warn!(%error, session = %session.session(), "Skipped a session");
                }
            }
        }
        if skipped > 0 {
            println!("  ({skipped} sessions skipped; see the log)");
        }
        match metrics::summarize(readings.into_iter()) {
            Some(distribution) => {
                let ratio = if distribution.standard_error > 0.0 {
                    distribution.mean / distribution.standard_error
                } else {
                    0.0
                };
                println!(
                    "  {name:<20} {:+.5}  se {:.5}  {ratio:+.2} standard errors  over {} sessions",
                    distribution.mean, distribution.standard_error, distribution.sessions
                );
            }
            None => println!("  {name:<20} not measurable over this window"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn arguments(values: &[&str]) -> Vec<String> {
        values.iter().map(|value| value.to_string()).collect()
    }

    #[test]
    fn test_the_lookback_defaults_and_parses() {
        let parameters = Parameters::parse(&arguments(&["2026-08-20"])).unwrap();
        assert_eq!(parameters.lookback_days, 90);
        assert_eq!(
            parameters.session,
            SessionDate::from_date(NaiveDate::from_ymd_opt(2026, 8, 20).unwrap())
        );

        let parameters = Parameters::parse(&arguments(&["2026-08-20", "30"])).unwrap();
        assert_eq!(parameters.lookback_days, 30);
    }

    #[test]
    fn test_an_unusable_window_is_refused() {
        assert!(Parameters::parse(&arguments(&["2026-08-20", "0"])).is_err());
        assert!(Parameters::parse(&arguments(&["2026-08-20", "-5"])).is_err());
        assert!(Parameters::parse(&arguments(&["not-a-date"])).is_err());
        assert!(Parameters::parse(&[]).is_err());
    }
}
