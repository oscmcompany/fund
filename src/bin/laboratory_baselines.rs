//! Measures the forecasts a model has to beat, over the archive and nothing else.
//!
//! Trains nothing: every forecast here is a rule, so a run is one archive read and some arithmetic.
//! That is what makes it the thing to run first — it says what the data alone is worth.

use chrono::Utc;
use tracing::{error, info, warn};

use fund::common::log::init_tracing;
use fund::common::types::SessionDate;
use fund::laboratory::dataset;
use fund::laboratory::journal as laboratory;
use fund::laboratory::metrics::Distribution;
use fund::laboratory::predictor::{
    evaluate, CrossSectionalMean, Momentum, Panel, Persistence, Predictor, RandomRanking,
};

const USAGE: &str = "Usage: laboratory_baselines [LOOKBACK_DAYS] [MOMENTUM_SESSIONS]";

/// Calendar days of archive to measure over by default.
///
/// Twice the trainer's window, because nothing here is trained and a standard error over one year
/// of sessions is wide enough to call a real weak effect nothing.
const DEFAULT_LOOKBACK_DAYS: i64 = 730;

/// Sessions the momentum baseline sums over by default.
const DEFAULT_MOMENTUM_SESSIONS: i64 = 20;

/// Fixed, so two runs over one archive draw the same orderings and differ only where the data does.
const RANDOM_SEED: u64 = 0x5EED;

/// What to measure, and over how much.
struct Parameters {
    lookback_days: i64,
    momentum_sessions: usize,
}

impl Parameters {
    /// Reads the two positional arguments, each falling back to its default.
    fn parse(arguments: &[String]) -> Result<Self, String> {
        let (lookback_days, momentum_sessions) = match arguments {
            [] => (DEFAULT_LOOKBACK_DAYS, DEFAULT_MOMENTUM_SESSIONS),
            [lookback] => (
                positive(lookback, "LOOKBACK_DAYS")?,
                DEFAULT_MOMENTUM_SESSIONS,
            ),
            [lookback, momentum] => (
                positive(lookback, "LOOKBACK_DAYS")?,
                positive(momentum, "MOMENTUM_SESSIONS")?,
            ),
            _ => return Err(format!("Too many arguments\n{USAGE}")),
        };
        Ok(Self {
            lookback_days,
            momentum_sessions: momentum_sessions as usize,
        })
    }
}

/// Parses a positive integer, refusing a typo rather than falling back to the default.
///
/// An operator who passed a window is asking for that window; quietly measuring a different one
/// would put a number in the journal against a fingerprint nobody chose.
fn positive(raw: &str, name: &str) -> Result<i64, String> {
    let value: i64 = raw
        .trim()
        .parse()
        .map_err(|_| format!("{name} must be a positive integer, got {raw:?}\n{USAGE}"))?;
    if value <= 0 {
        return Err(format!(
            "{name} must be greater than zero, got {value}\n{USAGE}"
        ));
    }
    Ok(value)
}

#[tokio::main]
async fn main() {
    fund::common::crypto::install_default_crypto_provider();
    let tracing_guard = init_tracing(
        "laboratory-baselines.log",
        Some("info"),
        "laboratory-baselines",
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
        Ok(scored) => {
            println!("{}", render(&scored));
            0
        }
        Err(error) => {
            error!(%error, "Measuring the baselines failed");
            eprintln!("Measuring the baselines failed: {error}");
            1
        }
    };

    // `std::process::exit` runs no destructors, so the non-blocking appender's guard would never
    // drop and its buffered lines would be lost — exactly when the failure log matters.
    drop(tracing_guard);
    std::process::exit(code);
}

/// Reads the window, lays out the panel, and scores every baseline against it.
///
/// Reads `AWS_S3_BUCKET_NAME` and writes to the laboratory journal. Nothing is fetched and nothing
/// is published: this measures what the archive already holds.
async fn run(
    parameters: &Parameters,
) -> Result<Vec<laboratory::ForecastScored>, Box<dyn std::error::Error>> {
    let bucket = std::env::var("AWS_S3_BUCKET_NAME")
        .map_err(|_| "AWS_S3_BUCKET_NAME must be set (the equity-bar data bucket)")?;
    let s3_client = fund::common::aws::s3_client().await;

    // One instant for the run, resolved once to its Eastern session, as the trainer does: the window
    // is bounded by a trading day and the journal is stamped with an instant.
    let now = Utc::now();
    let session = SessionDate::at(now);

    let run_id = uuid::Uuid::new_v4();
    let journal = match laboratory::Journal::from_env() {
        Ok(journal) => Some(journal),
        Err(error) => {
            warn!(%error, "No laboratory journal; this run is not recorded");
            None
        }
    };

    info!(
        bucket,
        lookback_days = parameters.lookback_days,
        momentum_sessions = parameters.momentum_sessions,
        %session,
        %run_id,
        "Measuring the baselines"
    );

    let dataset = dataset::returns(&s3_client, &bucket, parameters.lookback_days, session).await?;
    let fingerprint = dataset.fingerprint;
    info!(
        rows = fingerprint.rows,
        tickers = fingerprint.tickers,
        "Read the archive window"
    );

    if let Some(journal) = journal.as_ref() {
        // The same record the trainer writes, so a baseline and a model measured over one window
        // share a fingerprint and their results can be put side by side.
        journal
            .record(
                run_id,
                Utc::now(),
                laboratory::Observation::DatasetBuilt(laboratory::DatasetBuilt {
                    fingerprint,
                    revision: std::env::var("FUND_REVISION").ok(),
                }),
            )
            .await;
    }

    let panel = Panel::from_frame(&dataset.returns)?;
    info!(
        sessions = panel.sessions(),
        tickers = panel.tickers(),
        "Laid the window out session by session"
    );

    let baselines: Vec<Box<dyn Predictor>> = vec![
        Box::new(CrossSectionalMean),
        Box::new(Persistence),
        Box::new(Momentum {
            sessions: parameters.momentum_sessions,
        }),
        Box::new(RandomRanking { seed: RANDOM_SEED }),
    ];

    let mut scored = Vec::with_capacity(baselines.len());
    for baseline in &baselines {
        let evaluation = evaluate(baseline.as_ref(), &panel);
        let record = laboratory::ForecastScored::from(&evaluation);
        info!(
            predictor = record.predictor,
            sessions = record.sessions,
            information_coefficient = record
                .information_coefficient
                .map(|distribution| distribution.mean),
            "Scored a baseline"
        );

        if let Some(journal) = journal.as_ref() {
            journal
                .record(
                    run_id,
                    Utc::now(),
                    laboratory::Observation::ForecastScored(record.clone()),
                )
                .await;
        }
        scored.push(record);
    }

    Ok(scored)
}

/// One row per baseline, for the operator who ran it.
fn render(scored: &[laboratory::ForecastScored]) -> String {
    let mut rendered = format!(
        "{:<22}{:>10}{:>30}{:>30}{:>30}\n",
        "predictor", "sessions", "information_coefficient", "decile_spread", "directional_accuracy"
    );
    for record in scored {
        rendered.push_str(&format!(
            "{:<22}{:>10}{:>30}{:>30}{:>30}\n",
            record.predictor,
            record.sessions,
            distribution(record.information_coefficient),
            distribution(record.decile_spread),
            distribution(record.directional_accuracy),
        ));
    }
    rendered
}

/// A statistic with its standard error, or why there is none.
///
/// Rendered together because the mean alone invites reading 0.01 over a few hundred sessions as a
/// signal, and an absent distribution is a measurement that could not be made rather than a zero.
fn distribution(value: Option<Distribution>) -> String {
    value.map_or_else(
        || "unmeasurable".to_string(),
        |distribution| {
            format!(
                "{:+.6} ± {:.6} ({})",
                distribution.mean, distribution.standard_error, distribution.sessions
            )
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn arguments(values: &[&str]) -> Vec<String> {
        values.iter().map(|value| value.to_string()).collect()
    }

    #[test]
    fn test_no_arguments_measures_the_default_window() {
        let parameters = Parameters::parse(&[]).unwrap();
        assert_eq!(parameters.lookback_days, 730);
        assert_eq!(parameters.momentum_sessions, 20);
    }

    #[test]
    fn test_arguments_are_read_in_order_and_default_from_the_right() {
        let parameters = Parameters::parse(&arguments(&["365"])).unwrap();
        assert_eq!(parameters.lookback_days, 365);
        assert_eq!(parameters.momentum_sessions, 20);

        let parameters = Parameters::parse(&arguments(&["365", "5"])).unwrap();
        assert_eq!(parameters.lookback_days, 365);
        assert_eq!(parameters.momentum_sessions, 5);
    }

    /// A typo must stop the run rather than fall back. A baseline that quietly measured a different
    /// window would journal a number against a fingerprint nobody asked for, and the fingerprint is
    /// the only thing that makes two results comparable.
    #[test]
    fn test_an_unusable_argument_is_refused() {
        for value in ["3o5", "0", "-5", ""] {
            assert!(
                Parameters::parse(&arguments(&[value])).is_err(),
                "{value:?} must be refused"
            );
        }
        assert!(Parameters::parse(&arguments(&["365", "0"])).is_err());
        assert!(Parameters::parse(&arguments(&["365", "20", "7"])).is_err());
    }

    fn scored(
        predictor: &str,
        information_coefficient: Option<Distribution>,
    ) -> laboratory::ForecastScored {
        laboratory::ForecastScored {
            predictor: predictor.to_string(),
            sessions: 502,
            information_coefficient,
            decile_spread: None,
            directional_accuracy: None,
        }
    }

    /// The sign is what a reader acts on, and a negative coefficient is the expected result for a
    /// daily persistence forecast — so it must not be rendered bare, where it reads as a minus sign
    /// in a table of positives.
    #[test]
    fn test_a_rendered_statistic_carries_its_sign_and_its_error() {
        let rendered = render(&[scored(
            "persistence",
            Some(Distribution {
                mean: -0.013489,
                standard_error: 0.007973,
                sessions: 502,
            }),
        )]);
        assert!(
            rendered.contains("-0.013489 ± 0.007973 (502)"),
            "{rendered}"
        );
    }

    /// The cross-sectional mean cannot rank, so it has no information coefficient at all. Rendering
    /// that as `0.000000` would read as a forecast that ranked and got it exactly wrong-by-nothing.
    #[test]
    fn test_a_statistic_that_could_not_be_measured_is_not_rendered_as_zero() {
        let rendered = render(&[scored("cross_sectional_mean", None)]);
        assert!(rendered.contains("unmeasurable"), "{rendered}");
        assert!(!rendered.contains("0.000000"), "{rendered}");
    }
}
