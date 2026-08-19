//! Asks whether a forecast's per-session reading says anything about the next session's.
//!
//! Trains nothing. It measures the one thing a cross-sectional statistic is blind to: time.

use chrono::Utc;
use tracing::{error, info, warn};

use fund::common::log::init_tracing;
use fund::common::types::SessionDate;
use fund::laboratory::dataset;
use fund::laboratory::journal as laboratory;
use fund::laboratory::predictor::{
    evaluate, CrossSectionalMean, Momentum, Panel, Persistence, Predictor, RandomRanking,
};
use fund::laboratory::stability::{self, DEFAULT_LAGS};

const USAGE: &str = "Usage: laboratory_stability [LOOKBACK_DAYS] [MOMENTUM_SESSIONS]";

/// Calendar days of archive to measure over by default, matching the baselines runner.
const DEFAULT_LOOKBACK_DAYS: i64 = 730;

/// Sessions the momentum baseline sums over by default.
const DEFAULT_MOMENTUM_SESSIONS: i64 = 20;

/// Fixed, so the control draws the same orderings every run.
const RANDOM_SEED: u64 = 0x5EED;

/// The per-session statistic followed through time.
///
/// The information coefficient and nothing else: it is the ordering the book acts on, and it is the
/// statistic whose per-session spread of 0.16 against a mean of zero raised the question.
const STATISTIC: &str = "information_coefficient";

struct Parameters {
    lookback_days: i64,
    momentum_sessions: usize,
}

impl Parameters {
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
            momentum_sessions: usize::try_from(momentum_sessions).map_err(|_| {
                format!("MOMENTUM_SESSIONS is larger than this platform can index\n{USAGE}")
            })?,
        })
    }
}

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
        "laboratory-stability.log",
        Some("info"),
        "laboratory-stability",
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
        Ok(measured) => {
            println!("{}", render(&measured));
            0
        }
        Err(error) => {
            error!(%error, "Measuring session stability failed");
            eprintln!("Measuring session stability failed: {error}");
            1
        }
    };

    drop(tracing_guard);
    std::process::exit(code);
}

/// Scores every baseline session by session, then follows each one's readings through time.
async fn run(
    parameters: &Parameters,
) -> Result<Vec<laboratory::StabilityMeasured>, Box<dyn std::error::Error>> {
    let bucket = std::env::var("AWS_S3_BUCKET_NAME")
        .map_err(|_| "AWS_S3_BUCKET_NAME must be set (the equity-bar data bucket)")?;
    let s3_client = fund::common::aws::s3_client().await;

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
        lags = DEFAULT_LAGS,
        %session,
        %run_id,
        "Measuring session stability"
    );

    let dataset = dataset::returns(&s3_client, &bucket, parameters.lookback_days, session).await?;
    let fingerprint = dataset.fingerprint.clone();
    info!(
        rows = fingerprint.rows,
        tickers = fingerprint.tickers,
        "Read the archive window"
    );
    if let Some(journal) = journal.as_ref() {
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

    // RandomRanking last, so the control sits at the foot of the table it makes readable.
    let baselines: Vec<Box<dyn Predictor>> = vec![
        Box::new(Persistence),
        Box::new(Momentum {
            sessions: parameters.momentum_sessions,
        }),
        Box::new(CrossSectionalMean),
        Box::new(RandomRanking { seed: RANDOM_SEED }),
    ];

    let mut measured = Vec::with_capacity(baselines.len());
    for baseline in &baselines {
        let evaluation = evaluate(baseline.as_ref(), &panel);
        let readings: Vec<Option<f64>> = evaluation
            .sessions
            .iter()
            .map(|session| session.information_coefficient)
            .collect();

        let record = laboratory::StabilityMeasured {
            predictor: evaluation.predictor.clone(),
            statistic: STATISTIC.to_string(),
            sessions: readings.iter().flatten().count(),
            autocorrelations: (1..=DEFAULT_LAGS)
                .filter_map(|lag| stability::autocorrelation(&readings, lag))
                .collect(),
            sign_agreements: (1..=DEFAULT_LAGS)
                .filter_map(|lag| stability::sign_agreement(&readings, lag))
                .collect(),
        };
        info!(
            predictor = record.predictor,
            sessions = record.sessions,
            first_lag = record
                .autocorrelations
                .first()
                .map(|measured| measured.correlation),
            first_lag_agreement = record.sign_agreements.first().map(|measured| measured.rate),
            "Followed a forecast through time"
        );

        if let Some(journal) = journal.as_ref() {
            journal
                .record(
                    run_id,
                    Utc::now(),
                    laboratory::Observation::StabilityMeasured(record.clone()),
                )
                .await;
        }
        measured.push(record);
    }

    Ok(measured)
}

/// One block per forecast, one row per lag.
fn render(measured: &[laboratory::StabilityMeasured]) -> String {
    let mut rendered = String::new();
    for record in measured {
        rendered.push_str(&format!(
            "\n{} ({}, {} sessions measured)\n{:>5}{:>28}{:>28}\n",
            record.predictor,
            record.statistic,
            record.sessions,
            "lag",
            "autocorrelation",
            "sign_agreement"
        ));
        for lag in 1..=DEFAULT_LAGS {
            let correlation = record
                .autocorrelations
                .iter()
                .find(|measured| measured.lag == lag);
            let agreement = record
                .sign_agreements
                .iter()
                .find(|measured| measured.lag == lag);
            if correlation.is_none() && agreement.is_none() {
                continue;
            }
            rendered.push_str(&format!(
                "{lag:>5}{:>28}{:>28}\n",
                correlation.map_or_else(
                    || "unmeasurable".to_string(),
                    |measured| format!(
                        "{:+.4} ± {:.4} ({})",
                        measured.correlation, measured.standard_error, measured.pairs
                    )
                ),
                agreement.map_or_else(
                    || "unmeasurable".to_string(),
                    |measured| format!(
                        "{:.4} ± {:.4} ({})",
                        measured.rate, measured.standard_error, measured.pairs
                    )
                ),
            ));
        }
    }
    rendered
}

#[cfg(test)]
mod tests {
    use super::*;
    use fund::laboratory::stability::{Association, SignAgreement};

    fn arguments(values: &[&str]) -> Vec<String> {
        values.iter().map(|value| value.to_string()).collect()
    }

    #[test]
    fn test_arguments_default_from_the_right() {
        let parameters = Parameters::parse(&[]).unwrap();
        assert_eq!(parameters.lookback_days, 730);
        assert_eq!(parameters.momentum_sessions, 20);

        let parameters = Parameters::parse(&arguments(&["365"])).unwrap();
        assert_eq!(parameters.lookback_days, 365);
        assert_eq!(parameters.momentum_sessions, 20);

        let parameters = Parameters::parse(&arguments(&["365", "5"])).unwrap();
        assert_eq!(parameters.lookback_days, 365);
        assert_eq!(parameters.momentum_sessions, 5);
    }

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

    /// The sign is the whole finding, and a negative autocorrelation is a different and tradeable
    /// answer from no autocorrelation — so it must not render bare in a column of positives.
    #[test]
    fn test_a_rendered_row_carries_its_sign_and_its_error() {
        let rendered = render(&[laboratory::StabilityMeasured {
            predictor: "persistence".to_string(),
            statistic: STATISTIC.to_string(),
            sessions: 498,
            autocorrelations: vec![Association {
                lag: 1,
                correlation: -0.0731,
                standard_error: 0.0448,
                pairs: 498,
            }],
            sign_agreements: vec![SignAgreement {
                lag: 1,
                rate: 0.5341,
                standard_error: 0.0224,
                pairs: 498,
            }],
        }]);

        assert!(rendered.contains("-0.0731 ± 0.0448 (498)"), "{rendered}");
        assert!(rendered.contains("0.5341 ± 0.0224 (498)"), "{rendered}");
        assert!(rendered.contains("persistence"), "{rendered}");
    }

    /// A forecast that never ranked has no series to follow. Rendering that as zero would read as a
    /// measured absence of memory rather than an absence of measurement.
    #[test]
    fn test_a_forecast_with_no_series_is_not_rendered_as_zero() {
        let rendered = render(&[laboratory::StabilityMeasured {
            predictor: "cross_sectional_mean".to_string(),
            statistic: STATISTIC.to_string(),
            sessions: 0,
            autocorrelations: Vec::new(),
            sign_agreements: Vec::new(),
        }]);

        assert!(rendered.contains("cross_sectional_mean"), "{rendered}");
        assert!(!rendered.contains("0.0000"), "{rendered}");
    }
}
