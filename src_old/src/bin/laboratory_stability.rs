//! Asks whether a forecast's per-session reading says anything about the next session's.
//!
//! Trains nothing. It measures the one thing a cross-sectional statistic is blind to: time.

use chrono::Utc;
use clap::builder::RangedU64ValueParser;
use clap::Parser;
use tracing::{error, info, warn};

use fund::common::log::init_tracing;
use fund::common::types::SessionDate;
use fund::laboratory::dataset;
use fund::laboratory::journal as laboratory;
use fund::laboratory::predictor::{
    evaluate, CrossSectionalMean, Momentum, Panel, Persistence, Predictor, RandomRanking,
};
use fund::laboratory::stability::{self, DEFAULT_LAGS};

/// Calendar days of archive to measure over by default, matching the baselines runner.
const DEFAULT_LOOKBACK_DAYS: i64 = 730;

/// Sessions the momentum baseline sums over by default.
const DEFAULT_MOMENTUM_SESSIONS: usize = 20;

/// Fixed, so the control draws the same orderings every run.
const RANDOM_SEED: u64 = 0x5EED;

/// The per-session statistic followed through time.
///
/// The information coefficient and nothing else: it is the ordering the book acts on, and it is the
/// statistic whose per-session spread of 0.16 against a mean of zero raised the question.
const STATISTIC: &str = "information_coefficient";

#[derive(Debug, Parser)]
#[command(
    name = "laboratory_stability",
    about = "Asks whether a forecast's per-session reading predicts the next session's"
)]
struct Arguments {
    #[arg(
        default_value_t = DEFAULT_LOOKBACK_DAYS,
        value_parser = clap::value_parser!(i64).range(1..),
    )]
    lookback_days: i64,
    #[arg(
        default_value_t = DEFAULT_MOMENTUM_SESSIONS,
        value_parser = RangedU64ValueParser::<usize>::new().range(1..),
    )]
    momentum_sessions: usize,
}

#[tokio::main]
async fn main() {
    let parameters = Arguments::parse();
    fund::common::crypto::install_default_crypto_provider();
    let tracing_guard = init_tracing(
        "laboratory-stability.log",
        Some("info"),
        "laboratory-stability",
    );

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
    parameters: &Arguments,
) -> Result<Vec<laboratory::StabilityMeasured>, Box<dyn std::error::Error>> {
    let bucket = fund::common::aws::archive_bucket()?;
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

    let dataset = dataset::returns(
        &s3_client,
        &bucket,
        parameters.lookback_days,
        session,
        dataset::RESEARCH_SCREEN,
        dataset::Microstructure::Omitted,
    )
    .await?;
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
                laboratory::Observation::DatasetBuilt(laboratory::DatasetBuilt::new(fingerprint)),
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

    fn parse(values: &[&str]) -> Result<Arguments, clap::Error> {
        Arguments::try_parse_from(
            std::iter::once("laboratory_stability").chain(values.iter().copied()),
        )
    }

    #[test]
    fn test_arguments_default_from_the_right() {
        let parameters = parse(&[]).unwrap();
        assert_eq!(parameters.lookback_days, 730);
        assert_eq!(parameters.momentum_sessions, 20);

        let parameters = parse(&["365"]).unwrap();
        assert_eq!(parameters.lookback_days, 365);
        assert_eq!(parameters.momentum_sessions, 20);

        let parameters = parse(&["365", "5"]).unwrap();
        assert_eq!(parameters.lookback_days, 365);
        assert_eq!(parameters.momentum_sessions, 5);
    }

    #[test]
    fn test_an_unusable_argument_is_refused() {
        for value in ["3o5", "0", "-5", ""] {
            assert!(parse(&[value]).is_err(), "{value:?} must be refused");
        }
        assert!(parse(&["365", "0"]).is_err());
        assert!(parse(&["365", "20", "7"]).is_err());
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
