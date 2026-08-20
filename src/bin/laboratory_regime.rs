//! Asks whether a forecast's per-session reading follows from the state of the market.
//!
//! Trains nothing. Session stability ruled out a reading's own past; this asks about everything else.

use chrono::Utc;
use tracing::{error, info, warn};

use fund::common::log::init_tracing;
use fund::common::types::SessionDate;
use fund::laboratory::dataset;
use fund::laboratory::journal as laboratory;
use fund::laboratory::predictor::{
    evaluate, CrossSectionalMean, Momentum, Panel, Persistence, Predictor, RandomRanking,
};
use fund::laboratory::regime::{self, FIRST_HALF, SECOND_HALF, STATES, WHOLE};
use fund::laboratory::stability;

const USAGE: &str = "Usage: laboratory_regime [LOOKBACK_DAYS] [MOMENTUM_SESSIONS]";

const DEFAULT_LOOKBACK_DAYS: i64 = 730;
const DEFAULT_MOMENTUM_SESSIONS: i64 = 20;

/// Fixed, so the control draws the same orderings every run.
const RANDOM_SEED: u64 = 0x5EED;

/// The per-session statistic being explained.
const STATISTIC: &str = "information_coefficient";

/// How far ahead of the reading each state is read.
///
/// Zero describes the session itself, which explains a reading without anticipating one. One is the
/// session before, which is the only figure here a book could act on — and both are reported because
/// a relationship visible only at zero still composes into a tradeable one where the state itself is
/// forecastable, as volatility is.
const LAGS: &[usize] = &[0, 1];

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
    let tracing_guard = init_tracing("laboratory-regime.log", Some("info"), "laboratory-regime");

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
            error!(%error, "Measuring the regime association failed");
            eprintln!("Measuring the regime association failed: {error}");
            1
        }
    };

    drop(tracing_guard);
    std::process::exit(code);
}

/// Describes every session, then asks each baseline's readings what they follow from.
async fn run(
    parameters: &Parameters,
) -> Result<Vec<laboratory::RegimeMeasured>, Box<dyn std::error::Error>> {
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
        states = STATES.len(),
        %session,
        %run_id,
        "Measuring the regime association"
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
    let described = regime::describe(&panel);
    info!(
        sessions = panel.sessions(),
        described = described
            .iter()
            .filter(|state| state.dispersion.is_some())
            .count(),
        "Described every session"
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

    // The market does not depend on which forecast is being read against it, so describe each
    // state once rather than once per baseline.
    let states: Vec<(&str, Vec<Option<f64>>)> = STATES
        .iter()
        .map(|(name, read)| (*name, described.iter().map(read).collect()))
        .collect();

    let mut measured = Vec::new();
    for baseline in &baselines {
        let evaluation = evaluate(baseline.as_ref(), &panel);
        let readings: Vec<Option<f64>> = evaluation
            .sessions
            .iter()
            .map(|session| session.information_coefficient)
            .collect();

        for (name, state) in &states {
            // Both series cut with the same range, or a lag would pair a reading against the state
            // of a session outside the stretch being measured.
            for (segment, range) in regime::segments(panel.sessions()) {
                let state = &state[range.clone()];
                let readings = &readings[range];
                let record = laboratory::RegimeMeasured {
                    predictor: evaluation.predictor.clone(),
                    statistic: STATISTIC.to_string(),
                    state: name.to_string(),
                    segment: segment.to_string(),
                    sessions: readings.iter().flatten().count(),
                    associations: LAGS
                        .iter()
                        .filter_map(|lag| stability::association(state, readings, *lag))
                        .collect(),
                };
                info!(
                    predictor = record.predictor,
                    state = record.state,
                    segment = record.segment,
                    same_session = record
                        .associations
                        .iter()
                        .find(|measured| measured.lag == 0)
                        .map(|measured| measured.correlation),
                    prior_session = record
                        .associations
                        .iter()
                        .find(|measured| measured.lag == 1)
                        .map(|measured| measured.correlation),
                    "Asked what a reading follows from"
                );

                if let Some(journal) = journal.as_ref() {
                    journal
                        .record(
                            run_id,
                            Utc::now(),
                            laboratory::Observation::RegimeMeasured(record.clone()),
                        )
                        .await;
                }
                measured.push(record);
            }
        }
    }

    Ok(measured)
}

/// One block per forecast, one row per state and lag, one column per stretch of the window.
///
/// The halves sit beside the whole rather than under it, because the question they answer is whether
/// the whole's figure is one relationship or two unrelated ones that averaged into the look of one.
fn render(measured: &[laboratory::RegimeMeasured]) -> String {
    let mut rendered = String::new();
    let mut current = "";
    for record in measured {
        if record.predictor != current {
            current = &record.predictor;
            rendered.push_str(&format!(
                "\n{} ({})\n{:<24}{:>6}{:>26}{:>26}{:>26}\n",
                record.predictor,
                record.statistic,
                "state",
                "lag",
                "whole",
                "first half",
                "second half"
            ));
        }
        if record.segment != WHOLE {
            continue;
        }
        for lag in LAGS {
            rendered.push_str(&format!(
                "{:<24}{:>6}{:>26}{:>26}{:>26}\n",
                record.state,
                lag,
                correlation(measured, record, WHOLE, *lag),
                correlation(measured, record, FIRST_HALF, *lag),
                correlation(measured, record, SECOND_HALF, *lag),
            ));
        }
    }
    rendered
}

/// One association with its error, or why there is none.
fn correlation(
    measured: &[laboratory::RegimeMeasured],
    record: &laboratory::RegimeMeasured,
    segment: &str,
    lag: usize,
) -> String {
    measured
        .iter()
        .find(|other| {
            other.predictor == record.predictor
                && other.statistic == record.statistic
                && other.state == record.state
                && other.segment == segment
        })
        .and_then(|found| {
            found
                .associations
                .iter()
                .find(|association| association.lag == lag)
        })
        .map_or_else(
            || "unmeasurable".to_string(),
            |association| {
                format!(
                    "{:+.4} ± {:.4} ({})",
                    association.correlation, association.standard_error, association.pairs
                )
            },
        )
}

#[cfg(test)]
mod tests {
    use super::*;
    use fund::laboratory::stability::Association;

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

    fn measurement(
        state: &str,
        segment: &str,
        correlation: f64,
        lag: usize,
    ) -> laboratory::RegimeMeasured {
        laboratory::RegimeMeasured {
            predictor: "persistence".to_string(),
            statistic: STATISTIC.to_string(),
            state: state.to_string(),
            segment: segment.to_string(),
            sessions: 499,
            associations: vec![Association {
                lag,
                correlation,
                standard_error: 0.0448,
                pairs: 499,
            }],
        }
    }

    /// The whole and its halves belong in one row, because the question they answer together is
    /// whether the whole's figure is one relationship or two unrelated ones that averaged into the
    /// look of one.
    #[test]
    fn test_a_state_shows_its_whole_beside_its_halves() {
        let rendered = render(&[
            measurement("breadth", WHOLE, 0.1494, 1),
            measurement("breadth", FIRST_HALF, 0.1702, 1),
            measurement("breadth", SECOND_HALF, 0.1301, 1),
        ]);

        assert!(rendered.contains("first half"), "{rendered}");
        assert!(rendered.contains("second half"), "{rendered}");

        let row = rendered
            .lines()
            .find(|line| line.starts_with("breadth") && line.contains("+0.1494"))
            .unwrap_or_else(|| panic!("{rendered}"));
        assert!(row.contains("+0.1702"), "the halves share the row: {row}");
        assert!(row.contains("+0.1301"), "the halves share the row: {row}");
    }

    /// A half that could not be measured is not a zero, and rendering it as one would read as a
    /// replication that was run and failed rather than one that could not be run.
    #[test]
    fn test_an_unmeasured_half_is_not_rendered_as_zero() {
        let rendered = render(&[measurement("breadth", WHOLE, 0.1494, 1)]);
        assert!(rendered.contains("unmeasurable"), "{rendered}");
        assert!(!rendered.contains("+0.0000"), "{rendered}");
    }

    /// One header per forecast rather than one per row, or a table of four states across four
    /// forecasts is unreadable.
    #[test]
    fn test_each_forecast_is_headed_once() {
        let first = measurement("dispersion", WHOLE, 0.1, 1);
        let mut second = measurement("breadth", WHOLE, 0.1, 1);
        second.predictor = "random_ranking".to_string();

        let rendered = render(&[first, second]);
        assert_eq!(rendered.matches("persistence (").count(), 1, "{rendered}");
        assert_eq!(
            rendered.matches("random_ranking (").count(),
            1,
            "{rendered}"
        );
    }

    /// Only the whole opens a row. Letting every segment open one would print each state three
    /// times over and read as three separate measurements rather than one and its replications.
    #[test]
    fn test_a_state_opens_one_row_per_lag_and_not_one_per_segment() {
        let rendered = render(&[
            measurement("breadth", WHOLE, 0.1494, 1),
            measurement("breadth", FIRST_HALF, 0.1702, 1),
            measurement("breadth", SECOND_HALF, 0.1301, 1),
        ]);
        assert_eq!(
            rendered
                .lines()
                .filter(|line| line.starts_with("breadth"))
                .count(),
            2,
            "one row per lag, not one per segment: {rendered}"
        );
    }
}
