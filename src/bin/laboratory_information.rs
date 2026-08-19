//! Ranks the model's inputs by how much each says about the session it precedes.
//!
//! Trains nothing. It asks whether there is anything to learn before any architecture is committed to.

use chrono::Utc;
use tracing::{error, info, warn};

use fund::common::log::init_tracing;
use fund::common::types::SessionDate;
use fund::laboratory::information::{self, Outcome, DEFAULT_BINS};
use fund::laboratory::journal as laboratory;
use fund::laboratory::metrics::{self, Distribution};
use fund::laboratory::{dataset, information::Paired};

use polars::prelude::*;
use rand::{rngs::StdRng, RngExt, SeedableRng};

const USAGE: &str = "Usage: laboratory_information [LOOKBACK_DAYS] [SEED] [OUTCOME]\n\
                     OUTCOME is signed (default), magnitude or direction";

/// The whole archive, because a feature worth keeping should show over two years and not one month.
const DEFAULT_LOOKBACK_DAYS: i64 = 730;

/// Seeds the shuffle behind every null. Fixed so a ranking can be got back.
const DEFAULT_SEED: i64 = 0x4E11;

/// Every feature the model reads that varies across a session's names.
///
/// The calendar columns are absent deliberately: `day_of_week`, `month` and the rest hold one value
/// for every name in a session, so their cross-sectional information is zero by construction rather
/// than by measurement. `ticker` is absent for the opposite reason — it is unique per name, so every
/// cell of the table would hold one observation and the statistic would be pure bias.
const CONTINUOUS_FEATURES: &[&str] = &[
    "open_price",
    "high_price",
    "low_price",
    "close_price",
    "volume",
    "volume_weighted_average_price",
    "daily_return",
];

const CATEGORICAL_FEATURES: &[&str] = &["sector", "industry"];

/// A feature with nothing in it, triaged alongside the real ones.
///
/// The null is subtracted from every figure below, and this is the row that shows the subtraction
/// works: a uniform draw must score zero excess bits. Reading a table of positive numbers without
/// it would leave no way to tell a real dependence from a null that is set too low.
const CONTROL_FEATURE: &str = "uniform_control";

struct Parameters {
    lookback_days: i64,
    seed: u64,
    outcome: Outcome,
}

impl Parameters {
    fn parse(arguments: &[String]) -> Result<Self, String> {
        let (lookback_days, seed) = match arguments {
            [] => (DEFAULT_LOOKBACK_DAYS, DEFAULT_SEED),
            [lookback] => (positive(lookback, "LOOKBACK_DAYS")?, DEFAULT_SEED),
            [lookback, seed] | [lookback, seed, _] => (
                positive(lookback, "LOOKBACK_DAYS")?,
                positive(seed, "SEED")?,
            ),
            _ => return Err(format!("Too many arguments\n{USAGE}")),
        };
        let outcome = match arguments {
            [_, _, outcome] => match outcome.trim() {
                "signed" => Outcome::Signed,
                "magnitude" => Outcome::Magnitude,
                "direction" => Outcome::Direction,
                other => {
                    return Err(format!(
                        "OUTCOME must be signed, magnitude or direction, got {other:?}\n{USAGE}"
                    ))
                }
            },
            _ => Outcome::Signed,
        };
        Ok(Self {
            lookback_days,
            seed: seed as u64,
            outcome,
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
        "laboratory-information.log",
        Some("info"),
        "laboratory-information",
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
        Ok(triaged) => {
            println!("{}", render(&triaged));
            0
        }
        Err(error) => {
            error!(%error, "Triaging the features failed");
            eprintln!("Triaging the features failed: {error}");
            1
        }
    };

    drop(tracing_guard);
    std::process::exit(code);
}

async fn run(
    parameters: &Parameters,
) -> Result<Vec<laboratory::FeatureTriaged>, Box<dyn std::error::Error>> {
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
        seed = parameters.seed,
        bins = DEFAULT_BINS,
        outcome = format!("{:?}", parameters.outcome),
        %session,
        %run_id,
        "Triaging the model's inputs"
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

    // The control is drawn once over the whole frame rather than per session, so it is independent
    // of everything including the session it lands in.
    let mut frame = dataset.returns;
    let mut generator = StdRng::seed_from_u64(parameters.seed);
    let control: Vec<f64> = (0..frame.height())
        .map(|_| generator.random::<f64>())
        .collect();
    frame.with_column(Column::new(CONTROL_FEATURE.into(), control))?;

    let mut triaged = Vec::new();
    for feature in CONTINUOUS_FEATURES.iter().chain([&CONTROL_FEATURE]) {
        let column = *feature;
        let paired = information::pair_with_next_session(
            &frame,
            column,
            parameters.outcome,
            move |frame| {
                Ok(frame
                    .column(column)?
                    .cast(&DataType::Float64)?
                    .f64()?
                    .into_no_null_iter()
                    .collect())
            },
        )?;
        triaged.push(triage(column, &paired, parameters.seed));
    }
    for feature in CATEGORICAL_FEATURES {
        let column = *feature;
        let paired = information::pair_with_next_session(
            &frame,
            column,
            parameters.outcome,
            move |frame| {
                let values: Vec<&str> = frame.column(column)?.str()?.into_no_null_iter().collect();
                Ok(information::category_bins(&values)
                    .into_iter()
                    .map(|bin| bin as f64)
                    .collect())
            },
        )?;
        triaged.push(triage(column, &paired, parameters.seed));
    }

    // Ranked, because the question is which input carries the most and a table in column order
    // makes that a search rather than a reading.
    triaged.sort_by(|left, right| {
        bits_of(right)
            .partial_cmp(&bits_of(left))
            .unwrap_or(std::cmp::Ordering::Equal)
    });

    for record in &triaged {
        info!(
            feature = record.feature,
            sessions = record.sessions,
            excess_bits = record.excess_bits.map(|value| value.mean),
            "Triaged a feature"
        );
        if let Some(journal) = journal.as_ref() {
            journal
                .record(
                    run_id,
                    Utc::now(),
                    laboratory::Observation::FeatureTriaged(record.clone()),
                )
                .await;
        }
    }

    Ok(triaged)
}

/// Measures every session's cross-section and summarizes the three figures across them.
fn triage(feature: &str, paired: &Paired, seed: u64) -> laboratory::FeatureTriaged {
    let measured: Vec<Option<information::SessionInformation>> = paired
        .iter()
        .map(|(session, (features, targets))| {
            // Keyed on the session rather than its position, so a run over a different window
            // shuffles each cross-section the same way this one did.
            information::measure_session(features, targets, DEFAULT_BINS, seed ^ *session as u64)
        })
        .collect();

    laboratory::FeatureTriaged {
        feature: feature.to_string(),
        sessions: paired.len(),
        bits: metrics::summarize(measured.iter().map(|value| value.map(|value| value.bits))),
        null_bits: metrics::summarize(
            measured
                .iter()
                .map(|value| value.map(|value| value.null_bits)),
        ),
        excess_bits: metrics::summarize(
            measured
                .iter()
                .map(|value| value.map(|value| value.excess())),
        ),
    }
}

fn bits_of(record: &laboratory::FeatureTriaged) -> f64 {
    record
        .excess_bits
        .map_or(f64::NEG_INFINITY, |value| value.mean)
}

fn render(triaged: &[laboratory::FeatureTriaged]) -> String {
    let mut rendered = format!(
        "{:<32}{:>10}{:>28}{:>28}{:>28}\n",
        "feature", "sessions", "excess_bits", "bits", "null_bits"
    );
    for record in triaged {
        rendered.push_str(&format!(
            "{:<32}{:>10}{:>28}{:>28}{:>28}\n",
            record.feature,
            record.sessions,
            distribution(record.excess_bits),
            distribution(record.bits),
            distribution(record.null_bits),
        ));
    }
    rendered
}

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
    fn test_arguments_default_from_the_right() {
        let parameters = Parameters::parse(&[]).unwrap();
        assert_eq!(parameters.lookback_days, 730);
        assert_eq!(parameters.seed, DEFAULT_SEED as u64);

        let parameters = Parameters::parse(&arguments(&["365", "3"])).unwrap();
        assert_eq!(parameters.lookback_days, 365);
        assert_eq!(parameters.seed, 3);
        assert_eq!(parameters.outcome, Outcome::Signed);

        let parameters = Parameters::parse(&arguments(&["365", "3", "magnitude"])).unwrap();
        assert_eq!(parameters.outcome, Outcome::Magnitude);
        assert_eq!(
            Parameters::parse(&arguments(&["365", "3", "signed"]))
                .unwrap()
                .outcome,
            Outcome::Signed
        );
    }

    #[test]
    fn test_an_unusable_argument_is_refused() {
        for value in ["7f", "0", "-1", ""] {
            assert!(
                Parameters::parse(&arguments(&[value])).is_err(),
                "{value:?} must be refused"
            );
        }
        assert!(Parameters::parse(&arguments(&["365", "0"])).is_err());
        assert!(Parameters::parse(&arguments(&["365", "3", "extra"])).is_err());
        assert!(Parameters::parse(&arguments(&["365", "3", "signed", "more"])).is_err());
    }

    /// The calendar columns are excluded because they hold one value for every name in a session,
    /// so a cross-sectional statistic over them is zero by construction. Naming them here keeps a
    /// later reader from adding them back and reading the zero as a measurement.
    #[test]
    fn test_the_triaged_features_are_the_ones_that_vary_within_a_session() {
        for constant in [
            "day_of_week",
            "day_of_month",
            "day_of_year",
            "month",
            "year",
        ] {
            assert!(!CONTINUOUS_FEATURES.contains(&constant));
            assert!(!CATEGORICAL_FEATURES.contains(&constant));
        }
        assert!(!CATEGORICAL_FEATURES.contains(&"ticker"));
        assert!(CONTINUOUS_FEATURES.contains(&"daily_return"));
    }
}
