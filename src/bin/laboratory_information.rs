//! Ranks the model's inputs by how much each says about the session it precedes.
//!
//! Trains nothing. It asks whether there is anything to learn before any architecture is committed to.

use chrono::Utc;
use tracing::{error, info, warn};

use fund::common::log::init_tracing;
use fund::common::types::SessionDate;
use fund::laboratory::information::{self, Feature, Outcome, DEFAULT_BINS};
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
    // The microstructure columns, read for the first time by any study in this tree. They are the
    // whole reason the quote and trade archives exist, and until this ranking runs there is no
    // evidence either carries anything a daily bar does not already say.
    "quoted_spread_basis_points_mean",
    "quoted_spread_basis_points_median",
    "quoted_spread_basis_points_ninetieth_percentile",
    "quote_count",
    "covered_seconds",
    "signed_volume",
    "trade_count",
    "median_trade_size",
    "ninetieth_percentile_trade_size",
];

const CATEGORICAL_FEATURES: &[&str] = &["sector", "industry"];

/// Every feature's reading, and the panel they are shares of.
///
/// Carried together because a coverage figure is meaningless without the denominator it was taken
/// against, and the two would otherwise be recovered from different places by whoever renders them.
struct Triaged {
    readings: Vec<laboratory::FeatureTriaged>,
    panel_rows: usize,
}

/// The frame restricted to the rows where `column` holds a value.
fn defined_rows(frame: &DataFrame, column: &str) -> Result<DataFrame, PolarsError> {
    let defined = frame.column(column)?.is_not_null();
    frame.filter(&defined)
}

/// A feature with nothing in it, triaged alongside the real ones.
///
/// The null is subtracted from every figure below, and this is the row that shows the subtraction
/// works: a uniform draw must score zero excess bits.
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
            println!("{}", render(&triaged.readings, triaged.panel_rows));
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

async fn run(parameters: &Parameters) -> Result<Triaged, Box<dyn std::error::Error>> {
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
        seed = parameters.seed,
        bins = DEFAULT_BINS,
        outcome = format!("{:?}", parameters.outcome),
        %session,
        %run_id,
        "Triaging the model's inputs"
    );

    let dataset = dataset::returns(
        &s3_client,
        &bucket,
        parameters.lookback_days,
        session,
        dataset::RESEARCH_SCREEN,
        dataset::Microstructure::Joined,
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

    // The control is drawn once over the whole frame rather than per session, so it is independent
    // of everything including the session it lands in.
    let mut panel = dataset.returns;
    let mut generator = StdRng::seed_from_u64(parameters.seed);
    let control: Vec<f64> = (0..panel.height())
        .map(|_| generator.random::<f64>())
        .collect();
    panel.with_column(Column::new(CONTROL_FEATURE.into(), control))?;

    let mut triaged = Vec::new();
    for feature in CONTINUOUS_FEATURES.iter().chain([&CONTROL_FEATURE]) {
        let column = *feature;
        // Ranked over the rows where it is defined; the panel stays behind as the calendar, so a
        // session this feature is wholly absent from is still a gap rather than an adjacency.
        let defined = defined_rows(&panel, column)?;
        let measured_rows = defined.height();
        let paired = information::pair_with_next_session(
            &defined,
            &panel,
            column,
            parameters.outcome,
            move |frame| {
                Ok(Feature::Continuous(
                    frame
                        .column(column)?
                        .cast(&DataType::Float64)?
                        .f64()?
                        .into_no_null_iter()
                        .collect(),
                ))
            },
        )?;
        let mut reading = triage(column, &paired, parameters.seed);
        reading.defined_rows = measured_rows;
        triaged.push(reading);
    }
    for feature in CATEGORICAL_FEATURES {
        let column = *feature;
        // `clean_data` fills these for every row, so the defined set is the panel -- but recorded
        // from the frame rather than assumed, because "it is always defined" is how the continuous
        // loop's count came to be the only one that was true.
        let defined = defined_rows(&panel, column)?;
        let measured_rows = defined.height();
        let paired = information::pair_with_next_session(
            &defined,
            &panel,
            column,
            parameters.outcome,
            move |frame| {
                let values: Vec<&str> = frame.column(column)?.str()?.into_no_null_iter().collect();
                Ok(Feature::Nominal(information::category_bins(&values)))
            },
        )?;
        let mut reading = triage(column, &paired, parameters.seed);
        reading.defined_rows = measured_rows;
        triaged.push(reading);
    }

    // Ranked by the share rather than the raw bits: bits are capped by the target's own entropy, so
    // ranking on them ranks the ceilings wherever two runs cut the target differently.
    triaged.sort_by(|left, right| {
        share_of(right)
            .partial_cmp(&share_of(left))
            .unwrap_or(std::cmp::Ordering::Equal)
    });

    for record in &triaged {
        info!(
            feature = record.feature,
            sessions = record.sessions,
            excess_share = record.excess_share.map(|value| value.mean),
            excess_bits = record.excess_bits.map(|value| value.mean),
            target_entropy_bits = record.target_entropy_bits.map(|value| value.mean),
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

    Ok(Triaged {
        readings: triaged,
        panel_rows: panel.height(),
    })
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
        // Overwritten by the caller, which is the only place that knows how many rows it kept.
        defined_rows: 0,
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
        target_entropy_bits: metrics::summarize(
            measured
                .iter()
                .map(|value| value.map(|value| value.target_entropy)),
        ),
        excess_share: metrics::summarize(
            measured
                .iter()
                .map(|value| value.and_then(|value| value.excess_share())),
        ),
    }
}

/// What the ranking is by: excess bits as a share of what the target itself can carry.
fn share_of(record: &laboratory::FeatureTriaged) -> f64 {
    record
        .excess_share
        .map_or(f64::NEG_INFINITY, |value| value.mean)
}

/// The ranking, with each feature's coverage beside its reading.
///
/// `coverage` is the share of the panel the feature was defined on. Printed rather than journalled
/// alone because the directive is to report the undefined share *alongside* the estimate, and a
/// feature measured on two thirds of the panel renders identically to one measured on all of it.
fn render(triaged: &[laboratory::FeatureTriaged], panel_rows: usize) -> String {
    let mut rendered = format!(
        "{:<32}{:>10}{:>10}{:>28}{:>28}{:>28}{:>28}\n",
        "feature",
        "coverage",
        "sessions",
        "excess_share",
        "excess_bits",
        "target_entropy_bits",
        "null_bits"
    );
    for record in triaged {
        rendered.push_str(&format!(
            "{:<32}{:>10}{:>10}{:>28}{:>28}{:>28}{:>28}\n",
            record.feature,
            // Two decimals, so a gap of a few hundred rows in half a million cannot round to 100.
            format!(
                "{:.2}%",
                100.0 * record.defined_rows as f64 / panel_rows.max(1) as f64
            ),
            record.sessions,
            distribution(record.excess_share),
            distribution(record.excess_bits),
            distribution(record.target_entropy_bits),
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
        assert_eq!(parameters.seed, 19_985);

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

    fn distribution_of(mean: f64) -> Distribution {
        Distribution {
            mean,
            standard_error: 0.001,
            sessions: 499,
        }
    }

    /// The reported figure is the share of the target's own entropy, because raw bits are capped by
    /// that entropy and comparing them compares the caps.
    #[test]
    fn test_the_share_of_the_target_entropy_is_what_is_ranked_and_rendered() {
        let record = laboratory::FeatureTriaged {
            defined_rows: 500,
            feature: "daily_return".to_string(),
            sessions: 499,
            bits: Some(distribution_of(3.1000)),
            null_bits: Some(distribution_of(3.0000)),
            excess_bits: Some(distribution_of(0.1000)),
            target_entropy_bits: Some(distribution_of(3.3000)),
            excess_share: Some(distribution_of(0.0301)),
        };

        assert!((share_of(&record) - 0.0301).abs() < 1e-12);

        let rendered = render(std::slice::from_ref(&record), 1_000);
        assert!(rendered.contains("excess_share"), "{rendered}");
        assert!(rendered.contains("target_entropy_bits"), "{rendered}");
        assert!(rendered.contains("+0.030100"), "{rendered}");

        let unmeasured = laboratory::FeatureTriaged {
            excess_share: None,
            ..record
        };
        assert_eq!(
            share_of(&unmeasured),
            f64::NEG_INFINITY,
            "a feature with no share ranks last rather than at zero"
        );
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
