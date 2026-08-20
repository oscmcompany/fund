//! Asks whether a dislocated spread closes, which is the premise the pair book rests on.
//!
//! Trains nothing and consults no model: convergence is a fact about prices, not a forecast.

use chrono::Utc;
use tracing::{error, info, warn};

use fund::common::log::init_tracing;
use fund::common::types::SessionDate;
use fund::laboratory::convergence::{self, Closes, Entry, Selection, HORIZONS};
use fund::laboratory::dataset;
use fund::laboratory::journal as laboratory;
use fund::laboratory::regime::segments;

const USAGE: &str = "Usage: laboratory_convergence [LOOKBACK_DAYS] [UNIVERSE_SIZE]";

const DEFAULT_LOOKBACK_DAYS: i64 = 730;

/// Names drawn from the archive's universe.
///
/// Pair enumeration is quadratic per session, so this is the knob that decides whether the run
/// takes minutes or hours. Two hundred names is ~19,900 pairs against the full universe's 660,000.
const DEFAULT_UNIVERSE_SIZE: i64 = 200;

/// Fixed, so the sample is the same universe every run and two runs are comparable.
const RANDOM_SEED: u64 = 0x5EED;

/// The control runs second, so it sits under the arm it is there to make readable.
const SELECTIONS: &[Selection] = &[Selection::Screened, Selection::Unscreened];

struct Parameters {
    lookback_days: i64,
    universe_size: usize,
}

impl Parameters {
    fn parse(arguments: &[String]) -> Result<Self, String> {
        let (lookback_days, universe_size) = match arguments {
            [] => (DEFAULT_LOOKBACK_DAYS, DEFAULT_UNIVERSE_SIZE),
            [lookback] => (positive(lookback, "LOOKBACK_DAYS")?, DEFAULT_UNIVERSE_SIZE),
            [lookback, universe] => (
                positive(lookback, "LOOKBACK_DAYS")?,
                positive(universe, "UNIVERSE_SIZE")?,
            ),
            _ => return Err(format!("Too many arguments\n{USAGE}")),
        };
        Ok(Self {
            lookback_days,
            universe_size: usize::try_from(universe_size).map_err(|_| {
                format!("UNIVERSE_SIZE is larger than this platform can index\n{USAGE}")
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
        "laboratory-convergence.log",
        Some("info"),
        "laboratory-convergence",
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
            error!(%error, "Measuring spread convergence failed");
            eprintln!("Measuring spread convergence failed: {error}");
            1
        }
    };

    drop(tracing_guard);
    std::process::exit(code);
}

/// Opens every entry the screen's price tests admit, follows each forward, and splits the result.
async fn run(
    parameters: &Parameters,
) -> Result<Vec<laboratory::ConvergenceMeasured>, Box<dyn std::error::Error>> {
    let bucket = std::env::var("AWS_S3_BUCKET_NAME")
        .map_err(|_| "AWS_S3_BUCKET_NAME must be set (the equity-bar data bucket)")?;
    let s3_client = fund::common::aws::s3_client().await;

    let session = SessionDate::at(Utc::now());
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
        universe_size = parameters.universe_size,
        horizons = HORIZONS,
        %session,
        %run_id,
        "Measuring spread convergence"
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

    let closes = Closes::from_frame(&dataset.returns)?;
    let universe = convergence::sample_universe(&closes, parameters.universe_size, RANDOM_SEED);
    info!(
        sessions = closes.sessions(),
        universe = universe.len(),
        pairs = universe.len() * universe.len().saturating_sub(1) / 2,
        "Sampled the universe"
    );

    let mut measured = Vec::new();
    for selection in SELECTIONS {
        // Every session once, then split by where each entry opened. Splitting first would enumerate
        // the same pairs twice over, and enumeration is the whole cost of the run.
        let mut entries: Vec<Entry> = Vec::new();
        for index in 0..closes.sessions() {
            entries.extend(convergence::entries_at(
                &closes, &universe, index, *selection,
            ));
        }
        let admitted = entries.len();
        let entries = convergence::without_reentry(entries);
        info!(
            selection = selection.as_str(),
            admitted,
            entries = entries.len(),
            "Opened every entry the price tests admit, once per episode"
        );

        for (segment, range) in segments(closes.sessions()) {
            let within: Vec<Entry> = entries
                .iter()
                .filter(|entry| range.contains(&entry.session))
                .cloned()
                .collect();
            let record = laboratory::ConvergenceMeasured {
                selection: selection.as_str().to_string(),
                segment: segment.to_string(),
                sessions: range.len(),
                universe: universe.len(),
                entries: within.len(),
                median_sessions_to_convergence: convergence::median_sessions_to_convergence(
                    &within,
                ),
                mean_entry_z_score: convergence::mean_entry_z_score(&within),
                curves: convergence::curves(&within),
            };
            info!(
                selection = record.selection,
                segment = record.segment,
                entries = record.entries,
                median = record.median_sessions_to_convergence,
                converged_at_five = record.curves.get(4).map(|curve| curve.converged),
                converged_at_twenty = record.curves.last().map(|curve| curve.converged),
                "Followed a cohort forward"
            );

            if let Some(journal) = journal.as_ref() {
                journal
                    .record(
                        run_id,
                        Utc::now(),
                        laboratory::Observation::ConvergenceMeasured(record.clone()),
                    )
                    .await;
            }
            measured.push(record);
        }
    }

    Ok(measured)
}

/// One block per stretch of the window, with the control beside the screened arm rather than under it.
///
/// Side by side because the only readable quantity is the difference: regression to the mean lands
/// on both arms, so a screened share means nothing until the control's share sits next to it.
fn render(measured: &[laboratory::ConvergenceMeasured]) -> String {
    let mut rendered = String::new();
    // In the order the run produced them, so the block order is the measurement's rather than a
    // second list here that can drift from it.
    let mut seen: Vec<&str> = Vec::new();
    for record in measured {
        if !seen.contains(&record.segment.as_str()) {
            seen.push(&record.segment);
        }
    }

    for segment in seen {
        let arms: Vec<&laboratory::ConvergenceMeasured> = measured
            .iter()
            .filter(|record| record.segment == segment)
            .collect();

        rendered.push_str(&format!("\n{segment}\n"));
        for arm in &arms {
            rendered.push_str(&format!(
                "  {:<12} {} entries, entered at z {}, median {} sessions to convergence\n",
                arm.selection,
                arm.entries,
                arm.mean_entry_z_score
                    .map_or_else(|| "unmeasured".to_string(), |mean| format!("{mean:.3}")),
                arm.median_sessions_to_convergence
                    .map_or_else(|| "no".to_string(), |median| format!("{median:.1}"))
            ));
        }

        rendered.push_str(&format!("{:>8}", "horizon"));
        for arm in &arms {
            rendered.push_str(&format!("{:>34}", arm.selection));
        }
        rendered.push('\n');
        rendered.push_str(&format!("{:>8}", ""));
        for _ in &arms {
            rendered.push_str(&format!(
                "{:>9}{:>9}{:>9}{:>7}",
                "converged", "stopped", "open", "n"
            ));
        }
        rendered.push('\n');

        for horizon in 1..=HORIZONS {
            rendered.push_str(&format!("{horizon:>8}"));
            for arm in &arms {
                // A horizon no entry was priced at prints as absent rather than as three zeros,
                // which would read as a cohort that was followed and did nothing.
                match arm.curves.iter().find(|curve| curve.horizon == horizon) {
                    Some(curve) if curve.entries > 0 => rendered.push_str(&format!(
                        "{:>9.4}{:>9.4}{:>9.4}{:>7}",
                        curve.converged, curve.stopped, curve.open, curve.entries
                    )),
                    Some(_) | None => rendered.push_str(&format!("{:>34}", "unmeasured")),
                }
            }
            rendered.push('\n');
        }
    }
    rendered
}

#[cfg(test)]
mod tests {
    use super::*;
    use fund::laboratory::convergence::Curve;

    fn arguments(values: &[&str]) -> Vec<String> {
        values.iter().map(|value| value.to_string()).collect()
    }

    #[test]
    fn test_arguments_default_from_the_right() {
        let parameters = Parameters::parse(&[]).unwrap();
        assert_eq!(parameters.lookback_days, 730);
        assert_eq!(parameters.universe_size, 200);

        let parameters = Parameters::parse(&arguments(&["365"])).unwrap();
        assert_eq!(parameters.lookback_days, 365);
        assert_eq!(parameters.universe_size, 200);

        let parameters = Parameters::parse(&arguments(&["365", "50"])).unwrap();
        assert_eq!(parameters.lookback_days, 365);
        assert_eq!(parameters.universe_size, 50);
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
        assert!(Parameters::parse(&arguments(&["365", "200", "7"])).is_err());
    }

    fn measurement(
        selection: &str,
        segment: &str,
        converged: f64,
    ) -> laboratory::ConvergenceMeasured {
        laboratory::ConvergenceMeasured {
            selection: selection.to_string(),
            segment: segment.to_string(),
            sessions: 499,
            universe: 200,
            entries: 1_284,
            median_sessions_to_convergence: Some(4.0),
            mean_entry_z_score: Some(2.61),
            curves: (1..=HORIZONS)
                .map(|horizon| Curve {
                    horizon,
                    converged,
                    stopped: 0.1,
                    open: 0.9 - converged,
                    entries: 1_284,
                })
                .collect(),
        }
    }

    /// The control belongs on the same row as the arm it controls. Regression to the mean lands on
    /// both, so a screened share printed alone reads as a result when it may be arithmetic.
    #[test]
    fn test_an_arm_and_its_control_share_a_row() {
        let rendered = render(&[
            measurement("screened", "whole", 0.42),
            measurement("unscreened", "whole", 0.37),
        ]);

        let row = rendered
            .lines()
            .find(|line| line.trim_start().starts_with("5 "))
            .unwrap_or_else(|| panic!("{rendered}"));
        assert!(row.contains("0.4200"), "{row}");
        assert!(row.contains("0.3700"), "the control shares the row: {row}");
    }

    /// Each stretch of the window is its own block, or the replication reads as more of the same
    /// measurement rather than as a check on it.
    #[test]
    fn test_each_stretch_is_its_own_block() {
        let rendered = render(&[
            measurement("screened", "whole", 0.42),
            measurement("screened", "first_half", 0.11),
            measurement("screened", "second_half", 0.73),
        ]);

        for segment in ["whole", "first_half", "second_half"] {
            assert_eq!(
                rendered.matches(segment).count(),
                1,
                "{segment} heads exactly one block: {rendered}"
            );
        }
        assert!(
            rendered.contains("0.1100") && rendered.contains("0.7300"),
            "{rendered}"
        );
    }

    /// A horizon no entry was priced at prints as absent, not as three zeros — which would read as
    /// a cohort that was followed and stayed open rather than one that could not be read.
    #[test]
    fn test_a_horizon_with_no_entries_is_not_rendered_as_zero() {
        let mut record = measurement("screened", "whole", 0.42);
        record.curves[4].entries = 0;

        let rendered = render(&[record]);
        let row = rendered
            .lines()
            .find(|line| line.trim_start().starts_with("5 "))
            .unwrap_or_else(|| panic!("{rendered}"));
        assert!(row.contains("unmeasured"), "{row}");
        assert!(!row.contains("0.0000"), "{row}");
    }

    /// A cohort that never converged reports no median rather than a zero, which would read as
    /// convergence on the entry session itself.
    #[test]
    fn test_a_cohort_that_never_converged_has_no_median() {
        let mut record = measurement("screened", "whole", 0.0);
        record.median_sessions_to_convergence = None;

        let rendered = render(&[record]);
        assert!(
            rendered.contains("no sessions to convergence"),
            "{rendered}"
        );
        assert!(!rendered.contains("0.0 sessions"), "{rendered}");
    }
}
