//! Reports what the factor panel measures, what it refuses, and what it explains over nothing.
//!
//! Fits nothing beyond the per-session cross-section, so a run is one archive read and arithmetic.

use std::num::NonZeroUsize;

use chrono::Utc;
use rand::prelude::*;
use rand::rngs::StdRng;
use tracing::{error, info, warn};

use fund::common::types::SessionDate;
use fund::data::details::UNKNOWN;
use fund::laboratory::dataset;
use fund::laboratory::dataset::DatasetFingerprint;
use fund::laboratory::harness::{
    Arm, Declaration, DeclaredUniverse, Family, Horizon, Pairing, Quantity, ScreenWindow, Study,
    StudyResult,
};
use fund::laboratory::journal as laboratory;
use fund::laboratory::residual::{
    residual_returns, FactorSpecification, ResidualPanel, RESIDUAL_COLUMN,
};

use polars::prelude::*;

/// The multiple-testing bucket these comparisons are spent against.
const FAMILY: &str = "residual-panel";

/// Comparisons this binary draws, which is the denominator its threshold is set from.
///
/// Counted here rather than passed in, because it is a property of what the binary does: adding a
/// third comparison without raising this would spend a family's error rate three ways and report
/// the bar for two.
const TESTS_IN_FAMILY: usize = 2;

const TREATMENT_ARM: &str = "real sectors";
const CONTROL_ARM: &str = "permuted sectors";
const BASELINE_ARM: &str = "session mean";

const USAGE: &str =
    "Usage: laboratory_residuals [LOOKBACK_DAYS] [VOLATILITY_SESSIONS] [MINIMUM_VARIANCE_SHARE]";

/// Calendar days of archive to measure over by default, matching the baselines binary.
const DEFAULT_LOOKBACK_DAYS: i64 = 730;

/// Fixed, so the permutation control draws the same labels on every run over one archive.
const PERMUTATION_SEED: u64 = 0x5EED;

/// What to measure, and over how much.
struct Parameters {
    lookback_days: i64,
    specification: FactorSpecification,
}

impl Parameters {
    fn parse(arguments: &[String]) -> Result<Self, String> {
        let current = FactorSpecification::CURRENT;
        let (lookback_days, sessions, share) = match arguments {
            [] => (
                DEFAULT_LOOKBACK_DAYS,
                current.volatility_sessions() as i64,
                current.minimum_residual_variance_share(),
            ),
            [lookback] => (
                positive(lookback, "LOOKBACK_DAYS")?,
                current.volatility_sessions() as i64,
                current.minimum_residual_variance_share(),
            ),
            [lookback, sessions] => (
                positive(lookback, "LOOKBACK_DAYS")?,
                positive(sessions, "VOLATILITY_SESSIONS")?,
                current.minimum_residual_variance_share(),
            ),
            [lookback, sessions, share] => (
                positive(lookback, "LOOKBACK_DAYS")?,
                positive(sessions, "VOLATILITY_SESSIONS")?,
                share.trim().parse::<f64>().map_err(|_| {
                    format!("MINIMUM_VARIANCE_SHARE must be a number, got {share:?}\n{USAGE}")
                })?,
            ),
            _ => return Err(format!("Too many arguments\n{USAGE}")),
        };

        let sessions = usize::try_from(sessions).map_err(|_| {
            format!("VOLATILITY_SESSIONS is larger than this platform can index\n{USAGE}")
        })?;
        let specification = FactorSpecification::new(sessions, share).ok_or_else(|| {
            format!("{sessions} sessions at a {share} variance share cannot measure a residual\n{USAGE}")
        })?;

        Ok(Self {
            lookback_days,
            specification,
        })
    }
}

/// Parses a positive integer, refusing a typo rather than falling back to the default.
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
    let tracing_guard = fund::common::log::init_tracing(
        "laboratory-residuals.log",
        Some("info"),
        "laboratory-residuals",
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
        Ok(report) => {
            println!("{report}");
            0
        }
        Err(error) => {
            error!(%error, "Measuring the residual panel failed");
            eprintln!("Measuring the residual panel failed: {error}");
            1
        }
    };

    drop(tracing_guard);
    std::process::exit(code);
}

/// Reads the window, residualizes it, and scores the fit against a permuted-sector control.
async fn run(parameters: &Parameters) -> Result<String, Box<dyn std::error::Error>> {
    let bucket = fund::common::aws::archive_bucket()?;
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
        specification = %parameters.specification,
        %session,
        %run_id,
        "Measuring the residual panel"
    );

    let dataset = dataset::residuals(
        &s3_client,
        &bucket,
        parameters.lookback_days,
        session,
        parameters.specification,
    )
    .await?;

    if let Some(journal) = journal.as_ref() {
        journal
            .record(
                run_id,
                Utc::now(),
                laboratory::Observation::DatasetBuilt(laboratory::DatasetBuilt {
                    fingerprint: dataset.fingerprint.clone(),
                    revision: std::env::var("FUND_REVISION").ok(),
                }),
            )
            .await;
    }

    // The floor every real reading clears: a full set of dummies fits noise on its own.
    let permuted = permute_sectors(&dataset.panel.frame, PERMUTATION_SEED)?;
    let control = residual_returns(&permuted, parameters.specification)?;

    let shared = measured_in_both(&dataset.panel, &control);
    let measured = measure(&dataset.panel, &control, &shared, &dataset.fingerprint)?;

    if let Some(journal) = journal.as_ref() {
        for result in &measured.studies {
            journal
                .record(
                    run_id,
                    Utc::now(),
                    laboratory::Observation::StudyMeasured(laboratory::StudyMeasured::from(result)),
                )
                .await;
        }
    }

    Ok(render(
        parameters,
        &dataset.fingerprint,
        &dataset.panel,
        &shared,
        &measured,
    ))
}

/// The three readings and the two comparisons drawn between them.
///
/// The whole-window shares are carried alongside the studies because they answer a different
/// question — what the fit removed from this window — and the studies answer whether the sector
/// labels did any of it. Neither substitutes for the other, so both are reported and labelled.
struct Measured {
    baseline: Option<f64>,
    control: Option<f64>,
    treatment: Option<f64>,
    studies: Vec<StudyResult>,
}

/// Scores the panel against its permuted control and against the session mean.
///
/// Two comparisons, so the family holds two tests and each is read against a bar set by both. The
/// count is the number of comparisons this binary draws rather than a number typed beside them.
fn measure(
    panel: &ResidualPanel,
    control: &ResidualPanel,
    shared: &[usize],
    fingerprint: &DatasetFingerprint,
) -> Result<Measured, Box<dyn std::error::Error>> {
    let unreadable = || -> Box<dyn std::error::Error> {
        "the shared rows could not be grouped into sessions".into()
    };
    let sessions = sessions_of(panel, shared).ok_or_else(unreadable)?;
    let treatment = fitted_variance(panel, &sessions).ok_or_else(unreadable)?;
    let permuted = fitted_variance(control, &sessions).ok_or_else(unreadable)?;
    let baseline = session_mean_variance(panel, &sessions).ok_or_else(unreadable)?;

    let arm = |name: &str, variances: &[SessionVariance]| {
        Arm::new(name, shares(variances), shared.len()).ok_or_else(
            || -> Box<dyn std::error::Error> {
                format!("{name} produced no readings to score").into()
            },
        )
    };
    let study = |question: &str, treatment: Arm, control: Arm| {
        Study::new(
            Declaration::new(
                question,
                Family::new(
                    FAMILY,
                    NonZeroUsize::new(TESTS_IN_FAMILY).expect("a positive count"),
                ),
                // The panel fits one cross-section at a time and forecasts nothing, so the reading
                // is about the session it was fitted on rather than any session after it.
                Horizon::Sessions(NonZeroUsize::new(1).expect("a positive count")),
                declared_universe(fingerprint),
                Quantity::Unpriced {
                    units: "variance share",
                },
            ),
            Pairing::Matched,
            treatment,
            control,
            fingerprint,
        )
        .map(Study::measure)
        .map_err(|refusal| -> Box<dyn std::error::Error> { refusal.to_string().into() })
    };

    let studies = vec![
        study(
            "do real sector labels explain more than permuted ones",
            arm(TREATMENT_ARM, &treatment)?,
            arm(CONTROL_ARM, &permuted)?,
        )?,
        study(
            "do size and volatility explain more than the session mean",
            arm(CONTROL_ARM, &permuted)?,
            arm(BASELINE_ARM, &baseline)?,
        )?,
    ];

    Ok(Measured {
        baseline: pooled_share(&baseline),
        control: pooled_share(&permuted),
        treatment: pooled_share(&treatment),
        studies,
    })
}

/// The population the dataset was actually screened by, named and versioned.
///
/// Read off the fingerprint rather than asserted, because the harness refuses a study whose
/// declaration disagrees with it — and the window is stated as [`ScreenWindow::WholeFrame`]
/// because that is what `filter_training_bars` does, not what the traded universe does.
fn declared_universe(fingerprint: &DatasetFingerprint) -> DeclaredUniverse {
    match fingerprint.liquidity_floor {
        Some(floor) => DeclaredUniverse::Screened {
            name: "training-liquid-v1".to_string(),
            floor,
            window: ScreenWindow::WholeFrame,
        },
        None => DeclaredUniverse::Unscreened,
    }
}

/// Reassigns each ticker a sector drawn from the panel's own labels, without replacement.
///
/// Permuting across *tickers* rather than rows keeps a name in one sector for the whole window, as
/// it is in the archive, and preserves the sector size distribution exactly.
///
/// Unclassified names keep [`UNKNOWN`] and are left out of the pool entirely. `residual_returns`
/// refuses that sentinel, so shuffling it would move refusals onto different tickers and the arms
/// would differ in the rows they measure as well as in the labels under test.
fn permute_sectors(frame: &DataFrame, seed: u64) -> Result<DataFrame, PolarsError> {
    let tickers = frame.column("ticker")?.str()?;
    let sectors = frame.column("sector")?.str()?;

    let mut classified: Vec<String> = Vec::new();
    let mut labels: Vec<String> = Vec::new();
    let mut index: std::collections::HashMap<String, usize> = std::collections::HashMap::new();
    let mut unclassified: std::collections::HashSet<String> = std::collections::HashSet::new();
    for row in 0..frame.height() {
        let (Some(ticker), Some(sector)) = (tickers.get(row), sectors.get(row)) else {
            continue;
        };
        if sector == UNKNOWN {
            unclassified.insert(ticker.to_string());
            continue;
        }
        if !index.contains_key(ticker) {
            index.insert(ticker.to_string(), classified.len());
            classified.push(ticker.to_string());
            labels.push(sector.to_string());
        }
    }

    let mut generator = StdRng::seed_from_u64(seed);
    labels.shuffle(&mut generator);

    let permuted: Vec<Option<&str>> = (0..frame.height())
        .map(|row| {
            let ticker = tickers.get(row)?;
            if unclassified.contains(ticker) {
                return Some(UNKNOWN);
            }
            index.get(ticker).map(|position| labels[*position].as_str())
        })
        .collect();

    let mut shuffled = frame.clone();
    shuffled.with_column(Column::new("sector".into(), permuted))?;
    Ok(shuffled)
}

/// Rows carrying a residual in *both* panels, which is the population every share divides by.
///
/// Holding `UNKNOWN` out of the permutation is not enough on its own: leverage turns on sector size
/// and identification on the per-session design, so a permutation can still move a refusal even
/// where every sector is known. Scoring on the intersection is what makes the floor comparable.
fn measured_in_both(panel: &ResidualPanel, control: &ResidualPanel) -> Vec<usize> {
    let residuals = |panel: &ResidualPanel| {
        panel
            .frame
            .column(RESIDUAL_COLUMN)
            .ok()
            .and_then(|column| column.f64().ok())
            .map(|column| column.into_iter().collect::<Vec<_>>())
    };
    let (Some(left), Some(right)) = (residuals(panel), residuals(control)) else {
        return Vec::new();
    };

    (0..panel.frame.height())
        .filter(|row| left[*row].is_some() && right[*row].is_some())
        .collect()
}

/// The shared rows grouped into sessions, in session order.
///
/// Built once and handed to every arm, so the three readings are aligned session for session and
/// can be differenced as a matched pair rather than compared as two summaries. Sessions are the
/// level that varies here: a cross-section is fitted jointly, so an error over rows would divide by
/// far more independence than one session's 1,200 names contain.
fn sessions_of(panel: &ResidualPanel, rows: &[usize]) -> Option<Vec<Vec<usize>>> {
    let timestamps = panel.frame.column("timestamp").ok()?.i64().ok()?;

    let mut grouped: std::collections::BTreeMap<i64, Vec<usize>> = Default::default();
    for row in rows {
        grouped.entry(timestamps.get(*row)?).or_default().push(*row);
    }
    Some(grouped.into_values().collect())
}

/// One session's raw variation and what a fit left of it.
///
/// The two sums travel together rather than as a ratio, so the per-session share and the
/// whole-window share are both taken from one place and cannot drift into two definitions.
#[derive(Debug, Clone, Copy)]
struct SessionVariance {
    raw: f64,
    left: f64,
}

/// What the panel's own fit left behind, per session.
fn fitted_variance(panel: &ResidualPanel, sessions: &[Vec<usize>]) -> Option<Vec<SessionVariance>> {
    let returns = returns_of(panel)?;
    let residuals = panel.frame.column(RESIDUAL_COLUMN).ok()?.f64().ok()?;

    sessions
        .iter()
        .map(|rows| {
            let mut variance = SessionVariance {
                raw: 0.0,
                left: 0.0,
            };
            for row in rows {
                let (Some(actual), Some(residual)) = (returns.get(*row), residuals.get(*row))
                else {
                    return None;
                };
                variance.raw += actual * actual;
                variance.left += residual * residual;
            }
            Some(variance)
        })
        .collect()
}

/// What subtracting the session's own mean return alone leaves: the trivial baseline.
///
/// Quoted before the other two because the sector dummies span the intercept, so every figure here
/// already contains this one. Measured over the same sessions, which is what lets the three be
/// subtracted from each other at all.
fn session_mean_variance(
    panel: &ResidualPanel,
    sessions: &[Vec<usize>],
) -> Option<Vec<SessionVariance>> {
    let returns = returns_of(panel)?;

    sessions
        .iter()
        .map(|rows| {
            let mut total = 0.0;
            for row in rows {
                total += returns.get(*row)?;
            }
            let mean = total / rows.len() as f64;

            let mut variance = SessionVariance {
                raw: 0.0,
                left: 0.0,
            };
            for row in rows {
                let actual = returns.get(*row)?;
                variance.raw += actual * actual;
                variance.left += (actual - mean) * (actual - mean);
            }
            Some(variance)
        })
        .collect()
}

/// Share of the raw variation removed, one reading per session.
///
/// `None` on a session whose returns carry no variation, because a ratio against nothing is not
/// zero explanatory power — it is no reading at all.
fn shares(variances: &[SessionVariance]) -> Vec<Option<f64>> {
    variances
        .iter()
        .map(|variance| (variance.raw > 0.0).then(|| 1.0 - variance.left / variance.raw))
        .collect()
}

/// Share of the raw variation removed over the whole window, in one ratio.
///
/// Not the mean of [`shares`] and not meant to be: this weights a session by how much the market
/// moved in it, where the per-session readings weight every session alike. The first describes the
/// window and the second is what carries a standard error.
fn pooled_share(variances: &[SessionVariance]) -> Option<f64> {
    let raw: f64 = variances.iter().map(|variance| variance.raw).sum();
    let left: f64 = variances.iter().map(|variance| variance.left).sum();
    (raw > 0.0).then(|| 1.0 - left / raw)
}

/// The panel's returns as `f64`, whatever width the frame stores them at.
fn returns_of(panel: &ResidualPanel) -> Option<Float64Chunked> {
    let returns = panel.frame.column("daily_return").ok()?;
    returns.cast(&DataType::Float64).ok()?.f64().ok().cloned()
}

fn render(
    parameters: &Parameters,
    fingerprint: &DatasetFingerprint,
    panel: &ResidualPanel,
    shared: &[usize],
    measured: &Measured,
) -> String {
    let mut report = String::new();
    let percent = |share: Option<f64>| match share {
        Some(value) => format!("{:.2}%", value * 100.0),
        None => "unmeasurable".to_string(),
    };

    report.push_str(&format!(
        "Residual panel over {} days to {}\n  {}\n\n",
        parameters.lookback_days, fingerprint.session, parameters.specification
    ));
    // Labelled by frame: the fingerprint counts the archive window before `clean_data` runs, where
    // everything after it counts the cleaned panel the fit actually saw.
    report.push_str(&format!(
        "  archive rows {}  tickers {}  panel rows {}  measured {}  undefined {}\n",
        fingerprint.rows,
        fingerprint.tickers,
        panel.frame.height(),
        panel.measured,
        percent(panel.undefined_share())
    ));

    report.push_str("\n  refused, by cause\n");
    for (cause, count) in &panel.refused {
        report.push_str(&format!("    {count:>8}  {cause}\n"));
    }

    // Baseline first, then control, then treatment, so no reading is seen without its floor.
    report.push_str(&format!(
        "\n  variance explained over the whole window, across the {} rows both arms measured\n",
        shared.len()
    ));
    report.push_str(&format!(
        "    {:>12}  session mean alone                  (baseline)\n",
        percent(measured.baseline)
    ));
    report.push_str(&format!(
        "    {:>12}  + size, volatility, permuted sectors (control)\n",
        percent(measured.control)
    ));
    report.push_str(&format!(
        "    {:>12}  + size, volatility, real sectors     (treatment)\n",
        percent(measured.treatment)
    ));
    // The block above weights a session by how much the market moved in it and carries no error;
    // the table below weights every session alike and is the one a claim can be read off.
    report.push_str(
        "\n  Differenced per session, which is where the error comes from. The shares below are \
         means\n  across sessions and so do not equal the window figures above.\n\n",
    );
    report.push_str(&fund::laboratory::harness::render(&measured.studies));
    report
}

#[cfg(test)]
mod tests {
    use super::*;

    fn frame(rows: &[(&str, i64, &str)]) -> DataFrame {
        DataFrame::new(vec![
            Column::new(
                "ticker".into(),
                rows.iter().map(|row| row.0).collect::<Vec<_>>(),
            ),
            Column::new(
                "timestamp".into(),
                rows.iter().map(|row| row.1).collect::<Vec<_>>(),
            ),
            Column::new(
                "sector".into(),
                rows.iter().map(|row| row.2).collect::<Vec<_>>(),
            ),
        ])
        .expect("the fixture must build")
    }

    fn sectors_by_ticker(frame: &DataFrame) -> std::collections::BTreeMap<String, Vec<String>> {
        let tickers = frame.column("ticker").unwrap().str().unwrap();
        let sectors = frame.column("sector").unwrap().str().unwrap();
        let mut seen: std::collections::BTreeMap<String, Vec<String>> = Default::default();
        for row in 0..frame.height() {
            seen.entry(tickers.get(row).unwrap().to_string())
                .or_default()
                .push(sectors.get(row).unwrap().to_string());
        }
        seen
    }

    fn fixture() -> Vec<(&'static str, i64, &'static str)> {
        let mut rows = Vec::new();
        for (ticker, sector) in [
            ("AAA", "35"),
            ("BBB", "73"),
            ("CCC", "60"),
            ("DDD", "35"),
            ("EEE", UNKNOWN),
            ("FFF", UNKNOWN),
        ] {
            for session in 0..3i64 {
                rows.push((ticker, session, sector));
            }
        }
        rows
    }

    #[test]
    fn test_a_permuted_name_keeps_one_sector_across_all_its_rows() {
        let permuted =
            permute_sectors(&frame(&fixture()), 0x5EED).expect("the permutation must run");

        for (ticker, sectors) in sectors_by_ticker(&permuted) {
            assert!(
                sectors.windows(2).all(|pair| pair[0] == pair[1]),
                "{ticker} changed sector mid-window: {sectors:?}"
            );
        }
    }

    /// The sector size distribution is what the leverage refusal turns on, so it has to survive.
    #[test]
    fn test_the_multiset_of_sector_labels_is_preserved() {
        let original = frame(&fixture());
        let permuted = permute_sectors(&original, 0x5EED).expect("the permutation must run");

        let census = |frame: &DataFrame| {
            let mut counts: std::collections::BTreeMap<String, usize> = Default::default();
            for (_ticker, sectors) in sectors_by_ticker(frame) {
                *counts.entry(sectors[0].clone()).or_insert(0) += 1;
            }
            counts
        };

        assert_eq!(census(&original), census(&permuted));
    }

    /// The finding this was rewritten for: shuffling the sentinel moves refusals onto other names,
    /// so the arms would differ in which rows they measure as well as in the labels under test.
    #[test]
    fn test_an_unclassified_name_keeps_the_sentinel_rather_than_drawing_a_sector() {
        let permuted =
            permute_sectors(&frame(&fixture()), 0x5EED).expect("the permutation must run");
        let sectors = sectors_by_ticker(&permuted);

        assert_eq!(sectors["EEE"][0], UNKNOWN);
        assert_eq!(sectors["FFF"][0], UNKNOWN);
        for classified in ["AAA", "BBB", "CCC", "DDD"] {
            assert_ne!(
                sectors[classified][0], UNKNOWN,
                "{classified} must not be handed the sentinel"
            );
        }
    }

    /// Two sessions, and every row shared. The second is deliberately the quieter of the two.
    fn panel(residuals: &[f64]) -> ResidualPanel {
        let frame = DataFrame::new(vec![
            Column::new("timestamp".into(), vec![0_i64, 0, 1, 1]),
            Column::new("daily_return".into(), vec![0.02_f64, -0.02, 0.001, -0.001]),
            Column::new(RESIDUAL_COLUMN.into(), residuals.to_vec()),
        ])
        .expect("the fixture must build");
        ResidualPanel {
            frame,
            measured: 4,
            refused: Default::default(),
        }
    }

    #[test]
    fn test_the_shared_rows_group_into_sessions_in_order() {
        let panel = panel(&[0.0; 4]);
        let sessions = sessions_of(&panel, &[0, 1, 2, 3]).expect("the fixture groups");
        assert_eq!(sessions, vec![vec![0, 1], vec![2, 3]]);

        // Only the rows handed in, so an arm scored on an intersection stays on it.
        let sessions = sessions_of(&panel, &[1, 2]).expect("the fixture groups");
        assert_eq!(sessions, vec![vec![1], vec![2]]);
    }

    /// The whole-window figure is not the mean of the per-session ones, and the two must not be
    /// quoted as if they were: pooling weights a session by how much the market moved in it.
    ///
    /// Session one's names move twenty times as far as session two's, so four hundred times the
    /// variance, and the fit removes everything in the loud one and nothing in the quiet one.
    /// Pooled, that is 99.8% of the window; averaged across sessions, it is half.
    #[test]
    fn test_the_pooled_share_is_not_the_mean_of_the_per_session_shares() {
        let panel = panel(&[0.0, 0.0, 0.001, -0.001]);
        let sessions = sessions_of(&panel, &[0, 1, 2, 3]).expect("the fixture groups");
        let variances = fitted_variance(&panel, &sessions).expect("the fixture measures");

        let per_session = shares(&variances);
        assert_eq!(per_session, vec![Some(1.0), Some(0.0)]);

        let pooled = pooled_share(&variances).expect("the window carries variation");
        assert!(
            (pooled - 0.997_506_234_413_965).abs() < 1e-12,
            "got {pooled}"
        );

        let mean = 0.5;
        assert!((pooled - mean).abs() > 0.4, "the two must not coincide");
    }

    /// A session whose returns did not vary has no share, rather than a share of zero.
    #[test]
    fn test_a_session_that_did_not_move_carries_no_reading() {
        let frame = DataFrame::new(vec![
            Column::new("timestamp".into(), vec![0_i64, 0]),
            Column::new("daily_return".into(), vec![0.0_f64, 0.0]),
            Column::new(RESIDUAL_COLUMN.into(), vec![0.0_f64, 0.0]),
        ])
        .expect("the fixture must build");
        let panel = ResidualPanel {
            frame,
            measured: 2,
            refused: Default::default(),
        };

        let sessions = sessions_of(&panel, &[0, 1]).expect("the fixture groups");
        let variances = fitted_variance(&panel, &sessions).expect("the fixture measures");
        assert_eq!(shares(&variances), vec![None]);
        assert_eq!(pooled_share(&variances), None);
    }

    /// Demeaning within a session is what the baseline arm reads, and a cross-section that all
    /// moved together is entirely explained by its own mean.
    #[test]
    fn test_the_session_mean_explains_a_cross_section_that_moved_together() {
        let frame = DataFrame::new(vec![
            Column::new("timestamp".into(), vec![0_i64, 0, 1, 1]),
            Column::new("daily_return".into(), vec![0.02_f64, 0.02, 0.01, -0.01]),
            Column::new(RESIDUAL_COLUMN.into(), vec![0.0_f64; 4]),
        ])
        .expect("the fixture must build");
        let panel = ResidualPanel {
            frame,
            measured: 4,
            refused: Default::default(),
        };

        let sessions = sessions_of(&panel, &[0, 1, 2, 3]).expect("the fixture groups");
        let variances = session_mean_variance(&panel, &sessions).expect("the fixture measures");

        // Both names moved +2%, so the session mean leaves nothing.
        assert_eq!(shares(&variances)[0], Some(1.0));
        // They moved opposite ways, so the mean is zero and removes nothing.
        assert_eq!(shares(&variances)[1], Some(0.0));
    }

    #[test]
    fn test_the_seed_makes_the_permutation_reproducible() {
        let rows = frame(&fixture());

        let once = permute_sectors(&rows, 0x5EED).expect("the permutation must run");
        let twice = permute_sectors(&rows, 0x5EED).expect("the permutation must run");
        let elsewhere = permute_sectors(&rows, 0x1234).expect("the permutation must run");

        assert_eq!(sectors_by_ticker(&once), sectors_by_ticker(&twice));
        assert_ne!(sectors_by_ticker(&once), sectors_by_ticker(&elsewhere));
    }
}
