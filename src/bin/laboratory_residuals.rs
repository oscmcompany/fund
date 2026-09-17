//! Reports what the factor panel measures, what it refuses, and what it explains over nothing.
//!
//! Fits nothing beyond the per-session cross-section, so a run is one archive read and arithmetic.

use chrono::Utc;
use rand::prelude::*;
use rand::rngs::StdRng;
use tracing::{error, info, warn};

use fund::common::types::SessionDate;
use fund::data::details::UNKNOWN;
use fund::laboratory::dataset;
use fund::laboratory::journal as laboratory;
use fund::laboratory::residual::{
    residual_returns, FactorSpecification, ResidualPanel, RESIDUAL_COLUMN,
};

use polars::prelude::*;

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

    // The floor every real reading clears: sixty-five dummies fit a great deal of noise on their own.
    let permuted = permute_sectors(&dataset.panel.frame, PERMUTATION_SEED)?;
    let control = residual_returns(&permuted, parameters.specification)?;

    Ok(render(
        parameters,
        &dataset.fingerprint,
        &dataset.panel,
        &control,
    ))
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

/// Share of the raw return's variance a panel's fit removed, over `rows`.
///
/// `None` where the rows carry no variation, because a ratio against nothing is not zero
/// explanatory power — it is no reading at all.
fn explained_share(panel: &ResidualPanel, rows: &[usize]) -> Option<f64> {
    let returns = returns_of(panel)?;
    let residuals = panel.frame.column(RESIDUAL_COLUMN).ok()?.f64().ok()?;

    let mut raw = 0.0;
    let mut left = 0.0;
    for row in rows {
        let (Some(actual), Some(residual)) = (returns.get(*row), residuals.get(*row)) else {
            return None;
        };
        raw += actual * actual;
        left += residual * residual;
    }

    (raw > 0.0).then(|| 1.0 - left / raw)
}

/// What subtracting the session's own mean return alone removes: the trivial baseline.
///
/// Quoted before the other two because the sector dummies span the intercept, so every figure here
/// already contains this one. Measured over the same `rows`, which is what lets the three be
/// subtracted from each other at all.
fn market_only_share(panel: &ResidualPanel, rows: &[usize]) -> Option<f64> {
    let returns = returns_of(panel)?;
    let timestamps = panel.frame.column("timestamp").ok()?.i64().ok()?;

    let mut totals: std::collections::HashMap<i64, (f64, usize)> = std::collections::HashMap::new();
    for row in rows {
        let (Some(actual), Some(session)) = (returns.get(*row), timestamps.get(*row)) else {
            return None;
        };
        let entry = totals.entry(session).or_insert((0.0, 0));
        entry.0 += actual;
        entry.1 += 1;
    }

    let mut raw = 0.0;
    let mut left = 0.0;
    for row in rows {
        let (Some(actual), Some(session)) = (returns.get(*row), timestamps.get(*row)) else {
            return None;
        };
        let (total, names) = totals[&session];
        let demeaned = actual - total / names as f64;
        raw += actual * actual;
        left += demeaned * demeaned;
    }

    (raw > 0.0).then(|| 1.0 - left / raw)
}

/// The panel's returns as `f64`, whatever width the frame stores them at.
fn returns_of(panel: &ResidualPanel) -> Option<Float64Chunked> {
    let returns = panel.frame.column("daily_return").ok()?;
    returns.cast(&DataType::Float64).ok()?.f64().ok().cloned()
}

fn render(
    parameters: &Parameters,
    fingerprint: &fund::laboratory::dataset::DatasetFingerprint,
    panel: &ResidualPanel,
    control: &ResidualPanel,
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
    let shared = measured_in_both(panel, control);
    report.push_str(&format!(
        "\n  variance explained, over the {} rows both arms measured\n",
        shared.len()
    ));
    report.push_str(&format!(
        "    {:>12}  session mean alone        (baseline)\n",
        percent(market_only_share(panel, &shared))
    ));
    report.push_str(&format!(
        "    {:>12}  + size, volatility, permuted sectors (control)\n",
        percent(explained_share(control, &shared))
    ));
    report.push_str(&format!(
        "    {:>12}  + size, volatility, real sectors     (treatment)\n",
        percent(explained_share(panel, &shared))
    ));
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
