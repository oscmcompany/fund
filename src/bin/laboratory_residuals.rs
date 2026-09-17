//! Reports what the factor panel measures, what it refuses, and what it explains over nothing.
//!
//! Fits nothing beyond the per-session cross-section, so a run is one archive read and arithmetic.

use chrono::Utc;
use rand::prelude::*;
use rand::rngs::StdRng;
use tracing::{error, info, warn};

use fund::common::types::SessionDate;
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

    // The control: the same machinery over sector labels permuted across names. With sixty-five
    // groups over a cross-section this size the dummies explain a few percent by construction, and
    // whatever this reports is the floor every real reading has to clear.
    let permuted = permute_sectors(&dataset.panel.frame, PERMUTATION_SEED)?;
    let control = residual_returns(&permuted, parameters.specification)?;

    Ok(render(
        parameters,
        &dataset.fingerprint,
        &dataset.panel,
        &control,
    ))
}

/// Reassigns each ticker a sector drawn from the panel's own sector labels, without replacement.
///
/// Permuting across *tickers* rather than rows keeps a name in one sector for the whole window, as
/// it is in the archive, and keeps the sector size distribution exactly. What it destroys is the
/// only thing under test: whether a name's sector says anything about its return.
fn permute_sectors(frame: &DataFrame, seed: u64) -> Result<DataFrame, PolarsError> {
    let tickers = frame.column("ticker")?.str()?;
    let sectors = frame.column("sector")?.str()?;

    let mut first_seen: Vec<String> = Vec::new();
    let mut labels: Vec<String> = Vec::new();
    let mut index: std::collections::HashMap<String, usize> = std::collections::HashMap::new();
    for row in 0..frame.height() {
        let (Some(ticker), Some(sector)) = (tickers.get(row), sectors.get(row)) else {
            continue;
        };
        if !index.contains_key(ticker) {
            index.insert(ticker.to_string(), first_seen.len());
            first_seen.push(ticker.to_string());
            labels.push(sector.to_string());
        }
    }

    let mut generator = StdRng::seed_from_u64(seed);
    labels.shuffle(&mut generator);

    let permuted: Vec<Option<&str>> = (0..frame.height())
        .map(|row| {
            tickers
                .get(row)
                .and_then(|ticker| index.get(ticker))
                .map(|position| labels[*position].as_str())
        })
        .collect();

    let mut shuffled = frame.clone();
    shuffled.with_column(Column::new("sector".into(), permuted))?;
    Ok(shuffled)
}

/// Share of the raw return's variance the fit removed, over the rows it measured.
///
/// `None` where nothing was measured or the returns did not vary, because a ratio against no
/// variation is not zero explanatory power, it is no reading at all.
fn explained_share(panel: &ResidualPanel) -> Option<f64> {
    let returns = panel.frame.column("daily_return").ok()?;
    let returns = returns.cast(&DataType::Float64).ok()?;
    let returns = returns.f64().ok()?;
    let residuals = panel.frame.column(RESIDUAL_COLUMN).ok()?.f64().ok()?;

    let mut raw = 0.0;
    let mut left = 0.0;
    let mut measured = 0usize;
    for row in 0..panel.frame.height() {
        let (Some(actual), Some(residual)) = (returns.get(row), residuals.get(row)) else {
            continue;
        };
        raw += actual * actual;
        left += residual * residual;
        measured += 1;
    }

    (measured > 0 && raw > 0.0).then(|| 1.0 - left / raw)
}

/// What subtracting the session's own mean return alone removes: the trivial baseline.
///
/// Quoted before the other two because the sector dummies span the intercept, so every figure here
/// already contains this one. Without it, removing the market reads as a finding about sectors.
/// Measured over the rows the panel measured, so all three shares share a denominator.
fn market_only_share(panel: &ResidualPanel) -> Option<f64> {
    let returns = panel.frame.column("daily_return").ok()?;
    let returns = returns.cast(&DataType::Float64).ok()?;
    let returns = returns.f64().ok()?;
    let residuals = panel.frame.column(RESIDUAL_COLUMN).ok()?.f64().ok()?;
    let timestamps = panel.frame.column("timestamp").ok()?.i64().ok()?;

    let mut totals: std::collections::HashMap<i64, (f64, usize)> = std::collections::HashMap::new();
    for row in 0..panel.frame.height() {
        let (Some(actual), Some(_residual), Some(session)) =
            (returns.get(row), residuals.get(row), timestamps.get(row))
        else {
            continue;
        };
        let entry = totals.entry(session).or_insert((0.0, 0));
        entry.0 += actual;
        entry.1 += 1;
    }

    let mut raw = 0.0;
    let mut left = 0.0;
    for row in 0..panel.frame.height() {
        let (Some(actual), Some(_residual), Some(session)) =
            (returns.get(row), residuals.get(row), timestamps.get(row))
        else {
            continue;
        };
        let (total, names) = totals[&session];
        let demeaned = actual - total / names as f64;
        raw += actual * actual;
        left += demeaned * demeaned;
    }

    (raw > 0.0).then(|| 1.0 - left / raw)
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
    report.push_str(&format!(
        "  rows {}  tickers {}  measured {}  undefined {}\n",
        fingerprint.rows,
        fingerprint.tickers,
        panel.measured,
        percent(panel.undefined_share())
    ));

    report.push_str("\n  refused, by cause\n");
    for (cause, count) in &panel.refused {
        report.push_str(&format!("    {count:>8}  {cause}\n"));
    }

    // Baseline first, then control, then treatment, so no reading is ever seen without its floor.
    // The sector dummies span the intercept, so every figure below already contains the first one.
    report.push_str("\n  variance explained, over the measured rows\n");
    report.push_str(&format!(
        "    {:>12}  session mean alone        (baseline)\n",
        percent(market_only_share(panel))
    ));
    report.push_str(&format!(
        "    {:>12}  + size, volatility, permuted sectors (control)\n",
        percent(explained_share(control))
    ));
    report.push_str(&format!(
        "    {:>12}  + size, volatility, real sectors     (treatment)\n",
        percent(explained_share(panel))
    ));
    report
}
