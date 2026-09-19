//! Counts the names each declared screen admits over one window, and what they disagree about.
//!
//! Screens nothing new: it reads one archive window and applies the screens the tree already
//! declares, so the populations behind every laboratory figure can be read rather than argued.

use std::collections::{BTreeMap, BTreeSet};

use chrono::Utc;
use polars::prelude::*;
use tracing::{error, info};

use fund::common::log::init_tracing;
use fund::common::types::{Screen, ScreenWindow, SessionDate};
use fund::data::universe::filter_liquid_bars;
use fund::laboratory::dataset;

const USAGE: &str = "Usage: laboratory_universe [LOOKBACK_DAYS] [TRAILING_DAYS]";

/// Calendar days of archive to read, matching the other laboratory runners.
const DEFAULT_LOOKBACK_DAYS: i64 = 730;

/// The trailing window to compare the research screen against.
///
/// Defaults to the live book's own, because the question this binary exists to answer is what the
/// research population and the traded population differ by.
const DEFAULT_TRAILING_DAYS: u32 = fund::data::universe::LIQUIDITY_LOOKBACK_DAYS;

struct Parameters {
    lookback_days: i64,
    trailing_days: u32,
}

impl Parameters {
    fn parse(arguments: &[String]) -> Result<Self, String> {
        let (lookback_days, trailing_days) = match arguments {
            [] => (DEFAULT_LOOKBACK_DAYS, DEFAULT_TRAILING_DAYS),
            [lookback] => (positive(lookback, "LOOKBACK_DAYS")?, DEFAULT_TRAILING_DAYS),
            [lookback, trailing] => (
                positive(lookback, "LOOKBACK_DAYS")?,
                u32::try_from(positive(trailing, "TRAILING_DAYS")?)
                    .map_err(|_| format!("TRAILING_DAYS is too large a window\n{USAGE}"))?,
            ),
            _ => return Err(format!("Too many arguments\n{USAGE}")),
        };
        Ok(Self {
            lookback_days,
            trailing_days,
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

/// One screen, the anchor it was applied at, and what it admitted.
struct Admitted {
    name: String,
    screen: Screen,
    anchor: SessionDate,
    tickers: BTreeSet<String>,
    /// Rows surviving the screen, which is the panel a study actually measures over.
    ///
    /// Carried beside the name count because the two come apart exactly where this pull request
    /// does: a per-session screen can admit every name and still refuse half the panel.
    rows: usize,
    /// What the admitted set pays to cross, read from the quote archive rather than assumed.
    cost: Cost,
}

/// The quoted spread over one screen's admitted rows.
///
/// Screened first and averaged second, which is the whole point: a subset's cost read off the whole
/// market's distribution is the wrong number, and the whole-market median is known contaminated by
/// thousands of names nothing would ever trade.
struct Cost {
    /// Rows carrying a spread at all. Printed because a median over 12% of the panel is a different
    /// statistic from one over 98%, and the two render identically.
    measured: usize,
    median: Option<f64>,
    /// Equal-weighted, which is what every basket in the tree actually pays.
    ///
    /// Beside the dollar-weighted figure because the two answer different books and the gap between
    /// them is large: dollar volume concentrates in the tight names, so weighting by it prices a
    /// book nobody here trades.
    mean: Option<f64>,
    /// Weighted by each row's dollar volume, which is what a capitalisation-weighted basket pays.
    dollar_weighted_mean: Option<f64>,
    tenth_percentile: Option<f64>,
    ninetieth_percentile: Option<f64>,
}

/// Reads the spread distribution over whatever rows survived a screen.
fn cost_of(screened: &DataFrame) -> PolarsResult<Cost> {
    let spreads = screened
        .column("quoted_spread_basis_points_mean")?
        .f64()?
        .clone();
    let closes = screened.column("close_price")?.cast(&DataType::Float64)?;
    let volumes = screened.column("volume")?.cast(&DataType::Float64)?;
    let closes = closes.f64()?;
    let volumes = volumes.f64()?;

    let mut observed: Vec<f64> = Vec::new();
    let mut weighted_total = 0.0;
    let mut weight_total = 0.0;
    for row in 0..screened.height() {
        let Some(spread) = spreads.get(row).filter(|value| value.is_finite()) else {
            continue;
        };
        observed.push(spread);
        // Dollar volume from the bar rather than the trade summary, so the weight exists on every
        // row the panel holds and the two archives cannot disagree about it.
        if let (Some(close), Some(volume)) = (closes.get(row), volumes.get(row)) {
            let notional = close * volume;
            if notional.is_finite() && notional > 0.0 {
                weighted_total += spread * notional;
                weight_total += notional;
            }
        }
    }
    observed.sort_by(f64::total_cmp);

    let mean = (!observed.is_empty()).then(|| observed.iter().sum::<f64>() / observed.len() as f64);
    Ok(Cost {
        measured: observed.len(),
        median: percentile(&observed, 0.5),
        mean,
        dollar_weighted_mean: (weight_total > 0.0).then(|| weighted_total / weight_total),
        tenth_percentile: percentile(&observed, 0.1),
        ninetieth_percentile: percentile(&observed, 0.9),
    })
}

/// A reading or the reason there is none, never a zero standing in for one.
fn basis_points(value: Option<f64>) -> String {
    value.map_or_else(|| "none".to_string(), |value| format!("{value:.2}"))
}

/// The `fraction` quantile of a sorted slice, by nearest rank.
fn percentile(sorted: &[f64], fraction: f64) -> Option<f64> {
    if sorted.is_empty() {
        return None;
    }
    let rank = ((sorted.len() - 1) as f64 * fraction).round() as usize;
    sorted.get(rank).copied()
}

/// What a per-session screen costs beyond the names it refuses.
///
/// A name that leaves and returns loses its first session back as well, because `engineer_features`
/// gives that row no `daily_return` -- the frame-wide session rank says it does not follow the row
/// before it. So the panel pays for the churn twice, and the second charge is invisible here unless
/// it is counted.
struct Churn {
    names_ever_admitted: usize,
    names_without_a_break: usize,
    spells: usize,
    re_entries: usize,
}

/// Calendar days past the window's last session to re-anchor the trailing screen at.
///
/// Deliberately calendar days and not trading sessions. At pre-open the traded universe is built
/// for today while the newest daily bar is the last session's, and how far apart those are is a
/// property of the weekend rather than of the screen: one day midweek, three across a weekend, four
/// after a Monday holiday. Naming one of them "the next session" would claim a tradability
/// `SessionDate` does not carry, so the sweep reports the range instead of picking from it.
const ANCHOR_SHIFT_DAYS: [i64; 3] = [1, 2, 3];

#[tokio::main]
async fn main() {
    fund::common::crypto::install_default_crypto_provider();
    let tracing_guard = init_tracing(
        "laboratory-universe.log",
        Some("info"),
        "laboratory-universe",
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
            error!(%error, "Measuring the screened populations failed");
            eprintln!("Measuring the screened populations failed: {error}");
            1
        }
    };

    drop(tracing_guard);
    std::process::exit(code);
}

async fn run(parameters: &Parameters) -> Result<String, Box<dyn std::error::Error>> {
    let bucket = fund::common::aws::archive_bucket()?;
    let s3_client = fund::common::aws::s3_client().await;
    let session = SessionDate::at(Utc::now());

    info!(
        bucket,
        lookback_days = parameters.lookback_days,
        trailing_days = parameters.trailing_days,
        %session,
        "Measuring what each declared screen admits"
    );

    let window = dataset::unscreened_window(
        &s3_client,
        &bucket,
        parameters.lookback_days,
        session,
        dataset::Microstructure::Joined,
    )
    .await?;
    let population = distinct_tickers(&window.bars)?;
    let last_session = window
        .last_session
        .ok_or("the window holds no bars to screen")?;
    info!(
        rows = window.bars.height(),
        tickers = population.len(),
        %last_session,
        "Read the archive window unscreened"
    );

    let trailing = ScreenWindow::Trailing(
        std::num::NonZeroU32::new(parameters.trailing_days).ok_or("a positive trailing window")?,
    );
    let floor = dataset::RESEARCH_SCREEN.floor();
    let mut declarations = vec![
        (
            "research".to_string(),
            dataset::RESEARCH_SCREEN,
            last_session,
        ),
        (
            "trailing".to_string(),
            Screen::new(floor, trailing),
            last_session,
        ),
    ];
    // The same trailing screen re-anchored ahead of the window, which is the pre-open shape: the
    // universe is built for today and the newest daily bar is the last session's.
    declarations.push((
        "per-session".to_string(),
        Screen::new(
            floor,
            ScreenWindow::PerSession(
                std::num::NonZeroU32::new(parameters.trailing_days)
                    .ok_or("a positive trailing window")?,
            ),
        ),
        last_session,
    ));
    declarations.extend(ANCHOR_SHIFT_DAYS.map(|shift| {
        (
            format!("trailing+{shift}d"),
            Screen::new(floor, trailing),
            last_session.plus_calendar_days(shift),
        )
    }));

    let mut churn: Option<Churn> = None;
    let mut measured = Vec::with_capacity(declarations.len());
    for (name, screen, anchor) in declarations {
        let screened = filter_liquid_bars(window.bars.clone(), screen, anchor)?;
        let tickers = distinct_tickers(&screened)?;
        info!(screen = %name, %screen, %anchor, admitted = tickers.len(), rows = screened.height(), "Screened the window");
        if screen.window().is_per_session() {
            churn = Some(churn_of(&window.bars, &screened)?);
        }
        let cost = cost_of(&screened)?;
        measured.push(Admitted {
            name,
            screen,
            anchor,
            tickers,
            rows: screened.height(),
            cost,
        });
    }

    Ok(render(
        &population,
        window.bars.height(),
        &measured,
        churn.as_ref(),
    ))
}

/// Counts each name's admitted spells, and how many of them were re-entries.
///
/// A break is "had a bar, was refused", so the calendar each name is walked against is **its own
/// bars in the unscreened window** rather than any shared one. Ranking against the screened frame
/// loses a session no ticker survived and merges two spells into one; ranking against the whole
/// population charges a name for a session it simply had no bar on — a halt, a late listing, an
/// absent print — which is churn the screen did not cause.
fn churn_of(unscreened: &DataFrame, screened: &DataFrame) -> PolarsResult<Churn> {
    let present = sessions_by_ticker(unscreened)?;
    let admitted = sessions_by_ticker(screened)?;

    let mut spells_by_ticker: BTreeMap<&String, usize> = BTreeMap::new();
    for (ticker, admitted_sessions) in &admitted {
        let Some(own_sessions) = present.get(ticker) else {
            continue;
        };
        let mut previous_was_admitted = false;
        for session in own_sessions {
            let is_admitted = admitted_sessions.contains(session);
            if is_admitted && !previous_was_admitted {
                *spells_by_ticker.entry(ticker).or_default() += 1;
            }
            previous_was_admitted = is_admitted;
        }
    }

    let spells: usize = spells_by_ticker.values().sum();
    Ok(Churn {
        names_ever_admitted: spells_by_ticker.len(),
        names_without_a_break: spells_by_ticker
            .values()
            .filter(|count| **count == 1)
            .count(),
        spells,
        // Every spell after a name's first is a re-entry, and each costs one row its daily return.
        re_entries: spells - spells_by_ticker.len(),
    })
}

/// Each ticker's sessions in time order, as the given frame holds them.
fn sessions_by_ticker(frame: &DataFrame) -> PolarsResult<BTreeMap<String, BTreeSet<i64>>> {
    let tickers = frame.column("ticker")?.str()?;
    let timestamps = frame.column("timestamp")?.i64()?;
    let mut by_ticker: BTreeMap<String, BTreeSet<i64>> = BTreeMap::new();
    for (ticker, timestamp) in tickers.into_iter().zip(timestamps.into_iter()) {
        let (Some(ticker), Some(timestamp)) = (ticker, timestamp) else {
            continue;
        };
        by_ticker
            .entry(ticker.to_string())
            .or_default()
            .insert(timestamp);
    }
    Ok(by_ticker)
}

fn distinct_tickers(frame: &DataFrame) -> PolarsResult<BTreeSet<String>> {
    Ok(frame
        .column("ticker")?
        .str()?
        .into_no_null_iter()
        .map(str::to_string)
        .collect())
}

/// The populations, then every pairwise difference in both directions.
///
/// Both directions because neither set contains the other: a longer window can only lower a name's
/// minimum close and so makes the price bound harder, while its mean notional moves either way.
fn render(
    population: &BTreeSet<String>,
    population_rows: usize,
    measured: &[Admitted],
    churn: Option<&Churn>,
) -> String {
    let mut out = String::new();
    out.push_str(&format!(
        "Window population: {} names over {} rows\n\n{:<20} {:>9} {:>10}  {}\n",
        population.len(),
        population_rows,
        "screen",
        "admitted",
        "rows",
        "declared as"
    ));
    for entry in measured {
        out.push_str(&format!(
            "{:<20} {:>9} {:>10}  {} anchored {}\n",
            entry.name,
            entry.tickers.len(),
            entry.rows,
            entry.screen,
            entry.anchor
        ));
    }

    // The point of reading the quote archive at all: every study in the tree costs turnover at one
    // market-wide literal, and this is what the set it would actually trade pays instead.
    out.push_str(&format!(
        "\nquoted spread in basis points, over the rows each screen admits\n\
         {:<20} {:>9} {:>9} {:>9} {:>9} {:>9} {:>10}\n",
        "screen", "measured", "p10", "median", "p90", "mean", "$-weighted"
    ));
    for entry in measured {
        out.push_str(&format!(
            "{:<20} {:>9} {:>9} {:>9} {:>9} {:>9} {:>10}\n",
            entry.name,
            format!(
                "{:.0}%",
                100.0 * entry.cost.measured as f64 / entry.rows.max(1) as f64
            ),
            basis_points(entry.cost.tenth_percentile),
            basis_points(entry.cost.median),
            basis_points(entry.cost.ninetieth_percentile),
            basis_points(entry.cost.mean),
            basis_points(entry.cost.dollar_weighted_mean),
        ));
    }
    if let Some(churn) = churn {
        out.push_str(&format!(
            "\nper-session churn: {} names ever admitted, {} of them without a break\n\
             {} spells, so {} re-entries -- each one costs a row its daily return\n",
            churn.names_ever_admitted, churn.names_without_a_break, churn.spells, churn.re_entries
        ));
    }

    out.push_str(&format!(
        "\n{:<20} {:<20} {:>9} {:>9}\n",
        "in", "but not in", "count", "reverse"
    ));
    // Each pair once, carrying both directions on the row. Two rows per pair would read as two
    // findings, and the whole point is that the two counts are one fact about one disagreement.
    for (index, left) in measured.iter().enumerate() {
        for right in measured.iter().skip(index + 1) {
            let only_left = left.tickers.difference(&right.tickers).count();
            let only_right = right.tickers.difference(&left.tickers).count();
            out.push_str(&format!(
                "{:<20} {:<20} {:>9} {:>9}\n",
                left.name, right.name, only_left, only_right
            ));
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(arguments: &[&str]) -> Result<Parameters, String> {
        Parameters::parse(
            &arguments
                .iter()
                .map(|argument| (*argument).to_string())
                .collect::<Vec<_>>(),
        )
    }

    #[test]
    fn test_no_arguments_takes_both_defaults() {
        let parameters = parse(&[]).expect("no arguments is the default run");
        // Literals rather than the constants, so a change to either has to be made deliberately.
        assert_eq!(parameters.lookback_days, 730);
        assert_eq!(parameters.trailing_days, 30);
    }

    /// The sweep is calendar days and says so, because the alternative claims tradability.
    ///
    /// Pinned to literals: +1 is the midweek pre-open gap and +3 is the Monday one, and a sweep that
    /// silently lost its far end would report the midweek figure as though it were the whole range.
    #[test]
    fn test_the_anchor_sweep_spans_the_midweek_gap_and_the_weekend_one() {
        assert_eq!(ANCHOR_SHIFT_DAYS, [1, 2, 3]);
    }

    #[test]
    fn test_the_trailing_window_defaults_to_the_live_books_own() {
        // The comparison is only meaningful against the window the live book actually screens on.
        assert_eq!(DEFAULT_TRAILING_DAYS, 30);
    }

    #[test]
    fn test_both_arguments_are_read_in_order() {
        let parameters = parse(&["365", "60"]).expect("two positive integers");
        assert_eq!(parameters.lookback_days, 365);
        assert_eq!(parameters.trailing_days, 60);
    }

    #[test]
    fn test_a_window_of_zero_days_is_refused_rather_than_read_as_a_default() {
        assert!(parse(&["730", "0"]).is_err());
        assert!(parse(&["0"]).is_err());
    }

    #[test]
    fn test_a_non_integer_and_a_fourth_argument_are_both_refused() {
        assert!(parse(&["730", "thirty"]).is_err());
        assert!(parse(&["730", "30", "extra"]).is_err());
    }

    /// The report has to name the population, not only the differences.
    ///
    /// A difference of zero between two empty sets reads exactly like agreement between two full
    /// ones, and this project has shipped that mistake before.
    #[test]
    fn test_the_report_names_the_population_and_both_directions_of_each_difference() {
        let screen = dataset::RESEARCH_SCREEN;
        let anchor = SessionDate::from_date(
            chrono::NaiveDate::from_ymd_opt(2026, 6, 30).expect("a real calendar date"),
        );
        let names = |values: &[&str]| -> BTreeSet<String> {
            values.iter().map(|value| (*value).to_string()).collect()
        };
        let population = names(&["AAA", "BBB", "CCC"]);
        let measured = vec![
            Admitted {
                name: "wide".to_string(),
                screen,
                anchor,
                tickers: names(&["AAA", "BBB"]),
                rows: 4,
                cost: Cost {
                    measured: 4,
                    median: Some(2.5),
                    mean: Some(3.0),
                    dollar_weighted_mean: Some(1.75),
                    tenth_percentile: Some(0.5),
                    ninetieth_percentile: Some(9.0),
                },
            },
            Admitted {
                name: "narrow".to_string(),
                screen,
                anchor,
                tickers: names(&["BBB", "CCC"]),
                rows: 4,
                cost: Cost {
                    measured: 2,
                    median: None,
                    mean: None,
                    dollar_weighted_mean: None,
                    tenth_percentile: None,
                    ninetieth_percentile: None,
                },
            },
        ];

        let report = render(&population, 9, &measured, None);

        assert!(
            report.contains("Window population: 3 names over 9 rows"),
            "{report}"
        );
        // One row per pair carrying both directions: AAA is in wide only, CCC in narrow only.
        assert!(
            report.contains("wide                 narrow                       1         1"),
            "{report}"
        );
        // Counted in the difference block itself rather than by mentions of a name, which the cost
        // table would otherwise inflate: one pair, one row, both directions on it.
        let differences = report
            .split("but not in")
            .nth(1)
            .expect("the report must carry a difference block")
            .lines()
            // The header's own tail is the first line after the split point.
            .skip(1)
            .filter(|line| !line.trim().is_empty())
            .count();
        assert_eq!(differences, 1, "one pair must produce one row\n{report}");

        // An unmeasured cost reads as `none`, never as a zero that would price a free round trip.
        assert!(
            report
                .contains("narrow                     50%      none      none      none      none"),
            "{report}"
        );
    }
}
