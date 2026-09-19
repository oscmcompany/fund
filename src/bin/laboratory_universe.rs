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
}

/// What a per-session screen costs beyond the names it refuses.
///
/// A name that leaves and returns loses its first session back as well, because `engineer_features`
/// gives that row no `daily_return` -- the frame-wide session rank says it does not follow the row
/// before it. So the panel pays for the churn twice, and the second charge is invisible here unless
/// it is counted.
struct Churn {
    names_ever_admitted: usize,
    names_always_admitted: usize,
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

    let window =
        dataset::unscreened_window(&s3_client, &bucket, parameters.lookback_days, session).await?;
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
            churn = Some(churn_of(&screened)?);
        }
        measured.push(Admitted {
            name,
            screen,
            anchor,
            tickers,
            rows: screened.height(),
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
/// A spell is a run of consecutive admitted sessions in the frame's own session calendar. Counting
/// against that calendar rather than row adjacency is what makes a gap a gap: two rows either side
/// of a refused session are adjacent in the screened frame and are not consecutive sessions.
fn churn_of(screened: &DataFrame) -> PolarsResult<Churn> {
    let sorted = screened.sort(
        ["ticker", "timestamp"],
        SortMultipleOptions::default().with_maintain_order(true),
    )?;
    let ranks = session_ranks(sorted.column("timestamp")?.i64()?);
    let tickers = sorted.column("ticker")?.str()?;
    let timestamps = sorted.column("timestamp")?.i64()?;

    let mut spells_by_ticker: BTreeMap<String, usize> = BTreeMap::new();
    let mut previous: Option<(&str, usize)> = None;
    for row in 0..sorted.height() {
        let (Some(ticker), Some(timestamp)) = (tickers.get(row), timestamps.get(row)) else {
            continue;
        };
        let rank = ranks[&timestamp];
        let opens_a_spell = match previous {
            Some((name, last_rank)) => name != ticker || rank != last_rank + 1,
            None => true,
        };
        if opens_a_spell {
            *spells_by_ticker.entry(ticker.to_string()).or_default() += 1;
        }
        previous = Some((ticker, rank));
    }

    let spells: usize = spells_by_ticker.values().sum();
    Ok(Churn {
        names_ever_admitted: spells_by_ticker.len(),
        names_always_admitted: spells_by_ticker
            .values()
            .filter(|count| **count == 1)
            .count(),
        spells,
        // Every spell after a name's first is a re-entry, and each costs one row's `daily_return`.
        re_entries: spells - spells_by_ticker.len(),
    })
}

/// Each distinct session in the frame, numbered in time order.
fn session_ranks(timestamps: &Int64Chunked) -> BTreeMap<i64, usize> {
    let mut sessions: Vec<i64> = timestamps
        .into_no_null_iter()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    sessions.sort_unstable();
    sessions
        .into_iter()
        .enumerate()
        .map(|(rank, session)| (session, rank))
        .collect()
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
    if let Some(churn) = churn {
        out.push_str(&format!(
            "\nper-session churn: {} names ever admitted, {} of them without a break\n\
             {} spells, so {} re-entries -- each one costs a row its daily return\n",
            churn.names_ever_admitted, churn.names_always_admitted, churn.spells, churn.re_entries
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
            },
            Admitted {
                name: "narrow".to_string(),
                screen,
                anchor,
                tickers: names(&["BBB", "CCC"]),
                rows: 4,
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
        assert_eq!(
            report.matches("narrow").count(),
            2,
            "the pair must appear once as a row and once as a screen, never twice as a row\n{report}"
        );
    }
}
