//! Measures the forecasts a model has to beat, over the archive and nothing else.
//!
//! Trains nothing, so a run is one archive read and some arithmetic: what the data alone is worth.

use std::num::NonZeroUsize;

use chrono::Utc;
use tracing::{error, info, warn};

use fund::common::log::init_tracing;
use fund::common::types::{BasisPoints, Screen, SessionDate};
use fund::laboratory::cost::{CostModel, FillStyle, RoundTrip};
use fund::laboratory::dataset::{self, DatasetFingerprint};
use fund::laboratory::harness::{
    distribution, Arm, Declaration, DeclaredUniverse, Family, Horizon, Pairing, Quantity, Study,
    StudyResult,
};
use fund::laboratory::journal as laboratory;
use fund::laboratory::metrics::Distribution;
use fund::laboratory::predictor::{
    evaluate, CrossSectionalMean, Evaluation, Momentum, Panel, Persistence, Predictor,
    RandomRanking,
};

const USAGE: &str =
    "Usage: laboratory_baselines [LOOKBACK_DAYS] [MOMENTUM_SESSIONS] [QUOTED_SPREAD_BASIS_POINTS]";

/// Calendar days of archive to measure over by default.
///
/// Twice the trainer's window, because nothing here is trained and a standard error over one year
/// of sessions is wide enough to call a real weak effect nothing.
const DEFAULT_LOOKBACK_DAYS: i64 = 730;

/// Sessions the momentum baseline sums over by default.
const DEFAULT_MOMENTUM_SESSIONS: i64 = 20;

/// Fixed, so two runs over one archive draw the same orderings and differ only where the data does.
const RANDOM_SEED: u64 = 0x5EED;

/// The arm every other one is scored against.
///
/// Named rather than left last in the list: a control identified by its position in a `Vec` is a
/// convention, and the next predictor appended to that list silently becomes the control.
const CONTROL: &str = "random_ranking";

/// The multiple-testing bucket these comparisons are spent against.
const FAMILY: &str = "daily-baselines";

/// What one round trip is assumed to cost, when the operator declares no spread.
///
/// A placeholder rather than a measurement, and it stays one until a study screens its own names:
/// the whole-market spread distribution on record was taken over mixed-provenance partitions and is
/// flagged contaminated. It is an argument so that the free choice is made before the reading and
/// recorded beside it.
const DEFAULT_QUOTED_SPREAD_BASIS_POINTS: f64 = 10.0;

/// What to measure, and over how much.
struct Parameters {
    lookback_days: i64,
    momentum_sessions: usize,
    quoted_spread: BasisPoints,
}

impl Parameters {
    /// Reads the three positional arguments, each falling back to its default.
    fn parse(arguments: &[String]) -> Result<Self, String> {
        let (lookback_days, momentum_sessions, spread) = match arguments {
            [] => (
                DEFAULT_LOOKBACK_DAYS,
                DEFAULT_MOMENTUM_SESSIONS,
                DEFAULT_QUOTED_SPREAD_BASIS_POINTS,
            ),
            [lookback] => (
                positive(lookback, "LOOKBACK_DAYS")?,
                DEFAULT_MOMENTUM_SESSIONS,
                DEFAULT_QUOTED_SPREAD_BASIS_POINTS,
            ),
            [lookback, momentum] => (
                positive(lookback, "LOOKBACK_DAYS")?,
                positive(momentum, "MOMENTUM_SESSIONS")?,
                DEFAULT_QUOTED_SPREAD_BASIS_POINTS,
            ),
            [lookback, momentum, spread] => (
                positive(lookback, "LOOKBACK_DAYS")?,
                positive(momentum, "MOMENTUM_SESSIONS")?,
                spread.trim().parse::<f64>().map_err(|_| {
                    format!("QUOTED_SPREAD_BASIS_POINTS must be a number, got {spread:?}\n{USAGE}")
                })?,
            ),
            _ => return Err(format!("Too many arguments\n{USAGE}")),
        };
        Ok(Self {
            lookback_days,
            // Checked rather than cast: `as usize` truncates a value past the pointer width, and a
            // momentum window that truncated to zero would abstain on every session in silence.
            momentum_sessions: usize::try_from(momentum_sessions).map_err(|_| {
                format!("MOMENTUM_SESSIONS is larger than this platform can index\n{USAGE}")
            })?,
            // Zero is admissible and means a locked book, which is a cost assumption rather than a
            // missing one. Everything `BasisPoints` refuses is named, not just the negative case: a
            // NaN told it "must not be negative" sends an operator looking for a minus sign.
            quoted_spread: BasisPoints::new(spread).ok_or_else(|| {
                format!(
                    "QUOTED_SPREAD_BASIS_POINTS must be a finite width of zero or more, got \
                     {spread}\n{USAGE}"
                )
            })?,
        })
    }
}

/// Parses a positive integer, refusing a typo rather than falling back to the default.
///
/// An operator who passed a window is asking for that window; quietly measuring a different one
/// would put a number in the journal against a fingerprint nobody chose.
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
        "laboratory-baselines.log",
        Some("info"),
        "laboratory-baselines",
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
            error!(%error, "Measuring the baselines failed");
            eprintln!("Measuring the baselines failed: {error}");
            1
        }
    };

    // `std::process::exit` runs no destructors, so the non-blocking appender's guard would never
    // drop and its buffered lines would be lost — exactly when the failure log matters.
    drop(tracing_guard);
    std::process::exit(code);
}

/// Reads the window, lays out the panel, and scores every baseline against it.
///
/// Reads `AWS_S3_ARCHIVE_BUCKET_NAME` and writes to the laboratory journal. Nothing is fetched and nothing
/// is published: this measures what the archive already holds.
async fn run(parameters: &Parameters) -> Result<String, Box<dyn std::error::Error>> {
    let bucket = fund::common::aws::archive_bucket()?;
    let s3_client = fund::common::aws::s3_client().await;

    // One instant for the run, resolved once to its Eastern session, as the trainer does: the window
    // is bounded by a trading day and the journal is stamped with an instant.
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
        %session,
        %run_id,
        "Measuring the baselines"
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
    let fingerprint = dataset.fingerprint;
    info!(
        rows = fingerprint.rows,
        tickers = fingerprint.tickers,
        "Read the archive window"
    );

    if let Some(journal) = journal.as_ref() {
        // The same record the trainer writes, so a baseline and a model measured over one window
        // share a fingerprint and their results can be put side by side.
        journal
            .record(
                run_id,
                Utc::now(),
                laboratory::Observation::DatasetBuilt(laboratory::DatasetBuilt::new(
                    fingerprint.clone(),
                )),
            )
            .await;
    }

    let panel = Panel::from_frame(&dataset.returns)?;
    info!(
        sessions = panel.sessions(),
        tickers = panel.tickers(),
        "Laid the window out session by session"
    );

    let baselines: Vec<Box<dyn Predictor>> = vec![
        Box::new(CrossSectionalMean),
        Box::new(Persistence),
        Box::new(Momentum {
            sessions: parameters.momentum_sessions,
        }),
        Box::new(RandomRanking { seed: RANDOM_SEED }),
    ];

    let mut evaluations = Vec::with_capacity(baselines.len());
    let mut scored = Vec::with_capacity(baselines.len());
    for baseline in &baselines {
        let evaluation = evaluate(baseline.as_ref(), &panel);
        let record = laboratory::ForecastScored::from(&evaluation);
        info!(
            predictor = record.predictor,
            sessions = record.sessions,
            information_coefficient = record
                .information_coefficient
                .map(|distribution| distribution.mean),
            "Scored a baseline"
        );

        if let Some(journal) = journal.as_ref() {
            journal
                .record(
                    run_id,
                    Utc::now(),
                    laboratory::Observation::ForecastScored(record.clone()),
                )
                .await;
        }
        scored.push(record);
        evaluations.push(evaluation);
    }

    // The panel's own session keys, so each arm records which sessions it read and the harness can
    // refuse a matched pair that does not line up rather than zipping two lists regardless.
    let sessions: Vec<i64> = (0..panel.sessions())
        .map(|index| panel.session_at(index))
        .collect();
    let studies = measure(
        parameters,
        &evaluations,
        &sessions,
        dataset::RESEARCH_SCREEN,
        &fingerprint,
    )?;
    if let Some(journal) = journal.as_ref() {
        for result in &studies {
            journal
                .record(
                    run_id,
                    Utc::now(),
                    laboratory::Observation::StudyMeasured(laboratory::StudyMeasured::from(result)),
                )
                .await;
        }
    }

    let mut report = render(&scored);
    // The table above describes four predictors on three statistics; the one below tests the three
    // real ones against the random arm on the only statistic a book could be paid in.
    report.push_str(
        "\nThe decile spread, in basis points, against the random ranking and net of one round \
         trip.\n\n",
    );
    report.push_str(&fund::laboratory::harness::render(&studies));
    Ok(report)
}

/// Scores each real predictor's decile spread against the random ranking's.
///
/// The decile spread rather than the information coefficient, because it is the only one of the
/// three that carries return units and can therefore be asked whether it pays for its own spread. A
/// rank correlation of 0.02 is uninterpretable next to a cost.
fn measure(
    parameters: &Parameters,
    evaluations: &[Evaluation],
    sessions: &[i64],
    screen: Screen,
    fingerprint: &DatasetFingerprint,
) -> Result<Vec<StudyResult>, Box<dyn std::error::Error>> {
    let control = evaluations
        .iter()
        .find(|evaluation| evaluation.predictor == CONTROL)
        .ok_or_else(|| -> Box<dyn std::error::Error> {
            format!("no {CONTROL} arm was scored, so nothing has a control").into()
        })?;
    let treatments: Vec<&Evaluation> = evaluations
        .iter()
        .filter(|evaluation| evaluation.predictor != CONTROL)
        .collect();
    let tests =
        NonZeroUsize::new(treatments.len()).ok_or_else(|| -> Box<dyn std::error::Error> {
            "the control was the only arm scored".into()
        })?;

    // Every arm reads the same panel, session for session, so the difference is taken per session
    // and the variation the whole cross-section shared on a day cancels instead of being counted.
    let arm = |evaluation: &Evaluation| {
        Arm::new(
            &evaluation.predictor,
            sessions
                .iter()
                .zip(&evaluation.sessions)
                // The target is a fraction and the cost is in basis points, so the reading is
                // converted here rather than left for the subtraction to get wrong.
                .map(|(key, session)| (*key, session.decile_spread.map(|spread| spread * 10_000.0)))
                .collect(),
            fingerprint.rows,
        )
        .ok_or_else(|| -> Box<dyn std::error::Error> {
            format!("{} produced no readings to score", evaluation.predictor).into()
        })
    };

    treatments
        .into_iter()
        .map(|treatment| {
            Study::new(
                Declaration::new(
                    format!("does {} pay for its own spread", treatment.predictor),
                    Family::new(FAMILY, tests),
                    // One session ahead: the panel's target is the next session's return.
                    Horizon::Sessions(NonZeroUsize::new(1).expect("a positive count")),
                    declared_universe(screen),
                    Quantity::ReturnPerRoundTrip {
                        // A decile long-short crosses one name on each side per dollar deployed,
                        // whatever the decile's width, so the round trip is a pair.
                        cost_model: CostModel::new(FillStyle::Aggressive, RoundTrip::PAIR),
                        quoted_spread: parameters.quoted_spread,
                    },
                ),
                Pairing::Matched,
                arm(treatment)?,
                arm(control)?,
                fingerprint,
            )
            .map(Study::measure)
            .map_err(|refusal| -> Box<dyn std::error::Error> { refusal.to_string().into() })
        })
        .collect()
}

/// The population the study declares, built from the screen it handed the loader.
///
/// Declared from the same value that did the screening rather than read back off the fingerprint:
/// with the screen an input, reading it back would restate the input and the harness would be
/// checking a value against itself. What `Study::new` verifies from here is that the **loader
/// honoured** the declaration — which is a narrower guarantee than it was, and still catches the
/// composition error most likely to happen, a screened declaration over `dataset::intraday`, which
/// screens nothing.
fn declared_universe(screen: Screen) -> DeclaredUniverse {
    DeclaredUniverse::Screened {
        // Versioned rather than derived from the bounds: two studies quoting "liquid" should be
        // comparable or visibly not, and a name is the only part of this a reader can hold on to.
        name: "training-liquid-v1".to_string(),
        floor: screen.floor(),
        window: screen.window(),
    }
}

/// One row per baseline, for the operator who ran it.
fn render(scored: &[laboratory::ForecastScored]) -> String {
    let mut rendered = format!(
        "{:<22}{:>10}{:>30}{:>30}{:>30}\n",
        "predictor", "sessions", "information_coefficient", "decile_spread", "directional_accuracy"
    );
    for record in scored {
        rendered.push_str(&format!(
            "{:<22}{:>10}{:>30}{:>30}{:>30}\n",
            record.predictor,
            record.sessions,
            distribution(record.information_coefficient),
            distribution(record.decile_spread),
            distribution(record.directional_accuracy),
        ));
    }
    rendered
}

#[cfg(test)]
mod tests {
    use super::*;

    fn arguments(values: &[&str]) -> Vec<String> {
        values.iter().map(|value| value.to_string()).collect()
    }

    #[test]
    fn test_no_arguments_measures_the_default_window() {
        let parameters = Parameters::parse(&[]).unwrap();
        assert_eq!(parameters.lookback_days, 730);
        assert_eq!(parameters.momentum_sessions, 20);
        assert_eq!(parameters.quoted_spread.value(), 10.0);
    }

    #[test]
    fn test_arguments_are_read_in_order_and_default_from_the_right() {
        let parameters = Parameters::parse(&arguments(&["365"])).unwrap();
        assert_eq!(parameters.lookback_days, 365);
        assert_eq!(parameters.momentum_sessions, 20);

        let parameters = Parameters::parse(&arguments(&["365", "5"])).unwrap();
        assert_eq!(parameters.lookback_days, 365);
        assert_eq!(parameters.momentum_sessions, 5);

        let parameters = Parameters::parse(&arguments(&["365", "5", "3.25"])).unwrap();
        assert_eq!(parameters.quoted_spread.value(), 3.25);
    }

    /// Zero is a cost assumption — a locked book quotes no width — where a negative one is not a
    /// book at all. The distinction matters because a study netting zero cost is a claim, not a gap.
    #[test]
    fn test_a_locked_book_is_a_spread_and_a_negative_one_is_not() {
        assert_eq!(
            Parameters::parse(&arguments(&["365", "5", "0"]))
                .unwrap()
                .quoted_spread
                .value(),
            0.0
        );
        assert!(Parameters::parse(&arguments(&["365", "5", "-1"])).is_err());
        assert!(Parameters::parse(&arguments(&["365", "5", "wide"])).is_err());
    }

    /// A typo must stop the run rather than fall back. A baseline that quietly measured a different
    /// window would journal a number against a fingerprint nobody asked for, and the fingerprint is
    /// the only thing that makes two results comparable.
    #[test]
    fn test_an_unusable_argument_is_refused() {
        for value in ["3o5", "0", "-5", ""] {
            assert!(
                Parameters::parse(&arguments(&[value])).is_err(),
                "{value:?} must be refused"
            );
        }
        assert!(Parameters::parse(&arguments(&["365", "0"])).is_err());
        assert!(Parameters::parse(&arguments(&["365", "20", "7", "extra"])).is_err());
    }

    fn parameters() -> Parameters {
        Parameters::parse(&arguments(&["365", "20", "10"])).expect("the fixture must parse")
    }

    fn fingerprint() -> DatasetFingerprint {
        DatasetFingerprint {
            session: fund::common::types::SessionDate::from_date(
                chrono::NaiveDate::from_ymd_opt(2026, 9, 17).unwrap(),
            ),
            lookback_days: 365,
            liquidity_floor: Some(dataset::RESEARCH_SCREEN.floor()),
            screen_window: Some(dataset::RESEARCH_SCREEN.window()),
            rows: 317_465,
            tickers: 1_334,
            first_timestamp: None,
            last_timestamp: None,
            splits_digest: 0,
            boundaries_digest: 0,
            reference_digest: None,
            factor_specification: None,
            microstructure: dataset::Microstructure::Omitted,
            quote_summary_digest: None,
            trade_summary_digest: None,
        }
    }

    /// The four session keys every fixture arm reads, one a day.
    fn sessions() -> Vec<i64> {
        (0..4).map(|index| index * 86_400_000).collect()
    }

    /// Four sessions of decile spreads, in fractions of a return as the panel produces them.
    fn evaluation(predictor: &str, spreads: [Option<f64>; 4]) -> Evaluation {
        Evaluation {
            predictor: predictor.to_string(),
            sessions: spreads
                .into_iter()
                .map(|spread| fund::laboratory::metrics::SessionMetrics {
                    information_coefficient: None,
                    decile_spread: spread,
                    directional_accuracy: None,
                })
                .collect(),
            information_coefficient: None,
            decile_spread: None,
            directional_accuracy: None,
        }
    }

    /// The control is named, not the last row. Every other arm is a test, and the count is theirs.
    #[test]
    fn test_the_named_control_is_excluded_and_sets_the_family_count() {
        let evaluations = vec![
            evaluation("persistence", [Some(0.001); 4]),
            evaluation("momentum", [Some(0.002); 4]),
            evaluation(CONTROL, [Some(0.0); 4]),
        ];

        let studies = measure(
            &parameters(),
            &evaluations,
            &sessions(),
            dataset::RESEARCH_SCREEN,
            &fingerprint(),
        )
        .expect("scorable");

        assert_eq!(studies.len(), 2, "the control is not a test of itself");
        for study in &studies {
            assert_eq!(study.control_name(), CONTROL);
            assert_eq!(study.declaration().family().tests().get(), 2);
        }
        let questions: Vec<&str> = studies
            .iter()
            .map(|study| study.declaration().question())
            .collect();
        assert!(questions.iter().all(|question| !question.contains(CONTROL)));
    }

    /// A fraction on the panel and basis points in the cost model, so the conversion has to happen
    /// before the subtraction rather than inside a reader's head.
    #[test]
    fn test_a_decile_spread_reaches_the_study_in_basis_points() {
        let evaluations = vec![
            // Ten basis points a session, against a control that earns nothing.
            evaluation("persistence", [Some(0.001); 4]),
            evaluation(CONTROL, [Some(0.0); 4]),
        ];

        let studies = measure(
            &parameters(),
            &evaluations,
            &sessions(),
            dataset::RESEARCH_SCREEN,
            &fingerprint(),
        )
        .expect("scorable");
        let difference = studies[0].difference().expect("measurable");

        assert!((difference.mean - 10.0).abs() < 1e-9, "{difference:?}");
        // A pair round trip at the fixture's 10bp quoted spread costs 20bp, so ten does not pay.
        match studies[0].net_of_cost() {
            fund::laboratory::harness::NetOfCost::Net {
                net_basis_points, ..
            } => assert!((net_basis_points + 10.0).abs() < 1e-9, "{net_basis_points}"),
            other => panic!("an aggressive fill is costable, got {other:?}"),
        }
    }

    /// Without a control there is no comparison, and a table of raw arms is what this replaces.
    #[test]
    fn test_a_run_with_no_control_arm_is_refused() {
        let evaluations = vec![evaluation("persistence", [Some(0.001); 4])];
        assert!(measure(
            &parameters(),
            &evaluations,
            &sessions(),
            dataset::RESEARCH_SCREEN,
            &fingerprint(),
        )
        .is_err());

        let only_control = vec![evaluation(CONTROL, [Some(0.0); 4])];
        assert!(measure(
            &parameters(),
            &only_control,
            &sessions(),
            dataset::RESEARCH_SCREEN,
            &fingerprint(),
        )
        .is_err());
    }

    fn scored(
        predictor: &str,
        information_coefficient: Option<Distribution>,
    ) -> laboratory::ForecastScored {
        laboratory::ForecastScored {
            predictor: predictor.to_string(),
            sessions: 502,
            information_coefficient,
            decile_spread: None,
            directional_accuracy: None,
        }
    }

    /// The sign is what a reader acts on, and a negative coefficient is the expected result for a
    /// daily persistence forecast — so it must not be rendered bare, where it reads as a minus sign
    /// in a table of positives.
    #[test]
    fn test_a_rendered_statistic_carries_its_sign_and_its_error() {
        let rendered = render(&[scored(
            "persistence",
            Some(Distribution {
                mean: -0.013489,
                standard_error: 0.007973,
                sessions: 502,
            }),
        )]);
        assert!(
            rendered.contains("-0.013489 ± 0.007973 (502)"),
            "{rendered}"
        );
    }

    /// The cross-sectional mean cannot rank, so it has no information coefficient at all. Rendering
    /// that as `0.000000` would read as a forecast that ranked and got it exactly wrong-by-nothing.
    #[test]
    fn test_a_statistic_that_could_not_be_measured_is_not_rendered_as_zero() {
        let rendered = render(&[scored("cross_sectional_mean", None)]);
        assert!(rendered.contains("unmeasurable"), "{rendered}");
        assert!(!rendered.contains("0.000000"), "{rendered}");
    }
}
