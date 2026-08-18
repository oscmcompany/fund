//! Forecasts to measure a model against, and the panel they read.
//!
//! Every baseline here is something TiDE must beat to have earned its place.

use std::collections::BTreeSet;

use polars::prelude::*;
use rand::{rngs::StdRng, RngExt, SeedableRng};
use serde::Serialize;

use crate::laboratory::metrics::{self, SessionMetrics};

/// Errors building a panel.
#[derive(Debug, thiserror::Error)]
pub enum PanelError {
    #[error("dataframe operation failed: {0}")]
    Frame(#[from] PolarsError),
    #[error("{0}")]
    Shape(String),
}

/// Returns laid out session by session, oldest first, with one column per name.
///
/// Sessions are the distinct timestamps the frame holds, so adjacent indices are adjacent sessions
/// and a predictor cannot accidentally reach across a gap the archive never had.
pub struct Panel {
    sessions: Vec<i64>,
    tickers: Vec<String>,
    /// `returns[session][ticker]`, absent where that name did not trade that session.
    returns: Vec<Vec<Option<f64>>>,
}

impl Panel {
    /// Builds a panel from a frame carrying `ticker`, `timestamp`, and `daily_return`.
    ///
    /// Returns must be unscaled: standardizing is monotone so it leaves the rank correlation alone,
    /// but it moves the decile spread into scaled units and can flip the sign every name is judged on.
    pub fn from_frame(frame: &DataFrame) -> Result<Self, PanelError> {
        let tickers = frame.column("ticker")?.str()?;
        let timestamps = frame.column("timestamp")?.i64()?;
        let returns = frame.column("daily_return")?.cast(&DataType::Float64)?;
        let returns = returns.f64()?;
        if tickers.null_count() > 0 || timestamps.null_count() > 0 {
            return Err(PanelError::Shape(
                "a panel needs every row to name its ticker and its session".to_string(),
            ));
        }

        let session_axis: Vec<i64> = timestamps
            .into_no_null_iter()
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        let ticker_axis: Vec<String> = tickers
            .into_no_null_iter()
            .map(str::to_string)
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();

        let session_of: std::collections::HashMap<i64, usize> = session_axis
            .iter()
            .enumerate()
            .map(|(index, session)| (*session, index))
            .collect();
        let ticker_of: std::collections::HashMap<&str, usize> = ticker_axis
            .iter()
            .enumerate()
            .map(|(index, ticker)| (ticker.as_str(), index))
            .collect();

        let mut grid = vec![vec![None; ticker_axis.len()]; session_axis.len()];
        // Occupancy separately from the values, so a row whose return was absent still counts as
        // taken: a second row for the same name and session would otherwise overwrite it silently
        // and leave the panel depending on which order the frame happened to arrive in.
        let mut filled = vec![vec![false; ticker_axis.len()]; session_axis.len()];
        for ((ticker, timestamp), value) in tickers
            .into_no_null_iter()
            .zip(timestamps.into_no_null_iter())
            .zip(returns)
        {
            let (Some(session), Some(name)) = (session_of.get(&timestamp), ticker_of.get(ticker))
            else {
                continue;
            };
            if filled[*session][*name] {
                return Err(PanelError::Shape(format!(
                    "{ticker} appears twice in the session at {timestamp}"
                )));
            }
            filled[*session][*name] = true;
            grid[*session][*name] = value.filter(|number| number.is_finite());
        }

        Ok(Self {
            sessions: session_axis,
            tickers: ticker_axis,
            returns: grid,
        })
    }

    pub fn sessions(&self) -> usize {
        self.sessions.len()
    }

    pub fn tickers(&self) -> usize {
        self.tickers.len()
    }

    /// The timestamp of the session at `index`.
    pub fn session_at(&self, index: usize) -> i64 {
        self.sessions[index]
    }

    /// Every name's return in the session at `index`.
    pub fn returns_at(&self, index: usize) -> &[Option<f64>] {
        &self.returns[index]
    }

    /// One name's return in the session at `index`.
    pub fn return_of(&self, index: usize, ticker: usize) -> Option<f64> {
        self.returns[index][ticker]
    }

    /// Everything observable before the session at `index`.
    pub fn history_before(&self, index: usize) -> History<'_> {
        History {
            panel: self,
            sessions: index,
        }
    }
}

/// The sessions strictly before the one being forecast.
///
/// A predictor is handed this rather than the panel, so reading the outcome it is scored against is
/// not something it can get wrong — the sessions are not reachable through it.
pub struct History<'a> {
    panel: &'a Panel,
    sessions: usize,
}

impl History<'_> {
    /// How many sessions precede the one being forecast.
    pub fn sessions(&self) -> usize {
        self.sessions
    }

    pub fn tickers(&self) -> usize {
        self.panel.tickers()
    }

    /// The timestamp of the session at `index`, which is `None` past the end of the history.
    pub fn session_at(&self, index: usize) -> Option<i64> {
        (index < self.sessions).then(|| self.panel.session_at(index))
    }

    /// The timestamp of the session being forecast.
    ///
    /// Available because a predictor may legitimately key on which session it is scoring — a
    /// reproducible ordering needs it — without being able to read what happened.
    pub fn forecast_session(&self) -> i64 {
        self.panel.session_at(self.sessions)
    }

    /// Every name's return in the session at `index`, or `None` past the end of the history.
    pub fn returns_at(&self, index: usize) -> Option<&[Option<f64>]> {
        (index < self.sessions).then(|| self.panel.returns_at(index))
    }

    /// One name's return in the session at `index`.
    pub fn return_of(&self, index: usize, ticker: usize) -> Option<f64> {
        (index < self.sessions)
            .then(|| self.panel.return_of(index, ticker))
            .flatten()
    }
}

/// A cross-sectional forecast: one score per name for one session.
///
/// Reads a [`History`] rather than the panel, so the outcome it is scored against is out of reach
/// by construction. A forecast that could see it would report a coefficient nobody could trade.
pub trait Predictor {
    fn name(&self) -> &str;
    fn score(&self, history: &History) -> Vec<Option<f64>>;
}

/// Predicts the previous session's cross-sectional mean for every name alike.
///
/// The null a forecast must beat to be worth anything: it is a defensible estimate of the market
/// and carries no cross-sectional information at all, so its rank correlation is undefined rather
/// than poor. A model that scores like this has learned the drift and nothing else.
pub struct CrossSectionalMean;

impl Predictor for CrossSectionalMean {
    fn name(&self) -> &str {
        "cross_sectional_mean"
    }

    fn score(&self, history: &History) -> Vec<Option<f64>> {
        let Some(previous) = history.returns_at(history.sessions().wrapping_sub(1)) else {
            return vec![None; history.tickers()];
        };
        let observed: Vec<f64> = previous.iter().flatten().copied().collect();
        let mean =
            (!observed.is_empty()).then(|| observed.iter().sum::<f64>() / observed.len() as f64);
        vec![mean; history.tickers()]
    }
}

/// Predicts each name's previous session return.
pub struct Persistence;

impl Predictor for Persistence {
    fn name(&self) -> &str {
        "persistence"
    }

    fn score(&self, history: &History) -> Vec<Option<f64>> {
        history
            .returns_at(history.sessions().wrapping_sub(1))
            .map_or_else(|| vec![None; history.tickers()], <[_]>::to_vec)
    }
}

/// Predicts each name's return summed over the sessions immediately before.
///
/// A real if weak factor, so a model that cannot beat it has not earned its complexity.
pub struct Momentum {
    pub sessions: usize,
}

impl Predictor for Momentum {
    fn name(&self) -> &str {
        "momentum"
    }

    fn score(&self, history: &History) -> Vec<Option<f64>> {
        if self.sessions == 0 || history.sessions() < self.sessions {
            return vec![None; history.tickers()];
        }
        let window = history.sessions() - self.sessions..history.sessions();
        (0..history.tickers())
            .map(|ticker| {
                // Absent anywhere in the window means absent: summing over the sessions a name did
                // trade would compare a partial history against a whole one.
                window
                    .clone()
                    .map(|session| history.return_of(session, ticker))
                    .sum::<Option<f64>>()
            })
            .collect()
    }
}

/// Stirs two values into one seed.
///
/// The splitmix64 finalizer, so a run and a session combine without colliding the way an exclusive
/// or does: seed 7 at session 1 would otherwise draw the same ordering as seed 6 at session 0.
fn mix(seed: u64, session: u64) -> u64 {
    let mut value = seed
        .wrapping_mul(0x9E37_79B9_7F4A_7C15)
        .wrapping_add(session);
    value = (value ^ (value >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    value ^ (value >> 31)
}

/// Scores names arbitrarily, reproducibly.
///
/// The null for a ranking metric, where the cross-sectional mean is the null for a regression one:
/// this one can rank, and should rank no better than chance.
pub struct RandomRanking {
    pub seed: u64,
}

impl Predictor for RandomRanking {
    fn name(&self) -> &str {
        "random_ranking"
    }

    fn score(&self, history: &History) -> Vec<Option<f64>> {
        // Abstains where the others do, so every baseline is measured over the same sessions and
        // their coefficients are comparable, which is the only reason a baseline exists.
        if history.sessions() == 0 {
            return vec![None; history.tickers()];
        }
        let mut generator =
            StdRng::seed_from_u64(mix(self.seed, history.forecast_session() as u64));
        (0..history.tickers())
            .map(|_| Some(generator.random::<f64>()))
            .collect()
    }
}

/// What one predictor scored over one panel.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Evaluation {
    pub predictor: String,
    /// One entry per session the panel holds, in order. Nothing precedes the first, so every
    /// predictor abstains there and it carries no readings.
    pub sessions: Vec<SessionMetrics>,
    pub information_coefficient: Option<metrics::Distribution>,
    pub decile_spread: Option<metrics::Distribution>,
    pub directional_accuracy: Option<metrics::Distribution>,
}

/// Scores every session of `panel` and summarizes the result.
///
/// A name is measured only where the forecast and the outcome both exist, so a listing part-way
/// through the window costs its own rows and nobody else's.
pub fn evaluate(predictor: &dyn Predictor, panel: &Panel) -> Evaluation {
    let mut sessions = Vec::with_capacity(panel.sessions());
    for index in 0..panel.sessions() {
        let scored = predictor.score(&panel.history_before(index));
        let realized = panel.returns_at(index);
        let (scores, outcomes): (Vec<f64>, Vec<f64>) = scored
            .iter()
            .zip(realized)
            .filter_map(|(score, outcome)| score.zip(*outcome))
            .unzip();
        sessions.push(metrics::measure_session(&scores, &outcomes));
    }

    Evaluation {
        predictor: predictor.name().to_string(),
        information_coefficient: metrics::summarize(
            sessions.iter().map(|s| s.information_coefficient),
        ),
        decile_spread: metrics::summarize(sessions.iter().map(|s| s.decile_spread)),
        directional_accuracy: metrics::summarize(sessions.iter().map(|s| s.directional_accuracy)),
        sessions,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const DAY: i64 = 86_400_000;

    /// Three names over four sessions, with one name absent for one of them.
    fn panel_frame() -> DataFrame {
        let mut tickers: Vec<&str> = Vec::new();
        let mut timestamps: Vec<i64> = Vec::new();
        let mut returns: Vec<f64> = Vec::new();
        for session in 0..4_i64 {
            for (offset, ticker) in ["AAA", "BBB", "CCC"].iter().enumerate() {
                if session == 2 && *ticker == "CCC" {
                    continue;
                }
                tickers.push(ticker);
                timestamps.push(session * DAY);
                returns.push(session as f64 + offset as f64 / 10.0);
            }
        }
        DataFrame::new(vec![
            Column::new("ticker".into(), tickers),
            Column::new("timestamp".into(), timestamps),
            Column::new("daily_return".into(), returns),
        ])
        .unwrap()
    }

    fn panel() -> Panel {
        Panel::from_frame(&panel_frame()).unwrap()
    }

    #[test]
    fn test_a_panel_lays_out_every_session_against_every_name() {
        let panel = panel();
        assert_eq!(panel.sessions(), 4);
        assert_eq!(panel.tickers(), 3);
        assert_eq!(panel.session_at(0), 0);
        assert_eq!(panel.session_at(3), 3 * DAY);
        // AAA, BBB, CCC sorted; CCC is absent in session 2.
        assert_eq!(panel.returns_at(2), &[Some(2.0), Some(2.1), None]);
    }

    /// What makes the lookahead contract structural rather than remembered: the session being
    /// forecast, and every session after it, is simply not reachable through the view a predictor
    /// gets. Its timestamp is, because a reproducible ordering needs to key on something.
    #[test]
    fn test_a_history_does_not_reach_the_session_it_precedes() {
        let panel = panel();
        let history = panel.history_before(2);

        assert_eq!(history.sessions(), 2);
        assert!(history.returns_at(1).is_some());
        assert_eq!(history.returns_at(2), None, "the session being forecast");
        assert_eq!(history.returns_at(3), None, "and everything after it");
        assert_eq!(history.return_of(2, 0), None);
        assert_eq!(history.session_at(2), None);
        assert_eq!(history.forecast_session(), 2 * DAY);
    }

    /// The contract every measurement downstream assumes. A predictor that reads its own session
    /// reports a coefficient it could never have traded.
    ///
    /// Asserting only that the first session is empty would not catch it: a predictor reading
    /// `returns_at(index)` still returns nothing at index zero. So the outcome under test is moved
    /// and the scores have to be indifferent to it.
    #[test]
    fn test_no_baseline_reads_the_session_it_scores() {
        let baselines: Vec<Box<dyn Predictor>> = vec![
            Box::new(CrossSectionalMean),
            Box::new(Persistence),
            Box::new(Momentum { sessions: 2 }),
            Box::new(RandomRanking { seed: 3 }),
        ];

        let mut moved = panel_frame();
        let returns = moved.column("daily_return").unwrap().f64().unwrap();
        let timestamps = moved.column("timestamp").unwrap().i64().unwrap();
        let rewritten: Vec<f64> = returns
            .into_no_null_iter()
            .zip(timestamps.into_no_null_iter())
            .map(|(value, session)| if session == 3 * DAY { -value } else { value })
            .collect();
        moved
            .with_column(Column::new("daily_return".into(), rewritten))
            .unwrap();
        let moved = Panel::from_frame(&moved).unwrap();
        let original = panel();

        for baseline in baselines {
            assert!(
                baseline
                    .score(&original.history_before(0))
                    .iter()
                    .all(Option::is_none)
                    || baseline.name() == "random_ranking",
                "{} scored the first session, which has nothing before it",
                baseline.name()
            );
            assert_eq!(
                baseline.score(&original.history_before(3)),
                baseline.score(&moved.history_before(3)),
                "{} changed its forecast when the session it forecasts changed",
                baseline.name()
            );
        }
    }

    /// Constant across the cross-section by construction, which is what makes it the null: it can
    /// estimate the market and cannot rank within it.
    #[test]
    fn test_the_cross_sectional_mean_cannot_rank() {
        let panel = panel();
        let scored = CrossSectionalMean.score(&panel.history_before(2));
        // Session 1 holds 1.0, 1.1 and 1.2, whose mean is 1.1 for all three names alike.
        assert_eq!(scored.len(), 3);
        for score in &scored {
            assert!(
                (score.unwrap() - 1.1).abs() < 1e-12,
                "expected 1.1, scored {score:?}"
            );
        }

        let realized: Vec<f64> = panel.returns_at(2).iter().flatten().copied().collect();
        let scores: Vec<f64> = scored
            .iter()
            .take(realized.len())
            .flatten()
            .copied()
            .collect();
        assert_eq!(
            metrics::information_coefficient(&scores, &realized),
            None,
            "a constant forecast has no ordering to be right about"
        );
    }

    #[test]
    fn test_persistence_repeats_the_previous_session() {
        let panel = panel();
        assert_eq!(
            Persistence.score(&panel.history_before(3)),
            vec![Some(2.0), Some(2.1), None],
            "CCC did not trade in session 2, so it has nothing to repeat"
        );
    }

    /// Sessions 1 and 2 for AAA are 1.0 and 2.0, so the two-session momentum into session 3 is 3.0.
    /// CCC is absent in session 2, so its window is incomplete and it scores nothing.
    #[test]
    fn test_momentum_sums_a_whole_window_or_none_of_it() {
        let panel = panel();
        assert_eq!(
            Momentum { sessions: 2 }.score(&panel.history_before(3)),
            vec![Some(3.0), Some(3.2), None]
        );
    }

    #[test]
    fn test_random_ranking_is_reproducible_and_varies_by_session() {
        let panel = panel();
        let first = RandomRanking { seed: 7 };
        let again = RandomRanking { seed: 7 };
        assert_eq!(
            first.score(&panel.history_before(1)),
            again.score(&panel.history_before(1))
        );
        assert_ne!(
            first.score(&panel.history_before(1)),
            first.score(&panel.history_before(2))
        );
        assert_ne!(
            first.score(&panel.history_before(1)),
            RandomRanking { seed: 8 }.score(&panel.history_before(1))
        );
    }

    /// The same name twice in one session leaves the panel depending on the order the frame
    /// arrived in, so it is refused rather than resolved to whichever row came last.
    #[test]
    fn test_a_name_appearing_twice_in_one_session_is_refused() {
        let duplicated = DataFrame::new(vec![
            Column::new("ticker".into(), vec!["AAA", "AAA"]),
            Column::new("timestamp".into(), vec![0_i64, 0]),
            Column::new("daily_return".into(), vec![0.1_f64, 0.2]),
        ])
        .unwrap();
        assert!(matches!(
            Panel::from_frame(&duplicated),
            Err(PanelError::Shape(_))
        ));
    }

    /// A repeat whose first return was absent still counts as taken, which is why occupancy is
    /// tracked apart from the values.
    #[test]
    fn test_a_repeat_of_an_absent_return_is_still_a_duplicate() {
        let duplicated = DataFrame::new(vec![
            Column::new("ticker".into(), vec!["AAA", "AAA"]),
            Column::new("timestamp".into(), vec![0_i64, 0]),
            Column::new("daily_return".into(), vec![None, Some(0.2_f64)]),
        ])
        .unwrap();
        assert!(matches!(
            Panel::from_frame(&duplicated),
            Err(PanelError::Shape(_))
        ));
    }

    #[test]
    fn test_a_row_that_names_no_session_or_ticker_is_refused() {
        let unnamed = DataFrame::new(vec![
            Column::new("ticker".into(), vec![Some("AAA"), None]),
            Column::new("timestamp".into(), vec![0_i64, DAY]),
            Column::new("daily_return".into(), vec![0.1_f64, 0.2]),
        ])
        .unwrap();
        assert!(matches!(
            Panel::from_frame(&unnamed),
            Err(PanelError::Shape(_))
        ));

        let undated = DataFrame::new(vec![
            Column::new("ticker".into(), vec!["AAA", "BBB"]),
            Column::new("timestamp".into(), vec![Some(0_i64), None]),
            Column::new("daily_return".into(), vec![0.1_f64, 0.2]),
        ])
        .unwrap();
        assert!(matches!(
            Panel::from_frame(&undated),
            Err(PanelError::Shape(_))
        ));
    }

    #[test]
    fn test_a_frame_without_the_columns_a_panel_needs_is_refused() {
        let bare = DataFrame::new(vec![Column::new("ticker".into(), vec!["AAA"])]).unwrap();
        assert!(matches!(
            Panel::from_frame(&bare),
            Err(PanelError::Frame(_))
        ));
    }

    /// A non-finite return is absent rather than poisoning a cross-section, and an integer-typed
    /// column is cast rather than refused — the archive writes returns at more than one width.
    #[test]
    fn test_an_unusable_return_is_absent_and_a_narrow_one_is_cast() {
        let mixed = DataFrame::new(vec![
            Column::new("ticker".into(), vec!["AAA", "BBB"]),
            Column::new("timestamp".into(), vec![0_i64, 0]),
            Column::new("daily_return".into(), vec![f64::NAN, 0.2]),
        ])
        .unwrap();
        assert_eq!(
            Panel::from_frame(&mixed).unwrap().returns_at(0),
            &[None, Some(0.2)]
        );

        let narrow = DataFrame::new(vec![
            Column::new("ticker".into(), vec!["AAA"]),
            Column::new("timestamp".into(), vec![0_i64]),
            Column::new("daily_return".into(), vec![2_i32]),
        ])
        .unwrap();
        assert_eq!(
            Panel::from_frame(&narrow).unwrap().returns_at(0),
            &[Some(2.0)]
        );
    }

    #[test]
    fn test_an_evaluation_names_its_predictor_and_covers_every_session() {
        let panel = panel();
        let evaluation = evaluate(&Persistence, &panel);
        assert_eq!(evaluation.predictor, "persistence");
        assert_eq!(evaluation.sessions.len(), 4);
        // Nothing precedes the first session, so it yields no readings.
        assert_eq!(evaluation.sessions[0], SessionMetrics::default());
    }
}
