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
        for ((ticker, timestamp), value) in tickers
            .into_no_null_iter()
            .zip(timestamps.into_no_null_iter())
            .zip(returns)
        {
            let (Some(session), Some(name)) = (session_of.get(&timestamp), ticker_of.get(ticker))
            else {
                continue;
            };
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
}

/// A cross-sectional forecast: one score per name for one session.
///
/// `score` may read only sessions strictly before `index`. Everything downstream treats that as
/// given, so a predictor that reads its own session reports a coefficient it could never trade.
pub trait Predictor {
    fn name(&self) -> &str;
    fn score(&self, panel: &Panel, index: usize) -> Vec<Option<f64>>;
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

    fn score(&self, panel: &Panel, index: usize) -> Vec<Option<f64>> {
        if index == 0 {
            return vec![None; panel.tickers()];
        }
        let previous: Vec<f64> = panel
            .returns_at(index - 1)
            .iter()
            .flatten()
            .copied()
            .collect();
        let mean =
            (!previous.is_empty()).then(|| previous.iter().sum::<f64>() / previous.len() as f64);
        vec![mean; panel.tickers()]
    }
}

/// Predicts each name's previous session return.
pub struct Persistence;

impl Predictor for Persistence {
    fn name(&self) -> &str {
        "persistence"
    }

    fn score(&self, panel: &Panel, index: usize) -> Vec<Option<f64>> {
        if index == 0 {
            return vec![None; panel.tickers()];
        }
        panel.returns_at(index - 1).to_vec()
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

    fn score(&self, panel: &Panel, index: usize) -> Vec<Option<f64>> {
        if index < self.sessions || self.sessions == 0 {
            return vec![None; panel.tickers()];
        }
        (0..panel.tickers())
            .map(|ticker| {
                // Absent anywhere in the window means absent: summing over the sessions a name did
                // trade would compare a partial history against a whole one.
                (index - self.sessions..index)
                    .map(|session| panel.return_of(session, ticker))
                    .sum::<Option<f64>>()
            })
            .collect()
    }
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

    fn score(&self, panel: &Panel, index: usize) -> Vec<Option<f64>> {
        // Seeded from the session as well as the run, so one session's ordering is reproducible on
        // its own and two sessions do not share it.
        let mut generator = StdRng::seed_from_u64(self.seed ^ index as u64);
        (0..panel.tickers())
            .map(|_| Some(generator.random::<f64>()))
            .collect()
    }
}

/// What one predictor scored over one panel.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Evaluation {
    pub predictor: String,
    /// One entry per session the panel holds, in order. The first is always empty of readings.
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
        let scored = predictor.score(panel, index);
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
                baseline.score(&original, 0).iter().all(Option::is_none)
                    || baseline.name() == "random_ranking",
                "{} scored the first session, which has nothing before it",
                baseline.name()
            );
            assert_eq!(
                baseline.score(&original, 3),
                baseline.score(&moved, 3),
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
        let scored = CrossSectionalMean.score(&panel, 2);
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
            Persistence.score(&panel, 3),
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
            Momentum { sessions: 2 }.score(&panel, 3),
            vec![Some(3.0), Some(3.2), None]
        );
    }

    #[test]
    fn test_random_ranking_is_reproducible_and_varies_by_session() {
        let panel = panel();
        let first = RandomRanking { seed: 7 };
        let again = RandomRanking { seed: 7 };
        assert_eq!(first.score(&panel, 1), again.score(&panel, 1));
        assert_ne!(first.score(&panel, 1), first.score(&panel, 2));
        assert_ne!(
            first.score(&panel, 1),
            RandomRanking { seed: 8 }.score(&panel, 1)
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
