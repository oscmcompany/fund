//! Cross-cadence agreement over the summaries the archive already holds.
//!
//! Verifies rather than re-emits: re-deriving both sides would hide the cross-pass claim.

use std::collections::{BTreeMap, BTreeSet};

use polars::prelude::*;
use thiserror::Error;

use crate::common::types::{BarInterval, IntradayCadence, SessionDate};

/// Largest gap a reconstructed value may carry and still count as agreement.
///
/// Scaled by `1 + |coarse|` rather than taken absolute, because these columns span nine orders of
/// magnitude -- covered seconds in the tens, dollar volume in the billions -- and one epsilon cannot
/// serve both. A five-minute row is at most 390 one-minute terms, which accumulates roughly `1e-14`
/// of relative float error, so this leaves five decades of headroom over the summing noise.
const TOLERANCE: f64 = 1e-9;

/// Disagreements carried in a report before it stops collecting them.
///
/// A session that disagrees on one name and one that disagrees on every name need different
/// responses, and the counts already separate those -- what the examples are for is naming somewhere
/// to start looking.
const EXAMPLES_KEPT: usize = 5;

/// How a coarse row's value is rebuilt from the fine rows beneath it.
#[derive(Clone, Copy, Debug)]
enum Reconstruction {
    /// Added.
    Sum,
    /// Averaged across the fine rows, weighted by another column.
    Weighted { by: &'static str },
}

/// One column, and the rule that rebuilds it.
#[derive(Clone, Copy, Debug)]
struct Reconstructed {
    column: &'static str,
    rule: Reconstruction,
}

/// The additive and weighted columns of a quote summary.
const QUOTE_RECONSTRUCTED: &[Reconstructed] = &[
    Reconstructed {
        column: "quote_count",
        rule: Reconstruction::Sum,
    },
    Reconstructed {
        column: "covered_seconds",
        rule: Reconstruction::Sum,
    },
    Reconstructed {
        column: "quoted_spread_mean",
        rule: Reconstruction::Weighted {
            by: "covered_seconds",
        },
    },
    Reconstructed {
        column: "quoted_spread_basis_points_mean",
        rule: Reconstruction::Weighted {
            by: "covered_seconds",
        },
    },
    Reconstructed {
        column: "bid_size_mean",
        rule: Reconstruction::Weighted {
            by: "covered_seconds",
        },
    },
    Reconstructed {
        column: "ask_size_mean",
        rule: Reconstruction::Weighted {
            by: "covered_seconds",
        },
    },
];

/// The quote columns no fold reconstructs.
///
/// A five-minute median is not the median of five one-minute medians, and no weighting recovers it.
/// Outside the check by construction rather than by omission, and named in the report so nothing
/// reads a passing check as covering the whole row.
const QUOTE_OPAQUE: &[&str] = &[
    "quoted_spread_basis_points_median",
    "quoted_spread_basis_points_ninetieth_percentile",
];

/// The additive and weighted columns of a trade summary, exclusion counters included.
const TRADE_RECONSTRUCTED: &[Reconstructed] = &[
    Reconstructed {
        column: "trade_count",
        rule: Reconstruction::Sum,
    },
    Reconstructed {
        column: "volume",
        rule: Reconstruction::Sum,
    },
    Reconstructed {
        column: "dollar_volume",
        rule: Reconstruction::Sum,
    },
    Reconstructed {
        column: "signed_volume",
        rule: Reconstruction::Sum,
    },
    Reconstructed {
        column: "volume_ineligible_trades",
        rule: Reconstruction::Sum,
    },
    Reconstructed {
        column: "volume_ineligible_dollar_volume",
        rule: Reconstruction::Sum,
    },
    Reconstructed {
        column: "corrected_trades",
        rule: Reconstruction::Sum,
    },
    Reconstructed {
        column: "corrected_dollar_volume",
        rule: Reconstruction::Sum,
    },
    Reconstructed {
        column: "non_market_price_trades",
        rule: Reconstruction::Sum,
    },
    Reconstructed {
        column: "non_market_price_dollar_volume",
        rule: Reconstruction::Sum,
    },
    Reconstructed {
        column: "unresolved_condition_trades",
        rule: Reconstruction::Sum,
    },
    Reconstructed {
        column: "volume_weighted_average_price",
        rule: Reconstruction::Weighted { by: "volume" },
    },
];

/// The trade columns no fold reconstructs, for the reason the quote ones are not.
const TRADE_OPAQUE: &[&str] = &["median_trade_size", "ninetieth_percentile_trade_size"];

/// What can go wrong before any comparison happens.
#[derive(Debug, Error)]
pub enum CadenceError {
    #[error("{0}")]
    Frame(#[from] PolarsError),
    /// The fold runs one way. A coarse row is built from fine ones and never the reverse.
    #[error("{finer} rows do not fold into a {coarser} row")]
    NotCoarser {
        finer: BarInterval,
        coarser: BarInterval,
    },
    /// A session's daily partition is one row per name at one instant, which is what makes the
    /// whole-session fold a single bucket. More than one stamp means the partition is not a session.
    #[error("the {interval} partition for {session} carries {stamps} distinct timestamps")]
    AmbiguousSessionStamp {
        interval: BarInterval,
        session: SessionDate,
        stamps: usize,
    },
    #[error("column {column} is missing a value the comparison needs")]
    NullKey { column: &'static str },
    /// One partition holds rows of more than one cadence, so there is no interval to fold it as.
    ///
    /// Reachable from the archive rather than only from a malformed file: a merge keeps rows it does
    /// not overwrite, so a partition written twice under different rules can carry both.
    #[error("one partition holds both {first} and {second} rows")]
    MixedIntervals { first: String, second: String },
    #[error("{interval} is not a cadence this archive stores")]
    UnknownInterval { interval: String },
}

/// The columns of one summary family, split by whether a coarser row reconstructs them.
///
/// Table-driven because the two families differ only in their columns and their weight, and a check
/// that hardcoded either would have to be written twice and kept in step by hand.
#[derive(Clone, Copy, Debug)]
pub struct CadenceCheck {
    family: &'static str,
    reconstructed: &'static [Reconstructed],
    opaque: &'static [&'static str],
}

impl CadenceCheck {
    /// The quote summary's columns, weighted by prevailing time.
    pub const fn quotes() -> Self {
        Self {
            family: "quotes",
            reconstructed: QUOTE_RECONSTRUCTED,
            opaque: QUOTE_OPAQUE,
        }
    }

    /// The trade summary's columns, weighted by volume.
    pub const fn trades() -> Self {
        Self {
            family: "trades",
            reconstructed: TRADE_RECONSTRUCTED,
            opaque: TRADE_OPAQUE,
        }
    }

    /// The columns this check does not cover, named so a pass is never read as covering the row.
    pub fn opaque(&self) -> &'static [&'static str] {
        self.opaque
    }

    /// Folds `finer` up to `coarser_interval` and compares it to `coarser`, writing nothing.
    ///
    /// Equal intervals are permitted and are the degenerate fold: every row is its own bucket, which
    /// is how a derived session row is checked against the stored one. Rows present on one side only
    /// are counted as coverage rather than as disagreement -- the two cadences were written by
    /// different passes over universes that need not match.
    pub fn compare(
        &self,
        finer: &DataFrame,
        coarser: &DataFrame,
        coarser_interval: BarInterval,
        session: SessionDate,
    ) -> Result<CadenceAgreement, CadenceError> {
        let finer_interval = sole_interval(finer)?.unwrap_or(coarser_interval);
        if coarseness(finer_interval) > coarseness(coarser_interval) {
            return Err(CadenceError::NotCoarser {
                finer: finer_interval,
                coarser: coarser_interval,
            });
        }

        let bucketed = self.bucket(finer, coarser, coarser_interval, session)?;
        let reconstructed = self.reconstruct(bucketed)?;
        let matched = reconstructed
            .clone()
            .lazy()
            .join(
                coarser.clone().lazy(),
                [col("ticker"), col(BUCKET)],
                [col("ticker"), col("timestamp")],
                JoinArgs::new(JoinType::Inner),
            )
            .collect()?;

        let mut columns = Vec::with_capacity(self.reconstructed.len());
        for entry in self.reconstructed {
            columns.push(compare_column(&matched, entry.column)?);
        }

        Ok(CadenceAgreement {
            family: self.family,
            session,
            finer: finer_interval,
            coarser: coarser_interval,
            compared: matched.height(),
            // Subtracted rather than anti-joined: the reconstruction is unique on its key by
            // construction, so an inflated join would mean the coarse partition holds a duplicate
            // key -- which shows up as an impossible count here rather than being silently absorbed.
            finer_only: reconstructed.height().saturating_sub(matched.height()),
            coarser_only: coarser.height().saturating_sub(matched.height()),
            columns,
            opaque: self.opaque,
        })
    }

    /// Tags each fine row with the coarse bucket it belongs to.
    fn bucket(
        &self,
        finer: &DataFrame,
        coarser: &DataFrame,
        coarser_interval: BarInterval,
        session: SessionDate,
    ) -> Result<DataFrame, CadenceError> {
        let stamps = finer.column("timestamp")?.i64()?;
        let buckets: Vec<i64> = match coarser_interval {
            // Read off the coarse frame rather than recomputed. The session stamp is a convention --
            // 16:00 Eastern even on an early close -- so deriving it here would check this module
            // against itself instead of against what the archive stores.
            BarInterval::OneDay => {
                let stored: BTreeSet<i64> = coarser
                    .column("timestamp")?
                    .i64()?
                    .iter()
                    .flatten()
                    .collect();
                let [stamp] = stored.iter().copied().collect::<Vec<_>>()[..] else {
                    return Err(CadenceError::AmbiguousSessionStamp {
                        interval: coarser_interval,
                        session,
                        stamps: stored.len(),
                    });
                };
                vec![stamp; finer.height()]
            }
            BarInterval::OneMinute | BarInterval::FiveMinute => {
                let width = IntradayCadence::from_bar_interval(coarser_interval)
                    .expect("an intraday interval names a cadence")
                    .seconds()
                    * 1_000;
                stamps
                    .iter()
                    .map(|stamp| {
                        stamp.map(|stamp| stamp.div_euclid(width) * width).ok_or(
                            CadenceError::NullKey {
                                column: "timestamp",
                            },
                        )
                    })
                    .collect::<Result<_, _>>()?
            }
        };

        let mut bucketed = finer.clone();
        bucketed.with_column(Column::new(BUCKET.into(), buckets))?;
        Ok(bucketed)
    }

    /// Rebuilds one coarse row per `(ticker, bucket)` out of the fine rows in it.
    fn reconstruct(&self, bucketed: DataFrame) -> Result<DataFrame, CadenceError> {
        let aggregations: Vec<Expr> = self
            .reconstructed
            .iter()
            .map(|entry| {
                let expression = match entry.rule {
                    Reconstruction::Sum => col(entry.column).sum(),
                    // Weighted only by the rows that carry a value: a null VWAP is a bar where
                    // nothing eligible traded, and letting its volume into the denominator would
                    // pull the reconstruction toward zero on exactly the bars it says nothing about.
                    Reconstruction::Weighted { by } => {
                        let weight = when(col(entry.column).is_null())
                            .then(lit(0.0))
                            .otherwise(col(by));
                        let total = weight.clone().sum();
                        // A bucket where nothing carried a value reconstructs to nothing, not to
                        // `0/0`. NaN here compares against the stored null as a disagreement, which
                        // on the trade archive fires on every window the exclusions emptied.
                        when(total.clone().gt(lit(0.0)))
                            .then(
                                (col(entry.column).fill_null(lit(0.0)) * weight).sum()
                                    / total.clone(),
                            )
                            .otherwise(lit(NULL).cast(DataType::Float64))
                    }
                };
                expression.alias(reconstructed_name(entry.column))
            })
            .collect();

        Ok(bucketed
            .lazy()
            .group_by([col("ticker"), col(BUCKET)])
            .agg(aggregations)
            .collect()?)
    }
}

/// The column a bucketed fine frame carries its coarse key in.
const BUCKET: &str = "coarse_timestamp";

/// What the reconstruction of `column` is called once it sits beside the stored value.
fn reconstructed_name(column: &str) -> PlSmallStr {
    format!("{column}_reconstructed").into()
}

/// The interval every row of a partition carries, or `None` where the column is absent.
///
/// Read from the frame rather than passed in, so a comparison cannot be told the fine side is
/// one-minute while the partition it was handed is five. A value that is present and unreadable is
/// refused rather than treated as absent: absent falls back to the coarse interval, which would
/// silently disable the [`CadenceError::NotCoarser`] guard the fallback exists beneath.
fn sole_interval(frame: &DataFrame) -> Result<Option<BarInterval>, CadenceError> {
    let Ok(column) = frame.column("bar_interval") else {
        return Ok(None);
    };
    let intervals: BTreeSet<&str> = column.str()?.iter().flatten().collect();
    let mut named = intervals.into_iter();
    let Some(interval) = named.next() else {
        return Ok(None);
    };
    if let Some(second) = named.next() {
        return Err(CadenceError::MixedIntervals {
            first: interval.to_string(),
            second: second.to_string(),
        });
    }
    BarInterval::parse(interval)
        .map(Some)
        .ok_or_else(|| CadenceError::UnknownInterval {
            interval: interval.to_string(),
        })
}

/// Orders the intervals coarsest-last, which is the only comparison the fold direction needs.
///
/// Deliberately not a `seconds` method on [`BarInterval`]: a daily bar does not span 86,400 seconds
/// of quoting, it spans a session, and a number saying otherwise would be read as a duration.
fn coarseness(interval: BarInterval) -> u8 {
    match interval {
        BarInterval::OneMinute => 0,
        BarInterval::FiveMinute => 1,
        BarInterval::OneDay => 2,
    }
}

/// Compares one column's reconstruction against the value stored beside it.
///
/// Row by row rather than as an expression: the arithmetic is trivial and the loop is what makes
/// null-against-null an agreement, which no scalar comparison expresses.
fn compare_column(matched: &DataFrame, column: &str) -> Result<ColumnAgreement, CadenceError> {
    let reconstructed = matched
        .column(&reconstructed_name(column))?
        .cast(&DataType::Float64)?;
    let stored = matched.column(column)?.cast(&DataType::Float64)?;
    let tickers = matched.column("ticker")?.str()?.clone();
    let stamps = matched.column(BUCKET)?.i64()?.clone();

    let mut agreement = ColumnAgreement {
        column: column.to_string(),
        matched: 0,
        worst: None,
        examples: Vec::new(),
    };
    for (index, (left, right)) in reconstructed
        .f64()?
        .iter()
        .zip(stored.f64()?.iter())
        .enumerate()
    {
        let gap = match (left, right) {
            // Neither pass produced a value, which is agreement about there being nothing to say.
            (None, None) => {
                agreement.matched += 1;
                continue;
            }
            (Some(left), Some(right)) => (left - right).abs() / (1.0 + right.abs()),
            // One side has a value and the other does not, which no tolerance can reconcile.
            _ => f64::INFINITY,
        };
        if gap <= TOLERANCE {
            agreement.matched += 1;
            continue;
        }
        let disagreement = Disagreement {
            ticker: tickers.get(index).unwrap_or("?").to_string(),
            timestamp: stamps.get(index).unwrap_or_default(),
            reconstructed: left,
            stored: right,
            gap,
        };
        if agreement.examples.len() < EXAMPLES_KEPT {
            agreement.examples.push(disagreement.clone());
        }
        if agreement.worst.as_ref().is_none_or(|worst| gap > worst.gap) {
            agreement.worst = Some(disagreement);
        }
    }
    Ok(agreement)
}

/// One column's outcome across the rows both cadences hold.
#[derive(Clone, Debug)]
pub struct ColumnAgreement {
    column: String,
    matched: usize,
    /// The largest gap seen, which is the one number that says how bad a disagreement is.
    worst: Option<Disagreement>,
    examples: Vec<Disagreement>,
}

impl ColumnAgreement {
    pub fn column(&self) -> &str {
        &self.column
    }

    pub fn matched(&self) -> usize {
        self.matched
    }

    pub fn worst(&self) -> Option<&Disagreement> {
        self.worst.as_ref()
    }

    pub fn examples(&self) -> &[Disagreement] {
        &self.examples
    }
}

/// One row where the two cadences do not agree.
#[derive(Clone, Debug)]
pub struct Disagreement {
    ticker: String,
    timestamp: i64,
    reconstructed: Option<f64>,
    stored: Option<f64>,
    gap: f64,
}

impl std::fmt::Display for Disagreement {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let value = |value: Option<f64>| match value {
            Some(value) => format!("{value}"),
            None => "none".to_string(),
        };
        write!(
            formatter,
            "{} at {}: folded {} against stored {} (gap {:.3e})",
            self.ticker,
            self.timestamp,
            value(self.reconstructed),
            value(self.stored),
            self.gap
        )
    }
}

/// What one session's cross-cadence comparison found.
#[derive(Clone, Debug)]
pub struct CadenceAgreement {
    family: &'static str,
    session: SessionDate,
    finer: BarInterval,
    coarser: BarInterval,
    compared: usize,
    finer_only: usize,
    coarser_only: usize,
    columns: Vec<ColumnAgreement>,
    opaque: &'static [&'static str],
}

impl CadenceAgreement {
    pub fn session(&self) -> SessionDate {
        self.session
    }

    pub fn compared(&self) -> usize {
        self.compared
    }

    /// Rows one cadence holds and the other does not.
    ///
    /// A universe difference rather than a disagreement: the cadences were written by separate
    /// passes, and a name absent from one of them was never compared at all.
    pub fn one_sided(&self) -> (usize, usize) {
        (self.finer_only, self.coarser_only)
    }

    pub fn columns(&self) -> &[ColumnAgreement] {
        &self.columns
    }

    /// Columns this check does not cover, named so a pass is never read as covering the row.
    pub fn opaque(&self) -> &[&'static str] {
        self.opaque
    }

    /// Rows that disagreed, summed across columns, which is what makes a session pass or fail.
    pub fn disagreements(&self) -> usize {
        self.columns
            .iter()
            .map(|column| self.compared.saturating_sub(column.matched))
            .sum()
    }

    /// Whether every reconstructed column agreed on every row both cadences hold.
    ///
    /// Says nothing about the rows only one of them holds, which `one_sided` reports separately.
    /// **A comparison that matched nothing has not agreed** — the join key is `(ticker, bucket)`,
    /// so a fold bucketed to the wrong stamp agrees over an empty population.
    pub fn agrees(&self) -> bool {
        self.compared > 0 && self.disagreements() == 0
    }

    /// The largest gap any column carried, for a one-line report.
    pub fn worst(&self) -> Option<(&str, &Disagreement)> {
        self.columns
            .iter()
            .filter_map(|column| column.worst().map(|worst| (column.column(), worst)))
            .max_by(|left, right| left.1.gap.total_cmp(&right.1.gap))
    }
}

impl std::fmt::Display for CadenceAgreement {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "{} {} {}->{}: {} rows compared, {} disagreements, {} folded-only, {} stored-only",
            self.session,
            self.family,
            self.finer,
            self.coarser,
            self.compared,
            self.disagreements(),
            self.finer_only,
            self.coarser_only
        )
    }
}

/// What became of one session's comparison.
///
/// One session reaches exactly one of these, so they are variants rather than parallel lists. Only
/// [`SessionOutcome::Agreed`] is a pass: the other four are each a different way of not having
/// checked something, and summing them into a clean figure is what makes a check overstate itself.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum SessionOutcome {
    /// Every reconstructed column matched, over a population that was not empty.
    Agreed,
    /// At least one column disagreed.
    Disagreed,
    /// The two cadences shared no row, so nothing was compared.
    NothingCompared,
    /// The coarser prefix holds no partition for this session.
    Absent,
    /// A partition was there and could not be read as a cadence.
    Unusable,
}

impl SessionOutcome {
    /// The outcome a finished comparison reached.
    pub fn of(agreement: &CadenceAgreement) -> Self {
        match agreement {
            agreement if agreement.compared() == 0 => SessionOutcome::NothingCompared,
            agreement if agreement.agrees() => SessionOutcome::Agreed,
            _ => SessionOutcome::Disagreed,
        }
    }
}

impl std::fmt::Display for SessionOutcome {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            SessionOutcome::Agreed => "agreed",
            SessionOutcome::Disagreed => "disagreed",
            SessionOutcome::NothingCompared => "compared nothing",
            SessionOutcome::Absent => "absent",
            SessionOutcome::Unusable => "unusable",
        })
    }
}

/// What a check found across a window, one outcome per session.
#[derive(Clone, Debug, Default)]
pub struct CadenceTotals {
    outcomes: BTreeMap<SessionDate, SessionOutcome>,
    compared: usize,
    disagreements: usize,
    finer_only: usize,
    coarser_only: usize,
}

impl CadenceTotals {
    /// Records a comparison that ran, whatever it found.
    pub fn absorb(&mut self, agreement: &CadenceAgreement) {
        self.compared += agreement.compared();
        self.disagreements += agreement.disagreements();
        let (finer_only, coarser_only) = agreement.one_sided();
        self.finer_only += finer_only;
        self.coarser_only += coarser_only;
        self.outcomes
            .insert(agreement.session(), SessionOutcome::of(agreement));
    }

    /// Records a session no comparison could be made for, which is not the same as one that agreed.
    pub fn note(&mut self, session: SessionDate, outcome: SessionOutcome) {
        self.outcomes.insert(session, outcome);
    }

    /// Sessions the check reached, by any route including the ones it could not compare.
    pub fn sessions(&self) -> usize {
        self.outcomes.len()
    }

    pub fn sessions_agreeing(&self) -> usize {
        self.sessions_with(SessionOutcome::Agreed).len()
    }

    /// The sessions that reached `outcome`, dated rather than counted.
    ///
    /// A count says a re-fold is needed and cannot say which sessions to run it over.
    pub fn sessions_with(&self, outcome: SessionOutcome) -> Vec<SessionDate> {
        self.outcomes
            .iter()
            .filter(|(_, reached)| **reached == outcome)
            .map(|(session, _)| *session)
            .collect()
    }

    /// Every session that did not agree, with the reason it did not.
    pub fn unresolved(&self) -> Vec<(SessionDate, SessionOutcome)> {
        self.outcomes
            .iter()
            .filter(|(_, outcome)| **outcome != SessionOutcome::Agreed)
            .map(|(session, outcome)| (*session, *outcome))
            .collect()
    }

    pub fn compared(&self) -> usize {
        self.compared
    }

    pub fn disagreements(&self) -> usize {
        self.disagreements
    }

    pub fn one_sided(&self) -> (usize, usize) {
        (self.finer_only, self.coarser_only)
    }

    /// Whether every session reached agreed, which is the exit condition a check reports on.
    ///
    /// A window that checked nothing does not pass, and neither does one that could only pair half
    /// its sessions -- both reach zero disagreements without establishing anything.
    pub fn agrees(&self) -> bool {
        !self.outcomes.is_empty()
            && self
                .outcomes
                .values()
                .all(|outcome| *outcome == SessionOutcome::Agreed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use chrono::NaiveDate;

    fn session() -> SessionDate {
        session_on(20)
    }

    fn session_on(day: u32) -> SessionDate {
        SessionDate::from_date(NaiveDate::from_ymd_opt(2026, 8, day).expect("a real date"))
    }

    /// 2026-08-20T20:00Z, the 16:00 Eastern close of [`session`] and where the archive stamps a
    /// session row -- an instant no intraday bucket of that session lands on.
    const SESSION_STAMP: i64 = 1_787_256_000_000;

    /// The session fixtures are stamped inside the session they name.
    ///
    /// The join key is taken off the stored row, so a stamp from the wrong year compares green
    /// while describing a partition that could not exist. Nothing else here would catch it.
    #[test]
    fn test_the_session_stamp_falls_within_the_session_it_names() {
        let (start, end) = session().bounds();
        let stamp = chrono::DateTime::from_timestamp_millis(SESSION_STAMP).expect("a real instant");
        assert!(
            stamp >= start && stamp < end,
            "{stamp} is outside {}",
            session()
        );
        assert_eq!(stamp.to_rfc3339(), "2026-08-20T20:00:00+00:00");
    }

    /// A quote frame carrying only what a comparison reads.
    ///
    /// The four time-weighted columns are given one value per row, so a weighted reconstruction is
    /// checkable by hand rather than by re-running the arithmetic under test.
    fn quotes(interval: BarInterval, rows: &[(&str, i64, i64, f64, f64)]) -> DataFrame {
        let spreads: Vec<f64> = rows.iter().map(|row| row.4).collect();
        DataFrame::new(vec![
            Column::new(
                "ticker".into(),
                rows.iter().map(|row| row.0).collect::<Vec<_>>(),
            ),
            Column::new("bar_interval".into(), vec![interval.as_str(); rows.len()]),
            Column::new(
                "timestamp".into(),
                rows.iter().map(|row| row.1).collect::<Vec<_>>(),
            ),
            Column::new("quoted_spread_mean".into(), spreads.clone()),
            Column::new("quoted_spread_basis_points_mean".into(), spreads.clone()),
            // Deliberately absurd and deliberately different on the two sides: nothing reconstructs
            // a median, and a check that quietly compared one would fail on every real session.
            Column::new(
                "quoted_spread_basis_points_median".into(),
                vec![rows.len() as f64 * 1_000.0; rows.len()],
            ),
            Column::new(
                "quoted_spread_basis_points_ninetieth_percentile".into(),
                vec![rows.len() as f64 * 9_000.0; rows.len()],
            ),
            Column::new("bid_size_mean".into(), spreads.clone()),
            Column::new("ask_size_mean".into(), spreads),
            Column::new(
                "quote_count".into(),
                rows.iter().map(|row| row.2).collect::<Vec<_>>(),
            ),
            Column::new(
                "covered_seconds".into(),
                rows.iter().map(|row| row.3).collect::<Vec<_>>(),
            ),
        ])
        .expect("a frame of equal-length columns")
    }

    /// Five one-minute buckets of one name, each a full minute of book at `spread`.
    fn five_one_minute_buckets(spreads: [f64; 5], covered: [f64; 5]) -> DataFrame {
        let rows: Vec<(&str, i64, i64, f64, f64)> = (0..5)
            .map(|index| {
                (
                    "AAPL",
                    index as i64 * 60_000,
                    10,
                    covered[index],
                    spreads[index],
                )
            })
            .collect();
        quotes(BarInterval::OneMinute, &rows)
    }

    fn agreement(
        finer: &DataFrame,
        coarser: &DataFrame,
        coarser_interval: BarInterval,
    ) -> CadenceAgreement {
        CadenceCheck::quotes()
            .compare(finer, coarser, coarser_interval, session())
            .expect("a comparable pair")
    }

    #[test]
    fn test_five_one_minute_buckets_reconstruct_the_five_minute_row() {
        let finer = five_one_minute_buckets([2.0; 5], [60.0; 5]);
        let coarser = quotes(BarInterval::FiveMinute, &[("AAPL", 0, 50, 300.0, 2.0)]);

        let outcome = agreement(&finer, &coarser, BarInterval::FiveMinute);

        assert_eq!(outcome.compared(), 1);
        assert_eq!(outcome.disagreements(), 0);
        assert!(outcome.agrees());
        assert_eq!(outcome.one_sided(), (0, 0));
    }

    #[test]
    fn test_a_count_short_by_one_is_a_disagreement() {
        let finer = five_one_minute_buckets([2.0; 5], [60.0; 5]);
        // Forty-nine against the fifty the buckets hold. One quote, out of a session's hundreds of
        // thousands, and the check has to see it or it cannot see a lost bucket either.
        let coarser = quotes(BarInterval::FiveMinute, &[("AAPL", 0, 49, 300.0, 2.0)]);

        let outcome = agreement(&finer, &coarser, BarInterval::FiveMinute);

        assert!(!outcome.agrees());
        assert_eq!(outcome.disagreements(), 1);
        let (column, worst) = outcome.worst().expect("a disagreement names its column");
        assert_eq!(column, "quote_count");
        assert_eq!(worst.ticker, "AAPL");
    }

    #[test]
    fn test_a_time_weighted_mean_reconstructs_from_uneven_buckets() {
        // Ten seconds at one basis point and ninety at two, which is 1.9 and not the 1.5 an
        // unweighted average of the two buckets would give.
        let finer = quotes(
            BarInterval::OneMinute,
            &[("AAPL", 0, 4, 10.0, 1.0), ("AAPL", 60_000, 6, 90.0, 2.0)],
        );
        let coarser = quotes(BarInterval::FiveMinute, &[("AAPL", 0, 10, 100.0, 1.9)]);

        assert!(agreement(&finer, &coarser, BarInterval::FiveMinute).agrees());
    }

    #[test]
    fn test_an_unweighted_mean_would_fail_the_same_pair() {
        let finer = quotes(
            BarInterval::OneMinute,
            &[("AAPL", 0, 4, 10.0, 1.0), ("AAPL", 60_000, 6, 90.0, 2.0)],
        );
        // 1.5 is what averaging the buckets without their weights gives. Asserted so the weighting
        // is proved load-bearing here rather than by mutating the code and remembering to restore.
        let coarser = quotes(BarInterval::FiveMinute, &[("AAPL", 0, 10, 100.0, 1.5)]);

        let outcome = agreement(&finer, &coarser, BarInterval::FiveMinute);

        assert!(!outcome.agrees());
        // All four weighted columns carry the same value in this fixture, so all four disagree.
        assert_eq!(outcome.disagreements(), 4);
    }

    #[test]
    fn test_a_median_is_named_as_unchecked_rather_than_compared() {
        let finer = five_one_minute_buckets([2.0; 5], [60.0; 5]);
        let coarser = quotes(BarInterval::FiveMinute, &[("AAPL", 0, 50, 300.0, 2.0)]);

        let outcome = agreement(&finer, &coarser, BarInterval::FiveMinute);

        // The fixture's medians are 5,000 against 1,000 and its ninetieths 45,000 against 9,000.
        assert!(outcome.agrees());
        assert_eq!(
            outcome.opaque(),
            [
                "quoted_spread_basis_points_median",
                "quoted_spread_basis_points_ninetieth_percentile"
            ]
        );
        assert!(outcome
            .columns()
            .iter()
            .all(|column| !column.column().contains("median")));
    }

    #[test]
    fn test_every_bucket_of_a_session_folds_into_the_stored_session_row() {
        let finer = five_one_minute_buckets([1.0, 2.0, 3.0, 4.0, 5.0], [60.0; 5]);
        // Stamped at 16:00 Eastern, which no one-minute bucket carries -- the fold has to take the
        // coarse key off the stored row rather than off the fine ones.
        let coarser = quotes(
            BarInterval::OneDay,
            &[("AAPL", SESSION_STAMP, 50, 300.0, 3.0)],
        );

        assert!(agreement(&finer, &coarser, BarInterval::OneDay).agrees());
    }

    #[test]
    fn test_a_session_row_compared_against_itself_is_the_degenerate_fold() {
        let stored = quotes(
            BarInterval::OneDay,
            &[("AAPL", SESSION_STAMP, 50, 300.0, 3.0)],
        );

        let outcome = agreement(&stored, &stored, BarInterval::OneDay);

        assert_eq!(outcome.compared(), 1);
        assert!(outcome.agrees());
    }

    #[test]
    fn test_a_coarser_row_does_not_fold_into_a_finer_one() {
        let five_minute = quotes(BarInterval::FiveMinute, &[("AAPL", 0, 50, 300.0, 2.0)]);
        let one_minute = quotes(BarInterval::OneMinute, &[("AAPL", 0, 10, 60.0, 2.0)]);

        let error = CadenceCheck::quotes()
            .compare(&five_minute, &one_minute, BarInterval::OneMinute, session())
            .expect_err("the fold runs one way");

        assert!(matches!(
            error,
            CadenceError::NotCoarser {
                finer: BarInterval::FiveMinute,
                coarser: BarInterval::OneMinute
            }
        ));
    }

    #[test]
    fn test_a_name_only_one_cadence_holds_is_coverage_rather_than_disagreement() {
        let finer = quotes(
            BarInterval::OneMinute,
            &[("AAPL", 0, 10, 60.0, 2.0), ("MSFT", 0, 10, 60.0, 2.0)],
        );
        let coarser = quotes(
            BarInterval::FiveMinute,
            &[("AAPL", 0, 10, 60.0, 2.0), ("TSLA", 0, 10, 60.0, 2.0)],
        );

        let outcome = agreement(&finer, &coarser, BarInterval::FiveMinute);

        // MSFT and TSLA were never compared, so nothing can be said about them either way.
        assert_eq!(outcome.compared(), 1);
        assert_eq!(outcome.one_sided(), (1, 1));
        assert!(outcome.agrees());
    }

    #[test]
    fn test_a_comparison_that_matched_nothing_has_not_agreed() {
        let finer = five_one_minute_buckets([2.0; 5], [60.0; 5]);
        // A stored session row for a name the fine side never held. Every column then "agrees" over
        // an empty population, which is the shape a mis-bucketed fold produces.
        let coarser = quotes(
            BarInterval::OneDay,
            &[("MSFT", SESSION_STAMP, 50, 300.0, 2.0)],
        );

        let outcome = agreement(&finer, &coarser, BarInterval::OneDay);

        assert_eq!(outcome.compared(), 0);
        assert_eq!(outcome.disagreements(), 0);
        assert!(!outcome.agrees());
    }

    #[test]
    fn test_a_window_that_checked_nothing_does_not_pass() {
        let empty = CadenceTotals::default();
        assert_eq!(empty.sessions(), 0);
        assert_eq!(empty.disagreements(), 0);
        assert!(!empty.agrees());

        let mut totals = CadenceTotals::default();
        totals.absorb(&agreement(
            &five_one_minute_buckets([2.0; 5], [60.0; 5]),
            &quotes(
                BarInterval::OneDay,
                &[("MSFT", SESSION_STAMP, 50, 300.0, 2.0)],
            ),
            BarInterval::OneDay,
        ));

        assert_eq!(totals.sessions(), 1);
        assert_eq!(totals.sessions_agreeing(), 0);
        assert_eq!(totals.disagreements(), 0);
        assert_eq!(
            totals.sessions_with(SessionOutcome::NothingCompared),
            [session()]
        );
        assert!(totals.sessions_with(SessionOutcome::Disagreed).is_empty());
        assert!(!totals.agrees());
    }

    /// Four ways a session fails to agree, and only one way it passes.
    ///
    /// Pinned as a whole rather than one variant at a time: the failure this guards against is a
    /// window summing the four into a clean figure, which no single-variant assertion would catch.
    #[test]
    fn test_only_an_agreeing_window_passes() {
        let finer = five_one_minute_buckets([2.0; 5], [60.0; 5]);
        let agreeing = quotes(BarInterval::FiveMinute, &[("AAPL", 0, 50, 300.0, 2.0)]);

        for absent in [
            SessionOutcome::Disagreed,
            SessionOutcome::NothingCompared,
            SessionOutcome::Absent,
            SessionOutcome::Unusable,
        ] {
            let mut totals = CadenceTotals::default();
            totals.absorb(&agreement(&finer, &agreeing, BarInterval::FiveMinute));
            assert!(totals.agrees(), "one agreeing session should pass");

            totals.note(session_on(21), absent);
            assert!(!totals.agrees(), "a {absent} session must not pass");
            assert_eq!(totals.unresolved(), [(session_on(21), absent)]);
        }
    }

    #[test]
    fn test_totals_date_the_sessions_that_disagreed() {
        let mut totals = CadenceTotals::default();
        let finer = five_one_minute_buckets([2.0; 5], [60.0; 5]);

        totals.absorb(&agreement(
            &finer,
            &quotes(BarInterval::FiveMinute, &[("AAPL", 0, 50, 300.0, 2.0)]),
            BarInterval::FiveMinute,
        ));
        // A second session, because the totals key on the date: absorbing the same session twice
        // records one outcome, which is what makes a re-check correct rather than double-counted.
        totals.absorb(
            &CadenceCheck::quotes()
                .compare(
                    &finer,
                    &quotes(BarInterval::FiveMinute, &[("AAPL", 0, 49, 300.0, 2.0)]),
                    BarInterval::FiveMinute,
                    session_on(21),
                )
                .expect("a comparable pair"),
        );

        assert_eq!(totals.sessions(), 2);
        assert_eq!(totals.sessions_agreeing(), 1);
        assert_eq!(totals.compared(), 2);
        assert_eq!(totals.disagreements(), 1);
        assert!(!totals.agrees());
        assert_eq!(
            totals.sessions_with(SessionOutcome::Disagreed),
            [session_on(21)]
        );
    }

    #[test]
    fn test_re_checking_one_session_records_one_outcome() {
        let mut totals = CadenceTotals::default();
        let finer = five_one_minute_buckets([2.0; 5], [60.0; 5]);

        totals.note(session(), SessionOutcome::Absent);
        assert!(!totals.agrees());

        // The same session, now comparable. Keyed on the date, so the later outcome replaces the
        // earlier one rather than leaving the window permanently unresolved.
        totals.absorb(&agreement(
            &finer,
            &quotes(BarInterval::FiveMinute, &[("AAPL", 0, 50, 300.0, 2.0)]),
            BarInterval::FiveMinute,
        ));
        assert_eq!(totals.sessions(), 1);
        assert!(totals.agrees());
    }

    #[test]
    fn test_a_partition_holding_two_cadences_is_not_foldable() {
        // Reachable from the archive, not only from a malformed file: a merge keeps rows it does not
        // overwrite, so a partition written twice under different rules carries both.
        let mut mixed = quotes(
            BarInterval::OneMinute,
            &[("AAPL", 0, 10, 60.0, 2.0), ("AAPL", 60_000, 10, 60.0, 2.0)],
        );
        mixed
            .with_column(Column::new(
                "bar_interval".into(),
                vec!["one_minute", "five_minute"],
            ))
            .expect("a replaceable column");

        let error = CadenceCheck::quotes()
            .compare(
                &mixed,
                &quotes(BarInterval::FiveMinute, &[("AAPL", 0, 20, 120.0, 2.0)]),
                BarInterval::FiveMinute,
                session(),
            )
            .expect_err("one partition cannot be two cadences");

        assert!(matches!(error, CadenceError::MixedIntervals { .. }));
    }

    #[test]
    fn test_an_unreadable_interval_is_refused_rather_than_assumed() {
        // Refused rather than treated as absent: absent falls back to the coarse interval, which
        // would silently disable the `NotCoarser` guard the fallback sits beneath.
        let mut unreadable = quotes(BarInterval::OneMinute, &[("AAPL", 0, 10, 60.0, 2.0)]);
        unreadable
            .with_column(Column::new("bar_interval".into(), vec!["1Min"]))
            .expect("a replaceable column");

        let error = CadenceCheck::quotes()
            .compare(
                &unreadable,
                &quotes(BarInterval::FiveMinute, &[("AAPL", 0, 10, 60.0, 2.0)]),
                BarInterval::FiveMinute,
                session(),
            )
            .expect_err("an unreadable interval names no cadence");

        assert!(matches!(
            error,
            CadenceError::UnknownInterval { ref interval } if interval == "1Min"
        ));
    }

    #[test]
    fn test_a_frame_without_an_interval_column_still_compares() {
        // The column is optional, unlike its value: a frame that never carried one is legacy data
        // rather than corrupt, and falls back to the interval it is being folded up to.
        let mut legacy = five_one_minute_buckets([2.0; 5], [60.0; 5]);
        legacy.drop_in_place("bar_interval").expect("the column");

        let outcome = CadenceCheck::quotes()
            .compare(
                &legacy,
                &quotes(BarInterval::FiveMinute, &[("AAPL", 0, 50, 300.0, 2.0)]),
                BarInterval::FiveMinute,
                session(),
            )
            .expect("a comparable pair");

        assert!(outcome.agrees());
    }

    #[test]
    fn test_a_daily_partition_of_two_stamps_is_not_a_session() {
        let finer = five_one_minute_buckets([2.0; 5], [60.0; 5]);
        let coarser = quotes(
            BarInterval::OneDay,
            &[
                ("AAPL", SESSION_STAMP, 50, 300.0, 2.0),
                ("AAPL", SESSION_STAMP + 1, 50, 300.0, 2.0),
            ],
        );

        let error = CadenceCheck::quotes()
            .compare(&finer, &coarser, BarInterval::OneDay, session())
            .expect_err("two stamps is not one session");

        assert!(matches!(
            error,
            CadenceError::AmbiguousSessionStamp { stamps: 2, .. }
        ));
    }

    #[test]
    fn test_a_null_timestamp_has_no_bucket_to_fold_into() {
        let mut null_stamped = quotes(BarInterval::OneMinute, &[("AAPL", 0, 10, 60.0, 2.0)]);
        null_stamped
            .with_column(Column::new("timestamp".into(), vec![None::<i64>]))
            .expect("a replaceable column");

        let error = CadenceCheck::quotes()
            .compare(
                &null_stamped,
                &quotes(BarInterval::FiveMinute, &[("AAPL", 0, 10, 60.0, 2.0)]),
                BarInterval::FiveMinute,
                session(),
            )
            .expect_err("a row with no stamp belongs to no bucket");

        assert!(matches!(
            error,
            CadenceError::NullKey {
                column: "timestamp"
            }
        ));
    }

    /// A trade frame carrying only what a comparison reads, with a nullable volume-weighted price.
    fn trades(interval: BarInterval, rows: &[(&str, i64, i64, f64, Option<f64>)]) -> DataFrame {
        let counted = |name: &str, value: i64| Column::new(name.into(), vec![value; rows.len()]);
        let summed = |name: &str, value: f64| Column::new(name.into(), vec![value; rows.len()]);
        DataFrame::new(vec![
            Column::new(
                "ticker".into(),
                rows.iter().map(|row| row.0).collect::<Vec<_>>(),
            ),
            Column::new("bar_interval".into(), vec![interval.as_str(); rows.len()]),
            Column::new(
                "timestamp".into(),
                rows.iter().map(|row| row.1).collect::<Vec<_>>(),
            ),
            Column::new(
                "trade_count".into(),
                rows.iter().map(|row| row.2).collect::<Vec<_>>(),
            ),
            Column::new(
                "volume".into(),
                rows.iter().map(|row| row.3).collect::<Vec<_>>(),
            ),
            summed("dollar_volume", 0.0),
            Column::new(
                "volume_weighted_average_price".into(),
                rows.iter().map(|row| row.4).collect::<Vec<_>>(),
            ),
            summed("median_trade_size", 0.0),
            summed("ninetieth_percentile_trade_size", 0.0),
            summed("signed_volume", 0.0),
            counted("volume_ineligible_trades", 0),
            summed("volume_ineligible_dollar_volume", 0.0),
            counted("corrected_trades", 0),
            summed("corrected_dollar_volume", 0.0),
            counted("non_market_price_trades", 0),
            summed("non_market_price_dollar_volume", 0.0),
            counted("unresolved_condition_trades", 0),
        ])
        .expect("a frame of equal-length columns")
    }

    #[test]
    fn test_a_volume_weighted_price_reconstructs_across_trade_buckets() {
        // A hundred shares at ten dollars and three hundred at fourteen, which is thirteen and not
        // the twelve an unweighted average of the two buckets gives.
        let finer = trades(
            BarInterval::OneMinute,
            &[
                ("AAPL", 0, 3, 100.0, Some(10.0)),
                ("AAPL", 60_000, 5, 300.0, Some(14.0)),
            ],
        );
        let coarser = trades(
            BarInterval::FiveMinute,
            &[("AAPL", 0, 8, 400.0, Some(13.0))],
        );

        let outcome = CadenceCheck::trades()
            .compare(&finer, &coarser, BarInterval::FiveMinute, session())
            .expect("a comparable pair");

        assert!(outcome.agrees());
        assert_eq!(outcome.compared(), 1);
    }

    #[test]
    fn test_a_bar_that_admitted_no_eligible_share_keeps_its_weight_out_of_the_average() {
        // The null bucket carries volume the exclusions account for. Letting it into the denominator
        // would report 5.0 against the stored 10.0 and fail a session that is entirely correct.
        let finer = trades(
            BarInterval::OneMinute,
            &[
                ("AAPL", 0, 3, 100.0, Some(10.0)),
                ("AAPL", 60_000, 0, 100.0, None),
            ],
        );
        let coarser = trades(
            BarInterval::FiveMinute,
            &[("AAPL", 0, 3, 200.0, Some(10.0))],
        );

        assert!(CadenceCheck::trades()
            .compare(&finer, &coarser, BarInterval::FiveMinute, session())
            .expect("a comparable pair")
            .agrees());
    }

    #[test]
    fn test_a_window_where_nothing_eligible_traded_reconstructs_to_nothing() {
        // Every bucket null, so the weight sums to zero. Dividing there gives NaN, which compares
        // against the stored null as a disagreement -- and on the real trade archive this fires on
        // every window the exclusions emptied, which is hundreds a session.
        let finer = trades(
            BarInterval::OneMinute,
            &[
                ("AAPL", 0, 0, 100.0, None),
                ("AAPL", 60_000, 0, 100.0, None),
            ],
        );
        let coarser = trades(BarInterval::FiveMinute, &[("AAPL", 0, 0, 200.0, None)]);

        let outcome = CadenceCheck::trades()
            .compare(&finer, &coarser, BarInterval::FiveMinute, session())
            .expect("a comparable pair");

        assert_eq!(outcome.compared(), 1);
        assert!(outcome.agrees());
    }

    #[test]
    fn test_a_value_against_no_value_is_a_disagreement_no_tolerance_reconciles() {
        let finer = trades(BarInterval::OneMinute, &[("AAPL", 0, 3, 100.0, Some(10.0))]);
        // Stored as null where the tape says ten dollars, which is a pass that lost a column rather
        // than one that rounded it.
        let coarser = trades(BarInterval::FiveMinute, &[("AAPL", 0, 3, 100.0, None)]);

        let outcome = CadenceCheck::trades()
            .compare(&finer, &coarser, BarInterval::FiveMinute, session())
            .expect("a comparable pair");

        assert!(!outcome.agrees());
        assert_eq!(
            outcome.worst().expect("a disagreement names its column").0,
            "volume_weighted_average_price"
        );
    }
}
