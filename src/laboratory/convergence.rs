//! Whether a dislocated spread closes, which is the premise the pair book rests on.
//!
//! Measured without the model, because whether it converges is a fact about prices and not a forecast.

use std::collections::{BTreeMap, BTreeSet};

use polars::prelude::*;
use rand::{rngs::StdRng, RngExt, SeedableRng};
use serde::Serialize;

use crate::common::types::CloseReason;
use crate::models::tide::TideError;
use crate::portfolio::evaluate::exit_reason;
use crate::portfolio::screen::{
    logarithmic_returns, pearson_correlation, worst_session_move, SpreadModel, CORRELATION_MAXIMUM,
    CORRELATION_MINIMUM, CORRELATION_WINDOW_SESSIONS, ENTRY_Z_SCORE, ENTRY_Z_SCORE_CAP,
    MAXIMUM_SESSION_LOGARITHMIC_RETURN,
};

/// Closing prices for every name, along one session axis.
///
/// Keyed by ticker rather than laid out as a grid because pair work reaches for two whole series at
/// a time, never for one session across every name — which is the shape [`crate::laboratory::predictor::Panel`]
/// exists to serve.
pub struct Closes {
    sessions: Vec<i64>,
    by_ticker: BTreeMap<String, Vec<Option<f64>>>,
}

impl Closes {
    /// Reads `ticker`, `timestamp` and `close_price` into one series per name.
    pub fn from_frame(frame: &DataFrame) -> Result<Self, TideError> {
        let tickers = frame.column("ticker")?.str()?;
        let timestamps = frame.column("timestamp")?.i64()?;
        let closes = frame.column("close_price")?.cast(&DataType::Float64)?;
        let closes = closes.f64()?;
        if tickers.null_count() > 0 || timestamps.null_count() > 0 {
            return Err(TideError::Data(
                "closes need every row to name its ticker and its session".to_string(),
            ));
        }

        let sessions: Vec<i64> = timestamps
            .into_no_null_iter()
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        let session_of: BTreeMap<i64, usize> = sessions
            .iter()
            .enumerate()
            .map(|(index, session)| (*session, index))
            .collect();

        let mut by_ticker: BTreeMap<String, Vec<Option<f64>>> = BTreeMap::new();
        for ((ticker, timestamp), close) in tickers
            .into_no_null_iter()
            .zip(timestamps.into_no_null_iter())
            .zip(closes)
        {
            let Some(index) = session_of.get(&timestamp) else {
                continue;
            };
            let series = by_ticker
                .entry(ticker.to_string())
                .or_insert_with(|| vec![None; sessions.len()]);
            // A price of zero or below cannot be logged and a spread is built from logs, so it is
            // absent rather than carried forward as a number the fit would reject later.
            series[*index] = close.filter(|price| price.is_finite() && *price > 0.0);
        }

        Ok(Self {
            sessions,
            by_ticker,
        })
    }

    pub fn sessions(&self) -> usize {
        self.sessions.len()
    }

    pub fn session_at(&self, index: usize) -> Option<i64> {
        self.sessions.get(index).copied()
    }

    /// Every name, in a fixed order so a sampled universe is reproducible.
    pub fn tickers(&self) -> Vec<&str> {
        self.by_ticker.keys().map(String::as_str).collect()
    }

    pub fn close_at(&self, ticker: &str, session: usize) -> Option<f64> {
        self.by_ticker.get(ticker)?.get(session).copied().flatten()
    }

    /// The last `length` sessions at or before `session` where both names traded.
    ///
    /// Aligned on sessions both legs were present for, rather than on each leg's own last `length`
    /// prices: a hedge ratio fitted across a session one leg missed regresses prices from different
    /// days on each other.
    pub fn aligned_window(
        &self,
        first: &str,
        second: &str,
        session: usize,
        length: usize,
    ) -> Option<(Vec<f64>, Vec<f64>)> {
        let first = self.by_ticker.get(first)?;
        let second = self.by_ticker.get(second)?;
        if session >= self.sessions.len() || length == 0 {
            return None;
        }

        let mut left = Vec::with_capacity(length);
        let mut right = Vec::with_capacity(length);
        for index in (0..=session).rev() {
            if let (Some(one), Some(other)) = (first[index], second[index]) {
                left.push(one);
                right.push(other);
                if left.len() == length {
                    break;
                }
            }
        }
        if left.len() < length {
            return None;
        }
        // Collected newest first, and a fit reads them oldest first.
        left.reverse();
        right.reverse();
        Some((left, right))
    }
}

/// A reproducible sample of at most `size` names, drawn without replacement.
///
/// The *universe* is sampled and not the pairs, so the selection logic stays production's rather
/// than an approximation of it — just over a smaller universe. Pair enumeration is quadratic per
/// session, and the archive's ~1,150 names are 660,000 pairs against a sample of 200's 19,900.
pub fn sample_universe(closes: &Closes, size: usize, seed: u64) -> Vec<&str> {
    let mut universe = closes.tickers();
    if universe.len() <= size {
        return universe;
    }
    let mut generator = StdRng::seed_from_u64(seed);
    // Partial Fisher-Yates: the first `size` slots become the sample, so no name is drawn twice.
    for slot in 0..size {
        let swap = slot + generator.random_range(0..universe.len() - slot);
        universe.swap(slot, swap);
    }
    universe.truncate(size);
    // Sorted, so a pair is enumerated in the same order whatever the draw put where.
    universe.sort_unstable();
    universe
}

/// Sessions an entry is followed for before it is reported as unresolved.
///
/// A ceiling on the measurement, not a holding period: the terminal event and the horizon it
/// happened at are both recorded, so the curves say where to hold rather than assuming it.
pub const HORIZONS: usize = 20;

/// How a pair is admitted, which is the whole of the control.
///
/// The two differ in one test and share everything else — the same fit window, the same entry
/// threshold, the same fixed model, the same forward walk. An extreme z is extreme partly because
/// the mean and dispersion it is measured against were *estimated* on sixty sessions, so it is less
/// extreme next session under a pure random walk. That regression to the mean is arithmetic rather
/// than economics and it lands on both arms equally, which is what makes the difference readable.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Selection {
    /// The screen's band on log-return correlation, which is what makes two names a pair.
    Screened,
    /// The band dropped, so a pair is any two names whose spread happens to be dislocated.
    ///
    /// Exhaustive rather than a random sample of pairs: the population minus one filter is the
    /// same null with no seed to report and no sampling error of its own.
    Unscreened,
}

impl Selection {
    pub fn as_str(self) -> &'static str {
        match self {
            Selection::Screened => "screened",
            Selection::Unscreened => "unscreened",
        }
    }
}

/// What first happened to an entry, and how many sessions after it opened.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Resolution {
    /// The spread fell back through its fitted mean.
    Converged(usize),
    /// The spread widened past the stop, measured from this entry's own z-score.
    Stopped(usize),
    /// Neither, through every forward session the pair could be priced at.
    Unresolved,
}

/// One pair opened at one session, and what became of it.
///
/// `observed` is the last horizon both legs could be priced at, and it is what separates an entry
/// that survived twenty sessions without resolving from one the archive ran out under. Folding the
/// second into the first would report a truncated entry as a pair that failed to converge.
#[derive(Debug, Clone, PartialEq)]
pub struct Entry {
    pub session: usize,
    pub long: String,
    pub short: String,
    pub entry_z_score: f64,
    pub resolution: Resolution,
    pub observed: usize,
}

impl Entry {
    /// Where this entry stands at `horizon`, or `None` if it was never followed that far.
    fn at(&self, horizon: usize) -> Option<Resolution> {
        match self.resolution {
            Resolution::Converged(step) | Resolution::Stopped(step) if step <= horizon => {
                Some(self.resolution)
            }
            _ if self.observed >= horizon => Some(Resolution::Unresolved),
            _ => None,
        }
    }
}

/// Where a cohort of entries stands at one horizon.
///
/// The three shares are of `entries`, which counts only the entries followed this far — an entry
/// the archive ran out under leaves the denominator rather than counting as still open.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Curve {
    pub horizon: usize,
    pub converged: f64,
    pub stopped: f64,
    pub open: f64,
    pub entries: usize,
}

/// Every entry `session` admits under `selection`, each followed forward until it resolves.
///
/// The fit window ends at the session *before* the one entry is judged at, mirroring production,
/// where the window is closed daily bars and the observation is a live price. Scoring an
/// observation against a distribution it belongs to bounds the z-score by the sample size, and a
/// bounded z cannot reach the threshold it is compared against.
pub fn entries_at(
    closes: &Closes,
    universe: &[&str],
    session: usize,
    selection: Selection,
) -> Vec<Entry> {
    let Some(fitted_through) = session.checked_sub(1) else {
        return Vec::new();
    };

    let mut entries = Vec::new();
    for (offset, first) in universe.iter().enumerate() {
        for second in &universe[offset + 1..] {
            let Some((first_window, second_window)) =
                closes.aligned_window(first, second, fitted_through, CORRELATION_WINDOW_SESSIONS)
            else {
                continue;
            };
            if breaks(&first_window) || breaks(&second_window) {
                continue;
            }
            match selection {
                Selection::Screened => {
                    let correlation = pearson_correlation(
                        &logarithmic_returns(&first_window),
                        &logarithmic_returns(&second_window),
                    );
                    match correlation {
                        Some(correlation)
                            if (CORRELATION_MINIMUM..=CORRELATION_MAXIMUM)
                                .contains(&correlation) => {}
                        Some(_) | None => continue,
                    }
                }
                Selection::Unscreened => {}
            }

            let (Some(first_price), Some(second_price)) = (
                closes.close_at(first, session),
                closes.close_at(second, session),
            ) else {
                continue;
            };

            // Both orientations, because ordinary least squares is not symmetric and the spread has
            // to be fitted the way round it would be held. At most one clears a positive threshold.
            let orientations = [
                (
                    second,
                    &second_window,
                    second_price,
                    first,
                    &first_window,
                    first_price,
                ),
                (
                    first,
                    &first_window,
                    first_price,
                    second,
                    &second_window,
                    second_price,
                ),
            ];
            for (long, long_window, long_price, short, short_window, short_price) in orientations {
                let Some(model) = SpreadModel::fit(long_window, short_window) else {
                    continue;
                };
                let Some(entry_z_score) = model.z_score(long_price, short_price) else {
                    continue;
                };
                if !(ENTRY_Z_SCORE..=ENTRY_Z_SCORE_CAP).contains(&entry_z_score) {
                    continue;
                }
                entries.push(follow(closes, &model, long, short, session, entry_z_score));
                break;
            }
        }
    }
    entries
}

/// Walks an entry forward against the model it was opened on, never refitting.
///
/// Refitting would drag the fitted mean toward the current spread as the window absorbed the
/// dislocation, manufacturing convergence out of bookkeeping. Production's rolling behaviour is a
/// separate comparison, not this measurement.
fn follow(
    closes: &Closes,
    model: &SpreadModel,
    long: &str,
    short: &str,
    session: usize,
    entry_z_score: f64,
) -> Entry {
    let mut observed = 0;
    let mut resolution = Resolution::Unresolved;

    for step in 1..=HORIZONS {
        let index = session + step;
        if index >= closes.sessions() {
            break;
        }
        // A leg that did not trade leaves this horizon unobserved and the walk continues: the
        // horizon counts sessions elapsed, which a missing bar does not change.
        let (Some(long_price), Some(short_price)) =
            (closes.close_at(long, index), closes.close_at(short, index))
        else {
            continue;
        };
        let Some(z_score) = model.z_score(long_price, short_price) else {
            continue;
        };
        observed = step;

        match exit_reason(z_score, entry_z_score) {
            Some(CloseReason::Convergence) => {
                resolution = Resolution::Converged(step);
                break;
            }
            Some(CloseReason::StopLoss) => {
                resolution = Resolution::Stopped(step);
                break;
            }
            // Neither arises from a spread reading; both are the book flattening for its own reasons.
            Some(CloseReason::EndOfDay) | Some(CloseReason::PositionMissing) | None => {}
        }
    }

    Entry {
        session,
        long: long.to_string(),
        short: short.to_string(),
        entry_z_score,
        resolution,
        observed,
    }
}

/// A fit window holding a move too large to be one distribution, which the screen also refuses.
fn breaks(window: &[f64]) -> bool {
    worst_session_move(window).is_some_and(|logarithmic_return| {
        logarithmic_return.abs() > MAXIMUM_SESSION_LOGARITHMIC_RETURN
    })
}

/// Drops every entry taken on a pair that was already open, which production cannot take.
///
/// A spread stays dislocated for days, so the same episode is admitted on each of them and ten
/// correlated outcomes read as ten independent ones. [`crate::portfolio::screen::select_disjoint`]
/// excludes every held ticker, so the book opens an episode once.
///
/// Per pair rather than per ticker, which is the weaker of the two guarantees production gives.
/// Matching the stronger one means simulating the book's ranking and its position limit, which is a
/// replay and not a measurement of prices.
pub fn without_reentry(mut entries: Vec<Entry>) -> Vec<Entry> {
    entries.sort_by(|left, right| {
        left.session
            .cmp(&right.session)
            .then_with(|| left.long.cmp(&right.long))
            .then_with(|| left.short.cmp(&right.short))
    });

    let mut held_through: BTreeMap<(String, String), usize> = BTreeMap::new();
    let mut kept = Vec::with_capacity(entries.len());
    for entry in entries {
        // Keyed unordered, because the book holds the two tickers and not an orientation: a pair
        // that flips which leg is expensive is the same two positions being re-entered.
        let pair = if entry.long <= entry.short {
            (entry.long.clone(), entry.short.clone())
        } else {
            (entry.short.clone(), entry.long.clone())
        };
        if held_through
            .get(&pair)
            .is_some_and(|until| entry.session <= *until)
        {
            continue;
        }
        // An unresolved entry occupies the pair for as long as it could be priced, not for the full
        // horizon: past that the archive says nothing, so neither does the guard.
        let until = entry.session
            + match entry.resolution {
                Resolution::Converged(step) | Resolution::Stopped(step) => step,
                Resolution::Unresolved => entry.observed,
            };
        held_through.insert(pair, until);
        kept.push(entry);
    }
    kept
}

/// Where a cohort stands at each horizon from one to [`HORIZONS`].
pub fn curves(entries: &[Entry]) -> Vec<Curve> {
    (1..=HORIZONS)
        .map(|horizon| {
            let standing: Vec<Resolution> = entries
                .iter()
                .filter_map(|entry| entry.at(horizon))
                .collect();
            let share = |count: usize| {
                if standing.is_empty() {
                    0.0
                } else {
                    count as f64 / standing.len() as f64
                }
            };
            let count = |matching: fn(&Resolution) -> bool| {
                standing.iter().filter(|state| matching(state)).count()
            };
            Curve {
                horizon,
                converged: share(count(|state| matches!(state, Resolution::Converged(_)))),
                stopped: share(count(|state| matches!(state, Resolution::Stopped(_)))),
                open: share(count(|state| matches!(state, Resolution::Unresolved))),
                entries: standing.len(),
            }
        })
        .collect()
}

/// The average z-score entries were opened at, which is what a convergence is worth.
///
/// The curves count outcomes and the two outcomes are not the same size: converging travels the
/// whole way back to the mean, and stopping travels
/// [`crate::portfolio::screen::STOP_LOSS_WIDENING`]. Without this the shares cannot be turned into
/// a statement about money in either direction.
pub fn mean_entry_z_score(entries: &[Entry]) -> Option<f64> {
    if entries.is_empty() {
        return None;
    }
    let total: f64 = entries.iter().map(|entry| entry.entry_z_score).sum();
    Some(total / entries.len() as f64)
}

/// The median sessions a converging entry took, over the entries that converged.
///
/// Deliberately not over every entry: an unresolved entry has no convergence horizon, and giving it
/// one — [`HORIZONS`], say — would report a number no entry produced.
pub fn median_sessions_to_convergence(entries: &[Entry]) -> Option<f64> {
    let mut horizons: Vec<usize> = entries
        .iter()
        .filter_map(|entry| match entry.resolution {
            Resolution::Converged(step) => Some(step),
            Resolution::Stopped(_) | Resolution::Unresolved => None,
        })
        .collect();
    if horizons.is_empty() {
        return None;
    }
    horizons.sort_unstable();

    let middle = horizons.len() / 2;
    if horizons.len().is_multiple_of(2) {
        Some((horizons[middle - 1] + horizons[middle]) as f64 / 2.0)
    } else {
        Some(horizons[middle] as f64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const DAY: i64 = 86_400_000;

    /// Two names over five sessions, with the third missing for BBB.
    fn frame() -> DataFrame {
        let rows: Vec<(&str, i64, Option<f64>)> = vec![
            ("AAA", 0, Some(10.0)),
            ("AAA", DAY, Some(11.0)),
            ("AAA", 2 * DAY, Some(12.0)),
            ("AAA", 3 * DAY, Some(13.0)),
            ("AAA", 4 * DAY, Some(14.0)),
            ("BBB", 0, Some(50.0)),
            ("BBB", DAY, Some(51.0)),
            ("BBB", 2 * DAY, None),
            ("BBB", 3 * DAY, Some(53.0)),
            ("BBB", 4 * DAY, Some(54.0)),
        ];
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
                "close_price".into(),
                rows.iter().map(|row| row.2).collect::<Vec<_>>(),
            ),
        ])
        .unwrap()
    }

    #[test]
    fn test_a_series_is_read_per_name_along_one_session_axis() {
        let closes = Closes::from_frame(&frame()).unwrap();
        assert_eq!(closes.sessions(), 5);
        assert_eq!(closes.tickers(), vec!["AAA", "BBB"]);
        assert_eq!(closes.close_at("AAA", 0), Some(10.0));
        assert_eq!(closes.close_at("BBB", 2), None, "BBB did not trade");
        assert_eq!(closes.close_at("CCC", 0), None);
        assert_eq!(closes.session_at(4), Some(4 * DAY));
    }

    /// The window skips the session one leg missed rather than pairing prices from different days,
    /// which is what a hedge ratio fitted across the hole would regress against each other.
    #[test]
    fn test_a_window_aligns_on_the_sessions_both_legs_traded() {
        let closes = Closes::from_frame(&frame()).unwrap();

        let (first, second) = closes.aligned_window("AAA", "BBB", 4, 4).unwrap();
        assert_eq!(
            first,
            vec![10.0, 11.0, 13.0, 14.0],
            "session two is skipped"
        );
        assert_eq!(second, vec![50.0, 51.0, 53.0, 54.0]);
    }

    /// A window shorter than asked for is refused rather than returned short: the spread's mean and
    /// dispersion come from a fixed number of sessions, and a shorter sample moves both.
    #[test]
    fn test_a_window_that_cannot_be_filled_is_refused() {
        let closes = Closes::from_frame(&frame()).unwrap();

        assert_eq!(closes.aligned_window("AAA", "BBB", 4, 5), None, "only four");
        assert_eq!(closes.aligned_window("AAA", "BBB", 1, 3), None);
        assert_eq!(closes.aligned_window("AAA", "CCC", 4, 2), None);
        assert_eq!(closes.aligned_window("AAA", "BBB", 9, 2), None);
        assert_eq!(closes.aligned_window("AAA", "BBB", 4, 0), None);
    }

    /// The window ends at the session asked for and reaches backward, never forward. Reaching
    /// forward would fit a spread on prices the session it is judged at had not seen.
    #[test]
    fn test_a_window_ends_where_it_is_asked_to() {
        let closes = Closes::from_frame(&frame()).unwrap();

        let (first, _) = closes.aligned_window("AAA", "BBB", 1, 2).unwrap();
        assert_eq!(first, vec![10.0, 11.0]);
        let (first, _) = closes.aligned_window("AAA", "BBB", 3, 2).unwrap();
        assert_eq!(first, vec![11.0, 13.0], "session two is still skipped");
    }

    /// A price at or below zero has no logarithm, and a spread is built from logs.
    #[test]
    fn test_a_price_that_cannot_be_logged_is_absent() {
        let mut rows = frame();
        rows.with_column(Column::new(
            "close_price".into(),
            vec![
                Some(10.0),
                Some(0.0),
                Some(12.0),
                Some(-1.0),
                Some(14.0),
                Some(50.0),
                Some(51.0),
                None,
                Some(53.0),
                Some(54.0),
            ],
        ))
        .unwrap();

        let closes = Closes::from_frame(&rows).unwrap();
        assert_eq!(closes.close_at("AAA", 1), None, "zero has no logarithm");
        assert_eq!(closes.close_at("AAA", 3), None, "nor does a negative price");
        assert_eq!(closes.close_at("AAA", 0), Some(10.0));
    }

    /// The session both fixtures dislocate at, chosen so a full fit window precedes it.
    const DISLOCATION: usize = 65;
    const SESSIONS: usize = 80;

    /// Ordinary spread dispersion, as a log-price difference.
    ///
    /// The entry threshold is in units of the fitted standard deviation, which for a sine of this
    /// amplitude is `AMPLITUDE / sqrt(2)`, so a dislocation is written as a multiple of that.
    const AMPLITUDE: f64 = 0.01;

    /// A pair whose spread path is written directly rather than inferred from two price series.
    ///
    /// `AAA` is a common factor and `BBB` is that factor plus `spread`, so the fitted spread *is*
    /// `spread` up to a constant and a dislocation can be placed at a chosen number of standard
    /// deviations. `direction` flips `BBB`'s exposure to the factor, which is how an anti-correlated
    /// pair is built without disturbing the spread.
    fn pair(direction: f64, spread: impl Fn(usize) -> f64) -> DataFrame {
        let mut rows: Vec<(&str, i64, f64)> = Vec::new();
        let mut factor = 0.0;
        for session in 0..SESSIONS {
            // Increments roughly twice the spread's, which puts the log-return correlation inside
            // the screened band rather than at either edge of it.
            factor += 0.014 * (session as f64 * 1.7).sin();
            rows.push(("AAA", session as i64 * DAY, 100.0 * factor.exp()));
            rows.push((
                "BBB",
                session as i64 * DAY,
                50.0 * (direction * factor + spread(session)).exp(),
            ));
        }
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
                "close_price".into(),
                rows.iter().map(|row| row.2).collect::<Vec<_>>(),
            ),
        ])
        .unwrap()
    }

    /// The spread oscillates, steps out for two sessions, then crosses back through its own mean.
    fn dislocated(session: usize) -> f64 {
        match session {
            _ if session < DISLOCATION => AMPLITUDE * (session as f64 * 0.9).sin(),
            _ if session < DISLOCATION + 2 => 3.0 * AMPLITUDE / std::f64::consts::SQRT_2,
            _ => -AMPLITUDE,
        }
    }

    /// The fixture has to produce an entry at all, or every test built on it passes vacuously —
    /// which is how a screened arm reporting nothing reads as a strategy that never fires.
    #[test]
    fn test_the_fixture_opens_a_pair_at_the_dislocation() {
        let closes = Closes::from_frame(&pair(1.0, dislocated)).unwrap();
        let entries = entries_at(&closes, &["AAA", "BBB"], DISLOCATION, Selection::Screened);

        assert_eq!(entries.len(), 1, "{entries:?}");
        let entry = &entries[0];
        assert_eq!(entry.short, "BBB", "the expensive leg is the short one");
        assert_eq!(entry.long, "AAA");
        assert!(entry.entry_z_score >= 2.0, "{}", entry.entry_z_score);
        assert!(entry.entry_z_score <= 5.0, "{}", entry.entry_z_score);
    }

    /// The window the spread is measured against must end before the session being judged, or it
    /// contains the observation and inflates its own dispersion by exactly the move being scored.
    ///
    /// Read at the cap rather than at the entry threshold, because that is where the difference
    /// bites. A window of sixty admitting one point `d` deviations out reports it at roughly
    /// `d / sqrt(1 + d² / 61)`, which stays above two for every `d` above two — so an entry fires
    /// either way and only the *upper* bound separates them. At five and a half deviations the
    /// honest reading is past [`ENTRY_Z_SCORE_CAP`] and refused, and the contaminated one is 4.4
    /// and admitted: the guard against unadjusted corporate actions stops working silently.
    #[test]
    fn test_the_fit_window_ends_before_the_session_it_judges() {
        let beyond_the_cap = |session: usize| match session {
            _ if session < DISLOCATION => AMPLITUDE * (session as f64 * 0.9).sin(),
            _ => 5.5 * AMPLITUDE / std::f64::consts::SQRT_2,
        };
        let closes = Closes::from_frame(&pair(1.0, beyond_the_cap)).unwrap();

        assert!(
            entries_at(&closes, &["AAA", "BBB"], DISLOCATION, Selection::Screened).is_empty(),
            "a spread this far out is refused, not traded"
        );
    }

    /// Convergence is the spread falling back through the mean it was fitted against, and the
    /// fixture returns to its own level two sessions after it left.
    #[test]
    fn test_a_returning_spread_resolves_as_converged() {
        let closes = Closes::from_frame(&pair(1.0, dislocated)).unwrap();
        let entries = entries_at(&closes, &["AAA", "BBB"], DISLOCATION, Selection::Screened);

        assert_eq!(
            entries[0].resolution,
            Resolution::Converged(2),
            "{:?}",
            entries[0]
        );
    }

    /// A spread that keeps widening stops out rather than converging, and the stop is measured from
    /// this entry's own z-score rather than from a fixed line.
    #[test]
    fn test_a_widening_spread_resolves_as_stopped() {
        // Entry is three standard deviations out and the stop sits 1.5 further, so a spread that
        // steps to six and stays there is unambiguously past it.
        let widening = |session: usize| match session {
            _ if session < DISLOCATION => AMPLITUDE * (session as f64 * 0.9).sin(),
            _ if session < DISLOCATION + 1 => 3.0 * AMPLITUDE / std::f64::consts::SQRT_2,
            _ => 6.0 * AMPLITUDE / std::f64::consts::SQRT_2,
        };

        let closes = Closes::from_frame(&pair(1.0, widening)).unwrap();
        let entries = entries_at(&closes, &["AAA", "BBB"], DISLOCATION, Selection::Screened);
        assert_eq!(entries.len(), 1, "{entries:?}");
        assert_eq!(
            entries[0].resolution,
            Resolution::Stopped(1),
            "{:?}",
            entries[0]
        );
    }

    /// The correlation band is the only test the control drops, and dropping it has to admit pairs
    /// the screen refuses — otherwise the two arms measure the same population and the control
    /// cannot say anything.
    #[test]
    fn test_the_control_admits_what_the_band_refuses() {
        // The same spread path, with BBB's exposure to the common factor flipped. Their log returns
        // now anti-correlate, which the band refuses and the control does not.
        let closes = Closes::from_frame(&pair(-1.0, dislocated)).unwrap();

        assert!(
            entries_at(&closes, &["AAA", "BBB"], DISLOCATION, Selection::Screened).is_empty(),
            "an anti-correlated pair is outside the band"
        );
        assert!(
            !entries_at(&closes, &["AAA", "BBB"], DISLOCATION, Selection::Unscreened).is_empty(),
            "and the control has to see it, or it is not a control"
        );
    }

    /// The window ends one session before entry, so nothing can be screened at session zero.
    #[test]
    fn test_nothing_is_entered_at_the_first_session() {
        let closes = Closes::from_frame(&pair(1.0, dislocated)).unwrap();
        assert!(entries_at(&closes, &["AAA", "BBB"], 0, Selection::Screened).is_empty());
    }

    fn entry(resolution: Resolution, observed: usize) -> Entry {
        Entry {
            session: 10,
            long: "AAA".to_string(),
            short: "BBB".to_string(),
            entry_z_score: 2.5,
            resolution,
            observed,
        }
    }

    /// An entry the archive ran out under leaves the denominator rather than counting as still
    /// open. Counting it would report a truncated entry as a pair that failed to converge.
    #[test]
    fn test_an_entry_not_followed_to_a_horizon_is_not_counted_there() {
        let entries = vec![
            entry(Resolution::Converged(3), 3),
            entry(Resolution::Unresolved, 5),
        ];
        let curves = curves(&entries);

        assert_eq!(curves[4].horizon, 5);
        assert_eq!(curves[4].entries, 2, "both were followed five sessions");
        assert_eq!(curves[5].horizon, 6);
        assert_eq!(
            curves[5].entries, 1,
            "only the resolved one is still countable"
        );
        assert!((curves[5].converged - 1.0).abs() < 1e-12);
    }

    /// A resolution is carried forward to every later horizon: a pair that converged at three has
    /// converged at ten, and reporting it as open there would flatten the curve it is drawn on.
    #[test]
    fn test_a_resolution_carries_forward_and_does_not_carry_back() {
        let entries = vec![entry(Resolution::Stopped(4), 4)];
        let curves = curves(&entries);

        assert_eq!(curves[2].entries, 1, "horizon three, followed that far");
        assert!((curves[2].open - 1.0).abs() < 1e-12, "not yet stopped");
        assert!((curves[3].stopped - 1.0).abs() < 1e-12, "stopped at four");
        assert!((curves[19].stopped - 1.0).abs() < 1e-12, "still stopped");
    }

    /// The three shares partition the entries followed to a horizon, so they sum to one wherever
    /// there is anything to count. A gap between them would be an outcome nothing reports.
    #[test]
    fn test_the_shares_at_a_horizon_sum_to_one() {
        let entries = vec![
            entry(Resolution::Converged(2), 2),
            entry(Resolution::Stopped(6), 6),
            entry(Resolution::Unresolved, 20),
        ];
        for curve in curves(&entries) {
            if curve.entries == 0 {
                continue;
            }
            let total = curve.converged + curve.stopped + curve.open;
            assert!((total - 1.0).abs() < 1e-12, "{curve:?}");
        }
    }

    /// The median is over the entries that converged and not over every entry: an unresolved entry
    /// has no convergence horizon, and lending it one reports a number no entry produced.
    #[test]
    fn test_the_median_counts_only_the_entries_that_converged() {
        let entries = vec![
            entry(Resolution::Converged(2), 2),
            entry(Resolution::Converged(8), 8),
            entry(Resolution::Stopped(1), 1),
            entry(Resolution::Unresolved, 20),
        ];
        assert_eq!(median_sessions_to_convergence(&entries), Some(5.0));

        let entries = vec![entry(Resolution::Converged(3), 3)];
        assert_eq!(median_sessions_to_convergence(&entries), Some(3.0));
        assert_eq!(
            median_sessions_to_convergence(&[entry(Resolution::Unresolved, 20)]),
            None,
            "no convergence is absent, not zero"
        );
    }

    fn opened(long: &str, short: &str, session: usize, resolution: Resolution) -> Entry {
        Entry {
            session,
            long: long.to_string(),
            short: short.to_string(),
            entry_z_score: 2.5,
            resolution,
            observed: match resolution {
                Resolution::Converged(step) | Resolution::Stopped(step) => step,
                Resolution::Unresolved => HORIZONS,
            },
        }
    }

    /// A spread stays dislocated for days, so the same episode is admitted on each of them. Keeping
    /// them all reports one outcome several times over as several independent ones.
    #[test]
    fn test_a_pair_is_not_reentered_while_it_is_still_open() {
        let kept = without_reentry(vec![
            opened("AAA", "BBB", 10, Resolution::Converged(4)),
            opened("AAA", "BBB", 11, Resolution::Converged(3)),
            opened("AAA", "BBB", 13, Resolution::Converged(1)),
            opened("AAA", "BBB", 15, Resolution::Stopped(2)),
        ]);

        assert_eq!(kept.len(), 2, "{kept:?}");
        assert_eq!(kept[0].session, 10, "the first entry of the episode");
        assert_eq!(kept[1].session, 15, "and the next one after it closed");
    }

    /// The guard is on the two tickers and not on the orientation, because the book holds positions.
    /// A pair that flips which leg is expensive is the same two names being re-entered.
    #[test]
    fn test_a_flipped_orientation_is_the_same_pair() {
        let kept = without_reentry(vec![
            opened("AAA", "BBB", 10, Resolution::Converged(5)),
            opened("BBB", "AAA", 12, Resolution::Converged(1)),
        ]);
        assert_eq!(kept.len(), 1, "{kept:?}");
    }

    /// One pair being open says nothing about another, or the guard would silently thin the
    /// universe down to whichever pair happened to enter first.
    #[test]
    fn test_the_guard_is_per_pair_and_not_across_the_book() {
        let kept = without_reentry(vec![
            opened("AAA", "BBB", 10, Resolution::Unresolved),
            opened("CCC", "DDD", 11, Resolution::Unresolved),
        ]);
        assert_eq!(kept.len(), 2, "{kept:?}");
    }

    /// The sample is reproducible from its seed, drawn without replacement, and a different seed
    /// draws a different set — a sampler that ignored its seed would silently measure one universe.
    #[test]
    fn test_the_universe_sample_is_seeded_and_without_replacement() {
        let mut rows: Vec<(String, i64, f64)> = Vec::new();
        for name in 0..50 {
            rows.push((format!("T{name:03}"), 0, 10.0));
        }
        let frame = DataFrame::new(vec![
            Column::new(
                "ticker".into(),
                rows.iter().map(|row| row.0.as_str()).collect::<Vec<_>>(),
            ),
            Column::new(
                "timestamp".into(),
                rows.iter().map(|row| row.1).collect::<Vec<_>>(),
            ),
            Column::new(
                "close_price".into(),
                rows.iter().map(|row| row.2).collect::<Vec<_>>(),
            ),
        ])
        .unwrap();
        let closes = Closes::from_frame(&frame).unwrap();

        let sample = sample_universe(&closes, 10, 19_985);
        assert_eq!(sample.len(), 10);
        assert_eq!(sample, sample_universe(&closes, 10, 19_985), "reproducible");
        assert_ne!(sample, sample_universe(&closes, 10, 7), "and seeded");

        let mut distinct = sample.clone();
        distinct.sort_unstable();
        distinct.dedup();
        assert_eq!(distinct.len(), 10, "no name is drawn twice");

        assert_eq!(
            sample_universe(&closes, 500, 19_985).len(),
            50,
            "a sample larger than the universe is the universe"
        );
    }
}
