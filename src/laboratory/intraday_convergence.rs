//! Does a dislocated pair converge inside the session, which is the horizon the book can hold?
//!
//! The model is fitted on daily closes, as production fits it; only the observation is intraday.

use std::collections::BTreeMap;

use crate::common::types::SessionDate;
use crate::laboratory::convergence::{breaks, Closes, Observed, Resolution, Selection, HORIZONS};
use crate::portfolio::screen::{
    logarithmic_returns, pearson_correlation, SpreadModel, CONVERGENCE_Z_SCORE,
    CORRELATION_MAXIMUM, CORRELATION_MINIMUM, CORRELATION_WINDOW_SESSIONS, ENTRY_Z_SCORE,
    ENTRY_Z_SCORE_CAP, STOP_LOSS_WIDENING,
};

/// Bars between the reading an entry fires on and the first bar it is judged at.
///
/// One, not zero: a dislocation that "converges" at the very next bar is the spread oscillating
/// rather than the pair closing.
pub const RESOLUTION_SKIP: usize = 1;

/// One pair opened at one bar of one session, and what became of it before the close.
#[derive(Debug, Clone, PartialEq)]
pub struct IntradayEntry {
    pub session: SessionDate,
    /// Bars from the session open at which the dislocation was read.
    pub entry_bar: usize,
    pub long: String,
    pub short: String,
    pub entry_z_score: f64,
    /// The spread's z-score at the last bar of the session it could be priced at.
    ///
    /// Read to the session's end rather than to the horizon cap, because that is where the book
    /// flattens; stopping at the cap would measure a hundred minutes and call it a session.
    pub final_z_score: Option<f64>,
    pub resolution: Resolution,
    pub observed: Observed,
}

impl IntradayEntry {
    /// The pair this entry contributes to a curve, without its identity.
    pub fn state(&self) -> (Resolution, Observed) {
        (self.resolution, self.observed)
    }
}

/// Every name's volume-weighted price at every bar of one session.
pub type SessionPrices = BTreeMap<String, Vec<Option<f64>>>;

/// Reads one session's intraday prices against models fitted on the daily closes before it.
///
/// `daily_session` indexes the session inside `daily`; the fit window ends at the session *before*
/// it, so the dislocation being judged never enters the distribution it is judged against.
pub fn entries_in_session(
    daily: &Closes,
    prices: &SessionPrices,
    universe: &[&str],
    session: SessionDate,
    daily_session: usize,
    selection: Selection,
) -> Vec<IntradayEntry> {
    let Some(fitted_through) = daily_session.checked_sub(1) else {
        return Vec::new();
    };

    let mut entries = Vec::new();
    for (offset, first) in universe.iter().enumerate() {
        for second in &universe[offset + 1..] {
            let Some((first_window, second_window)) =
                daily.aligned_window(first, second, fitted_through, CORRELATION_WINDOW_SESSIONS)
            else {
                continue;
            };
            // The same guard the daily measurement applies. A corporate-action-sized move inside
            // the window corrupts the fit it is the whole basis of, in both cohorts, so it is
            // checked before the correlation screen rather than as part of it.
            if breaks(&first_window) || breaks(&second_window) {
                continue;
            }
            if let Selection::Screened = selection {
                let correlation = pearson_correlation(
                    &logarithmic_returns(&first_window),
                    &logarithmic_returns(&second_window),
                );
                let Some(correlation) = correlation else {
                    continue;
                };
                if !(CORRELATION_MINIMUM..=CORRELATION_MAXIMUM).contains(&correlation) {
                    continue;
                }
            }
            let (Some(first_prices), Some(second_prices)) =
                (prices.get(*first), prices.get(*second))
            else {
                continue;
            };

            // Both orientations, because the pair opens with whichever leg is the expensive one
            // short. Only the first that clears the entry band is taken, so one pair yields at most
            // one entry per session and cannot be counted twice in the same cohort.
            let orientations = [
                (
                    *first,
                    &first_window,
                    first_prices,
                    *second,
                    &second_window,
                    second_prices,
                ),
                (
                    *second,
                    &second_window,
                    second_prices,
                    *first,
                    &first_window,
                    first_prices,
                ),
            ];
            for (long, long_window, long_prices, short, short_window, short_prices) in orientations
            {
                let Some(model) = SpreadModel::fit(long_window, short_window) else {
                    continue;
                };
                let Some(entry) = follow(&model, session, long, long_prices, short, short_prices)
                else {
                    continue;
                };
                entries.push(entry);
                break;
            }
        }
    }
    entries
}

/// Walks the session's bars, opening at the first dislocation and following it to the close.
///
/// The model is never refitted. Refitting intraday would drag the fitted mean toward the current
/// spread as the window absorbed the dislocation, manufacturing convergence out of bookkeeping.
fn follow(
    model: &SpreadModel,
    session: SessionDate,
    long: &str,
    long_prices: &[Option<f64>],
    short: &str,
    short_prices: &[Option<f64>],
) -> Option<IntradayEntry> {
    let bars = long_prices.len().min(short_prices.len());
    let z_at = |bar: usize| -> Option<f64> {
        let (Some(long_price), Some(short_price)) = (long_prices[bar], short_prices[bar]) else {
            return None;
        };
        model.z_score(long_price, short_price)
    };

    let mut entry_bar = None;
    let mut entry_z_score = 0.0;
    for bar in 0..bars {
        let Some(z_score) = z_at(bar) else { continue };
        if (ENTRY_Z_SCORE..=ENTRY_Z_SCORE_CAP).contains(&z_score) {
            entry_bar = Some(bar);
            entry_z_score = z_score;
            break;
        }
    }
    let entry_bar = entry_bar?;
    let stop_at = entry_z_score + STOP_LOSS_WIDENING;

    // Walked to the end of the session, independently of the resolution walk below: the book holds
    // to the close whatever the rules did in between, so the drift statistic must see that bar.
    let final_z_score = (entry_bar + RESOLUTION_SKIP + 1..bars).rev().find_map(z_at);

    let mut observed = Observed::default();
    let mut resolution = Resolution::Unresolved;
    for horizon in 1..=HORIZONS {
        // `+ horizon` after the skip, so horizon one is the first bar the skip has not hidden.
        // Writing `+ horizon - 1` here judged the adjacent bar and defeated the constant entirely.
        let bar = entry_bar + RESOLUTION_SKIP + horizon;
        if bar >= bars {
            break;
        }
        let Some(z_score) = z_at(bar) else { continue };
        observed.mark(horizon);
        if z_score <= CONVERGENCE_Z_SCORE {
            resolution = Resolution::Converged(horizon);
            break;
        }
        if z_score >= stop_at {
            resolution = Resolution::Stopped(horizon);
            break;
        }
    }

    Some(IntradayEntry {
        session,
        entry_bar,
        long: long.to_string(),
        short: short.to_string(),
        entry_z_score,
        final_z_score,
        resolution,
        observed,
    })
}

/// The average z-score entries were opened at, which is what a convergence would be worth.
pub fn mean_entry_z_score(entries: &[IntradayEntry]) -> Option<f64> {
    (!entries.is_empty()).then(|| {
        entries.iter().map(|entry| entry.entry_z_score).sum::<f64>() / entries.len() as f64
    })
}

/// How far the spread travelled, in fitted standard deviations, between entry and the session's end.
///
/// Negative is movement toward the fitted mean. Reported instead of a convergence rate because a
/// threshold set in daily sigma is not reachable inside one session.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Drift {
    /// Mean over sessions of each session's own mean drift.
    pub mean: f64,
    /// Standard error over sessions, not over entries.
    pub standard_error: f64,
    /// Share of entries whose spread moved toward the fitted mean. A coin lands at one half.
    pub share_converging: f64,
    pub sessions: usize,
    pub entries: usize,
}

/// Measures drift with one reading per session.
///
/// Aggregated by session because a session's entries are drawn from one universe over one set of
/// hours and move together; per-entry errors would divide by far more than the information present.
pub fn drift(entries: &[IntradayEntry]) -> Option<Drift> {
    let mut by_session: BTreeMap<SessionDate, Vec<f64>> = BTreeMap::new();
    let mut toward = 0_usize;
    let mut counted = 0_usize;
    for entry in entries {
        let Some(exit) = entry.final_z_score else {
            continue;
        };
        let travelled = exit - entry.entry_z_score;
        by_session.entry(entry.session).or_default().push(travelled);
        if travelled < 0.0 {
            toward += 1;
        }
        counted += 1;
    }
    if counted == 0 {
        return None;
    }

    let session_means: Vec<f64> = by_session
        .values()
        .map(|values| values.iter().sum::<f64>() / values.len() as f64)
        .collect();
    if session_means.len() < 2 {
        return None;
    }
    let sessions = session_means.len() as f64;
    let mean = session_means.iter().sum::<f64>() / sessions;
    let variance = session_means
        .iter()
        .map(|value| (value - mean).powi(2))
        .sum::<f64>()
        / (sessions - 1.0);

    Some(Drift {
        mean,
        standard_error: (variance / sessions).sqrt(),
        share_converging: toward as f64 / counted as f64,
        sessions: session_means.len(),
        entries: counted,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use polars::prelude::*;

    fn session_of(year: i32, month: u32, day: u32) -> SessionDate {
        SessionDate::from_date(chrono::NaiveDate::from_ymd_opt(year, month, day).unwrap())
    }

    /// A pair whose spread sits at `dislocation` standard deviations at `at_bar`, and returns to
    /// zero afterwards. Written as a spread path so the dislocation is exact rather than hoped for.
    fn pair_prices(
        dislocation: f64,
        at_bar: usize,
        converge_after: Option<usize>,
    ) -> SessionPrices {
        let sigma = 0.01;
        let mut long = Vec::new();
        let mut short = Vec::new();
        for bar in 0..40 {
            // Convergence overshoots slightly rather than returning to exactly zero: the fitted
            // mean of the daily window is near zero but not identically so, and a fixture that
            // stops on the boundary would test the tie rather than the crossing.
            let spread = if bar < at_bar {
                0.0
            } else {
                match converge_after {
                    Some(after) if bar >= at_bar + after => -sigma,
                    _ => dislocation * sigma,
                }
            };
            long.push(Some(100.0_f64));
            short.push(Some(100.0 * spread.exp()));
        }
        let mut prices = SessionPrices::new();
        prices.insert("LONG".to_string(), long);
        prices.insert("SHORT".to_string(), short);
        prices
    }

    /// Sixty daily sessions of two names whose log spread has standard deviation `sigma`.
    fn daily_frame(sigma: f64) -> DataFrame {
        let mut tickers = Vec::new();
        let mut timestamps = Vec::new();
        let mut closes = Vec::new();
        for index in 0..CORRELATION_WINDOW_SESSIONS + 2 {
            let wobble = if index % 2 == 0 { sigma } else { -sigma };
            let drift = index as f64 * 0.001;
            for (ticker, price) in [
                ("LONG", 100.0 * drift.exp()),
                ("SHORT", 100.0 * (drift + wobble).exp()),
            ] {
                tickers.push(ticker);
                timestamps.push(index as i64 * 86_400_000);
                closes.push(price);
            }
        }
        DataFrame::new(vec![
            Column::new("ticker".into(), tickers),
            Column::new("timestamp".into(), timestamps),
            Column::new("close_price".into(), closes),
        ])
        .unwrap()
    }

    fn universe() -> Vec<&'static str> {
        vec!["LONG", "SHORT"]
    }

    /// A spread that is dislocated throughout except for a single bar immediately after entry.
    /// That one bar is the shape bid-ask bounce makes: a price that lands on the other side of the
    /// spread once and comes straight back, with nothing having actually converged.
    fn pair_with_a_single_bar_dip(dislocation: f64, at_bar: usize) -> SessionPrices {
        let sigma = 0.01;
        let mut long = Vec::new();
        let mut short = Vec::new();
        for bar in 0..40 {
            let spread = match bar {
                bar if bar < at_bar => 0.0,
                bar if bar == at_bar + 1 => -sigma,
                _ => dislocation * sigma,
            };
            long.push(Some(100.0_f64));
            short.push(Some(100.0 * spread.exp()));
        }
        let mut prices = SessionPrices::new();
        prices.insert("LONG".to_string(), long);
        prices.insert("SHORT".to_string(), short);
        prices
    }

    /// The bar immediately after entry must be invisible to the resolution walk. Judged there, the
    /// single-bar dip above reads as an instant convergence — which is the spread oscillating, the
    /// same artefact that made five-minute persistence read eighteen standard errors from zero and
    /// vanish when one bar was skipped.
    #[test]
    fn test_the_bar_immediately_after_entry_is_not_judged() {
        assert_eq!(RESOLUTION_SKIP, 1);
        let closes = Closes::from_frame(&daily_frame(0.01)).unwrap();
        let prices = pair_with_a_single_bar_dip(3.0, 10);

        let entries = entries_in_session(
            &closes,
            &prices,
            &universe(),
            session_of(2026, 6, 1),
            closes.sessions() - 1,
            Selection::Unscreened,
        );

        assert!(!entries.is_empty(), "a three sigma dislocation must open");
        for entry in &entries {
            assert!(
                !matches!(entry.resolution, Resolution::Converged(_)),
                "the dip sits on the skipped bar and must not resolve the entry, got {:?}",
                entry.resolution
            );
        }
    }

    /// A dislocation that persists and then closes is a real convergence and must be counted.
    #[test]
    fn test_a_lasting_dislocation_that_closes_is_counted() {
        let closes = Closes::from_frame(&daily_frame(0.01)).unwrap();
        let session = session_of(2026, 6, 1);
        let prices = pair_prices(3.0, 10, Some(8));

        let entries = entries_in_session(
            &closes,
            &prices,
            &universe(),
            session,
            closes.sessions() - 1,
            Selection::Unscreened,
        );

        assert!(!entries.is_empty(), "a three sigma dislocation must open");
        assert!(
            entries
                .iter()
                .any(|entry| matches!(entry.resolution, Resolution::Converged(_))),
            "a spread that returns to its mean inside the session converges"
        );
    }

    /// A dislocation that never closes must run to the session's end unresolved, not be counted as
    /// either outcome — the book flattens at the close whatever the spread is doing.
    #[test]
    fn test_a_dislocation_that_never_closes_is_unresolved() {
        let closes = Closes::from_frame(&daily_frame(0.01)).unwrap();
        let session = session_of(2026, 6, 1);
        let prices = pair_prices(3.0, 10, None);

        let entries = entries_in_session(
            &closes,
            &prices,
            &universe(),
            session,
            closes.sessions() - 1,
            Selection::Unscreened,
        );

        assert!(!entries.is_empty());
        for entry in &entries {
            assert_eq!(entry.resolution, Resolution::Unresolved);
        }
    }

    /// The fit window must end before the session being judged. A window containing the dislocation
    /// inflates its own dispersion by the move being scored and quietly refuses the entry.
    #[test]
    fn test_the_fit_window_ends_before_the_session_it_judges() {
        let closes = Closes::from_frame(&daily_frame(0.01)).unwrap();
        let session = session_of(2026, 6, 1);
        let prices = pair_prices(3.0, 10, Some(8));

        let honest = entries_in_session(
            &closes,
            &prices,
            &universe(),
            session,
            closes.sessions() - 1,
            Selection::Unscreened,
        );

        assert!(
            !honest.is_empty(),
            "the fit must not contain the move it scores"
        );
        // Session zero has nothing before it, so there is no window and no entry.
        let none = entries_in_session(
            &closes,
            &prices,
            &universe(),
            session,
            0,
            Selection::Unscreened,
        );
        assert!(none.is_empty());
    }

    /// A pair opens with whichever leg is expensive short, so entry z is positive by construction
    /// and convergence is unambiguously a fall toward zero.
    #[test]
    fn test_entry_z_scores_are_positive_by_orientation() {
        let closes = Closes::from_frame(&daily_frame(0.01)).unwrap();
        let prices = pair_prices(3.0, 10, Some(8));

        let entries = entries_in_session(
            &closes,
            &prices,
            &universe(),
            session_of(2026, 6, 1),
            closes.sessions() - 1,
            Selection::Unscreened,
        );

        assert!(!entries.is_empty());
        for entry in &entries {
            // Literals, not the constants the entry logic reads: an expectation derived from the
            // value under test moves with it and can never fail.
            assert!(entry.entry_z_score >= 2.0);
            assert!(entry.entry_z_score <= 5.0);
        }
    }

    /// One pair yields at most one entry per session, or a cohort would double-count the pair that
    /// happened to clear the band in both orientations.
    #[test]
    fn test_a_pair_opens_at_most_once_in_a_session() {
        let closes = Closes::from_frame(&daily_frame(0.01)).unwrap();
        let prices = pair_prices(3.0, 10, Some(8));

        let entries = entries_in_session(
            &closes,
            &prices,
            &universe(),
            session_of(2026, 6, 1),
            closes.sessions() - 1,
            Selection::Unscreened,
        );

        // Exactly one, not at most one: `<= 1` also passes a fixture that stopped opening at all.
        assert_eq!(entries.len(), 1, "one pair yields one entry per session");
    }

    fn entry_with(session: SessionDate, entry: f64, final_z: Option<f64>) -> IntradayEntry {
        IntradayEntry {
            session,
            entry_bar: 0,
            long: "LONG".to_string(),
            short: "SHORT".to_string(),
            entry_z_score: entry,
            final_z_score: final_z,
            resolution: Resolution::Unresolved,
            observed: Observed::default(),
        }
    }

    /// An entry the session ran out under carries no travel, and counting it as zero drift would
    /// pull the mean toward a null nothing measured.
    #[test]
    fn test_drift_ignores_entries_that_were_never_priced_again() {
        let session = session_of(2026, 6, 1);
        assert_eq!(drift(&[entry_with(session, 2.5, None)]), None);
        assert_eq!(drift(&[]), None);
    }

    /// One session gives no spread between sessions to take a standard error from, and reporting
    /// zero there would make a single day read as infinitely significant.
    #[test]
    fn test_drift_needs_more_than_one_session() {
        let session = session_of(2026, 6, 1);
        let entries = vec![
            entry_with(session, 2.5, Some(2.0)),
            entry_with(session, 2.5, Some(2.2)),
        ];
        assert_eq!(drift(&entries), None);
    }

    /// The mean is over sessions, not over entries: a session with a thousand entries must not
    /// outvote one with ten, which is what treating entries as independent would do.
    #[test]
    fn test_drift_weights_sessions_not_entries() {
        let busy = session_of(2026, 6, 1);
        let quiet = session_of(2026, 6, 2);
        let mut entries = vec![entry_with(quiet, 2.5, Some(3.5))];
        // Ten entries in one session, all travelling -1.0.
        entries.extend((0..10).map(|_| entry_with(busy, 2.5, Some(1.5))));

        let reading = drift(&entries).expect("two sessions");

        // Session means are -1.0 and +1.0, so the mean over sessions is zero. Weighted by entries
        // it would have been about -0.82.
        assert!(
            reading.mean.abs() < 1e-12,
            "expected the two sessions to cancel, got {}",
            reading.mean
        );
        assert_eq!(reading.sessions, 2);
        assert_eq!(reading.entries, 11);
    }

    /// The share counts entries whose spread moved toward the mean, which is the population check
    /// on a mean one large move could otherwise carry.
    #[test]
    fn test_share_converging_counts_entries_moving_toward_the_mean() {
        let first = session_of(2026, 6, 1);
        let second = session_of(2026, 6, 2);
        let entries = vec![
            entry_with(first, 2.5, Some(2.0)),
            entry_with(first, 2.5, Some(3.0)),
            entry_with(second, 2.5, Some(2.0)),
            entry_with(second, 2.5, Some(2.5)),
        ];

        let reading = drift(&entries).expect("two sessions");

        // Two of four fell, one rose, one was unchanged — unchanged is not toward the mean.
        assert!((reading.share_converging - 0.5).abs() < 1e-12);
    }

    /// A window carrying a corporate-action-sized move corrupts the fit it is the whole basis of,
    /// so the pair must be dropped before anything is fitted on it.
    #[test]
    fn test_a_window_with_a_structural_break_is_refused() {
        let mut frame_rows = daily_frame(0.01);
        // A ten-for-one split leaves a -230% log return in the window.
        let closes = frame_rows
            .column("close_price")
            .unwrap()
            .f64()
            .unwrap()
            .into_no_null_iter()
            .enumerate()
            .map(|(index, price)| if index > 60 { price / 10.0 } else { price })
            .collect::<Vec<f64>>();
        frame_rows
            .with_column(Column::new("close_price".into(), closes))
            .unwrap();
        let closes = Closes::from_frame(&frame_rows).unwrap();
        let prices = pair_prices(3.0, 10, Some(8));

        let entries = entries_in_session(
            &closes,
            &prices,
            &universe(),
            session_of(2026, 6, 1),
            closes.sessions() - 1,
            Selection::Unscreened,
        );

        assert!(
            entries.is_empty(),
            "a window containing a split must not be fitted on"
        );
    }

    /// A name absent from the session's prices cannot be entered, and must not panic the walk.
    #[test]
    fn test_a_name_without_intraday_prices_is_skipped() {
        let closes = Closes::from_frame(&daily_frame(0.01)).unwrap();
        let mut prices = pair_prices(3.0, 10, Some(8));
        prices.remove("SHORT");

        let entries = entries_in_session(
            &closes,
            &prices,
            &universe(),
            session_of(2026, 6, 1),
            closes.sessions() - 1,
            Selection::Unscreened,
        );

        assert!(entries.is_empty());
    }
}
