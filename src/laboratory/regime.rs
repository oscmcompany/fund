//! What a session looked like in the market, described without reference to any forecast.
//!
//! A forecast's reading varies enormously between sessions; these are the candidates for why.

use crate::laboratory::metrics::MINIMUM_CROSS_SECTION;
use crate::laboratory::predictor::Panel;

/// One session's market state, read off its own cross-section.
///
/// Every field is optional because a session too thin to describe has no state rather than a zero,
/// and a state series with zeros in it would correlate against those zeros as if they were readings.
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct MarketState {
    /// Spread of the session's returns across names, which is the room a ranking has to work in.
    pub dispersion: Option<f64>,
    /// The session's equal-weighted return, signed. Whether reversal pays on down days and momentum
    /// on up days is the oldest form of the question this module exists to ask.
    pub market_move: Option<f64>,
    /// Its size without its direction, for a relationship that holds on any large day.
    pub absolute_market_move: Option<f64>,
    /// Share of names that rose, which separates a broad move from one carried by a few.
    pub breadth: Option<f64>,
}

/// The names that actually traded, which is what every figure below is taken over.
fn traded(returns: &[Option<f64>]) -> Vec<f64> {
    returns
        .iter()
        .flatten()
        .copied()
        .filter(|value| value.is_finite())
        .collect()
}

/// Describes one session from the returns of the names that traded in it.
pub fn market_state(returns: &[Option<f64>]) -> MarketState {
    let traded = traded(returns);
    if traded.len() < MINIMUM_CROSS_SECTION {
        return MarketState::default();
    }
    let names = traded.len() as f64;
    let mean = traded.iter().sum::<f64>() / names;
    let variance = traded
        .iter()
        .map(|value| (value - mean).powi(2))
        .sum::<f64>()
        / (names - 1.0);

    MarketState {
        dispersion: Some(variance.sqrt()),
        market_move: Some(mean),
        absolute_market_move: Some(mean.abs()),
        breadth: Some(traded.iter().filter(|value| **value > 0.0).count() as f64 / names),
    }
}

/// Every session's state, in the panel's own order.
pub fn describe(panel: &Panel) -> Vec<MarketState> {
    (0..panel.sessions())
        .map(|index| market_state(panel.returns_at(index)))
        .collect()
}

/// The state variables this module reports, each with the field it reads.
///
/// Named here rather than at the call site so the count is fixed in one place: reading the largest
/// of several figures without saying how many were looked at is how a table manufactures a finding.
pub const STATES: &[(&str, fn(&MarketState) -> Option<f64>)] = &[
    ("dispersion", |state| state.dispersion),
    ("market_move", |state| state.market_move),
    ("absolute_market_move", |state| state.absolute_market_move),
    ("breadth", |state| state.breadth),
];

/// The label each stretch is reported under.
///
/// Shared with whatever renders them, which matches on the label: one changed here and not there
/// would drop every row or report every half as unmeasurable, in silence.
pub const WHOLE: &str = "whole";
pub const FIRST_HALF: &str = "first_half";
pub const SECOND_HALF: &str = "second_half";

/// The stretches of a window a measurement is repeated over, so a finding can be asked to appear
/// twice rather than once.
///
/// Split by time rather than at random: halves drawn from the same sessions would share whatever
/// made the whole look significant. Every series must be cut with the same range or a lag would
/// pair a reading against the state of a session outside its own stretch.
pub fn segments(sessions: usize) -> Vec<(&'static str, std::ops::Range<usize>)> {
    let mut segments = vec![(WHOLE, 0..sessions)];
    // A window of one halves into an empty stretch and a copy of itself, and reporting that copy as
    // the second half would show one measurement twice where the whole point is to show two.
    if sessions < 2 {
        return segments;
    }
    let middle = sessions / 2;
    segments.push((FIRST_HALF, 0..middle));
    segments.push((SECOND_HALF, middle..sessions));
    segments
}

#[cfg(test)]
mod tests {
    use super::*;

    fn session(values: &[f64]) -> Vec<Option<f64>> {
        values.iter().copied().map(Some).collect()
    }

    /// A session where every name moved the same way has a market move and no spread; one where they
    /// split has a spread and no market move. Conflating the two would make a violent flat day and a
    /// calm trending day look alike.
    #[test]
    fn test_a_move_and_a_spread_are_different_things() {
        let together = market_state(&session(&[0.02; 20]));
        assert!((together.market_move.unwrap() - 0.02).abs() < 1e-12);
        assert!(together.dispersion.unwrap().abs() < 1e-12);
        assert_eq!(together.breadth, Some(1.0));

        let split: Vec<f64> = (0..20)
            .map(|index| if index % 2 == 0 { 0.05 } else { -0.05 })
            .collect();
        let apart = market_state(&session(&split));
        assert!(apart.market_move.unwrap().abs() < 1e-12);
        assert!(apart.dispersion.unwrap() > 0.04);
        assert_eq!(apart.breadth, Some(0.5));
    }

    /// The absolute move keeps a down day and an up day of the same size together, which the signed
    /// move deliberately does not.
    #[test]
    fn test_the_absolute_move_drops_the_direction_the_signed_one_keeps() {
        let down = market_state(&session(&[-0.03; 20]));
        let up = market_state(&session(&[0.03; 20]));

        assert_eq!(down.absolute_market_move, up.absolute_market_move);
        assert_ne!(down.market_move, up.market_move);
        assert_eq!(down.breadth, Some(0.0));
    }

    /// Names that did not trade are absent, not zero. Counting them as unchanged would drag every
    /// figure toward nothing in exactly the sessions where the fewest names traded.
    #[test]
    fn test_a_name_that_did_not_trade_is_left_out() {
        let mut returns = session(&[0.04; 20]);
        returns.extend(std::iter::repeat_n(None, 20));

        let state = market_state(&returns);
        assert!((state.market_move.unwrap() - 0.04).abs() < 1e-12);
        assert_eq!(state.breadth, Some(1.0), "not 0.5");
    }

    /// A cross-section too thin to describe has no state. Reporting a spread over three names as if
    /// it were a market would put noise into the series everything else is correlated against.
    #[test]
    fn test_a_session_too_thin_to_describe_has_no_state() {
        let state = market_state(&session(&[0.01, -0.02, 0.03]));
        assert_eq!(state, MarketState::default());
        assert_eq!(state.dispersion, None);
        assert_eq!(state.breadth, None);
    }

    /// The halves must not overlap and must not lose a session between them, or a finding could
    /// appear in both because they share the sessions that produced it.
    #[test]
    fn test_the_halves_are_disjoint_and_cover_the_window() {
        let segments = segments(499);
        let names: Vec<&str> = segments.iter().map(|(name, _)| *name).collect();
        assert_eq!(names, vec![WHOLE, FIRST_HALF, SECOND_HALF]);

        let (_, whole) = &segments[0];
        let (_, first) = &segments[1];
        let (_, second) = &segments[2];
        assert_eq!(*whole, 0..499);
        assert_eq!(first.end, second.start, "no session falls between them");
        assert_eq!(first.start, whole.start);
        assert_eq!(second.end, whole.end);
        assert_eq!(first.len() + second.len(), whole.len());
        // An odd window cannot split evenly, and the halves may differ by one and no more.
        assert!(first.len().abs_diff(second.len()) <= 1);
    }

    /// A window too short to halve reports only itself. Halving one session gives an empty stretch
    /// and a copy of the whole, and labelling that copy the second half would show one measurement
    /// twice where the point of the split is to show two.
    #[test]
    fn test_a_window_too_short_to_halve_reports_no_halves() {
        for sessions in [0, 1] {
            let segments = segments(sessions);
            assert_eq!(segments.len(), 1, "{sessions} sessions");
            assert_eq!(segments[0].0, WHOLE);
            assert_eq!(segments[0].1, 0..sessions);
        }

        // Two sessions is the shortest window with two stretches, one session apiece.
        let segments = segments(2);
        assert_eq!(segments.len(), 3);
        assert_eq!(segments[1].1, 0..1);
        assert_eq!(segments[2].1, 1..2);
    }

    #[test]
    fn test_every_state_is_reported_under_its_own_name() {
        let names: Vec<&str> = STATES.iter().map(|(name, _)| *name).collect();
        assert_eq!(
            names,
            vec![
                "dispersion",
                "market_move",
                "absolute_market_move",
                "breadth"
            ]
        );

        let state = market_state(&session(&[0.01; 20]));
        for (name, read) in STATES {
            assert!(read(&state).is_some(), "{name} is not read");
        }
    }
}
