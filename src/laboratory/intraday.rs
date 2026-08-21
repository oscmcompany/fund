//! What the five-minute grid is worth before any model touches it.
//!
//! Everything here measures inside one session, so the overnight gap cannot enter as an intraday
//! return — the horizon mismatch that made every daily result unusable.

use std::collections::BTreeMap;

use polars::prelude::*;

use chrono::Timelike;

use crate::common::types::SessionDate;
use crate::data::calendar::eastern_datetime;

/// Eastern wall-clock minute the regular session opens.
const REGULAR_OPEN_MINUTE: u32 = 9 * 60 + 30;

/// Eastern wall-clock minute the regular session closes.
///
/// Exclusive: a bar stamped 16:00 opened at the close and belongs to no tradeable interval.
const REGULAR_CLOSE_MINUTE: u32 = 16 * 60;

/// Fewest returns a name needs within a session before its autocorrelation is reported.
///
/// A handful of bars gives an autocorrelation that is almost all estimation noise, and the intraday
/// universe is full of names that trade twice an hour.
const MINIMUM_RETURNS: usize = 20;

/// Errors reading intraday bars into per-session returns.
#[derive(Debug, thiserror::Error)]
pub enum IntradayError {
    #[error("dataframe operation failed: {0}")]
    Frame(#[from] PolarsError),
    /// A frame that cannot produce returns, named rather than returned empty.
    #[error("{0}")]
    Shape(String),
}

/// One session's consecutive intraday log returns, by name.
///
/// Each name's first bar has no predecessor and is dropped, so a name with `n` regular-hours bars
/// contributes `n - 1` returns and a name with one bar contributes none.
#[derive(Debug, Clone)]
pub struct SessionReturns {
    session: SessionDate,
    by_ticker: BTreeMap<String, Vec<f64>>,
}

impl SessionReturns {
    pub fn session(&self) -> SessionDate {
        self.session
    }

    pub fn names(&self) -> usize {
        self.by_ticker.len()
    }

    /// Every return in the session, across every name.
    pub fn observations(&self) -> usize {
        self.by_ticker.values().map(Vec::len).sum()
    }

    pub fn returns_of(&self, ticker: &str) -> Option<&[f64]> {
        self.by_ticker.get(ticker).map(Vec::as_slice)
    }

    pub fn iter(&self) -> impl Iterator<Item = (&str, &[f64])> {
        self.by_ticker
            .iter()
            .map(|(ticker, returns)| (ticker.as_str(), returns.as_slice()))
    }
}

/// Splits a frame of intraday bars into per-session log returns over regular hours.
///
/// Bars outside 09:30–16:00 Eastern are dropped before differencing, so the first return of a
/// session is 09:35 against 09:30 rather than against the pre-market.
pub fn session_returns(bars: &DataFrame) -> Result<Vec<SessionReturns>, IntradayError> {
    let sorted = bars.sort(
        ["ticker", "timestamp"],
        SortMultipleOptions::default().with_maintain_order(true),
    )?;

    let tickers = sorted.column("ticker")?.str()?;
    let timestamps = sorted.column("timestamp")?.i64()?;
    let closes = sorted.column("close_price")?.cast(&DataType::Float64)?;
    let closes = closes.f64()?;
    if tickers.null_count() > 0 || timestamps.null_count() > 0 {
        return Err(IntradayError::Shape(
            "every bar must name its ticker and its instant".to_string(),
        ));
    }

    // Keyed by session as well as ticker: one frame spans many sessions, and a return must never
    // difference the last bar of one against the first bar of the next.
    let mut closes_by_key: BTreeMap<(SessionDate, String), Vec<f64>> = BTreeMap::new();
    for ((ticker, timestamp), close) in tickers
        .into_no_null_iter()
        .zip(timestamps.into_no_null_iter())
        .zip(closes)
    {
        let Some(close) = close.filter(|price| price.is_finite() && *price > 0.0) else {
            continue;
        };
        let Some(instant) = chrono::DateTime::from_timestamp_millis(timestamp) else {
            continue;
        };
        if !is_regular_hours(instant) {
            continue;
        }
        closes_by_key
            .entry((SessionDate::at(instant), ticker.to_string()))
            .or_default()
            .push(close);
    }

    let mut by_session: BTreeMap<SessionDate, BTreeMap<String, Vec<f64>>> = BTreeMap::new();
    for ((session, ticker), prices) in closes_by_key {
        let returns: Vec<f64> = prices
            .windows(2)
            .map(|pair| (pair[1] / pair[0]).ln())
            .filter(|value| value.is_finite())
            .collect();
        if returns.is_empty() {
            continue;
        }
        by_session
            .entry(session)
            .or_default()
            .insert(ticker, returns);
    }

    Ok(by_session
        .into_iter()
        .map(|(session, by_ticker)| SessionReturns { session, by_ticker })
        .collect())
}

/// Whether an instant falls inside the regular session on the Eastern wall clock.
///
/// Read through `eastern_datetime` rather than a fixed offset, because the archive spans both sides
/// of every daylight-saving change and a hardcoded offset would cut the wrong hour for half the year.
fn is_regular_hours(instant: chrono::DateTime<chrono::Utc>) -> bool {
    let eastern = eastern_datetime(instant);
    let minute = eastern.time().hour() * 60 + eastern.time().minute();
    (REGULAR_OPEN_MINUTE..REGULAR_CLOSE_MINUTE).contains(&minute)
}

/// Sample autocorrelation of a series at `lag`, or `None` when it is not estimable.
///
/// Returns `None` on a series too short for the lag or one with no variance, rather than a zero
/// that would read as "measured, and it is nothing".
pub fn autocorrelation(series: &[f64], lag: usize) -> Option<f64> {
    if lag == 0 || series.len() <= lag + 1 {
        return None;
    }
    let count = series.len() as f64;
    let mean = series.iter().sum::<f64>() / count;
    let variance: f64 = series.iter().map(|value| (value - mean).powi(2)).sum();
    if variance <= 0.0 || !variance.is_finite() {
        return None;
    }
    let covariance: f64 = series[lag..]
        .iter()
        .zip(series)
        .map(|(later, earlier)| (later - mean) * (earlier - mean))
        .sum();
    let correlation = covariance / variance;
    correlation.is_finite().then_some(correlation)
}

/// What the bounce check found, pooled across names and sessions.
///
/// `lag_one` is the tell: a five-minute close is whichever side of the spread the last trade hit, so
/// bounce alternates consecutive closes and shows up as a strongly negative first-order coefficient.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct BounceReading {
    /// Mean first-order autocorrelation across qualifying name-sessions.
    pub lag_one: f64,
    /// Mean second-order autocorrelation, which bounce alone should leave near zero.
    pub lag_two: f64,
    /// Median Roll effective spread, as a fraction of price, over the name-sessions that admit one.
    pub roll_spread: Option<f64>,
    /// Name-sessions clearing [`MINIMUM_RETURNS`].
    pub measured: usize,
    /// Share of those whose first-order coefficient is negative.
    pub share_negative: f64,
}

/// Measures bid-ask bounce over per-session returns.
///
/// Each name-session is measured on its own and the coefficients averaged, because concatenating
/// names would difference one name's last bar against another's first and manufacture the very
/// adjacency the measurement is about.
pub fn bounce(sessions: &[SessionReturns]) -> Option<BounceReading> {
    let mut lag_one = Vec::new();
    let mut lag_two = Vec::new();
    let mut spreads = Vec::new();

    for session in sessions {
        for (_, returns) in session.iter() {
            if returns.len() < MINIMUM_RETURNS {
                continue;
            }
            let Some(first) = autocorrelation(returns, 1) else {
                continue;
            };
            lag_one.push(first);
            if let Some(second) = autocorrelation(returns, 2) {
                lag_two.push(second);
            }
            if let Some(spread) = roll_spread(returns) {
                spreads.push(spread);
            }
        }
    }

    if lag_one.is_empty() {
        return None;
    }
    let measured = lag_one.len();
    let negative = lag_one.iter().filter(|value| **value < 0.0).count();

    Some(BounceReading {
        lag_one: mean(&lag_one)?,
        lag_two: mean(&lag_two).unwrap_or(0.0),
        roll_spread: median(&mut spreads),
        measured,
        share_negative: negative as f64 / measured as f64,
    })
}

/// Roll's effective spread, `2 * sqrt(-cov(r_t, r_t-1))`, as a fraction of price.
///
/// Defined only where the first-order autocovariance is negative, which is what makes it a
/// measurement *of* bounce rather than one contaminated by it; a non-negative covariance means the
/// estimator has nothing to say and `None` says so.
pub fn roll_spread(returns: &[f64]) -> Option<f64> {
    if returns.len() < 3 {
        return None;
    }
    let count = returns.len() as f64;
    let mean = returns.iter().sum::<f64>() / count;
    let covariance: f64 = returns[1..]
        .iter()
        .zip(returns)
        .map(|(later, earlier)| (later - mean) * (earlier - mean))
        .sum::<f64>()
        / (count - 1.0);
    (covariance < 0.0).then(|| 2.0 * (-covariance).sqrt())
}

/// A frame of one session's returns, shaped for [`crate::laboratory::predictor::Panel`].
///
/// The time axis is bars rather than days, which is the whole point: a panel built per session
/// cannot reach across the overnight gap.
pub fn panel_frame(session: &SessionReturns) -> Result<DataFrame, IntradayError> {
    let mut tickers: Vec<String> = Vec::new();
    let mut periods: Vec<i64> = Vec::new();
    let mut values: Vec<f64> = Vec::new();

    for (ticker, returns) in session.iter() {
        for (index, value) in returns.iter().enumerate() {
            tickers.push(ticker.to_string());
            // The bar's ordinal within the session, not its instant. Names start trading at
            // different times, so instants would leave the grid mostly holes.
            periods.push(index as i64);
            values.push(*value);
        }
    }

    Ok(DataFrame::new(vec![
        Column::new("ticker".into(), tickers),
        Column::new("timestamp".into(), periods),
        Column::new("intraday_return".into(), values),
    ])?)
}

/// Predicts the next bar from the bar `skip` places back, leaving the ones between unread.
///
/// **The control that separates real reversion from bid-ask bounce.** Bounce is an artefact of two
/// *adjacent* closes landing on opposite sides of one spread, so it mostly dies at `skip = 2` while
/// genuine convergence, which takes longer than five minutes, does not.
pub struct SkippedPersistence {
    pub skip: usize,
    name: String,
}

impl SkippedPersistence {
    /// Refuses `skip` below two, which is plain persistence and skips nothing.
    pub fn new(skip: usize) -> Option<Self> {
        (skip >= 2).then(|| Self {
            skip,
            name: format!("persistence_skip_{skip}"),
        })
    }
}

impl crate::laboratory::predictor::Predictor for SkippedPersistence {
    fn name(&self) -> &str {
        &self.name
    }

    fn score(&self, history: &crate::laboratory::predictor::History) -> Vec<Option<f64>> {
        // Saturating, not wrapping: early in a session there is no bar `skip` back, and wrapping
        // would index from the end and score the forecast against a bar from the session's close.
        let Some(index) = history.sessions().checked_sub(self.skip) else {
            return vec![None; history.tickers()];
        };
        history
            .returns_at(index)
            .map_or_else(|| vec![None; history.tickers()], <[_]>::to_vec)
    }
}

fn mean(values: &[f64]) -> Option<f64> {
    (!values.is_empty()).then(|| values.iter().sum::<f64>() / values.len() as f64)
}

fn median(values: &mut [f64]) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    values.sort_by(f64::total_cmp);
    let middle = values.len() / 2;
    Some(if values.len() % 2 == 0 {
        (values[middle - 1] + values[middle]) / 2.0
    } else {
        values[middle]
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    /// 09:35 Eastern on a summer session, in milliseconds. Eastern is UTC-4 then, so 13:35 UTC.
    fn eastern_bar(year: i32, month: u32, day: u32, hour: u32, minute: u32) -> i64 {
        let eastern: chrono_tz::Tz = chrono_tz::America::New_York;
        eastern
            .with_ymd_and_hms(year, month, day, hour, minute, 0)
            .unwrap()
            .timestamp_millis()
    }

    fn frame(rows: &[(&str, i64, f64)]) -> DataFrame {
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

    /// The whole reason this module exists: a five-minute return must never be an overnight one.
    #[test]
    fn test_a_return_never_spans_two_sessions() {
        let bars = frame(&[
            ("AAA", eastern_bar(2026, 6, 1, 15, 50), 100.0),
            ("AAA", eastern_bar(2026, 6, 1, 15, 55), 101.0),
            // Next session opens far away; differencing across the gap would read as +9.9%.
            ("AAA", eastern_bar(2026, 6, 2, 9, 35), 111.0),
            ("AAA", eastern_bar(2026, 6, 2, 9, 40), 112.0),
        ]);

        let sessions = session_returns(&bars).unwrap();

        assert_eq!(sessions.len(), 2, "one entry per session, not one series");
        assert_eq!(sessions[0].returns_of("AAA").unwrap().len(), 1);
        assert_eq!(sessions[1].returns_of("AAA").unwrap().len(), 1);
        let overnight = (111.0_f64 / 101.0).ln();
        for session in &sessions {
            for (_, returns) in session.iter() {
                assert!(
                    returns.iter().all(|value| (value - overnight).abs() > 1e-9),
                    "the overnight gap must not appear as an intraday return"
                );
            }
        }
    }

    /// The archive carries 04:00-19:59, and a pre-market print differenced against the open would
    /// put a wide, illiquid move into the first return of every session.
    #[test]
    fn test_bars_outside_regular_hours_are_dropped() {
        let bars = frame(&[
            ("AAA", eastern_bar(2026, 6, 1, 8, 0), 90.0),
            ("AAA", eastern_bar(2026, 6, 1, 9, 30), 100.0),
            ("AAA", eastern_bar(2026, 6, 1, 9, 35), 101.0),
            ("AAA", eastern_bar(2026, 6, 1, 18, 0), 130.0),
        ]);

        let sessions = session_returns(&bars).unwrap();

        let returns = sessions[0].returns_of("AAA").unwrap();
        assert_eq!(returns.len(), 1, "only 09:30 to 09:35 survives the cut");
        assert!((returns[0] - (101.0_f64 / 100.0).ln()).abs() < 1e-12);
    }

    /// A bar stamped exactly at the close opened at 16:00 and belongs to no tradeable interval.
    #[test]
    fn test_the_close_bound_is_exclusive_and_the_open_bound_is_not() {
        let eastern: chrono_tz::Tz = chrono_tz::America::New_York;
        let open = eastern
            .with_ymd_and_hms(2026, 6, 1, 9, 30, 0)
            .unwrap()
            .with_timezone(&chrono::Utc);
        let close = eastern
            .with_ymd_and_hms(2026, 6, 1, 16, 0, 0)
            .unwrap()
            .with_timezone(&chrono::Utc);

        assert!(is_regular_hours(open));
        assert!(!is_regular_hours(close));
    }

    /// Eastern is UTC-4 in June and UTC-5 in December. A fixed offset would cut the wrong hour for
    /// half the archive, and the five-year window spans ten changeovers.
    #[test]
    fn test_the_regular_hours_cut_follows_daylight_saving() {
        let summer = eastern_bar(2026, 6, 1, 9, 35);
        let winter = eastern_bar(2026, 12, 1, 9, 35);

        let summer_utc = chrono::DateTime::from_timestamp_millis(summer).unwrap();
        let winter_utc = chrono::DateTime::from_timestamp_millis(winter).unwrap();

        assert!(is_regular_hours(summer_utc));
        assert!(is_regular_hours(winter_utc));
        // Same Eastern wall clock, one hour apart in UTC — which is what a fixed offset gets wrong.
        assert_eq!(summer_utc.time().hour(), 13);
        assert_eq!(winter_utc.time().hour(), 14);
    }

    /// A perfectly alternating series is what pure bounce looks like, and its first-order
    /// coefficient sits at -1. Pinned to literals so it cannot drift with the code under test.
    #[test]
    fn test_an_alternating_series_reads_as_full_negative_autocorrelation() {
        let alternating: Vec<f64> = (0..40)
            .map(|index| if index % 2 == 0 { 0.01 } else { -0.01 })
            .collect();

        let first = autocorrelation(&alternating, 1).unwrap();
        let second = autocorrelation(&alternating, 2).unwrap();

        assert!(
            first < -0.9,
            "alternating closes read as bounce, got {first}"
        );
        assert!(
            second > 0.9,
            "two bars apart the alternation realigns, got {second}"
        );
    }

    /// Roll's estimator is defined only where the autocovariance is negative; a trending series has
    /// nothing to say and must say so rather than returning zero.
    #[test]
    fn test_roll_refuses_a_series_without_negative_autocovariance() {
        let trending: Vec<f64> = (0..40).map(|index| 0.001 * index as f64).collect();
        assert_eq!(roll_spread(&trending), None);

        let alternating: Vec<f64> = (0..40)
            .map(|index| if index % 2 == 0 { 0.01 } else { -0.01 })
            .collect();
        let spread = roll_spread(&alternating).expect("alternating returns admit a spread");
        assert!(
            (spread - 0.02).abs() < 1e-3,
            "a one-cent-in-a-dollar bounce reads near 2%, got {spread}"
        );
    }

    /// Zero variance is not zero autocorrelation, and reporting it as such would read as a measured
    /// null rather than an unmeasurable one.
    #[test]
    fn test_a_flat_series_is_unmeasurable_rather_than_zero() {
        assert_eq!(autocorrelation(&[0.0; 40], 1), None);
        assert_eq!(autocorrelation(&[0.01, 0.02], 1), None);
        assert_eq!(autocorrelation(&[0.01; 40], 0), None);
    }

    /// Pooling names into one series would difference one name's last bar against another's first.
    #[test]
    fn test_bounce_measures_each_name_separately() {
        let mut by_ticker = BTreeMap::new();
        by_ticker.insert(
            "AAA".to_string(),
            (0..40)
                .map(|index| if index % 2 == 0 { 0.01 } else { -0.01 })
                .collect::<Vec<f64>>(),
        );
        by_ticker.insert(
            "BBB".to_string(),
            (0..40)
                .map(|index| if index % 2 == 0 { 0.02 } else { -0.02 })
                .collect::<Vec<f64>>(),
        );
        let sessions = vec![SessionReturns {
            session: SessionDate::from_date(chrono::NaiveDate::from_ymd_opt(2026, 6, 1).unwrap()),
            by_ticker,
        }];

        let reading = bounce(&sessions).expect("two qualifying names");

        assert_eq!(reading.measured, 2);
        assert!((reading.share_negative - 1.0).abs() < 1e-12);
        assert!(reading.lag_one < -0.9);
    }

    /// A name that traded four times in a session carries almost no information about its own
    /// autocorrelation, and averaging it in would let the thin tail outvote the liquid names.
    #[test]
    fn test_a_name_with_too_few_returns_is_not_measured() {
        let mut by_ticker = BTreeMap::new();
        by_ticker.insert("AAA".to_string(), vec![0.01, -0.01, 0.01, -0.01]);
        let sessions = vec![SessionReturns {
            session: SessionDate::from_date(chrono::NaiveDate::from_ymd_opt(2026, 6, 1).unwrap()),
            by_ticker,
        }];

        assert!(bounce(&sessions).is_none());
    }

    /// The panel's time axis is the bar's ordinal within the session, so names that start trading at
    /// different times still line up instead of leaving the grid mostly holes.
    #[test]
    fn test_the_panel_frame_is_indexed_by_bar_ordinal() {
        let mut by_ticker = BTreeMap::new();
        by_ticker.insert("AAA".to_string(), vec![0.01, 0.02, 0.03]);
        by_ticker.insert("BBB".to_string(), vec![-0.01, -0.02]);
        let session = SessionReturns {
            session: SessionDate::from_date(chrono::NaiveDate::from_ymd_opt(2026, 6, 1).unwrap()),
            by_ticker,
        };

        let frame = panel_frame(&session).unwrap();

        assert_eq!(frame.height(), 5);
        let periods: Vec<i64> = frame
            .column("timestamp")
            .unwrap()
            .i64()
            .unwrap()
            .into_no_null_iter()
            .collect();
        assert_eq!(periods, vec![0, 1, 2, 0, 1]);
    }

    /// Plain persistence skips nothing, so accepting it under this name would silently report the
    /// contaminated reading as the control that rules contamination out.
    #[test]
    fn test_a_skip_below_two_is_refused() {
        assert!(SkippedPersistence::new(0).is_none());
        assert!(SkippedPersistence::new(1).is_none());
        assert_eq!(SkippedPersistence::new(2).unwrap().skip, 2);
    }

    /// The finding rests entirely on this: if `skip = 2` still read the adjacent bar it would carry
    /// the bounce it exists to exclude, and the control would agree with the contaminated reading
    /// for the wrong reason.
    #[test]
    fn test_a_skipped_score_reads_past_the_adjacent_bar() {
        use crate::laboratory::predictor::{Panel, Predictor};

        let mut by_ticker = BTreeMap::new();
        // Distinct values, so which bar was read is unambiguous from the score alone.
        by_ticker.insert("AAA".to_string(), vec![0.10, 0.20, 0.30, 0.40]);
        let session = SessionReturns {
            session: SessionDate::from_date(chrono::NaiveDate::from_ymd_opt(2026, 6, 1).unwrap()),
            by_ticker,
        };
        let frame = panel_frame(&session).unwrap();
        let panel = Panel::from_frame_of(&frame, "intraday_return").unwrap();

        // Forecasting the bar at index 3 (0.40): the adjacent bar is 0.30, two back is 0.20.
        let history = panel.history_before(3);
        let plain = crate::laboratory::predictor::Persistence.score(&history);
        let skipped = SkippedPersistence::new(2).unwrap().score(&history);

        assert_eq!(
            plain[0],
            Some(0.30),
            "plain persistence reads the adjacent bar"
        );
        assert_eq!(skipped[0], Some(0.20), "skip-2 must read past it");
    }

    /// Early in a session there is no bar `skip` back. Wrapping would index from the end and score
    /// the forecast against a bar from the close, which is the session's own future.
    #[test]
    fn test_a_skip_before_the_session_has_room_abstains() {
        use crate::laboratory::predictor::{Panel, Predictor};

        let mut by_ticker = BTreeMap::new();
        by_ticker.insert("AAA".to_string(), vec![0.10, 0.20, 0.30]);
        let session = SessionReturns {
            session: SessionDate::from_date(chrono::NaiveDate::from_ymd_opt(2026, 6, 1).unwrap()),
            by_ticker,
        };
        let frame = panel_frame(&session).unwrap();
        let panel = Panel::from_frame_of(&frame, "intraday_return").unwrap();

        let scores = SkippedPersistence::new(3)
            .unwrap()
            .score(&panel.history_before(1));

        assert_eq!(scores, vec![None], "no bar three back, so no score");
    }

    /// A zero or negative close would make the log return infinite or absent; the row goes rather
    /// than poisoning the name's whole series.
    #[test]
    fn test_a_non_positive_close_is_skipped() {
        let bars = frame(&[
            ("AAA", eastern_bar(2026, 6, 1, 9, 30), 100.0),
            ("AAA", eastern_bar(2026, 6, 1, 9, 35), 0.0),
            ("AAA", eastern_bar(2026, 6, 1, 9, 40), 102.0),
        ]);

        let sessions = session_returns(&bars).unwrap();

        let returns = sessions[0].returns_of("AAA").unwrap();
        assert_eq!(returns.len(), 1);
        assert!((returns[0] - (102.0_f64 / 100.0).ln()).abs() < 1e-12);
    }
}
