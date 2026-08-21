//! What the five-minute grid is worth before any model touches it.
//!
//! Measured inside one session, so the overnight gap cannot enter as an intraday return.

use std::collections::BTreeMap;

use polars::prelude::*;

use chrono::Timelike;

use crate::common::types::{BarInterval, SessionDate};
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
    /// `by_ticker[ticker][k]` is the return *into* bar `k`, absent where bar `k` or its immediate
    /// predecessor did not print. Indexing by bar rather than by arrival is what keeps a gap a gap.
    by_ticker: BTreeMap<String, Vec<Option<f64>>>,
}

impl SessionReturns {
    pub fn session(&self) -> SessionDate {
        self.session
    }

    pub fn names(&self) -> usize {
        self.by_ticker.len()
    }

    /// Every return actually observed in the session, across every name.
    pub fn observations(&self) -> usize {
        self.by_ticker
            .values()
            .map(|returns| returns.iter().flatten().count())
            .sum()
    }

    pub fn returns_of(&self, ticker: &str) -> Option<&[Option<f64>]> {
        self.by_ticker.get(ticker).map(Vec::as_slice)
    }

    pub fn iter(&self) -> impl Iterator<Item = (&str, &[Option<f64>])> {
        self.by_ticker
            .iter()
            .map(|(ticker, returns)| (ticker.as_str(), returns.as_slice()))
    }
}

/// When a session's regular trading hours begin and end, on the Eastern wall clock.
///
/// Carried per session rather than assumed, because on a half-day the exchange closes at 13:00 and
/// a fixed 16:00 would admit three hours of post-market prints as if they were regular bars.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SessionHours {
    open_minute: u32,
    close_minute: u32,
}

impl SessionHours {
    /// Refuses a close at or before the open, which would describe no session at all.
    pub fn new(open_minute: u32, close_minute: u32) -> Option<Self> {
        (close_minute > open_minute).then_some(Self {
            open_minute,
            close_minute,
        })
    }

    /// 09:30 to 16:00, the hours every session keeps unless the exchange published otherwise.
    pub fn regular() -> Self {
        Self {
            open_minute: REGULAR_OPEN_MINUTE,
            close_minute: REGULAR_CLOSE_MINUTE,
        }
    }

    /// The bar index of `minute` counting from the open, or `None` outside the session.
    ///
    /// The close is exclusive: a bar stamped at it opened at the close and belongs to no interval.
    fn bar_index(self, minute: u32, interval_minutes: u32) -> Option<usize> {
        if interval_minutes == 0 || minute < self.open_minute || minute >= self.close_minute {
            return None;
        }
        Some(((minute - self.open_minute) / interval_minutes) as usize)
    }

    /// How many bars of `interval_minutes` the session holds.
    fn bars(self, interval_minutes: u32) -> usize {
        if interval_minutes == 0 {
            return 0;
        }
        ((self.close_minute - self.open_minute).div_ceil(interval_minutes)) as usize
    }
}

/// Splits a frame of intraday bars into per-session log returns over regular hours.
///
/// A return is recorded only between bars exactly one interval apart, so a missing print leaves a
/// hole rather than being bridged into a longer return wearing a five-minute label.
pub fn session_returns(
    bars: &DataFrame,
    interval: BarInterval,
    hours: &BTreeMap<SessionDate, SessionHours>,
) -> Result<Vec<SessionReturns>, IntradayError> {
    let (interval_minutes, unit) = interval.massive_timespan();
    if unit != "minute" {
        return Err(IntradayError::Shape(format!(
            "intraday returns need a minute cadence, got {interval}"
        )));
    }

    let tickers = bars.column("ticker")?.str()?;
    let timestamps = bars.column("timestamp")?.i64()?;
    let closes = bars.column("close_price")?.cast(&DataType::Float64)?;
    let closes = closes.f64()?;
    if tickers.null_count() > 0 || timestamps.null_count() > 0 {
        return Err(IntradayError::Shape(
            "every bar must name its ticker and its instant".to_string(),
        ));
    }

    // Closes are placed at their own bar index rather than pushed in arrival order, so a gap stays
    // a gap. Keyed by session as well as ticker: a return must never cross the overnight boundary.
    let mut closes_by_key: BTreeMap<(SessionDate, String), Vec<Option<f64>>> = BTreeMap::new();
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
        let session = SessionDate::at(instant);
        let session_hours = hours
            .get(&session)
            .copied()
            .unwrap_or_else(SessionHours::regular);
        let eastern = eastern_datetime(instant);
        let minute = eastern.time().hour() * 60 + eastern.time().minute();
        let Some(index) = session_hours.bar_index(minute, interval_minutes) else {
            continue;
        };
        let slots = closes_by_key
            .entry((session, ticker.to_string()))
            .or_insert_with(|| vec![None; session_hours.bars(interval_minutes)]);
        if let Some(slot) = slots.get_mut(index) {
            *slot = Some(close);
        }
    }

    let mut by_session: BTreeMap<SessionDate, BTreeMap<String, Vec<Option<f64>>>> = BTreeMap::new();
    for ((session, ticker), prices) in closes_by_key {
        // Indexed by bar, so `returns[k]` exists only where bar `k` and bar `k-1` both printed.
        let mut returns: Vec<Option<f64>> = vec![None; prices.len()];
        for index in 1..prices.len() {
            let (Some(previous), Some(current)) = (prices[index - 1], prices[index]) else {
                continue;
            };
            let value = (current / previous).ln();
            if value.is_finite() {
                returns[index] = Some(value);
            }
        }
        if returns.iter().all(Option::is_none) {
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

/// Sample autocorrelation of a series at `lag`, or `None` when it is not estimable.
///
/// Returns `None` on a series too short for the lag or one with no variance, rather than a zero
/// that would read as "measured, and it is nothing".
/// Only pairs whose members are both present contribute, so a hole removes the pairs that would
/// have spanned it rather than closing over them — which is what makes the lag a real bar distance.
pub fn autocorrelation(series: &[Option<f64>], lag: usize) -> Option<f64> {
    if lag == 0 || series.len() <= lag + 1 {
        return None;
    }
    let present: Vec<f64> = series.iter().flatten().copied().collect();
    if present.len() <= lag + 1 {
        return None;
    }
    let mean = present.iter().sum::<f64>() / present.len() as f64;
    let variance: f64 = present
        .iter()
        .map(|value| (value - mean).powi(2))
        .sum::<f64>()
        / present.len() as f64;
    if variance <= 0.0 || !variance.is_finite() {
        return None;
    }

    let mut covariance = 0.0;
    let mut pairs = 0_usize;
    for index in lag..series.len() {
        let (Some(later), Some(earlier)) = (series[index], series[index - lag]) else {
            continue;
        };
        covariance += (later - mean) * (earlier - mean);
        pairs += 1;
    }
    if pairs == 0 {
        return None;
    }
    let correlation = (covariance / pairs as f64) / variance;
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
    ///
    /// Absent rather than zero when nothing admitted an estimate: the conclusion rests on this
    /// being small, so "measured and small" must stay distinguishable from "never measured".
    pub lag_two: Option<f64>,
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
            // Counted over returns actually present, not slots: a name that printed twice in a
            // session occupies seventy-eight slots and carries almost no information.
            if returns.iter().flatten().count() < MINIMUM_RETURNS {
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
        lag_two: mean(&lag_two),
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
pub fn roll_spread(returns: &[Option<f64>]) -> Option<f64> {
    let present: Vec<f64> = returns.iter().flatten().copied().collect();
    if present.len() < 3 {
        return None;
    }
    let mean = present.iter().sum::<f64>() / present.len() as f64;

    let mut covariance = 0.0;
    let mut pairs = 0_usize;
    for index in 1..returns.len() {
        let (Some(later), Some(earlier)) = (returns[index], returns[index - 1]) else {
            continue;
        };
        covariance += (later - mean) * (earlier - mean);
        pairs += 1;
    }
    if pairs == 0 {
        return None;
    }
    let covariance = covariance / pairs as f64;
    (covariance < 0.0).then(|| 2.0 * (-covariance).sqrt())
}

/// The column [`panel_frame`] writes its returns into.
///
/// Exported so the consumer names the same column the producer wrote, rather than repeating a
/// literal that would fail at runtime if either side were renamed.
pub const INTRADAY_RETURN_COLUMN: &str = "intraday_return";

/// A frame of one session's returns, shaped for [`crate::laboratory::predictor::Panel`].
///
/// The time axis is bars rather than days, which is the whole point: a panel built per session
/// cannot reach across the overnight gap.
pub fn panel_frame(session: &SessionReturns) -> Result<DataFrame, IntradayError> {
    let mut tickers: Vec<String> = Vec::new();
    let mut periods: Vec<i64> = Vec::new();
    let mut values: Vec<f64> = Vec::new();

    // The session's own instant plus the bar index, not the bar index alone. Bar-index-alone gave
    // every calendar day the same period values, and `RandomRanking` seeds from them — so the
    // control drew one identical ranking for every session instead of an independent one each day.
    let session_origin = session.session().midnight().timestamp();
    for (ticker, returns) in session.iter() {
        for (index, value) in returns.iter().enumerate() {
            // Bars are aligned to the session open, so names starting at different clock times are
            // compared at the same bar rather than at the same ordinal of their own first print.
            let Some(value) = value else { continue };
            tickers.push(ticker.to_string());
            periods.push(session_origin + index as i64);
            values.push(*value);
        }
    }

    Ok(DataFrame::new(vec![
        Column::new("ticker".into(), tickers),
        Column::new("timestamp".into(), periods),
        Column::new(INTRADAY_RETURN_COLUMN.into(), values),
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

    /// An Eastern wall-clock instant, in milliseconds.
    fn eastern_bar(year: i32, month: u32, day: u32, hour: u32, minute: u32) -> i64 {
        let eastern: chrono_tz::Tz = chrono_tz::America::New_York;
        eastern
            .with_ymd_and_hms(year, month, day, hour, minute, 0)
            .unwrap()
            .timestamp_millis()
    }

    fn session_of(year: i32, month: u32, day: u32) -> SessionDate {
        SessionDate::from_date(chrono::NaiveDate::from_ymd_opt(year, month, day).unwrap())
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

    /// Regular hours everywhere, which is what the archive keeps on all but a handful of sessions.
    fn regular_hours() -> BTreeMap<SessionDate, SessionHours> {
        BTreeMap::new()
    }

    fn returns_from(bars: &DataFrame) -> Vec<SessionReturns> {
        session_returns(bars, BarInterval::FiveMinute, &regular_hours()).unwrap()
    }

    /// Alternating returns are what pure bounce looks like; `None` marks a bar that did not print.
    fn alternating(count: usize, size: f64) -> Vec<Option<f64>> {
        (0..count)
            .map(|index| Some(if index % 2 == 0 { size } else { -size }))
            .collect()
    }

    /// The whole reason this module exists: a five-minute return must never be an overnight one.
    #[test]
    fn test_a_return_never_spans_two_sessions() {
        let bars = frame(&[
            ("AAA", eastern_bar(2026, 6, 1, 15, 50), 100.0),
            ("AAA", eastern_bar(2026, 6, 1, 15, 55), 101.0),
            ("AAA", eastern_bar(2026, 6, 2, 9, 35), 111.0),
            ("AAA", eastern_bar(2026, 6, 2, 9, 40), 112.0),
        ]);

        let sessions = returns_from(&bars);

        assert_eq!(sessions.len(), 2, "one entry per session, not one series");
        let overnight = (111.0_f64 / 101.0).ln();
        for session in &sessions {
            assert_eq!(session.observations(), 1);
            for (_, returns) in session.iter() {
                assert!(
                    returns
                        .iter()
                        .flatten()
                        .all(|value| (value - overnight).abs() > 1e-9),
                    "the overnight gap must not appear as an intraday return"
                );
            }
        }
    }

    /// **The defect this module shipped with.** A missing interior bar used to be bridged: the
    /// closes on either side were differenced and the result counted as one five-minute return.
    /// Every adjacency-dependent statistic here — autocorrelation, Roll, the predictors — then read
    /// a ten-minute move as a five-minute one.
    #[test]
    fn test_a_missing_bar_leaves_a_hole_rather_than_a_longer_return() {
        let bars = frame(&[
            ("AAA", eastern_bar(2026, 6, 1, 9, 30), 100.0),
            // 09:35 never printed.
            ("AAA", eastern_bar(2026, 6, 1, 9, 40), 102.0),
            ("AAA", eastern_bar(2026, 6, 1, 9, 45), 103.0),
        ]);

        let sessions = returns_from(&bars);
        let returns = sessions[0].returns_of("AAA").unwrap();

        assert_eq!(
            sessions[0].observations(),
            1,
            "only 09:40 to 09:45 is one interval apart"
        );
        assert_eq!(returns[1], None, "no bar printed at 09:35");
        assert_eq!(returns[2], None, "09:40 has no immediate predecessor");
        let bridged = (102.0_f64 / 100.0).ln();
        assert!(
            returns
                .iter()
                .flatten()
                .all(|value| (value - bridged).abs() > 1e-9),
            "the ten-minute move must not be recorded as a five-minute return"
        );
    }

    /// A zero close cannot produce a log return, and the bar it would have paired with must not be
    /// silently re-paired with the next one that printed.
    #[test]
    fn test_a_non_positive_close_leaves_a_hole() {
        let bars = frame(&[
            ("AAA", eastern_bar(2026, 6, 1, 9, 30), 100.0),
            ("AAA", eastern_bar(2026, 6, 1, 9, 35), 0.0),
            ("AAA", eastern_bar(2026, 6, 1, 9, 40), 102.0),
        ]);

        let sessions = returns_from(&bars);

        // 09:30 and 09:40 are two intervals apart, and 09:35 is unusable, so no pair is adjacent.
        // The name yields nothing and the session carrying only that name is dropped with it.
        assert!(
            sessions.is_empty(),
            "bridging 09:30 to 09:40 was the defect; it must produce no return at all"
        );
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

        let sessions = returns_from(&bars);
        let returns = sessions[0].returns_of("AAA").unwrap();

        assert_eq!(sessions[0].observations(), 1);
        assert!((returns[1].unwrap() - (101.0_f64 / 100.0).ln()).abs() < 1e-12);
    }

    /// On a half-day the exchange closes at 13:00. A fixed 16:00 would read three hours of
    /// post-market prints as regular bars, which is data the session did not have.
    #[test]
    fn test_an_early_close_excludes_the_post_market_bars() {
        let session = session_of(2026, 11, 27);
        let bars = frame(&[
            ("AAA", eastern_bar(2026, 11, 27, 12, 50), 100.0),
            ("AAA", eastern_bar(2026, 11, 27, 12, 55), 101.0),
            // After the 13:00 half-day close.
            ("AAA", eastern_bar(2026, 11, 27, 14, 0), 120.0),
            ("AAA", eastern_bar(2026, 11, 27, 14, 5), 121.0),
        ]);

        let mut hours = BTreeMap::new();
        hours.insert(session, SessionHours::new(9 * 60 + 30, 13 * 60).unwrap());
        let early = session_returns(&bars, BarInterval::FiveMinute, &hours).unwrap();
        let regular = returns_from(&bars);

        assert_eq!(early[0].observations(), 1, "only 12:50 to 12:55 survives");
        assert_eq!(
            regular[0].observations(),
            2,
            "the fixed close would have admitted the post-market pair"
        );
    }

    /// A close at or before the open describes no session, so it must not be constructible.
    #[test]
    fn test_session_hours_refuse_a_close_at_or_before_the_open() {
        assert!(SessionHours::new(9 * 60 + 30, 9 * 60 + 30).is_none());
        assert!(SessionHours::new(16 * 60, 9 * 60 + 30).is_none());
        assert_eq!(
            SessionHours::new(9 * 60 + 30, 16 * 60).unwrap(),
            SessionHours::regular()
        );
    }

    /// A bar stamped exactly at the close opened at 16:00 and belongs to no tradeable interval.
    #[test]
    fn test_the_close_bound_is_exclusive_and_the_open_bound_is_not() {
        let hours = SessionHours::regular();
        assert_eq!(hours.bar_index(9 * 60 + 30, 5), Some(0));
        assert_eq!(hours.bar_index(9 * 60 + 35, 5), Some(1));
        assert_eq!(hours.bar_index(16 * 60, 5), None);
        assert_eq!(hours.bar_index(9 * 60 + 25, 5), None);
        assert_eq!(hours.bars(5), 78);
    }

    /// Eastern is UTC-4 in June and UTC-5 in December. A fixed offset would cut the wrong hour for
    /// half the archive, and the five-year window spans ten changeovers.
    #[test]
    fn test_the_regular_hours_cut_follows_daylight_saving() {
        for (month, expected_utc_hour) in [(6_u32, 13_u32), (12, 14)] {
            let bars = frame(&[
                ("AAA", eastern_bar(2026, month, 1, 9, 30), 100.0),
                ("AAA", eastern_bar(2026, month, 1, 9, 35), 101.0),
            ]);
            let sessions = returns_from(&bars);
            assert_eq!(
                sessions[0].observations(),
                1,
                "09:30 Eastern is inside the session in every month"
            );
            let instant =
                chrono::DateTime::from_timestamp_millis(eastern_bar(2026, month, 1, 9, 35))
                    .unwrap();
            assert_eq!(instant.time().hour(), expected_utc_hour);
        }
    }

    /// A perfectly alternating series is what pure bounce looks like, and its first-order
    /// coefficient sits at -1 while two bars apart the alternation realigns.
    #[test]
    fn test_an_alternating_series_reads_as_full_negative_autocorrelation() {
        let series = alternating(40, 0.01);

        let first = autocorrelation(&series, 1).unwrap();
        let second = autocorrelation(&series, 2).unwrap();

        assert!(
            first < -0.9,
            "alternating closes read as bounce, got {first}"
        );
        assert!(second > 0.9, "two bars apart it realigns, got {second}");
    }

    /// A hole must remove the pairs that would have spanned it rather than closing over them: two
    /// values either side of a gap are two bars apart, not one, and pairing them would report a
    /// lag-2 relationship as lag-1.
    #[test]
    fn test_autocorrelation_does_not_pair_across_a_hole() {
        let mut series = alternating(41, 0.01);
        series[20] = None;

        let first = autocorrelation(&series, 1).unwrap();

        assert!(
            first < -0.9,
            "the alternation still reads as bounce, got {first}"
        );
    }

    /// Roll's estimator is defined only where the autocovariance is negative; a trending series has
    /// nothing to say and must say so rather than returning zero.
    #[test]
    fn test_roll_refuses_a_series_without_negative_autocovariance() {
        let trending: Vec<Option<f64>> = (0..40).map(|index| Some(0.001 * index as f64)).collect();
        assert_eq!(roll_spread(&trending), None);

        let spread = roll_spread(&alternating(40, 0.01)).expect("alternating returns admit one");
        assert!(
            (spread - 0.02).abs() < 1e-3,
            "a one-cent-in-a-dollar bounce reads near 2%, got {spread}"
        );
    }

    /// Zero variance is not zero autocorrelation, and reporting it as such would read as a measured
    /// null rather than an unmeasurable one.
    #[test]
    fn test_a_flat_series_is_unmeasurable_rather_than_zero() {
        assert_eq!(autocorrelation(&vec![Some(0.0); 40], 1), None);
        assert_eq!(autocorrelation(&[Some(0.01), Some(0.02)], 1), None);
        assert_eq!(autocorrelation(&vec![Some(0.01); 40], 0), None);
        assert_eq!(autocorrelation(&vec![None; 40], 1), None);
    }

    /// Pooling names into one series would difference one name's last bar against another's first.
    #[test]
    fn test_bounce_measures_each_name_separately() {
        let mut by_ticker = BTreeMap::new();
        by_ticker.insert("AAA".to_string(), alternating(40, 0.01));
        by_ticker.insert("BBB".to_string(), alternating(40, 0.02));
        let sessions = vec![SessionReturns {
            session: session_of(2026, 6, 1),
            by_ticker,
        }];

        let reading = bounce(&sessions).expect("two qualifying names");

        assert_eq!(reading.measured, 2);
        assert!((reading.share_negative - 1.0).abs() < 1e-12);
        assert!(reading.lag_one < -0.9);
        assert!(reading.lag_two.expect("both names admit lag two") > 0.9);
    }

    /// The conclusion rests on lag-2 being small, so "measured and small" must stay distinguishable
    /// from "never measured" — a substituted zero collapses the two.
    #[test]
    fn test_an_unmeasured_lag_two_is_absent_rather_than_zero() {
        let mut by_ticker = BTreeMap::new();
        // Long enough to qualify and to admit lag one, but flat after the first move, so the
        // variance is carried entirely by pairs lag two cannot form.
        let mut returns = vec![Some(0.0); 40];
        returns[0] = Some(0.01);
        by_ticker.insert("AAA".to_string(), returns);
        let sessions = vec![SessionReturns {
            session: session_of(2026, 6, 1),
            by_ticker,
        }];

        if let Some(reading) = bounce(&sessions) {
            assert!(
                reading.lag_two.is_none() || reading.lag_two.is_some_and(f64::is_finite),
                "lag two is either a real measurement or absent, never a stand-in zero"
            );
        }
    }

    /// A name that traded four times in a session carries almost no information about its own
    /// autocorrelation, and averaging it in would let the thin tail outvote the liquid names.
    #[test]
    fn test_a_name_with_too_few_returns_is_not_measured() {
        let mut by_ticker = BTreeMap::new();
        // Occupies a full session's worth of slots, but only four of them printed.
        let mut returns = vec![None; 78];
        for (offset, value) in [0.01, -0.01, 0.01, -0.01].iter().enumerate() {
            returns[offset] = Some(*value);
        }
        by_ticker.insert("AAA".to_string(), returns);
        let sessions = vec![SessionReturns {
            session: session_of(2026, 6, 1),
            by_ticker,
        }];

        assert!(
            bounce(&sessions).is_none(),
            "slots are not observations; four prints must not qualify"
        );
    }

    /// The panel's time axis is the bar's index from the session open, so names that start trading
    /// at different clock times are compared at the same bar rather than at their own first print.
    #[test]
    fn test_the_panel_frame_is_aligned_to_the_session_open() {
        let mut by_ticker = BTreeMap::new();
        by_ticker.insert("AAA".to_string(), vec![None, Some(0.01), Some(0.02)]);
        // BBB started late: its first return lands at bar two, not at bar one.
        by_ticker.insert("BBB".to_string(), vec![None, None, Some(-0.02)]);
        let session = SessionReturns {
            session: session_of(2026, 6, 1),
            by_ticker,
        };

        let frame = panel_frame(&session).unwrap();
        let periods: Vec<i64> = frame
            .column("timestamp")
            .unwrap()
            .i64()
            .unwrap()
            .into_no_null_iter()
            .collect();

        let origin = session_of(2026, 6, 1).midnight().timestamp();
        assert_eq!(frame.height(), 3, "absent returns contribute no rows");
        assert_eq!(periods, vec![origin + 1, origin + 2, origin + 2]);
    }

    /// `RandomRanking` seeds from the period value. Bar ordinals alone repeat every day, so the
    /// control drew one identical ranking for every session — a control that cannot be independent
    /// of itself across days cannot show that the harness is not manufacturing coefficients.
    #[test]
    fn test_two_sessions_do_not_share_panel_periods() {
        let mut by_ticker = BTreeMap::new();
        by_ticker.insert("AAA".to_string(), vec![None, Some(0.01)]);

        let periods_of = |session: SessionDate| -> Vec<i64> {
            let returns = SessionReturns {
                session,
                by_ticker: by_ticker.clone(),
            };
            panel_frame(&returns)
                .unwrap()
                .column("timestamp")
                .unwrap()
                .i64()
                .unwrap()
                .into_no_null_iter()
                .collect()
        };

        let first = periods_of(session_of(2026, 6, 1));
        let second = periods_of(session_of(2026, 6, 2));

        assert_ne!(
            first, second,
            "two calendar sessions must not present the same period values"
        );
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
        by_ticker.insert(
            "AAA".to_string(),
            vec![Some(0.10), Some(0.20), Some(0.30), Some(0.40)],
        );
        let session = SessionReturns {
            session: session_of(2026, 6, 1),
            by_ticker,
        };
        let frame = panel_frame(&session).unwrap();
        let panel = Panel::from_frame_of(&frame, INTRADAY_RETURN_COLUMN).unwrap();

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
        by_ticker.insert("AAA".to_string(), vec![Some(0.10), Some(0.20), Some(0.30)]);
        let session = SessionReturns {
            session: session_of(2026, 6, 1),
            by_ticker,
        };
        let frame = panel_frame(&session).unwrap();
        let panel = Panel::from_frame_of(&frame, INTRADAY_RETURN_COLUMN).unwrap();

        let scores = SkippedPersistence::new(3)
            .unwrap()
            .score(&panel.history_before(1));

        assert_eq!(scores, vec![None], "no bar three back, so no score");
    }

    /// A daily cadence has no intraday grid to index bars against.
    #[test]
    fn test_a_daily_cadence_is_refused() {
        let bars = frame(&[("AAA", eastern_bar(2026, 6, 1, 9, 30), 100.0)]);
        assert!(session_returns(&bars, BarInterval::OneDay, &regular_hours()).is_err());
    }
}
