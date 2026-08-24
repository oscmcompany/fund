//! The quoted book folded on ingest: ticks in, a dozen numbers per bar out.
//!
//! Quotes come from Alpaca rather than Massive; see [`crate::common::alpaca`] for that split.

use chrono::{DateTime, Duration, TimeZone, Utc};
use chrono_tz::America::New_York;
use polars::prelude::*;
use tracing::warn;

use crate::common::alpaca::{ClientError, MarketDataClient, QuoteFetch, QuoteTick};
use crate::common::types::{BarInterval, BasisPoints, QuoteSummary, SessionDate, Ticker};
use crate::data::calendar::TradingCalendar;

/// Seconds in one intraday bucket, matching the five-minute bar grid the archive is built on.
const BUCKET_SECONDS: i64 = 300;

/// The upper quantile every summary reports beside its median.
const UPPER_QUANTILE: f64 = 0.9;

/// The column set and order every quote partition is written in.
///
/// Declared rather than enforced: nothing projects onto it yet, because nothing reads these
/// partitions back. What it buys today is that [`summaries_to_dataframe`] is pinned to it by test,
/// so the shape cannot drift before the reader that will have to project exists.
pub const QUOTE_FRAME_COLUMNS: [(&str, DataType); 11] = [
    ("ticker", DataType::String),
    ("bar_interval", DataType::String),
    ("timestamp", DataType::Int64),
    ("quoted_spread_mean", DataType::Float64),
    ("quoted_spread_basis_points_mean", DataType::Float64),
    ("quoted_spread_basis_points_median", DataType::Float64),
    (
        "quoted_spread_basis_points_ninetieth_percentile",
        DataType::Float64,
    ),
    ("bid_size_mean", DataType::Float64),
    ("ask_size_mean", DataType::Float64),
    ("quote_count", DataType::Int64),
    ("covered_seconds", DataType::Float64),
];

/// One bar's worth of quoted book, weighed by prevailing time rather than by update count.
///
/// The weighting is the whole point of the type. A name's quote traffic is dominated by flicker no
/// order ever interacts with — AAPL's spread at the open reads 3.44bp weighted by time and 2.99bp
/// weighted by count, and the second number describes the message rate rather than the market.
#[derive(Debug, Default)]
struct SpreadAccumulator {
    covered_seconds: f64,
    spread_weighted: f64,
    spread_basis_points_weighted: f64,
    bid_size_weighted: f64,
    ask_size_weighted: f64,
    quote_count: i64,
    /// `(spread in basis points, seconds it prevailed)`, which the quantiles are read off.
    observations: Vec<(f64, f64)>,
}

impl SpreadAccumulator {
    /// Records that `tick` was the standing book for `seconds`.
    fn add(&mut self, tick: &QuoteTick, spread_basis_points: f64, seconds: f64) {
        self.covered_seconds += seconds;
        self.spread_weighted += tick.spread() * seconds;
        self.spread_basis_points_weighted += spread_basis_points * seconds;
        self.bid_size_weighted += f64::from(tick.bid_size()) * seconds;
        self.ask_size_weighted += f64::from(tick.ask_size()) * seconds;
        self.observations.push((spread_basis_points, seconds));
    }

    /// Takes everything `other` accumulated, consuming it.
    ///
    /// How the session figure is built: it is the merge of its own buckets rather than a second
    /// pass, so the two cadences cannot disagree about the session they describe. By value rather
    /// than by `&mut`, so a bucket cannot be absorbed twice and double-count itself.
    fn absorb(&mut self, mut other: Self) {
        self.covered_seconds += other.covered_seconds;
        self.spread_weighted += other.spread_weighted;
        self.spread_basis_points_weighted += other.spread_basis_points_weighted;
        self.bid_size_weighted += other.bid_size_weighted;
        self.ask_size_weighted += other.ask_size_weighted;
        self.quote_count += other.quote_count;
        self.observations.append(&mut other.observations);
    }

    /// Reduces the fold to a summary, or `None` when there is nothing coherent to report.
    ///
    /// Sorts in place, so the observations are left ordered by spread for whoever absorbs them.
    fn summarize(
        &mut self,
        ticker: &Ticker,
        bar_interval: BarInterval,
        timestamp: DateTime<Utc>,
    ) -> Option<QuoteSummary> {
        if self.covered_seconds <= 0.0 {
            return None;
        }
        self.observations
            .sort_unstable_by(|left, right| left.0.total_cmp(&right.0));

        let weight = self.covered_seconds;
        let summary = QuoteSummary::new(
            ticker.clone(),
            bar_interval,
            timestamp,
            self.spread_weighted / weight,
            BasisPoints::new(self.spread_basis_points_weighted / weight)?,
            BasisPoints::new(quantile(&self.observations, weight, 0.5)?)?,
            BasisPoints::new(quantile(&self.observations, weight, UPPER_QUANTILE)?)?,
            self.bid_size_weighted / weight,
            self.ask_size_weighted / weight,
            self.quote_count,
            weight,
        );
        match summary {
            Ok(summary) => Some(summary),
            // A fold that cannot make a coherent summary is this module's bug, not the feed's, and
            // dropping the bar keeps one arithmetic slip from poisoning a whole session's archive.
            Err(error) => {
                warn!(%ticker, %bar_interval, %timestamp, %error, "Discarded an incoherent quote fold");
                None
            }
        }
    }
}

/// The spread that `quantile` of the fold's time was spent at or below.
///
/// No interpolation: the reported figure is one an order could actually have faced, which matters
/// more here than a smooth estimate because a spread is quantized to the tick. `observations` must
/// already be sorted by spread.
fn quantile(observations: &[(f64, f64)], weight: f64, quantile: f64) -> Option<f64> {
    if weight <= 0.0 {
        return None;
    }
    let target = weight * quantile;
    let mut cumulative = 0.0;
    for (spread_basis_points, seconds) in observations {
        cumulative += seconds;
        if cumulative >= target {
            return Some(*spread_basis_points);
        }
    }
    // Reachable only through floating-point drift in the running sum, since the last observation's
    // cumulative weight is `weight` itself and the target never exceeds it.
    observations.last().map(|(spread, _)| *spread)
}

/// Accumulates one name's session into a five-minute grid and the session that grid sums to.
///
/// Fed tick by tick and never holding them: the ticks a session's fetch delivers run to most of a
/// million, and what survives is one summary per bucket.
pub struct SessionFold {
    ticker: Ticker,
    session: SessionDate,
    open: DateTime<Utc>,
    close: DateTime<Utc>,
    buckets: Vec<SpreadAccumulator>,
    previous: Option<QuoteTick>,
    out_of_order: usize,
}

impl SessionFold {
    /// Opens a fold over `[open, close)`, which must be a positive interval.
    ///
    /// The bounds are the session's own trading hours, so an early close is 3.5 hours of buckets
    /// rather than 6.5 — folding to a fixed 16:00 would weigh three hours of post-close book into
    /// a figure that is supposed to describe the session.
    pub fn new(
        ticker: Ticker,
        session: SessionDate,
        open: DateTime<Utc>,
        close: DateTime<Utc>,
    ) -> Option<Self> {
        let span = (close - open).num_milliseconds();
        if span <= 0 {
            return None;
        }
        // Rounded up, so a session whose close does not land on the grid keeps its short last
        // bucket rather than dropping the minutes past the final boundary.
        let bucket_milliseconds = BUCKET_SECONDS * 1_000;
        let buckets = ((span + bucket_milliseconds - 1) / bucket_milliseconds) as usize;
        Some(Self {
            ticker,
            session,
            open,
            close,
            buckets: (0..buckets).map(|_| SpreadAccumulator::default()).collect(),
            previous: None,
            out_of_order: 0,
        })
    }

    /// Accepts the next tick, which closes out the interval the previous one prevailed for.
    ///
    /// A tick older than the one before it is counted and dropped rather than weighed: the fold
    /// measures forward from each quote to the next, so a backwards step would subtract time.
    pub fn push(&mut self, tick: QuoteTick) {
        if let Some(previous) = self.previous {
            if tick.timestamp() < previous.timestamp() {
                self.out_of_order += 1;
                return;
            }
            self.weigh(&previous, tick.timestamp());
        }
        if let Some(index) = self.bucket_index(tick.timestamp()) {
            self.buckets[index].quote_count += 1;
        }
        self.previous = Some(tick);
    }

    /// Closes the fold, returning the five-minute summaries followed by the session's.
    ///
    /// The session summary is the merge of the buckets rather than a separate accumulation, so the
    /// two cadences describe the same weight by construction.
    pub fn finish(mut self) -> Vec<QuoteSummary> {
        if let Some(previous) = self.previous.take() {
            // The standing quote prevails to the close; nothing else marks the end of its interval.
            self.weigh(&previous, self.close);
        }
        if self.out_of_order > 0 {
            warn!(
                ticker = %self.ticker,
                session = %self.session,
                out_of_order = self.out_of_order,
                "Dropped quotes that arrived older than the one before them"
            );
        }

        let mut session = SpreadAccumulator::default();
        let mut summaries = Vec::with_capacity(self.buckets.len() + 1);
        for (index, mut bucket) in std::mem::take(&mut self.buckets).into_iter().enumerate() {
            let opens_at = self.open + Duration::seconds(BUCKET_SECONDS * index as i64);
            if let Some(summary) = bucket.summarize(&self.ticker, BarInterval::FiveMinute, opens_at)
            {
                summaries.push(summary);
            }
            session.absorb(bucket);
        }
        summaries.extend(session.summarize(
            &self.ticker,
            BarInterval::OneDay,
            daily_stamp(self.session),
        ));
        summaries
    }

    /// Weighs `quote` across every bucket its prevailing interval touches.
    ///
    /// Split rather than assigned whole, so a quote standing across a boundary contributes to both
    /// buckets — which is what makes the five-minute rows sum back to the session row.
    fn weigh(&mut self, quote: &QuoteTick, until: DateTime<Utc>) {
        let from = quote.timestamp().max(self.open);
        let until = until.min(self.close);
        if until <= from {
            return;
        }
        let Some(spread_basis_points) = BasisPoints::from_ratio(quote.spread(), quote.mid_price())
        else {
            return;
        };

        let mut cursor = from;
        while cursor < until {
            let Some(index) = self.bucket_index(cursor) else {
                return;
            };
            let boundary =
                (self.open + Duration::seconds(BUCKET_SECONDS * (index as i64 + 1))).min(until);
            // Nanoseconds, not milliseconds: truncating loses a quarter of one per quote, which
            // cost AAPL 211 seconds of a 23,400-second session. One bucket cannot overflow it.
            let seconds = (boundary - cursor)
                .num_nanoseconds()
                .expect("an interval of at most one bucket fits in nanoseconds")
                as f64
                / 1e9;
            self.buckets[index].add(quote, spread_basis_points.value(), seconds);
            cursor = boundary;
        }
    }

    /// Which bucket an instant falls in, or `None` outside the session.
    fn bucket_index(&self, instant: DateTime<Utc>) -> Option<usize> {
        if instant < self.open || instant >= self.close {
            return None;
        }
        let offset = (instant - self.open).num_milliseconds() / (BUCKET_SECONDS * 1_000);
        let index = usize::try_from(offset).ok()?;
        (index < self.buckets.len()).then_some(index)
    }
}

/// The instant the archive stamps a session's daily row at, which is 16:00 Eastern.
///
/// Not the session's own close. An early-close day still carries a 16:00 stamp in the daily bar
/// archive — 2025-11-28 does — so reading the calendar here would put the summary three hours from
/// the bar it is meant to join.
fn daily_stamp(session: SessionDate) -> DateTime<Utc> {
    let local_close = session
        .date()
        .and_hms_opt(16, 0, 0)
        .expect("16:00 is a valid wall-clock time");
    New_York
        .from_local_datetime(&local_close)
        .earliest()
        .map(|zoned| zoned.with_timezone(&Utc))
        .unwrap_or_else(|| local_close.and_utc())
}

/// The UTC instants bounding one session's regular hours, or `None` if it did not trade.
///
/// Read from the published calendar rather than assumed, so an early close bounds at 13:00 and the
/// three hours of post-close book that follow it never enter a session figure.
pub fn trading_hours(
    calendar: &TradingCalendar,
    session: SessionDate,
) -> Option<(DateTime<Utc>, DateTime<Utc>)> {
    let day = calendar.session(session)?;
    let eastern = |time| {
        New_York
            .from_local_datetime(&session.date().and_time(time))
            .earliest()
            .map(|zoned| zoned.with_timezone(&Utc))
    };
    Some((eastern(day.session_open())?, eastern(day.session_close())?))
}

/// Folds one name's session, holding the ticks only long enough to weigh each one.
///
/// The pair it returns is the summaries and what they cost: the tick count is the only trace the
/// fetch leaves, and it is what decides whether a wider backfill is affordable.
pub async fn fold_session(
    market_data: &MarketDataClient,
    ticker: &Ticker,
    session: SessionDate,
    open: DateTime<Utc>,
    close: DateTime<Utc>,
) -> Result<(Vec<QuoteSummary>, QuoteFetch), ClientError> {
    let Some(mut fold) = SessionFold::new(ticker.clone(), session, open, close) else {
        return Err(ClientError::Parse(format!(
            "{session} spans no time between {open} and {close}"
        )));
    };
    let fetch = market_data
        .fetch_quotes(ticker, open, close, session.date(), |tick| fold.push(tick))
        .await?;
    Ok((fold.finish(), fetch))
}

/// Builds the canonical quote frame, in [`QUOTE_FRAME_COLUMNS`] order.
pub fn summaries_to_dataframe(summaries: &[QuoteSummary]) -> Result<DataFrame, PolarsError> {
    let mut tickers: Vec<String> = Vec::with_capacity(summaries.len());
    let mut intervals: Vec<String> = Vec::with_capacity(summaries.len());
    let mut timestamps: Vec<i64> = Vec::with_capacity(summaries.len());
    let mut spread_means: Vec<f64> = Vec::with_capacity(summaries.len());
    let mut basis_point_means: Vec<f64> = Vec::with_capacity(summaries.len());
    let mut medians: Vec<f64> = Vec::with_capacity(summaries.len());
    let mut ninetieths: Vec<f64> = Vec::with_capacity(summaries.len());
    let mut bid_sizes: Vec<f64> = Vec::with_capacity(summaries.len());
    let mut ask_sizes: Vec<f64> = Vec::with_capacity(summaries.len());
    let mut counts: Vec<i64> = Vec::with_capacity(summaries.len());
    let mut covered: Vec<f64> = Vec::with_capacity(summaries.len());

    for summary in summaries {
        tickers.push(summary.ticker().to_string());
        intervals.push(summary.bar_interval().as_str().to_string());
        timestamps.push(summary.timestamp().timestamp_millis());
        spread_means.push(summary.quoted_spread_mean());
        basis_point_means.push(summary.quoted_spread_basis_points_mean().value());
        medians.push(summary.quoted_spread_basis_points_median().value());
        ninetieths.push(
            summary
                .quoted_spread_basis_points_ninetieth_percentile()
                .value(),
        );
        bid_sizes.push(summary.bid_size_mean());
        ask_sizes.push(summary.ask_size_mean());
        counts.push(summary.quote_count());
        covered.push(summary.covered_seconds());
    }

    DataFrame::new(vec![
        Column::new("ticker".into(), tickers),
        Column::new("bar_interval".into(), intervals),
        Column::new("timestamp".into(), timestamps),
        Column::new("quoted_spread_mean".into(), spread_means),
        Column::new("quoted_spread_basis_points_mean".into(), basis_point_means),
        Column::new("quoted_spread_basis_points_median".into(), medians),
        Column::new(
            "quoted_spread_basis_points_ninetieth_percentile".into(),
            ninetieths,
        ),
        Column::new("bid_size_mean".into(), bid_sizes),
        Column::new("ask_size_mean".into(), ask_sizes),
        Column::new("quote_count".into(), counts),
        Column::new("covered_seconds".into(), covered),
    ])
}

#[cfg(test)]
mod tests {
    use super::*;

    use chrono::NaiveDate;

    /// Spreads are ratios of decimal prices no binary float represents exactly, so the fold's
    /// arithmetic lands within a rounding error of the literal rather than on it.
    fn assert_close(actual: f64, expected: f64) {
        assert!(
            (actual - expected).abs() < 1e-9,
            "expected {expected}, got {actual}"
        );
    }

    fn ticker() -> Ticker {
        Ticker::new("AAPL").expect("AAPL is a valid ticker")
    }

    fn session() -> SessionDate {
        SessionDate::from_date(NaiveDate::from_ymd_opt(2026, 8, 20).expect("a real date"))
    }

    fn at(minute: u32, second: u32) -> DateTime<Utc> {
        session()
            .date()
            .and_hms_opt(13, minute, second)
            .expect("a valid wall clock")
            .and_utc()
    }

    fn tick(minute: u32, bid: f64, ask: f64, bid_size: i32, ask_size: i32) -> QuoteTick {
        QuoteTick::new(at(minute, 0), bid, ask, bid_size, ask_size).expect("a usable book")
    }

    /// Two quotes over two buckets: a ten-basis-point book standing from 13:30 to 13:36, then a
    /// two-basis-point book standing to the 13:40 close.
    fn folded() -> Vec<QuoteSummary> {
        let mut fold = SessionFold::new(ticker(), session(), at(30, 0), at(40, 0))
            .expect("a positive session");
        fold.push(tick(30, 99.95, 100.05, 100, 200));
        fold.push(tick(36, 99.99, 100.01, 300, 400));
        fold.finish()
    }

    #[test]
    fn test_a_quote_standing_across_a_boundary_is_split_between_both_buckets() {
        let summaries = folded();
        assert_eq!(summaries.len(), 3, "two buckets and the session");

        // The 13:30 book prevailed 360 seconds, 300 of them in the first bucket and 60 in the
        // second. Assigning it whole to either one would leave a bucket at 600 or 0 seconds.
        assert_eq!(summaries[0].covered_seconds(), 300.0);
        assert_eq!(summaries[1].covered_seconds(), 300.0);
        assert_close(summaries[0].quoted_spread_basis_points_mean().value(), 10.0);
        assert_close(summaries[1].quoted_spread_basis_points_mean().value(), 3.6);
    }

    /// The property that makes two cadences safe to publish side by side: they describe the same
    /// weight, because the session figure is the merge of the buckets rather than a second pass.
    #[test]
    fn test_the_five_minute_rows_sum_back_to_the_session_row() {
        let summaries = folded();
        let session_summary = summaries.last().expect("a session summary");
        assert_eq!(session_summary.bar_interval(), BarInterval::OneDay);

        let buckets = &summaries[..summaries.len() - 1];
        let covered: f64 = buckets.iter().map(QuoteSummary::covered_seconds).sum();
        assert_eq!(covered, session_summary.covered_seconds());
        assert_eq!(covered, 600.0);

        let weighted: f64 = buckets
            .iter()
            .map(|bucket| {
                bucket.quoted_spread_basis_points_mean().value() * bucket.covered_seconds()
            })
            .sum();
        assert_close(weighted / covered, 6.8);
        assert_close(
            session_summary.quoted_spread_basis_points_mean().value(),
            6.8,
        );
    }

    /// Weighting by count instead would read 6.0 here. It reads 6.0 on AAPL's open too, against a
    /// time-weighted 3.44 — the count is a message rate, not a market.
    #[test]
    fn test_the_mean_is_weighted_by_time_rather_than_by_update_count() {
        let session_summary = folded().pop().expect("a session summary");
        assert_close(
            session_summary.quoted_spread_basis_points_mean().value(),
            6.8,
        );
        assert!(
            (session_summary.quoted_spread_basis_points_mean().value() - 6.0).abs() > 0.5,
            "the unweighted mean of 10bp and 2bp is 6.0"
        );
        assert_close(session_summary.quoted_spread_mean(), 0.068);
        assert_close(session_summary.bid_size_mean(), 180.0);
        assert_close(session_summary.ask_size_mean(), 280.0);
    }

    /// Read off prevailing time, not off the update stream: the two-basis-point book held 240 of
    /// the second bucket's 300 seconds, so it is the median even though it is one quote of two.
    #[test]
    fn test_the_quantiles_are_weighted_by_time_too() {
        let summaries = folded();
        assert_close(
            summaries[1].quoted_spread_basis_points_median().value(),
            2.0,
        );
        assert_close(
            summaries[1]
                .quoted_spread_basis_points_ninetieth_percentile()
                .value(),
            10.0,
        );
    }

    /// Nothing else marks the end of the last quote's interval, so without this the session loses
    /// however long the final book stood — four minutes of the ten here.
    #[test]
    fn test_the_last_quote_prevails_to_the_close() {
        let mut fold = SessionFold::new(ticker(), session(), at(30, 0), at(40, 0))
            .expect("a positive session");
        fold.push(tick(30, 99.95, 100.05, 100, 200));
        let session_summary = fold.finish().pop().expect("a session summary");
        assert_eq!(session_summary.covered_seconds(), 600.0);
        assert_eq!(session_summary.quote_count(), 1);
    }

    /// The fold measures forward from each quote to the next, so a backwards step would subtract
    /// time from the bucket it landed in.
    #[test]
    fn test_a_tick_older_than_the_one_before_it_is_dropped() {
        let mut fold = SessionFold::new(ticker(), session(), at(30, 0), at(40, 0))
            .expect("a positive session");
        fold.push(tick(30, 99.95, 100.05, 100, 200));
        fold.push(tick(36, 99.99, 100.01, 300, 400));
        fold.push(tick(33, 99.00, 101.00, 100, 100));
        let summaries = fold.finish();

        let session_summary = summaries.last().expect("a session summary");
        assert_eq!(session_summary.quote_count(), 2, "the third is not counted");
        assert_eq!(session_summary.covered_seconds(), 600.0);
        assert_close(
            session_summary.quoted_spread_basis_points_mean().value(),
            6.8,
        );
    }

    /// A name that stops quoting has bars with no book at all, and a zero-weighted row would put a
    /// zero spread into a cost model rather than an absence.
    #[test]
    fn test_a_bucket_no_quote_stood_across_produces_no_row() {
        let mut fold = SessionFold::new(ticker(), session(), at(30, 0), at(45, 0))
            .expect("a positive session");
        fold.push(tick(40, 99.95, 100.05, 100, 200));
        let summaries = fold.finish();

        // Three buckets exist; only the one holding the quote and the session are summarized.
        assert_eq!(summaries.len(), 2);
        assert_eq!(summaries[0].timestamp(), at(40, 0));
        assert_eq!(summaries[0].covered_seconds(), 300.0);
    }

    /// A quote arriving before the fold opens still prevails into it, but only the part inside the
    /// session is weighed — otherwise a pre-open book would stretch the denominator.
    #[test]
    fn test_weight_outside_the_session_bounds_is_not_counted() {
        let mut fold = SessionFold::new(ticker(), session(), at(30, 0), at(40, 0))
            .expect("a positive session");
        fold.push(QuoteTick::new(at(20, 0), 99.95, 100.05, 100, 200).expect("a usable book"));
        let session_summary = fold.finish().pop().expect("a session summary");
        assert_eq!(session_summary.covered_seconds(), 600.0);
        assert_eq!(
            session_summary.quote_count(),
            0,
            "it arrived before the session opened"
        );
    }

    /// Found against real data, not in review. Truncating each interval to a whole millisecond
    /// cost AAPL 211 seconds of a 23,400-second session on 2026-08-20 — a quarter of a millisecond
    /// per quote, 846,000 times — while every mean it fed still looked entirely plausible.
    #[test]
    fn test_sub_millisecond_intervals_are_weighed_rather_than_truncated() {
        let opens = at(30, 0);
        let mut fold = SessionFold::new(
            ticker(),
            session(),
            opens,
            opens + Duration::milliseconds(1),
        )
        .expect("a positive session");
        fold.push(QuoteTick::new(opens, 99.95, 100.05, 100, 200).expect("a usable book"));
        fold.push(
            QuoteTick::new(
                opens + Duration::nanoseconds(500_000),
                99.99,
                100.01,
                100,
                200,
            )
            .expect("a usable book"),
        );

        let session_summary = fold
            .finish()
            .pop()
            .expect("half a millisecond of book is still book");
        assert_close(session_summary.covered_seconds(), 0.001);
        assert_close(
            session_summary.quoted_spread_basis_points_mean().value(),
            6.0,
        );
    }

    #[test]
    fn test_a_session_with_no_span_is_refused() {
        assert!(SessionFold::new(ticker(), session(), at(40, 0), at(40, 0)).is_none());
        assert!(SessionFold::new(ticker(), session(), at(40, 0), at(30, 0)).is_none());
    }

    /// Pinned to the stamp the daily bar archive actually carries, which 2025-11-28 shows is 16:00
    /// Eastern even though that session closed at 13:00.
    #[test]
    fn test_the_session_row_is_stamped_where_the_daily_bar_is() {
        let early_close =
            SessionDate::from_date(NaiveDate::from_ymd_opt(2025, 11, 28).expect("a real date"));
        assert_eq!(
            daily_stamp(early_close).to_rfc3339(),
            "2025-11-28T21:00:00+00:00"
        );
        assert_eq!(
            daily_stamp(session()).to_rfc3339(),
            "2026-08-20T20:00:00+00:00"
        );
    }

    #[test]
    fn test_the_frame_column_set_and_order_is_fixed() {
        let frame = summaries_to_dataframe(&folded()).expect("a frame");
        let names: Vec<&str> = frame.get_column_names_str();
        let expected: Vec<&str> = QUOTE_FRAME_COLUMNS.iter().map(|(name, _)| *name).collect();
        assert_eq!(names, expected);
        assert_eq!(frame.height(), 3);
    }
}
