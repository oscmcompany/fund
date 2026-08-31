//! The printed tape folded on ingest: trades in, one row per bar out.
//!
//! Eligibility is decided here, by [`crate::data::conditions`], because the fold cannot be undone.

use std::collections::{BTreeSet, HashMap};

use chrono::{DateTime, Duration, TimeZone, Utc};
use chrono_tz::America::New_York;
use polars::prelude::*;
use tracing::warn;

use crate::common::alpaca::TradeTick;
use crate::common::flatfiles::TradeSink;
use crate::common::types::{
    BarInterval, IntradayCadence, SessionDate, Ticker, TradeExclusions, TradeSummary,
};
use crate::data::conditions::{carries_a_market_price, volume_eligibility, Eligibility};

/// The upper quantile every summary reports beside its median.
const UPPER_QUANTILE: f64 = 0.9;

/// The finest grid the fold accumulates on; the coarser cadences are merges of it.
///
/// Accumulating once and merging upward is what makes the three cadences agree by construction
/// rather than by a second pass that could disagree.
const BASE_CADENCE: IntradayCadence = IntradayCadence::OneMinute;

/// The column set and order every trade partition is written in.
pub const TRADE_FRAME_COLUMNS: [(&str, DataType); 17] = [
    ("ticker", DataType::String),
    ("bar_interval", DataType::String),
    ("timestamp", DataType::Int64),
    ("trade_count", DataType::Int64),
    ("volume", DataType::Float64),
    ("dollar_volume", DataType::Float64),
    ("volume_weighted_average_price", DataType::Float64),
    ("median_trade_size", DataType::Float64),
    ("ninetieth_percentile_trade_size", DataType::Float64),
    ("signed_volume", DataType::Float64),
    ("volume_ineligible_trades", DataType::Int64),
    ("volume_ineligible_dollar_volume", DataType::Float64),
    ("corrected_trades", DataType::Int64),
    ("corrected_dollar_volume", DataType::Float64),
    ("non_market_price_trades", DataType::Int64),
    ("non_market_price_dollar_volume", DataType::Float64),
    ("unresolved_condition_trades", DataType::Int64),
];

/// One bar's worth of printed tape, counting only what the policy admits.
#[derive(Debug, Default)]
struct TradeAccumulator {
    trade_count: i64,
    volume: f64,
    dollar_volume: f64,
    signed_volume: f64,
    /// Every eligible print's size, which the quantiles are read off. Held rather than summarised
    /// because a five-minute median is not derivable from five one-minute medians.
    sizes: Vec<f64>,
    exclusions: TradeExclusions,
}

impl TradeAccumulator {
    /// Records an eligible print, signed by `direction`.
    fn add(&mut self, tick: &TradeTick, direction: f64) {
        self.trade_count += 1;
        self.volume += tick.size();
        self.dollar_volume += tick.notional();
        self.signed_volume += direction * tick.size();
        self.sizes.push(tick.size());
    }

    /// Takes everything `other` accumulated, consuming it.
    ///
    /// By value rather than by `&mut`, so a bar cannot be absorbed twice and double-count itself.
    fn absorb(&mut self, mut other: Self) {
        self.trade_count += other.trade_count;
        self.volume += other.volume;
        self.dollar_volume += other.dollar_volume;
        self.signed_volume += other.signed_volume;
        self.sizes.append(&mut other.sizes);
        self.exclusions.absorb(other.exclusions);
    }

    /// Reduces the bar to a summary, or `None` when nothing eligible traded in it.
    ///
    /// Sorts in place, so the sizes are left ordered for whoever absorbs them.
    fn summarize(
        &mut self,
        ticker: &Ticker,
        bar_interval: BarInterval,
        timestamp: DateTime<Utc>,
    ) -> Option<TradeSummary> {
        if self.trade_count == 0 || self.volume <= 0.0 {
            return None;
        }
        self.sizes.sort_unstable_by(f64::total_cmp);

        let summary = TradeSummary::new(
            ticker.clone(),
            bar_interval,
            timestamp,
            self.trade_count,
            self.volume,
            self.dollar_volume,
            quantile(&self.sizes, 0.5)?,
            quantile(&self.sizes, UPPER_QUANTILE)?,
            self.signed_volume,
            self.exclusions,
        );
        match summary {
            Ok(summary) => Some(summary),
            // A fold that cannot make a coherent summary is this module's bug, not the tape's, and
            // dropping the bar keeps one arithmetic slip from poisoning a whole session's archive.
            Err(error) => {
                warn!(%ticker, %bar_interval, %timestamp, %error, "Discarded an incoherent trade fold");
                None
            }
        }
    }
}

/// The size at or below which `quantile` of the bar's prints fell.
///
/// No interpolation: the reported figure is a size that actually printed. `sizes` must be sorted.
fn quantile(sizes: &[f64], quantile: f64) -> Option<f64> {
    if sizes.is_empty() {
        return None;
    }
    let index = ((sizes.len() as f64 - 1.0) * quantile).round() as usize;
    sizes.get(index).copied()
}

/// Accumulates one name's session onto a one-minute grid and the coarser bars it merges into.
///
/// Fed print by print and never holding them beyond their size, which the quantiles need.
pub struct SessionFold {
    ticker: Ticker,
    session: SessionDate,
    open: DateTime<Utc>,
    close: DateTime<Utc>,
    buckets: Vec<TradeAccumulator>,
    /// The last price that moved, which the tick rule signs against.
    previous_price: Option<f64>,
    /// The direction an unchanged price inherits, per the tick rule's zero-tick convention.
    previous_direction: f64,
    out_of_order: usize,
}

impl SessionFold {
    /// Opens a fold over `[open, close)`, which must be a positive interval.
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
        let bucket_milliseconds = BASE_CADENCE.seconds() * 1_000;
        let buckets = ((span + bucket_milliseconds - 1) / bucket_milliseconds) as usize;
        Some(Self {
            ticker,
            session,
            open,
            close,
            buckets: (0..buckets).map(|_| TradeAccumulator::default()).collect(),
            previous_price: None,
            previous_direction: 1.0,
            out_of_order: 0,
        })
    }

    /// Accepts the next print, applying the eligibility policy before it can weigh anything.
    ///
    /// Exclusions are recorded against the bucket the print fell in, so a bar says what it dropped
    /// even when it admits nothing — a session whose only prints were the auction is visible.
    pub fn push(&mut self, tick: TradeTick) {
        let Some(index) = self.bucket_index(tick.timestamp()) else {
            return;
        };
        // Signed before any exclusion test: the tick rule reads the tape as printed, and skipping
        // an ineligible print would make the next one's direction depend on the policy.
        let direction = self.direction_of(&tick);

        let bucket = &mut self.buckets[index];
        if tick.corrected() {
            bucket.exclusions.corrected_trades += 1;
            bucket.exclusions.corrected_dollar_volume += tick.notional();
            return;
        }
        match volume_eligibility(tick.conditions()) {
            Eligibility::Ineligible => {
                bucket.exclusions.volume_ineligible_trades += 1;
                bucket.exclusions.volume_ineligible_dollar_volume += tick.notional();
                return;
            }
            // Counted and still folded: an unknown code is likelier to be a namespace this table
            // does not cover than a volume rule the provider forgot to publish.
            Eligibility::Ambiguous => bucket.exclusions.unresolved_condition_trades += 1,
            Eligibility::Eligible => {}
        }
        if !carries_a_market_price(tick.conditions()) {
            bucket.exclusions.non_market_price_trades += 1;
            bucket.exclusions.non_market_price_dollar_volume += tick.notional();
        }
        bucket.add(&tick, direction);
    }

    /// The tick rule's verdict on this print, updating the state it carries forward.
    ///
    /// An unchanged price inherits the last direction rather than counting as neither, which is what
    /// makes signed volume sum to something other than noise on a name that prints flat.
    fn direction_of(&mut self, tick: &TradeTick) -> f64 {
        let direction = match self.previous_price {
            Some(previous) if tick.price() > previous => 1.0,
            Some(previous) if tick.price() < previous => -1.0,
            Some(_) => self.previous_direction,
            // The session's first print has nothing to compare against; treated as a buy by
            // convention, and one print cannot move a bar of any size.
            None => 1.0,
        };
        self.previous_price = Some(tick.price());
        self.previous_direction = direction;
        direction
    }

    /// Closes the fold, returning one-minute, five-minute and session rows in that order.
    ///
    /// The coarser bars are merges of the finer ones rather than separate accumulations, so all
    /// three describe the same tape by construction.
    pub fn finish(mut self) -> Vec<TradeSummary> {
        if self.out_of_order > 0 {
            warn!(
                ticker = %self.ticker,
                session = %self.session,
                out_of_order = self.out_of_order,
                "Dropped trades that arrived older than the one before them"
            );
        }

        let minutes = std::mem::take(&mut self.buckets);
        let per_five_minutes =
            (IntradayCadence::FiveMinute.seconds() / BASE_CADENCE.seconds()).max(1) as usize;

        let mut summaries =
            Vec::with_capacity(minutes.len() + minutes.len() / per_five_minutes + 1);
        let mut session = TradeAccumulator::default();
        let mut five_minute = TradeAccumulator::default();
        let mut five_minute_index = 0usize;

        for (index, mut minute) in minutes.into_iter().enumerate() {
            let opens_at = self.open + Duration::seconds(BASE_CADENCE.seconds() * index as i64);
            if let Some(summary) = minute.summarize(&self.ticker, BarInterval::OneMinute, opens_at)
            {
                summaries.push(summary);
            }
            five_minute.absorb(minute);

            let closes_the_five = (index + 1) % per_five_minutes == 0;
            if closes_the_five {
                let opens_at = self.open
                    + Duration::seconds(
                        IntradayCadence::FiveMinute.seconds() * five_minute_index as i64,
                    );
                let mut bar = std::mem::take(&mut five_minute);
                if let Some(summary) =
                    bar.summarize(&self.ticker, BarInterval::FiveMinute, opens_at)
                {
                    summaries.push(summary);
                }
                session.absorb(bar);
                five_minute_index += 1;
            }
        }
        // A session whose length is not a multiple of five minutes leaves a short last bar, which is
        // emitted rather than dropped — an early close is 3.5 hours and would otherwise lose its tail.
        if five_minute.trade_count > 0 || five_minute.exclusions != TradeExclusions::default() {
            let opens_at = self.open
                + Duration::seconds(
                    IntradayCadence::FiveMinute.seconds() * five_minute_index as i64,
                );
            if let Some(summary) =
                five_minute.summarize(&self.ticker, BarInterval::FiveMinute, opens_at)
            {
                summaries.push(summary);
            }
            session.absorb(five_minute);
        }

        summaries.extend(session.summarize(
            &self.ticker,
            BarInterval::OneDay,
            daily_stamp(self.session),
        ));
        summaries
    }

    /// Which bucket an instant falls in, or `None` outside the session.
    fn bucket_index(&self, instant: DateTime<Utc>) -> Option<usize> {
        if instant < self.open || instant >= self.close {
            return None;
        }
        let offset = (instant - self.open).num_milliseconds() / (BASE_CADENCE.seconds() * 1_000);
        let index = usize::try_from(offset).ok()?;
        (index < self.buckets.len()).then_some(index)
    }
}

/// The instant the archive stamps a session's daily row at, which is 16:00 Eastern.
///
/// Matches [`crate::data::quotes`] deliberately: a trade row and a quote row for the same session
/// must join, and an early close still carries a 16:00 stamp in the daily bar archive.
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

/// Folds a whole session out of one ticker-major flat file, resuming a name that comes back.
///
/// The same shape as [`crate::data::quotes::MarketFold`] and for the same reason: two names a
/// session arrive in more than one run, and a released fold cannot take the second.
pub struct MarketFold {
    session: SessionDate,
    open: DateTime<Utc>,
    close: DateTime<Utc>,
    current: Option<(Ticker, SessionFold)>,
    parked: HashMap<Ticker, SessionFold>,
    resumed: BTreeSet<Ticker>,
    universe: BTreeSet<Ticker>,
    folded: usize,
}

impl MarketFold {
    /// Opens a fold over one session's regular hours.
    pub fn new(
        session: SessionDate,
        open: DateTime<Utc>,
        close: DateTime<Utc>,
        universe: BTreeSet<Ticker>,
    ) -> Self {
        Self {
            session,
            open,
            close,
            current: None,
            parked: HashMap::new(),
            resumed: BTreeSet::new(),
            universe,
            folded: 0,
        }
    }

    /// Prints folded, which is this pass's cost.
    pub fn folded(&self) -> usize {
        self.folded
    }

    /// Accepts the next row of the file, which carries its own ticker.
    pub fn push(&mut self, ticker: Ticker, tick: TradeTick) {
        if !self.universe.contains(&ticker) {
            return;
        }
        self.folded += 1;
        let same = matches!(&self.current, Some((open, _)) if *open == ticker);
        if !same {
            self.rotate(ticker);
        }
        if let Some((_, fold)) = self.current.as_mut() {
            fold.push(tick);
        }
    }

    /// Parks the run in progress and takes up `ticker`, resuming its fold if it already has one.
    fn rotate(&mut self, ticker: Ticker) {
        if let Some((held, fold)) = self.current.take() {
            self.parked.insert(held, fold);
        }
        let fold = match self.parked.remove(&ticker) {
            Some(fold) => {
                self.resumed.insert(ticker.clone());
                Some(fold)
            }
            None => SessionFold::new(ticker.clone(), self.session, self.open, self.close),
        };
        self.current = fold.map(|fold| (ticker, fold));
    }

    /// Closes every fold and reports what the session yielded.
    pub fn finish(mut self) -> SessionFolded {
        if let Some((held, fold)) = self.current.take() {
            self.parked.insert(held, fold);
        }
        if !self.resumed.is_empty() {
            warn!(
                session = %self.session,
                names = %self
                    .resumed
                    .iter()
                    .map(|ticker| ticker.as_str())
                    .collect::<Vec<_>>()
                    .join(","),
                "Resumed names whose trades arrived in more than one run"
            );
        }
        let parked = std::mem::take(&mut self.parked);
        let seen = parked.len();
        let summaries = parked.into_values().flat_map(SessionFold::finish).collect();
        SessionFolded {
            summaries,
            resumed: self.resumed,
            seen,
        }
    }
}

/// What one session's trade file yielded.
pub struct SessionFolded {
    pub summaries: Vec<TradeSummary>,
    pub resumed: BTreeSet<Ticker>,
    pub seen: usize,
}

impl TradeSink for MarketFold {
    fn push(&mut self, ticker: Ticker, tick: TradeTick) {
        MarketFold::push(self, ticker, tick)
    }
}

/// Builds the canonical trade frame, in [`TRADE_FRAME_COLUMNS`] order.
pub fn summaries_to_dataframe(summaries: &[TradeSummary]) -> Result<DataFrame, PolarsError> {
    let column = |name: &str, values: Vec<f64>| Column::new(name.into(), values);
    let counted = |name: &str, values: Vec<i64>| Column::new(name.into(), values);

    DataFrame::new(vec![
        Column::new(
            "ticker".into(),
            summaries
                .iter()
                .map(|row| row.ticker().to_string())
                .collect::<Vec<_>>(),
        ),
        Column::new(
            "bar_interval".into(),
            summaries
                .iter()
                .map(|row| row.bar_interval().as_str().to_string())
                .collect::<Vec<_>>(),
        ),
        counted(
            "timestamp",
            summaries
                .iter()
                .map(|row| row.timestamp().timestamp_millis())
                .collect(),
        ),
        counted(
            "trade_count",
            summaries.iter().map(TradeSummary::trade_count).collect(),
        ),
        column(
            "volume",
            summaries.iter().map(TradeSummary::volume).collect(),
        ),
        column(
            "dollar_volume",
            summaries.iter().map(TradeSummary::dollar_volume).collect(),
        ),
        column(
            "volume_weighted_average_price",
            summaries
                .iter()
                .map(TradeSummary::volume_weighted_average_price)
                .collect(),
        ),
        column(
            "median_trade_size",
            summaries
                .iter()
                .map(TradeSummary::median_trade_size)
                .collect(),
        ),
        column(
            "ninetieth_percentile_trade_size",
            summaries
                .iter()
                .map(TradeSummary::ninetieth_percentile_trade_size)
                .collect(),
        ),
        column(
            "signed_volume",
            summaries.iter().map(TradeSummary::signed_volume).collect(),
        ),
        counted(
            "volume_ineligible_trades",
            summaries
                .iter()
                .map(|row| row.exclusions().volume_ineligible_trades)
                .collect(),
        ),
        column(
            "volume_ineligible_dollar_volume",
            summaries
                .iter()
                .map(|row| row.exclusions().volume_ineligible_dollar_volume)
                .collect(),
        ),
        counted(
            "corrected_trades",
            summaries
                .iter()
                .map(|row| row.exclusions().corrected_trades)
                .collect(),
        ),
        column(
            "corrected_dollar_volume",
            summaries
                .iter()
                .map(|row| row.exclusions().corrected_dollar_volume)
                .collect(),
        ),
        counted(
            "non_market_price_trades",
            summaries
                .iter()
                .map(|row| row.exclusions().non_market_price_trades)
                .collect(),
        ),
        column(
            "non_market_price_dollar_volume",
            summaries
                .iter()
                .map(|row| row.exclusions().non_market_price_dollar_volume)
                .collect(),
        ),
        counted(
            "unresolved_condition_trades",
            summaries
                .iter()
                .map(|row| row.exclusions().unresolved_condition_trades)
                .collect(),
        ),
    ])
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveDate;

    fn ticker() -> Ticker {
        Ticker::new("AAPL").expect("AAPL is a valid ticker")
    }

    fn session() -> SessionDate {
        SessionDate::from_date(NaiveDate::from_ymd_opt(2026, 8, 20).expect("a real date"))
    }

    /// 13:30 UTC through 13:40, ten minutes: two five-minute bars over ten one-minute ones.
    fn at(minute: u32, second: u32) -> DateTime<Utc> {
        session()
            .date()
            .and_hms_opt(13, minute, second)
            .expect("a valid wall clock")
            .and_utc()
    }

    fn trade(minute: u32, price: f64, size: f64) -> TradeTick {
        TradeTick::new(at(minute, 0), price, size, Vec::new(), false).expect("a usable print")
    }

    fn marked(
        minute: u32,
        price: f64,
        size: f64,
        conditions: Vec<u32>,
        corrected: bool,
    ) -> TradeTick {
        TradeTick::new(at(minute, 0), price, size, conditions, corrected).expect("a usable print")
    }

    fn fold_of(ticks: Vec<TradeTick>) -> Vec<TradeSummary> {
        let mut fold = SessionFold::new(ticker(), session(), at(30, 0), at(40, 0))
            .expect("a positive session");
        for tick in ticks {
            fold.push(tick);
        }
        fold.finish()
    }

    fn rows(summaries: &[TradeSummary], interval: BarInterval) -> Vec<&TradeSummary> {
        summaries
            .iter()
            .filter(|row| row.bar_interval() == interval)
            .collect()
    }

    /// The property that makes three cadences safe to publish side by side.
    ///
    /// Pinned to literals — 600 shares, $60,150 — rather than to the finer rows' own sum, because an
    /// assertion derived from the thing under test moves with it and can never fail.
    #[test]
    fn test_the_finer_bars_sum_back_to_the_coarser_ones() {
        let summaries = fold_of(vec![
            trade(30, 100.0, 100.0),
            trade(32, 100.5, 200.0),
            trade(36, 100.25, 300.0),
        ]);

        let daily = rows(&summaries, BarInterval::OneDay);
        assert_eq!(daily.len(), 1);
        assert_eq!(daily[0].volume(), 600.0);
        assert_eq!(daily[0].trade_count(), 3);
        assert_eq!(
            daily[0].dollar_volume(),
            100.0 * 100.0 + 100.5 * 200.0 + 100.25 * 300.0
        );

        let minutes = rows(&summaries, BarInterval::OneMinute);
        assert_eq!(minutes.len(), 3, "one bar per minute that printed");
        let minute_volume: f64 = minutes.iter().map(|row| row.volume()).sum();
        assert_eq!(minute_volume, 600.0);

        let fives = rows(&summaries, BarInterval::FiveMinute);
        assert_eq!(fives.len(), 2, "13:30-13:35 and 13:35-13:40");
        let five_volume: f64 = fives.iter().map(|row| row.volume()).sum();
        assert_eq!(five_volume, 600.0);
        assert_eq!(fives[0].volume(), 300.0, "the 13:30 and 13:32 prints");
        assert_eq!(fives[1].volume(), 300.0, "the 13:36 print");
    }

    /// VWAP is the identity it claims, not a separately accumulated mean.
    #[test]
    fn test_the_average_price_is_dollar_volume_over_volume() {
        let summaries = fold_of(vec![trade(30, 100.0, 100.0), trade(31, 200.0, 300.0)]);
        let daily = rows(&summaries, BarInterval::OneDay);
        // 100*100 + 200*300 = 70,000 over 400 shares. A size-blind mean would read 150.
        assert_eq!(daily[0].volume_weighted_average_price(), 175.0);
    }

    /// The auction prints are counted and excluded, and the bar says so.
    ///
    /// Condition 16 is Market Center Official Open, which the provider marks volume-ineligible.
    #[test]
    fn test_a_volume_ineligible_print_is_excluded_and_recorded() {
        let summaries = fold_of(vec![
            marked(30, 100.0, 1_000_000.0, vec![16], false),
            trade(31, 100.0, 100.0),
        ]);
        let daily = rows(&summaries, BarInterval::OneDay);
        assert_eq!(daily[0].volume(), 100.0, "the auction is not in volume");
        assert_eq!(daily[0].trade_count(), 1);
        assert_eq!(daily[0].exclusions().volume_ineligible_trades, 1);
        assert_eq!(
            daily[0].exclusions().volume_ineligible_dollar_volume,
            100_000_000.0
        );
    }

    /// A corrected print is excluded before its conditions are even consulted.
    #[test]
    fn test_a_corrected_print_is_excluded_and_recorded() {
        let summaries = fold_of(vec![
            marked(30, 100.0, 5_000.0, vec![37], true),
            trade(31, 100.0, 100.0),
        ]);
        let daily = rows(&summaries, BarInterval::OneDay);
        assert_eq!(daily[0].volume(), 100.0);
        assert_eq!(daily[0].exclusions().corrected_trades, 1);
        assert_eq!(daily[0].exclusions().corrected_dollar_volume, 500_000.0);
        assert_eq!(daily[0].exclusions().volume_ineligible_trades, 0);
    }

    /// An average-price print counts as volume and is flagged as not a market price.
    ///
    /// Both at once: the provider says it is volume, and the house rule says its price cannot be
    /// differenced against a quote. Recording only one of those would lose the other.
    #[test]
    fn test_a_non_market_price_print_counts_as_volume_and_is_flagged() {
        let summaries = fold_of(vec![marked(30, 100.0, 500.0, vec![2], false)]);
        let daily = rows(&summaries, BarInterval::OneDay);
        assert_eq!(daily[0].volume(), 500.0, "still real volume");
        assert_eq!(daily[0].exclusions().non_market_price_trades, 1);
        assert_eq!(
            daily[0].exclusions().non_market_price_dollar_volume,
            50_000.0
        );
        assert_eq!(daily[0].exclusions().volume_ineligible_trades, 0);
    }

    /// An unknown code is folded and counted, never treated as ordinary in silence.
    #[test]
    fn test_an_unresolved_condition_is_folded_and_counted() {
        let summaries = fold_of(vec![marked(30, 100.0, 200.0, vec![41], false)]);
        let daily = rows(&summaries, BarInterval::OneDay);
        assert_eq!(daily[0].volume(), 200.0, "41 is not a sale condition");
        assert_eq!(daily[0].exclusions().unresolved_condition_trades, 1);
    }

    /// The tick rule signs on price direction, and a flat print inherits the last one.
    #[test]
    fn test_signed_volume_follows_the_tick_rule() {
        // 100 up (first, by convention), 100 up, 100 down, 100 flat inheriting the down.
        let summaries = fold_of(vec![
            trade(30, 100.0, 100.0),
            trade(31, 101.0, 100.0),
            trade(32, 100.0, 100.0),
            trade(33, 100.0, 100.0),
        ]);
        let daily = rows(&summaries, BarInterval::OneDay);
        assert_eq!(daily[0].volume(), 400.0);
        // +100 +100 -100 -100
        assert_eq!(daily[0].signed_volume(), 0.0);
    }

    /// The tick rule reads the tape as printed, not as filtered.
    ///
    /// An excluded print still moves the reference price. Skipping it would make a later trade's
    /// direction depend on the eligibility policy, which is a different measurement.
    #[test]
    fn test_an_excluded_print_still_moves_the_tick_rule() {
        let summaries = fold_of(vec![
            trade(30, 100.0, 100.0),
            marked(31, 200.0, 100.0, vec![16], false),
            trade(32, 150.0, 100.0),
        ]);
        let daily = rows(&summaries, BarInterval::OneDay);
        // The 150 print is a downtick against the excluded 200, not an uptick against the 100.
        assert_eq!(daily[0].signed_volume(), 0.0, "+100 then -100");
        assert_eq!(daily[0].volume(), 200.0);
    }

    /// A print outside regular hours is not folded at all.
    #[test]
    fn test_a_print_outside_the_session_is_not_folded() {
        let mut fold = SessionFold::new(ticker(), session(), at(30, 0), at(40, 0))
            .expect("a positive session");
        fold.push(trade(29, 100.0, 100.0));
        fold.push(trade(45, 100.0, 100.0));
        assert!(fold.finish().is_empty(), "nothing inside the session");
    }

    /// The frame is pinned to its declared column set and order.
    #[test]
    fn test_the_frame_column_set_and_order_is_fixed() {
        let summaries = fold_of(vec![trade(30, 100.0, 100.0)]);
        let frame = summaries_to_dataframe(&summaries).expect("a frame");
        let observed: Vec<(String, DataType)> = frame
            .get_columns()
            .iter()
            .map(|column| (column.name().to_string(), column.dtype().clone()))
            .collect();
        let expected: Vec<(String, DataType)> = TRADE_FRAME_COLUMNS
            .iter()
            .map(|(name, dtype)| (name.to_string(), dtype.clone()))
            .collect();
        assert_eq!(observed, expected);
    }

    /// A session with no span is refused rather than folded into zero buckets.
    #[test]
    fn test_a_session_with_no_span_is_refused() {
        assert!(SessionFold::new(ticker(), session(), at(40, 0), at(40, 0)).is_none());
        assert!(SessionFold::new(ticker(), session(), at(40, 0), at(30, 0)).is_none());
    }
}
