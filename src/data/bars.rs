//! Equity bars: one fetch from Massive, three destinations.
//!
//! The application writes bars to PostgreSQL after the close; the trainer writes the same bars to
//! S3 parquet. Both go through [`fetch_daily_bars`] and [`bars_to_dataframe`] — if the two
//! diverged, the model would train on columns the inference path does not produce and it would
//! surface as bad predictions rather than a build error.
//!
//! Bars come from Massive rather than Alpaca; see [`crate::common::massive`].

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use chrono::{DateTime, Duration, Utc};
use polars::prelude::*;
use sqlx::PgPool;
use tracing::{info, warn};

use crate::common::massive::{MassiveClient, MassiveError};
use crate::common::types::{BarInterval, EquityBar, SessionDate, Ticker};
use crate::data::adjust::{self, SplitTable};
use crate::data::truncate::{self, BoundaryTable};

/// Rows per insert chunk.
///
/// PostgreSQL's bind parameter limit is 65,535; at ten columns a thousand rows uses ten thousand,
/// which leaves ample headroom.
const INSERT_CHUNK_ROWS: usize = 1_000;

/// Trailing window loaded for inference and screening.
///
/// The model's lookback is 70 sessions and the correlation screen uses 60; 70 calendar days is not
/// 70 sessions, so this is deliberately wider than either needs.
pub const HISTORY_LOOKBACK_DAYS: i64 = 120;

/// The bar frame both loaders produce and [`crate::models::tide::predict::consolidate_data`] reads.
///
/// Stated here rather than in the trainer, where the PostgreSQL loader satisfying it could not see
/// it, so the two readers are held to one shape.
pub const BAR_FRAME_COLUMNS: [(&str, DataType); 8] = [
    ("ticker", DataType::String),
    ("timestamp", DataType::Int64),
    ("open_price", DataType::Float64),
    ("high_price", DataType::Float64),
    ("low_price", DataType::Float64),
    ("close_price", DataType::Float64),
    ("volume", DataType::Int64),
    ("volume_weighted_average_price", DataType::Float64),
];

/// Narrows a frame to [`BAR_FRAME_COLUMNS`] so frames written by different versions still combine.
///
/// The archive is two years deep and outlives the schema that wrote any given day of it, and
/// `concat` rejects the whole set when one member differs.
///
/// `strict_cast`, not `cast`: the non-strict form turns an incompatible value into a null, so a
/// partition whose `close_price` arrived as text would be accepted here and have its rows dropped
/// silently much later by `clean_data`, reported as a thin session rather than a bad one.
pub fn project_bar_frame(frame: DataFrame) -> Result<DataFrame, PolarsError> {
    frame
        .lazy()
        .select(
            BAR_FRAME_COLUMNS
                .iter()
                .map(|(name, data_type)| col(*name).strict_cast(data_type.clone()))
                .collect::<Vec<_>>(),
        )
        .collect()
}

/// Errors syncing or reading bars.
#[derive(Debug, thiserror::Error)]
pub enum BarsError {
    #[error("failed to fetch bars from Massive: {0}")]
    Massive(#[from] MassiveError),
    #[error("failed to persist bars: {0}")]
    Database(#[from] sqlx::Error),
    #[error("failed to build a bar frame: {0}")]
    Frame(#[from] PolarsError),
}

/// What a multi-date fetch produced, and which dates it could not.
///
/// The failed dates are carried rather than folded into an error, because a partial fetch is
/// usable: a backfill spanning months should keep the sessions it retrieved. The caller decides
/// whether the gaps matter, and it is the caller that knows whether this run was a nightly
/// three-session top-up or a cold seed.
#[derive(Debug, Default)]
pub struct FetchedBars {
    pub bars: Vec<EquityBar>,
    pub dates_failed: Vec<SessionDate>,
}

/// Fetches whole-market daily bars for each of `dates`.
///
/// One request per date, the grouped endpoint's unit. Non-session dates cost a request and return
/// nothing, so a caller with a calendar should filter first.
///
/// A failed date is recorded and stepped over rather than aborting, which would discard every date
/// already retrieved. The upsert makes re-running a range cheap.
pub async fn fetch_daily_bars(client: &MassiveClient, dates: &[SessionDate]) -> FetchedBars {
    let mut fetched = FetchedBars::default();

    for date in dates {
        match client.fetch_grouped_daily(date.date()).await {
            Ok(bars) => fetched.bars.extend(bars),
            Err(error) => {
                warn!(%date, %error, "Failed to fetch a session's bars, continuing");
                fetched.dates_failed.push(*date);
            }
        }
    }

    info!(
        dates = dates.len(),
        bars = fetched.bars.len(),
        dates_failed = fetched.dates_failed.len(),
        "Daily bars fetched"
    );
    fetched
}

/// Removes repeated `(ticker, bar_interval, timestamp)` rows, keeping the last occurrence.
///
/// A single `INSERT ... ON CONFLICT DO UPDATE` rejects a repeated conflict target with "ON CONFLICT
/// DO UPDATE command cannot affect row a second time", which fails the whole chunk rather than the
/// offending row. Keeping the last occurrence mirrors the upsert's latest-write semantics.
fn deduplicate(bars: &[EquityBar]) -> Vec<EquityBar> {
    let mut seen: HashSet<(Ticker, BarInterval, DateTime<Utc>)> =
        HashSet::with_capacity(bars.len());
    let mut deduplicated: Vec<EquityBar> = Vec::with_capacity(bars.len());
    for bar in bars.iter().rev() {
        if seen.insert((bar.ticker().clone(), bar.bar_interval(), bar.timestamp())) {
            deduplicated.push(bar.clone());
        }
    }
    deduplicated.reverse();
    deduplicated
}

/// Upserts bars into `equity_bars`.
///
/// The conflict target is the full primary key including `bar_interval`, so a daily bar and a
/// one-minute bar for the same ticker and timestamp coexist rather than overwriting each other.
pub async fn store_bars(pool: &PgPool, bars: &[EquityBar]) -> Result<u64, BarsError> {
    if bars.is_empty() {
        return Ok(0);
    }

    let bars = deduplicate(bars);
    let mut rows_affected: u64 = 0;
    let mut transaction = pool.begin().await?;

    for chunk in bars.chunks(INSERT_CHUNK_ROWS) {
        let mut query_builder = sqlx::QueryBuilder::new(
            "INSERT INTO equity_bars (ticker, bar_interval, timestamp, open_price, high_price, \
             low_price, close_price, volume, volume_weighted_average_price, transactions) ",
        );

        query_builder.push_values(chunk, |mut builder, bar| {
            builder
                .push_bind(bar.ticker())
                .push_bind(bar.bar_interval())
                .push_bind(bar.timestamp())
                .push_bind(bar.open_price())
                .push_bind(bar.high_price())
                .push_bind(bar.low_price())
                .push_bind(bar.close_price())
                .push_bind(bar.volume())
                .push_bind(bar.volume_weighted_average_price())
                .push_bind(bar.transactions());
        });

        query_builder.push(
            " ON CONFLICT (ticker, bar_interval, timestamp) DO UPDATE SET \
             open_price = EXCLUDED.open_price, \
             high_price = EXCLUDED.high_price, \
             low_price = EXCLUDED.low_price, \
             close_price = EXCLUDED.close_price, \
             volume = EXCLUDED.volume, \
             volume_weighted_average_price = EXCLUDED.volume_weighted_average_price, \
             transactions = EXCLUDED.transactions, \
             inserted_at = now()",
        );

        rows_affected += query_builder
            .build()
            .execute(&mut *transaction)
            .await?
            .rows_affected();
    }

    transaction.commit().await?;
    info!(rows = rows_affected, "Equity bars stored");
    Ok(rows_affected)
}

/// Builds the canonical bar frame.
///
/// This column set and order is the contract between the trainer's S3 dataset and the application's
/// inference input. `timestamp` is Unix milliseconds because that is what the feature engineering
/// in [`crate::models::tide::data`] expects.
pub fn bars_to_dataframe(bars: &[EquityBar]) -> Result<DataFrame, PolarsError> {
    let mut tickers: Vec<String> = Vec::with_capacity(bars.len());
    let mut intervals: Vec<String> = Vec::with_capacity(bars.len());
    let mut timestamps: Vec<i64> = Vec::with_capacity(bars.len());
    let mut opens: Vec<f64> = Vec::with_capacity(bars.len());
    let mut highs: Vec<f64> = Vec::with_capacity(bars.len());
    let mut lows: Vec<f64> = Vec::with_capacity(bars.len());
    let mut closes: Vec<f64> = Vec::with_capacity(bars.len());
    let mut volumes: Vec<i64> = Vec::with_capacity(bars.len());
    let mut volume_weighted: Vec<Option<f64>> = Vec::with_capacity(bars.len());
    let mut transactions: Vec<Option<i64>> = Vec::with_capacity(bars.len());

    for bar in bars {
        tickers.push(bar.ticker().to_string());
        intervals.push(bar.bar_interval().as_str().to_string());
        timestamps.push(bar.timestamp().timestamp_millis());
        opens.push(bar.open_price());
        highs.push(bar.high_price());
        lows.push(bar.low_price());
        closes.push(bar.close_price());
        volumes.push(bar.volume());
        volume_weighted.push(bar.volume_weighted_average_price());
        transactions.push(bar.transactions());
    }

    DataFrame::new(vec![
        Column::new("ticker".into(), tickers),
        Column::new("bar_interval".into(), intervals),
        Column::new("timestamp".into(), timestamps),
        Column::new("open_price".into(), opens),
        Column::new("high_price".into(), highs),
        Column::new("low_price".into(), lows),
        Column::new("close_price".into(), closes),
        Column::new("volume".into(), volumes),
        Column::new("volume_weighted_average_price".into(), volume_weighted),
        Column::new("transactions".into(), transactions),
    ])
}

/// Loads a trailing window of bars at one interval as a frame, restated onto `as_of`'s share basis.
///
/// The interval filter is not optional. Without it a query would mix daily and intraday rows into
/// one series, and the feature engineering would silently compute returns across incompatible
/// sampling rates.
///
/// Adjustment happens here rather than in the caller because the stored prices are raw, so a loader
/// that returned them unadjusted would hand out a series no caller should be comparing.
pub async fn load_bars_dataframe(
    pool: &PgPool,
    bar_interval: BarInterval,
    lookback_days: i64,
    splits: &SplitTable,
    boundaries: &BoundaryTable,
    as_of: SessionDate,
) -> Result<DataFrame, BarsError> {
    // Anchored to `as_of` rather than the clock. `bounds` is half-open, so the upper comparison is
    // exclusive: its end is the next session's opening instant, which belongs to that session.
    let (_, end) = as_of.bounds();
    let start = as_of.plus_calendar_days(-lookback_days).midnight();

    let rows = sqlx::query!(
        r#"
        SELECT ticker AS "ticker!",
               EXTRACT(EPOCH FROM timestamp)::bigint * 1000 AS "timestamp_ms!",
               open_price AS "open_price!",
               high_price AS "high_price!",
               low_price AS "low_price!",
               close_price AS "close_price!",
               volume AS "volume!",
               volume_weighted_average_price
        FROM equity_bars
        WHERE bar_interval = $1
          AND timestamp >= $2
          AND timestamp < $3
        ORDER BY ticker, timestamp
        "#,
        bar_interval.as_str(),
        start,
        end
    )
    .fetch_all(pool)
    .await?;

    let row_count = rows.len();
    let mut tickers: Vec<String> = Vec::with_capacity(row_count);
    let mut timestamps: Vec<i64> = Vec::with_capacity(row_count);
    let mut opens: Vec<f64> = Vec::with_capacity(row_count);
    let mut highs: Vec<f64> = Vec::with_capacity(row_count);
    let mut lows: Vec<f64> = Vec::with_capacity(row_count);
    let mut closes: Vec<f64> = Vec::with_capacity(row_count);
    let mut volumes: Vec<i64> = Vec::with_capacity(row_count);
    let mut volume_weighted: Vec<Option<f64>> = Vec::with_capacity(row_count);

    for row in rows {
        tickers.push(row.ticker);
        timestamps.push(row.timestamp_ms);
        opens.push(row.open_price);
        highs.push(row.high_price);
        lows.push(row.low_price);
        closes.push(row.close_price);
        volumes.push(row.volume);
        volume_weighted.push(row.volume_weighted_average_price);
    }

    let dataframe = DataFrame::new(vec![
        Column::new("ticker".into(), tickers),
        Column::new("timestamp".into(), timestamps),
        Column::new("open_price".into(), opens),
        Column::new("high_price".into(), highs),
        Column::new("low_price".into(), lows),
        Column::new("close_price".into(), closes),
        Column::new("volume".into(), volumes),
        Column::new("volume_weighted_average_price".into(), volume_weighted),
    ])?;

    // Stitched, then truncated, then folded. The stitch moves a renamed company's bars onto the
    // symbol it trades as now — which are exactly the bars truncation would otherwise drop — and the
    // fold runs last because it keys on the symbol a bar carries after both.
    let dataframe = truncate::stitch_bars(dataframe, boundaries)?;
    let dataframe = truncate::truncate_bars(dataframe, boundaries, as_of)?;
    let dataframe = adjust::adjust_bars(dataframe, &splits.following_renames(boundaries), as_of)?;

    info!(
        rows = dataframe.height(),
        bar_interval = bar_interval.as_str(),
        "Equity bars loaded"
    );
    Ok(dataframe)
}

/// Loads a trailing window of closes per ticker, aligned across tickers by session and restated
/// onto `as_of`'s share basis.
///
/// Every series has the same length and position `i` is the same session across all of them.
/// Tickers missing any session are dropped rather than gap-filled — this is why it is not a
/// `GROUP BY ticker`: two equal-length series covering different dates correlate different days,
/// and nothing downstream carries a sign that it happened.
///
/// Both bounds on `timestamp` are expressed against the column directly rather than wrapped in an
/// expression, so hypertable chunk exclusion applies. The upper one is what keeps the window and
/// `as_of` the same day: without it the most recent sessions in the table are taken whatever
/// `as_of` says, and a replay would load today's closes and restate them onto a past date.
///
/// Adjusting per row while the timestamps are still in hand is why the returned shape needs no
/// dates: it drops them, and a caller adjusting afterwards would have nothing to key the factor on.
pub async fn load_aligned_closes(
    pool: &PgPool,
    bar_interval: BarInterval,
    sessions: usize,
    splits: &SplitTable,
    boundaries: &BoundaryTable,
    as_of: SessionDate,
) -> Result<HashMap<Ticker, Vec<f64>>, BarsError> {
    if sessions == 0 {
        return Ok(HashMap::new());
    }

    // Twice the session count in calendar days covers weekends and holidays with room to spare;
    // a 60-session window spans roughly 84 calendar days. Measured back from `as_of` rather than
    // the clock, so the sessions loaded are the ones the factor restates them onto.
    let (_, latest) = as_of.bounds();
    let earliest = latest - Duration::days(sessions as i64 * 2);

    let rows = sqlx::query!(
        r#"
        WITH recent_sessions AS (
            SELECT DISTINCT timestamp
            FROM equity_bars
            WHERE bar_interval = $1 AND timestamp >= $2 AND timestamp < $4
            ORDER BY timestamp DESC
            LIMIT $3
        )
        SELECT ticker AS "ticker!", timestamp AS "timestamp!", close_price AS "close_price!"
        FROM equity_bars
        WHERE bar_interval = $1
          AND timestamp >= $2
          AND timestamp < $4
          AND timestamp IN (SELECT timestamp FROM recent_sessions)
        ORDER BY ticker, timestamp
        "#,
        bar_interval.as_str(),
        earliest,
        sessions as i64,
        latest,
    )
    .fetch_all(pool)
    .await?;

    let session_count = rows
        .iter()
        .map(|row| row.timestamp)
        .collect::<HashSet<_>>()
        .len();

    // Splits are re-filed through the same renames the bars are, so a stitched close takes its own
    // company's factor rather than the one belonging to whoever holds its old symbol now.
    let splits = splits.following_renames(boundaries);

    // The third element records whether the bar was already filed under this symbol, which decides
    // the overlap: a predecessor and its successor can both trade for a stretch — BK and BNY share
    // two dozen sessions — and the symbol still trading is the one whose price is authoritative.
    let mut dated_closes: HashMap<Ticker, Vec<(SessionDate, f64, bool)>> = HashMap::new();
    let mut bounded: HashSet<Ticker> = HashSet::new();
    for row in rows {
        let Some(stored) = Ticker::new(&row.ticker) else {
            continue;
        };
        let session = SessionDate::at(row.timestamp);
        // Resolved before it is bounded, so a bar moved onto its successor is judged against that
        // symbol's boundaries rather than against the ones that sent it there.
        let (ticker, own) = match boundaries.current_symbol(stored.as_str(), session) {
            Some(successor) => match Ticker::new(&successor) {
                Some(ticker) => (ticker, false),
                None => (stored, true),
            },
            None => (stored, true),
        };
        // Dropped rather than kept short: the retain below needs a full-length series, so a ticker
        // whose boundary falls inside this window loses its alignment and with it its candidacy.
        if let Some(earliest) = boundaries.earliest_usable_session(ticker.as_str(), as_of) {
            if session < earliest {
                bounded.insert(ticker);
                continue;
            }
        }
        let factor = splits.factor_at(ticker.as_str(), session, as_of);
        dated_closes
            .entry(ticker)
            .or_default()
            .push((session, row.close_price * factor, own));
    }

    // Sorted because a stitched series arrives in two runs — the successor's rows and the
    // predecessor's — and the query orders within a symbol, not across a rename.
    let mut closes_by_ticker: HashMap<Ticker, Vec<f64>> =
        HashMap::with_capacity(dated_closes.len());
    for (ticker, mut dated) in dated_closes {
        // Ordered by session, and within a session the symbol's own bar first, so the deduplication
        // below keeps it and discards the inherited one.
        dated.sort_by(|left, right| left.0.cmp(&right.0).then(right.2.cmp(&left.2)));
        dated.dedup_by_key(|(session, _, _)| *session);
        closes_by_ticker.insert(
            ticker,
            dated.into_iter().map(|(_, close, _)| close).collect(),
        );
    }

    let before = closes_by_ticker.len();
    closes_by_ticker.retain(|_, closes| closes.len() == session_count);
    // Reported apart from the gap count, because the two are different facts about a ticker: one
    // has a hole in its history, the other has history belonging to a different company.
    let dropped_at_a_boundary = bounded
        .iter()
        .filter(|ticker| !closes_by_ticker.contains_key(*ticker))
        .count();

    info!(
        tickers = closes_by_ticker.len(),
        dropped_for_gaps = before - closes_by_ticker.len(),
        dropped_at_a_boundary,
        sessions = session_count,
        "Aligned close history loaded"
    );
    Ok(closes_by_ticker)
}

/// Session-aligned close series, shared by reference because every caller only reads.
pub type AlignedCloses = Arc<HashMap<Ticker, Vec<f64>>>;

/// The aligned close history, loaded at most once per Eastern date.
///
/// Daily bars are written after the close, so this cannot change intraday, and the evaluation pass
/// needs all of it on every screening pass — seventy-eight times a session.
///
/// A value held in state and passed explicitly, not a process-wide static, like the calendar and
/// universe caches.
#[derive(Default)]
pub struct CloseHistoryCache {
    inner: tokio::sync::Mutex<Option<(SessionDate, AlignedCloses)>>,
}

impl CloseHistoryCache {
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns today's aligned close history, loading it if the cache is cold or stale.
    ///
    /// Behind an `Arc` because the map is large and every caller only reads it. The lock is
    /// released before the query and re-taken to store, which lets two cold callers both load; both
    /// reads are deterministic, so the second store is a harmless overwrite.
    pub async fn get(
        &self,
        pool: &PgPool,
        bar_interval: BarInterval,
        sessions: usize,
        splits: &SplitTable,
        boundaries: &BoundaryTable,
        now: DateTime<Utc>,
    ) -> Result<AlignedCloses, BarsError> {
        let today = SessionDate::at(now);

        if let Some((cached_date, closes)) = self.inner.lock().await.as_ref() {
            if *cached_date == today {
                return Ok(Arc::clone(closes));
            }
        }

        // Keyed by the Eastern date, which is also what the closes were restated onto, so a cache
        // hit can never hand back a series adjusted for a different day than it is asked about.
        let closes = Arc::new(
            load_aligned_closes(pool, bar_interval, sessions, splits, boundaries, today).await?,
        );
        if !closes.is_empty() {
            *self.inner.lock().await = Some((today, Arc::clone(&closes)));
        }
        Ok(closes)
    }

    /// Replaces the cached history. Used by tests and by the pre-open warm path.
    pub async fn install(&self, now: DateTime<Utc>, closes: HashMap<Ticker, Vec<f64>>) {
        *self.inner.lock().await = Some((SessionDate::at(now), Arc::new(closes)));
    }

    /// Drops the cached history so the next caller reloads it.
    ///
    /// Used after the post-close bar sync, whose new rows the cached window predates. Clearing is
    /// not the same as installing an empty map: an empty map keyed to today would answer "no
    /// history" for the rest of the Eastern date rather than reloading.
    pub async fn invalidate(&self) {
        *self.inner.lock().await = None;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ticker(raw: &str) -> Ticker {
        Ticker::new(raw).expect("test ticker must be valid")
    }

    fn bar(symbol: &str, interval: BarInterval, millis: i64, close: f64) -> EquityBar {
        EquityBar::new(
            ticker(symbol),
            interval,
            DateTime::from_timestamp_millis(millis).unwrap(),
            close,
            close,
            close,
            close,
            1_000,
            Some(close),
            Some(10),
        )
        .expect("test bar must be coherent")
    }

    /// A repeated key would fail the whole insert chunk, not just the offending row, so the last
    /// occurrence wins before the query is built.
    #[test]
    fn test_deduplicate_keeps_the_last_occurrence() {
        let bars = vec![
            bar("AAPL", BarInterval::OneDay, 1_000, 100.0),
            bar("AAPL", BarInterval::OneDay, 1_000, 101.0),
            bar("MSFT", BarInterval::OneDay, 1_000, 200.0),
        ];
        let deduplicated = deduplicate(&bars);
        assert_eq!(deduplicated.len(), 2);
        let apple = deduplicated
            .iter()
            .find(|bar| bar.ticker() == &ticker("AAPL"))
            .unwrap();
        assert_eq!(apple.close_price(), 101.0);
    }

    /// The interval is part of the identity. A daily and a one-minute bar sharing a ticker and
    /// timestamp are different rows, and collapsing them would silently discard intraday history
    /// once it starts being written.
    #[test]
    fn test_deduplicate_treats_intervals_as_distinct() {
        let bars = vec![
            bar("AAPL", BarInterval::OneDay, 1_000, 100.0),
            bar("AAPL", BarInterval::OneMinute, 1_000, 101.0),
        ];
        assert_eq!(deduplicate(&bars).len(), 2);
    }

    #[test]
    fn test_deduplicate_preserves_order() {
        let bars = vec![
            bar("AAPL", BarInterval::OneDay, 1_000, 100.0),
            bar("MSFT", BarInterval::OneDay, 2_000, 200.0),
            bar("NVDA", BarInterval::OneDay, 3_000, 300.0),
        ];
        let deduplicated = deduplicate(&bars);
        let symbols: Vec<String> = deduplicated
            .iter()
            .map(|bar| bar.ticker().to_string())
            .collect();
        assert_eq!(symbols, vec!["AAPL", "MSFT", "NVDA"]);
    }

    /// The column set is the contract between the trainer's S3 dataset and the application's
    /// inference input. Changing it here without changing the feature engineering breaks training
    /// silently, so the shape is asserted rather than assumed.
    #[test]
    fn test_dataframe_column_set_and_order_is_fixed() {
        let frame = bars_to_dataframe(&[bar("AAPL", BarInterval::OneDay, 1_000, 100.0)]).unwrap();
        assert_eq!(
            frame.get_column_names_str(),
            vec![
                "ticker",
                "bar_interval",
                "timestamp",
                "open_price",
                "high_price",
                "low_price",
                "close_price",
                "volume",
                "volume_weighted_average_price",
                "transactions",
            ]
        );
        assert_eq!(frame.height(), 1);
    }

    #[test]
    fn test_dataframe_writes_timestamps_as_unix_milliseconds() {
        let frame =
            bars_to_dataframe(&[bar("AAPL", BarInterval::OneDay, 1_700_000_000_000, 1.0)]).unwrap();
        let timestamps = frame.column("timestamp").unwrap().i64().unwrap();
        assert_eq!(timestamps.get(0), Some(1_700_000_000_000));
    }

    #[test]
    fn test_empty_bars_produce_an_empty_frame() {
        let frame = bars_to_dataframe(&[]).unwrap();
        assert_eq!(frame.height(), 0);
        assert_eq!(frame.get_column_names_str().len(), 10);
    }

    #[test]
    fn test_optional_columns_survive_as_nulls() {
        let sparse = EquityBar::new(
            ticker("AAPL"),
            BarInterval::OneDay,
            DateTime::from_timestamp_millis(1_000).unwrap(),
            1.0,
            1.0,
            1.0,
            1.0,
            10,
            None,
            None,
        )
        .expect("test bar must be coherent");
        let frame = bars_to_dataframe(&[sparse]).unwrap();
        assert_eq!(
            frame
                .column("volume_weighted_average_price")
                .unwrap()
                .null_count(),
            1
        );
        assert_eq!(frame.column("transactions").unwrap().null_count(), 1);
    }
}
