//! Equity bars: one fetch from Alpaca, three destinations.
//!
//! The application writes bars to PostgreSQL after the close. The trainer, on a different machine
//! with no database, writes the same bars to S3 parquet before it trains. Both call
//! [`fetch_bars`] and both build their frames through [`bars_to_dataframe`], which is the whole
//! point: if the two ever diverged, the model would train on columns the inference path does not
//! produce, and the failure would surface as bad predictions rather than as a build error.
//!
//! [`load_bars_dataframe`] is the read side, feeding the prediction pipeline and the correlation
//! screen.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use chrono::{DateTime, Duration, NaiveDate, Utc};
use polars::prelude::*;
use sqlx::PgPool;
use tracing::info;

use crate::common::alpaca::{ClientError, MarketDataClient};
use crate::common::types::{BarInterval, EquityBar, Ticker};

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

/// Errors syncing or reading bars.
#[derive(Debug, thiserror::Error)]
pub enum BarsError {
    #[error("failed to fetch bars from Alpaca: {0}")]
    Alpaca(#[from] ClientError),
    #[error("failed to persist bars: {0}")]
    Database(#[from] sqlx::Error),
    #[error("failed to build a bar frame: {0}")]
    Frame(#[from] PolarsError),
}

/// Fetches bars for `symbols` over an inclusive date range.
///
/// A thin pass-through to the market data client, present so both the application and the trainer
/// have one entry point to call and one place to change.
pub async fn fetch_bars(
    client: &MarketDataClient,
    symbols: &[String],
    bar_interval: BarInterval,
    start: NaiveDate,
    end: NaiveDate,
) -> Result<Vec<EquityBar>, BarsError> {
    Ok(client.fetch_bars(symbols, bar_interval, start, end).await?)
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

/// Loads a trailing window of bars at one interval as a frame.
///
/// The interval filter is not optional. Without it a query would mix daily and intraday rows into
/// one series, and the feature engineering would silently compute returns across incompatible
/// sampling rates.
pub async fn load_bars_dataframe(
    pool: &PgPool,
    bar_interval: BarInterval,
    lookback_days: i64,
) -> Result<DataFrame, BarsError> {
    let end = Utc::now();
    let start = end - Duration::days(lookback_days);

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
          AND timestamp <= $3
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

    info!(
        rows = dataframe.height(),
        bar_interval = bar_interval.as_str(),
        "Equity bars loaded"
    );
    Ok(dataframe)
}

/// Loads a trailing window of closes per ticker, aligned across tickers by session.
///
/// Every returned series has the same length, and position `i` in one is the same session as
/// position `i` in every other. Tickers missing any session in the window are dropped rather than
/// gap-filled, which is the whole reason this exists and is not a `GROUP BY ticker`: two series of
/// equal length covering different dates produce a correlation between different days, and neither
/// the correlation nor the spread that follows carries any sign that it happened.
///
/// The lower bound on `timestamp` is what keeps this from scanning every chunk of the hypertable.
/// It is expressed against the column directly rather than wrapped in an expression, so chunk
/// exclusion applies.
pub async fn load_aligned_closes(
    pool: &PgPool,
    bar_interval: BarInterval,
    sessions: usize,
) -> Result<HashMap<Ticker, Vec<f64>>, BarsError> {
    if sessions == 0 {
        return Ok(HashMap::new());
    }

    // Twice the session count in calendar days covers weekends and holidays with room to spare;
    // a 60-session window spans roughly 84 calendar days.
    let earliest = Utc::now() - Duration::days(sessions as i64 * 2);

    let rows = sqlx::query!(
        r#"
        WITH recent_sessions AS (
            SELECT DISTINCT timestamp
            FROM equity_bars
            WHERE bar_interval = $1 AND timestamp >= $2
            ORDER BY timestamp DESC
            LIMIT $3
        )
        SELECT ticker AS "ticker!", timestamp AS "timestamp!", close_price AS "close_price!"
        FROM equity_bars
        WHERE bar_interval = $1
          AND timestamp >= $2
          AND timestamp IN (SELECT timestamp FROM recent_sessions)
        ORDER BY ticker, timestamp
        "#,
        bar_interval.as_str(),
        earliest,
        sessions as i64,
    )
    .fetch_all(pool)
    .await?;

    let session_count = rows
        .iter()
        .map(|row| row.timestamp)
        .collect::<HashSet<_>>()
        .len();

    let mut closes_by_ticker: HashMap<Ticker, Vec<f64>> = HashMap::new();
    for row in rows {
        let Some(ticker) = Ticker::new(&row.ticker) else {
            continue;
        };
        closes_by_ticker
            .entry(ticker)
            .or_default()
            .push(row.close_price);
    }

    let before = closes_by_ticker.len();
    closes_by_ticker.retain(|_, closes| closes.len() == session_count);

    info!(
        tickers = closes_by_ticker.len(),
        dropped_for_gaps = before - closes_by_ticker.len(),
        sessions = session_count,
        "Aligned close history loaded"
    );
    Ok(closes_by_ticker)
}

/// The aligned close history, loaded at most once per Eastern date.
///
/// Daily bars are written after the close, so this does not change intraday — and the evaluation
/// pass runs seventy-eight times a session and needs the whole thing on every pass that screens.
/// Reloading it each time would be a six-figure row count re-read for a result known not to have
/// moved.
///
/// Same shape as [`crate::data::calendar::CalendarCache`] and
/// [`crate::data::universe::UniverseCache`], and for the same reason: a value held in state and
/// passed explicitly, not a process-wide static.
/// Session-aligned close series, shared by reference because every caller only reads.
pub type AlignedCloses = Arc<HashMap<Ticker, Vec<f64>>>;

#[derive(Default)]
pub struct CloseHistoryCache {
    inner: tokio::sync::Mutex<Option<(NaiveDate, AlignedCloses)>>,
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
        now: DateTime<Utc>,
    ) -> Result<AlignedCloses, BarsError> {
        let today = crate::data::calendar::eastern_date(now);

        if let Some((cached_date, closes)) = self.inner.lock().await.as_ref() {
            if *cached_date == today {
                return Ok(Arc::clone(closes));
            }
        }

        let closes = Arc::new(load_aligned_closes(pool, bar_interval, sessions).await?);
        if !closes.is_empty() {
            *self.inner.lock().await = Some((today, Arc::clone(&closes)));
        }
        Ok(closes)
    }

    /// Replaces the cached history. Used by tests and by the pre-open warm path.
    pub async fn install(&self, now: DateTime<Utc>, closes: HashMap<Ticker, Vec<f64>>) {
        *self.inner.lock().await =
            Some((crate::data::calendar::eastern_date(now), Arc::new(closes)));
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
