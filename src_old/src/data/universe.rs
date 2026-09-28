//! The day's tradable universe: roughly seven thousand symbols enter and a few hundred survive.
//!
//! Computed once at pre-open and held for the Eastern date, because it cannot change intraday.

use std::collections::{HashMap, HashSet};
use std::num::NonZeroU32;

use chrono::{DateTime, Utc};
use polars::prelude::*;
use sqlx::PgPool;
use tracing::info;

use uuid::Uuid;

use crate::common::alpaca::{ClientError, TradableAssets, TradingClient};
use crate::common::journal::{Journal, Observation, UniverseRefreshed};
use crate::common::types::{
    BarInterval, LiquidityFloor, LiquidityRefusal, Screen, ScreenWindow, SessionDate, Ticker,
};
use crate::data::cache::DailyCache;

/// Trailing window over which liquidity is averaged.
///
/// Long enough that a single quiet week does not evict a normally liquid name, short enough to
/// notice one that has genuinely dried up. Declared by the live path at `ServiceState::from_env`
/// rather than read here, so the number keeps one home and the screens keep none.
pub const LIQUIDITY_LOOKBACK_DAYS: u32 = 30;

/// The same window as a value, for the screen to carry.
const LIQUIDITY_LOOKBACK: NonZeroU32 = match NonZeroU32::new(LIQUIDITY_LOOKBACK_DAYS) {
    Some(days) => days,
    None => panic!("the liquidity lookback must be a positive number of days"),
};

/// The screen the live book both trades and predicts through.
///
/// One declaration for the two live call sites rather than one each, so they cannot drift apart on
/// the bounds, the window's length, or — since both now name their anchor — the days it covers.
pub const LIVE_SCREEN: Screen = Screen::new(
    LiquidityFloor::CURRENT,
    ScreenWindow::Trailing(LIQUIDITY_LOOKBACK),
);

/// Errors building the universe.
#[derive(Debug, thiserror::Error)]
pub enum UniverseError {
    #[error("failed to read the asset universe from Alpaca: {0}")]
    Alpaca(#[from] ClientError),
    #[error("failed to read liquidity history: {0}")]
    Database(#[from] sqlx::Error),
}

/// One ticker's liquidity over the trailing window.
///
/// Price is summarised by its minimum and dollar volume by its average. A price is a level, so one
/// that ever fell below the floor disqualifies the name; dollar volume is a flow, where one quiet
/// session says nothing about tradability.
#[derive(Debug, Clone, PartialEq)]
pub struct LiquidityRow {
    ticker: Ticker,
    minimum_close_price: f64,
    average_dollar_volume: f64,
}

impl LiquidityRow {
    pub fn new(ticker: Ticker, minimum_close_price: f64, average_dollar_volume: f64) -> Self {
        Self {
            ticker,
            minimum_close_price,
            average_dollar_volume,
        }
    }

    /// Whether this ticker clears `floor`, and which bound it failed if not.
    fn admission(&self, floor: LiquidityFloor) -> Result<(), LiquidityRefusal> {
        floor.admits(self.minimum_close_price, self.average_dollar_volume)
    }
}

/// Screens a bar frame down to the tickers that clear `screen` as of `as_of`.
///
/// `MIN(close_price)` against the price bound and `MEAN(close_price * volume)` against the notional
/// bound, per ticker, measured over the window and applied to the whole frame — an admitted name
/// keeps the history its features need.
///
/// `as_of` is the session the screen is being asked about. A window defined relative to it is
/// closed at both ends; a whole-frame window is the frame, which is why it reads no `timestamp` at
/// all rather than being capped at an anchor it does not use.
///
/// `bars` must carry `ticker`, `close_price` and `volume`, plus `timestamp` for a trailing window.
pub fn filter_liquid_bars(
    bars: DataFrame,
    screen: Screen,
    as_of: SessionDate,
) -> PolarsResult<DataFrame> {
    let measured = match screen.window() {
        ScreenWindow::WholeFrame => bars.clone(),
        ScreenWindow::Trailing(days) => trailing_window(&bars, i64::from(days.get()), as_of)?,
        // Answers a different question and so returns before the per-ticker aggregate below: this
        // one admits `(ticker, session)` pairs, and there is no single set of names to reduce to.
        ScreenWindow::PerSession(days) => {
            let admitted =
                per_session_admission(&bars, screen.floor(), i64::from(days.get()), as_of)?;
            return bars
                .lazy()
                .join(
                    admitted.lazy(),
                    [col("ticker"), col("timestamp")],
                    [col("ticker"), col("timestamp")],
                    JoinArgs::new(JoinType::Semi),
                )
                .collect();
        }
    };
    let floor = screen.floor();

    let liquid_tickers = measured
        .lazy()
        .group_by([col("ticker")])
        .agg([
            col("close_price")
                .cast(DataType::Float64)
                .min()
                .alias("minimum_close_price"),
            // The product per session and then the mean, because the mean of a product is not the
            // product of the means once price and volume move together.
            (col("close_price").cast(DataType::Float64) * col("volume").cast(DataType::Float64))
                .mean()
                .alias("average_dollar_volume"),
        ])
        .filter(
            col("minimum_close_price")
                .gt_eq(lit(floor.minimum_close_price()))
                .and(col("average_dollar_volume").gt_eq(lit(floor.minimum_dollar_volume()))),
        )
        .select([col("ticker")]);

    bars.lazy()
        .join(
            liquid_tickers,
            [col("ticker")],
            [col("ticker")],
            JoinArgs::new(JoinType::Semi),
        )
        .collect()
}

/// The rows within `days` Eastern calendar days of `as_of`, inclusive at the lower edge.
///
/// The same half-open interval [`load_liquidity`] reads, on both edges rather than just the one:
/// anchored on the caller's `as_of` where it used to read the frame's newest bar, and closed above
/// it where it used to run to the end of the frame. A bar after `as_of` is outside the window
/// however the frame came to hold it, which is not something a `pub` function can leave to its
/// callers to have arranged.
fn trailing_window(bars: &DataFrame, days: i64, as_of: SessionDate) -> PolarsResult<DataFrame> {
    let start = as_of
        .plus_calendar_days(-days)
        .midnight()
        .timestamp_millis();
    let end = window_closes_at(as_of);

    bars.clone()
        .lazy()
        .filter(
            col("timestamp")
                .gt_eq(lit(start))
                .and(col("timestamp").lt(lit(end))),
        )
        .collect()
}

/// The exclusive instant a window anchored on `as_of` closes at.
///
/// One expression for the three windows that have an upper edge, because a daily bar is stamped at
/// the 16:00 Eastern close and the next midnight is what admits `as_of`'s own bar and no more.
fn window_closes_at(as_of: SessionDate) -> i64 {
    as_of.plus_calendar_days(1).midnight().timestamp_millis()
}

/// The `(ticker, timestamp)` pairs that clear `floor` over the `days` ending at their own session.
///
/// One sliding window per ticker rather than one aggregate per ticker: the answer for a session is
/// what the live screen would have said that morning, so a name can be admitted in March and
/// refused in April on the same bars.
///
/// The window bound is computed once per distinct session and looked up, because Eastern calendar
/// arithmetic per row over two years of bars is the same thousand answers half a million times.
/// The two aggregates are taken over different row sets, exactly as the other three paths take them:
/// a null volume drops out of the average and still binds the minimum.
fn per_session_admission(
    bars: &DataFrame,
    floor: LiquidityFloor,
    days: i64,
    as_of: SessionDate,
) -> PolarsResult<DataFrame> {
    // Capped like every other window anchored on `as_of`, before anything is measured: a frame
    // running past it would otherwise screen sessions the caller did not ask about.
    let sorted = bars
        .clone()
        .lazy()
        .filter(col("timestamp").lt(lit(window_closes_at(as_of))))
        .collect()?
        .sort(
            ["ticker", "timestamp"],
            SortMultipleOptions::default().with_maintain_order(true),
        )?;
    let tickers = sorted.column("ticker")?.str()?;
    let timestamps = sorted.column("timestamp")?.i64()?;
    let close_prices = sorted.column("close_price")?.cast(&DataType::Float64)?;
    let close_prices = close_prices.f64()?;
    let volumes = sorted.column("volume")?.cast(&DataType::Float64)?;
    let volumes = volumes.f64()?;

    let window_starts = window_starts_by_session(timestamps, days)?;

    let mut admitted_tickers: Vec<&str> = Vec::new();
    let mut admitted_timestamps: Vec<i64> = Vec::new();
    let mut window = LiquidityWindow::default();
    let mut start = 0usize;
    for row in 0..sorted.height() {
        let (Some(ticker), Some(timestamp)) = (tickers.get(row), timestamps.get(row)) else {
            continue;
        };
        if row == 0 || tickers.get(row - 1) != Some(ticker) {
            start = row;
            window.clear();
        }
        window.push(row, close_prices.get(row), volumes.get(row));

        let opens_at = window_starts[&timestamp];
        while start < row && timestamps.get(start).is_some_and(|value| value < opens_at) {
            window.pop(start, close_prices.get(start), volumes.get(start));
            start += 1;
        }

        // No usable bar in the window is unmeasurable, not a refusal at zero: the aggregate has no
        // value to compare, and `f64::INFINITY` would clear any price bound if it reached one.
        let (Some(minimum_close), Some(average_notional)) =
            (window.minimum_close(), window.average_notional())
        else {
            continue;
        };
        if floor.admits(minimum_close, average_notional).is_ok() {
            admitted_tickers.push(ticker);
            admitted_timestamps.push(timestamp);
        }
    }

    DataFrame::new(vec![
        Column::new("ticker".into(), admitted_tickers),
        Column::new("timestamp".into(), admitted_timestamps),
    ])
}

/// One ticker's trailing window, kept as aggregates rather than re-read per session.
///
/// The minimum rides a monotonic deque of `(row, close)` with non-decreasing closes, so the front is
/// always the window's minimum and every row is pushed and popped at most once. The notional is a
/// running sum; it drifts by roughly 1e-7 of its own magnitude over a full archive, the same order
/// Polars' `mean()` carries on the three paths this one has to agree with.
#[derive(Default)]
struct LiquidityWindow {
    closes: std::collections::VecDeque<(usize, f64)>,
    total_notional: f64,
    notional_observations: usize,
}

impl LiquidityWindow {
    fn clear(&mut self) {
        self.closes.clear();
        self.total_notional = 0.0;
        self.notional_observations = 0;
    }

    fn push(&mut self, row: usize, close: Option<f64>, volume: Option<f64>) {
        let Some(close) = close else {
            return;
        };
        // Anything at or above the incoming close can never be the minimum again: it leaves the
        // window no later than this row does, and is no smaller while it stays.
        while self.closes.back().is_some_and(|(_, back)| *back >= close) {
            self.closes.pop_back();
        }
        self.closes.push_back((row, close));
        if let Some(volume) = volume {
            self.total_notional += close * volume;
            self.notional_observations += 1;
        }
    }

    fn pop(&mut self, row: usize, close: Option<f64>, volume: Option<f64>) {
        let Some(close) = close else {
            return;
        };
        // Only if it is still there: a row dropped by `push` for being no smaller left long ago.
        if self.closes.front().is_some_and(|(index, _)| *index == row) {
            self.closes.pop_front();
        }
        if volume.is_some() {
            self.total_notional -= close * volume.unwrap_or_default();
            self.notional_observations -= 1;
        }
    }

    /// `None` when the window holds no close at all, which is unmeasurable rather than a refusal.
    fn minimum_close(&self) -> Option<f64> {
        self.closes.front().map(|(_, close)| *close)
    }

    /// `None` when no bar in the window carried both a close and a volume.
    fn average_notional(&self) -> Option<f64> {
        (self.notional_observations > 0)
            .then(|| self.total_notional / self.notional_observations as f64)
    }
}

/// Each distinct session's window opening instant, in milliseconds.
///
/// Computed per session rather than per row, and through [`SessionDate`] rather than a fixed
/// offset, so the window opens at the same Eastern midnight the other two screens use.
fn window_starts_by_session(
    timestamps: &Int64Chunked,
    days: i64,
) -> PolarsResult<HashMap<i64, i64>> {
    let mut starts = HashMap::new();
    for timestamp in timestamps.into_no_null_iter() {
        if starts.contains_key(&timestamp) {
            continue;
        }
        let instant = DateTime::from_timestamp_millis(timestamp).ok_or_else(|| {
            PolarsError::ComputeError(format!("bar timestamp {timestamp} is not an instant").into())
        })?;
        let opens_at = SessionDate::at(instant)
            .plus_calendar_days(-days)
            .midnight()
            .timestamp_millis();
        starts.insert(timestamp, opens_at);
    }
    Ok(starts)
}

/// The symbols eligible to trade today, and which of them can be shorted.
///
/// A `Universe` in scope is proof that every ticker in it passed all three filters. The shortable
/// set is a subset: a pair needs one of each, so the screen draws its long leg from the whole
/// universe and its short leg from [`Universe::shortable`].
#[derive(Debug, Clone, Default)]
pub struct Universe {
    tickers: Vec<Ticker>,
    eligible: HashSet<Ticker>,
    shortable: HashSet<Ticker>,
}

impl Universe {
    /// Composes the three filters, all of which are necessary.
    ///
    /// Alpaca must permit it, we must hold bars for it, and it must clear `floor`.
    pub fn build(
        assets: &TradableAssets,
        liquidity: &[LiquidityRow],
        floor: LiquidityFloor,
    ) -> Self {
        let mut tickers = Vec::new();
        let mut shortable = HashSet::new();

        for row in liquidity {
            if row.admission(floor).is_err() {
                continue;
            }
            if !assets.is_tradable(&row.ticker) {
                continue;
            }
            if assets.is_shortable(&row.ticker) {
                shortable.insert(row.ticker.clone());
            }
            tickers.push(row.ticker.clone());
        }

        tickers.sort();
        let eligible: HashSet<Ticker> = tickers.iter().cloned().collect();
        Self {
            tickers,
            eligible,
            shortable,
        }
    }

    /// Every eligible ticker, in a stable order.
    pub fn tickers(&self) -> &[Ticker] {
        &self.tickers
    }

    /// Tickers this universe holds that `other` does not, in the stable order this one keeps.
    pub fn difference(&self, other: &Universe) -> Vec<Ticker> {
        self.tickers
            .iter()
            .filter(|ticker| !other.contains(ticker))
            .cloned()
            .collect()
    }

    /// Whether `ticker` is eligible to trade at all.
    ///
    /// Backed by a set rather than a scan of [`Universe::tickers`]. The screen asks this once per
    /// prediction, and a linear scan there turns a seven-thousand-symbol universe into forty-nine
    /// million comparisons on a pass that runs every five minutes.
    pub fn contains(&self, ticker: &Ticker) -> bool {
        self.eligible.contains(ticker)
    }

    /// Whether `ticker` can take the short leg of a pair.
    pub fn is_shortable(&self, ticker: &Ticker) -> bool {
        self.shortable.contains(ticker)
    }

    pub fn len(&self) -> usize {
        self.tickers.len()
    }

    pub fn is_empty(&self) -> bool {
        self.tickers.is_empty()
    }

    pub fn shortable_count(&self) -> usize {
        self.shortable.len()
    }
}

/// Reads per-ticker minimum close and average dollar volume over `screen`'s window, ending `as_of`.
///
/// The SQL twin of [`filter_liquid_bars`], over daily bars because the thresholds are calibrated on
/// daily dynamics, and bounded by `as_of` at both ends so a historical call reads history rather
/// than the rows ingested since. It reads the table raw where the frame twin is handed
/// split-adjusted prices, so the two agree on the window and not on what a name's minimum close was
/// across a split — `test_the_two_liquidity_screens_part_company_across_a_split` holds the size.
pub async fn load_liquidity(
    pool: &PgPool,
    as_of: SessionDate,
    screen: Screen,
) -> Result<Vec<LiquidityRow>, sqlx::Error> {
    let start = match screen.window() {
        // A per-session window asked about one session *is* the trailing one: the anchor it
        // re-anchors on is `as_of`. The live path only ever asks about today, which is why the
        // generalisation costs this reader nothing.
        ScreenWindow::Trailing(days) | ScreenWindow::PerSession(days) => {
            as_of.plus_calendar_days(-i64::from(days.get())).midnight()
        }
        // The epoch rather than an `Option<DateTime>` threaded through the query: no equity bar
        // predates it, so an unbounded lower edge and this one select the same rows.
        ScreenWindow::WholeFrame => DateTime::<Utc>::from_timestamp_nanos(0),
    };
    // Exclusive, and applied to both windows: `as_of` is the anchor and the window says only how far
    // back to reach, so a whole-frame call that read past it would be a second rule. A daily bar is
    // stamped at the 16:00 Eastern close, so the next midnight admits `as_of`'s own bar and no more.
    let end = as_of.plus_calendar_days(1).midnight();
    let rows = sqlx::query!(
        r#"
        SELECT ticker AS "ticker!",
               MIN(close_price) AS "minimum_close_price!",
               AVG(close_price * volume::double precision) AS "average_dollar_volume!"
        FROM equity_bars
        WHERE bar_interval = $1
          AND timestamp >= $2
          AND timestamp < $3
        GROUP BY ticker
        "#,
        BarInterval::OneDay.as_str(),
        start,
        end,
    )
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .filter_map(|row| {
            Ticker::new(&row.ticker).map(|ticker| {
                LiquidityRow::new(ticker, row.minimum_close_price, row.average_dollar_volume)
            })
        })
        .collect())
}

/// A [`Universe`] cached per Eastern date.
///
/// Warmed by the pre-open handler and refreshed on demand by anything that finds it cold, so a
/// restart mid-session repopulates rather than trading an empty universe until the next morning.
/// The screen is held here rather than passed per call, so every rebuild within a process screens
/// the same way — and so does the predict path, which reads it back off the cache rather than
/// declaring a second one that agrees by inspection.
pub struct UniverseCache {
    inner: DailyCache<Universe>,
    screen: Screen,
}

impl UniverseCache {
    pub fn new(screen: Screen) -> Self {
        Self {
            inner: DailyCache::default(),
            screen,
        }
    }

    /// The screen this cache builds its universe with.
    pub fn screen(&self) -> Screen {
        self.screen
    }

    /// Returns today's universe, rebuilding it if the cache is cold or was filled on an earlier
    /// date.
    ///
    /// An empty universe is returned but never stored: it means the Alpaca read or the liquidity
    /// query came back with nothing, and caching that would leave the session untradable.
    pub async fn get(
        &self,
        client: &TradingClient,
        pool: &PgPool,
        journal: &Journal,
        correlation_id: Uuid,
        now: DateTime<Utc>,
    ) -> Result<Universe, UniverseError> {
        let today = SessionDate::at(now);

        // Read before the call rather than inside the rebuild, which would re-enter the cache and
        // hold only because the lock happens to be released across it. `get` writes nothing until
        // the rebuild returns, so the answer is the same either way.
        let previous = self.inner.previous().await;
        self.inner
            .get(
                today,
                || async {
                    let assets = client.fetch_tradable_assets().await?;
                    let liquidity = load_liquidity(pool, today, self.screen).await?;
                    let universe = Universe::build(&assets, &liquidity, self.screen.floor());

                    info!(
                        alpaca_tradable = assets.tradable_count(),
                        with_history = liquidity.len(),
                        eligible = universe.len(),
                        shortable = universe.shortable_count(),
                        "Tradable universe built"
                    );

                    // What entered and what fell out, rather than only the size. A universe that
                    // holds steady at seven hundred names while churning fifty of them is the case
                    // a count cannot show.
                    let (admitted, removed) = match previous {
                        Some(previous) => (
                            universe.difference(&previous),
                            previous.difference(&universe),
                        ),
                        None => (Vec::new(), Vec::new()),
                    };
                    journal
                        .record(
                            correlation_id,
                            now,
                            Observation::UniverseRefreshed(UniverseRefreshed {
                                alpaca_tradable: assets.tradable_count(),
                                alpaca_shortable: assets.shortable_count(),
                                liquid: liquidity
                                    .iter()
                                    .filter(|row| row.admission(self.screen.floor()).is_ok())
                                    .count(),
                                universe_size: universe.len(),
                                admitted,
                                removed,
                                error: None,
                            }),
                        )
                        .await;

                    Ok(universe)
                },
                |universe| !universe.is_empty(),
            )
            .await
    }

    /// Replaces the cached universe. Used by tests and by the pre-open warm path.
    pub async fn install(&self, now: DateTime<Utc>, universe: Universe) {
        self.inner.install(SessionDate::at(now), universe).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ticker(raw: &str) -> Ticker {
        Ticker::new(raw).expect("test ticker must be valid")
    }

    /// Pinned to literals rather than `LiquidityFloor::CURRENT`, so these tests fail if the screen
    /// changes rather than moving with it.
    fn floor() -> LiquidityFloor {
        LiquidityFloor::new(10.0, 50_000_000.0).expect("test floor must be valid")
    }

    /// The test floor applied across every session the fixture holds.
    ///
    /// The `bars` fixture below carries no `timestamp` column at all, which is how the claim that a
    /// whole-frame screen never reads one is enforced rather than merely written down.
    fn screen() -> Screen {
        Screen::new(floor(), ScreenWindow::WholeFrame)
    }

    /// The same floor over a trailing window of `days`.
    fn trailing(days: u32) -> Screen {
        Screen::new(
            floor(),
            ScreenWindow::Trailing(NonZeroU32::new(days).expect("a positive window")),
        )
    }

    fn session(year: i32, month: u32, day: u32) -> SessionDate {
        SessionDate::from_date(
            chrono::NaiveDate::from_ymd_opt(year, month, day).expect("a real calendar date"),
        )
    }

    /// An anchor for the whole-frame cases, which never consult one.
    ///
    /// That it is never consulted is asserted by
    /// `test_a_whole_frame_screen_reads_the_frame_whatever_the_anchor`, so this helper leans on a
    /// test rather than on its own name.
    fn unread_anchor() -> SessionDate {
        session(2026, 6, 30)
    }

    fn assets() -> TradableAssets {
        TradableAssets::from_sets(
            HashSet::from([
                ticker("AAPL"),
                ticker("MSFT"),
                ticker("NVDA"),
                ticker("PENNY"),
                ticker("THIN"),
            ]),
            HashSet::from([ticker("AAPL"), ticker("MSFT")]),
        )
    }

    fn liquid(symbol: &str) -> LiquidityRow {
        LiquidityRow::new(ticker(symbol), 100.0, 500_000_000.0)
    }

    #[test]
    fn test_build_keeps_liquid_tradable_tickers() {
        let universe = Universe::build(&assets(), &[liquid("AAPL"), liquid("MSFT")], floor());
        assert_eq!(universe.tickers(), &[ticker("AAPL"), ticker("MSFT")]);
        assert_eq!(universe.len(), 2);
    }

    /// A ticker Alpaca permits but we have no history for cannot be screened or hedged, so it must
    /// not enter the universe even though nothing about it is wrong.
    #[test]
    fn test_build_excludes_tickers_without_history() {
        let universe = Universe::build(&assets(), &[liquid("AAPL")], floor());
        assert!(!universe.tickers().contains(&ticker("NVDA")));
    }

    /// A ticker we have history for but Alpaca will not trade must be excluded, which is the case
    /// a delisting produces.
    #[test]
    fn test_build_excludes_tickers_alpaca_will_not_trade() {
        let universe = Universe::build(&assets(), &[liquid("AAPL"), liquid("GONE")], floor());
        assert!(!universe.tickers().contains(&ticker("GONE")));
    }

    /// Both thresholds must bind independently: a cheap-but-heavily-traded name and an
    /// expensive-but-untraded name are each excluded for their own reason.
    #[test]
    fn test_build_applies_both_liquidity_thresholds() {
        let cheap = LiquidityRow::new(ticker("PENNY"), 9.99, 900_000_000.0);
        let thin = LiquidityRow::new(ticker("THIN"), 500.0, 49_999_999.0);

        let universe = Universe::build(&assets(), &[liquid("AAPL"), cheap, thin], floor());

        assert_eq!(universe.tickers(), &[ticker("AAPL")]);
    }

    #[test]
    fn test_thresholds_are_inclusive_at_the_boundary() {
        let exactly_at_threshold = LiquidityRow::new(ticker("AAPL"), 10.0, 50_000_000.0);
        let universe = Universe::build(&assets(), &[exactly_at_threshold], floor());
        assert_eq!(universe.len(), 1, "the threshold itself must pass");
    }

    /// The reason the screen counts dollars. CBOE trades ~1M shares a day at ~$290, which is $290M
    /// of notional and under the retired one-million-share floor on most sessions; a $12 name at
    /// four million shares is $48M and was admitted by it. The old screen inverted both.
    #[test]
    fn test_dollar_volume_admits_expensive_names_and_drops_cheap_heavy_ones() {
        let expensive = LiquidityRow::new(ticker("MSFT"), 290.0, 290_000_000.0);
        let cheap_and_heavy = LiquidityRow::new(ticker("NVDA"), 12.0, 48_000_000.0);

        let universe = Universe::build(&assets(), &[expensive, cheap_and_heavy], floor());

        assert_eq!(universe.tickers(), &[ticker("MSFT")]);
    }

    /// The short leg needs both Alpaca flags. A tradable-but-not-shortable name stays in the
    /// universe for the long leg and is excluded from the shortable subset.
    #[test]
    fn test_shortable_is_a_subset_of_tradable() {
        let universe = Universe::build(
            &assets(),
            &[liquid("AAPL"), liquid("MSFT"), liquid("NVDA")],
            floor(),
        );
        assert!(universe.is_shortable(&ticker("AAPL")));
        assert!(universe.is_shortable(&ticker("MSFT")));
        assert!(
            !universe.is_shortable(&ticker("NVDA")),
            "NVDA is tradable but not shortable"
        );
        assert!(universe.tickers().contains(&ticker("NVDA")));
        assert_eq!(universe.shortable_count(), 2);
    }

    #[test]
    fn test_tickers_are_sorted_and_stable() {
        let universe = Universe::build(
            &assets(),
            &[liquid("NVDA"), liquid("AAPL"), liquid("MSFT")],
            floor(),
        );
        assert_eq!(
            universe.tickers(),
            &[ticker("AAPL"), ticker("MSFT"), ticker("NVDA")]
        );
    }

    #[test]
    fn test_empty_inputs_produce_an_empty_universe() {
        assert!(Universe::build(&assets(), &[], floor()).is_empty());
        assert!(Universe::build(&TradableAssets::default(), &[liquid("AAPL")], floor()).is_empty());
    }

    /// Two sessions of one ticker, so the minimum and the mean differ.
    fn bars(rows: &[(&str, f64, i64)]) -> DataFrame {
        let tickers: Vec<&str> = rows.iter().map(|(ticker, _, _)| *ticker).collect();
        let close_prices: Vec<f64> = rows.iter().map(|(_, close, _)| *close).collect();
        let volumes: Vec<i64> = rows.iter().map(|(_, _, volume)| *volume).collect();
        DataFrame::new(vec![
            Column::new("ticker".into(), tickers),
            Column::new("close_price".into(), close_prices),
            Column::new("volume".into(), volumes),
        ])
        .expect("the fixture frame must build")
    }

    fn surviving_tickers(frame: &DataFrame) -> Vec<String> {
        let mut names: Vec<String> = frame
            .column("ticker")
            .unwrap()
            .str()
            .unwrap()
            .into_no_null_iter()
            .map(str::to_string)
            .collect();
        names.sort();
        names.dedup();
        names
    }

    /// The dataframe screen and [`LiquidityFloor::admits`] have to agree at the boundary, or the
    /// traded population and the trained one differ by the names sitting exactly on the floor.
    #[test]
    fn test_the_dataframe_screen_matches_the_floor_at_the_boundary() {
        // 10.00 close and 50,000,000 notional: on the bound in both, which both admit.
        let frame = bars(&[("EDGE", 10.0, 5_000_000), ("EDGE", 10.0, 5_000_000)]);
        assert_eq!(floor().admits(10.0, 50_000_000.0), Ok(()));
        assert_eq!(
            surviving_tickers(&filter_liquid_bars(frame, screen(), unread_anchor()).unwrap()),
            vec!["EDGE".to_string()]
        );

        let under = bars(&[("EDGE", 9.99, 5_000_000), ("EDGE", 9.99, 5_000_000)]);
        assert!(filter_liquid_bars(under, screen(), unread_anchor())
            .unwrap()
            .is_empty());
    }

    /// Price on the window minimum and notional on the window average, which is the definition the
    /// universe query already reads. A screen that averaged the price instead would admit a name
    /// that spent half the window below the floor.
    #[test]
    fn test_the_dataframe_screen_takes_the_minimum_price_and_the_average_notional() {
        // Mean close 55, minimum close 5: the minimum is what binds, so this is refused.
        let dipped = bars(&[("DIPS", 105.0, 1_000_000), ("DIPS", 5.0, 20_000_000)]);
        assert!(filter_liquid_bars(dipped, screen(), unread_anchor())
            .unwrap()
            .is_empty());

        // One quiet session against one heavy one: the average carries it, which the price
        // treatment deliberately does not do.
        let quiet = bars(&[("FLOW", 100.0, 10_000), ("FLOW", 100.0, 2_000_000)]);
        assert_eq!(
            surviving_tickers(&filter_liquid_bars(quiet, screen(), unread_anchor()).unwrap()),
            vec!["FLOW".to_string()]
        );
    }

    /// A name whose liquidity predates the window is not admitted by it.
    ///
    /// Moved here with the arithmetic it tests: the window used to be cut by the predict path
    /// before it called this screen, so the behaviour was exercised a layer above the code.
    #[test]
    fn test_liquidity_older_than_the_window_does_not_admit_a_name() {
        let day = 24 * 60 * 60 * 1_000i64;
        let as_of = session(2026, 6, 30);
        let newest = as_of.midnight().timestamp_millis();
        let frame = DataFrame::new(vec![
            Column::new("ticker".into(), vec!["FADED", "FADED"]),
            // One bar inside the 30-day window, one 60 days before it.
            Column::new("timestamp".into(), vec![newest - 60 * day, newest]),
            Column::new("close_price".into(), vec![50.0, 50.0]),
            Column::new("volume".into(), vec![10_000_000i64, 100]),
        ])
        .expect("the fixture frame must build");

        assert!(filter_liquid_bars(frame.clone(), trailing(30), as_of)
            .unwrap()
            .is_empty());
        // The same rows over the whole frame do admit it, which is what makes the window the
        // difference rather than the bounds.
        assert_eq!(
            filter_liquid_bars(frame, screen(), unread_anchor())
                .unwrap()
                .height(),
            2
        );
    }

    /// The window's own lower edge is inside it, matching `load_liquidity`'s `timestamp >= $2`.
    ///
    /// An exclusive bound here and an inclusive one in SQL disagree on exactly one session, which is
    /// the session most likely to decide a marginal name.
    #[test]
    fn test_a_bar_exactly_on_the_window_edge_is_inside_it() {
        let newest = session(2026, 6, 30);
        let edge = newest
            .plus_calendar_days(-i64::from(LIQUIDITY_LOOKBACK_DAYS))
            .midnight()
            .timestamp_millis();
        let frame = DataFrame::new(vec![
            Column::new("ticker".into(), vec!["EDGE", "EDGE"]),
            // The older bar sits on the edge and carries all the notional; drop it and the name
            // averages below the floor.
            Column::new(
                "timestamp".into(),
                vec![edge, newest.midnight().timestamp_millis()],
            ),
            Column::new("close_price".into(), vec![50.0, 50.0]),
            Column::new("volume".into(), vec![4_000_000i64, 0]),
        ])
        .expect("the fixture frame must build");

        let result = filter_liquid_bars(frame, trailing(LIQUIDITY_LOOKBACK_DAYS), newest).unwrap();

        assert_eq!(result.height(), 2);
    }

    /// The window is Eastern calendar days, not multiples of 24 hours.
    ///
    /// A window ending after the March transition opens an hour later in UTC than a fixed offset
    /// does, so a fixed offset reaches back into a session the universe has already stopped counting
    /// and can admit a name on liquidity that is outside the window.
    #[test]
    fn test_the_window_counts_calendar_days_across_a_daylight_saving_transition() {
        // 2026-03-08 is the spring transition; a window ending 2026-03-20 spans it.
        let newest = session(2026, 3, 20);
        let oldest = newest.plus_calendar_days(-i64::from(LIQUIDITY_LOOKBACK_DAYS));
        let fixed_offset_start =
            newest.midnight().timestamp_millis() - i64::from(LIQUIDITY_LOOKBACK_DAYS) * 86_400_000;

        assert_eq!(
            oldest.midnight().timestamp_millis() - fixed_offset_start,
            3_600_000,
            "the calendar bound must open an hour after the fixed-offset one across the transition"
        );

        // All of the name's notional sits in that one disputed hour, so whether it clears the floor
        // is exactly the question of which bound is used.
        let frame = DataFrame::new(vec![
            Column::new("ticker".into(), vec!["SPRUNG", "SPRUNG"]),
            Column::new(
                "timestamp".into(),
                vec![fixed_offset_start, newest.midnight().timestamp_millis()],
            ),
            Column::new("close_price".into(), vec![50.0, 50.0]),
            Column::new("volume".into(), vec![4_000_000i64, 0]),
        ])
        .expect("the fixture frame must build");

        let result = filter_liquid_bars(frame, trailing(LIQUIDITY_LOOKBACK_DAYS), newest).unwrap();

        assert!(
            result.is_empty(),
            "a bar an hour outside the calendar window must not admit the name"
        );
    }

    /// A trailing window reaches back from the caller's session, not from the frame's newest bar.
    ///
    /// This is the pre-open case the live book actually runs: the newest daily bar belongs to the
    /// previous session while the traded universe is being built for today. Inferring the anchor
    /// from the data reached back one day further and admitted a name the universe refuses.
    #[test]
    fn test_a_trailing_window_reads_the_callers_anchor_not_the_frames_newest_bar() {
        let newest = session(2026, 6, 30);
        let today = newest.plus_calendar_days(1);
        // The whole of this name's notional sits on the window's far edge as measured from the
        // frame's newest bar, which is one session outside the window measured from today.
        let edge = newest
            .plus_calendar_days(-i64::from(LIQUIDITY_LOOKBACK_DAYS))
            .midnight()
            .timestamp_millis();
        let frame = DataFrame::new(vec![
            Column::new("ticker".into(), vec!["MARGIN", "MARGIN"]),
            Column::new(
                "timestamp".into(),
                vec![edge, newest.midnight().timestamp_millis()],
            ),
            Column::new("close_price".into(), vec![50.0, 50.0]),
            Column::new("volume".into(), vec![4_000_000i64, 0]),
        ])
        .expect("the fixture frame must build");

        let from_the_frame =
            filter_liquid_bars(frame.clone(), trailing(LIQUIDITY_LOOKBACK_DAYS), newest).unwrap();
        let from_today =
            filter_liquid_bars(frame, trailing(LIQUIDITY_LOOKBACK_DAYS), today).unwrap();

        assert_eq!(
            from_the_frame.height(),
            2,
            "anchored on the frame's newest bar, the edge session is inside the window"
        );
        assert!(
            from_today.is_empty(),
            "anchored on today, the edge session is one day outside it and the name is refused"
        );
    }

    /// A bar after `as_of` is outside the trailing window, matching `load_liquidity`'s upper bound.
    ///
    /// The frame's extent used to be the window's upper edge, which was a promise about callers
    /// rather than a property of the function. A historical screen over a frame that runs past its
    /// anchor would have admitted a name on liquidity it did not yet have.
    #[test]
    fn test_a_bar_after_the_anchor_is_outside_the_trailing_window() {
        let as_of = session(2026, 6, 30);
        // All of the notional arrives the session after the one being screened for.
        let frame = DataFrame::new(vec![
            Column::new("ticker".into(), vec!["LATER", "LATER"]),
            Column::new(
                "timestamp".into(),
                vec![
                    as_of.midnight().timestamp_millis(),
                    as_of.plus_calendar_days(1).midnight().timestamp_millis(),
                ],
            ),
            Column::new("close_price".into(), vec![50.0, 50.0]),
            Column::new("volume".into(), vec![0i64, 4_000_000]),
        ])
        .expect("the fixture frame must build");

        assert!(
            filter_liquid_bars(frame.clone(), trailing(30), as_of)
                .unwrap()
                .is_empty(),
            "liquidity arriving after the anchor must not admit the name"
        );
        // The control: asked about the later session, the same bar is inside the window and does
        // admit it, so the bound is excluding the future rather than the name.
        assert_eq!(
            filter_liquid_bars(frame, trailing(30), as_of.plus_calendar_days(1))
                .unwrap()
                .height(),
            2
        );
    }

    /// The same floor over `PerSession`, for the per-session cases.
    fn per_session(days: u32) -> Screen {
        Screen::new(
            floor(),
            ScreenWindow::PerSession(NonZeroU32::new(days).expect("a positive window")),
        )
    }

    /// Every `(ticker, session)` pair a screened frame holds, sorted.
    fn surviving_pairs(frame: &DataFrame) -> Vec<(String, i64)> {
        let tickers = frame.column("ticker").unwrap();
        let tickers = tickers.str().unwrap();
        let timestamps = frame.column("timestamp").unwrap();
        let timestamps = timestamps.i64().unwrap();
        let mut pairs: Vec<(String, i64)> = tickers
            .into_no_null_iter()
            .zip(timestamps.into_no_null_iter())
            .map(|(ticker, timestamp)| (ticker.to_string(), timestamp))
            .collect();
        pairs.sort();
        pairs
    }

    /// A frame of one ticker's closes and volumes over consecutive sessions ending `last`.
    fn series(ticker: &str, last: SessionDate, rows: &[(f64, i64)]) -> DataFrame {
        let offsets: Vec<i64> = (0..rows.len() as i64).rev().map(|back| -back).collect();
        DataFrame::new(vec![
            Column::new("ticker".into(), vec![ticker; rows.len()]),
            Column::new(
                "timestamp".into(),
                offsets
                    .iter()
                    .map(|offset| {
                        last.plus_calendar_days(*offset)
                            .midnight()
                            .timestamp_millis()
                    })
                    .collect::<Vec<_>>(),
            ),
            Column::new(
                "close_price".into(),
                rows.iter().map(|(close, _)| *close).collect::<Vec<_>>(),
            ),
            Column::new(
                "volume".into(),
                rows.iter().map(|(_, volume)| *volume).collect::<Vec<_>>(),
            ),
        ])
        .expect("the fixture frame must build")
    }

    /// A per-session screen admits a name for some sessions and refuses it for others.
    ///
    /// The whole point of the variant, and the case where the two single-anchor windows give
    /// *opposite* answers on the same bars: whole-frame averages the quiet sessions away and admits
    /// the name throughout, trailing-at-the-end sees only the quiet ones and refuses it throughout.
    /// Both are wrong about most of the window.
    #[test]
    fn test_a_per_session_screen_admits_a_name_for_part_of_its_history() {
        let last = session(2026, 6, 30);
        // Three liquid sessions at 100M notional, then two dead ones. Over the whole frame that
        // averages 60M and clears; over the trailing two days it averages 33M and does not.
        let frame = series(
            "FADES",
            last,
            &[
                (50.0, 2_000_000),
                (50.0, 2_000_000),
                (50.0, 2_000_000),
                (50.0, 0),
                (50.0, 0),
            ],
        );

        let admitted =
            surviving_pairs(&filter_liquid_bars(frame.clone(), per_session(2), last).unwrap());

        assert_eq!(
            admitted.len(),
            4,
            "the last session's trailing two days average 33M and are refused; the rest clear"
        );
        assert_eq!(
            filter_liquid_bars(frame.clone(), screen(), last)
                .unwrap()
                .height(),
            5,
            "a whole-frame screen averages 60M and admits every session, including the dead ones"
        );
        assert_eq!(
            filter_liquid_bars(frame, trailing(2), last)
                .unwrap()
                .height(),
            0,
            "a trailing screen at the end sees only the dead sessions and refuses every one"
        );
    }

    /// At a single session, the per-session screen and the trailing screen are the same screen.
    ///
    /// The generalisation has to collapse: `load_liquidity` reads them through one arm, and that is
    /// only sound if a one-session frame cannot tell them apart.
    #[test]
    fn test_a_per_session_screen_is_the_trailing_screen_over_one_session() {
        let last = session(2026, 6, 30);
        for rows in [
            vec![(50.0, 2_000_000)],
            vec![(50.0, 1)],
            vec![(9.99, 2_000_000)],
        ] {
            let frame = series("SOLO", last, &rows);
            assert_eq!(
                surviving_pairs(&filter_liquid_bars(frame.clone(), per_session(30), last).unwrap()),
                surviving_pairs(&filter_liquid_bars(frame, trailing(30), last).unwrap()),
                "the two windows must agree on a frame holding one session"
            );
        }
    }

    /// A per-session window is anchored too, so a session after `as_of` is outside it.
    ///
    /// It is `Trailing` re-anchored, which means it inherits the upper bound and not only the
    /// length. Without it a historical call screens and returns sessions the caller did not ask
    /// about, where the trailing branch and `load_liquidity` both refuse them.
    #[test]
    fn test_a_per_session_screen_is_capped_at_the_anchor() {
        let last = session(2026, 6, 30);
        let as_of = last.plus_calendar_days(-1);
        // Both sessions clear the floor on their own, so only the cap can exclude the later one.
        let frame = series("AFTER", last, &[(50.0, 2_000_000), (50.0, 2_000_000)]);

        let admitted = surviving_pairs(&filter_liquid_bars(frame, per_session(30), as_of).unwrap());
        let expected = vec![("AFTER".to_string(), as_of.midnight().timestamp_millis())];

        assert_eq!(
            admitted, expected,
            "the session after the anchor must not be screened or returned"
        );
    }

    /// A close with no volume still binds the minimum, as it does on all three other paths.
    ///
    /// The two aggregates are taken over different row sets: `MIN(close_price)` reads every present
    /// close and `MEAN(close * volume)` reads only the rows carrying both. Skipping the whole row
    /// on a null volume would let a sub-floor close escape the price bound here alone.
    #[test]
    fn test_a_null_volume_drops_from_the_average_and_still_binds_the_minimum() {
        let last = session(2026, 6, 30);
        let frame = DataFrame::new(vec![
            Column::new("ticker".into(), vec!["NULLV", "NULLV"]),
            Column::new(
                "timestamp".into(),
                vec![
                    last.plus_calendar_days(-1).midnight().timestamp_millis(),
                    last.midnight().timestamp_millis(),
                ],
            ),
            // The sub-floor close carries no volume, so only the minimum can refuse the name.
            Column::new("close_price".into(), vec![Some(9.0), Some(50.0)]),
            Column::new("volume".into(), vec![None, Some(4_000_000i64)]),
        ])
        .expect("the fixture frame must build");

        let admitted = surviving_pairs(&filter_liquid_bars(frame, per_session(30), last).unwrap());

        assert!(
            admitted.is_empty(),
            "a 9.00 close is below the 10.00 bound whether or not it traded"
        );
    }

    /// A name that recovers is re-admitted, because the screen has no memory of the refusal.
    ///
    /// The property that makes this point-in-time rather than a one-way eviction, and the source of
    /// the re-entry cost: `engineer_features` gives the first session back no return, because the
    /// frame-wide session rank says it does not follow the row before it.
    #[test]
    fn test_a_per_session_screen_readmits_a_name_that_recovers() {
        let last = session(2026, 6, 30);
        // 60M notional when liquid, so a one-day window holding one dead session averages 30M and
        // refuses -- the recovery therefore lags by a session, which is the trailing mean working.
        let frame = series(
            "RETRY",
            last,
            &[
                (50.0, 1_200_000),
                (50.0, 0),
                (50.0, 0),
                (50.0, 1_200_000),
                (50.0, 1_200_000),
            ],
        );

        let sessions: Vec<i64> =
            surviving_pairs(&filter_liquid_bars(frame, per_session(1), last).unwrap())
                .iter()
                .map(|(_, session)| *session)
                .collect();
        let expected: Vec<i64> = [-4i64, 0]
            .iter()
            .map(|back| last.plus_calendar_days(*back).midnight().timestamp_millis())
            .collect();

        assert_eq!(
            sessions, expected,
            "out for the middle three sessions and back for the last, not evicted for good"
        );
    }

    /// A whole-frame screen reads the frame it was handed, whatever session it is asked about.
    ///
    /// The frame's own extent is the window there, because the caller already chose it. Asserted
    /// rather than left to a parameter name, since `unread_anchor` claims exactly this.
    #[test]
    fn test_a_whole_frame_screen_reads_the_frame_whatever_the_anchor() {
        let frame = bars(&[
            ("KEEP", 100.0, 1_000_000),
            ("KEEP", 100.0, 1_000_000),
            ("DROP", 100.0, 1),
            ("DROP", 100.0, 1),
        ]);

        let early = filter_liquid_bars(frame.clone(), screen(), session(2001, 1, 2)).unwrap();
        let late = filter_liquid_bars(frame, screen(), session(2049, 12, 31)).unwrap();

        assert_eq!(surviving_tickers(&early), vec!["KEEP".to_string()]);
        assert_eq!(surviving_tickers(&late), surviving_tickers(&early));
        assert_eq!(late.height(), early.height());
    }

    /// An empty frame yields an empty window, by the same arithmetic as a full one.
    ///
    /// It used to be a branch: with no newest bar there was no anchor, so the frame was returned
    /// unchanged and happened to be empty. The anchor now arrives from the caller, so the window is
    /// computed the one way and the empty case is a consequence rather than a special case.
    #[test]
    fn test_a_trailing_window_over_an_empty_frame_is_empty_rather_than_refused() {
        let frame = DataFrame::new(vec![
            Column::new("ticker".into(), Vec::<&str>::new()),
            Column::new("timestamp".into(), Vec::<i64>::new()),
            Column::new("close_price".into(), Vec::<f64>::new()),
            Column::new("volume".into(), Vec::<i64>::new()),
        ])
        .expect("the fixture frame must build");

        assert!(
            filter_liquid_bars(frame, trailing(30), session(2026, 6, 30))
                .unwrap()
                .is_empty()
        );
    }

    /// Every bar of an admitted ticker survives, and none of a refused one: the screen is per
    /// ticker, not per session, so a name's quiet days travel with its busy ones.
    #[test]
    fn test_the_dataframe_screen_keeps_or_drops_a_ticker_whole() {
        let frame = bars(&[
            ("KEEP", 100.0, 1_000_000),
            ("KEEP", 100.0, 1_000_000),
            ("DROP", 100.0, 1),
            ("DROP", 100.0, 1),
        ]);
        let filtered = filter_liquid_bars(frame, screen(), unread_anchor()).unwrap();
        assert_eq!(filtered.height(), 2);
        assert_eq!(surviving_tickers(&filtered), vec!["KEEP".to_string()]);
    }

    #[tokio::test]
    async fn test_cache_serves_installed_universe_without_fetching() {
        let cache = UniverseCache::new(screen());
        let now = "2026-06-10T14:00:00Z".parse::<DateTime<Utc>>().unwrap();
        let universe = Universe::build(&assets(), &[liquid("AAPL")], floor());
        cache.install(now, universe).await;

        // Unreachable client and pool: a cache hit must touch neither.
        let client = TradingClient::with_base_url(
            crate::common::alpaca::AlpacaCredentials::new("k".into(), "s".into()).unwrap(),
            "http://127.0.0.1:1".to_string(),
        );
        let pool = sqlx::PgPool::connect_lazy("postgresql://user:pass@127.0.0.1:1/none").unwrap();

        let journal = Journal::new(
            std::env::temp_dir().join(format!("fund-universe-cache-{}", std::process::id())),
        )
        .expect("the journal directory must be creatable");
        let served = cache
            .get(&client, &pool, &journal, Uuid::nil(), now)
            .await
            .expect("cache hit must not fetch");
        assert_eq!(served.len(), 1);
    }
}
