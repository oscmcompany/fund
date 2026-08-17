//! The day's tradable universe.
//!
//! Three filters compose to produce it, and all three are necessary:
//!
//! 1. **Alpaca says we can trade it.** Active, tradable, and — for anything that can take the short
//!    leg — both shortable and easy to borrow.
//! 2. **We have price history for it.** A ticker with no bars cannot be screened for correlation or
//!    hedged, so it cannot enter a pair regardless of what Alpaca allows.
//! 3. **It is liquid enough to model.** The same thresholds the model trains on, applied per ticker
//!    average. A universe wider than the training population means predicting on names the scaler
//!    never saw.
//!
//! Roughly seven thousand symbols enter and a few hundred survive, so it is computed once at
//! pre-open and held for the Eastern date. The universe does not change intraday.

use std::collections::HashSet;

use chrono::{DateTime, Utc};
use sqlx::PgPool;
use tracing::info;

use uuid::Uuid;

use crate::common::alpaca::{ClientError, TradableAssets, TradingClient};
use crate::common::journal::{Journal, Observation, UniverseRefreshed};
use crate::common::types::{BarInterval, SessionDate, Ticker, MINIMUM_CLOSE_PRICE, MINIMUM_VOLUME};
use crate::data::cache::DailyCache;

/// Trailing window over which liquidity is averaged.
///
/// Long enough that a single quiet week does not evict a normally liquid name, short enough to
/// notice one that has genuinely dried up.
const LIQUIDITY_LOOKBACK_DAYS: i64 = 30;

/// Errors building the universe.
#[derive(Debug, thiserror::Error)]
pub enum UniverseError {
    #[error("failed to read the asset universe from Alpaca: {0}")]
    Alpaca(#[from] ClientError),
    #[error("failed to read liquidity history: {0}")]
    Database(#[from] sqlx::Error),
}

/// One ticker's average liquidity over the trailing window.
#[derive(Debug, Clone, PartialEq)]
pub struct LiquidityRow {
    ticker: Ticker,
    average_close_price: f64,
    average_volume: f64,
}

impl LiquidityRow {
    pub fn new(ticker: Ticker, average_close_price: f64, average_volume: f64) -> Self {
        Self {
            ticker,
            average_close_price,
            average_volume,
        }
    }

    /// Whether this ticker clears both liquidity thresholds.
    fn is_liquid(&self) -> bool {
        self.average_close_price >= MINIMUM_CLOSE_PRICE && self.average_volume >= MINIMUM_VOLUME
    }
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
    /// Composes the three filters.
    ///
    /// `assets` is what Alpaca permits, `liquidity` is what our own bar history supports. A ticker
    /// must appear in both, and clear the thresholds, to survive.
    pub fn build(assets: &TradableAssets, liquidity: &[LiquidityRow]) -> Self {
        let mut tickers = Vec::new();
        let mut shortable = HashSet::new();

        for row in liquidity {
            if !row.is_liquid() {
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

/// Reads per-ticker average close and volume over the trailing window.
///
/// Averaged over daily bars specifically: the liquidity thresholds are calibrated on daily
/// dynamics, and averaging intraday bars would compare a per-minute volume against a per-day
/// threshold and reject the entire universe.
pub async fn load_liquidity(
    pool: &PgPool,
    as_of: SessionDate,
) -> Result<Vec<LiquidityRow>, sqlx::Error> {
    let start = as_of.plus_calendar_days(-LIQUIDITY_LOOKBACK_DAYS);
    let rows = sqlx::query!(
        r#"
        SELECT ticker AS "ticker!",
               AVG(close_price) AS "average_close_price!",
               AVG(volume::double precision) AS "average_volume!"
        FROM equity_bars
        WHERE bar_interval = $1
          AND timestamp >= $2
        GROUP BY ticker
        "#,
        BarInterval::OneDay.as_str(),
        start.midnight(),
    )
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .filter_map(|row| {
            Ticker::new(&row.ticker).map(|ticker| {
                LiquidityRow::new(ticker, row.average_close_price, row.average_volume)
            })
        })
        .collect())
}

/// A [`Universe`] rebuilt at most once per Eastern date.
///
/// Warmed by the pre-open handler and refreshed on demand by anything that finds it cold, so a
/// restart mid-session repopulates rather than trading an empty universe until the next morning.
#[derive(Default)]
pub struct UniverseCache {
    inner: DailyCache<Universe>,
}

impl UniverseCache {
    pub fn new() -> Self {
        Self::default()
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

        self.inner
            .get(
                today,
                || async {
                    let previous = self.inner.previous().await;
                    let assets = client.fetch_tradable_assets().await?;
                    let liquidity = load_liquidity(pool, today).await?;
                    let universe = Universe::build(&assets, &liquidity);

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
                                liquid: liquidity.iter().filter(|row| row.is_liquid()).count(),
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
        LiquidityRow::new(ticker(symbol), 100.0, 5_000_000.0)
    }

    #[test]
    fn test_build_keeps_liquid_tradable_tickers() {
        let universe = Universe::build(&assets(), &[liquid("AAPL"), liquid("MSFT")]);
        assert_eq!(universe.tickers(), &[ticker("AAPL"), ticker("MSFT")]);
        assert_eq!(universe.len(), 2);
    }

    /// A ticker Alpaca permits but we have no history for cannot be screened or hedged, so it must
    /// not enter the universe even though nothing about it is wrong.
    #[test]
    fn test_build_excludes_tickers_without_history() {
        let universe = Universe::build(&assets(), &[liquid("AAPL")]);
        assert!(!universe.tickers().contains(&ticker("NVDA")));
    }

    /// A ticker we have history for but Alpaca will not trade must be excluded, which is the case
    /// a delisting produces.
    #[test]
    fn test_build_excludes_tickers_alpaca_will_not_trade() {
        let universe = Universe::build(&assets(), &[liquid("AAPL"), liquid("GONE")]);
        assert!(!universe.tickers().contains(&ticker("GONE")));
    }

    /// Both thresholds must bind independently: a cheap-but-heavily-traded name and an
    /// expensive-but-untraded name are each excluded for their own reason.
    #[test]
    fn test_build_applies_both_liquidity_thresholds() {
        let cheap = LiquidityRow::new(ticker("PENNY"), MINIMUM_CLOSE_PRICE - 0.01, 50_000_000.0);
        let thin = LiquidityRow::new(ticker("THIN"), 500.0, MINIMUM_VOLUME - 1.0);

        let universe = Universe::build(&assets(), &[liquid("AAPL"), cheap, thin]);

        assert_eq!(universe.tickers(), &[ticker("AAPL")]);
    }

    #[test]
    fn test_thresholds_are_inclusive_at_the_boundary() {
        let exactly_at_threshold =
            LiquidityRow::new(ticker("AAPL"), MINIMUM_CLOSE_PRICE, MINIMUM_VOLUME);
        let universe = Universe::build(&assets(), &[exactly_at_threshold]);
        assert_eq!(universe.len(), 1, "the threshold itself must pass");
    }

    /// The short leg needs both Alpaca flags. A tradable-but-not-shortable name stays in the
    /// universe for the long leg and is excluded from the shortable subset.
    #[test]
    fn test_shortable_is_a_subset_of_tradable() {
        let universe =
            Universe::build(&assets(), &[liquid("AAPL"), liquid("MSFT"), liquid("NVDA")]);
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
        let universe =
            Universe::build(&assets(), &[liquid("NVDA"), liquid("AAPL"), liquid("MSFT")]);
        assert_eq!(
            universe.tickers(),
            &[ticker("AAPL"), ticker("MSFT"), ticker("NVDA")]
        );
    }

    #[test]
    fn test_empty_inputs_produce_an_empty_universe() {
        assert!(Universe::build(&assets(), &[]).is_empty());
        assert!(Universe::build(&TradableAssets::default(), &[liquid("AAPL")]).is_empty());
    }

    #[tokio::test]
    async fn test_cache_serves_installed_universe_without_fetching() {
        let cache = UniverseCache::new();
        let now = "2026-06-10T14:00:00Z".parse::<DateTime<Utc>>().unwrap();
        let universe = Universe::build(&assets(), &[liquid("AAPL")]);
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
