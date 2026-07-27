//! In-memory cache of the latest streamed mid-price per ticker.
//!
//! Subscribes to the shared [`MarketDataBuffer`] and keeps only the most recent
//! quote for each symbol. Nothing here is persisted: the cache is the read side
//! of the ephemeral tier, and a restart simply repopulates it from the stream.
//!
//! Reads are guarded by a staleness window. A symbol that has stopped quoting —
//! halted, thinly traded, or dropped when the subscription set changed — reports
//! no price rather than a stale one, and callers fall back to the prior close.
//! Returning a minutes-old book as if it were current is the failure this guard
//! exists to prevent.

use std::collections::HashMap;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use tokio::sync::RwLock;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::domain::freshness::StalenessWindow;
use crate::domain::market::{EquityQuote, Ticker};
use crate::stream::buffer::MarketDataBuffer;

/// The most recent quote observed for a ticker.
#[derive(Debug, Clone, Copy)]
struct LiveMidPrice {
    mid_price: f64,
    observed_at: DateTime<Utc>,
}

/// Latest streamed mid-price per ticker, with a staleness guard on read.
#[derive(Clone)]
pub struct LivePriceCache {
    prices: Arc<RwLock<HashMap<Ticker, LiveMidPrice>>>,
    staleness_window: StalenessWindow,
}

impl Default for LivePriceCache {
    fn default() -> Self {
        Self::new(StalenessWindow::quotes())
    }
}

impl LivePriceCache {
    /// Creates an empty cache using `staleness_window` for read filtering.
    pub fn new(staleness_window: StalenessWindow) -> Self {
        Self {
            prices: Arc::new(RwLock::new(HashMap::new())),
            staleness_window,
        }
    }

    /// Records a quote, keeping the later of the stored and incoming timestamps.
    ///
    /// Out-of-order frames are ignored rather than overwriting a newer quote:
    /// the buffer is a broadcast channel with no ordering guarantee across
    /// reconnects, and rewinding a price would flip a spread back across a
    /// threshold it had already crossed.
    pub async fn record(&self, quote: &EquityQuote) {
        let mid_price = (quote.bid_price() + quote.ask_price()) / 2.0;
        if !mid_price.is_finite() || mid_price <= 0.0 {
            warn!(
                ticker = quote.ticker().as_str(),
                bid = quote.bid_price(),
                ask = quote.ask_price(),
                "Ignoring quote with an unusable mid-price"
            );
            return;
        }

        let mut prices = self.prices.write().await;
        match prices.get(quote.ticker()) {
            Some(existing) if existing.observed_at >= quote.timestamp() => return,
            _ => {}
        }
        prices.insert(
            quote.ticker().clone(),
            LiveMidPrice {
                mid_price,
                observed_at: quote.timestamp(),
            },
        );
    }

    /// Returns every mid-price still inside the staleness window at `now`.
    ///
    /// Stale entries are filtered rather than evicted: the symbol may resume
    /// quoting, and dropping it would lose the timestamp used to decide that.
    pub async fn fresh_mid_prices(&self, now: DateTime<Utc>) -> HashMap<Ticker, f64> {
        let prices = self.prices.read().await;
        prices
            .iter()
            .filter(|(_, price)| {
                now.signed_duration_since(price.observed_at) <= self.staleness_window.0
            })
            .map(|(ticker, price)| (ticker.clone(), price.mid_price))
            .collect()
    }

    /// Returns the number of cached symbols, fresh or not.
    pub async fn len(&self) -> usize {
        self.prices.read().await.len()
    }

    /// Returns whether the cache holds no quotes at all.
    pub async fn is_empty(&self) -> bool {
        self.len().await == 0
    }
}

/// Spawns the task that drains the quote buffer into `cache`.
///
/// `BufferSubscriber::receive` already absorbs broadcast lag by skipping to the
/// current position, which is exactly right here: a lagged reader has missed
/// intermediate quotes, but the next one it reads is the current price, and the
/// current price is all this cache stores.
pub fn spawn_live_price_cache(
    cache: LivePriceCache,
    buffer: Arc<MarketDataBuffer<EquityQuote>>,
    shutdown_token: CancellationToken,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut subscriber = buffer.subscribe();
        info!("Live price cache subscribed to the quote buffer");

        loop {
            tokio::select! {
                received = subscriber.receive() => match received {
                    Some(quote) => cache.record(&quote).await,
                    None => {
                        info!("Quote buffer closed; live price cache draining");
                        break;
                    }
                },
                _ = shutdown_token.cancelled() => break,
            }
        }

        info!("Live price cache stopped");
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration;

    fn ticker(symbol: &str) -> Ticker {
        Ticker::new(symbol).expect("valid ticker")
    }

    fn quote_at(symbol: &str, bid: f64, ask: f64, observed_at: DateTime<Utc>) -> EquityQuote {
        EquityQuote::new(observed_at, ticker(symbol), bid, ask, 1, 1)
    }

    #[tokio::test]
    async fn test_record_stores_mid_price() {
        let now = Utc::now();
        let cache = LivePriceCache::default();
        cache.record(&quote_at("AAPL", 180.0, 180.2, now)).await;

        let prices = cache.fresh_mid_prices(now).await;
        assert!((prices[&ticker("AAPL")] - 180.1).abs() < 1e-9);
    }

    #[tokio::test]
    async fn test_later_quote_replaces_earlier() {
        let now = Utc::now();
        let cache = LivePriceCache::default();
        cache
            .record(&quote_at("AAPL", 180.0, 180.0, now - Duration::seconds(10)))
            .await;
        cache.record(&quote_at("AAPL", 181.0, 181.0, now)).await;

        let prices = cache.fresh_mid_prices(now).await;
        assert!((prices[&ticker("AAPL")] - 181.0).abs() < 1e-9);
    }

    #[tokio::test]
    async fn test_out_of_order_quote_is_ignored() {
        // A reconnect can replay an older frame. Rewinding the price would flip
        // a spread back across a threshold it had already crossed.
        let now = Utc::now();
        let cache = LivePriceCache::default();
        cache.record(&quote_at("AAPL", 181.0, 181.0, now)).await;
        cache
            .record(&quote_at("AAPL", 170.0, 170.0, now - Duration::seconds(30)))
            .await;

        let prices = cache.fresh_mid_prices(now).await;
        assert!((prices[&ticker("AAPL")] - 181.0).abs() < 1e-9);
    }

    #[tokio::test]
    async fn test_stale_quote_is_not_returned() {
        let now = Utc::now();
        let cache = LivePriceCache::default();
        cache
            .record(&quote_at("AAPL", 180.0, 180.2, now - Duration::seconds(61)))
            .await;

        assert!(cache.fresh_mid_prices(now).await.is_empty());
        // Still cached, just not fresh — the symbol may resume quoting.
        assert_eq!(cache.len().await, 1);
    }

    #[tokio::test]
    async fn test_quote_at_staleness_boundary_is_fresh() {
        let now = Utc::now();
        let cache = LivePriceCache::default();
        cache
            .record(&quote_at("AAPL", 180.0, 180.2, now - Duration::seconds(60)))
            .await;

        assert_eq!(cache.fresh_mid_prices(now).await.len(), 1);
    }

    #[tokio::test]
    async fn test_stale_symbol_does_not_hide_fresh_one() {
        let now = Utc::now();
        let cache = LivePriceCache::default();
        cache
            .record(&quote_at(
                "AAPL",
                180.0,
                180.2,
                now - Duration::seconds(120),
            ))
            .await;
        cache.record(&quote_at("MSFT", 300.0, 300.2, now)).await;

        let prices = cache.fresh_mid_prices(now).await;
        assert_eq!(prices.len(), 1);
        assert!(prices.contains_key(&ticker("MSFT")));
    }

    #[tokio::test]
    async fn test_unusable_mid_price_is_rejected() {
        // A zero or negative book would silently corrupt every spread it feeds.
        let now = Utc::now();
        let cache = LivePriceCache::default();
        cache.record(&quote_at("AAPL", 0.0, 0.0, now)).await;
        cache.record(&quote_at("MSFT", -1.0, 1.0, now)).await;

        assert!(cache.is_empty().await);
    }

    #[tokio::test]
    async fn test_cache_starts_empty() {
        assert!(LivePriceCache::default().is_empty().await);
    }
}
