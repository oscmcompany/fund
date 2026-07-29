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
use tracing::{debug, info};

use crate::domain::freshness::StalenessWindow;
use crate::domain::market::{BookQualityLimits, EquityQuote, Ticker, UsableQuote};
use crate::stream::buffer::MarketDataBuffer;

/// Latest streamed book per ticker, with a staleness guard on read.
///
/// Stores the validated quote rather than a bare mid so the spread it was drawn
/// from survives into the cache. Downstream filters need the spread to decide
/// entry eligibility, and recovering it from a midpoint is impossible.
#[derive(Clone)]
pub struct LivePriceCache {
    quotes: Arc<RwLock<HashMap<Ticker, UsableQuote>>>,
    staleness_window: StalenessWindow,
    book_limits: BookQualityLimits,
}

impl Default for LivePriceCache {
    fn default() -> Self {
        Self::new(StalenessWindow::quotes(), BookQualityLimits::default())
    }
}

impl LivePriceCache {
    /// Creates an empty cache using `staleness_window` and `book_limits` on read.
    pub fn new(staleness_window: StalenessWindow, book_limits: BookQualityLimits) -> Self {
        Self {
            quotes: Arc::new(RwLock::new(HashMap::new())),
            staleness_window,
            book_limits,
        }
    }

    /// Records a quote, keeping the later of the stored and incoming timestamps.
    ///
    /// Out-of-order frames are ignored rather than overwriting a newer quote:
    /// the buffer is a broadcast channel with no ordering guarantee across
    /// reconnects, and rewinding a price would flip a spread back across a
    /// threshold it had already crossed.
    pub async fn record(&self, quote: &EquityQuote) {
        let Some(usable) = UsableQuote::new(quote, self.book_limits) else {
            // Debug rather than warn: a symbol quoting one-sided or wide does so
            // on every frame, which on a busy feed is thousands of warnings a
            // second through the file appender.
            debug!(
                ticker = quote.ticker().as_str(),
                bid = quote.bid_price(),
                ask = quote.ask_price(),
                bid_size = quote.bid_size(),
                ask_size = quote.ask_size(),
                "Ignoring quote with an unusable book"
            );
            return;
        };

        let mut quotes = self.quotes.write().await;
        match quotes.get(quote.ticker()) {
            Some(existing) if existing.observed_at() >= quote.timestamp() => return,
            _ => {}
        }
        quotes.insert(quote.ticker().clone(), usable);
    }

    /// Returns every mid-price still inside the staleness window at `now`.
    ///
    /// Stale entries are filtered rather than evicted: the symbol may resume
    /// quoting, and dropping it would lose the timestamp used to decide that.
    pub async fn fresh_mid_prices(&self, now: DateTime<Utc>) -> HashMap<Ticker, f64> {
        self.fresh_quotes(now)
            .await
            .into_iter()
            .map(|(ticker, quote)| (ticker, quote.mid_price()))
            .collect()
    }

    /// Returns every validated quote still inside the staleness window at `now`.
    ///
    /// Callers needing the spread — entry eligibility, feed health logging — use
    /// this rather than reconstructing it from a midpoint, which cannot be done.
    pub async fn fresh_quotes(&self, now: DateTime<Utc>) -> HashMap<Ticker, UsableQuote> {
        let quotes = self.quotes.read().await;
        quotes
            .iter()
            .filter(|(_, quote)| {
                now.signed_duration_since(quote.observed_at()) <= self.staleness_window.0
            })
            .map(|(ticker, quote)| (ticker.clone(), *quote))
            .collect()
    }

    /// Returns the number of cached symbols, fresh or not.
    pub async fn len(&self) -> usize {
        self.quotes.read().await.len()
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

    /// Builds a quote sized at the round-lot minimum so fixtures exercise the
    /// price and staleness rules rather than tripping the size gate.
    fn quote_at(symbol: &str, bid: f64, ask: f64, observed_at: DateTime<Utc>) -> EquityQuote {
        EquityQuote::new(
            observed_at,
            ticker(symbol),
            bid,
            ask,
            crate::domain::market::MINIMUM_QUOTE_SIZE,
            crate::domain::market::MINIMUM_QUOTE_SIZE,
        )
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
    async fn test_unusable_book_is_rejected() {
        let now = Utc::now();
        let cache = LivePriceCache::default();
        cache.record(&quote_at("AAPL", 0.0, 0.0, now)).await;
        cache.record(&quote_at("MSFT", -1.0, 1.0, now)).await;
        // The case the previous fixtures missed: a one-sided book whose average
        // is a plausible-looking positive number. Validating only the mid would
        // cache 100.0 for a symbol with no bid.
        cache.record(&quote_at("GOOG", 0.0, 200.0, now)).await;
        cache.record(&quote_at("TSLA", 200.0, 0.0, now)).await;

        assert!(cache.is_empty().await);
    }

    #[tokio::test]
    async fn test_crossed_book_is_rejected() {
        // Bid above ask is a stale frame or a feed artifact; its midpoint is not
        // a price anything traded at.
        let now = Utc::now();
        let cache = LivePriceCache::default();
        cache.record(&quote_at("AAPL", 181.0, 180.0, now)).await;

        assert!(cache.is_empty().await);
    }

    #[tokio::test]
    async fn test_wide_book_is_rejected() {
        // 180.0 / 200.0 is a 1,053 basis point book. Its midpoint of 190 is not
        // a price either side would trade at, and both legs of a pair built on
        // it would carry that error into the spread.
        let now = Utc::now();
        let cache = LivePriceCache::default();
        cache.record(&quote_at("AAPL", 180.0, 200.0, now)).await;

        assert!(cache.is_empty().await);
    }

    #[tokio::test]
    async fn test_odd_lot_book_is_rejected() {
        // A tight spread quoted for a handful of shares is not liquidity a real
        // position can cross.
        let now = Utc::now();
        let cache = LivePriceCache::default();
        cache
            .record(&EquityQuote::new(
                now,
                ticker("AAPL"),
                180.0,
                180.2,
                1,
                crate::domain::market::MINIMUM_QUOTE_SIZE,
            ))
            .await;

        assert!(cache.is_empty().await);
    }

    #[tokio::test]
    async fn test_fresh_quotes_exposes_spread() {
        // The cache must retain the spread, not just the midpoint: entry
        // eligibility needs it and it cannot be recovered from a mid.
        let now = Utc::now();
        let cache = LivePriceCache::default();
        cache.record(&quote_at("AAPL", 180.0, 180.18, now)).await;

        let quotes = cache.fresh_quotes(now).await;
        let quote = quotes[&ticker("AAPL")];
        assert!((quote.mid_price() - 180.09).abs() < 1e-9);
        assert!((quote.relative_spread() * 10_000.0 - 9.995).abs() < 0.01);
    }

    #[tokio::test]
    async fn test_cache_starts_empty() {
        assert!(LivePriceCache::default().is_empty().await);
    }
}
