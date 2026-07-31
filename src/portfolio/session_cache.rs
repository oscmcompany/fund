//! A [`Trading`] client that fetches the trading session once per Eastern date.
//!
//! `/v2/clock` was the highest-volume REST call in the system by a wide margin —
//! roughly 400 requests a session — and it returns a schedule that is fixed
//! before the open. The quote-stream producer polled it every 60 seconds, the
//! portfolio consumer fetched it per evaluation, and the rebalance pass fetched
//! it again inside the same pass the consumer had just fetched it for.
//!
//! Caching is applied here, as a decorator over the client, rather than at each
//! of those call sites. Wrapping once at construction means the producer — which
//! holds only an `Arc<dyn Trading>` and runs in its own task — gets the cache
//! without a signature change, and it leaves the deliberately divergent failure
//! handling at each call site exactly where it is.

use std::sync::Arc;

use chrono::Utc;

use crate::common::market_hours::MarketSession;
use crate::portfolio::alpaca::{
    AccountInfo, ClientError, LatestQuote, OrderFill, Position, TradableAssets, Trading,
};
use crate::portfolio::daily_cache::DailyCache;

/// Wraps a [`Trading`] client, serving the trading session from a per-date cache.
///
/// Every other method delegates untouched.
pub struct SessionCachingClient {
    inner: Arc<dyn Trading>,
    session: DailyCache<MarketSession>,
}

impl SessionCachingClient {
    /// Wraps `inner` so its trading session is fetched once per Eastern date.
    pub fn new(inner: Arc<dyn Trading>) -> Self {
        Self {
            inner,
            session: DailyCache::default(),
        }
    }
}

#[async_trait::async_trait]
impl Trading for SessionCachingClient {
    /// Returns the session for today, fetching only on the first call of the date.
    ///
    /// A fetch failure is propagated rather than cached, so each caller's own
    /// fallback still applies: the quote-stream producer falls back to the fixed
    /// 09:25–16:05 window and keeps streaming, the trading paths skip. Those two
    /// behaviors are opposite on purpose and this cache does not unify them.
    ///
    /// **[`MarketSession::is_open`] on the returned value is as of the fetch, not
    /// as of now.** It is the one field of the session that is not fixed for the
    /// day, so a value cached before the open reports `false` all session. Use
    /// [`MarketSession::contains`], which derives liveness from the schedule, for
    /// "is the market open right now"; `trades_on_date_of`, `close`, `open`, and
    /// the window helpers are all schedule-derived and safe to read directly.
    async fn fetch_market_session(&self) -> Result<MarketSession, ClientError> {
        self.session
            .get_or_fetch(Utc::now(), || self.inner.fetch_market_session())
            .await
    }

    async fn get_account(&self) -> Result<AccountInfo, ClientError> {
        self.inner.get_account().await
    }

    async fn submit_long_order(&self, ticker: &str, notional: f64) -> Result<String, ClientError> {
        self.inner.submit_long_order(ticker, notional).await
    }

    async fn submit_short_order(&self, ticker: &str, quantity: i64) -> Result<String, ClientError> {
        self.inner.submit_short_order(ticker, quantity).await
    }

    async fn get_order(&self, alpaca_order_id: &str) -> Result<OrderFill, ClientError> {
        self.inner.get_order(alpaca_order_id).await
    }

    async fn close_position(&self, ticker: &str) -> Result<bool, ClientError> {
        self.inner.close_position(ticker).await
    }

    async fn fetch_tradable_assets(&self) -> Result<TradableAssets, ClientError> {
        self.inner.fetch_tradable_assets().await
    }

    async fn cancel_order(&self, alpaca_order_id: &str) -> Result<bool, ClientError> {
        self.inner.cancel_order(alpaca_order_id).await
    }

    async fn fetch_positions(&self) -> Result<Vec<Position>, ClientError> {
        self.inner.fetch_positions().await
    }

    async fn fetch_latest_quotes(
        &self,
        symbols: &[String],
    ) -> Result<Vec<LatestQuote>, ClientError> {
        self.inner.fetch_latest_quotes(symbols).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::portfolio::alpaca::MockTrading;
    use chrono::{Duration, TimeZone};

    fn client_with_close(close_hour: u32) -> (Arc<MockTrading>, SessionCachingClient) {
        let mock = Arc::new(MockTrading {
            market_open: true,
            session_close: Utc.with_ymd_and_hms(2026, 3, 10, close_hour, 0, 0).unwrap(),
            ..MockTrading::default()
        });
        let cached = SessionCachingClient::new(mock.clone() as Arc<dyn Trading>);
        (mock, cached)
    }

    #[tokio::test]
    async fn test_repeated_calls_hit_the_clock_once() {
        let (mock, cached) = client_with_close(20);

        for _ in 0..5 {
            cached
                .fetch_market_session()
                .await
                .expect("mock session must resolve");
        }

        assert_eq!(mock.market_session_fetch_count(), 1);
    }

    #[tokio::test]
    async fn test_fetch_failure_is_not_cached() {
        let mock = Arc::new(MockTrading {
            should_fail_session_fetch: true,
            ..MockTrading::default()
        });
        let cached = SessionCachingClient::new(mock.clone() as Arc<dyn Trading>);

        assert!(cached.fetch_market_session().await.is_err());
        assert!(cached.fetch_market_session().await.is_err());

        // Both calls reached the client: a failure must not poison the day.
        assert_eq!(mock.market_session_fetch_count(), 2);
    }

    #[tokio::test]
    async fn test_other_methods_are_not_cached() {
        let (mock, cached) = client_with_close(20);

        cached.fetch_positions().await.expect("mock positions");
        cached.fetch_positions().await.expect("mock positions");

        assert_eq!(mock.position_fetch_count(), 2);
    }

    #[tokio::test]
    async fn test_cached_session_still_answers_schedule_questions() {
        let (_mock, cached) = client_with_close(20);
        let session = cached.fetch_market_session().await.unwrap();

        // 2026-03-10 20:00 UTC is 16:00 Eastern, so the session runs 09:30-16:00
        // Eastern and `contains` is the liveness question the cache preserves.
        let mid_session = Utc.with_ymd_and_hms(2026, 3, 10, 15, 0, 0).unwrap();
        let after_close = session.close() + Duration::hours(1);

        assert!(session.contains(mid_session));
        assert!(!session.contains(after_close));
        assert!(session.trades_on_date_of(mid_session));
    }
}
