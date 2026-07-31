//! A value fetched once per Eastern trading date and reused for the rest of it.
//!
//! Two things the portfolio service asks Alpaca for are fixed for a whole
//! session — the trading session schedule and the tradable asset universe — and
//! both were being re-fetched far more often than they change. The session was
//! the highest-volume REST call in the system at roughly 400 requests a day; the
//! asset universe sat in a cache with no invalidation at all, so a process
//! spanning several sessions never re-read shortability.
//!
//! Both are the same shape: fetch on the first miss of the day, reuse until the
//! Eastern date rolls over. Keying on the Eastern date rather than on an elapsed
//! duration means a restart mid-session repopulates immediately and a rollover
//! invalidates without a timer.

use std::future::Future;
use std::sync::Arc;

use chrono::{DateTime, NaiveDate, Utc};
use chrono_tz::US::Eastern;
use tokio::sync::RwLock;

/// A `T` held for the Eastern date it was fetched on.
///
/// Cloneable: clones share one underlying cache, so a handle passed to another
/// task reads and populates the same slot.
#[derive(Clone)]
pub struct DailyCache<T> {
    held: Arc<RwLock<Option<(NaiveDate, T)>>>,
}

impl<T> Default for DailyCache<T> {
    fn default() -> Self {
        Self {
            held: Arc::new(RwLock::new(None)),
        }
    }
}

impl<T: Clone> DailyCache<T> {
    /// Returns the value held for `now`'s Eastern date, fetching it on a miss.
    ///
    /// A failed fetch is propagated and nothing is cached, so the caller's own
    /// fallback applies and the next call retries. That matters because the two
    /// callers of this handle a fetch failure in deliberately opposite ways —
    /// the quote-stream producer fails open and keeps streaming, the trading
    /// paths fail closed and skip — and a cache that swallowed or memoized the
    /// error would quietly unify them.
    pub async fn get_or_fetch<Fetch, Fetching, Error>(
        &self,
        now: DateTime<Utc>,
        fetch: Fetch,
    ) -> Result<T, Error>
    where
        Fetch: FnOnce() -> Fetching,
        Fetching: Future<Output = Result<T, Error>>,
    {
        let today = now.with_timezone(&Eastern).date_naive();

        // The read guard is bound to a block so its release point is explicit.
        // As an `if let` scrutinee the temporary would live for the whole
        // statement including its body, and `tokio::sync::RwLock` is not
        // reentrant — so any future statement added to that body which took the
        // write lock would deadlock. Nothing does today; this removes the shape
        // that would let it happen quietly.
        let held = {
            let guard = self.held.read().await;
            guard
                .as_ref()
                .filter(|(held_date, _)| *held_date == today)
                .map(|(_, value)| value.clone())
        };
        if let Some(value) = held {
            return Ok(value);
        }

        // Two concurrent misses both fetch and the second overwrites the first
        // with an equal value. Holding the write lock across the fetch would
        // serialize every caller behind one network round-trip, which is the
        // cost this type exists to avoid.
        let value = fetch().await?;
        *self.held.write().await = Some((today, value.clone()));
        Ok(value)
    }

    /// Returns the value currently held, without fetching.
    ///
    /// For callers that want to know whether a session is already established
    /// rather than establish one.
    pub async fn peek(&self, now: DateTime<Utc>) -> Option<T> {
        let today = now.with_timezone(&Eastern).date_naive();
        self.held
            .read()
            .await
            .as_ref()
            .filter(|(held_date, _)| *held_date == today)
            .map(|(_, value)| value.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// 2026-03-10 14:00 UTC is 10:00 Eastern, mid-session.
    fn mid_session() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 3, 10, 14, 0, 0).unwrap()
    }

    #[tokio::test]
    async fn test_fetches_once_within_the_same_eastern_date() {
        let cache: DailyCache<u32> = DailyCache::default();
        let calls = AtomicUsize::new(0);

        let fetch = || async {
            calls.fetch_add(1, Ordering::SeqCst);
            Ok::<u32, ()>(7)
        };

        assert_eq!(cache.get_or_fetch(mid_session(), fetch).await, Ok(7));
        assert_eq!(cache.get_or_fetch(mid_session(), fetch).await, Ok(7));
        // Four hours later, same Eastern date.
        let later = mid_session() + chrono::Duration::hours(4);
        assert_eq!(cache.get_or_fetch(later, fetch).await, Ok(7));

        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_refetches_after_an_eastern_date_rollover() {
        let cache: DailyCache<u32> = DailyCache::default();
        let calls = AtomicUsize::new(0);

        let fetch = || async {
            let call = calls.fetch_add(1, Ordering::SeqCst);
            Ok::<u32, ()>(call as u32)
        };

        assert_eq!(cache.get_or_fetch(mid_session(), fetch).await, Ok(0));
        let next_day = mid_session() + chrono::Duration::days(1);
        assert_eq!(cache.get_or_fetch(next_day, fetch).await, Ok(1));
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn test_rollover_is_keyed_on_eastern_not_utc_date() {
        let cache: DailyCache<u32> = DailyCache::default();
        let calls = AtomicUsize::new(0);
        let fetch = || async {
            calls.fetch_add(1, Ordering::SeqCst);
            Ok::<u32, ()>(1)
        };

        // Both instants fall on Eastern date 2026-03-10 — 16:00 and 23:00 EDT —
        // but on different UTC dates, because 23:00 EDT is already 03:00 UTC the
        // next day. Keying on the UTC date would refetch at the second call.
        let afternoon = Utc.with_ymd_and_hms(2026, 3, 10, 20, 0, 0).unwrap();
        let late_evening = Utc.with_ymd_and_hms(2026, 3, 11, 3, 0, 0).unwrap();
        cache.get_or_fetch(afternoon, fetch).await.unwrap();
        cache.get_or_fetch(late_evening, fetch).await.unwrap();

        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_failed_fetch_caches_nothing_and_retries() {
        let cache: DailyCache<u32> = DailyCache::default();
        let calls = AtomicUsize::new(0);

        let failing = || async {
            calls.fetch_add(1, Ordering::SeqCst);
            Err::<u32, &str>("clock unavailable")
        };

        assert!(cache.get_or_fetch(mid_session(), failing).await.is_err());
        assert!(cache.get_or_fetch(mid_session(), failing).await.is_err());
        assert_eq!(calls.load(Ordering::SeqCst), 2);

        // A later success populates normally.
        let succeeding = || async { Ok::<u32, &str>(3) };
        assert_eq!(cache.get_or_fetch(mid_session(), succeeding).await, Ok(3));
        assert_eq!(cache.peek(mid_session()).await, Some(3));
    }

    #[tokio::test]
    async fn test_peek_returns_nothing_before_a_fetch_or_after_rollover() {
        let cache: DailyCache<u32> = DailyCache::default();
        assert_eq!(cache.peek(mid_session()).await, None);

        cache
            .get_or_fetch(mid_session(), || async { Ok::<u32, ()>(5) })
            .await
            .unwrap();
        assert_eq!(cache.peek(mid_session()).await, Some(5));

        let next_day = mid_session() + chrono::Duration::days(1);
        assert_eq!(cache.peek(next_day).await, None);
    }

    #[tokio::test]
    async fn test_clones_share_one_slot() {
        let cache: DailyCache<u32> = DailyCache::default();
        let handle = cache.clone();
        let calls = AtomicUsize::new(0);
        let fetch = || async {
            calls.fetch_add(1, Ordering::SeqCst);
            Ok::<u32, ()>(9)
        };

        cache.get_or_fetch(mid_session(), fetch).await.unwrap();
        handle.get_or_fetch(mid_session(), fetch).await.unwrap();

        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }
}
