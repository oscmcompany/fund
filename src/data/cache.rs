//! A value cached per Eastern date.
//!
//! Five caches in this module wanted this and each hand-rolled it.

use std::future::Future;

use crate::common::types::SessionDate;

/// A value cached per Eastern date, so the rollover invalidates it without a timer.
///
/// Not a bound on rebuilds. The lock is released across the rebuild and re-taken only to store, so
/// two callers arriving cold both rebuild, and a value `worth_caching` rejects is rebuilt on every
/// call until it passes. Both are harmless because a rebuild is a deterministic read of one date.
pub struct DailyCache<T> {
    inner: tokio::sync::Mutex<Option<(SessionDate, T)>>,
}

impl<T> Default for DailyCache<T> {
    fn default() -> Self {
        Self {
            inner: tokio::sync::Mutex::new(None),
        }
    }
}

impl<T: Clone> DailyCache<T> {
    pub fn new() -> Self {
        Self::default()
    }

    /// Today's value, rebuilding it when the cache is cold or was filled on an earlier date.
    ///
    /// `worth_caching` decides whether the rebuilt value is stored, and the answer is not always
    /// yes. An empty universe or an empty close history is a failed read, and storing one would
    /// answer "nothing is there" for the rest of the Eastern date; an empty splits table is a real
    /// answer that should be kept.
    pub async fn get<Error, Rebuild, Rebuilding>(
        &self,
        today: SessionDate,
        rebuild: Rebuild,
        worth_caching: impl Fn(&T) -> bool,
    ) -> Result<T, Error>
    where
        Rebuild: FnOnce() -> Rebuilding,
        Rebuilding: Future<Output = Result<T, Error>>,
    {
        if let Some(fresh) = self.get_if_fresh(today).await {
            return Ok(fresh);
        }

        let value = rebuild().await?;
        if worth_caching(&value) {
            *self.inner.lock().await = Some((today, value.clone()));
        }
        Ok(value)
    }

    /// The cached value when it was filled on `today`, without rebuilding.
    pub async fn get_if_fresh(&self, today: SessionDate) -> Option<T> {
        match self.inner.lock().await.as_ref() {
            Some((cached_date, value)) if *cached_date == today => Some(value.clone()),
            _ => None,
        }
    }

    /// The cached value whatever date it was filled on.
    ///
    /// For a rebuild that reports what changed: the universe names what entered and what fell out,
    /// which needs the previous answer as well as the new one.
    pub async fn previous(&self) -> Option<T> {
        self.inner
            .lock()
            .await
            .as_ref()
            .map(|(_, value)| value.clone())
    }

    /// Replaces the cached value. Used by tests and by the pre-open warm path.
    pub async fn install(&self, today: SessionDate, value: T) {
        *self.inner.lock().await = Some((today, value));
    }

    /// Drops the cached value so the next caller rebuilds.
    ///
    /// Not the same as installing an empty one: an empty value keyed to today would answer "nothing
    /// is there" for the rest of the Eastern date rather than reloading.
    pub async fn invalidate(&self) {
        *self.inner.lock().await = None;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn session(value: &str) -> SessionDate {
        SessionDate::from_date(value.parse().expect("a valid session date"))
    }

    /// Counts rebuilds so a cache hit is proved by the rebuild not running, rather than by the
    /// value happening to match.
    async fn counted(
        cache: &DailyCache<usize>,
        today: SessionDate,
        rebuilds: &AtomicUsize,
        value: usize,
        worth_caching: impl Fn(&usize) -> bool,
    ) -> usize {
        cache
            .get::<(), _, _>(
                today,
                || async {
                    rebuilds.fetch_add(1, Ordering::Relaxed);
                    Ok(value)
                },
                worth_caching,
            )
            .await
            .expect("the rebuild cannot fail")
    }

    #[tokio::test]
    async fn test_a_second_read_on_the_same_date_does_not_rebuild() {
        let cache = DailyCache::new();
        let rebuilds = AtomicUsize::new(0);
        let today = session("2026-08-17");

        assert_eq!(counted(&cache, today, &rebuilds, 1, |_| true).await, 1);
        assert_eq!(counted(&cache, today, &rebuilds, 2, |_| true).await, 1);
        assert_eq!(rebuilds.load(Ordering::Relaxed), 1);
    }

    /// The rollover is the whole point of keying on the date.
    #[tokio::test]
    async fn test_the_next_eastern_date_rebuilds() {
        let cache = DailyCache::new();
        let rebuilds = AtomicUsize::new(0);

        assert_eq!(
            counted(&cache, session("2026-08-17"), &rebuilds, 1, |_| true).await,
            1
        );
        assert_eq!(
            counted(&cache, session("2026-08-18"), &rebuilds, 2, |_| true).await,
            2
        );
        assert_eq!(rebuilds.load(Ordering::Relaxed), 2);
    }

    /// A refused value is still returned to the caller that asked for it, and still rebuilt for the
    /// next one. Storing it would answer "nothing is there" until the date rolls over.
    #[tokio::test]
    async fn test_a_value_not_worth_caching_is_returned_but_not_kept() {
        let cache = DailyCache::new();
        let rebuilds = AtomicUsize::new(0);
        let today = session("2026-08-17");
        let non_empty = |value: &usize| *value > 0;

        assert_eq!(counted(&cache, today, &rebuilds, 0, non_empty).await, 0);
        assert_eq!(counted(&cache, today, &rebuilds, 0, non_empty).await, 0);
        assert_eq!(rebuilds.load(Ordering::Relaxed), 2, "neither was cached");

        assert_eq!(counted(&cache, today, &rebuilds, 7, non_empty).await, 7);
        assert_eq!(counted(&cache, today, &rebuilds, 9, non_empty).await, 7);
        assert_eq!(rebuilds.load(Ordering::Relaxed), 3, "the third one stuck");
    }

    #[tokio::test]
    async fn test_invalidate_rebuilds_where_installing_an_empty_value_would_not() {
        let cache = DailyCache::new();
        let rebuilds = AtomicUsize::new(0);
        let today = session("2026-08-17");

        cache.install(today, 5).await;
        assert_eq!(counted(&cache, today, &rebuilds, 1, |_| true).await, 5);
        assert_eq!(rebuilds.load(Ordering::Relaxed), 0);

        cache.invalidate().await;
        assert_eq!(counted(&cache, today, &rebuilds, 1, |_| true).await, 1);
        assert_eq!(rebuilds.load(Ordering::Relaxed), 1);
    }

    /// `previous` ignores the date, which is what makes a day-over-day difference possible.
    #[tokio::test]
    async fn test_previous_answers_across_the_rollover() {
        let cache = DailyCache::new();
        assert_eq!(cache.previous().await, None);

        cache.install(session("2026-08-17"), 5).await;
        assert_eq!(cache.get_if_fresh(session("2026-08-18")).await, None);
        assert_eq!(cache.previous().await, Some(5));
    }
}
