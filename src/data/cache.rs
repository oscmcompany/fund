//! A value cached per Eastern date.
//!
//! Five caches in this module wanted this and each hand-rolled it.

use std::future::Future;

use crate::common::types::SessionDate;

/// A value cached per Eastern date, so the rollover invalidates it without a timer.
///
/// Not a bound on rebuilds. The lock is released across the rebuild, so two callers arriving cold
/// both rebuild, and a value `worth_caching` rejects is rebuilt on every call until it passes.
pub struct DailyCache<T> {
    inner: tokio::sync::Mutex<Slot<T>>,
}

/// The cached value beside a counter of every write that has landed on it.
///
/// The counter is what lets a rebuild tell whether it was superseded while the lock was released.
/// It lives inside the mutex rather than beside it as an atomic so that the value and the count
/// cannot be read out of step with each other.
struct Slot<T> {
    value: Option<(SessionDate, T)>,
    writes: u64,
}

impl<T> Default for DailyCache<T> {
    fn default() -> Self {
        Self {
            inner: tokio::sync::Mutex::new(Slot {
                value: None,
                writes: 0,
            }),
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
    ///
    /// A rebuild that finds the slot written since it started discards its own result rather than
    /// storing it. Otherwise an [`DailyCache::invalidate`] landing mid-rebuild would be undone by
    /// the rebuild it was meant to invalidate, pinning the superseded value until the date rolls.
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
        let writes_before = {
            let slot = self.inner.lock().await;
            match &slot.value {
                Some((cached_date, value)) if *cached_date == today => return Ok(value.clone()),
                _ => slot.writes,
            }
        };

        let value = rebuild().await?;
        if worth_caching(&value) {
            let mut slot = self.inner.lock().await;
            if slot.writes == writes_before {
                slot.value = Some((today, value.clone()));
                slot.writes += 1;
            }
        }
        Ok(value)
    }

    /// The cached value when it was filled on `today`, without rebuilding.
    pub async fn get_if_fresh(&self, today: SessionDate) -> Option<T> {
        match &self.inner.lock().await.value {
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
            .value
            .as_ref()
            .map(|(_, value)| value.clone())
    }

    /// Replaces the cached value. Used by tests and by the pre-open warm path.
    pub async fn install(&self, today: SessionDate, value: T) {
        let mut slot = self.inner.lock().await;
        slot.value = Some((today, value));
        slot.writes += 1;
    }

    /// Drops the cached value so the next caller rebuilds.
    ///
    /// Not the same as installing an empty one: an empty value keyed to today would answer "nothing
    /// is there" for the rest of the Eastern date rather than reloading.
    pub async fn invalidate(&self) {
        let mut slot = self.inner.lock().await;
        slot.value = None;
        slot.writes += 1;
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

    /// An invalidation that lands while a rebuild is in flight must survive it.
    ///
    /// The post-close bar sync invalidates the close history because the cached window predates the
    /// rows it just wrote. A pass already loading that window read the old rows, so storing its
    /// result would undo the invalidation and serve pre-sync closes until the date rolled over.
    #[tokio::test]
    async fn test_an_invalidation_during_a_rebuild_is_not_undone_by_it() {
        let cache: DailyCache<usize> = DailyCache::new();
        let today = session("2026-08-17");
        let reading = tokio::sync::Notify::new();
        let invalidated = tokio::sync::Notify::new();

        let rebuilding = cache.get::<(), _, _>(
            today,
            || async {
                reading.notify_one();
                invalidated.notified().await;
                Ok(1)
            },
            |_| true,
        );

        let invalidating = async {
            reading.notified().await;
            cache.invalidate().await;
            invalidated.notify_one();
        };

        let (rebuilt, ()) = tokio::join!(rebuilding, invalidating);
        assert_eq!(
            rebuilt,
            Ok(1),
            "the caller still gets the value it asked for"
        );
        assert_eq!(
            cache.get_if_fresh(today).await,
            None,
            "the invalidation stands, so the next caller reloads"
        );
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
