//! A [`Trading`] client that answers the trading session without asking Alpaca.
//!
//! `/v2/clock` was the highest-volume REST call in the system by a wide margin —
//! roughly 400 requests a session — and it returns a schedule that is fixed
//! before the open. The quote-stream producer polled it every 60 seconds, the
//! portfolio consumer fetched it per evaluation, and the rebalance pass fetched
//! it again inside the same pass the consumer had just fetched it for.
//!
//! Two sources answer the question now, in order:
//!
//! 1. **The published calendar**, synced daily and held in memory. It carries
//!    the real open and close for every trading day in its horizon, so a
//!    half-day is the shortened session it actually is rather than an assumed
//!    16:00. This path makes no request at all.
//! 2. **`/v2/clock`**, cached per Eastern date, when the calendar has no
//!    coverage — a fresh database, a failed sync, a date past the horizon.
//!
//! The fallback is the reason the clock stays wired in at all. It also still
//! covers what the calendar structurally cannot: the calendar publishes the
//! *schedule*, so an unscheduled closure is invisible to it.
//!
//! This is a decorator over the client rather than a change at each call site.
//! Wrapping once at construction means the producer — which holds only an
//! `Arc<dyn Trading>` and runs in its own task — gets it without a signature
//! change, and the deliberately divergent failure handling at each call site
//! stays exactly where it is.

use std::sync::Arc;

use chrono::{DateTime, NaiveDate, TimeZone, Utc};
use chrono_tz::US::Eastern;
use tracing::debug;

use crate::common::market_hours::MarketSession;
use crate::data::market_calendar;
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
    /// **[`MarketSession::is_open`] on the returned value is as of when the
    /// session was established, not as of now.** It is the one field that is not
    /// fixed for the day, so a value cached before the open reports `false` all
    /// session. Use [`MarketSession::contains`], which derives liveness from the
    /// schedule, for "is the market open right now"; `trades_on_date_of`,
    /// `close`, `open`, and the window helpers are all schedule-derived and safe
    /// to read directly.
    async fn fetch_market_session(&self) -> Result<MarketSession, ClientError> {
        let now = Utc::now();
        if let Some(session) = session_from_calendar(now) {
            return Ok(session);
        }
        self.session
            .get_or_fetch(now, || self.inner.fetch_market_session())
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

/// Builds the session for `now` from the published calendar.
///
/// Returns the session the market is in, or the next one when today has closed
/// or does not trade — matching what `/v2/clock` reports, so callers see the
/// same shape from either source. `trades_on_date_of` remains how a caller asks
/// whether that session is today's.
///
/// `None` when the calendar has no coverage for the date, which is the signal to
/// fall back to the clock. An uncovered date is not the same as a closed one.
fn session_from_calendar(now: DateTime<Utc>) -> Option<MarketSession> {
    let today = now.with_timezone(&Eastern).date_naive();
    if !market_calendar::calendar_covers(today) {
        return None;
    }

    // Today's session, unless it has already closed — after the bell the
    // relevant session is the next one, which is what the clock reports too.
    let session = todays_session(today, now).or_else(|| {
        let (next_date, hours) = market_calendar::next_session_on_or_after(today.succ_opt()?)?;
        build_session(next_date, hours, now)
    })?;

    debug!(
        session_open = %session.open(),
        session_close = %session.close(),
        "Trading session answered from the published calendar"
    );
    Some(session)
}

/// Returns today's session when it exists and has not yet closed.
fn todays_session(today: NaiveDate, now: DateTime<Utc>) -> Option<MarketSession> {
    let hours = market_calendar::session_hours(today)?;
    let session = build_session(today, hours, now)?;
    (now < session.close()).then_some(session)
}

/// Resolves published Eastern hours on `date` into a session.
fn build_session(
    date: NaiveDate,
    hours: market_calendar::SessionHours,
    now: DateTime<Utc>,
) -> Option<MarketSession> {
    let open = eastern_instant(date, hours.open())?;
    let close = eastern_instant(date, hours.close())?;
    MarketSession::from_published_hours(open, close, now)
}

/// Resolves an Eastern local date and time to a UTC instant.
///
/// US DST transitions happen at 02:00 local, well before any session open, so
/// `single` always resolves for published hours; `earliest` is a total fallback
/// rather than a real case.
fn eastern_instant(date: NaiveDate, time: chrono::NaiveTime) -> Option<DateTime<Utc>> {
    let local = date.and_time(time);
    Eastern
        .from_local_datetime(&local)
        .single()
        .or_else(|| Eastern.from_local_datetime(&local).earliest())
        .map(|instant| instant.with_timezone(&Utc))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::portfolio::alpaca::MockTrading;
    use chrono::{Datelike, Duration, TimeZone};

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

    // --- Calendar-backed sessions ---
    //
    // These install into the process-wide published calendar, so they run
    // serially and restore an empty one afterwards. Fixtures sit in 2035,
    // outside every date any other test in the crate asserts on: a calendar
    // only overrides inside its own horizon, so a briefly-live install here
    // cannot be observed by an unmarked test reading `is_trading_day`.

    use crate::data::market_calendar::{self, MarketCalendar, SessionHours};
    use chrono::{NaiveDate, NaiveTime};
    use std::collections::BTreeMap;

    fn published_date(text: &str) -> NaiveDate {
        NaiveDate::parse_from_str(text, "%Y-%m-%d").unwrap()
    }

    fn time(hour: u32, minute: u32) -> NaiveTime {
        NaiveTime::from_hms_opt(hour, minute, 0).unwrap()
    }

    fn install_sessions(sessions: &[(&str, NaiveTime, NaiveTime)]) {
        let mut published = BTreeMap::new();
        for &(date_text, open, close) in sessions {
            published.insert(
                published_date(date_text),
                SessionHours::new(open, close).expect("valid hours"),
            );
        }
        market_calendar::install(MarketCalendar::new(published));
    }

    /// Resolves an Eastern wall-clock time on a fixture date to a UTC instant.
    fn eastern_instant(date_text: &str, hour: u32, minute: u32) -> DateTime<Utc> {
        Eastern
            .with_ymd_and_hms(
                published_date(date_text).year(),
                published_date(date_text).month(),
                published_date(date_text).day(),
                hour,
                minute,
                0,
            )
            .single()
            .expect("fixture instant is unambiguous")
            .with_timezone(&Utc)
    }

    #[test]
    #[serial_test::serial]
    fn test_a_regular_session_comes_from_the_calendar() {
        install_sessions(&[("2035-03-13", time(9, 30), time(16, 0))]);

        let session = session_from_calendar(eastern_instant("2035-03-13", 10, 0))
            .expect("the calendar covers today");
        assert!(session.contains(eastern_instant("2035-03-13", 10, 0)));
        assert!(!session.contains(eastern_instant("2035-03-13", 16, 30)));
        assert!(session.trades_on_date_of(eastern_instant("2035-03-13", 10, 0)));

        market_calendar::install(MarketCalendar::default());
    }

    /// The case that motivated this whole path.
    #[test]
    #[serial_test::serial]
    fn test_a_half_day_closes_when_the_calendar_says_it_does() {
        // Shaped like the day after Thanksgiving, which closes at 13:00.
        install_sessions(&[("2035-11-23", time(9, 30), time(13, 0))]);

        let midday = eastern_instant("2035-11-23", 12, 0);
        let session = session_from_calendar(midday).expect("the calendar covers the half day");
        assert!(session.contains(midday));

        // 14:00 is inside a regular session and outside this one. Deriving the
        // close as 16:00, which is all the static holiday table could do, would
        // have had the system trading into a closed market for three hours.
        assert!(!session.contains(eastern_instant("2035-11-23", 14, 0)));

        market_calendar::install(MarketCalendar::default());
    }

    #[test]
    #[serial_test::serial]
    fn test_a_closed_day_reports_the_next_session() {
        // A closed weekday inside the horizon: no row, so it does not trade.
        install_sessions(&[
            ("2035-07-03", time(9, 30), time(16, 0)),
            ("2035-07-06", time(9, 30), time(16, 0)),
        ]);

        let holiday_morning = eastern_instant("2035-07-04", 10, 0);
        let session = session_from_calendar(holiday_morning).expect("a later session is published");

        // Matches what /v2/clock reports on a holiday: the next session, which
        // `trades_on_date_of` then reads as "not today".
        assert!(!session.trades_on_date_of(holiday_morning));
        assert!(!session.contains(holiday_morning));
        assert_eq!(
            session.close(),
            eastern_instant("2035-07-06", 16, 0),
            "the next session's close should be Monday's"
        );

        market_calendar::install(MarketCalendar::default());
    }

    #[test]
    #[serial_test::serial]
    fn test_after_the_close_the_next_session_is_reported() {
        install_sessions(&[
            ("2035-03-13", time(9, 30), time(16, 0)),
            ("2035-03-14", time(9, 30), time(16, 0)),
        ]);

        let after_the_bell = eastern_instant("2035-03-13", 16, 30);
        let session = session_from_calendar(after_the_bell).expect("tomorrow is published");
        assert_eq!(session.close(), eastern_instant("2035-03-14", 16, 0));
        assert!(!session.trades_on_date_of(after_the_bell));

        market_calendar::install(MarketCalendar::default());
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn test_an_uncovered_date_falls_back_to_the_clock() {
        market_calendar::install(MarketCalendar::default());

        let (mock, cached) = client_with_close(20);
        cached
            .fetch_market_session()
            .await
            .expect("the clock fallback answers");

        assert_eq!(
            mock.market_session_fetch_count(),
            1,
            "an uncovered date must reach the clock"
        );
    }

    #[test]
    #[serial_test::serial]
    fn test_an_uncovered_date_yields_no_calendar_session() {
        install_sessions(&[("2035-03-13", time(9, 30), time(16, 0))]);

        // Well outside the published horizon: the calendar declines rather than
        // reporting the date closed, which is what routes it to the clock.
        assert!(session_from_calendar(eastern_instant("2036-06-02", 10, 0)).is_none());

        market_calendar::install(MarketCalendar::default());
    }
}
