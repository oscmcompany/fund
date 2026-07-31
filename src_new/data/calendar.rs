//! The trading calendar, as Alpaca publishes it.
//!
//! Two questions get asked constantly — does the market trade today, and when does it close — and
//! both are answered from a calendar fetched once and held in memory for the Eastern date. There is
//! no `market_calendar` table any more: the calendar is reference data that Alpaca owns, and
//! persisting a copy bought nothing except a second source that could disagree.
//!
//! There is also no hardcoded holiday fallback. The old one could not express a half-day, so every
//! early close looked like a 16:00 session and the fail-safe liquidation fired hours after the
//! market had shut. More importantly it made an unreachable Alpaca look like a normal trading day.
//! **An absent calendar now means "do not trade", not "assume open"** — see
//! [`TradingCalendar::is_trading_day`], which answers `false` for a date outside its horizon.
//!
//! Everything here is Eastern local time, because that is the timezone the trading day actually has.

use std::collections::BTreeMap;

use chrono::{DateTime, Datelike, Duration, NaiveDate, NaiveTime, Utc};
use chrono_tz::America::New_York;
use tracing::{info, warn};

use crate::common::alpaca::{CalendarDay, ClientError, TradingClient};

/// How far forward to fetch published sessions.
///
/// Wide enough that trading-day arithmetic — the previous session, the next session, a lookback
/// window — never runs off the end during a session, and narrow enough to stay one small request.
const HORIZON_DAYS_FORWARD: i64 = 90;

/// How far back to fetch published sessions.
///
/// The bar sync asks for the previous trading day, and a long weekend plus a holiday is the worst
/// case. Thirty days is generous.
const HORIZON_DAYS_BACKWARD: i64 = 30;

/// The Eastern calendar date at an instant.
///
/// The trading day rolls over at Eastern midnight, not UTC midnight, and the two differ for five
/// hours of every day. Every cache key, session comparison, and "is this still today" check in the
/// system goes through here.
pub fn eastern_date(instant: DateTime<Utc>) -> NaiveDate {
    instant.with_timezone(&New_York).date_naive()
}

/// The Eastern wall-clock time at an instant.
pub fn eastern_time(instant: DateTime<Utc>) -> NaiveTime {
    instant.with_timezone(&New_York).time()
}

/// Published trading sessions over a bounded horizon.
///
/// A `TradingCalendar` in scope is proof of nothing about any particular date — ask
/// [`TradingCalendar::is_trading_day`]. It is proof that the days it does contain came from Alpaca
/// with their real hours.
#[derive(Debug, Clone, Default)]
pub struct TradingCalendar {
    days: BTreeMap<NaiveDate, CalendarDay>,
}

impl TradingCalendar {
    /// Builds a calendar from published days.
    pub fn from_days(days: Vec<CalendarDay>) -> Self {
        Self {
            days: days
                .into_iter()
                .map(|day| (day.session_date(), day))
                .collect(),
        }
    }

    /// Whether the market trades on `date`.
    ///
    /// A date outside the published horizon answers `false`. That is deliberately conservative: the
    /// caller cannot distinguish "holiday" from "we do not know", and the safe response to not
    /// knowing is to sit out rather than to trade into a closed or unknown market.
    pub fn is_trading_day(&self, date: NaiveDate) -> bool {
        self.days.contains_key(&date)
    }

    /// The published session for `date`, if it trades.
    pub fn session(&self, date: NaiveDate) -> Option<&CalendarDay> {
        self.days.get(&date)
    }

    /// Whether the calendar has an opinion about `date` at all.
    ///
    /// Distinct from [`TradingCalendar::is_trading_day`]: a date inside the horizon with no session
    /// is a holiday, while a date outside it is unknown. Both answer `false` to "does it trade",
    /// but only the second means the calendar needs refreshing.
    pub fn covers(&self, date: NaiveDate) -> bool {
        match self.horizon() {
            Some((first, last)) => date >= first && date <= last,
            None => false,
        }
    }

    /// The first and last published dates, if any.
    pub fn horizon(&self) -> Option<(NaiveDate, NaiveDate)> {
        let first = *self.days.keys().next()?;
        let last = *self.days.keys().next_back()?;
        Some((first, last))
    }

    /// The number of published sessions held.
    pub fn len(&self) -> usize {
        self.days.len()
    }

    pub fn is_empty(&self) -> bool {
        self.days.is_empty()
    }

    /// The most recent trading day strictly before `date`.
    ///
    /// This is what the post-close bar sync asks for: the session whose bars are now final.
    pub fn previous_trading_day(&self, date: NaiveDate) -> Option<NaiveDate> {
        self.days.range(..date).next_back().map(|(day, _)| *day)
    }

    /// The next trading day on or after `date`.
    pub fn next_trading_day(&self, date: NaiveDate) -> Option<NaiveDate> {
        self.days.range(date..).next().map(|(day, _)| *day)
    }

    /// Every trading day in an inclusive range.
    pub fn trading_days_in_range(&self, start: NaiveDate, end: NaiveDate) -> Vec<NaiveDate> {
        self.days.range(start..=end).map(|(day, _)| *day).collect()
    }

    /// Minutes remaining until today's close, or `None` when today does not trade or has already
    /// closed.
    ///
    /// The entry path uses this to refuse opening a pair it cannot plausibly exit before the bell,
    /// which is the one piece of the old end-of-day feasibility check worth keeping. Reading the
    /// close from the published calendar rather than assuming 16:00 is what makes it correct on a
    /// half-day.
    pub fn minutes_until_close(&self, instant: DateTime<Utc>) -> Option<i64> {
        let session = self.session(eastern_date(instant))?;
        let now = eastern_time(instant);
        if now >= session.session_close() {
            return None;
        }
        Some((session.session_close() - now).num_minutes())
    }

    /// Whether the regular session is open at `instant`.
    pub fn is_open_at(&self, instant: DateTime<Utc>) -> bool {
        match self.session(eastern_date(instant)) {
            Some(session) => {
                let now = eastern_time(instant);
                now >= session.session_open() && now < session.session_close()
            }
            None => false,
        }
    }
}

/// A [`TradingCalendar`] refreshed from Alpaca at most once per Eastern date.
///
/// The pre-open handler warms it; anything that finds it cold or stale refreshes on demand, so a
/// restart mid-session repopulates without waiting for the next morning. Keying on the Eastern date
/// rather than an elapsed duration means the rollover invalidates without a timer.
#[derive(Default)]
pub struct CalendarCache {
    inner: tokio::sync::Mutex<Option<(NaiveDate, TradingCalendar)>>,
}

impl CalendarCache {
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns today's calendar, fetching it if the cache is cold or was filled on an earlier date.
    pub async fn get(
        &self,
        client: &TradingClient,
        now: DateTime<Utc>,
    ) -> Result<TradingCalendar, ClientError> {
        let today = eastern_date(now);
        let mut guard = self.inner.lock().await;

        if let Some((cached_date, calendar)) = guard.as_ref() {
            if *cached_date == today {
                return Ok(calendar.clone());
            }
        }

        let start = today - Duration::days(HORIZON_DAYS_BACKWARD);
        let end = today + Duration::days(HORIZON_DAYS_FORWARD);
        let calendar = TradingCalendar::from_days(client.fetch_calendar(start, end).await?);

        if calendar.is_empty() {
            // Reported rather than cached: an empty calendar would otherwise pin "nothing trades"
            // for the rest of the day, and the next caller should get a real attempt.
            warn!(start = %start, end = %end, "Alpaca published no trading days for the horizon");
        } else {
            info!(
                sessions = calendar.len(),
                trades_today = calendar.is_trading_day(today),
                "Trading calendar cached"
            );
            *guard = Some((today, calendar.clone()));
        }
        Ok(calendar)
    }

    /// Replaces the cached calendar. Used by tests and by the pre-open warm path.
    pub async fn install(&self, now: DateTime<Utc>, calendar: TradingCalendar) {
        *self.inner.lock().await = Some((eastern_date(now), calendar));
    }
}

/// Whether `date` falls on a weekend.
///
/// Not a substitute for [`TradingCalendar::is_trading_day`] — it knows nothing about holidays — but
/// useful for bounding a fetch range before the calendar is available.
pub fn is_weekend(date: NaiveDate) -> bool {
    matches!(date.weekday(), chrono::Weekday::Sat | chrono::Weekday::Sun)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn date(year: i32, month: u32, day: u32) -> NaiveDate {
        NaiveDate::from_ymd_opt(year, month, day).unwrap()
    }

    fn day(date_value: NaiveDate, open: (u32, u32), close: (u32, u32)) -> CalendarDay {
        CalendarDay::new(
            date_value,
            NaiveTime::from_hms_opt(open.0, open.1, 0).unwrap(),
            NaiveTime::from_hms_opt(close.0, close.1, 0).unwrap(),
        )
        .expect("test session must be valid")
    }

    /// A regular week plus a half-day, with a holiday and a weekend absent.
    fn calendar() -> TradingCalendar {
        TradingCalendar::from_days(vec![
            day(date(2026, 11, 24), (9, 30), (16, 0)),
            day(date(2026, 11, 25), (9, 30), (16, 0)),
            // 26 November is Thanksgiving: no session.
            day(date(2026, 11, 27), (9, 30), (13, 0)),
            // 28-29 November is the weekend.
            day(date(2026, 11, 30), (9, 30), (16, 0)),
        ])
    }

    #[test]
    fn test_holiday_is_not_a_trading_day() {
        assert!(!calendar().is_trading_day(date(2026, 11, 26)));
        assert!(calendar().is_trading_day(date(2026, 11, 27)));
    }

    /// The specific bug the hardcoded fallback could not express: a half-day must report its real
    /// 13:00 close, not an assumed 16:00.
    #[test]
    fn test_half_day_reports_its_real_close() {
        let calendar = calendar();
        let session = calendar
            .session(date(2026, 11, 27))
            .expect("half-day must be published");
        assert_eq!(
            session.session_close(),
            NaiveTime::from_hms_opt(13, 0, 0).unwrap()
        );
    }

    /// A date the calendar has never heard of must answer "does not trade". Answering "trades"
    /// would let an unreachable Alpaca look like a normal session.
    #[test]
    fn test_unknown_date_does_not_trade() {
        assert!(!calendar().is_trading_day(date(2030, 1, 2)));
        assert!(!calendar().covers(date(2030, 1, 2)));
    }

    /// An empty calendar must refuse every date rather than defaulting to open.
    #[test]
    fn test_empty_calendar_refuses_everything() {
        let empty = TradingCalendar::default();
        assert!(!empty.is_trading_day(date(2026, 11, 24)));
        assert!(!empty.covers(date(2026, 11, 24)));
        assert_eq!(empty.horizon(), None);
        assert!(empty.is_empty());
    }

    /// Holidays and weekends must both be skipped walking backwards, which is what the post-close
    /// bar sync depends on to find the session whose bars are final.
    #[test]
    fn test_previous_trading_day_skips_holiday_and_weekend() {
        let calendar = calendar();
        assert_eq!(
            calendar.previous_trading_day(date(2026, 11, 27)),
            Some(date(2026, 11, 25)),
            "Thanksgiving must be skipped"
        );
        assert_eq!(
            calendar.previous_trading_day(date(2026, 11, 30)),
            Some(date(2026, 11, 27)),
            "the weekend must be skipped"
        );
    }

    #[test]
    fn test_next_trading_day_is_inclusive_of_today() {
        let calendar = calendar();
        assert_eq!(
            calendar.next_trading_day(date(2026, 11, 26)),
            Some(date(2026, 11, 27))
        );
        assert_eq!(
            calendar.next_trading_day(date(2026, 11, 27)),
            Some(date(2026, 11, 27))
        );
    }

    #[test]
    fn test_trading_days_in_range_excludes_non_sessions() {
        assert_eq!(
            calendar().trading_days_in_range(date(2026, 11, 24), date(2026, 11, 30)),
            vec![
                date(2026, 11, 24),
                date(2026, 11, 25),
                date(2026, 11, 27),
                date(2026, 11, 30),
            ]
        );
    }

    /// 14:00 UTC on a standard-time day is 09:00 Eastern — before the open.
    #[test]
    fn test_is_open_at_respects_eastern_session_bounds() {
        let calendar = calendar();
        let before_open = "2026-11-24T14:00:00Z".parse::<DateTime<Utc>>().unwrap();
        let mid_session = "2026-11-24T16:00:00Z".parse::<DateTime<Utc>>().unwrap();
        let after_close = "2026-11-24T21:30:00Z".parse::<DateTime<Utc>>().unwrap();

        assert!(!calendar.is_open_at(before_open));
        assert!(calendar.is_open_at(mid_session));
        assert!(!calendar.is_open_at(after_close));
    }

    /// The entry guard reads this. On the half-day it must count down to 13:00, not 16:00 —
    /// otherwise the last entry pass opens a pair three hours after the market has shut.
    #[test]
    fn test_minutes_until_close_uses_the_published_close() {
        let calendar = calendar();
        // 17:00 UTC = 12:00 Eastern on a standard-time day.
        let noon_on_half_day = "2026-11-27T17:00:00Z".parse::<DateTime<Utc>>().unwrap();
        assert_eq!(calendar.minutes_until_close(noon_on_half_day), Some(60));

        let noon_on_full_day = "2026-11-24T17:00:00Z".parse::<DateTime<Utc>>().unwrap();
        assert_eq!(calendar.minutes_until_close(noon_on_full_day), Some(240));
    }

    #[test]
    fn test_minutes_until_close_is_none_after_the_bell_and_on_holidays() {
        let calendar = calendar();
        let after_close = "2026-11-27T20:00:00Z".parse::<DateTime<Utc>>().unwrap();
        let holiday = "2026-11-26T17:00:00Z".parse::<DateTime<Utc>>().unwrap();
        assert_eq!(calendar.minutes_until_close(after_close), None);
        assert_eq!(calendar.minutes_until_close(holiday), None);
    }

    /// The Eastern date must roll at Eastern midnight. 03:00 UTC is still the previous evening in
    /// New York, and a UTC-based key would put it on the wrong trading day.
    #[test]
    fn test_eastern_date_rolls_at_eastern_midnight() {
        let late_evening = "2026-06-11T03:00:00Z".parse::<DateTime<Utc>>().unwrap();
        assert_eq!(eastern_date(late_evening), date(2026, 6, 10));

        let morning = "2026-06-11T13:00:00Z".parse::<DateTime<Utc>>().unwrap();
        assert_eq!(eastern_date(morning), date(2026, 6, 11));
    }

    /// Daylight saving must be handled by the timezone database, not by an offset constant.
    /// 13:30 UTC is 09:30 Eastern in summer and 08:30 in winter.
    #[test]
    fn test_eastern_time_follows_daylight_saving() {
        let summer = "2026-06-10T13:30:00Z".parse::<DateTime<Utc>>().unwrap();
        let winter = "2026-01-14T13:30:00Z".parse::<DateTime<Utc>>().unwrap();
        assert_eq!(
            eastern_time(summer),
            NaiveTime::from_hms_opt(9, 30, 0).unwrap()
        );
        assert_eq!(
            eastern_time(winter),
            NaiveTime::from_hms_opt(8, 30, 0).unwrap()
        );
    }

    #[test]
    fn test_is_weekend() {
        assert!(is_weekend(date(2026, 11, 28)));
        assert!(is_weekend(date(2026, 11, 29)));
        assert!(!is_weekend(date(2026, 11, 27)));
    }

    #[tokio::test]
    async fn test_cache_serves_installed_calendar_without_fetching() {
        let cache = CalendarCache::new();
        let now = "2026-11-24T14:00:00Z".parse::<DateTime<Utc>>().unwrap();
        cache.install(now, calendar()).await;

        // An unreachable client: a cache hit must not touch it.
        let client = TradingClient::with_base_url(
            crate::common::alpaca::AlpacaCredentials::new("k".into(), "s".into()).unwrap(),
            "http://127.0.0.1:1".to_string(),
        );
        let served = cache
            .get(&client, now)
            .await
            .expect("cache hit must not fetch");
        assert!(served.is_trading_day(date(2026, 11, 24)));
    }
}
