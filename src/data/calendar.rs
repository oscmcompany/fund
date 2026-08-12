//! The trading calendar, as Alpaca publishes it.
//!
//! Two questions get asked constantly — does the market trade today, and when does it close — and
//! both are answered from a calendar fetched once and held in memory for the Eastern date. It is
//! deliberately not persisted: the calendar is reference data Alpaca owns, and a stored copy is
//! just a second source that can disagree.
//!
//! There is deliberately no hardcoded holiday fallback: one cannot express a half-day, and it
//! would make an unreachable Alpaca look like a normal trading day. **An absent calendar means "do
//! not trade", not "assume open"** — [`TradingCalendar::is_trading_day`] answers `false` outside
//! its horizon.
//!
//! Everything here is Eastern local time, because that is the timezone the trading day actually has.

use std::collections::BTreeMap;

use chrono::{DateTime, NaiveTime, Utc};
use chrono_tz::America::New_York;
use tracing::{info, warn};

use crate::common::alpaca::{CalendarDay, ClientError, TradingClient};
use crate::common::types::SessionDate;

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

/// The Eastern wall-clock time at an instant.
pub fn eastern_time(instant: DateTime<Utc>) -> NaiveTime {
    instant.with_timezone(&New_York).time()
}

/// The Eastern wall-clock date and time at an instant.
///
/// For callers that need both halves as one value — naming an artifact after the session that
/// produced it, where the date must be the Eastern one and the time is what orders two runs within
/// it. Reading [`SessionDate::at`] and [`eastern_time`] separately would work, but only because
/// both resolve the same zone; taking them from one conversion means they cannot disagree.
pub fn eastern_datetime(instant: DateTime<Utc>) -> chrono::NaiveDateTime {
    instant.with_timezone(&New_York).naive_local()
}

/// Published trading sessions over a bounded horizon.
///
/// A `TradingCalendar` in scope is proof of nothing about any particular date — ask
/// [`TradingCalendar::is_trading_day`]. It is proof that the days it does contain came from Alpaca
/// with their real hours.
#[derive(Debug, Clone, Default)]
pub struct TradingCalendar {
    days: BTreeMap<SessionDate, CalendarDay>,
}

impl TradingCalendar {
    /// Builds a calendar from published days.
    ///
    /// This is the boundary where a date Alpaca sent over the wire becomes a [`SessionDate`].
    /// [`CalendarDay`] stays on `NaiveDate` because it is what the transport parsed; everything
    /// above this line deals in sessions.
    pub fn from_days(days: Vec<CalendarDay>) -> Self {
        Self {
            days: days
                .into_iter()
                .map(|day| (SessionDate::from_date(day.session_date()), day))
                .collect(),
        }
    }

    /// Whether the market trades on `date`.
    ///
    /// A date outside the published horizon answers `false`. That is deliberately conservative: the
    /// caller cannot distinguish "holiday" from "we do not know", and the safe response to not
    /// knowing is to sit out rather than to trade into a closed or unknown market.
    pub fn is_trading_day(&self, date: SessionDate) -> bool {
        self.days.contains_key(&date)
    }

    /// The published session for `date`, if it trades.
    pub fn session(&self, date: SessionDate) -> Option<&CalendarDay> {
        self.days.get(&date)
    }

    /// Whether the calendar has an opinion about `date` at all.
    ///
    /// Distinct from [`TradingCalendar::is_trading_day`]: a date inside the horizon with no session
    /// is a holiday, while a date outside it is unknown. Both answer `false` to "does it trade",
    /// but only the second means the calendar needs refreshing.
    pub fn covers(&self, date: SessionDate) -> bool {
        match self.horizon() {
            Some((first, last)) => date >= first && date <= last,
            None => false,
        }
    }

    /// The first and last published dates, if any.
    pub fn horizon(&self) -> Option<(SessionDate, SessionDate)> {
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
    pub fn previous_trading_day(&self, date: SessionDate) -> Option<SessionDate> {
        self.days.range(..date).next_back().map(|(day, _)| *day)
    }

    /// The next trading day on or after `date`.
    pub fn next_trading_day(&self, date: SessionDate) -> Option<SessionDate> {
        self.days.range(date..).next().map(|(day, _)| *day)
    }

    /// Every trading day in an inclusive range.
    pub fn trading_days_in_range(&self, start: SessionDate, end: SessionDate) -> Vec<SessionDate> {
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
        let session = self.session(SessionDate::at(instant))?;
        let now = eastern_time(instant);
        if now >= session.session_close() {
            return None;
        }
        Some((session.session_close() - now).num_minutes())
    }

    /// Whether the regular session is open at `instant`.
    pub fn is_open_at(&self, instant: DateTime<Utc>) -> bool {
        match self.session(SessionDate::at(instant)) {
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
    inner: tokio::sync::Mutex<Option<(SessionDate, TradingCalendar)>>,
}

impl CalendarCache {
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns today's calendar, fetching it if the cache is cold or was filled on an earlier date.
    ///
    /// The lock is released before the Alpaca call and re-taken only to store the result, so a cold
    /// fetch does not block every other caller for its duration. Two callers arriving cold may both
    /// fetch; the request is read-only and the result deterministic, so the duplicate is harmless.
    pub async fn get(
        &self,
        client: &TradingClient,
        now: DateTime<Utc>,
    ) -> Result<TradingCalendar, ClientError> {
        let today = SessionDate::at(now);

        if let Some((cached_date, calendar)) = self.inner.lock().await.as_ref() {
            if *cached_date == today {
                return Ok(calendar.clone());
            }
        }

        let start = today.plus_calendar_days(-HORIZON_DAYS_BACKWARD);
        let end = today.plus_calendar_days(HORIZON_DAYS_FORWARD);
        let calendar =
            TradingCalendar::from_days(client.fetch_calendar(start.date(), end.date()).await?);

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
            *self.inner.lock().await = Some((today, calendar.clone()));
        }
        Ok(calendar)
    }

    /// Replaces the cached calendar. Used by tests and by the pre-open warm path.
    pub async fn install(&self, now: DateTime<Utc>, calendar: TradingCalendar) {
        *self.inner.lock().await = Some((SessionDate::at(now), calendar));
    }
}

#[cfg(test)]
mod tests {
    use chrono::NaiveDate;

    use super::*;

    fn date(year: i32, month: u32, day: u32) -> SessionDate {
        SessionDate::from_date(NaiveDate::from_ymd_opt(year, month, day).unwrap())
    }

    fn day(date_value: SessionDate, open: (u32, u32), close: (u32, u32)) -> CalendarDay {
        CalendarDay::new(
            date_value.date(),
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

    /// The case the trainer's artifact identifier depends on. A run that starts at 23:00 UTC and
    /// takes over an hour finishes on the *next* UTC date but the same Eastern evening, so a
    /// UTC-formatted identifier names the session after the one it was built for.
    #[test]
    fn test_eastern_datetime_names_the_session_the_instant_belongs_to() {
        // 00:30 UTC on 1 August is 20:30 on 31 July in New York.
        let after_utc_midnight = "2026-08-01T00:30:00Z".parse::<DateTime<Utc>>().unwrap();

        // The precondition, so this cannot pass by agreeing with UTC: the UTC date is a day later.
        assert_eq!(after_utc_midnight.date_naive(), date(2026, 8, 1).date());

        assert_eq!(
            eastern_datetime(after_utc_midnight).date(),
            date(2026, 7, 31).date()
        );
        assert_eq!(
            eastern_datetime(after_utc_midnight)
                .format("%Y-%m-%d-%H-%M-%S")
                .to_string(),
            "2026-07-31-20-30-00"
        );
    }

    /// The two must not be able to disagree — the run identifier's date prefix is read back and
    /// compared against a date produced by `SessionDate::at`.
    #[test]
    fn test_eastern_datetime_agrees_with_eastern_date() {
        for instant in [
            "2026-08-01T00:30:00Z",
            "2026-01-14T13:30:00Z",
            "2026-03-08T07:00:00Z",
            "2026-11-01T05:30:00Z",
        ] {
            let parsed = instant.parse::<DateTime<Utc>>().unwrap();
            assert_eq!(
                eastern_datetime(parsed).date(),
                SessionDate::at(parsed).date(),
                "{instant} disagreed"
            );
        }
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
