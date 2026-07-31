//! Fetches the published NYSE trading calendar and persists it.
//!
//! Alpaca's `/v2/calendar` returns one entry per trading day over a requested
//! range, each carrying that day's real open and close. That is the only source
//! in the system that knows about half-days: the hardcoded holiday table in
//! [`market_calendar`](crate::data::market_calendar) cannot express a 13:00
//! close and treats every early-close day as running until 16:00, and
//! `/v2/clock` knows the shortened close but only for the session it is
//! currently reporting — it cannot answer a question about next Tuesday.
//!
//! The fetch lives here rather than behind the portfolio's `Trading` trait for
//! two reasons: the calendar is not trading, and the data service owns the
//! scheduled reference-data jobs but has no `Trading` client. It does have an
//! HTTP client and credentials, which is all this needs.

use std::collections::BTreeMap;

use chrono::{Duration, NaiveDate, NaiveTime, Utc};
use chrono_tz::US::Eastern;
use serde::Deserialize;
use tracing::{info, warn};

use crate::data::market_calendar::{MarketCalendar, SessionHours};
use crate::data::state::{AlpacaCredentials, State};

/// Alpaca trading API base URLs. The calendar is served from the trading API,
/// not the market data API, so it follows the paper/live split.
const PAPER_BASE_URL: &str = "https://paper-api.alpaca.markets";
const LIVE_BASE_URL: &str = "https://api.alpaca.markets";

const HEADER_KEY_ID: &str = "APCA-API-KEY-ID";
const HEADER_SECRET_KEY: &str = "APCA-API-SECRET-KEY";

/// Days of calendar history to keep synced.
///
/// Gap detection looks back 90 days, and trading-day arithmetic over that window
/// has to agree with what actually happened, so the synced history covers it
/// with room to spare.
const HISTORY_DAYS: i64 = 120;

/// Days of calendar future to keep synced.
///
/// The forward horizon is what makes an impending session answerable: with it,
/// "does the market open on date D, and when" is a lookup rather than a question
/// that has to wait until D arrives. A quarter is far more than any current
/// caller needs and still one small response.
const HORIZON_DAYS: i64 = 120;

/// One published trading session, as Alpaca returns it.
///
/// `open` and `close` are Eastern local times in `HH:MM`. Alpaca sends more
/// fields — settlement dates, session extensions — which are deliberately not
/// deserialized: nothing here needs them, and naming them would imply they were
/// checked.
#[derive(Debug, Deserialize)]
struct PublishedSession {
    date: String,
    open: String,
    close: String,
}

/// Error returned when the calendar cannot be refreshed.
#[derive(Debug)]
pub enum CalendarSyncError {
    /// No Alpaca credentials are configured, so the calendar cannot be fetched.
    NoCredentials,
    /// The request failed or the API returned an error.
    Request(String),
    /// The response could not be parsed into sessions.
    Parse(String),
    /// The fetched calendar could not be written.
    Database(sqlx::Error),
}

impl std::fmt::Display for CalendarSyncError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NoCredentials => write!(
                formatter,
                "Alpaca credentials are not configured; cannot fetch the market calendar"
            ),
            Self::Request(message) => write!(formatter, "Calendar request failed: {message}"),
            Self::Parse(message) => write!(formatter, "Calendar response unusable: {message}"),
            Self::Database(error) => write!(formatter, "Calendar write failed: {error}"),
        }
    }
}

impl std::error::Error for CalendarSyncError {}

/// Returns the date range to sync, centred on today's Eastern date.
pub fn sync_range(today: NaiveDate) -> (NaiveDate, NaiveDate) {
    (
        today - Duration::days(HISTORY_DAYS),
        today + Duration::days(HORIZON_DAYS),
    )
}

/// Parses one published session into a date and validated hours.
///
/// Returns `None` for an entry this build cannot use — an unparseable date, a
/// malformed time, or a close at or before the open. A single bad entry is
/// skipped rather than failing the sync: the rest of the calendar is still worth
/// having, and the gap shows up as a date with no row.
fn parse_session(session: &PublishedSession) -> Option<(NaiveDate, SessionHours)> {
    let date = NaiveDate::parse_from_str(&session.date, "%Y-%m-%d").ok()?;
    let open = parse_session_time(&session.open)?;
    let close = parse_session_time(&session.close)?;
    SessionHours::new(open, close).map(|hours| (date, hours))
}

/// Parses an `HH:MM` session time, tolerating an `HH:MM:SS` form.
fn parse_session_time(value: &str) -> Option<NaiveTime> {
    NaiveTime::parse_from_str(value, "%H:%M")
        .or_else(|_| NaiveTime::parse_from_str(value, "%H:%M:%S"))
        .ok()
}

/// Builds a calendar from published sessions, skipping unusable entries.
fn calendar_from_published(sessions: &[PublishedSession]) -> (MarketCalendar, usize) {
    let mut parsed = BTreeMap::new();
    let mut skipped = 0;
    for session in sessions {
        match parse_session(session) {
            Some((date, hours)) => {
                parsed.insert(date, hours);
            }
            None => skipped += 1,
        }
    }
    (MarketCalendar::new(parsed), skipped)
}

/// Fetches the published calendar for `[start, end]` from Alpaca.
async fn fetch_published_calendar(
    state: &State,
    start: NaiveDate,
    end: NaiveDate,
) -> Result<Vec<PublishedSession>, CalendarSyncError> {
    let credentials: &AlpacaCredentials = state
        .alpaca_credentials
        .as_ref()
        .ok_or(CalendarSyncError::NoCredentials)?;

    let is_paper = std::env::var("ALPACA_IS_PAPER")
        .map(|value| !value.eq_ignore_ascii_case("false"))
        .unwrap_or(true);
    let base_url = if is_paper {
        PAPER_BASE_URL
    } else {
        LIVE_BASE_URL
    };

    let response = state
        .http_client
        .get(format!("{base_url}/v2/calendar"))
        .header(HEADER_KEY_ID, credentials.key_id())
        .header(HEADER_SECRET_KEY, credentials.secret())
        .query(&[("start", start.to_string()), ("end", end.to_string())])
        .send()
        .await
        .map_err(|error| CalendarSyncError::Request(error.to_string()))?;

    let status = response.status();
    if !status.is_success() {
        let body = response.text().await.unwrap_or_default();
        return Err(CalendarSyncError::Request(format!("{status}: {body}")));
    }

    response
        .json::<Vec<PublishedSession>>()
        .await
        .map_err(|error| CalendarSyncError::Parse(error.to_string()))
}

/// Writes the calendar to `market_calendar`, replacing the synced range.
///
/// Deletes the range before inserting rather than upserting alone, so a date
/// that stops being a trading day — a newly declared holiday, a day of mourning
/// — loses its row instead of lingering. The whole thing runs in one
/// transaction, because a calendar with a hole in it is worse than a stale one.
async fn persist_calendar(
    pool: &sqlx::PgPool,
    calendar: &MarketCalendar,
    start: NaiveDate,
    end: NaiveDate,
) -> Result<(), sqlx::Error> {
    let mut transaction = pool.begin().await?;

    sqlx::query("DELETE FROM market_calendar WHERE session_date BETWEEN $1 AND $2")
        .bind(start)
        .bind(end)
        .execute(&mut *transaction)
        .await?;

    for date in start.iter_days().take_while(|date| *date <= end) {
        let Some(hours) = calendar.session_hours(date) else {
            continue;
        };
        sqlx::query(
            "INSERT INTO market_calendar (session_date, session_open, session_close) \
             VALUES ($1, $2, $3) \
             ON CONFLICT (session_date) DO UPDATE \
             SET session_open = EXCLUDED.session_open, \
                 session_close = EXCLUDED.session_close, \
                 updated_at = now()",
        )
        .bind(date)
        .bind(hours.open())
        .bind(hours.close())
        .execute(&mut *transaction)
        .await?;
    }

    transaction.commit().await
}

/// Loads the persisted calendar into memory.
///
/// Used at startup, before any service reads a trading day, so a process that
/// starts between syncs still has the published calendar rather than the
/// hardcoded fallback.
pub async fn load_persisted_calendar(
    pool: &sqlx::PgPool,
) -> Result<MarketCalendar, CalendarSyncError> {
    let rows: Vec<(NaiveDate, NaiveTime, NaiveTime)> = sqlx::query_as(
        "SELECT session_date, session_open, session_close FROM market_calendar \
         ORDER BY session_date",
    )
    .fetch_all(pool)
    .await
    .map_err(CalendarSyncError::Database)?;

    let mut sessions = BTreeMap::new();
    for (date, open, close) in rows {
        match SessionHours::new(open, close) {
            Some(hours) => {
                sessions.insert(date, hours);
            }
            None => warn!(
                %date,
                "Persisted calendar row has a close at or before its open; skipping"
            ),
        }
    }

    Ok(MarketCalendar::new(sessions))
}

/// Refreshes the calendar from Alpaca, persists it, and publishes it.
///
/// Returns the number of sessions published.
pub async fn run_market_calendar_sync(
    state: &State,
    pool: &sqlx::PgPool,
) -> Result<usize, CalendarSyncError> {
    let today = Utc::now().with_timezone(&Eastern).date_naive();
    let (start, end) = sync_range(today);

    let published = fetch_published_calendar(state, start, end).await?;
    let (calendar, skipped) = calendar_from_published(&published);

    if calendar.is_empty() {
        return Err(CalendarSyncError::Parse(
            "Alpaca returned no usable sessions for the requested range".to_string(),
        ));
    }
    if skipped > 0 {
        warn!(
            skipped,
            "Skipped unusable entries while parsing the published calendar"
        );
    }

    persist_calendar(pool, &calendar, start, end)
        .await
        .map_err(CalendarSyncError::Database)?;

    let session_count = calendar.len();
    let shortened = calendar_shortened_session_count(&calendar);
    crate::data::market_calendar::install(calendar);

    info!(
        sessions = session_count,
        shortened,
        %start,
        %end,
        "Market calendar published"
    );

    Ok(session_count)
}

/// Counts sessions that close early, for the sync log.
///
/// Reported because it is the one figure that says whether this sync is earning
/// its place: a year holds roughly three shortened sessions, and every one of
/// them is a day the hardcoded fallback would have treated as running to 16:00.
fn calendar_shortened_session_count(calendar: &MarketCalendar) -> usize {
    let Some((start, end)) = calendar.horizon() else {
        return 0;
    };
    start
        .iter_days()
        .take_while(|date| *date <= end)
        .filter_map(|date| calendar.session_hours(date))
        .filter(|hours| hours.is_shortened())
        .count()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn session(date: &str, open: &str, close: &str) -> PublishedSession {
        PublishedSession {
            date: date.to_string(),
            open: open.to_string(),
            close: close.to_string(),
        }
    }

    fn date(text: &str) -> NaiveDate {
        NaiveDate::parse_from_str(text, "%Y-%m-%d").unwrap()
    }

    #[test]
    fn test_a_regular_session_parses_to_full_hours() {
        let (parsed_date, hours) = parse_session(&session("2026-07-30", "09:30", "16:00"))
            .expect("regular session parses");
        assert_eq!(parsed_date, date("2026-07-30"));
        assert_eq!(hours.open(), NaiveTime::from_hms_opt(9, 30, 0).unwrap());
        assert_eq!(hours.close(), NaiveTime::from_hms_opt(16, 0, 0).unwrap());
        assert!(!hours.is_shortened());
    }

    /// The case neither existing source could express.
    #[test]
    fn test_a_half_day_parses_as_a_shortened_session() {
        let (_, hours) =
            parse_session(&session("2026-11-27", "09:30", "13:00")).expect("half day parses");
        assert_eq!(hours.close(), NaiveTime::from_hms_opt(13, 0, 0).unwrap());
        assert!(hours.is_shortened());
    }

    #[test]
    fn test_seconds_in_a_session_time_are_tolerated() {
        assert!(parse_session(&session("2026-07-30", "09:30:00", "16:00:00")).is_some());
    }

    #[test]
    fn test_unusable_entries_are_skipped_rather_than_failing() {
        let sessions = [
            session("2026-07-30", "09:30", "16:00"),
            session("not-a-date", "09:30", "16:00"),
            session("2026-07-31", "bad", "16:00"),
            // A close at or before the open cannot describe a session.
            session("2026-08-03", "16:00", "09:30"),
        ];
        let (calendar, skipped) = calendar_from_published(&sessions);

        assert_eq!(skipped, 3);
        assert_eq!(calendar.len(), 1);
        assert!(calendar.session_hours(date("2026-07-30")).is_some());
    }

    #[test]
    fn test_a_date_with_no_entry_does_not_trade() {
        // 2026-07-03 is the observed Independence Day holiday: Alpaca simply
        // omits it, and absence inside the horizon is the answer.
        let sessions = [
            session("2026-07-02", "09:30", "16:00"),
            session("2026-07-06", "09:30", "16:00"),
        ];
        let (calendar, _) = calendar_from_published(&sessions);

        assert!(calendar.covers(date("2026-07-03")));
        assert!(calendar.session_hours(date("2026-07-03")).is_none());
    }

    #[test]
    fn test_next_session_skips_a_closed_stretch() {
        let sessions = [
            session("2026-07-02", "09:30", "16:00"),
            session("2026-07-06", "09:30", "16:00"),
        ];
        let (calendar, _) = calendar_from_published(&sessions);

        let (next_date, _) = calendar
            .next_session_on_or_after(date("2026-07-03"))
            .expect("a later session exists");
        assert_eq!(next_date, date("2026-07-06"));
    }

    #[test]
    fn test_sync_range_brackets_today() {
        let (start, end) = sync_range(date("2026-07-31"));
        assert!(start < date("2026-07-31"));
        assert!(end > date("2026-07-31"));
        assert_eq!((end - start).num_days(), HISTORY_DAYS + HORIZON_DAYS);
    }

    #[test]
    fn test_shortened_sessions_are_counted() {
        let sessions = [
            session("2026-11-25", "09:30", "16:00"),
            session("2026-11-27", "09:30", "13:00"),
        ];
        let (calendar, _) = calendar_from_published(&sessions);
        assert_eq!(calendar_shortened_session_count(&calendar), 1);
    }

    #[test]
    fn test_sync_error_messages_name_their_cause() {
        assert!(CalendarSyncError::NoCredentials
            .to_string()
            .contains("credentials"));
        assert!(CalendarSyncError::Request("503".to_string())
            .to_string()
            .contains("503"));
        assert!(CalendarSyncError::Parse("bad".to_string())
            .to_string()
            .contains("bad"));
    }
}
