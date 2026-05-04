use crate::equity_bars::fetch_and_store;
use crate::state::State;
use chrono::{DateTime, Datelike, NaiveDate, NaiveTime, TimeZone, Utc, Weekday};
use chrono_tz::US::Eastern;
use std::time::Duration;
use tokio::time::sleep;
use tracing::{error, info};

const SYNC_HOUR: u32 = 1;
const SYNC_MINUTE: u32 = 0;

fn prior_trading_day(date: NaiveDate) -> NaiveDate {
    let mut prior = date.pred_opt().unwrap();
    while matches!(prior.weekday(), Weekday::Sat | Weekday::Sun) {
        prior = prior.pred_opt().unwrap();
    }
    prior
}

fn duration_until_next_sync(now: DateTime<Utc>) -> Duration {
    let now_eastern = now.with_timezone(&Eastern);
    let sync_time = NaiveTime::from_hms_opt(SYNC_HOUR, SYNC_MINUTE, 0).unwrap();

    let mut target_date = now_eastern.date_naive();
    let mut target_eastern = Eastern
        .from_local_datetime(&target_date.and_time(sync_time))
        .earliest()
        .unwrap();

    if now_eastern >= target_eastern {
        target_date = target_date.succ_opt().unwrap();
        target_eastern = Eastern
            .from_local_datetime(&target_date.and_time(sync_time))
            .earliest()
            .unwrap();
    }

    while matches!(target_eastern.weekday(), Weekday::Sat | Weekday::Sun) {
        let next_date = target_eastern.date_naive().succ_opt().unwrap();
        target_eastern = Eastern
            .from_local_datetime(&next_date.and_time(sync_time))
            .earliest()
            .unwrap();
    }

    (target_eastern.with_timezone(&Utc) - now)
        .to_std()
        .unwrap_or(Duration::ZERO)
}

fn sync_date_for(now: DateTime<Utc>) -> NaiveDate {
    prior_trading_day(now.with_timezone(&Eastern).date_naive())
}

pub fn spawn_sync_scheduler(state: State) {
    tokio::spawn(sync_loop(state));
}

async fn sync_loop(state: State) {
    loop {
        let wait_duration = duration_until_next_sync(Utc::now());
        info!(
            "Waiting for next equity bar sync, seconds_until_sync: {}",
            wait_duration.as_secs()
        );
        sleep(wait_duration).await;

        let now_utc = Utc::now();
        let now_eastern = now_utc.with_timezone(&Eastern);
        if matches!(now_eastern.weekday(), Weekday::Sat | Weekday::Sun) {
            info!("Weekend detected, skipping equity bar sync");
            continue;
        }

        let sync_date = sync_date_for(now_utc);
        let sync_noon_eastern = Eastern
            .from_local_datetime(&sync_date.and_hms_opt(12, 0, 0).unwrap())
            .earliest()
            .unwrap();
        let sync_utc = sync_noon_eastern.with_timezone(&Utc);

        info!(
            "Starting scheduled equity bar sync for {}",
            sync_date.format("%Y-%m-%d")
        );

        match fetch_and_store(&state, &sync_utc).await {
            Ok(Some(s3_key)) => {
                info!("Scheduled equity bar sync completed, s3_key: {}", s3_key);
            }
            Ok(None) => {
                info!(
                    "No equity bar data available for sync date {}",
                    sync_date.format("%Y-%m-%d")
                );
            }
            Err(err) => {
                error!("Scheduled equity bar sync failed: {}", err);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{duration_until_next_sync, prior_trading_day, sync_date_for};
    use chrono::{NaiveDate, TimeZone, Utc};
    use chrono_tz::US::Eastern;
    use std::time::Duration;

    #[test]
    fn test_duration_until_next_sync_is_positive() {
        let duration = duration_until_next_sync(Utc::now());
        assert!(
            duration.as_secs() > 0,
            "Expected positive wait time, got {:?}",
            duration
        );
    }

    #[test]
    fn test_duration_until_next_sync_is_within_one_week() {
        let duration = duration_until_next_sync(Utc::now());
        let one_week = std::time::Duration::from_secs(7 * 24 * 60 * 60);
        assert!(
            duration <= one_week,
            "Expected wait time within one week, got {:?}",
            duration
        );
    }

    #[test]
    fn test_prior_trading_day_wednesday_returns_tuesday() {
        let wednesday = NaiveDate::from_ymd_opt(2026, 4, 29).unwrap();
        let prior = prior_trading_day(wednesday);
        assert_eq!(prior, NaiveDate::from_ymd_opt(2026, 4, 28).unwrap());
    }

    #[test]
    fn test_prior_trading_day_monday_returns_friday() {
        let monday = NaiveDate::from_ymd_opt(2026, 4, 27).unwrap();
        let prior = prior_trading_day(monday);
        assert_eq!(prior, NaiveDate::from_ymd_opt(2026, 4, 24).unwrap());
    }

    #[test]
    fn test_prior_trading_day_tuesday_returns_monday() {
        let tuesday = NaiveDate::from_ymd_opt(2026, 4, 28).unwrap();
        let prior = prior_trading_day(tuesday);
        assert_eq!(prior, NaiveDate::from_ymd_opt(2026, 4, 27).unwrap());
    }

    // --- duration_until_next_sync with injected now ---

    #[test]
    fn test_duration_until_next_sync_fires_within_one_minute_just_before_1am_et() {
        // Monday 2026-04-27 at 00:59 ET — should fire in ≤ 60 seconds
        let now = Eastern
            .with_ymd_and_hms(2026, 4, 27, 0, 59, 0)
            .unwrap()
            .with_timezone(&Utc);
        let duration = duration_until_next_sync(now);
        assert!(
            duration.as_secs() <= 60,
            "Expected ≤ 60s, got {:?}",
            duration
        );
    }

    #[test]
    fn test_duration_until_next_sync_after_1am_waits_until_next_weekday() {
        // Monday 2026-04-27 at 02:00 ET — next fire is Tuesday 2026-04-28 at 01:00 ET (~23h)
        let now = Eastern
            .with_ymd_and_hms(2026, 4, 27, 2, 0, 0)
            .unwrap()
            .with_timezone(&Utc);
        let duration = duration_until_next_sync(now);
        let twenty_two_hours = Duration::from_secs(22 * 3600);
        let twenty_four_hours = Duration::from_secs(24 * 3600);
        assert!(
            duration > twenty_two_hours && duration < twenty_four_hours,
            "Expected ~23h, got {:?}",
            duration
        );
    }

    #[test]
    fn test_duration_until_next_sync_from_friday_skips_to_monday() {
        // Friday 2026-05-01 at 02:00 ET — next fire is Monday 2026-05-04 at 01:00 ET (~71h)
        let now = Eastern
            .with_ymd_and_hms(2026, 5, 1, 2, 0, 0)
            .unwrap()
            .with_timezone(&Utc);
        let duration = duration_until_next_sync(now);
        let seventy_hours = Duration::from_secs(70 * 3600);
        let seventy_two_hours = Duration::from_secs(72 * 3600);
        assert!(
            duration > seventy_hours && duration < seventy_two_hours,
            "Expected ~71h, got {:?}",
            duration
        );
    }

    // --- sync_date_for with injected now ---

    #[test]
    fn test_sync_date_for_tuesday_fire_is_monday() {
        // Tuesday 2026-04-28 at 01:00 ET — should sync Monday 2026-04-27
        let now = Eastern
            .with_ymd_and_hms(2026, 4, 28, 1, 0, 0)
            .unwrap()
            .with_timezone(&Utc);
        let sync_date = sync_date_for(now);
        assert_eq!(sync_date, NaiveDate::from_ymd_opt(2026, 4, 27).unwrap());
    }

    #[test]
    fn test_sync_date_for_monday_fire_is_friday() {
        // Monday 2026-04-27 at 01:00 ET — should sync Friday 2026-04-24
        let now = Eastern
            .with_ymd_and_hms(2026, 4, 27, 1, 0, 0)
            .unwrap()
            .with_timezone(&Utc);
        let sync_date = sync_date_for(now);
        assert_eq!(sync_date, NaiveDate::from_ymd_opt(2026, 4, 24).unwrap());
    }

    #[test]
    fn test_sync_date_for_wednesday_fire_is_tuesday() {
        // Wednesday 2026-04-29 at 01:00 ET — should sync Tuesday 2026-04-28
        let now = Eastern
            .with_ymd_and_hms(2026, 4, 29, 1, 0, 0)
            .unwrap()
            .with_timezone(&Utc);
        let sync_date = sync_date_for(now);
        assert_eq!(sync_date, NaiveDate::from_ymd_opt(2026, 4, 28).unwrap());
    }
}
