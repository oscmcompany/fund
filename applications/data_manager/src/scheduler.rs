use crate::equity_bars::fetch_and_store;
use crate::state::State;
use chrono::{Datelike, NaiveTime, TimeZone, Utc, Weekday};
use chrono_tz::US::Eastern;
use std::time::Duration;
use tokio::time::sleep;
use tracing::{error, info};

const SYNC_HOUR: u32 = 18;
const SYNC_MINUTE: u32 = 0;

fn duration_until_next_sync() -> Duration {
    let now_utc = Utc::now();
    let now_eastern = now_utc.with_timezone(&Eastern);
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

    (target_eastern.with_timezone(&Utc) - now_utc)
        .to_std()
        .unwrap_or(Duration::ZERO)
}

pub fn spawn_sync_scheduler(state: State) {
    tokio::spawn(sync_loop(state));
}

async fn sync_loop(state: State) {
    loop {
        let wait_duration = duration_until_next_sync();
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

        info!(
            "Starting scheduled equity bar sync for {}",
            now_eastern.format("%Y-%m-%d")
        );

        match fetch_and_store(&state, &now_utc).await {
            Ok(Some(s3_key)) => {
                info!("Scheduled equity bar sync completed, s3_key: {}", s3_key);
            }
            Ok(None) => {
                info!("No equity bar data available for today");
            }
            Err(err) => {
                error!("Scheduled equity bar sync failed: {}", err);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::duration_until_next_sync;

    #[test]
    fn test_duration_until_next_sync_is_positive() {
        let duration = duration_until_next_sync();
        assert!(
            duration.as_secs() > 0,
            "Expected positive wait time, got {:?}",
            duration
        );
    }

    #[test]
    fn test_duration_until_next_sync_is_within_one_week() {
        let duration = duration_until_next_sync();
        let one_week = std::time::Duration::from_secs(7 * 24 * 60 * 60);
        assert!(
            duration <= one_week,
            "Expected wait time within one week, got {:?}",
            duration
        );
    }
}
