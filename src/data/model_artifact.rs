//! Scheduled observation of the trainer's model artifact publishing.
//!
//! The trainer runs on its own VM. It reads bars from S3, trains, and uploads an
//! artifact back to S3. It has no database connection — PostgreSQL is bound to
//! `127.0.0.1`, and exposing it across VMs so a once-daily batch job can insert
//! one row is not worth the attack surface — so the application cannot *enforce*
//! that the 05:00 bars sync, the 06:00 training run, and the 09:00 prediction
//! run happened in that order.
//!
//! What it can do is *observe*. Three things could previously go wrong in
//! silence: the sync fails and training runs on yesterday's bars; training fails
//! and predictions run against the previous artifact; either repeats and the
//! model quietly ages for days. `MODEL_VERSION = "latest"` means whatever is
//! newest wins, so none of that surfaced anywhere.
//!
//! This check runs at 06:30 UTC, half an hour after the trainer's crontab entry,
//! and turns a training run that did not happen into an event.

use chrono::{DateTime, NaiveDateTime, TimeZone, Utc};
use chrono_tz::US::Eastern;
use tracing::{info, warn};

use crate::data::market_calendar;
use crate::inference::artifact;

/// Trading days an artifact may age before the check reports it stale.
///
/// Two, because this is a daily-bar model: a one-day-old artifact is the normal
/// state for most of a session, and one missed run is the first thing worth
/// knowing about. Counted in trading days rather than hours for the same reason
/// event freshness is — a fixed hour count reports every Monday as a failure.
const STALE_AFTER_TRADING_DAYS: usize = 2;

/// Default S3 prefix holding training run folders.
///
/// Matches `AWS_S3_MODEL_ARTIFACT_PATH` in `devenv.nix`; the environment
/// variable wins when set, so the data service and inference resolve the same
/// keys without either owning the configuration.
const DEFAULT_ARTIFACT_PREFIX: &str = "models/tide/";

/// Returns the artifact prefix from the environment, or the default.
pub fn artifact_prefix() -> String {
    std::env::var("AWS_S3_MODEL_ARTIFACT_PATH")
        .unwrap_or_else(|_| DEFAULT_ARTIFACT_PREFIX.to_string())
}

/// What the artifact check found.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ArtifactStatus {
    /// A key not previously recorded in `model_runs`.
    Published {
        artifact_key: String,
        trained_at: Option<DateTime<Utc>>,
    },
    /// The newest key is one already recorded, and old enough to report.
    Stale {
        artifact_key: String,
        trading_days_old: usize,
    },
    /// The newest key is one already recorded, and recent enough to be normal.
    Unchanged { artifact_key: String },
}

/// Extracts the training timestamp from an artifact key.
///
/// Keys are `{prefix}{timestamp}/output/model.tar.gz`, where the timestamp
/// segment is `YYYY-MM-DD-HH-MM-SS-mmm` as written by the trainer. Returns
/// `None` for a key that does not carry a parseable segment, which is a
/// malformed key rather than an error worth aborting the check for — the key
/// itself is still reported.
pub fn trained_at_from_key(artifact_key: &str) -> Option<DateTime<Utc>> {
    let segment = artifact_key
        .split('/')
        .find(|segment| segment.len() >= 19 && segment.starts_with(|c: char| c.is_ascii_digit()))?;

    // The trainer writes UTC, and the milliseconds suffix is optional across
    // older runs, so both widths parse.
    let without_milliseconds = segment.get(..19)?;
    NaiveDateTime::parse_from_str(without_milliseconds, "%Y-%m-%d-%H-%M-%S")
        .ok()
        .map(|naive| Utc.from_utc_datetime(&naive))
}

/// Returns how many trading days have elapsed since `trained_at`.
///
/// Counts the trading days strictly between the artifact's Eastern date and
/// today's, so an artifact trained this morning is zero days old and one trained
/// on the previous trading day is one.
pub fn trading_days_since(trained_at: DateTime<Utc>, now: DateTime<Utc>) -> usize {
    let trained_date = trained_at.with_timezone(&Eastern).date_naive();
    let today = now.with_timezone(&Eastern).date_naive();
    if trained_date >= today {
        return 0;
    }

    let mut elapsed = 0;
    let mut date = today;
    while date > trained_date {
        if market_calendar::is_trading_day(date) {
            elapsed += 1;
        }
        date = match date.pred_opt() {
            Some(previous) => previous,
            None => break,
        };
    }
    elapsed
}

/// Compares the newest artifact in S3 against the newest one already recorded.
///
/// `recorded_key` is the `artifact_key` of the most recent `model_runs` row.
/// A `None` means nothing has been recorded, so any resolvable artifact counts
/// as newly published.
pub fn classify(
    latest_key: String,
    recorded_key: Option<&str>,
    now: DateTime<Utc>,
) -> ArtifactStatus {
    if recorded_key != Some(latest_key.as_str()) {
        let trained_at = trained_at_from_key(&latest_key);
        return ArtifactStatus::Published {
            artifact_key: latest_key,
            trained_at,
        };
    }

    // Unchanged. Whether that is normal or a missed training run is a question
    // about the artifact's age, not about the comparison.
    match trained_at_from_key(&latest_key) {
        Some(trained_at) => {
            let trading_days_old = trading_days_since(trained_at, now);
            if trading_days_old >= STALE_AFTER_TRADING_DAYS {
                ArtifactStatus::Stale {
                    artifact_key: latest_key,
                    trading_days_old,
                }
            } else {
                ArtifactStatus::Unchanged {
                    artifact_key: latest_key,
                }
            }
        }
        // A key whose age cannot be read is reported unchanged rather than
        // stale: the check should not raise an alert it cannot substantiate.
        None => {
            warn!(
                artifact_key = latest_key,
                "Artifact key carries no parseable training timestamp; age unknown"
            );
            ArtifactStatus::Unchanged {
                artifact_key: latest_key,
            }
        }
    }
}

/// Resolves the newest artifact key in S3.
///
/// Delegates to the same resolution inference uses, which walks run folders
/// newest-first and verifies each `model.tar.gz` actually exists — so a trainer
/// that crashed mid-run, or one still uploading when this fires at 06:30, yields
/// the previous good artifact rather than an error or a partial read.
pub async fn resolve_latest_artifact_key(
    s3_client: &aws_sdk_s3::Client,
    bucket: &str,
) -> Result<String, artifact::ArtifactError> {
    artifact::resolve_artifact_key(s3_client, bucket, &artifact_prefix(), "latest", None).await
}

/// Returns the `artifact_key` of the most recently started `model_runs` row.
pub async fn latest_recorded_artifact_key(
    pool: &sqlx::PgPool,
) -> Result<Option<String>, sqlx::Error> {
    let recorded: Option<Option<String>> = sqlx::query_scalar(
        "SELECT artifact_key FROM model_runs \
         WHERE artifact_key IS NOT NULL \
         ORDER BY started_at DESC LIMIT 1",
    )
    .fetch_optional(pool)
    .await?;
    Ok(recorded.flatten())
}

/// Logs the outcome at a level matching what it means for the day's predictions.
pub fn report(status: &ArtifactStatus) {
    match status {
        ArtifactStatus::Published {
            artifact_key,
            trained_at,
        } => info!(
            artifact_key,
            trained_at = trained_at.map(|instant| instant.to_string()),
            "New model artifact published"
        ),
        ArtifactStatus::Stale {
            artifact_key,
            trading_days_old,
        } => warn!(
            artifact_key,
            trading_days_old, "Model artifact is stale; a training run did not produce a new one"
        ),
        ArtifactStatus::Unchanged { artifact_key } => {
            info!(artifact_key, "Model artifact is unchanged and current")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn utc(text: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(text)
            .expect("fixture timestamp parses")
            .with_timezone(&Utc)
    }

    const KEY: &str = "models/tide/2026-07-29-06-05-11-482/output/model.tar.gz";

    #[test]
    fn test_trained_at_is_read_from_the_key_timestamp() {
        assert_eq!(trained_at_from_key(KEY), Some(utc("2026-07-29T06:05:11Z")));
    }

    #[test]
    fn test_trained_at_reads_a_key_without_milliseconds() {
        assert_eq!(
            trained_at_from_key("models/tide/2026-07-29-06-05-11/output/model.tar.gz"),
            Some(utc("2026-07-29T06:05:11Z"))
        );
    }

    #[test]
    fn test_malformed_keys_yield_no_timestamp() {
        for key in [
            "models/tide/latest/output/model.tar.gz",
            "models/tide//output/model.tar.gz",
            "models/tide/2026-13-45-99-99-99-000/output/model.tar.gz",
            "",
        ] {
            assert_eq!(trained_at_from_key(key), None, "key {key} must not parse");
        }
    }

    #[test]
    fn test_a_new_key_is_published() {
        let status = classify(KEY.to_string(), None, utc("2026-07-29T10:00:00Z"));
        assert_eq!(
            status,
            ArtifactStatus::Published {
                artifact_key: KEY.to_string(),
                trained_at: Some(utc("2026-07-29T06:05:11Z")),
            }
        );
    }

    #[test]
    fn test_a_different_recorded_key_is_published() {
        let status = classify(
            KEY.to_string(),
            Some("models/tide/2026-07-28-06-05-11-482/output/model.tar.gz"),
            utc("2026-07-29T10:00:00Z"),
        );
        assert!(matches!(status, ArtifactStatus::Published { .. }));
    }

    #[test]
    fn test_the_same_key_trained_today_is_unchanged() {
        let status = classify(KEY.to_string(), Some(KEY), utc("2026-07-29T14:00:00Z"));
        assert_eq!(
            status,
            ArtifactStatus::Unchanged {
                artifact_key: KEY.to_string()
            }
        );
    }

    #[test]
    fn test_the_same_key_one_trading_day_old_is_still_unchanged() {
        // 2026-07-29 is a Wednesday; one day later is normal for a daily model.
        let status = classify(KEY.to_string(), Some(KEY), utc("2026-07-30T14:00:00Z"));
        assert!(matches!(status, ArtifactStatus::Unchanged { .. }));
    }

    #[test]
    fn test_the_same_key_two_trading_days_old_is_stale() {
        let status = classify(KEY.to_string(), Some(KEY), utc("2026-07-31T14:00:00Z"));
        assert_eq!(
            status,
            ArtifactStatus::Stale {
                artifact_key: KEY.to_string(),
                trading_days_old: 2,
            }
        );
    }

    /// The reason the threshold counts trading days rather than hours.
    #[test]
    fn test_a_weekend_does_not_make_a_friday_artifact_stale() {
        // 2026-07-31 is a Friday. On Monday 2026-08-03 the artifact is roughly
        // 74 hours old but only one trading day has passed.
        let friday_key = "models/tide/2026-07-31-06-05-11-482/output/model.tar.gz";
        let status = classify(
            friday_key.to_string(),
            Some(friday_key),
            utc("2026-08-03T14:00:00Z"),
        );
        assert!(
            matches!(status, ArtifactStatus::Unchanged { .. }),
            "a Friday artifact must not be stale on Monday: {status:?}"
        );

        // By Tuesday two trading days have passed and it is genuinely overdue.
        let status = classify(
            friday_key.to_string(),
            Some(friday_key),
            utc("2026-08-04T14:00:00Z"),
        );
        assert!(matches!(status, ArtifactStatus::Stale { .. }));
    }

    #[test]
    fn test_an_unreadable_timestamp_is_never_reported_stale() {
        // The check should not raise an alert whose basis it cannot state.
        let key = "models/tide/latest/output/model.tar.gz";
        let status = classify(key.to_string(), Some(key), utc("2027-01-01T14:00:00Z"));
        assert_eq!(
            status,
            ArtifactStatus::Unchanged {
                artifact_key: key.to_string()
            }
        );
    }

    /// Age is counted in Eastern dates, not UTC ones.
    ///
    /// The trainer writes UTC timestamps and this runs against a US market
    /// calendar, so the two disagree for the last four or five hours of every
    /// UTC day: 2026-07-30T00:30Z is still 2026-07-29 in Eastern, and an
    /// artifact trained that morning is not yet a day old.
    #[test]
    fn test_trading_days_since_counts_eastern_dates() {
        let trained_at = utc("2026-07-29T06:05:11Z");

        assert_eq!(
            trading_days_since(trained_at, utc("2026-07-29T23:00:00Z")),
            0
        );
        assert_eq!(
            trading_days_since(trained_at, utc("2026-07-30T00:30:00Z")),
            0
        );
        assert_eq!(
            trading_days_since(trained_at, utc("2026-07-30T14:00:00Z")),
            1
        );
    }

    #[test]
    fn test_artifact_prefix_defaults_to_the_configured_path() {
        // Reads the environment, so this asserts the default shape rather than a
        // specific value: the point is that it ends in a slash and names tide.
        let prefix = artifact_prefix();
        assert!(prefix.ends_with('/'), "prefix must be a folder: {prefix}");
    }
}
