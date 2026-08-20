//! AWS client construction shared by all services.

/// Load the default AWS configuration (region, credentials) from the environment.
pub async fn load_config() -> aws_config::SdkConfig {
    aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await
}

/// Construct an S3 client from the default AWS configuration.
pub async fn s3_client() -> aws_sdk_s3::Client {
    aws_sdk_s3::Client::new(&load_config().await)
}

/// Build the Hive-partitioned S3 key for one day of parquet data, e.g.
/// `data/equity/bars/interval=one_day/year=2026/month=06/day=10/data.parquet`. The single
/// source of truth for the date-partition layout: the data manager's daily
/// writers and exports, the historical backfill, and the tide trainer's
/// reader all derive their keys here so they can never diverge.
pub fn date_partitioned_key(prefix: &str, date: chrono::NaiveDate) -> String {
    use chrono::Datelike;
    format!(
        "{}/year={}/month={:02}/day={:02}/data.parquet",
        prefix,
        date.year(),
        date.month(),
        date.day()
    )
}

/// Recover the date from a key built by [`date_partitioned_key`], or `None` if the key does not
/// have that shape.
///
/// The inverse exists so a `ListObjectsV2` result can become a set of dates, which is what lets the
/// archive answer "which sessions am I missing" by comparing what is present against what is
/// expected. Written against the tail of the key rather than the whole of it, so it is indifferent
/// to the prefix and cannot be broken by one being renamed.
///
/// `None` rather than an error: a bucket may hold objects this layout knows nothing about, and a
/// stray key is something to skip, not something to fail a run over.
pub fn date_from_partitioned_key(key: &str) -> Option<chrono::NaiveDate> {
    let mut segments = key.rsplit('/');
    if segments.next()? != "data.parquet" {
        return None;
    }
    let day: u32 = segments.next()?.strip_prefix("day=")?.parse().ok()?;
    let month: u32 = segments.next()?.strip_prefix("month=")?.parse().ok()?;
    let year: i32 = segments.next()?.strip_prefix("year=")?.parse().ok()?;
    chrono::NaiveDate::from_ymd_opt(year, month, day)
}

#[cfg(test)]
mod tests {
    use super::{date_from_partitioned_key, date_partitioned_key};

    #[test]
    fn test_date_partitioned_key_zero_pads_month_and_day() {
        let date = chrono::NaiveDate::from_ymd_opt(2026, 6, 3).unwrap();
        assert_eq!(
            date_partitioned_key("data/equity/bars", date),
            "data/equity/bars/year=2026/month=06/day=03/data.parquet"
        );
    }

    #[test]
    fn test_date_partitioned_key_double_digit_components() {
        let date = chrono::NaiveDate::from_ymd_opt(2025, 12, 31).unwrap();
        assert_eq!(
            date_partitioned_key("exports/equity/orders", date),
            "exports/equity/orders/year=2025/month=12/day=31/data.parquet"
        );
    }

    /// The property that matters: whatever the writer produced, the reader recovers. A round trip
    /// rather than a hand-written expectation, because the two functions only need to agree with
    /// each other, and a literal in the test would be a third source able to disagree with both.
    #[test]
    fn test_date_round_trips_through_a_partitioned_key() {
        for (year, month, day) in [(2026, 6, 3), (2025, 12, 31), (2024, 2, 29), (2026, 1, 1)] {
            let date = chrono::NaiveDate::from_ymd_opt(year, month, day).unwrap();
            let key = date_partitioned_key("data/equity/bars", date);
            assert_eq!(date_from_partitioned_key(&key), Some(date), "key: {key}");
        }
    }

    /// Prefix-independent, so renaming a prefix cannot silently stop the archive from recognising
    /// its own objects.
    #[test]
    fn test_date_recovered_regardless_of_prefix() {
        let date = chrono::NaiveDate::from_ymd_opt(2026, 6, 3).unwrap();
        for prefix in ["data/equity/bars", "exports/equity/orders", "a", ""] {
            let key = date_partitioned_key(prefix, date);
            assert_eq!(
                date_from_partitioned_key(&key),
                Some(date),
                "prefix: {prefix}"
            );
        }
    }

    #[test]
    fn test_malformed_keys_are_skipped_rather_than_parsed() {
        for key in [
            "",
            "data/equity/bars/details.csv",
            "data/equity/bars/year=2026/month=06/day=03/manifest.json",
            "data/equity/bars/year=2026/month=06/data.parquet",
            "data/equity/bars/year=2026/month=6x/day=03/data.parquet",
            "data/equity/bars/year=2026/month=13/day=03/data.parquet",
            "data/equity/bars/year=2026/month=02/day=30/data.parquet",
            "data/equity/bars/2026/06/03/data.parquet",
        ] {
            assert_eq!(date_from_partitioned_key(key), None, "key: {key}");
        }
    }
}
