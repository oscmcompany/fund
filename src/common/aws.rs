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
/// `data/equity/bars/year=2026/month=06/day=10/data.parquet`. The single
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

#[cfg(test)]
mod tests {
    use super::date_partitioned_key;

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
}
