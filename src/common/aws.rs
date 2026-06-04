//! AWS client construction shared by all services.

/// Load the default AWS configuration (region, credentials) from the environment.
pub async fn load_config() -> aws_config::SdkConfig {
    aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await
}

/// Construct an S3 client from the default AWS configuration.
pub async fn s3_client() -> aws_sdk_s3::Client {
    aws_sdk_s3::Client::new(&load_config().await)
}
