use crate::router::create_app;
use axum::Router;
use std::env;
use tokio::net::TcpListener;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

pub fn initialize_sentry() -> sentry::ClientInitGuard {
    sentry::init((
        env::var("SENTRY_DSN").expect("SENTRY_DSN environment variable must be set"),
        sentry::ClientOptions {
            release: sentry::release_name!(),
            environment: Some(
                env::var("ENVIRONMENT")
                    .expect("ENVIRONMENT environment variable must be set")
                    .into(),
            ),
            traces_sample_rate: 1.0,
            ..Default::default()
        },
    ))
}

pub fn initialize_tracing() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::try_from_default_env()?)
        .with(tracing_subscriber::fmt::layer())
        .with(
            sentry::integrations::tracing::layer().event_filter(|metadata| {
                use sentry::integrations::tracing::EventFilter;
                match metadata.level() {
                    &tracing::Level::ERROR | &tracing::Level::WARN => EventFilter::Event,
                    _ => EventFilter::Breadcrumb,
                }
            }),
        )
        .try_init()?;
    Ok(())
}

pub async fn serve_app(listener: TcpListener, app: Router) -> std::io::Result<()> {
    axum::serve(listener, app).await
}

pub async fn run_server(bind_address: &str) -> std::io::Result<()> {
    tracing::info!("Starting datamanager service");

    let app = create_app().await;
    let listener = TcpListener::bind(bind_address).await?;

    serve_app(listener, app).await
}

#[cfg(test)]
mod tests {
    use super::{initialize_sentry, initialize_tracing, run_server, serve_app};
    use aws_credential_types::Credentials;
    use aws_sdk_s3::config::Region;
    use reqwest::StatusCode;
    use serial_test::serial;
    use std::time::Duration;

    use crate::{
        router::create_app_with_state,
        state::{MassiveSecrets, State},
    };

    struct EnvironmentVariableGuard {
        name: String,
        original_value: Option<String>,
    }

    impl EnvironmentVariableGuard {
        fn set(name: &str, value: &str) -> Self {
            let original_value = std::env::var(name).ok();
            // SAFETY: Environment variable mutation is safe here because:
            // 1. Tests using this guard are marked with #[serial] to prevent concurrent execution
            // 2. Env vars are set synchronously before spawning async tasks
            // 3. The Drop implementation ensures cleanup when guard goes out of scope
            unsafe {
                std::env::set_var(name, value);
            }

            Self {
                name: name.to_string(),
                original_value,
            }
        }
    }

    impl Drop for EnvironmentVariableGuard {
        fn drop(&mut self) {
            match self.original_value.as_ref() {
                Some(value) => {
                    // SAFETY: See set() method - protected by #[serial] annotation
                    unsafe {
                        std::env::set_var(&self.name, value);
                    }
                }
                None => {
                    // SAFETY: See set() method - protected by #[serial] annotation
                    unsafe {
                        std::env::remove_var(&self.name);
                    }
                }
            }
        }
    }

    async fn create_test_state() -> State {
        let credentials =
            Credentials::new("test-access-key", "test-secret-key", None, None, "tests");

        let shared_config = aws_config::defaults(aws_config::BehaviorVersion::latest())
            .region(Region::new("us-east-1"))
            .credentials_provider(credentials)
            .endpoint_url("http://127.0.0.1:9")
            .load()
            .await;

        let s3_config = aws_sdk_s3::config::Builder::from(&shared_config)
            .force_path_style(true)
            .build();

        let s3_client = aws_sdk_s3::Client::from_conf(s3_config);

        State::new(
            reqwest::Client::new(),
            MassiveSecrets {
                base: "http://127.0.0.1:1".to_string(),
                key: "test-api-key".to_string(),
            },
            s3_client,
            "test-bucket".to_string(),
        )
    }

    #[test]
    #[serial]
    fn test_initialize_observability_functions() {
        let _environment_guard = EnvironmentVariableGuard::set("ENVIRONMENT", "test");
        let _sentry_dsn_guard = EnvironmentVariableGuard::set("SENTRY_DSN", "");
        let _rust_log_guard =
            EnvironmentVariableGuard::set("RUST_LOG", "datamanager=debug,tower_http=debug");
        let _sentry_guard = initialize_sentry();
        let _ = initialize_tracing();
        let _ = initialize_tracing();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[serial]
    async fn test_serve_app_responds_on_health_route() {
        let app = create_app_with_state(create_test_state().await);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();

        let server_task = tokio::spawn(async move { serve_app(listener, app).await });

        let client = reqwest::Client::new();
        let health_url = format!("http://{}/health", address);

        let mut healthy = false;
        for _ in 0..20 {
            if let Ok(response) = client.get(&health_url).send().await {
                if response.status() == StatusCode::OK {
                    healthy = true;
                    break;
                }
            }

            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        server_task.abort();
        let _ = server_task.await;

        assert!(healthy);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[serial]
    async fn test_run_server_returns_error_for_invalid_bind_address() {
        let _region_guard = EnvironmentVariableGuard::set("AWS_REGION", "us-east-1");
        let _access_key_guard =
            EnvironmentVariableGuard::set("AWS_ACCESS_KEY_ID", "test-access-key");
        let _secret_key_guard =
            EnvironmentVariableGuard::set("AWS_SECRET_ACCESS_KEY", "test-secret-key");
        let _metadata_guard = EnvironmentVariableGuard::set("AWS_EC2_METADATA_DISABLED", "true");
        let _bucket_guard = EnvironmentVariableGuard::set("AWS_S3_DATA_BUCKET_NAME", "test-bucket");
        let _massive_base_guard =
            EnvironmentVariableGuard::set("MASSIVE_BASE_URL", "http://127.0.0.1:1");
        let _massive_key_guard = EnvironmentVariableGuard::set("MASSIVE_API_KEY", "test-api-key");

        let result = run_server("invalid-address").await;

        assert!(result.is_err());
    }
}
