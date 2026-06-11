use crate::common::server::serve;
use crate::data_manager::database::seed_equity_details;
use crate::data_manager::equity_details::read_equity_details_from_s3;
use crate::data_manager::equity_quotes::spawn_quote_stream;
use crate::data_manager::router::create_app_with_state;
use crate::data_manager::scheduler::spawn_sync_scheduler;
use crate::data_manager::state::State;
use tokio::net::TcpListener;

async fn migrate_equity_details(state: &State) {
    let pool = match state.database.pool() {
        Some(pool) => pool,
        None => {
            tracing::debug!("No database pool; skipping equity_details migration");
            return;
        }
    };

    match read_equity_details_from_s3(state).await {
        Ok(details) => match seed_equity_details(pool, &details).await {
            Ok(count) if count > 0 => {
                tracing::info!("Seeded equity_details from S3 ({} rows)", count);
            }
            Ok(_) => {}
            Err(err) => {
                tracing::warn!("equity_details migration failed: {}", err);
            }
        },
        Err(err) => {
            tracing::warn!(
                "Could not read equity details from S3 for migration: {}",
                err
            );
        }
    }
}

pub async fn run_server(bind_address: &str) -> std::io::Result<()> {
    tracing::info!("Starting data_manager service");

    let state = State::from_env().await;
    migrate_equity_details(&state).await;
    let listener = TcpListener::bind(bind_address).await?;
    spawn_sync_scheduler(state.clone());
    spawn_quote_stream(state.clone());
    let app = create_app_with_state(state);

    serve(listener, app).await
}

#[cfg(test)]
mod tests {
    use super::run_server;
    use crate::common::server::serve;
    use aws_credential_types::Credentials;
    use aws_sdk_s3::config::Region;
    use reqwest::StatusCode;
    use serial_test::serial;
    use std::time::Duration;

    use crate::data_manager::{
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

        fn remove(name: &str) -> Self {
            let original_value = std::env::var(name).ok();
            // SAFETY: See set() method - protected by #[serial] annotation
            unsafe {
                std::env::remove_var(name);
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

        // PgPool is None in tests that don't need PostgreSQL
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

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[serial]
    async fn test_serve_responds_on_health_route() {
        let app = create_app_with_state(create_test_state().await);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();

        let server_task = tokio::spawn(async move { serve(listener, app).await });

        let client = reqwest::Client::new();
        let health_url = format!("http://{}/health", address);

        let mut responded = false;
        for _ in 0..20 {
            if let Ok(response) = client.get(&health_url).send().await {
                let status = response.status();
                assert_eq!(
                    status,
                    StatusCode::SERVICE_UNAVAILABLE,
                    "Expected 503 with fake S3 endpoint, got {}",
                    status
                );
                let body: serde_json::Value = response.json().await.unwrap();
                assert_eq!(body["status"], "degraded");
                assert_eq!(body["checks"]["s3"], "error");
                responded = true;
                break;
            }

            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        server_task.abort();
        let _ = server_task.await;

        assert!(responded, "Health endpoint did not respond");
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
        let _bucket_guard = EnvironmentVariableGuard::set("AWS_S3_BUCKET_NAME", "test-bucket");
        let _massive_base_guard =
            EnvironmentVariableGuard::set("MASSIVE_BASE_URL", "http://127.0.0.1:1");
        let _massive_key_guard = EnvironmentVariableGuard::set("MASSIVE_API_KEY", "test-api-key");
        let _alpaca_key_guard = EnvironmentVariableGuard::set("ALPACA_KEY_ID", "test-key-id");
        let _alpaca_secret_guard = EnvironmentVariableGuard::set("ALPACA_SECRET", "test-secret");
        let _database_url_guard = EnvironmentVariableGuard::remove("DATABASE_URL");

        let result = run_server("invalid-address").await;

        assert!(result.is_err());
    }
}
