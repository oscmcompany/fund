//! Data manager service: syncs equity data from the Massive API and serves it
//! over HTTP, backed by S3 and an optional PostgreSQL rolling buffer.

pub mod data;
pub mod database;
pub mod equity_bars;
pub mod equity_details;
pub mod equity_quotes;
pub mod errors;
pub mod export;
pub mod health;
pub mod router;
pub mod scheduler;
pub mod startup;
pub mod state;

/// Initialize tracing and run the HTTP server, returning a process exit code.
pub async fn run(bind_address: &str) -> i32 {
    let _tracing_guard =
        crate::common::observability::init_tracing("data-manager-errors.log", Some("warn"));
    handle_server_result(startup::run_server(bind_address).await)
}

fn handle_server_result(server_result: Result<(), std::io::Error>) -> i32 {
    match server_result {
        Ok(_) => 0,
        Err(error) => {
            tracing::error!("Server error: {}", error);
            1
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{handle_server_result, run};
    use serial_test::serial;

    #[test]
    fn test_handle_server_result_success() {
        assert_eq!(handle_server_result(Ok(())), 0);
    }

    #[test]
    fn test_handle_server_result_error() {
        let error = std::io::Error::new(std::io::ErrorKind::AddrNotAvailable, "bind failed");
        assert_eq!(handle_server_result(Err(error)), 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[serial]
    async fn test_run_returns_error_code_for_invalid_bind_address() {
        // SAFETY: Environment variable mutation is safe here because:
        // 1. Test is marked with #[serial] to prevent concurrent execution
        // 2. Env vars are set synchronously before spawning async tasks
        unsafe {
            std::env::set_var("AWS_S3_BUCKET_NAME", "test-bucket");
            std::env::set_var("MASSIVE_BASE_URL", "http://test");
            std::env::set_var("MASSIVE_API_KEY", "test-key");
            std::env::set_var("ALPACA_KEY_ID", "test-key-id");
            std::env::set_var("ALPACA_SECRET", "test-secret");
            std::env::set_var("RUST_LOG", "data_manager=debug,tower_http=debug");
            std::env::remove_var("DATABASE_URL");
        }

        let exit_code = run("invalid-address").await;

        assert_eq!(exit_code, 1);

        unsafe {
            std::env::remove_var("AWS_S3_BUCKET_NAME");
            std::env::remove_var("MASSIVE_BASE_URL");
            std::env::remove_var("MASSIVE_API_KEY");
            std::env::remove_var("ALPACA_KEY_ID");
            std::env::remove_var("ALPACA_SECRET");
            std::env::remove_var("RUST_LOG");
        }
    }
}
