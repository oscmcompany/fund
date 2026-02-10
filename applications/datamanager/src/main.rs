use datamanager::startup::{initialize_sentry, initialize_tracing, run_server};

async fn run_with_bind_address(bind_address: &str) -> i32 {
    let _sentry_guard = initialize_sentry();
    initialize_tracing();

    handle_server_result(run_server(bind_address).await)
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

#[tokio::main]
async fn main() {
    let exit_code = run_with_bind_address("0.0.0.0:8080").await;

    if exit_code != 0 {
        std::process::exit(exit_code);
    }
}

#[cfg(test)]
mod tests {
    use super::{handle_server_result, run_with_bind_address};

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
    async fn test_run_with_bind_address_returns_error_code_for_invalid_bind_address() {
        let exit_code = run_with_bind_address("invalid-address").await;

        assert_eq!(exit_code, 1);
    }
}
