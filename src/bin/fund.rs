//! Consolidated fund binary.
//!
//! Runs the data, inference, and portfolio services either together (default)
//! or individually via `--module <data|inference|portfolio>`. When split into
//! separate processes (e.g. via devenv process-compose), each process gets its
//! own log stream and TUI panel while still sharing the same PostgreSQL
//! event bus for inter-service coordination.

use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

const USAGE: &str = "Usage: fund [--module <data|inference|portfolio>]";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Module {
    Data,
    Inference,
    Portfolio,
}

impl Module {
    fn parse(value: &str) -> Result<Self, String> {
        match value {
            "data" => Ok(Self::Data),
            "inference" => Ok(Self::Inference),
            "portfolio" => Ok(Self::Portfolio),
            _ => Err(format!(
                "Unknown module '{}': expected 'data', 'inference', or 'portfolio'",
                value
            )),
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Data => "data",
            Self::Inference => "inference",
            Self::Portfolio => "portfolio",
        }
    }
}

fn parse_args() -> Result<Option<Module>, String> {
    let arguments: Vec<String> = std::env::args().skip(1).collect();
    if arguments.is_empty() {
        return Ok(None);
    }
    if arguments.len() == 2 && arguments[0] == "--module" {
        return Module::parse(&arguments[1]).map(Some);
    }
    Err(USAGE.to_string())
}

#[tokio::main]
async fn main() {
    fund::common::crypto::install_default_crypto_provider();

    let module = match parse_args() {
        Ok(module) => module,
        Err(message) => {
            eprintln!("{}", message);
            std::process::exit(1);
        }
    };

    let service_name = module.map(Module::as_str).unwrap_or("fund");
    let log_file = format!("{}.log", service_name);
    let _tracing_guard = fund::common::observability::init_tracing(&log_file, None, service_name);

    if let Err(error) = run(module).await {
        error!(error = %error, "Run failed");
        eprintln!("{}", error);
        std::process::exit(1);
    }
}

async fn run(module: Option<Module>) -> Result<(), Box<dyn std::error::Error>> {
    match module {
        Some(module) => info!(module = module.as_str(), "Starting fund service"),
        None => info!("Starting consolidated fund service"),
    }

    let database_url = std::env::var("DATABASE_URL")
        .map_err(|_| "DATABASE_URL environment variable must be set")?;
    let pool = sqlx::PgPool::connect(&database_url).await?;
    info!("Connected to PostgreSQL");

    let s3_client = fund::common::aws::s3_client().await;

    let run_data = module.is_none() || module == Some(Module::Data);
    let run_inference = module.is_none() || module == Some(Module::Inference);
    let run_portfolio = module.is_none() || module == Some(Module::Portfolio);

    // Construct fallible state objects before spawning any tasks so that a
    // configuration error (e.g. missing ALPACA_API_KEY_ID) aborts before any
    // background work starts, rather than leaving already-spawned tasks to be
    // killed without draining.
    let portfolio_state = if run_portfolio {
        Some(fund::portfolio::state::AppState::with_pool(pool.clone())?)
    } else {
        None
    };

    let shutdown_token = CancellationToken::new();
    let mut handles: Vec<JoinHandle<()>> = Vec::new();

    if run_data {
        let state = fund::data::state::State::with_pool(pool.clone(), s3_client.clone());
        let data_handles =
            fund::data::scheduler::spawn_sync_scheduler(state.clone(), shutdown_token.clone());
        handles.extend(data_handles);
        info!("Data service started");
    }

    if run_inference {
        let state = fund::inference::state::AppState::with_pool(pool.clone(), s3_client.clone());
        fund::inference::pipeline::poll_artifact_once(&state).await;
        handles.push(tokio::spawn(
            fund::inference::pipeline::start_artifact_polling(
                state.clone(),
                shutdown_token.clone(),
            ),
        ));
        handles.extend(fund::inference::consumer::spawn_event_consumer(
            state,
            shutdown_token.clone(),
        ));
        info!("Inference service started");
    }

    if let Some(state) = portfolio_state {
        handles.push(fund::portfolio::consumer::spawn_event_consumer(
            state,
            shutdown_token.clone(),
        ));
        info!("Portfolio service started");
    }

    info!("Waiting for events");
    await_shutdown_signal().await;
    info!("Shutdown signal received, waiting for consumers to drain");
    shutdown_token.cancel();

    for handle in handles {
        if let Err(error) = handle.await {
            warn!(error = %error, "Task failed during shutdown");
        }
    }

    info!("Shutdown complete");
    Ok(())
}

/// Waits for either SIGTERM or Ctrl+C (SIGINT).
async fn await_shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        match signal(SignalKind::terminate()) {
            Ok(mut sigterm) => {
                tokio::select! {
                    _ = tokio::signal::ctrl_c() => {}
                    _ = sigterm.recv() => {}
                }
            }
            Err(error) => {
                warn!(error = %error, "Failed to install SIGTERM handler, falling back to Ctrl+C only");
                let _ = tokio::signal::ctrl_c().await;
            }
        }
    }

    #[cfg(not(unix))]
    {
        tokio::signal::ctrl_c()
            .await
            .expect("Failed to listen for Ctrl+C");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_module_parse_valid_values() {
        assert_eq!(Module::parse("data").unwrap(), Module::Data);
        assert_eq!(Module::parse("inference").unwrap(), Module::Inference);
        assert_eq!(Module::parse("portfolio").unwrap(), Module::Portfolio);
    }

    #[test]
    fn test_module_parse_rejects_unknown() {
        assert!(Module::parse("unknown").is_err());
        assert!(Module::parse("").is_err());
    }

    #[test]
    fn test_module_as_str_round_trips() {
        for module in [Module::Data, Module::Inference, Module::Portfolio] {
            assert_eq!(Module::parse(module.as_str()).unwrap(), module);
        }
    }

    #[tokio::test]
    async fn test_cancellation_token_propagates_to_child() {
        let token = CancellationToken::new();
        let child = token.child_token();

        // Cancelling the parent token should resolve the child.
        token.cancel();
        child.cancelled().await;
        assert!(child.is_cancelled());
    }

    #[tokio::test]
    async fn test_cancel_token_drains_spawned_tasks() {
        let token = CancellationToken::new();
        let completed = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let completed_clone = completed.clone();

        let handle = tokio::spawn({
            let token = token.clone();
            async move {
                token.cancelled().await;
                completed_clone.store(true, std::sync::atomic::Ordering::SeqCst);
            }
        });

        // Task should be waiting on cancellation.
        tokio::task::yield_now().await;
        assert!(!completed.load(std::sync::atomic::Ordering::SeqCst));

        // Cancel and await — task should complete.
        token.cancel();
        handle.await.unwrap();
        assert!(completed.load(std::sync::atomic::Ordering::SeqCst));
    }
}
