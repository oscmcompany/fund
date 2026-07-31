//! Structured tracing setup shared by all services.

use std::env;
use tracing_appender::non_blocking::WorkerGuard;
use tracing_appender::rolling::{RollingFileAppender, Rotation};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, Layer};

/// Initialize structured JSON tracing for a service.
///
/// The `service` parameter identifies which service is emitting logs
/// (e.g. `"data"`, `"inference"`, `"portfolio"`) and is included in the
/// initial `Tracing initialized` log line for correlation.
///
/// Logs to stdout at the `RUST_LOG` level (default `info`). When the log
/// directory is writable, also logs to a rolling daily file there; the
/// directory is `FUND_LOG_DIR` when set, otherwise `/var/log/fund` (local
/// dev sets `FUND_LOG_DIR` to a writable path via devenv). When `file_filter`
/// is `Some`, the file layer uses that fixed directive
/// (e.g. `"warn"` for an errors-only file), otherwise it follows the stdout
/// level. If the log directory cannot be created (e.g. in tests or restricted
/// environments), file logging is disabled and only stdout is used.
///
/// Returns `Some(WorkerGuard)` when file logging is active; the guard MUST be
/// held for the lifetime of the process, since dropping it tears down the
/// non-blocking file writer and buffered lines would be lost. Returns `None`
/// when file logging is disabled. Uses `try_init`, so calling this more than
/// once (e.g. across tests) is a no-op rather than a panic.
///
/// Services that own stdout for terminal rendering (e.g. the dashboard TUI)
/// should call [`init_tracing_file_only`] instead to avoid corrupting the
/// alternate screen with JSON log output.
pub fn init_tracing(
    log_file: &str,
    file_filter: Option<&str>,
    service: &str,
) -> Option<WorkerGuard> {
    let fund_profile = env::var("FUND_PROFILE").unwrap_or_else(|_| "unknown".to_string());

    let stdout_layer = tracing_subscriber::fmt::layer().json().with_target(true);
    let global_filter = || {
        tracing_subscriber::EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"))
    };

    let log_dir = env::var("FUND_LOG_DIR").unwrap_or_else(|_| "/var/log/fund".to_string());
    let guard = match std::fs::create_dir_all(&log_dir).and_then(|()| {
        RollingFileAppender::builder()
            .rotation(Rotation::DAILY)
            .filename_suffix(log_file)
            .build(&log_dir)
            .map_err(std::io::Error::other)
    }) {
        Ok(file_appender) => {
            let (non_blocking, guard) = tracing_appender::non_blocking(file_appender);
            let file_layer = tracing_subscriber::fmt::layer()
                .json()
                .with_writer(non_blocking)
                .with_filter(match file_filter {
                    Some(directive) => tracing_subscriber::EnvFilter::new(directive),
                    None => global_filter(),
                });

            // Only report file logging active when this call actually installed
            // the subscriber; a later try_init loses the race and its file layer
            // never attaches, so handing back the guard would mislead callers.
            let initialized = tracing_subscriber::registry()
                .with(global_filter())
                .with(stdout_layer)
                .with(file_layer)
                .try_init()
                .is_ok();
            if initialized {
                Some(guard)
            } else {
                None
            }
        }
        Err(error) => {
            eprintln!("File logging disabled: {error}");
            tracing_subscriber::registry()
                .with(global_filter())
                .with(stdout_layer)
                .try_init()
                .ok();
            None
        }
    };

    tracing::info!(
        service = service,
        fund_profile = %fund_profile,
        "Tracing initialized"
    );
    guard
}

/// Initialize structured JSON tracing to a file only, with no stdout output.
///
/// Identical to [`init_tracing`] except the stdout layer is omitted. Use this
/// for services that own stdout for terminal rendering (e.g. the dashboard TUI)
/// where writing JSON to stdout would corrupt the alternate screen buffer.
///
/// When the log directory is not writable, tracing is silently disabled rather
/// than falling back to stdout.
pub fn init_tracing_file_only(log_file: &str, service: &str) -> Option<WorkerGuard> {
    let fund_profile = env::var("FUND_PROFILE").unwrap_or_else(|_| "unknown".to_string());

    let log_dir = env::var("FUND_LOG_DIR").unwrap_or_else(|_| "/var/log/fund".to_string());
    let guard = match std::fs::create_dir_all(&log_dir).and_then(|()| {
        RollingFileAppender::builder()
            .rotation(Rotation::DAILY)
            .filename_suffix(log_file)
            .build(&log_dir)
            .map_err(std::io::Error::other)
    }) {
        Ok(file_appender) => {
            let (non_blocking, guard) = tracing_appender::non_blocking(file_appender);
            let file_layer = tracing_subscriber::fmt::layer()
                .json()
                .with_writer(non_blocking)
                .with_filter(
                    tracing_subscriber::EnvFilter::try_from_default_env()
                        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
                );
            let initialized = tracing_subscriber::registry()
                .with(file_layer)
                .try_init()
                .is_ok();
            if initialized {
                Some(guard)
            } else {
                None
            }
        }
        Err(_) => {
            // Deliberately installs nothing. Calling `try_init()` here would claim the global
            // dispatcher with an empty subscriber, and that claim is permanent for the process --
            // a later, working initialization would silently lose the race and emit nothing.
            // Leaving the default no-op dispatcher in place costs the same log output now and keeps
            // that door open.
            None
        }
    };

    tracing::info!(
        service = service,
        fund_profile = %fund_profile,
        "Tracing initialized"
    );
    guard
}

#[cfg(test)]
mod tests {
    use super::init_tracing;
    use serial_test::serial;
    use std::env;

    /// RAII guard that restores an environment variable on drop, panic-safe.
    ///
    /// Tests using this must be marked `#[serial]` to prevent concurrent env access.
    struct EnvVarRestoreGuard {
        key: &'static str,
        previous: Option<String>,
    }

    impl EnvVarRestoreGuard {
        fn save(key: &'static str) -> Self {
            Self {
                key,
                previous: env::var(key).ok(),
            }
        }
    }

    impl Drop for EnvVarRestoreGuard {
        fn drop(&mut self) {
            // SAFETY: Protected by #[serial_test::serial] — no concurrent env access.
            unsafe {
                match self.previous.as_ref() {
                    Some(value) => env::set_var(self.key, value),
                    None => env::remove_var(self.key),
                }
            }
        }
    }

    #[test]
    #[serial]
    fn test_init_tracing_is_idempotent() {
        let _first = init_tracing("test-observability.log", Some("warn"), "test");
        let _second = init_tracing("test-observability.log", None, "test");
    }

    #[test]
    #[serial]
    fn test_fund_log_dir_override_enables_file_logging() {
        let log_dir = env::temp_dir().join("fund-observability-test");
        let _ = std::fs::remove_dir_all(&log_dir);
        // Restored on unwind as well as on return. The manual restore this replaces was skipped
        // when the assertion below failed, leaving FUND_LOG_DIR pointing at a directory this test
        // then deleted -- which every later #[serial] test in the file inherited.
        let _restore = EnvVarRestoreGuard::save("FUND_LOG_DIR");
        // SAFETY: Protected by #[serial_test::serial] — no concurrent env access.
        unsafe { env::set_var("FUND_LOG_DIR", &log_dir) };

        let guard = init_tracing("test-observability.log", None, "test");

        assert!(log_dir.is_dir());
        let _ = guard;

        let _ = std::fs::remove_dir_all(&log_dir);
    }

    #[test]
    #[serial]
    fn test_fund_profile_env_var_is_read() {
        let _restore = EnvVarRestoreGuard::save("FUND_PROFILE");
        // SAFETY: Protected by #[serial_test::serial] — no concurrent env access.
        unsafe { env::set_var("FUND_PROFILE", "test-profile") };
        let _tracing_guard = init_tracing("test-profile-observability.log", None, "test");
    }

    #[test]
    #[serial]
    fn test_fund_profile_env_var_absent_uses_unknown() {
        let _restore = EnvVarRestoreGuard::save("FUND_PROFILE");
        // SAFETY: Protected by #[serial_test::serial] — no concurrent env access.
        unsafe { env::remove_var("FUND_PROFILE") };
        let _tracing_guard = init_tracing("test-no-profile-observability.log", None, "test");
    }
}
