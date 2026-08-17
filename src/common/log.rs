//! Structured tracing setup shared by all services.

use std::env;
use tracing_appender::non_blocking::WorkerGuard;
use tracing_appender::rolling::{RollingFileAppender, Rotation};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, Layer};

/// Where service logs are written when nothing overrides it.
///
/// Distinct from [`crate::common::journal::DEFAULT_JOURNAL_DIRECTORY`]: these are diagnostics and
/// are filtered by `RUST_LOG`, so a line that never reaches this tree is a line nobody asked for.
/// The journal is the opposite and lives on its own path for that reason.
pub const DEFAULT_LOG_DIRECTORY: &str = "/var/log/fund";

/// Initialize structured JSON tracing for a service.
///
/// `service` (e.g. `"fund"`, `"tide-model-trainer"`) is included in the initial log line for
/// correlation.
///
/// Logs to stdout at `RUST_LOG` (default `info`), and to a rolling daily file under
/// `FUND_LOG_DIRECTORY`. `file_filter` overrides the file layer's level; `None` follows stdout. An
/// uncreatable log directory disables file logging rather than failing.
///
/// `Some` means *this call* installed the file layer, and the `WorkerGuard` MUST then be held for
/// the process lifetime — dropping it tears down the non-blocking writer and buffered lines are
/// lost. `None` means it did not, either because file logging is off or because `try_init` found a
/// subscriber already installed, which is what makes repeated calls across tests a no-op.
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

    let log_directory =
        env::var("FUND_LOG_DIRECTORY").unwrap_or_else(|_| DEFAULT_LOG_DIRECTORY.to_string());
    let guard = match std::fs::create_dir_all(&log_directory).and_then(|()| {
        RollingFileAppender::builder()
            .rotation(Rotation::DAILY)
            .filename_suffix(log_file)
            .build(&log_directory)
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
/// Like [`init_tracing`] but file-only and with no `file_filter` — the file layer always follows
/// `RUST_LOG`. Used by the dashboard. An unwritable log directory installs nothing at all rather
/// than falling back to stdout.
pub fn init_tracing_file_only(log_file: &str, service: &str) -> Option<WorkerGuard> {
    let fund_profile = env::var("FUND_PROFILE").unwrap_or_else(|_| "unknown".to_string());

    let log_directory =
        env::var("FUND_LOG_DIRECTORY").unwrap_or_else(|_| DEFAULT_LOG_DIRECTORY.to_string());
    let guard = match std::fs::create_dir_all(&log_directory).and_then(|()| {
        RollingFileAppender::builder()
            .rotation(Rotation::DAILY)
            .filename_suffix(log_file)
            .build(&log_directory)
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
    fn test_the_log_directory_override_enables_file_logging() {
        let log_directory = env::temp_dir().join("fund-log-directory-test");
        let _ = std::fs::remove_dir_all(&log_directory);
        // Restored on unwind as well as on return. The manual restore this replaces was skipped
        // when the assertion below failed, leaving FUND_LOG_DIRECTORY pointing at a directory this test
        // then deleted -- which every later #[serial] test in the file inherited.
        let _restore = EnvVarRestoreGuard::save("FUND_LOG_DIRECTORY");
        // SAFETY: Protected by #[serial_test::serial] — no concurrent env access.
        unsafe { env::set_var("FUND_LOG_DIRECTORY", &log_directory) };

        let guard = init_tracing("test-observability.log", None, "test");

        assert!(log_directory.is_dir());
        let _ = guard;

        let _ = std::fs::remove_dir_all(&log_directory);
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
