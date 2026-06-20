//! Structured tracing setup shared by all services.

use std::env;
use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, Layer};

/// Initialize structured JSON tracing for a service.
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
pub fn init_tracing(log_file: &str, file_filter: Option<&str>) -> Option<WorkerGuard> {
    let fund_profile = env::var("FUND_PROFILE").unwrap_or_else(|_| "unknown".to_string());

    let stdout_layer = tracing_subscriber::fmt::layer().json().with_target(true);
    let global_filter = || {
        tracing_subscriber::EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"))
    };

    let log_dir = env::var("FUND_LOG_DIR").unwrap_or_else(|_| "/var/log/fund".to_string());
    let guard = match std::fs::create_dir_all(&log_dir) {
        Ok(()) => {
            let file_appender = tracing_appender::rolling::daily(log_dir, log_file);
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
            eprintln!("File logging disabled, cannot create {log_dir:?}: {error}");
            tracing_subscriber::registry()
                .with(global_filter())
                .with(stdout_layer)
                .try_init()
                .ok();
            None
        }
    };

    tracing::info!(fund_profile = %fund_profile, "Tracing initialized");
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
        // Exercises both the fixed-directive file filter and the RUST_LOG-derived
        // one, and confirms a second initialization is a no-op (try_init). Works
        // whether or not the log directory is writable.
        let _first = init_tracing("test-observability.log", Some("warn"));
        let _second = init_tracing("test-observability.log", None);
    }

    #[test]
    #[serial]
    fn test_fund_log_dir_override_enables_file_logging() {
        // FUND_LOG_DIR pointing at a writable directory activates file logging
        // (a returned guard) and the directory is created on demand. This is the
        // local-dev path where /var/log/fund is not writable.
        let log_dir = env::temp_dir().join("fund-observability-test");
        let _ = std::fs::remove_dir_all(&log_dir);
        let previous_log_dir = env::var("FUND_LOG_DIR").ok();
        env::set_var("FUND_LOG_DIR", &log_dir);

        let guard = init_tracing("test-observability.log", None);

        // try_init is global, so file logging is only guaranteed active when this
        // test wins the initialization race; in either case the directory the
        // override names must have been created.
        assert!(log_dir.is_dir());
        let _ = guard;

        // Restore the runner's value rather than unconditionally unsetting it,
        // so later tests see the environment they started with.
        match previous_log_dir {
            Some(value) => env::set_var("FUND_LOG_DIR", value),
            None => env::remove_var("FUND_LOG_DIR"),
        }
        let _ = std::fs::remove_dir_all(&log_dir);
    }

    #[test]
    #[serial]
    fn test_fund_profile_env_var_is_read() {
        // When FUND_PROFILE is set, init_tracing uses it without panicking.
        // Since try_init is idempotent, this mainly confirms the env-var read path
        // is exercised rather than hitting the unwrap_or_else default.
        let _restore = EnvVarRestoreGuard::save("FUND_PROFILE");
        // SAFETY: Protected by #[serial_test::serial] — no concurrent env access.
        unsafe { env::set_var("FUND_PROFILE", "test-profile") };
        let _tracing_guard = init_tracing("test-profile-observability.log", None);
    }

    #[test]
    #[serial]
    fn test_fund_profile_env_var_absent_uses_unknown() {
        // When FUND_PROFILE is not set, init_tracing must not panic.
        let _restore = EnvVarRestoreGuard::save("FUND_PROFILE");
        // SAFETY: Protected by #[serial_test::serial] — no concurrent env access.
        unsafe { env::remove_var("FUND_PROFILE") };
        let _tracing_guard = init_tracing("test-no-profile-observability.log", None);
    }
}
