//! Shared Alpaca API credentials for services that interact with the Alpaca trading API.

/// Alpaca API credentials for trading operations.
///
/// Constructed via `from_env()` to read credentials from environment variables,
/// or via `new()` for explicit construction (e.g. in tests).
#[derive(Clone)]
pub struct AlpacaCredentials {
    key_id: String,
    secret: String,
}

impl AlpacaCredentials {
    /// Constructs `AlpacaCredentials` from explicit field values.
    pub fn new(key_id: String, secret: String) -> Self {
        Self { key_id, secret }
    }

    /// Reads Alpaca credentials from environment variables.
    ///
    /// Reads `ALPACA_KEY_ID` and `ALPACA_SECRET`. Missing variables default to
    /// empty strings; the caller is responsible for validating that the values
    /// are non-empty before using them.
    pub fn from_env() -> Self {
        let key_id = std::env::var("ALPACA_KEY_ID").unwrap_or_default();
        let secret = std::env::var("ALPACA_SECRET").unwrap_or_default();
        Self { key_id, secret }
    }

    pub fn key_id(&self) -> &str {
        &self.key_id
    }

    pub fn secret(&self) -> &str {
        &self.secret
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_stores_fields() {
        let credentials = AlpacaCredentials::new("key123".to_string(), "secret456".to_string());
        assert_eq!(credentials.key_id(), "key123");
        assert_eq!(credentials.secret(), "secret456");
    }

    #[test]
    fn test_clone() {
        let credentials = AlpacaCredentials::new("key123".to_string(), "secret456".to_string());
        let cloned = credentials.clone();
        assert_eq!(cloned.key_id(), "key123");
        assert_eq!(cloned.secret(), "secret456");
    }

    #[test]
    fn test_from_env_falls_back_to_empty_strings_when_unset() {
        // Remove both vars to test the missing-variable path without touching
        // any other test's state — these are documented to default to "".
        let key_id_backup = std::env::var("ALPACA_KEY_ID").ok();
        let secret_backup = std::env::var("ALPACA_SECRET").ok();

        // SAFETY: environment mutation is safe in single-threaded test context.
        unsafe {
            std::env::remove_var("ALPACA_KEY_ID");
            std::env::remove_var("ALPACA_SECRET");
        }

        let credentials = AlpacaCredentials::from_env();
        assert_eq!(credentials.key_id(), "");
        assert_eq!(credentials.secret(), "");

        // Restore originals.
        unsafe {
            if let Some(value) = key_id_backup {
                std::env::set_var("ALPACA_KEY_ID", value);
            }
            if let Some(value) = secret_backup {
                std::env::set_var("ALPACA_SECRET", value);
            }
        }
    }
}
