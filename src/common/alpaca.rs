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
    ///
    /// Returns `Err` if either `key_id` or `secret` is empty.
    pub fn new(key_id: String, secret: String) -> Result<Self, String> {
        if key_id.is_empty() {
            return Err("key_id must not be empty".to_string());
        }
        if secret.is_empty() {
            return Err("secret must not be empty".to_string());
        }
        Ok(Self { key_id, secret })
    }

    /// Reads Alpaca credentials from environment variables.
    ///
    /// Reads `ALPACA_KEY_ID` and `ALPACA_SECRET`. Returns `Err` if either
    /// variable is absent or set to an empty string.
    pub fn from_env() -> Result<Self, String> {
        let key_id = std::env::var("ALPACA_KEY_ID")
            .map_err(|_| "ALPACA_KEY_ID environment variable is not set".to_string())?;
        let secret = std::env::var("ALPACA_SECRET")
            .map_err(|_| "ALPACA_SECRET environment variable is not set".to_string())?;
        Self::new(key_id, secret)
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
    use serial_test::serial;

    #[test]
    fn test_new_stores_fields() {
        let credentials =
            AlpacaCredentials::new("key123".to_string(), "secret456".to_string()).unwrap();
        assert_eq!(credentials.key_id(), "key123");
        assert_eq!(credentials.secret(), "secret456");
    }

    #[test]
    fn test_new_rejects_empty_key_id() {
        let result = AlpacaCredentials::new(String::new(), "secret456".to_string());
        assert!(result.is_err());
        assert!(result.err().unwrap().contains("key_id"));
    }

    #[test]
    fn test_new_rejects_empty_secret() {
        let result = AlpacaCredentials::new("key123".to_string(), String::new());
        assert!(result.is_err());
        assert!(result.err().unwrap().contains("secret"));
    }

    #[test]
    fn test_clone() {
        let credentials =
            AlpacaCredentials::new("key123".to_string(), "secret456".to_string()).unwrap();
        let cloned = credentials.clone();
        assert_eq!(cloned.key_id(), "key123");
        assert_eq!(cloned.secret(), "secret456");
    }

    #[test]
    #[serial]
    fn test_from_env_returns_error_when_vars_unset() {
        // Remove both vars to test the missing-variable path.
        let key_id_backup = std::env::var("ALPACA_KEY_ID").ok();
        let secret_backup = std::env::var("ALPACA_SECRET").ok();

        // SAFETY: environment mutation is safe in single-threaded test context.
        unsafe {
            std::env::remove_var("ALPACA_KEY_ID");
            std::env::remove_var("ALPACA_SECRET");
        }

        let result = AlpacaCredentials::from_env();
        assert!(result.is_err());

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
