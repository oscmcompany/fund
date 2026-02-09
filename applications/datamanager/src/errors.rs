use aws_credential_types::provider::error::CredentialsError;
use duckdb::Error as DuckError;
use polars::prelude::PolarsError;
use thiserror::Error as ThisError;

#[derive(ThisError, Debug)]
pub enum Error {
    #[error("DuckDB error: {0}")]
    DuckDB(#[from] DuckError),
    #[error("Credentials error: {0}")]
    Credentials(#[from] CredentialsError),
    #[error("Polars error: {0}")]
    Polars(#[from] PolarsError),
    #[error("Other error: {0}")]
    Other(String),
}

#[cfg(test)]
mod tests {
    use super::*;
    use aws_credential_types::provider::error::CredentialsError;
    use duckdb::Error as DuckError;
    use polars::prelude::PolarsError;

    #[test]
    fn test_other_error_display() {
        let err = Error::Other("Test error message".to_string());
        let display = format!("{}", err);
        assert_eq!(display, "Other error: Test error message");
    }

    #[test]
    fn test_other_error_debug() {
        let err = Error::Other("Debug test".to_string());
        let debug = format!("{:?}", err);
        assert!(debug.contains("Other"));
        assert!(debug.contains("Debug test"));
    }

    #[test]
    fn test_duckdb_error_conversion() {
        let duck_err = DuckError::ExecuteReturnedResults;
        let err: Error = duck_err.into();
        let display = format!("{}", err);
        assert!(display.starts_with("DuckDB error:"));
    }

    #[test]
    fn test_polars_error_conversion() {
        let polars_err = PolarsError::NoData("test data".into());
        let err: Error = polars_err.into();
        let display = format!("{}", err);
        assert!(display.starts_with("Polars error:"));
    }

    #[test]
    fn test_credentials_error_conversion() {
        let cred_err = CredentialsError::not_loaded("test credentials error");
        let err: Error = cred_err.into();
        let display = format!("{}", err);
        assert!(display.starts_with("Credentials error:"));
    }
}
