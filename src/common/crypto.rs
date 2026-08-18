//! Process-wide TLS crypto provider selection.
//!
//! Every service binary calls [`install_default_crypto_provider`] first thing in `main`.

use rustls::crypto::aws_lc_rs;

/// Installs the `aws-lc-rs` Rustls [`CryptoProvider`](rustls::crypto::CryptoProvider)
/// as the process default.
///
/// The graph links both `aws-lc-rs` and `ring`, so Rustls cannot pick a default itself and the
/// first client configuration needing one panics — inside a spawned task, which surfaces as a
/// truncated sync rather than a crash. Idempotent, so calling it from every binary is correct.
pub fn install_default_crypto_provider() {
    let _ = aws_lc_rs::default_provider().install_default();
}

#[cfg(test)]
mod tests {
    use super::*;

    /// After installation a process-wide default provider must exist. Without it,
    /// Rustls panics on the first TLS handshake when several providers are linked
    /// (the bug this module guards against), so a present default is the fix.
    #[test]
    fn test_install_sets_a_default_provider() {
        install_default_crypto_provider();
        assert!(
            rustls::crypto::CryptoProvider::get_default().is_some(),
            "a default CryptoProvider must be installed after calling install_default_crypto_provider",
        );
    }

    /// Calling twice must not panic: the helper is invoked from several binaries
    /// and tests, so a second call has to be a harmless no-op.
    #[test]
    fn test_install_is_idempotent() {
        install_default_crypto_provider();
        install_default_crypto_provider();
        assert!(rustls::crypto::CryptoProvider::get_default().is_some());
    }
}
