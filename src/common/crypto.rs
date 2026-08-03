//! Process-wide TLS crypto provider selection.
//!
//! The dependency graph pulls in Rustls transitively and enables *both* the `aws-lc-rs` and `ring`
//! providers. With more than one linked, Rustls cannot pick a process default, and the first client
//! configuration that needs one panics. Requests run as spawned tasks, so the panic surfaces as a
//! failed task rather than a crash — a large sync comes back truncated instead of erroring.
//!
//! Every service binary calls [`install_default_crypto_provider`] as the first statement in `main`,
//! before any TLS client is constructed.

use rustls::crypto::aws_lc_rs;

/// Installs the `aws-lc-rs` Rustls [`CryptoProvider`](rustls::crypto::CryptoProvider)
/// as the process default.
///
/// Idempotent: `install_default` returns `Err` if a provider is already
/// installed, which is exactly the desired end state, so the result is ignored.
/// Safe to call from multiple binaries or more than once.
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
