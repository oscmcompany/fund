//! The dashboard process. Everything it does lives in [`fund::dashboard`].

#[tokio::main]
async fn main() {
    fund::common::crypto::install_default_crypto_provider();
    fund::dashboard::run().await;
}
