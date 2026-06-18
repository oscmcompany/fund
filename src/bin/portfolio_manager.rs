#[tokio::main]
async fn main() {
    fund::common::crypto::install_default_crypto_provider();

    fund::portfolio_manager::run("0.0.0.0:8083").await;
}
