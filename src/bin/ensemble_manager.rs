#[tokio::main]
async fn main() {
    fund::common::crypto::install_default_crypto_provider();

    fund::ensemble_manager::run("0.0.0.0:8082").await;
}
