#[tokio::main]
async fn main() {
    fund::ensemble_manager::run("0.0.0.0:8082").await;
}
