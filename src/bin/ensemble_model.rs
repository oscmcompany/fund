#[tokio::main]
async fn main() {
    fund::ensemble_model::run("0.0.0.0:8082").await;
}
