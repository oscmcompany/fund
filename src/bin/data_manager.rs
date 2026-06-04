#[tokio::main]
async fn main() {
    let exit_code = fund::data_manager::run("0.0.0.0:8080").await;

    if exit_code != 0 {
        std::process::exit(exit_code);
    }
}
