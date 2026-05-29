use tracing::info;
use tracing_appender::rolling;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{fmt, EnvFilter};

use ensemble_model::server;
use ensemble_model::state::AppState;

#[tokio::main]
async fn main() {
    let file_appender = rolling::daily("/var/log/fund", "ensemble-model.log");
    let (non_blocking, _guard) = tracing_appender::non_blocking(file_appender);

    tracing_subscriber::registry()
        .with(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .with(fmt::layer().json().with_writer(std::io::stdout))
        .with(fmt::layer().json().with_writer(non_blocking))
        .init();

    info!("Starting ensemble model service");

    let state = AppState::from_env().await;

    let app = server::create_router(state.clone());

    tokio::spawn(server::start_artifact_polling(state));

    let listener = tokio::net::TcpListener::bind("0.0.0.0:8082")
        .await
        .expect("Failed to bind to port 8082");

    info!("Listening on 0.0.0.0:8082");
    axum::serve(listener, app).await.expect("Server failed");
}
