mod data;
mod equity_bars;
mod equity_details;
mod errors;
mod health;
mod portfolios;
mod predictions;
mod router;
mod state;
mod storage;

use router::create_app;
use std::env;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[tokio::main]
async fn main() {
    let _sentry_guard = sentry::init((
        env::var("SENTRY_DSN").unwrap_or_default(),
        sentry::ClientOptions {
            release: sentry::release_name!(),
            environment: Some(
                env::var("ENVIRONMENT")
                    .unwrap_or_else(|_| "development".to_string())
                    .into(),
            ),
            traces_sample_rate: 1.0,
            ..Default::default()
        },
    ));

    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "datamanager=debug,tower_http=debug,axum=debug".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .with(
            sentry::integrations::tracing::layer().event_filter(|metadata| {
                use sentry::integrations::tracing::EventFilter;
                match metadata.level() {
                    &tracing::Level::ERROR | &tracing::Level::WARN => EventFilter::Event,
                    _ => EventFilter::Breadcrumb,
                }
            }),
        )
        .init();

    tracing::info!("Starting datamanager service");

    let app = create_app().await;
    let listener = tokio::net::TcpListener::bind("0.0.0.0:8080").await.unwrap();

    if let Err(e) = axum::serve(listener, app).await {
        tracing::error!("Server error: {}", e);
        std::process::exit(1);
    }
}
