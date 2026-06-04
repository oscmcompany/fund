use crate::data_manager::equity_bars;
use crate::data_manager::equity_details;
use crate::data_manager::health;
use crate::data_manager::state::State;
use axum::{
    routing::{get, post},
    Router,
};
use tower_http::trace::TraceLayer;

pub async fn create_app() -> Router {
    let state = State::from_env().await;
    create_app_with_state(state)
}

pub fn create_app_with_state(state: State) -> Router {
    Router::new()
        .route("/health", get(health::get_health))
        .route("/equity-bars", post(equity_bars::sync))
        .route("/equity-bars/recent", get(equity_bars::query_recent))
        .route("/equity-details", get(equity_details::get))
        .route("/equity-details", post(equity_details::sync))
        .with_state(state)
        .layer(TraceLayer::new_for_http())
}
