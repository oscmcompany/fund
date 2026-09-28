//! The HTTP surface: one page and one health check.
//!
//! Binds all interfaces so the exe.dev HTTP proxy can reach it, and serves until the process ends.

use axum::extract::State;
use axum::response::Html;
use axum::routing::get;
use axum::Router;
use tracing::info;

use crate::dashboard::cache::SharedState;
use crate::dashboard::html::render_html;

/// Port the dashboard listens on. Published externally via `ssh exe.dev publish`.
const PORT: u16 = 8084;

/// Builds the router. Separated from binding so the routes can be exercised without a socket.
pub fn build_router(state: SharedState) -> Router {
    Router::new()
        .route("/", get(render_dashboard))
        .route("/health", get(|| async { "ok" }))
        .with_state(state)
}

/// Serves on [`PORT`] until the process ends.
pub async fn run_server(state: SharedState) {
    let listener = tokio::net::TcpListener::bind(("0.0.0.0", PORT))
        .await
        .unwrap_or_else(|error| panic!("Failed to bind port {PORT}: {error}"));

    info!(port = PORT, "Dashboard server listening");
    axum::serve(listener, build_router(state))
        .await
        .unwrap_or_else(|error| panic!("Server error: {error}"));
}

/// Renders the cached state. Never touches the database.
async fn render_dashboard(State(state): State<SharedState>) -> Html<String> {
    let dashboard = state.read().await;
    Html(render_html(&dashboard))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dashboard::cache::DashboardState;
    use http::Request;
    use std::sync::Arc;
    use tokio::sync::RwLock;
    use tower::ServiceExt;

    fn router() -> Router {
        build_router(Arc::new(RwLock::new(DashboardState::default())))
    }

    async fn body_of(response: axum::response::Response) -> String {
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("a readable body");
        String::from_utf8(bytes.to_vec()).expect("valid utf-8")
    }

    #[tokio::test]
    async fn test_the_health_endpoint_answers_ok() {
        let request = Request::builder()
            .uri("/health")
            .body(axum::body::Body::empty())
            .expect("a valid request");
        let response = router().oneshot(request).await.expect("a response");

        assert_eq!(response.status(), 200);
        assert_eq!(body_of(response).await, "ok");
    }

    #[tokio::test]
    async fn test_the_index_serves_the_dashboard_as_html() {
        let request = Request::builder()
            .uri("/")
            .body(axum::body::Body::empty())
            .expect("a valid request");
        let response = router().oneshot(request).await.expect("a response");

        assert_eq!(response.status(), 200);
        let content_type = response
            .headers()
            .get("content-type")
            .expect("a content-type header")
            .to_str()
            .expect("an ascii content type");
        assert!(
            content_type.contains("text/html"),
            "expected text/html, got {content_type}"
        );

        let html = body_of(response).await;
        assert!(html.contains("<!DOCTYPE html>"));
        assert!(html.contains("OSCM"));
    }

    #[tokio::test]
    async fn test_an_unknown_path_is_a_404() {
        let request = Request::builder()
            .uri("/does-not-exist")
            .body(axum::body::Body::empty())
            .expect("a valid request");
        let response = router().oneshot(request).await.expect("a response");
        assert_eq!(response.status(), 404);
    }
}
