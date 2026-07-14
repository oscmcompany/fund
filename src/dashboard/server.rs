//! Axum HTTP server for the dashboard service.
//!
//! Serves a single HTML page at `/` that renders the full dashboard state,
//! plus a `/health` endpoint for liveness checks. The page auto-refreshes
//! every 30 seconds via a `<meta>` tag matching the background poll interval.

use axum::extract::State;
use axum::response::Html;
use axum::routing::get;
use axum::Router;
use tracing::info;

use crate::dashboard::cache::SharedState;
use crate::dashboard::html::render_html;

/// Port the dashboard HTTP server listens on.
const PORT: u16 = 8084;

/// Starts the Axum HTTP server on [`PORT`].
///
/// Binds to all interfaces so the service is reachable via the exe.dev HTTP
/// proxy. Runs until the process is terminated.
pub async fn run_server(state: SharedState) {
    let application = Router::new()
        .route("/", get(render_dashboard))
        .route("/health", get(|| async { "ok" }))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(("0.0.0.0", PORT))
        .await
        .unwrap_or_else(|error| panic!("Failed to bind port {PORT}: {error}"));

    info!("Dashboard server listening on port {PORT}");
    axum::serve(listener, application)
        .await
        .unwrap_or_else(|error| panic!("Server error: {error}"));
}

/// Handles `GET /`: reads cached dashboard state and renders the full HTML page.
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

    fn build_router() -> Router {
        let state: SharedState = Arc::new(RwLock::new(DashboardState::default()));
        Router::new()
            .route("/", get(render_dashboard))
            .route("/health", get(|| async { "ok" }))
            .with_state(state)
    }

    #[tokio::test]
    async fn test_health_endpoint() {
        let router = build_router();
        let request = Request::builder()
            .uri("/health")
            .body(axum::body::Body::empty())
            .unwrap();
        let response = router.oneshot(request).await.unwrap();
        assert_eq!(response.status(), 200);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        assert_eq!(body.as_ref(), b"ok");
    }

    #[tokio::test]
    async fn test_dashboard_endpoint() {
        let router = build_router();
        let request = Request::builder()
            .uri("/")
            .body(axum::body::Body::empty())
            .unwrap();
        let response = router.oneshot(request).await.unwrap();
        assert_eq!(response.status(), 200);
        let content_type = response
            .headers()
            .get("content-type")
            .expect("content-type header should be present")
            .to_str()
            .unwrap();
        assert!(
            content_type.contains("text/html"),
            "expected text/html, got {content_type}"
        );
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let html = String::from_utf8(body.to_vec()).unwrap();
        assert!(html.contains("<!DOCTYPE html>"));
        assert!(html.contains("OSCM"));
    }
}
