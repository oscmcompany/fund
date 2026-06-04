//! HTTP serving shared by all services.

use axum::Router;
use tokio::net::TcpListener;

/// Serve an Axum application on the given listener until it shuts down.
pub async fn serve(listener: TcpListener, app: Router) -> std::io::Result<()> {
    axum::serve(listener, app).await
}
