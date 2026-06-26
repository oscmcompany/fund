//! HTTP serving shared by all services.

use axum::Router;
use tokio::net::TcpListener;

/// Serve an Axum application on the given listener until it shuts down.
pub async fn serve(listener: TcpListener, app: Router) -> std::io::Result<()> {
    axum::serve(listener, app).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::routing::get;

    /// Binds a loopback listener, spawns `serve` on it, and immediately
    /// sends a GET request to confirm the server is reachable, then drops
    /// the runtime to shut it down.
    #[tokio::test]
    async fn test_serve_accepts_requests() {
        let app = Router::new().route("/ping", get(|| async { "pong" }));

        // Bind to a random free port on loopback.
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();

        // Spawn the server; it runs until the task is aborted when we drop
        // the JoinHandle at the end of the test.
        let server_handle = tokio::spawn(async move {
            serve(listener, app).await.ok();
        });

        let url = format!("http://{address}/ping");
        let deadline = tokio::time::Instant::now() + tokio::time::Duration::from_secs(1);
        let response = loop {
            match reqwest::get(&url).await {
                Ok(response) => break response,
                Err(error) if tokio::time::Instant::now() < deadline => {
                    let _ = error;
                    tokio::time::sleep(tokio::time::Duration::from_millis(25)).await;
                }
                Err(error) => panic!("HTTP request failed: {error}"),
            }
        };
        assert_eq!(response.status(), 200);

        server_handle.abort();
        let _ = server_handle.await;
    }
}
