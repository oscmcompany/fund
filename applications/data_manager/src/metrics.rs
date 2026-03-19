use axum::response::IntoResponse;
use metrics_exporter_prometheus::PrometheusHandle;
use std::sync::OnceLock;

static PROMETHEUS_HANDLE: OnceLock<PrometheusHandle> = OnceLock::new();

pub fn setup_metrics() {
    let handle = metrics_exporter_prometheus::PrometheusBuilder::new()
        .install_recorder()
        .expect("Failed to install Prometheus recorder");
    PROMETHEUS_HANDLE
        .set(handle)
        .expect("Metrics already initialized");
}

pub async fn get_metrics() -> impl IntoResponse {
    match PROMETHEUS_HANDLE.get() {
        Some(handle) => handle.render(),
        None => String::new(),
    }
}
