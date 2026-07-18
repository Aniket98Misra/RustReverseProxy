use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use hyper::{Request, StatusCode};

use crate::state::{AppState, ProxyClient};

/// Runs forever, polling every backend's health-check path on a fixed
/// interval and flipping `Backend::alive` based on the result. Each
/// backend is checked concurrently (spawned as its own task) so one slow
/// or hung backend can't delay the check for the others — with N
/// backends and a naive sequential loop, a single stuck backend would
/// stall detection for everything after it in the list.
pub async fn run(state: Arc<AppState>, interval: Duration) {
    let mut ticker = tokio::time::interval(interval);
    // First tick fires immediately; skip it so we don't double-check
    // right after the initial (optimistic) startup state.
    ticker.tick().await;

    loop {
        ticker.tick().await;
        let backends = state.all_backends();
        let path = state
            .health_check_path
            .read()
            .expect("health_check_path lock poisoned")
            .clone();
        let timeout = *state
            .health_check_timeout
            .read()
            .expect("health_check_timeout lock poisoned");
        let client = state.client.clone();

        for backend in backends {
            let client = client.clone();
            let path = path.clone();
            tokio::spawn(async move {
                let healthy = check_once(&client, &backend.addr, &path, timeout).await;
                let was_alive = backend.alive.swap(healthy, Ordering::Relaxed);
                if was_alive && !healthy {
                    tracing::warn!(
                        backend = %backend.addr,
                        "health check failed — removing from rotation"
                    );
                } else if !was_alive && healthy {
                    tracing::info!(
                        backend = %backend.addr,
                        "health check recovered — back in rotation"
                    );
                }
            });
        }
    }
}

async fn check_once(client: &ProxyClient, base_addr: &str, path: &str, timeout: Duration) -> bool {
    let uri: hyper::Uri = match format!("{base_addr}{path}").parse() {
        Ok(u) => u,
        Err(_) => return false,
    };

    let req = match Request::builder()
        .method("GET")
        .uri(uri)
        .body(axum::body::Body::empty())
    {
        Ok(r) => r,
        Err(_) => return false,
    };

    match tokio::time::timeout(timeout, client.request(req)).await {
        // Request completed within the timeout — healthy iff it's a 2xx.
        Ok(Ok(resp)) => resp.status().is_success() || resp.status() == StatusCode::OK,
        // Request completed but errored (connection refused, etc), or
        // timed out — either way, not healthy.
        Ok(Err(_)) | Err(_) => false,
    }
}
