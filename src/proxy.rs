use std::sync::Arc;

use axum::extract::State;
use axum::http::{StatusCode, Uri};
use axum::response::{IntoResponse, Json, Response};
use axum::body::Body;
use hyper::Request;
use serde::Serialize;

use crate::balancer::pick_backend;
use crate::state::{AppState, ConnectionGuard};

/// Forwards an incoming request to whichever backend the configured
/// algorithm selects, and streams the backend's response straight back
/// to the client.
pub async fn proxy_handler(
    State(state): State<Arc<AppState>>,
    mut req: Request<Body>,
) -> Response {
    let backend = match pick_backend(&state) {
        Some(b) => b,
        None => {
            tracing::warn!("no healthy backends available");
            return (StatusCode::SERVICE_UNAVAILABLE, "no healthy backends available")
                .into_response();
        }
    };

    // Held for the lifetime of this function; decrements on every exit
    // path (success, error, early return) via Drop.
    let _guard = ConnectionGuard::acquire(Arc::clone(&backend));

    let downstream_uri = match rewrite_uri(&backend.addr, req.uri()) {
        Ok(u) => u,
        Err(_) => {
            return (StatusCode::BAD_GATEWAY, "failed to construct downstream URI")
                .into_response();
        }
    };
    *req.uri_mut() = downstream_uri;

    match state.client.request(req).await {
        Ok(resp) => resp.into_response(),
        Err(e) => {
            tracing::warn!(backend = %backend.addr, error = %e, "upstream request failed");
            (StatusCode::BAD_GATEWAY, "upstream request failed").into_response()
        }
    }
}

fn rewrite_uri(backend_addr: &str, original: &Uri) -> Result<Uri, hyper::http::uri::InvalidUri> {
    let path_and_query = original
        .path_and_query()
        .map(|pq| pq.as_str())
        .unwrap_or("/");
    format!("{backend_addr}{path_and_query}").parse()
}

#[derive(Serialize)]
pub struct BackendStatus {
    id: usize,
    addr: String,
    alive: bool,
    active_connections: usize,
}

#[derive(Serialize)]
pub struct ProxyStatus {
    algorithm: String,
    backends: Vec<BackendStatus>,
}

/// `GET /_proxy/status` — live introspection of routing state. Not
/// proxied to a backend; handled directly so you can watch the effect
/// of a config-file edit or a backend dying in real time, e.g.:
///
///   watch -n1 'curl -s localhost:8080/_proxy/status | jq'
pub async fn status_handler(State(state): State<Arc<AppState>>) -> Json<ProxyStatus> {
    let algorithm = format!("{:?}", *state.algorithm.read().expect("algorithm lock poisoned"));
    let backends = state
        .all_backends()
        .into_iter()
        .map(|b| BackendStatus {
            id: b.id,
            addr: b.addr.clone(),
            alive: b.is_alive(),
            active_connections: b.connections(),
        })
        .collect();

    Json(ProxyStatus { algorithm, backends })
}
