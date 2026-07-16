pub mod balancer;
pub mod config;
pub mod health;
pub mod proxy;
pub mod state;
pub mod watcher;

use std::sync::Arc;

use axum::routing::{any, get};
use axum::Router;

use state::AppState;

/// Builds the axum router: `/_proxy/status` for introspection, and
/// everything else falls through to the proxy handler.
pub fn build_router(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/_proxy/status", get(proxy::status_handler))
        .fallback(any(proxy::proxy_handler))
        .with_state(state)
}
