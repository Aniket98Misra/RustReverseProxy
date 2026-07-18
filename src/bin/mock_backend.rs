//! A tiny standalone HTTP server used to stand in for a "real" backend
//! when demoing or testing the proxy. Run several of these on different
//! ports to see round-robin / least-connections routing and health-check
//! failover in action.
//!
//! Each instance can be told to go "unhealthy" on demand via
//! `POST /admin/toggle-health`, without killing the process — useful for
//! demoing active health checking without needing to actually crash
//! anything (and for integration tests that need a deterministic way to
//! flip a backend's health).

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{Json, Router};
use clap::Parser;
use serde::Serialize;

#[derive(Parser, Debug)]
struct Args {
    #[arg(short, long)]
    port: u16,

    #[arg(short, long, default_value = "backend")]
    name: String,
}

struct MockState {
    name: String,
    port: u16,
    healthy: AtomicBool,
    request_count: std::sync::atomic::AtomicU64,
}

#[derive(Serialize)]
struct RootResponse {
    name: String,
    port: u16,
    request_count: u64,
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt().init();
    let args = Args::parse();

    let state = Arc::new(MockState {
        name: args.name.clone(),
        port: args.port,
        healthy: AtomicBool::new(true),
        request_count: std::sync::atomic::AtomicU64::new(0),
    });

    let app = Router::new()
        .route("/", get(root))
        .route("/health", get(health))
        .route("/admin/toggle-health", post(toggle_health))
        .with_state(state);

    let addr = format!("127.0.0.1:{}", args.port);
    tracing::info!(name = %args.name, %addr, "mock backend listening");
    let listener = tokio::net::TcpListener::bind(&addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}

async fn root(State(state): State<Arc<MockState>>) -> Json<RootResponse> {
    let count = state.request_count.fetch_add(1, Ordering::Relaxed) + 1;
    Json(RootResponse {
        name: state.name.clone(),
        port: state.port,
        request_count: count,
    })
}

async fn health(State(state): State<Arc<MockState>>) -> StatusCode {
    if state.healthy.load(Ordering::Relaxed) {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    }
}

async fn toggle_health(State(state): State<Arc<MockState>>) -> Json<serde_json::Value> {
    let new_val = !state.healthy.load(Ordering::Relaxed);
    state.healthy.store(new_val, Ordering::Relaxed);
    tracing::info!(name = %state.name, healthy = new_val, "health toggled via admin endpoint");
    Json(serde_json::json!({ "name": state.name, "healthy": new_val }))
}
