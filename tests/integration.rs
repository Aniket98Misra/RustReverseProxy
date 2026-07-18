//! End-to-end tests: spin up real mock backend servers, spin up the real
//! proxy router in-process, and hit it over real HTTP — no mocking of
//! the routing logic itself. Uses the project's own `hyper_util` client
//! (already a main dependency) rather than pulling in a separate HTTP
//! client crate just for tests.

use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::get as get_route;
use axum::{Json, Router};
use hyper::Request;
use tokio::net::TcpListener;

use reverse_proxy::config::{Algorithm, BackendConfig, Config};
use reverse_proxy::state::{make_client, AppState};

/// A minimal in-process stand-in for `mock-backend`, spun up directly as
/// a task rather than a subprocess so tests stay fast and self-contained.
struct TestBackend {
    addr: SocketAddr,
    healthy: Arc<AtomicBool>,
    hits: Arc<AtomicU64>,
}

async fn spawn_test_backend(name: &'static str) -> TestBackend {
    #[derive(Clone)]
    struct S {
        name: &'static str,
        healthy: Arc<AtomicBool>,
        hits: Arc<AtomicU64>,
    }

    async fn root(State(s): State<S>) -> Json<serde_json::Value> {
        s.hits.fetch_add(1, Ordering::Relaxed);
        Json(serde_json::json!({ "name": s.name }))
    }

    async fn health(State(s): State<S>) -> StatusCode {
        if s.healthy.load(Ordering::Relaxed) {
            StatusCode::OK
        } else {
            StatusCode::SERVICE_UNAVAILABLE
        }
    }

    async fn slow_root(State(s): State<S>) -> Json<serde_json::Value> {
        // Used to test least-connections: holds the "connection" open
        // long enough for a concurrent request to observe it as busy.
        s.hits.fetch_add(1, Ordering::Relaxed);
        tokio::time::sleep(Duration::from_millis(300)).await;
        Json(serde_json::json!({ "name": s.name }))
    }

    let healthy = Arc::new(AtomicBool::new(true));
    let hits = Arc::new(AtomicU64::new(0));
    let state = S {
        name,
        healthy: healthy.clone(),
        hits: hits.clone(),
    };

    let app = Router::new()
        .route("/", get_route(root))
        .route("/slow", get_route(slow_root))
        .route("/health", get_route(health))
        .with_state(state);

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    TestBackend { addr, healthy, hits }
}

async fn spawn_proxy(cfg: Config) -> SocketAddr {
    let state = Arc::new(AppState::from_config(&cfg));
    let router = reverse_proxy::build_router(state.clone());

    // Fire off one health-check pass immediately-ish so tests don't have
    // to wait a full interval for backends to be marked alive/dead —
    // `run` intentionally skips its first tick (see health.rs), so tests
    // that care about health-driven behavior trigger a check manually.
    tokio::spawn(reverse_proxy::health::run(state, Duration::from_millis(100)));

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    addr
}

async fn http_get(addr: SocketAddr, path: &str) -> (StatusCode, String) {
    let client = make_client();
    let uri: hyper::Uri = format!("http://{addr}{path}").parse().unwrap();
    let req = Request::builder()
        .uri(uri)
        .body(Body::empty())
        .unwrap();
    let resp = client.request(req).await.unwrap();
    let status = resp.status();
    let body = Body::new(resp.into_body());
    let bytes = axum::body::to_bytes(body, usize::MAX).await.unwrap();
    (status, String::from_utf8_lossy(&bytes).to_string())
}

fn cfg_with(backends: &[SocketAddr], algorithm: Algorithm) -> Config {
    Config {
        listen_addr: "127.0.0.1:0".into(),
        algorithm,
        health_check_interval_secs: 1,
        health_check_timeout_secs: 1,
        health_check_path: "/health".into(),
        backends: backends
            .iter()
            .map(|a| BackendConfig {
                addr: format!("http://{a}"),
            })
            .collect(),
    }
}

#[tokio::test]
async fn proxy_forwards_requests_round_robin() {
    let b0 = spawn_test_backend("b0").await;
    let b1 = spawn_test_backend("b1").await;

    let cfg = cfg_with(&[b0.addr, b1.addr], Algorithm::RoundRobin);
    let proxy_addr = spawn_proxy(cfg).await;

    // Round robin, no health-check dependency: everything starts alive.
    for _ in 0..4 {
        let (status, _) = http_get(proxy_addr, "/").await;
        assert_eq!(status, StatusCode::OK);
    }

    let h0 = b0.hits.load(Ordering::Relaxed);
    let h1 = b1.hits.load(Ordering::Relaxed);
    assert_eq!(h0, 2, "backend 0 should get exactly half the requests");
    assert_eq!(h1, 2, "backend 1 should get exactly half the requests");
}

#[tokio::test]
async fn proxy_routes_away_from_unhealthy_backend() {
    let b0 = spawn_test_backend("b0").await;
    let b1 = spawn_test_backend("b1").await;
    b1.healthy.store(false, Ordering::Relaxed);

    let cfg = cfg_with(&[b0.addr, b1.addr], Algorithm::RoundRobin);
    let proxy_addr = spawn_proxy(cfg).await;

    // Give the health-check loop (100ms interval in spawn_proxy) time to
    // run at least once and mark b1 dead.
    tokio::time::sleep(Duration::from_millis(400)).await;

    for _ in 0..5 {
        let (status, _) = http_get(proxy_addr, "/").await;
        assert_eq!(status, StatusCode::OK);
    }

    assert_eq!(
        b1.hits.load(Ordering::Relaxed),
        0,
        "unhealthy backend should never receive traffic"
    );
    assert_eq!(
        b0.hits.load(Ordering::Relaxed),
        5,
        "all traffic should land on the sole healthy backend"
    );
}

#[tokio::test]
async fn proxy_returns_503_when_no_backends_are_healthy() {
    let b0 = spawn_test_backend("b0").await;
    b0.healthy.store(false, Ordering::Relaxed);

    let cfg = cfg_with(&[b0.addr], Algorithm::RoundRobin);
    let proxy_addr = spawn_proxy(cfg).await;

    tokio::time::sleep(Duration::from_millis(400)).await;

    let (status, _) = http_get(proxy_addr, "/").await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
}

#[tokio::test]
async fn least_connections_prefers_the_idler_backend_under_concurrent_load() {
    let b0 = spawn_test_backend("b0").await;
    let b1 = spawn_test_backend("b1").await;

    let cfg = cfg_with(&[b0.addr, b1.addr], Algorithm::LeastConnections);
    let proxy_addr = spawn_proxy(cfg).await;

    // Send a slow request to b0's counterpart first and let it sit
    // in-flight, occupying one backend's connection slot, then fire a
    // burst of fast requests and confirm they all land on the other
    // (idle) backend rather than round-robining onto the busy one.
    let slow_handle = tokio::spawn(async move {
        http_get(proxy_addr, "/slow").await;
    });
    // Let the slow request actually be picked and in-flight before firing more.
    tokio::time::sleep(Duration::from_millis(50)).await;

    for _ in 0..4 {
        let (status, _) = http_get(proxy_addr, "/").await;
        assert_eq!(status, StatusCode::OK);
    }
    slow_handle.await.unwrap();

    // Whichever backend took the slow request should have gotten
    // strictly fewer of the subsequent fast requests than the other one
    // — least-connections should have steered around it while it was busy.
    let h0 = b0.hits.load(Ordering::Relaxed);
    let h1 = b1.hits.load(Ordering::Relaxed);
    assert_eq!(h0 + h1, 5, "1 slow + 4 fast requests total");
    assert!(
        (h0 == 1 && h1 == 4) || (h0 == 4 && h1 == 1),
        "expected a 1/4 split favoring the idle backend, got b0={h0} b1={h1}"
    );
}

#[tokio::test]
async fn status_endpoint_reports_live_backend_state() {
    let b0 = spawn_test_backend("b0").await;
    let cfg = cfg_with(&[b0.addr], Algorithm::RoundRobin);
    let proxy_addr = spawn_proxy(cfg).await;

    let (status, body) = http_get(proxy_addr, "/_proxy/status").await;
    assert_eq!(status, StatusCode::OK);
    assert!(body.contains("RoundRobin"));
    assert!(body.contains(&b0.addr.to_string()));
}
