use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::client::legacy::Client;
use hyper_util::rt::TokioExecutor;

use crate::config::Algorithm;

/// The hyper client type shared by both the request proxy path and the
/// health-check loop. Cloning is cheap (it's an `Arc` internally).
pub type ProxyClient = Client<HttpConnector, axum::body::Body>;

pub fn make_client() -> ProxyClient {
    Client::builder(TokioExecutor::new()).build_http()
}

/// A single backend target. Liveness and in-flight connection count are
/// tracked with atomics rather than behind the outer `RwLock` — health
/// checks and the proxy hot path both touch these on every request/tick,
/// and neither needs to block the other or block a config reload.
#[derive(Debug)]
pub struct Backend {
    pub id: usize,
    pub addr: String,
    pub alive: AtomicBool,
    pub active_connections: AtomicUsize,
}

impl Backend {
    pub fn new(id: usize, addr: String) -> Self {
        Backend {
            id,
            addr,
            // Optimistic: assume alive until the first health check says
            // otherwise, so a freshly hot-reloaded backend isn't dead on
            // arrival for the whole first health-check interval.
            alive: AtomicBool::new(true),
            active_connections: AtomicUsize::new(0),
        }
    }

    pub fn is_alive(&self) -> bool {
        self.alive.load(Ordering::Relaxed)
    }

    pub fn connections(&self) -> usize {
        self.active_connections.load(Ordering::Relaxed)
    }
}

/// RAII guard that increments a backend's in-flight connection count on
/// creation and decrements it on drop — including on early return, panic
/// unwind, or the client disconnecting mid-request. This is what makes
/// least-connections routing trustworthy: the count can never leak from
/// a code path that forgets to decrement it, because there's nothing to
/// forget.
pub struct ConnectionGuard {
    backend: Arc<Backend>,
}

impl ConnectionGuard {
    pub fn acquire(backend: Arc<Backend>) -> Self {
        backend.active_connections.fetch_add(1, Ordering::Relaxed);
        ConnectionGuard { backend }
    }
}

impl Drop for ConnectionGuard {
    fn drop(&mut self) {
        self.backend
            .active_connections
            .fetch_sub(1, Ordering::Relaxed);
    }
}

/// Everything the running proxy needs, shared across the axum handler
/// tasks, the health-check background loop, and the config-file watcher.
///
/// Fields that change shape on hot reload (backend list, algorithm,
/// health-check tuning) live behind `RwLock`. Fields that are pure
/// per-request counters (`Backend::alive`, `Backend::active_connections`)
/// are atomics *inside* the individually-Arc'd `Backend`, specifically so
/// that a request in flight never needs to take the outer `backends`
/// write lock — only `reload_config` does that, briefly.
pub struct AppState {
    pub backends: RwLock<Vec<Arc<Backend>>>,
    pub algorithm: RwLock<Algorithm>,
    pub health_check_path: RwLock<String>,
    pub health_check_timeout: RwLock<Duration>,
    pub rr_counter: AtomicUsize,
    pub next_backend_id: AtomicUsize,
    pub client: ProxyClient,
}

impl AppState {
    pub fn from_config(cfg: &crate::config::Config) -> Self {
        let backends: Vec<Arc<Backend>> = cfg
            .backends
            .iter()
            .enumerate()
            .map(|(id, b)| Arc::new(Backend::new(id, b.addr.clone())))
            .collect();
        let next_id = backends.len();

        AppState {
            backends: RwLock::new(backends),
            algorithm: RwLock::new(cfg.algorithm),
            health_check_path: RwLock::new(cfg.health_check_path.clone()),
            health_check_timeout: RwLock::new(Duration::from_secs(cfg.health_check_timeout_secs)),
            rr_counter: AtomicUsize::new(0),
            next_backend_id: AtomicUsize::new(next_id),
            client: make_client(),
        }
    }

    /// Snapshot of currently-alive backends, cloning only the `Arc`
    /// pointers (cheap) while holding the read lock for as short a time
    /// as possible.
    pub fn alive_backends(&self) -> Vec<Arc<Backend>> {
        self.backends
            .read()
            .expect("backends lock poisoned")
            .iter()
            .filter(|b| b.is_alive())
            .cloned()
            .collect()
    }

    pub fn all_backends(&self) -> Vec<Arc<Backend>> {
        self.backends
            .read()
            .expect("backends lock poisoned")
            .clone()
    }
}
