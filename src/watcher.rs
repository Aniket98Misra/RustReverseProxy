use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use notify::{Event, EventKind, RecommendedWatcher, RecursiveMode, Watcher};

use crate::config::Config;
use crate::state::{AppState, Backend};

/// Starts watching `config_path` for changes and reloads `state` in
/// place whenever it's modified — no process restart, no dropped
/// connections on backends that didn't change.
///
/// Returns the `RecommendedWatcher` handle, which the caller **must
/// keep alive** for the lifetime of the program (dropping it stops the
/// underlying OS-level watch). `main.rs` holds onto it with a
/// `let _watcher = ...` binding for exactly this reason.
pub fn spawn(state: Arc<AppState>, config_path: PathBuf) -> notify::Result<RecommendedWatcher> {
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<notify::Result<Event>>();

    let mut watcher = RecommendedWatcher::new(
        move |res| {
            // notify's callback runs on its own OS-thread; we just hand
            // the event off to the async world via an unbounded channel.
            let _ = tx.send(res);
        },
        notify::Config::default(),
    )?;

    watcher.watch(&config_path, RecursiveMode::NonRecursive)?;
    tracing::info!(path = %config_path.display(), "watching config for changes");

    tokio::spawn(async move {
        while let Some(res) = rx.recv().await {
            match res {
                Ok(event) => {
                    if matches!(event.kind, EventKind::Modify(_) | EventKind::Create(_)) {
                        // Editors often emit several rapid events for a
                        // single save (write + metadata touch). A short
                        // debounce avoids reloading the file mid-write
                        // and avoids redundant reload spam.
                        tokio::time::sleep(Duration::from_millis(150)).await;
                        reload(&state, &config_path).await;
                    }
                }
                Err(e) => tracing::error!(error = %e, "config watcher error"),
            }
        }
    });

    Ok(watcher)
}

async fn reload(state: &Arc<AppState>, config_path: &Path) {
    let new_cfg = match Config::load(config_path) {
        Ok(cfg) => cfg,
        Err(e) => {
            tracing::error!(error = %e, "config reload failed, keeping previous config");
            return;
        }
    };

    *state.algorithm.write().expect("algorithm lock poisoned") = new_cfg.algorithm;
    *state
        .health_check_path
        .write()
        .expect("health_check_path lock poisoned") = new_cfg.health_check_path.clone();
    *state
        .health_check_timeout
        .write()
        .expect("health_check_timeout lock poisoned") =
        Duration::from_secs(new_cfg.health_check_timeout_secs);

    let mut backends = state.backends.write().expect("backends lock poisoned");
    let existing_addrs: HashSet<String> = backends.iter().map(|b| b.addr.clone()).collect();
    let new_addrs: HashSet<String> = new_cfg.backends.iter().map(|b| b.addr.clone()).collect();

    let removed_count = backends.iter().filter(|b| !new_addrs.contains(&b.addr)).count();
    // NOTE: this drops a backend immediately, even mid-request. A more
    // production-grade version would mark it `draining` and only remove
    // it once `active_connections` hits zero. Flagging this as a known
    // simplification rather than quietly shipping a footgun.
    backends.retain(|b| new_addrs.contains(&b.addr));

    let mut added_count = 0;
    for nb in &new_cfg.backends {
        if !existing_addrs.contains(&nb.addr) {
            let id = state.next_backend_id.fetch_add(1, Ordering::Relaxed);
            backends.push(Arc::new(Backend::new(id, nb.addr.clone())));
            added_count += 1;
        }
    }

    tracing::info!(
        total = backends.len(),
        added = added_count,
        removed = removed_count,
        algorithm = ?new_cfg.algorithm,
        "config reloaded"
    );
}
