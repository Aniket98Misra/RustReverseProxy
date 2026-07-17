use std::sync::atomic::Ordering;
use std::sync::Arc;

use crate::config::Algorithm;
use crate::state::{AppState, Backend};

/// Pick the next backend to route a request to, per the currently
/// configured algorithm. Returns `None` if there are no alive backends
/// (caller should respond 503).
pub fn pick_backend(state: &AppState) -> Option<Arc<Backend>> {
    let alive = state.alive_backends();
    if alive.is_empty() {
        return None;
    }
    let algorithm = *state.algorithm.read().expect("algorithm lock poisoned");
    match algorithm {
        Algorithm::RoundRobin => pick_round_robin(&state.rr_counter, &alive),
        Algorithm::LeastConnections => pick_least_connections(&alive),
    }
}

fn pick_round_robin(
    counter: &std::sync::atomic::AtomicUsize,
    alive: &[Arc<Backend>],
) -> Option<Arc<Backend>> {
    if alive.is_empty() {
        return None;
    }
    let idx = counter.fetch_add(1, Ordering::Relaxed) % alive.len();
    alive.get(idx).cloned()
}

fn pick_least_connections(alive: &[Arc<Backend>]) -> Option<Arc<Backend>> {
    alive
        .iter()
        .min_by_key(|b| b.connections())
        .cloned()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;

    fn backend(id: usize) -> Arc<Backend> {
        Arc::new(Backend::new(id, format!("http://127.0.0.1:900{id}")))
    }

    #[test]
    fn round_robin_cycles_through_all_backends_in_order() {
        let counter = AtomicUsize::new(0);
        let backends = vec![backend(0), backend(1), backend(2)];

        let picks: Vec<usize> = (0..6)
            .map(|_| pick_round_robin(&counter, &backends).unwrap().id)
            .collect();

        assert_eq!(picks, vec![0, 1, 2, 0, 1, 2]);
    }

    #[test]
    fn round_robin_returns_none_on_empty_backend_list() {
        let counter = AtomicUsize::new(0);
        assert!(pick_round_robin(&counter, &[]).is_none());
    }

    #[test]
    fn least_connections_picks_the_backend_with_fewest_in_flight() {
        let backends = vec![backend(0), backend(1), backend(2)];
        backends[0].active_connections.store(5, Ordering::Relaxed);
        backends[1].active_connections.store(1, Ordering::Relaxed);
        backends[2].active_connections.store(3, Ordering::Relaxed);

        let chosen = pick_least_connections(&backends).unwrap();
        assert_eq!(chosen.id, 1);
    }

    #[test]
    fn least_connections_updates_pick_as_load_shifts() {
        let backends = vec![backend(0), backend(1)];
        backends[0].active_connections.store(0, Ordering::Relaxed);
        backends[1].active_connections.store(0, Ordering::Relaxed);

        // Tie -> first one wins deterministically (min_by_key keeps first
        // minimum on ties).
        assert_eq!(pick_least_connections(&backends).unwrap().id, 0);

        // Load backend 0 up; backend 1 should now win.
        backends[0].active_connections.store(4, Ordering::Relaxed);
        assert_eq!(pick_least_connections(&backends).unwrap().id, 1);
    }

    #[test]
    fn dead_backends_are_excluded_from_selection() {
        let state_backends = vec![backend(0), backend(1), backend(2)];
        state_backends[1].alive.store(false, Ordering::Relaxed);

        let alive: Vec<Arc<Backend>> = state_backends
            .iter()
            .filter(|b| b.is_alive())
            .cloned()
            .collect();

        assert_eq!(alive.len(), 2);
        assert!(alive.iter().all(|b| b.id != 1));
    }
}
