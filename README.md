# reverse-proxy

A reverse proxy that behaves like an actual piece of infrastructure, not
a routing toy: two load-balancing algorithms, active background health
checking with automatic failover, and **zero-downtime config hot-reload**
— edit `config.toml`, save, and the running process picks up new
backends and a new algorithm with no restart and no dropped connections
on backends that didn't change.

Built in Rust on `axum` + `hyper` + `tokio`. No unsafe code.

## Why this exists

Forwarding a request from A to B is a 20-minute weekend project with
almost any framework. What separates a load balancer that *works in a
demo* from one you'd actually put in front of traffic is what it does
when things go wrong or when the world changes under it:

- A backend silently stops responding — does traffic keep getting routed
  to it until every client notices, or does something notice first and
  route around it?
- You need to add a backend under load — do you restart the process
  (dropping every in-flight connection) or does it just... show up?
- Two requests land on the least-loaded backend at the exact same
  instant — does your connection counter race and drift, quietly lying
  about load until a rolling restart resets it?

This project is built around answering those questions concretely,
in code, with tests that prove it — not just describing the behavior in
a README.

## Features

- **Round-robin** and **least-connections** load balancing, switchable
  live via config (no restart)
- **Active health checking** — a background `tokio` task polls every
  backend's `/health` endpoint on a configurable interval and pulls it
  out of rotation the moment it fails, independently per backend (one
  slow/hung backend can't delay detection for the others)
- **Hot-reloading config** — a `notify`-based file watcher diffs
  `config.toml` on every save and applies backend additions/removals and
  algorithm changes directly to the live routing table
- **Race-free connection counting** — in-flight connection counts are
  tracked with an RAII guard (`ConnectionGuard`), so a count can never
  leak on an error path, panic, or early return, because there's no code
  path that has to remember to decrement it
- **Live introspection** — `GET /_proxy/status` shows real-time backend
  health and load, no separate metrics stack needed to watch it work

## The concurrency design, briefly

The one piece of this worth reading the source for is `AppState`
(`src/state.rs`). Two different kinds of mutable state are handled
deliberately differently:

- **Structural state** — the backend list, the active algorithm, health
  check tuning — lives behind `RwLock`. It changes rarely (only on a
  config reload) and every request needs a consistent read of it, which
  is exactly the access pattern `RwLock` is for.
- **Per-backend counters** — `alive`, `active_connections` — are
  `Ordering::Relaxed` atomics *inside* each individually-`Arc`'d
  `Backend`, specifically so that neither the hot request path nor the
  health-check loop ever has to take the outer `backends` write lock.
  Only `reload_config` does that, and only briefly.

This is why a config reload doesn't stall in-flight requests, and why
in-flight requests don't contend with each other over a single global
lock on every single call.

## Quickstart

```bash
cargo build

# terminal 1-3: three mock backends
./target/debug/mock-backend --port 9001 --name alpha
./target/debug/mock-backend --port 9002 --name bravo
./target/debug/mock-backend --port 9003 --name charlie

# terminal 4: the proxy
RUST_LOG=info ./target/debug/reverse-proxy --config config.toml

# terminal 5: hit it
curl http://127.0.0.1:8080/
curl http://127.0.0.1:8080/_proxy/status
```

### See health-check failover live

```bash
curl -X POST http://127.0.0.1:9002/admin/toggle-health   # bravo goes unhealthy
sleep 4                                                     # wait one health-check interval
curl http://127.0.0.1:8080/_proxy/status                   # bravo now shows alive:false
for i in 1 2 3 4; do curl -s http://127.0.0.1:8080/; echo; done  # never lands on bravo
```

### See hot-reload live — no restart

With the proxy still running, edit `config.toml`: change `algorithm` to
`"least_connections"`, remove one backend, add another. Save it.

```bash
curl http://127.0.0.1:8080/_proxy/status
```

The response reflects the new algorithm and backend set within
~150ms (the file-watcher debounce window) of the save — the proxy
process never restarted.

## Testing

```bash
cargo test --lib             # algorithm unit tests (round-robin ordering,
                              # least-connections selection, dead-backend
                              # exclusion) — pure logic, no network
cargo test --test integration  # real end-to-end: spins up real mock
                                # backend servers and a real proxy
                                # instance, hits it over real HTTP, and
                                # asserts on actual routing behavior —
                                # including a genuine concurrent-load
                                # test for least-connections
```

Both suites are green in CI on every push (`.github/workflows/ci.yml`).
The integration suite in particular is doing real work worth reading —
`tests/integration.rs` spins up an in-process HTTP server per mock
backend, a real proxy router, and fires actual requests through
`hyper_util`'s client, rather than calling internal functions directly.
It caught a real bug during development: an early version of
`pick_round_robin` divided by the backend count before checking whether
the list was empty, which would panic on an all-backends-down request.
The test that caught it is still in the suite
(`round_robin_returns_none_on_empty_backend_list`).

## Configuration reference

```toml
listen_addr = "127.0.0.1:8080"
algorithm = "round_robin"          # or "least_connections"
health_check_interval_secs = 3
health_check_timeout_secs = 1
health_check_path = "/health"

[[backends]]
addr = "http://127.0.0.1:9001"

[[backends]]
addr = "http://127.0.0.1:9002"
```

All amounts/timings are plain integers; no special syntax to learn.

## Known simplifications (called out on purpose, not hidden)

Being upfront about scope is part of making this credible:

- **Hot-reload removal is not graceful.** Removing a backend from
  `config.toml` drops it from rotation immediately, even if it has
  in-flight requests. A production version would mark it `draining` and
  only remove it once `active_connections` hits zero. Flagged explicitly
  in `watcher.rs` where the simplification is made.
- **No TLS termination.** Proxies HTTP only. Adding TLS is a config +
  `axum-server`/`rustls` change, not an architecture change.
- **No retries/circuit breaker beyond health-check exclusion.** A failed
  in-flight request to a backend that fails mid-request is not retried
  against a different backend — it's surfaced as a 502. Automatic retry
  is a natural Phase 2.
- **Least-connections has no tie-breaking beyond list order.** When
  multiple backends have identical load, the first one in the list wins
  deterministically. Fine for a proxy; worth knowing if you're
  benchmarking against a truly random tie-break.

## Project layout

```
src/
  main.rs        entry point — loads config, spawns background tasks, serves
  lib.rs         router assembly, re-exports modules for tests
  config.rs       config.toml parsing (serde + toml)
  state.rs        Backend, ConnectionGuard, AppState — the concurrency core
  balancer.rs      round-robin / least-connections selection (+ unit tests)
  health.rs        background health-check loop
  watcher.rs       notify-based hot-reload
  proxy.rs         the actual request-forwarding handler + /_proxy/status
  bin/mock_backend.rs   standalone test/demo backend server
tests/
  integration.rs   real end-to-end HTTP tests
```
