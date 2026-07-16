use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use clap::Parser;

use reverse_proxy::{build_router, config::Config, health, state::AppState, watcher};

#[derive(Parser, Debug)]
#[command(name = "reverse-proxy", about = "Load-balancing reverse proxy with hot reload")]
struct Args {
    /// Path to config.toml
    #[arg(short, long, default_value = "config.toml")]
    config: PathBuf,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "reverse_proxy=info,tower_http=info".into()),
        )
        .init();

    let args = Args::parse();

    let cfg = Config::load(&args.config)
        .map_err(|e| anyhow::anyhow!("failed to load {}: {e}", args.config.display()))?;

    if cfg.backends.is_empty() {
        tracing::warn!("config has zero backends — every request will 503 until one is added");
    }

    let listen_addr: SocketAddr = cfg
        .listen_addr
        .parse()
        .map_err(|e| anyhow::anyhow!("invalid listen_addr '{}': {e}", cfg.listen_addr))?;
    let health_interval = Duration::from_secs(cfg.health_check_interval_secs);

    let state = Arc::new(AppState::from_config(&cfg));

    tracing::info!(
        listen_addr = %listen_addr,
        algorithm = ?cfg.algorithm,
        backend_count = cfg.backends.len(),
        health_check_interval_secs = cfg.health_check_interval_secs,
        "starting reverse proxy"
    );

    // Background health-check loop.
    tokio::spawn(health::run(Arc::clone(&state), health_interval));

    // Config hot-reload watcher. The returned handle must stay alive for
    // the watch to keep firing — dropping it silently stops the watch.
    let _watcher = watcher::spawn(Arc::clone(&state), args.config.clone())
        .map_err(|e| anyhow::anyhow!("failed to start config watcher: {e}"))?;

    let router = build_router(state);
    let listener = tokio::net::TcpListener::bind(listen_addr).await?;
    axum::serve(listener, router).await?;

    Ok(())
}
