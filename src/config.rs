use std::path::Path;

use serde::Deserialize;

/// Which routing strategy the proxy uses to pick a backend for each
/// incoming request. Deserialized directly from the `algorithm` string
/// in config.toml (e.g. `algorithm = "least_connections"`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Algorithm {
    RoundRobin,
    LeastConnections,
}

impl Default for Algorithm {
    fn default() -> Self {
        Algorithm::RoundRobin
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct BackendConfig {
    /// Base URL of the backend, e.g. "http://127.0.0.1:9001". No trailing slash.
    pub addr: String,
}

fn default_health_check_interval() -> u64 {
    5
}

fn default_health_check_timeout() -> u64 {
    2
}

fn default_health_check_path() -> String {
    "/health".to_string()
}

fn default_listen_addr() -> String {
    "127.0.0.1:8080".to_string()
}

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    #[serde(default = "default_listen_addr")]
    pub listen_addr: String,

    #[serde(default)]
    pub algorithm: Algorithm,

    #[serde(default = "default_health_check_interval")]
    pub health_check_interval_secs: u64,

    #[serde(default = "default_health_check_timeout")]
    pub health_check_timeout_secs: u64,

    #[serde(default = "default_health_check_path")]
    pub health_check_path: String,

    #[serde(default)]
    pub backends: Vec<BackendConfig>,
}

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("failed to read config file {path}: {source}")]
    Read {
        path: String,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to parse config file {path}: {source}")]
    Parse {
        path: String,
        #[source]
        source: toml::de::Error,
    },
}

impl Config {
    pub fn load(path: &Path) -> Result<Config, ConfigError> {
        let raw = std::fs::read_to_string(path).map_err(|source| ConfigError::Read {
            path: path.display().to_string(),
            source,
        })?;
        let cfg: Config = toml::from_str(&raw).map_err(|source| ConfigError::Parse {
            path: path.display().to_string(),
            source,
        })?;
        Ok(cfg)
    }
}
