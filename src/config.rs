use std::env;
use std::fs;
use std::net::{SocketAddr, ToSocketAddrs};
use std::path::{Path, PathBuf};

use anyhow::Context;
use serde::Deserialize;

const DEFAULT_HOST: &str = "127.0.0.1";
const DEFAULT_PORT: u16 = 4214;

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct ServerConfig {
    pub host: String,
    pub port: u16,
    pub duckdb_path: Option<PathBuf>,
    /// Maximum size of the DuckDB connection pool.
    pub pool_size: u32,
    /// Whether to install/load the DuckLake extension during startup.
    pub ducklake_enable: bool,
    /// Optional SQL statement executed during startup for ducklake integration.
    pub ducklake_init_sql: Option<String>,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            host: DEFAULT_HOST.to_string(),
            port: DEFAULT_PORT,
            duckdb_path: None,
            pool_size: 4,
            ducklake_enable: true,
            ducklake_init_sql: None,
        }
    }
}

impl ServerConfig {
    pub fn load(config_path: Option<&Path>) -> anyhow::Result<Self> {
        let mut cfg = if let Some(path) = config_path {
            let contents = fs::read_to_string(path).with_context(|| {
                format!("failed to read configuration file at {}", path.display())
            })?;
            toml::from_str(&contents).with_context(|| {
                format!(
                    "failed to deserialize configuration from {}",
                    path.display()
                )
            })?
        } else {
            ServerConfig::default()
        };
        cfg.apply_env_overrides()?;
        Ok(cfg)
    }

    fn apply_env_overrides(&mut self) -> anyhow::Result<()> {
        if let Ok(host) = env::var("SWANDB_HOST") {
            if !host.trim().is_empty() {
                self.host = host;
            }
        }

        if let Ok(port) = env::var("SWANDB_PORT") {
            if !port.trim().is_empty() {
                self.port = port.parse::<u16>().with_context(|| {
                    format!("invalid u16 value for SWANDB_PORT: {}", port.trim())
                })?;
            }
        }

        if let Ok(path) = env::var("SWANDB_DUCKDB_PATH") {
            if !path.trim().is_empty() {
                self.duckdb_path = Some(PathBuf::from(path));
            }
        }

        if let Ok(enable) = env::var("SWANDB_ENABLE_DUCKLAKE") {
            if !enable.trim().is_empty() {
                self.ducklake_enable = enable.parse::<bool>().with_context(|| {
                    format!("invalid boolean for SWANDB_ENABLE_DUCKLAKE: {enable}")
                })?;
            }
        }

        if let Ok(size) = env::var("SWANDB_POOL_SIZE") {
            if !size.trim().is_empty() {
                self.pool_size = size
                    .parse::<u32>()
                    .with_context(|| format!("invalid u32 value for SWANDB_POOL_SIZE: {size}"))?;
            }
        }

        if let Ok(sql) = env::var("SWANDB_DUCKLAKE_INIT_SQL") {
            let trimmed = sql.trim();
            if !trimmed.is_empty() {
                self.ducklake_init_sql = Some(trimmed.to_string());
            }
        }

        Ok(())
    }

    pub fn bind_addr(&self) -> anyhow::Result<SocketAddr> {
        let addr = format!("{}:{}", self.host, self.port);
        addr.to_socket_addrs()?
            .next()
            .ok_or_else(|| anyhow::anyhow!("unable to resolve bind address for {addr}"))
    }
}
