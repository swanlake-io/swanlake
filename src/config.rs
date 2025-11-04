use std::net::{SocketAddr, ToSocketAddrs};
use std::path::Path;

use anyhow::Context;
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct ServerConfig {
    pub host: String,
    pub port: u16,
    /// Maximum size of the DuckDB connection pool.
    pub pool_size: u32,
    /// Optional size override for the read-only DuckDB connection pool.
    pub read_pool_size: Option<u32>,
    /// Optional size override for the write DuckDB connection pool.
    pub write_pool_size: Option<u32>,
    /// Whether write operations are permitted.
    pub enable_writes: bool,
    /// Whether to install/load the DuckLake extension during startup.
    pub ducklake_enable: bool,
    /// Optional SQL statement executed during startup for ducklake integration.
    pub ducklake_init_sql: Option<String>,
}

impl ServerConfig {
    pub fn load(config_path: &Path) -> anyhow::Result<Self> {
        let settings = config::Config::builder()
            .add_source(config::File::with_name(config_path.to_str().unwrap()).required(true))
            .add_source(config::Environment::with_prefix("SWANDB_").separator("_"))
            .build()
            .with_context(|| {
                format!(
                    "failed to load configuration from {}",
                    config_path.display()
                )
            })?;
        let cfg: ServerConfig = settings.try_deserialize().with_context(|| {
            format!(
                "failed to deserialize configuration from {}",
                config_path.display()
            )
        })?;
        Ok(cfg)
    }

    pub fn bind_addr(&self) -> anyhow::Result<SocketAddr> {
        let addr = format!("{}:{}", self.host, self.port);
        addr.to_socket_addrs()?
            .next()
            .ok_or_else(|| anyhow::anyhow!("unable to resolve bind address for {addr}"))
    }
}
