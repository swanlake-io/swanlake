use std::net::{SocketAddr, ToSocketAddrs};

use anyhow::Context;
use serde::{Deserialize, Serialize};
use serde_json;

#[derive(Debug, Clone, Deserialize, Serialize)]
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
    /// Whether to start the UI server automatically.
    pub enable_ui_server: bool,
    /// Maximum number of concurrent sessions.
    pub max_sessions: Option<usize>,
    /// Session idle timeout in seconds.
    pub session_timeout_seconds: Option<u64>,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            host: "127.0.0.1".to_string(),
            port: 4214,
            pool_size: 10,
            read_pool_size: Some(10),
            write_pool_size: Some(3),
            enable_writes: true,
            ducklake_enable: true,
            ducklake_init_sql: None,
            enable_ui_server: true,
            max_sessions: Some(100),
            session_timeout_seconds: Some(900),
        }
    }
}

impl ServerConfig {
    pub fn load() -> anyhow::Result<Self> {
        let defaults_json = serde_json::to_string(&Self::default())
            .with_context(|| "failed to serialize defaults")?;
        let settings = config::Config::builder()
            .add_source(
                config::File::from_str(&defaults_json, config::FileFormat::Json).required(false),
            )
            .add_source(config::Environment::with_prefix("SWANDB"))
            .build()
            .with_context(|| "failed to load configuration")?;
        let cfg: ServerConfig = settings
            .try_deserialize()
            .with_context(|| "failed to deserialize configuration")?;
        Ok(cfg)
    }

    pub fn bind_addr(&self) -> anyhow::Result<SocketAddr> {
        let addr = format!("{}:{}", self.host, self.port);
        addr.to_socket_addrs()?
            .next()
            .ok_or_else(|| anyhow::anyhow!("unable to resolve bind address for {addr}"))
    }
}
