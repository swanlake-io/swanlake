use std::net::{SocketAddr, ToSocketAddrs};

use anyhow::Context;
use serde::{Deserialize, Serialize};

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
    /// Maximum number of concurrent sessions.
    pub max_sessions: Option<usize>,
    /// Session idle timeout in seconds.
    pub session_timeout_seconds: Option<u64>,
    /// Log format: "compact" or "json".
    pub log_format: String,
    /// Whether to enable ANSI colors in logs.
    pub log_ansi: bool,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            host: "0.0.0.0".to_string(),
            port: 4214,
            pool_size: 10,
            read_pool_size: Some(10),
            write_pool_size: Some(3),
            enable_writes: true,
            ducklake_enable: true,
            ducklake_init_sql: None,
            max_sessions: Some(100),
            session_timeout_seconds: Some(900),
            log_format: "compact".to_string(),
            log_ansi: true,
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
            .add_source(config::Environment::with_prefix("SWANLAKE"))
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
