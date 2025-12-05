use std::net::{SocketAddr, ToSocketAddrs};

use anyhow::Context;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ServerConfig {
    pub host: String,
    pub port: u16,
    /// Optional SQL statement executed during startup for ducklake integration.
    pub ducklake_init_sql: Option<String>,
    /// Optional comma-separated list of DuckLake databases to checkpoint periodically.
    pub checkpoint_databases: Option<String>,
    /// Interval in hours between checkpoints for each configured database.
    pub checkpoint_interval_hours: Option<u64>,
    /// Maximum number of concurrent sessions.
    pub max_sessions: Option<usize>,
    /// Session idle timeout in seconds.
    pub session_timeout_seconds: Option<u64>,
    /// Log format: "compact" or "json".
    pub log_format: String,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            host: "0.0.0.0".to_string(),
            port: 4214,
            ducklake_init_sql: None,
            checkpoint_databases: None,
            checkpoint_interval_hours: Some(24),
            max_sessions: Some(100),
            session_timeout_seconds: Some(900),
            log_format: "compact".to_string(),
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
        cfg.validate()?;
        Ok(cfg)
    }

    pub fn bind_addr(&self) -> anyhow::Result<SocketAddr> {
        let addr = format!("{}:{}", self.host, self.port);
        addr.to_socket_addrs()?
            .next()
            .ok_or_else(|| anyhow::anyhow!("unable to resolve bind address for {addr}"))
    }

    fn validate(&self) -> anyhow::Result<()> {
        Ok(())
    }
}
