use std::fs;
use std::net::{SocketAddr, ToSocketAddrs};
use std::path::Path;

use anyhow::Context;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ServerConfig {
    pub host: String,
    pub port: u16,
    /// Optional SQL statement executed during startup for ducklake integration.
    pub ducklake_init_sql: Option<String>,
    /// Maximum number of concurrent sessions.
    pub max_sessions: Option<usize>,
    /// Session idle timeout in seconds.
    pub session_timeout_seconds: Option<u64>,
    /// Log format: "compact" or "json".
    pub log_format: String,
    /// Persistent directory root for Duckling Queue files.
    pub duckling_queue_root: String,
    /// Time-based rotation threshold in seconds.
    pub duckling_queue_rotate_interval_seconds: u64,
    /// Size-based rotation threshold in bytes.
    pub duckling_queue_rotate_size_bytes: u64,
    /// Interval in seconds for scanning sealed files to flush.
    pub duckling_queue_flush_interval_seconds: u64,
    /// Maximum number of parallel flush tasks.
    pub duckling_queue_max_parallel_flushes: usize,
    /// Target schema name for flushing Duckling Queue data.
    pub duckling_queue_target_schema: String,
    /// Automatically create tables if they don't exist when flushing Duckling Queue data.
    pub duckling_queue_auto_create_tables: bool,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            host: "0.0.0.0".to_string(),
            port: 4214,
            ducklake_init_sql: None,
            max_sessions: Some(100),
            session_timeout_seconds: Some(900),
            log_format: "compact".to_string(),
            duckling_queue_root: "target/ducklake-tests/duckling_queue".to_string(),
            duckling_queue_rotate_interval_seconds: 300,
            duckling_queue_rotate_size_bytes: 100_000_000,
            duckling_queue_flush_interval_seconds: 60,
            duckling_queue_max_parallel_flushes: 2,

            duckling_queue_target_schema: "swanlake".to_string(),
            duckling_queue_auto_create_tables: false,
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
        let root = &self.duckling_queue_root;
        let path = Path::new(root);
        if !path.exists() {
            fs::create_dir_all(path).with_context(|| {
                format!(
                    "failed to create duckling queue root directory '{}'",
                    path.display()
                )
            })?;
        } else if !path.is_dir() {
            anyhow::bail!(
                "duckling queue root path '{}' is not a directory",
                path.display()
            );
        }
        Ok(())
    }
}
