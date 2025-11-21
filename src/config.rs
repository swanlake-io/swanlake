use std::net::{SocketAddr, ToSocketAddrs};

use anyhow::Context;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ServerConfig {
    pub host: String,
    pub port: u16,
    /// Enable DuckDB's embedded UI server (listens on port 4213).
    pub duckdb_ui_server_enabled: bool,
    /// Enable Duckling Queue ingestion.
    pub duckling_queue_enabled: bool,
    /// Optional SQL statement executed during startup for ducklake integration.
    pub ducklake_init_sql: Option<String>,
    /// Maximum number of concurrent sessions.
    pub max_sessions: Option<usize>,
    /// Session idle timeout in seconds.
    pub session_timeout_seconds: Option<u64>,
    /// Log format: "compact" or "json".
    pub log_format: String,
    /// Time-based rotation threshold in seconds.
    pub duckling_queue_rotate_interval_seconds: u64,
    /// Size-based rotation threshold in bytes.
    pub duckling_queue_rotate_size_bytes: u64,
    /// Interval in seconds for scanning sealed files to flush.
    pub duckling_queue_flush_interval_seconds: u64,
    /// Maximum number of parallel flush tasks.
    pub duckling_queue_max_parallel_flushes: usize,
    /// Maximum number of buffered rows per target table before forcing a flush.
    pub duckling_queue_buffer_max_rows: usize,
    /// Target catalog name for flushing Duckling Queue data.
    pub duckling_queue_target_catalog: String,
    /// Directory where the Duckling Queue persists buffered batches.
    pub duckling_queue_root: String,
    /// Optional destination to copy failed duckling_queue chunks (e.g. r2://bucket/path).
    pub duckling_queue_dlq_target: Option<String>,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            host: "0.0.0.0".to_string(),
            port: 4214,
            duckdb_ui_server_enabled: false,
            duckling_queue_enabled: false,
            ducklake_init_sql: None,
            max_sessions: Some(100),
            session_timeout_seconds: Some(900),
            log_format: "compact".to_string(),
            duckling_queue_rotate_interval_seconds: 300,
            duckling_queue_rotate_size_bytes: 100_000_000,
            duckling_queue_flush_interval_seconds: 60,
            duckling_queue_max_parallel_flushes: 2,
            duckling_queue_buffer_max_rows: 50_000,

            duckling_queue_target_catalog: "swanlake".to_string(),
            duckling_queue_root: "target/duckling_queue".to_string(),
            duckling_queue_dlq_target: None,
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
