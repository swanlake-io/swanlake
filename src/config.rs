use std::net::{SocketAddr, ToSocketAddrs};
use std::path::Path;

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
    /// Enable the Duckling Queue staging layer.
    pub duckling_queue_enable: bool,
    /// Persistent directory root for Duckling Queue files.
    pub duckling_queue_root: Option<String>,
    /// Time-based rotation threshold in seconds.
    pub duckling_queue_rotate_interval_seconds: u64,
    /// Size-based rotation threshold in bytes.
    pub duckling_queue_rotate_size_bytes: u64,
    /// Interval in seconds for scanning sealed files to flush.
    pub duckling_queue_flush_interval_seconds: u64,
    /// Maximum number of parallel flush tasks.
    pub duckling_queue_max_parallel_flushes: usize,

    /// Optional override template for the ATTACH SQL snippet.
    pub duckling_queue_attach_template: Option<String>,
    /// Target schema name for flushing Duckling Queue data.
    pub duckling_queue_target_schema: String,
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
            duckling_queue_enable: true,
            duckling_queue_root: None,
            duckling_queue_rotate_interval_seconds: 300,
            duckling_queue_rotate_size_bytes: 100_000_000,
            duckling_queue_flush_interval_seconds: 60,
            duckling_queue_max_parallel_flushes: 2,

            duckling_queue_attach_template: None,
            duckling_queue_target_schema: "swanlake".to_string(),
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
        if self.duckling_queue_enable {
            let root = self.duckling_queue_root.as_deref().ok_or_else(|| {
                anyhow::anyhow!("DUCKLING_QUEUE_ROOT must be set when duckling queue is enabled")
            })?;
            let path = Path::new(root);
            if !path.exists() {
                anyhow::bail!(
                    "duckling queue root path '{}' does not exist",
                    path.display()
                );
            }
            if !path.is_dir() {
                anyhow::bail!(
                    "duckling queue root path '{}' is not a directory",
                    path.display()
                );
            }
        }
        Ok(())
    }
}
