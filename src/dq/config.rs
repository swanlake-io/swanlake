use std::time::Duration;

use crate::config::ServerConfig;

/// Settings controlling the Duckling Queue buffering runtime.
#[derive(Debug, Clone)]
pub struct Settings {
    /// Maximum number of buffered rows per table before forcing a flush.
    pub buffer_max_rows: usize,
    /// Maximum number of buffered bytes per table before forcing a flush.
    pub buffer_max_bytes: u64,
    /// Maximum amount of time data may sit in memory without being flushed.
    pub buffer_max_age: Duration,
    /// Interval for running the background age-based flush sweep.
    pub flush_interval: Duration,
    /// Maximum number of concurrent flush tasks.
    pub max_parallel_flushes: usize,
    /// Target catalog in DuckLake that receives flushed data.
    pub target_catalog: String,
}

impl Settings {
    pub fn from_config(config: &ServerConfig) -> Self {
        Self {
            buffer_max_rows: config.duckling_queue_buffer_max_rows,
            buffer_max_bytes: config.duckling_queue_rotate_size_bytes,
            buffer_max_age: Duration::from_secs(config.duckling_queue_rotate_interval_seconds),
            flush_interval: Duration::from_secs(config.duckling_queue_flush_interval_seconds),
            max_parallel_flushes: config.duckling_queue_max_parallel_flushes.max(1),
            target_catalog: config.duckling_queue_target_catalog.clone(),
        }
    }
}
