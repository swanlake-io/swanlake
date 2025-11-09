use std::fs;
use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result};

use crate::config::ServerConfig;

/// Duckling Queue settings derived from configuration.
#[derive(Debug, Clone)]
pub struct Settings {
    pub root: PathBuf,
    pub rotate_interval: Duration,
    pub rotate_size_bytes: u64,
    pub flush_interval: Duration,
    pub max_parallel_flushes: usize,
    pub lock_ttl: Duration,
    pub target_schema: String,
}

impl Settings {
    pub fn from_config(config: &ServerConfig) -> Self {
        let root = PathBuf::from(&config.duckling_queue_root);
        let rotate_interval = Duration::from_secs(config.duckling_queue_rotate_interval_seconds);
        let lock_ttl = if rotate_interval > Duration::ZERO {
            let base = rotate_interval
                .checked_mul(3)
                .unwrap_or(Duration::from_secs(u64::MAX));
            let min_ttl = Duration::from_secs(300); // 5 minutes minimum
            let max_ttl = Duration::from_secs(1800); // 30 minutes maximum
            base.max(min_ttl).min(max_ttl)
        } else {
            Duration::from_secs(900) // 15 minutes as fallback
        };
        Self {
            root,
            rotate_interval,
            rotate_size_bytes: config.duckling_queue_rotate_size_bytes,
            flush_interval: Duration::from_secs(config.duckling_queue_flush_interval_seconds),
            max_parallel_flushes: config.duckling_queue_max_parallel_flushes,
            lock_ttl,
            target_schema: config.duckling_queue_target_schema.clone(),
        }
    }
}

/// Directory layout (active/sealed/flushed) under the Duckling Queue root.
#[derive(Debug, Clone)]
pub struct QueueDirectories {
    pub active: PathBuf,
    pub sealed: PathBuf,
    pub flushed: PathBuf,
}

impl QueueDirectories {
    pub fn new(root: PathBuf) -> Result<Self> {
        let dirs = Self {
            active: root.join("active"),
            sealed: root.join("sealed"),
            flushed: root.join("flushed"),
        };
        dirs.ensure()?;
        Ok(dirs)
    }

    fn ensure(&self) -> Result<()> {
        fs::create_dir_all(&self.active)
            .with_context(|| format!("failed to create {:?}", &self.active))?;
        fs::create_dir_all(&self.sealed)
            .with_context(|| format!("failed to create {:?}", &self.sealed))?;
        fs::create_dir_all(&self.flushed)
            .with_context(|| format!("failed to create {:?}", &self.flushed))?;
        Ok(())
    }
}

/// Shared queue context that can be cloned by per-session handles.
#[derive(Debug, Clone)]
pub struct QueueContext {
    pub(crate) settings: Settings,
    pub(crate) dirs: QueueDirectories,
}

impl QueueContext {
    pub fn new(settings: Settings, dirs: QueueDirectories) -> Self {
        Self { settings, dirs }
    }

    pub fn settings(&self) -> &Settings {
        &self.settings
    }

    pub fn dirs(&self) -> &QueueDirectories {
        &self.dirs
    }
}
