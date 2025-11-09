use std::ffi::OsStr;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context, Result};

use crate::config::ServerConfig;
use crate::dq::lock::FileLock;

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
            3 * rotate_interval
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

/// Manages Duckling Queue directories and settings.
/// Per-session queue files are managed by SessionQueue in manager.rs.
pub struct DucklingQueueSettings {
    settings: Settings,
    dirs: QueueDirectories,
}

impl DucklingQueueSettings {
    /// Build settings and initialize directories.
    pub fn new(config: &ServerConfig) -> Result<Self> {
        let settings = Settings::from_config(config);
        let dirs = QueueDirectories::new(settings.root.clone())?;

        // On startup, sweep any leftover active files and seal them
        // (they are orphaned from previous server runs)
        let _ = sweep_orphaned_active_files(&dirs, &[], settings.lock_ttl);

        Ok(Self { settings, dirs })
    }

    /// Returns a reference to the settings.
    pub fn settings(&self) -> &Settings {
        &self.settings
    }

    /// Returns a reference to the queue directories.
    pub fn dirs(&self) -> &QueueDirectories {
        &self.dirs
    }

    /// Sweep the active directory for orphaned files (not owned by any current session)
    /// and move them into `sealed/`.
    pub fn sweep_orphaned_files(&self, active_session_ids: &[SessionId]) -> Result<Vec<PathBuf>> {
        sweep_orphaned_active_files(&self.dirs, active_session_ids, self.settings.lock_ttl)
    }

    /// List all sealed queue files awaiting flush.
    pub fn sealed_files(&self) -> Result<Vec<PathBuf>> {
        list_db_files_in_dir(&self.dirs.sealed)
    }
}

use crate::session::SessionId;

/// Sweep active directory for orphaned files and seal them.
/// A file is orphaned if its session_id (parsed from filename) is not in active_session_ids.
fn sweep_orphaned_active_files(
    dirs: &QueueDirectories,
    active_session_ids: &[SessionId],
    ttl: Duration,
) -> Result<Vec<PathBuf>> {
    let mut sealed_paths = Vec::new();
    let db_files = list_db_files_in_dir(&dirs.active)?;

    for path in db_files {
        // Try to parse session_id from filename
        let is_orphaned = if let Some(session_id) = parse_session_id_from_path(&path) {
            !active_session_ids
                .iter()
                .any(|id| id.as_ref() == session_id)
        } else {
            // Can't parse session_id, treat as orphaned
            true
        };

        if !is_orphaned {
            continue;
        }

        // Try to acquire lock; if successful, the file is orphaned and can be moved.
        if let Some(_lock) = FileLock::try_acquire(&path, ttl)? {
            let sealed_path = dirs.sealed.join(
                path.file_name()
                    .ok_or_else(|| anyhow::anyhow!("orphaned file has no filename"))?,
            );
            fs::rename(&path, &sealed_path).with_context(|| {
                format!(
                    "failed to move orphaned active queue file {:?} -> {:?}",
                    path, sealed_path
                )
            })?;
            sealed_paths.push(sealed_path);
        }
        // If lock acquisition fails, skip (file is in use by another process).
    }
    Ok(sealed_paths)
}

fn list_db_files_in_dir(dir: &Path) -> Result<Vec<PathBuf>> {
    let mut files = Vec::new();
    for entry in fs::read_dir(dir).with_context(|| format!("failed to read directory {:?}", dir))? {
        let entry = entry?;
        let path = entry.path();
        if path.is_file() && path.extension().is_some_and(|ext| ext == OsStr::new("db")) {
            files.push(path);
        }
    }
    Ok(files)
}

/// Parse session_id from queue file path.
/// Expected format: duckling_queue_{session_id}_{timestamp}.db
fn parse_session_id_from_path(path: &Path) -> Option<String> {
    let filename = path.file_stem()?.to_str()?;

    // Remove "duckling_queue_" prefix
    let without_prefix = filename.strip_prefix("duckling_queue_")?;

    // Session ID is everything before the last underscore (timestamp)
    let last_underscore = without_prefix.rfind('_')?;
    let session_id = &without_prefix[..last_underscore];

    Some(session_id.to_string())
}
