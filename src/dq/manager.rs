use std::ffi::OsStr;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use anyhow::{Context, Result};
use tracing::{info, warn};

use crate::config::ServerConfig;
use crate::dq::config::{QueueContext, QueueDirectories, Settings};
use crate::dq::session::QueueSession;
use crate::lock::{DistributedLock, PostgresLock};
use crate::session::SessionId;

/// Global queue manager responsible for directory lifecycle and
/// exposing per-session queue handles.
#[derive(Clone)]
pub struct QueueManager {
    ctx: Arc<QueueContext>,
}

impl QueueManager {
    /// Build settings, initialize directories and sweep leftover active files.
    pub async fn new(config: &ServerConfig) -> Result<Self> {
        let settings = Settings::from_config(config);
        let dirs = QueueDirectories::new(settings.root.clone())?;
        let ctx = Arc::new(QueueContext::new(settings, dirs));
        let manager = Self { ctx };

        // Best-effort orphan sweep during startup so we don't leave straggler files.
        if let Err(err) = manager.sweep_orphaned_files(&[]).await {
            warn!(error = %err, "failed to sweep orphaned duckling queue files on startup");
        }

        Ok(manager)
    }

    /// Low-level access to queue settings.
    pub fn settings(&self) -> &Settings {
        self.ctx.settings()
    }

    /// Low-level access to queue directories.
    pub fn dirs(&self) -> &QueueDirectories {
        self.ctx.dirs()
    }

    /// Create a session-scoped queue handle.
    pub async fn open_session_queue(&self, session_id: SessionId) -> Result<QueueSession> {
        QueueSession::create(session_id, self.ctx.clone()).await
    }

    /// Sweep orphaned files from `active/` into `sealed/`.
    pub async fn sweep_orphaned_files(
        &self,
        active_session_ids: &[SessionId],
    ) -> Result<Vec<PathBuf>> {
        sweep_orphaned_active_files(
            self.ctx.dirs(),
            active_session_ids,
            self.ctx.settings().lock_ttl,
        )
        .await
    }

    /// Enumerate sealed queue files ready to flush.
    pub fn sealed_files(&self) -> Result<Vec<PathBuf>> {
        list_db_files_in_dir(&self.ctx.dirs().sealed)
    }

    /// Cleanup old flushed files older than retention period.
    pub fn cleanup_flushed_files(&self) -> Result<()> {
        let flushed_dir = &self.dirs().flushed;
        let retention_duration = Duration::from_secs(3 * 24 * 3600); // 3 days
        let now = SystemTime::now();

        for entry in fs::read_dir(flushed_dir)
            .with_context(|| format!("failed to read flushed queue directory {:?}", flushed_dir))?
        {
            let entry = entry?;
            let path = entry.path();
            if !path.is_file() {
                continue;
            }

            let metadata = match entry.metadata() {
                Ok(meta) => meta,
                Err(err) => {
                    warn!(error = %err, file = %path.display(), "failed to get metadata for flushed file");
                    continue;
                }
            };

            let modified = match metadata.modified() {
                Ok(time) => time,
                Err(err) => {
                    warn!(error = %err, file = %path.display(), "failed to get modified time for flushed file");
                    continue;
                }
            };

            if let Ok(age) = now.duration_since(modified) {
                if age > retention_duration {
                    if let Err(err) = fs::remove_file(&path) {
                        warn!(error = %err, file = %path.display(), "failed to remove old flushed file");
                    } else {
                        info!(file = %path.display(), "removed old flushed file");
                    }
                }
            }
        }

        Ok(())
    }
}

async fn sweep_orphaned_active_files(
    dirs: &QueueDirectories,
    _active_session_ids: &[SessionId],
    ttl: Duration,
) -> Result<Vec<PathBuf>> {
    let mut sealed_paths = Vec::new();
    let db_files = list_db_files_in_dir(&dirs.active)?;

    for path in db_files {
        // Since we're using distributed locks, we don't track session IDs in lock files
        // Instead, just try to acquire the lock - if successful, it means the file is orphaned
        if let Some(_lock) = PostgresLock::try_acquire(&path, ttl, None).await? {
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
