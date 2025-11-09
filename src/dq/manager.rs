use std::ffi::OsStr;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use tracing::warn;

use crate::config::ServerConfig;
use crate::dq::config::{QueueContext, QueueDirectories, Settings};
use crate::dq::lock::{read_lock_session_id, FileLock};
use crate::dq::session::QueueSession;
use crate::session::SessionId;

/// Global queue manager responsible for directory lifecycle and
/// exposing per-session queue handles.
#[derive(Clone)]
pub struct QueueManager {
    ctx: Arc<QueueContext>,
}

impl QueueManager {
    /// Build settings, initialize directories and sweep leftover active files.
    pub fn new(config: &ServerConfig) -> Result<Self> {
        let settings = Settings::from_config(config);
        let dirs = QueueDirectories::new(settings.root.clone())?;
        let ctx = Arc::new(QueueContext::new(settings, dirs));
        let manager = Self { ctx };

        // Best-effort orphan sweep during startup so we don't leave straggler files.
        let _ = manager.sweep_orphaned_files(&[]);

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
    pub fn open_session_queue(&self, session_id: SessionId) -> Result<QueueSession> {
        QueueSession::create(session_id, self.ctx.clone())
    }

    /// Sweep orphaned files from `active/` into `sealed/`.
    pub fn sweep_orphaned_files(&self, active_session_ids: &[SessionId]) -> Result<Vec<PathBuf>> {
        sweep_orphaned_active_files(
            self.ctx.dirs(),
            active_session_ids,
            self.ctx.settings().lock_ttl,
        )
    }

    /// Enumerate sealed queue files ready to flush.
    pub fn sealed_files(&self) -> Result<Vec<PathBuf>> {
        list_db_files_in_dir(&self.ctx.dirs().sealed)
    }
}

fn sweep_orphaned_active_files(
    dirs: &QueueDirectories,
    active_session_ids: &[SessionId],
    ttl: Duration,
) -> Result<Vec<PathBuf>> {
    let mut sealed_paths = Vec::new();
    let db_files = list_db_files_in_dir(&dirs.active)?;

    for path in db_files {
        let lock_owner = match read_lock_session_id(&path) {
            Ok(owner) => owner,
            Err(err) => {
                warn!(error = %err, file = %path.display(), "failed to read duckling queue lock metadata");
                None
            }
        };
        let is_orphaned = if let Some(session_id) = lock_owner {
            !active_session_ids
                .iter()
                .any(|id| id.as_ref() == session_id)
        } else {
            true
        };

        if !is_orphaned {
            continue;
        }

        if let Some(_lock) = FileLock::try_acquire(&path, ttl, None)? {
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
