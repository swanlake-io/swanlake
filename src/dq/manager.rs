use std::ffi::OsStr;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use uuid::Uuid;

use crate::config::ServerConfig;
use crate::dq::lock::FileLock;
use duckdb::Connection;

/// Derived Duckling Queue settings used by the manager.
#[derive(Debug, Clone)]
pub struct DucklingQueueSettings {
    pub root: PathBuf,
    pub rotate_interval: Duration,
    pub rotate_size_bytes: u64,
    pub flush_interval: Duration,
    pub max_parallel_flushes: usize,
    pub lock_ttl: Duration,
    pub target_schema: String,
}

impl DucklingQueueSettings {
    pub fn from_config(config: &ServerConfig) -> Option<Self> {
        if !config.duckling_queue_enable {
            return None;
        }
        let root = config.duckling_queue_root.as_ref()?;
        let rotate_interval = Duration::from_secs(config.duckling_queue_rotate_interval_seconds);
        let lock_ttl = if rotate_interval > Duration::ZERO {
            3 * rotate_interval
        } else {
            Duration::from_secs(900) // 15 minutes as fallback
        };
        Some(Self {
            root: PathBuf::from(root),
            rotate_interval,
            rotate_size_bytes: config.duckling_queue_rotate_size_bytes,
            flush_interval: Duration::from_secs(config.duckling_queue_flush_interval_seconds),
            max_parallel_flushes: config.duckling_queue_max_parallel_flushes,
            lock_ttl,
            target_schema: config.duckling_queue_target_schema.clone(),
        })
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

#[derive(Debug, Clone)]
pub struct QueueFile {
    pub path: PathBuf,
    pub created_at: SystemTime,
}

#[derive(Debug, Clone)]
pub struct SealedQueueFile {
    pub path: PathBuf,
}

struct QueueState {
    active: QueueFile,
    active_lock: Option<FileLock>,
}

/// Manages Duckling Queue files and renders the ATTACH snippet used by DuckDB connections.
pub struct DucklingQueueManager {
    settings: DucklingQueueSettings,
    dirs: QueueDirectories,
    server_id: String,
    state: Mutex<QueueState>,
}

impl DucklingQueueManager {
    /// Build a manager when the feature is enabled. Returns `Ok(None)` if disabled.
    pub fn try_new(config: &ServerConfig) -> Result<Option<Self>> {
        let Some(settings) = DucklingQueueSettings::from_config(config) else {
            return Ok(None);
        };
        let dirs = QueueDirectories::new(settings.root.clone())?;

        // Before creating a new active file, move any leftover ones into sealed state.
        let _ = seal_orphaned_active_files(&dirs, None, settings.lock_ttl);

        let server_id = Uuid::new_v4().to_string();
        let active = create_new_active_file(&dirs.active, &server_id)?;
        let lock = FileLock::try_acquire(&active.path, settings.lock_ttl)?
            .ok_or_else(|| anyhow::anyhow!("failed to acquire lock for active file"))?;

        Ok(Some(Self {
            settings,
            dirs,
            server_id,
            state: Mutex::new(QueueState {
                active,
                active_lock: Some(lock),
            }),
        }))
    }

    /// Returns a reference to the derived settings.
    pub fn settings(&self) -> &DucklingQueueSettings {
        &self.settings
    }

    /// Returns the currently active file metadata.
    pub fn active_file(&self) -> QueueFile {
        let state = self.state.lock().expect("queue state poisoned");
        state.active.clone()
    }

    /// Returns a reference to the queue directories.
    pub fn dirs(&self) -> &QueueDirectories {
        &self.dirs
    }

    /// Renders the ATTACH SQL snippet for the currently active file.
    pub fn attach_sql(&self) -> String {
        let active = self.active_file();
        self.render_attach_sql(&active.path)
    }

    pub fn maybe_rotate_with<F>(&self, switch: F) -> Result<Option<SealedQueueFile>>
    where
        F: FnOnce(&QueueFile) -> Result<()>,
    {
        let mut state = self.state.lock().expect("queue state poisoned");

        // Refresh the active file lock to prevent expiration.
        if let Some(ref lock) = state.active_lock {
            lock.refresh()?;
        }

        let mut should_rotate = false;
        if self.settings.rotate_interval > Duration::ZERO {
            if let Ok(elapsed) = state.active.created_at.elapsed() {
                if elapsed >= self.settings.rotate_interval {
                    should_rotate = true;
                }
            }
        }
        if !should_rotate && self.settings.rotate_size_bytes > 0 {
            if let Ok(meta) = fs::metadata(&state.active.path) {
                if meta.len() >= self.settings.rotate_size_bytes {
                    should_rotate = true;
                }
            }
        }

        if should_rotate {
            Ok(Some(self.rotate_locked_with(&mut state, switch)?))
        } else {
            Ok(None)
        }
    }

    pub fn force_rotate_with<F>(&self, switch: F) -> Result<SealedQueueFile>
    where
        F: FnOnce(&QueueFile) -> Result<()>,
    {
        let mut state = self.state.lock().expect("queue state poisoned");
        self.rotate_locked_with(&mut state, switch)
    }

    /// Sweep the active directory for orphaned files and move them into `sealed/`.
    pub fn sweep_active_dir(&self) -> Result<Vec<SealedQueueFile>> {
        let active_path = {
            let state = self.state.lock().expect("queue state poisoned");
            state.active.path.clone()
        };
        seal_orphaned_active_files(&self.dirs, Some(&active_path), self.settings.lock_ttl)
    }

    /// List all sealed queue files awaiting flush.
    pub fn sealed_files(&self) -> Result<Vec<PathBuf>> {
        list_db_files_in_dir(&self.dirs.sealed)
    }

    fn render_attach_sql(&self, path: &Path) -> String {
        let path_str = path.display().to_string();
        format!("ATTACH '{}' AS duckling_queue;", path_str)
    }
}

fn create_new_active_file(active_dir: &Path, server_id: &str) -> Result<QueueFile> {
    let filename = format!(
        "duckling_queue_{}_{}.db",
        server_id,
        current_timestamp_millis()
    );
    let path = active_dir.join(filename);

    // Ensure the parent directory exists in case it was removed.
    fs::create_dir_all(active_dir)
        .with_context(|| format!("failed to recreate {:?}", active_dir))?;

    // Initialize the file via DuckDB so it's a valid database before attachments occur.
    let path_str = path
        .to_str()
        .ok_or_else(|| anyhow::anyhow!("invalid duckling_queue path {:?}", path))?
        .to_string();
    let _ = Connection::open(&path_str)
        .with_context(|| format!("failed to initialize duckling queue db {:?}", &path))?;

    Ok(QueueFile {
        path,
        created_at: SystemTime::now(),
    })
}

fn seal_orphaned_active_files(
    dirs: &QueueDirectories,
    current_active: Option<&Path>,
    ttl: Duration,
) -> Result<Vec<SealedQueueFile>> {
    let mut moved = Vec::new();
    let db_files = list_db_files_in_dir(&dirs.active)?;
    for path in db_files {
        if current_active.is_some_and(|active| active == path) {
            continue;
        }
        // Try to acquire lock; if successful, the file is orphaned and can be moved.
        if let Some(_lock) = FileLock::try_acquire(&path, ttl)? {
            let basename = active_basename(&path).unwrap_or_else(|| {
                format!("duckling_queue_orphan_{}.db", current_timestamp_millis())
            });
            let sealed_path = dirs.sealed.join(basename);
            fs::rename(&path, &sealed_path).with_context(|| {
                format!(
                    "failed to move orphaned active queue file {:?} -> {:?}",
                    path, sealed_path
                )
            })?;
            moved.push(SealedQueueFile { path: sealed_path });
        }
        // If lock acquisition fails, skip (file is in use by another instance).
    }
    Ok(moved)
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

fn active_basename(path: &Path) -> Option<String> {
    path.file_name()
        .map(|name| name.to_string_lossy().into_owned())
}

fn current_timestamp_millis() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0)
}

impl DucklingQueueManager {
    fn rotate_locked_with<F>(&self, state: &mut QueueState, switch: F) -> Result<SealedQueueFile>
    where
        F: FnOnce(&QueueFile) -> Result<()>,
    {
        checkpoint_database(&state.active.path)?;
        let sealed_path =
            self.dirs
                .sealed
                .join(active_basename(&state.active.path).unwrap_or_else(|| {
                    format!(
                        "duckling_queue_{}_{}.db",
                        self.server_id,
                        current_timestamp_millis()
                    )
                }));

        let old_active = state.active.clone();
        let old_lock = state.active_lock.take();

        let new_active = create_new_active_file(&self.dirs.active, &self.server_id)?;
        let new_lock = FileLock::try_acquire(&new_active.path, self.settings.lock_ttl)?
            .ok_or_else(|| anyhow::anyhow!("failed to acquire lock for new active file"))?;

        state.active = new_active.clone();
        state.active_lock = Some(new_lock);

        switch(&new_active)?;

        fs::rename(&old_active.path, &sealed_path).with_context(|| {
            format!(
                "failed to move active queue file {:?} to sealed location {:?}",
                &old_active.path, &sealed_path
            )
        })?;

        drop(old_lock);

        Ok(SealedQueueFile { path: sealed_path })
    }
}

fn checkpoint_database(path: &Path) -> Result<()> {
    let path_str = path
        .to_str()
        .ok_or_else(|| anyhow::anyhow!("invalid duckling queue path {:?}", path))?
        .to_string();
    let conn = Connection::open(&path_str)?;
    conn.execute_batch("FORCE CHECKPOINT;")
        .with_context(|| format!("failed to checkpoint duckling queue at {:?}", path))?;
    Ok(())
}
