use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use tracing::{debug, info};
use uuid::Uuid;

use crate::dq::config::{QueueContext, QueueDirectories};
use crate::engine::DuckDbConnection;
use crate::lock::{DistributedLock, PostgresLock};
use crate::session::SessionId;

/// Per-session queue file manager.
///
/// Each session owns exactly one active queue file at a time.
/// This struct encapsulates all queue file lifecycle operations:
/// - Creation (on session creation)
/// - Rotation (based on size/time thresholds)
/// - Sealing (before session cleanup)
pub struct QueueSession {
    session_id: SessionId,
    active_file: PathBuf,
    created_at: Instant,
    context: Arc<QueueContext>,
    _lock: PostgresLock,
}

impl QueueSession {
    /// Create a new session queue with a fresh active file.
    pub(crate) async fn create(session_id: SessionId, context: Arc<QueueContext>) -> Result<Self> {
        let active_file = create_session_queue_file(context.dirs())?;
        let lock = PostgresLock::try_acquire(
            &active_file,
            context.settings().lock_ttl,
            Some(session_id.as_ref()),
        )
        .await?
        .ok_or_else(|| anyhow::anyhow!("failed to acquire lock for session queue file"))?;

        info!(
            session_id = %session_id,
            file = %active_file.display(),
            "created session queue file"
        );

        Ok(Self {
            session_id,
            active_file,
            created_at: Instant::now(),
            context,
            _lock: lock,
        })
    }

    /// Generate ATTACH SQL for this session's queue file.
    pub fn attach_sql(&self) -> String {
        format!("ATTACH '{}' AS duckling_queue;", self.active_file.display())
    }

    /// Check if rotation is needed based on size or time thresholds.
    pub fn should_rotate(&self) -> Result<bool> {
        // Note: PostgreSQL advisory locks don't need refresh (they're session-based)
        // so we skip the refresh step that was needed for file locks

        // Time-based rotation
        let settings = self.context.settings();
        if settings.rotate_interval > Duration::ZERO {
            let age = self.created_at.elapsed();
            if age >= settings.rotate_interval {
                debug!(
                    session_id = %self.session_id,
                    age_secs = age.as_secs(),
                    threshold_secs = settings.rotate_interval.as_secs(),
                    "rotation triggered by age"
                );
                return Ok(true);
            }
        }

        // Size-based rotation
        if settings.rotate_size_bytes > 0 {
            let size = self.current_file_size()?;
            if size >= settings.rotate_size_bytes {
                debug!(
                    session_id = %self.session_id,
                    size_bytes = size,
                    threshold_bytes = settings.rotate_size_bytes,
                    "rotation triggered by size"
                );
                return Ok(true);
            }
        }

        Ok(false)
    }

    /// Rotate the queue file: detach, seal current file, create and attach new file.
    /// Returns the path to the sealed file.
    pub async fn rotate(&mut self, conn: &DuckDbConnection) -> Result<PathBuf> {
        debug!(
            session_id = %self.session_id,
            old_file = %self.active_file.display(),
            "starting rotation"
        );

        // Detach the current queue
        self.detach_queue(conn)?;

        // Seal the current file (move to sealed/)
        let sealed_path = self.seal_current_file()?;

        // Create new active file
        let new_file = create_session_queue_file(self.context.dirs())?;
        let new_lock = PostgresLock::try_acquire(
            &new_file,
            self.context.settings().lock_ttl,
            Some(self.session_id.as_ref()),
        )
        .await?
        .ok_or_else(|| anyhow::anyhow!("failed to acquire lock for new session queue file"))?;

        // Update state
        self.active_file = new_file;
        self.created_at = Instant::now();
        self._lock = new_lock;

        // Attach the new queue
        self.attach_queue(conn)?;

        info!(
            session_id = %self.session_id,
            sealed_file = %sealed_path.display(),
            new_file = %self.active_file.display(),
            "rotation completed"
        );

        Ok(sealed_path)
    }

    /// Force flush: rotate and return sealed file for immediate flushing.
    pub async fn force_flush(&mut self, conn: &DuckDbConnection) -> Result<PathBuf> {
        debug!(session_id = %self.session_id, "force flush requested");
        self.rotate(conn).await
    }

    /// Seal the current file before session cleanup.
    /// Consumes self, ensuring no further operations on this queue.
    pub fn seal_on_cleanup(self) -> Result<PathBuf> {
        debug!(
            session_id = %self.session_id,
            file = %self.active_file.display(),
            "sealing queue file on session cleanup"
        );

        let sealed_path = self.context.dirs().sealed.join(
            self.active_file
                .file_name()
                .ok_or_else(|| anyhow::anyhow!("queue file has no filename"))?,
        );

        std::fs::rename(&self.active_file, &sealed_path).with_context(|| {
            format!(
                "failed to seal session queue file {:?} -> {:?}",
                self.active_file, sealed_path
            )
        })?;

        info!(
            session_id = %self.session_id,
            sealed_file = %sealed_path.display(),
            "session queue file sealed"
        );

        // Lock is automatically released when self is dropped
        Ok(sealed_path)
    }

    /// Get current file size in bytes.
    fn current_file_size(&self) -> Result<u64> {
        let metadata = std::fs::metadata(&self.active_file).with_context(|| {
            format!(
                "failed to get metadata for queue file {:?}",
                self.active_file
            )
        })?;
        Ok(metadata.len())
    }

    /// Detach the duckling_queue database from the connection.
    fn detach_queue(&self, conn: &DuckDbConnection) -> Result<()> {
        // Ignore errors - detaching non-existent DB is fine
        let _ = conn.execute_batch("DETACH duckling_queue;");
        Ok(())
    }

    /// Attach the current queue file to the connection.
    fn attach_queue(&self, conn: &DuckDbConnection) -> Result<()> {
        let sql = self.attach_sql();
        conn.execute_batch(&sql).with_context(|| {
            format!("failed to attach session queue file {:?}", self.active_file)
        })?;
        Ok(())
    }

    /// Seal the current active file by moving it to sealed/.
    fn seal_current_file(&self) -> Result<PathBuf> {
        let sealed_path = self.context.dirs().sealed.join(
            self.active_file
                .file_name()
                .ok_or_else(|| anyhow::anyhow!("queue file has no filename"))?,
        );

        std::fs::rename(&self.active_file, &sealed_path).with_context(|| {
            format!(
                "failed to move queue file {:?} to sealed location {:?}",
                self.active_file, sealed_path
            )
        })?;

        Ok(sealed_path)
    }
}

/// Create a new session queue file in the active/ directory.
/// File ID is a UUID, safe for use in filenames.
fn create_session_queue_file(dirs: &QueueDirectories) -> Result<PathBuf> {
    let timestamp_nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);

    let filename = format!("duckling_queue_{}_{}.db", Uuid::new_v4(), timestamp_nanos);
    let path = dirs.active.join(filename);

    // Ensure active directory exists
    std::fs::create_dir_all(&dirs.active)
        .with_context(|| format!("failed to create active directory {:?}", dirs.active))?;

    // Initialize the file via DuckDB so it's a valid database
    let path_str = path
        .to_str()
        .ok_or_else(|| anyhow::anyhow!("invalid session queue path {:?}", path))?;

    let conn = duckdb::Connection::open(path_str)
        .with_context(|| format!("failed to initialize session queue db {:?}", path))?;

    // Create a dummy table to ensure the file is non-empty and valid
    conn.execute_batch("CREATE TABLE IF NOT EXISTS __session_queue_metadata (created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP);")
        .with_context(|| format!("failed to initialize session queue metadata in {:?}", path))?;

    drop(conn);

    Ok(path)
}
