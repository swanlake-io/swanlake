use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use tokio::sync::{mpsc, Semaphore};
use tracing::{debug, error, info, warn};

use crate::dq::lock::FileLock;
use crate::dq::DucklingQueueManager;
use crate::engine::EngineFactory;

const MIN_ROTATION_TICK: Duration = Duration::from_secs(5);
const MIN_FLUSH_TICK: Duration = Duration::from_secs(5);

/// Background runtime that drives Duckling Queue rotation and flushing.
pub struct DucklingQueueRuntime {
    manager: Arc<DucklingQueueManager>,
    #[allow(dead_code)]
    factory: Arc<EngineFactory>,
}

impl DucklingQueueRuntime {
    pub fn new(manager: Arc<DucklingQueueManager>, factory: EngineFactory) -> Self {
        let factory = Arc::new(factory);
        let (tx, rx) = mpsc::channel::<PathBuf>(64);

        tokio::spawn(rotation_loop(manager.clone(), tx.clone()));
        tokio::spawn(sealed_scan_loop(manager.clone(), tx.clone()));
        tokio::spawn(flush_loop(manager.clone(), factory.clone(), rx));
        tokio::spawn(cleanup_loop(manager.clone()));

        Self { manager, factory }
    }

    /// Execute a Duckling Queue administrative command on the given connection.
    ///
    /// Returns the number of rows affected (typically 0 for admin commands),
    /// or None if the SQL is not a DQ command.
    pub fn execute_command(&self, sql: &str, conn: &duckdb::Connection) -> Result<Option<i64>> {
        if !is_dq_command(sql) {
            return Ok(None);
        }

        // Force flush handles detach/flush/re-attach internally
        self.force_flush_on_connection(conn)?;
        Ok(Some(0))
    }

    /// Force a rotation and flush of all pending queue files on a given connection.
    pub fn force_flush_on_connection(&self, conn: &duckdb::Connection) -> Result<()> {
        let mut pending: Vec<PathBuf> = self.manager.sealed_files()?;
        pending.push(self.manager.force_rotate()?.path);
        for path in pending {
            safe_flush_file(&self.manager, conn, &path)?;
        }

        // Re-attach the active duckling_queue file after flushing
        let active_file = self.manager.active_file();
        let attach_sql = format!(
            "ATTACH IF NOT EXISTS '{}' AS duckling_queue;",
            active_file.path.display()
        );
        conn.execute_batch(&attach_sql)
            .context("failed to re-attach active duckling_queue after flush")?;

        Ok(())
    }
}

/// Detect if SQL is a Duckling Queue administrative command
fn is_dq_command(sql: &str) -> bool {
    let trimmed = sql.trim();
    if trimmed.is_empty() {
        return false;
    }

    let normalized = trimmed.trim_end_matches(';').trim().to_ascii_lowercase();
    normalized == "pragma duckling_queue.flush" || normalized == "call duckling_queue_flush()"
}

async fn rotation_loop(manager: Arc<DucklingQueueManager>, tx: mpsc::Sender<PathBuf>) {
    let mut interval = tokio::time::interval(
        manager
            .settings()
            .rotate_interval
            .min(Duration::from_secs(300))
            .max(MIN_ROTATION_TICK),
    );

    info!("rotation_loop interval {}", interval.period().as_secs());
    loop {
        interval.tick().await;

        if let Ok(orphaned) = manager.sweep_active_dir() {
            info!(
                "found orphaned duckling queue files to flush {:?}",
                orphaned
            );
            for sealed in orphaned {
                let _ = tx.send(sealed.path).await;
            }
        }

        match manager.maybe_rotate() {
            Ok(Some(sealed)) => {
                info!(file = %sealed.path.display(), "rotated duckling queue file");
                let _ = tx.send(sealed.path).await;
            }
            Ok(None) => {}
            Err(err) => {
                warn!(error = %err, "duckling queue rotation attempt failed");
            }
        }
    }
}

async fn sealed_scan_loop(manager: Arc<DucklingQueueManager>, tx: mpsc::Sender<PathBuf>) {
    let mut interval = tokio::time::interval(manager.settings().flush_interval.max(MIN_FLUSH_TICK));
    info!("sealed scan loop interval {}", interval.period().as_secs());
    loop {
        interval.tick().await;
        match manager.sealed_files() {
            Ok(files) => {
                info!("try to flush sealed files {:?}", files);
                for file in files {
                    let _ = tx.send(file).await;
                }
            }
            Err(err) => {
                warn!(error = %err, "failed to enumerate sealed duckling queue files");
            }
        }
    }
}

async fn flush_loop(
    manager: Arc<DucklingQueueManager>,
    factory: Arc<EngineFactory>,
    mut rx: mpsc::Receiver<PathBuf>,
) {
    let semaphore = Arc::new(Semaphore::new(
        manager.settings().max_parallel_flushes.max(1),
    ));

    while let Some(path) = rx.recv().await {
        let permit = semaphore.clone().acquire_owned().await.unwrap();
        let manager_clone = manager.clone();
        let factory_clone = factory.clone();
        tokio::spawn(async move {
            if let Err(err) = tokio::task::spawn_blocking(move || {
                let conn = factory_clone.create_raw_connection()?;
                safe_flush_file(&manager_clone, &conn, &path)
            })
            .await
            .map_err(|join_err| anyhow!(join_err))
            .and_then(|res| res)
            {
                warn!(error = %err, "duckling queue flush failed");
            }
            drop(permit);
        });
    }
}

fn safe_flush_file(
    manager: &DucklingQueueManager,
    conn: &duckdb::Connection,
    path: &Path,
) -> Result<()> {
    info!(file = %path.display(), "start flush duckling queue file");
    if !path.exists() {
        return Ok(());
    }

    // Safety check: only process .db files
    if path.extension() != Some(std::ffi::OsStr::new("db")) {
        warn!(file = %path.display(), "skipping non-db file in sealed directory");
        return Ok(());
    }

    let Some(lock) = FileLock::try_acquire(path, manager.settings().lock_ttl)? else {
        warn!(file = %path.display(), "duckling queue file already being flushed by another worker");
        return Ok(());
    };

    detach_if_attached(conn, "duckling_queue")
        .context("failed to detach active duckling_queue before flush")?;
    conn.execute_batch(&format!("ATTACH '{}' AS duckling_queue;", path.display()))
        .map_err(|err| {
            error!(error = %err, file = %path.display(), "failed to attach sealed duckling queue file");
            err
        })
        .with_context(|| {
            format!(
                "failed to attach sealed duckling queue file {:?}",
                path.display()
            )
        })?;
    let target_schema = &manager.settings().target_schema;
    let tables = list_queue_tables(conn)?;
    for table in tables {
        let quoted = quote_ident(&table);
        debug!(table = %table, "flushing duckling queue table");

        // Check source row count
        let source_count: i64 = conn
            .query_row(
                &format!("SELECT COUNT(*) FROM duckling_queue.{quoted}"),
                [],
                |row| row.get(0),
            )
            .unwrap_or(0);
        if source_count == 0 {
            debug!(table = %table, "source table is empty, skipping");
            continue;
        }
        debug!(table = %table, source_count = %source_count, "source table row count");

        let sql = format!(
            "CREATE TABLE IF NOT EXISTS {target_schema}.{quoted} AS FROM duckling_queue.{quoted} LIMIT 0;
            INSERT INTO {target_schema}.{quoted} SELECT * FROM duckling_queue.{quoted};"
        );
        conn.execute_batch(&sql)
            .map_err(|err| {
                error!(error = %err, table = %table, path = %path.display(), "failed to insert data");
                err
            })
            .with_context(|| format!("failed to flush table {} from duckling queue", table))?;

        // Check target row count after insert
        let target_count: i64 = conn
            .query_row(
                &format!("SELECT COUNT(*) FROM {target_schema}.{quoted}"),
                [],
                |row| row.get(0),
            )
            .unwrap_or(0);
        debug!(table = %table, target_count = %target_count, "target table row count after insert");
    }

    detach_if_attached(conn, "duckling_queue").context("failed to detach sealed duckling queue")?;

    // Move the flushed file to the flushed directory
    let flushed_path = manager.dirs().flushed.join(
        path.file_name()
            .ok_or_else(|| anyhow!("flushed queue file has no filename"))?,
    );
    std::fs::rename(path, &flushed_path).with_context(|| {
        format!(
            "failed to move flushed queue file {:?} to {:?}",
            path, flushed_path
        )
    })?;

    info!(file = %path.display(), flushed = %flushed_path.display(), "duckling queue file flushed and moved");
    drop(lock);
    Ok(())
}

fn list_queue_tables(conn: &duckdb::Connection) -> Result<Vec<String>> {
    let mut stmt = conn.prepare(
        "SELECT table_name FROM information_schema.tables WHERE table_catalog = 'duckling_queue'",
    )?;
    let rows = stmt.query_map([], |row| row.get::<usize, String>(0))?;
    let mut tables = Vec::new();
    for row in rows {
        let name = row?;
        tables.push(name);
    }
    Ok(tables)
}

fn quote_ident(ident: &str) -> String {
    let mut escaped = String::with_capacity(ident.len() + 2);
    escaped.push('"');
    for c in ident.chars() {
        if c == '"' {
            escaped.push('"');
        }
        escaped.push(c);
    }
    escaped.push('"');
    escaped
}

async fn cleanup_loop(manager: Arc<DucklingQueueManager>) {
    let mut interval = tokio::time::interval(Duration::from_secs(3600)); // Run every hour
    loop {
        interval.tick().await;
        if let Err(err) = tokio::task::spawn_blocking({
            let manager = manager.clone();
            move || cleanup_flushed_files(&manager)
        })
        .await
        .map_err(|join_err| anyhow!(join_err))
        .and_then(|res| res)
        {
            warn!(error = %err, "duckling queue cleanup failed");
        }
    }
}

fn cleanup_flushed_files(manager: &DucklingQueueManager) -> Result<()> {
    let flushed_dir = &manager.dirs().flushed;
    let retention_duration = Duration::from_secs(3 * 24 * 3600); // 3 days
    let now = std::time::SystemTime::now();

    for entry in std::fs::read_dir(flushed_dir)
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
                if let Err(err) = std::fs::remove_file(&path) {
                    warn!(error = %err, file = %path.display(), "failed to remove old flushed file");
                } else {
                    info!(file = %path.display(), "removed old flushed file");
                }
            }
        }
    }

    Ok(())
}

fn detach_if_attached(conn: &duckdb::Connection, alias: &str) -> Result<()> {
    let sql = format!("DETACH {alias};");
    let _ = conn.execute_batch(&sql); // Ignore errors, as detaching non-existing is fine
    Ok(())
}
