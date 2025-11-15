use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use tokio::sync::{mpsc, Semaphore};
use tracing::{debug, error, info, warn};

use crate::dq::QueueManager;
use crate::engine::EngineFactory;
use crate::lock::{DistributedLock, PostgresLock};
use crate::session::registry::SessionRegistry;

const MIN_ROTATION_TICK: Duration = Duration::from_secs(5);
const MIN_FLUSH_TICK: Duration = Duration::from_secs(5);
const FLUSHED_TABLE_PREFIX: &str = "__dq_flushed_";

/// Background runtime that drives Duckling Queue rotation and flushing.
pub struct QueueRuntime {
    #[allow(dead_code)]
    manager: Arc<QueueManager>,
    #[allow(dead_code)]
    factory: Arc<Mutex<EngineFactory>>,
    #[allow(dead_code)]
    registry: Arc<SessionRegistry>,
}

impl QueueRuntime {
    pub fn new(
        manager: Arc<QueueManager>,
        factory: Arc<Mutex<EngineFactory>>,
        registry: Arc<SessionRegistry>,
    ) -> Self {
        let registry_clone = registry.clone();
        let (tx, rx) = mpsc::channel::<PathBuf>(64);

        tokio::spawn(rotation_loop(manager.clone(), registry_clone, tx.clone()));
        tokio::spawn(sealed_scan_loop(manager.clone(), tx.clone()));
        tokio::spawn(flush_loop(manager.clone(), factory.clone(), rx));
        tokio::spawn(cleanup_loop(manager.clone()));

        Self {
            manager,
            factory,
            registry,
        }
    }
}

async fn rotation_loop(
    manager: Arc<QueueManager>,
    registry: Arc<SessionRegistry>,
    tx: mpsc::Sender<PathBuf>,
) {
    let mut interval = tokio::time::interval(
        manager
            .settings()
            .rotate_interval
            .min(Duration::from_secs(300))
            .max(MIN_ROTATION_TICK),
    );

    info!(
        "rotation_loop interval {} seconds",
        interval.period().as_secs()
    );
    loop {
        interval.tick().await;

        // 1. Check all sessions for size/time-based rotation
        registry.maybe_rotate_all_queues().await;

        // 2. Sweep orphaned active files
        let active_ids = registry.get_all_session_ids();
        if let Ok(orphaned) = manager.sweep_orphaned_files(&active_ids).await {
            if !orphaned.is_empty() {
                info!(
                    "found {} orphaned duckling queue files to flush",
                    orphaned.len()
                );
                for sealed in orphaned {
                    if let Err(err) = tx.send(sealed).await {
                        warn!("failed to queue orphaned file for flushing: {}", err);
                    }
                }
            }
        }
    }
}

async fn sealed_scan_loop(manager: Arc<QueueManager>, tx: mpsc::Sender<PathBuf>) {
    let mut interval = tokio::time::interval(manager.settings().flush_interval.max(MIN_FLUSH_TICK));
    info!(
        "sealed scan loop interval {} seconds",
        interval.period().as_secs()
    );
    loop {
        interval.tick().await;
        match manager.sealed_files() {
            Ok(files) => {
                if !files.is_empty() {
                    info!("found {} sealed files to flush", files.len());
                    for file in files {
                        let _ = tx.send(file).await;
                    }
                }
            }
            Err(err) => {
                warn!(error = %err, "failed to enumerate sealed duckling queue files");
            }
        }
    }
}

async fn flush_loop(
    manager: Arc<QueueManager>,
    factory: Arc<Mutex<EngineFactory>>,
    mut rx: mpsc::Receiver<PathBuf>,
) {
    let semaphore = Arc::new(Semaphore::new(
        manager.settings().max_parallel_flushes.max(1),
    ));

    while let Some(path) = rx.recv().await {
        let permit = semaphore.clone().acquire_owned().await.unwrap();
        let manager_clone = manager.clone();
        let factory_clone = factory.clone();
        let path_for_log = path.clone();
        tokio::spawn(async move {
            // Acquire lock first (async operation)
            let lock_result =
                PostgresLock::try_acquire(&path, manager_clone.settings().lock_ttl, None).await;

            let flush_result = match lock_result {
                Ok(Some(_lock)) => {
                    // Lock acquired, now do the flush with sync DuckDB operations
                    let conn = factory_clone.lock().unwrap().create_connection();
                    match conn {
                        Ok(conn) => conn.with_inner(|inner| {
                            flush_sealed_file_sync(&manager_clone, inner, &path)
                        }),
                        Err(e) => Err(e.into()),
                    }
                }
                Ok(None) => {
                    // Lock not acquired - file busy
                    Ok(false)
                }
                Err(e) => Err(e),
            };

            match flush_result {
                Ok(true) => {}
                Ok(false) => {
                    debug!(
                        file = %path_for_log.display(),
                        "duckling queue file busy, will retry later"
                    );
                }
                Err(err) => {
                    warn!(error = %err, file = %path_for_log.display(), "duckling queue flush failed");
                }
            }
            drop(permit);
        });
    }
}

/// Flush a sealed queue file into the target DuckLake schema (synchronous version).
/// Returns true if flushed, false if file is busy (locked by another worker).
/// Note: Lock must be acquired before calling this function.
pub fn flush_sealed_file_sync(
    manager: &QueueManager,
    conn: &duckdb::Connection,
    path: &Path,
) -> Result<bool> {
    if !path.exists() {
        return Ok(true);
    }

    // Safety check: only process .db files
    if path.extension() != Some(std::ffi::OsStr::new("db")) {
        warn!(file = %path.display(), "skipping non-db file in sealed directory");
        return Ok(true);
    }

    info!(file = %path.display(), "start flush duckling queue file");

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
    debug!(file = %path.display(), tables = ?tables, "duckling queue tables discovered");
    for table in tables {
        let quoted = quote_ident(&table);
        debug!(table = %table, "flushing duckling queue table");

        let sql = if manager.settings().auto_create_tables {
            format!(
                "CREATE TABLE IF NOT EXISTS {target_schema}.{quoted} AS FROM duckling_queue.{quoted} LIMIT 0;
                INSERT INTO {target_schema}.{quoted} SELECT * FROM duckling_queue.{quoted};"
            )
        } else {
            format!("INSERT INTO {target_schema}.{quoted} SELECT * FROM duckling_queue.{quoted};")
        };
        conn.execute_batch(&sql)
            .map_err(|err| {
                error!(error = %err, table = %table, path = %path.display(), "failed to insert data");
                err
            })
            .with_context(|| format!("failed to flush table {} from duckling queue", table))?;

        // Mark table as flushed so replays skip it
        let flushed_name = format!("{FLUSHED_TABLE_PREFIX}{table}");
        let quoted_flushed = quote_ident(&flushed_name);
        conn.execute_batch(&format!(
            "ALTER TABLE duckling_queue.{quoted} RENAME TO {quoted_flushed};"
        ))
        .with_context(|| format!("failed to rename flushed duckling queue table {}", table))?;

        debug!(table = %table, "duckling queue table flushed");
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
            path.display(),
            flushed_path.display()
        )
    })?;

    info!(
        flushed_path = %flushed_path.display(),
        "duckling queue file flushed successfully"
    );
    Ok(true)
}

fn list_queue_tables(conn: &duckdb::Connection) -> Result<Vec<String>> {
    let mut stmt = conn.prepare(
        "SELECT table_name FROM information_schema.tables WHERE table_catalog = 'duckling_queue'",
    )?;
    let rows = stmt.query_map([], |row| row.get::<usize, String>(0))?;
    let mut tables = Vec::new();
    for row in rows {
        let name = row?;
        if !name.starts_with(FLUSHED_TABLE_PREFIX) && !name.starts_with("__session_queue_") {
            tables.push(name);
        }
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

async fn cleanup_loop(manager: Arc<QueueManager>) {
    let mut interval = tokio::time::interval(Duration::from_secs(3600)); // Run every hour
    loop {
        interval.tick().await;
        if let Err(err) = tokio::task::spawn_blocking({
            let manager = manager.clone();
            move || manager.cleanup_flushed_files()
        })
        .await
        {
            warn!(error = %err, "duckling queue cleanup failed");
        }
    }
}

fn detach_if_attached(conn: &duckdb::Connection, alias: &str) -> Result<()> {
    let sql = format!("DETACH {alias};");
    let _ = conn.execute_batch(&sql); // Ignore errors, as detaching non-existing is fine
    Ok(())
}
