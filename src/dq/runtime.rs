use std::path::PathBuf;
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

    pub fn manager(&self) -> Arc<DucklingQueueManager> {
        self.manager.clone()
    }

    /// Force a rotation and flush of all pending queue files. Used by tests/admin commands.
    pub async fn force_flush_now(&self) -> Result<()> {
        let mut pending: Vec<PathBuf> = self.manager.sealed_files()?;
        pending.push(self.manager.force_rotate()?.path);
        info!("pending flush file list {:?}", pending);
        for path in pending {
            self.flush_file_now(path).await?;
        }
        Ok(())
    }

    async fn flush_file_now(&self, path: PathBuf) -> Result<()> {
        let manager = self.manager.clone();
        let factory = self.factory.clone();
        tokio::task::spawn_blocking(move || flush_file(manager, factory, path))
            .await
            .map_err(|err| anyhow!(err))??;
        Ok(())
    }
}

async fn rotation_loop(manager: Arc<DucklingQueueManager>, tx: mpsc::Sender<PathBuf>) {
    let mut interval = tokio::time::interval(
        manager
            .settings()
            .rotate_interval
            .min(Duration::from_secs(60))
            .max(MIN_ROTATION_TICK),
    );

    loop {
        interval.tick().await;

        if let Ok(orphaned) = manager.sweep_active_dir() {
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
    loop {
        interval.tick().await;
        match manager.sealed_files() {
            Ok(files) => {
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
            if let Err(err) =
                tokio::task::spawn_blocking(move || flush_file(manager_clone, factory_clone, path))
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

fn flush_file(
    manager: Arc<DucklingQueueManager>,
    factory: Arc<EngineFactory>,
    path: PathBuf,
) -> Result<()> {
    info!(file = %path.display(), "start flush duckling queue file");
    if !path.exists() {
        return Ok(());
    }

    let Some(lock) = FileLock::try_acquire(&path, manager.settings().lock_ttl)? else {
        error!(file = %path.display(), "duckling queue file locked elsewhere");
        return Err(anyhow!(
            "duckling queue file {:?} locked elsewhere",
            path.display()
        ));
    };

    let conn = factory.create_raw_connection()?;
    detach_if_attached(&conn, "duckling_queue")
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
    let tables = list_queue_tables(&conn)?;
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
        debug!(table = %table, source_count = %source_count, "source table row count");

        let create_sql = format!(
            "CREATE TABLE IF NOT EXISTS swanlake.{quoted} AS FROM duckling_queue.{quoted} LIMIT 0;"
        );
        conn.execute_batch(&create_sql)
            .map_err(|err| {
                error!(error = %err, table = %table, path = %path.display(), "failed to create target table");
                err
            })
            .with_context(|| format!("failed to create table swanlake.{}", table))?;

        let insert_sql =
            format!("INSERT INTO swanlake.{quoted} SELECT * FROM duckling_queue.{quoted};");
        conn.execute_batch(&insert_sql)
            .map_err(|err| {
                error!(error = %err, table = %table, path = %path.display(), "failed to insert data");
                err
            })
            .with_context(|| format!("failed to flush table {} from duckling queue", table))?;

        // Check target row count after insert
        let target_count: i64 = conn
            .query_row(
                &format!("SELECT COUNT(*) FROM swanlake.{quoted}"),
                [],
                |row| row.get(0),
            )
            .unwrap_or(0);
        debug!(table = %table, target_count = %target_count, "target table row count after insert");

        // conn.execute_batch(&format!("DROP TABLE duckling_queue.{quoted};"))?;
    }

    detach_if_attached(&conn, "duckling_queue")
        .context("failed to detach sealed duckling queue")?;
    drop(lock);

    // Move the flushed file to the flushed directory
    let flushed_path = manager.dirs().flushed.join(
        path.file_name()
            .ok_or_else(|| anyhow!("flushed queue file has no filename"))?,
    );
    std::fs::rename(&path, &flushed_path).with_context(|| {
        format!(
            "failed to move flushed queue file {:?} to {:?}",
            path, flushed_path
        )
    })?;

    info!(file = %path.display(), flushed = %flushed_path.display(), "duckling queue file flushed and moved");
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
    info!("duckling_queue tables {:?}", tables);
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
    match conn.execute_batch(&sql) {
        Ok(_) => Ok(()),
        Err(err) => {
            let message = err.to_string();
            if message.contains("does not exist") {
                Ok(())
            } else {
                Err(anyhow!("failed to detach {alias}: {err}"))
            }
        }
    }
}
