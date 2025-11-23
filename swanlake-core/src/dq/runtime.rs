use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::mpsc::{self, UnboundedReceiver};
use tokio::sync::Semaphore;
use tracing::{debug, info, warn};

use crate::dq::config::Settings;
use crate::dq::coordinator::{DqCoordinator, FlushPayload, FlushTracker};
use crate::dq::storage::DurableStorage;
use crate::engine::{batch::align_batch_to_table_schema, EngineFactory};
use crate::error::ServerError;

/// Background runtime that receives flush payloads and writes them into DuckLake.
pub struct QueueRuntime;

impl QueueRuntime {
    /// Bootstrap the runtime and return the coordinator alongside it.
    pub fn bootstrap(
        factory: Arc<Mutex<EngineFactory>>,
        settings: Settings,
    ) -> Result<(Arc<DqCoordinator>, Arc<Self>), ServerError> {
        let (tx, rx) = mpsc::unbounded_channel::<FlushPayload>();
        let storage = Arc::new(DurableStorage::new(settings.root_dir.clone())?);
        let tracker = Arc::new(FlushTracker::new());
        let coordinator = Arc::new(DqCoordinator::new(
            settings.clone(),
            storage.clone(),
            tracker.clone(),
            tx,
        )?);
        let runtime = Arc::new(Self);
        spawn_flush_workers(
            factory,
            settings.clone(),
            storage.clone(),
            tracker.clone(),
            coordinator.clone(),
            rx,
        );
        spawn_age_sweeper(coordinator.clone(), settings.flush_interval);
        Ok((coordinator, runtime))
    }
}

fn spawn_age_sweeper(coordinator: Arc<DqCoordinator>, interval: Duration) {
    let mut ticker = tokio::time::interval(interval.max(Duration::from_secs(1)));
    tokio::spawn(async move {
        loop {
            ticker.tick().await;
            coordinator.flush_stale_buffers();
        }
    });
}

fn spawn_flush_workers(
    factory: Arc<Mutex<EngineFactory>>,
    settings: Settings,
    storage: Arc<DurableStorage>,
    tracker: Arc<FlushTracker>,
    coordinator: Arc<DqCoordinator>,
    mut rx: UnboundedReceiver<FlushPayload>,
) {
    let semaphore = Arc::new(Semaphore::new(settings.max_parallel_flushes));
    tokio::spawn(async move {
        let handle_dlq_offload = |factory: Arc<Mutex<EngineFactory>>,
                                  storage: Arc<DurableStorage>,
                                  settings: Settings,
                                  payload: FlushPayload| async move {
            match tokio::task::spawn_blocking(move || {
                attempt_dlq_offload(factory, storage, &settings, &payload)
            })
            .await
            {
                Ok(result) => result,
                Err(join_err) => {
                    warn!(error = %join_err, "duckling queue DLQ offload panicked");
                    false
                }
            }
        };
        while let Some(payload) = rx.recv().await {
            let permit = semaphore.clone().acquire_owned().await.unwrap();
            let factory_for_flush = factory.clone();
            let factory_for_dlq = factory.clone();
            let coordinator_cloned = coordinator.clone();
            let settings_for_flush = settings.clone();
            let settings_for_dlq = settings.clone();
            let storage_for_flush = storage.clone();
            let storage_for_dlq = storage.clone();
            let tracker_clone = tracker.clone();
            let retry_payload = payload.clone();
            tokio::spawn(async move {
                let res = tokio::task::spawn_blocking(move || {
                    flush_payload(
                        factory_for_flush,
                        &settings_for_flush,
                        storage_for_flush,
                        payload,
                    )
                })
                .await;

                let handled = match res {
                    Ok(Ok(())) => {
                        debug!("duckling queue flush task completed");
                        true
                    }
                    Ok(Err(err)) => {
                        warn!(error = %err, "duckling queue flush failed");
                        handle_dlq_offload(
                            factory_for_dlq.clone(),
                            storage_for_dlq.clone(),
                            settings_for_dlq.clone(),
                            retry_payload.clone(),
                        )
                        .await
                    }
                    Err(join_err) => {
                        warn!(error = %join_err, "duckling queue flush panicked");
                        handle_dlq_offload(
                            factory_for_dlq.clone(),
                            storage_for_dlq.clone(),
                            settings_for_dlq.clone(),
                            retry_payload.clone(),
                        )
                        .await
                    }
                };

                if !handled {
                    coordinator_cloned.requeue(retry_payload);
                }
                tracker_clone.complete();
                drop(permit);
            });
        }
    });
}

fn flush_payload(
    factory: Arc<Mutex<EngineFactory>>,
    settings: &Settings,
    storage: Arc<DurableStorage>,
    payload: FlushPayload,
) -> Result<(), ServerError> {
    if payload.is_empty() {
        return Ok(());
    }

    let rows = payload.total_rows();
    let table = payload.table.clone();
    let (batches, chunk_handles) = payload.into_batches_and_chunks();

    info!(
        table = %table,
        rows = rows,
        "flushing duckling queue payload"
    );

    let conn = factory.lock().unwrap().create_connection()?;
    let qualified_table = format!("{}.{}", settings.target_catalog, table);
    let table_schema = Arc::new(conn.table_schema(&qualified_table)?);
    let aligned_batches = batches
        .iter()
        .map(|batch| align_batch_to_table_schema(batch, &table_schema, None))
        .collect::<Result<Vec<_>, ServerError>>()?;
    conn.insert_with_appender(&settings.target_catalog, &table, aligned_batches)?;
    storage.remove_chunks(chunk_handles)?;
    Ok(())
}

fn attempt_dlq_offload(
    factory: Arc<Mutex<EngineFactory>>,
    storage: Arc<DurableStorage>,
    settings: &Settings,
    payload: &FlushPayload,
) -> bool {
    let Some(target_root) = settings.dlq_target.as_ref() else {
        return false;
    };

    match offload_payload_to_dlq(factory, storage, target_root, payload) {
        Ok(()) => {
            info!(
                table = %payload.table,
                target = %target_root,
                "duckling queue payload copied to DLQ target"
            );
            true
        }
        Err(err) => {
            warn!(
                error = %err,
                table = %payload.table,
                target = %target_root,
                "failed to offload duckling queue payload to DLQ target"
            );
            false
        }
    }
}

fn offload_payload_to_dlq(
    factory: Arc<Mutex<EngineFactory>>,
    storage: Arc<DurableStorage>,
    target_root: &str,
    payload: &FlushPayload,
) -> Result<(), ServerError> {
    if payload.is_empty() {
        return Ok(());
    }

    let conn = factory.lock().unwrap().create_connection()?;
    let handles = payload.chunk_handles();
    if handles.is_empty() {
        return Ok(());
    }

    let target_root = target_root.trim_end_matches('/');
    let app_conn = conn
        .conn
        .lock()
        .map_err(|_| ServerError::Internal("connection mutex poisoned".to_string()))?;

    for handle in &handles {
        let src_path = handle.path();
        let file_name = src_path
            .file_name()
            .and_then(|f| f.to_str())
            .ok_or_else(|| {
                ServerError::Internal(format!(
                    "failed to read duckling queue chunk filename from {}",
                    src_path.display()
                ))
            })?;

        let dest_path = format!("{}/{}/{}", target_root, payload.table, file_name);
        create_parent_dir_if_local(&dest_path)?;
        copy_parquet_chunk(src_path, &dest_path, &app_conn)?;
    }

    // Only remove local chunks after they have been copied successfully.
    drop(app_conn);
    storage.remove_chunks(handles)?;
    Ok(())
}

fn escape_single_quotes(input: &str) -> String {
    input.replace("'", "''")
}

fn create_parent_dir_if_local(dest: &str) -> Result<(), ServerError> {
    if dest.contains("://") {
        return Ok(());
    }
    if let Some(parent) = std::path::Path::new(dest).parent() {
        std::fs::create_dir_all(parent).map_err(|err| {
            ServerError::Internal(format!(
                "failed to create local DLQ directory {}: {err}",
                parent.display()
            ))
        })?;
    }
    Ok(())
}

fn copy_parquet_chunk(
    src_path: &Path,
    dest_path: &str,
    conn: &duckdb::Connection,
) -> Result<(), ServerError> {
    let sql = format!(
        "COPY (SELECT * FROM read_parquet('{}')) TO '{}' (FORMAT 'parquet');",
        escape_single_quotes(src_path.to_string_lossy().as_ref()),
        escape_single_quotes(dest_path)
    );
    conn.execute(&sql, []).map_err(ServerError::DuckDb)?;
    Ok(())
}
