use std::sync::{Arc, Mutex};
use std::time::Duration;

use arrow_array::RecordBatch;
use arrow_cast::pretty::pretty_format_batches;
use arrow_schema::SchemaRef;
use chrono::Utc;
use parquet::arrow::arrow_writer::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use tokio::sync::mpsc::{self, UnboundedReceiver};
use tokio::sync::Semaphore;
use tracing::{debug, info, warn};

use crate::dq::config::Settings;
use crate::dq::coordinator::{DqCoordinator, FlushPayload, FlushTracker};
use crate::dq::duckdb_buffer::DuckDbBuffer;
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
        let factory_guard = factory.lock().unwrap();
        let buffer = Arc::new(DuckDbBuffer::new(
            settings.root_dir.clone(),
            settings.flush_chunk_rows,
            settings.target_catalog.clone(),
            &factory_guard,
        )?);
        drop(factory_guard);
        let tracker = Arc::new(FlushTracker::new());
        let coordinator = Arc::new(DqCoordinator::new(
            settings.clone(),
            buffer.clone(),
            tracker.clone(),
            tx,
        )?);
        let runtime = Arc::new(Self);
        spawn_flush_workers(
            factory,
            settings.clone(),
            buffer,
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
    _buffer: Arc<DuckDbBuffer>,
    tracker: Arc<FlushTracker>,
    coordinator: Arc<DqCoordinator>,
    mut rx: UnboundedReceiver<FlushPayload>,
) {
    let semaphore = Arc::new(Semaphore::new(settings.max_parallel_flushes));
    tokio::spawn(async move {
        while let Some(payload) = rx.recv().await {
            debug!(
                table = %payload.table,
                rows = payload.rows,
                bytes = payload.bytes,
                "duckling queue flush worker received payload"
            );
            let permit = semaphore.clone().acquire_owned().await.unwrap();
            let factory_for_flush = factory.clone();
            let factory_for_dlq = factory.clone();
            let coordinator_cloned = coordinator.clone();
            let coordinator_for_flush = coordinator_cloned.clone();
            let settings_for_flush = settings.clone();
            let settings_for_dlq = settings.clone();
            let tracker_clone = tracker.clone();
            let payload_for_dlq = payload.clone();
            let table_name = payload.table.clone();
            tokio::spawn(async move {
                debug!(
                    table = %payload.table,
                    rows = payload.rows,
                    bytes = payload.bytes,
                    "duckling queue flush worker started"
                );
                let res = tokio::task::spawn_blocking(move || {
                    flush_payload(
                        factory_for_flush,
                        &settings_for_flush,
                        coordinator_for_flush,
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
                        attempt_dlq_offload(
                            factory_for_dlq.clone(),
                            &settings_for_dlq,
                            &coordinator_cloned,
                            &payload_for_dlq,
                        )
                        .unwrap_or(false)
                    }
                    Err(join_err) => {
                        warn!(error = %join_err, "duckling queue flush panicked");
                        attempt_dlq_offload(
                            factory_for_dlq.clone(),
                            &settings_for_dlq,
                            &coordinator_cloned,
                            &payload_for_dlq,
                        )
                        .unwrap_or(false)
                    }
                };

                if !handled {
                    coordinator_cloned.mark_failed(&table_name);
                    coordinator_cloned.requeue(&table_name);
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
    coordinator: Arc<DqCoordinator>,
    payload: FlushPayload,
) -> Result<(), ServerError> {
    if payload.is_empty() {
        return Ok(());
    }

    let FlushPayload {
        table,
        schema: _,
        batches,
        handle,
        rows,
        bytes,
    } = payload;

    info!(
        table = %table,
        rows = rows,
        "flushing duckling queue payload"
    );

    let conn = factory.lock().unwrap().create_connection()?;
    let qualified_table = format!("{}.{}", settings.target_catalog, table);
    let table_schema = Arc::new(conn.table_schema(&qualified_table)?);
    debug!(
        table = %table,
        qualified = %qualified_table,
        rows = rows,
        bytes = bytes,
        "flush_payload: fetched table schema"
    );
    // Pretty-print original batches
    if let Ok(formatted) = pretty_format_batches(&batches) {
        debug!(
            table = %table,
            "Original batches before alignment:\n{}",
            formatted
        );
    }

    let aligned_batches = batches
        .iter()
        .map(|batch| align_batch_to_table_schema(batch, &table_schema, None))
        .collect::<Result<Vec<_>, ServerError>>()?;

    // Pretty-print aligned batches
    if let Ok(formatted) = pretty_format_batches(&aligned_batches) {
        debug!(
            table = %table,
            "Aligned batches after alignment:\n{}",
            formatted
        );
    }

    // Defensive checks: validate alignment preserved data integrity
    let original_row_count: usize = batches.iter().map(|b| b.num_rows()).sum();
    let aligned_row_count: usize = aligned_batches.iter().map(|b| b.num_rows()).sum();
    if original_row_count != aligned_row_count {
        return Err(ServerError::Internal(format!(
            "Row count mismatch after alignment: original={} aligned={}",
            original_row_count, aligned_row_count
        )));
    }

    // Validate column count matches table schema
    for (idx, batch) in aligned_batches.iter().enumerate() {
        if batch.num_columns() != table_schema.fields().len() {
            return Err(ServerError::Internal(format!(
                "Column count mismatch in batch {}: expected={} got={}",
                idx,
                table_schema.fields().len(),
                batch.num_columns()
            )));
        }
    }

    debug!(
        table = %table,
        batches = aligned_batches.len(),
        rows = aligned_row_count,
        "flush_payload: aligned batches validated"
    );
    conn.insert_with_appender(&settings.target_catalog, &table, aligned_batches)?;
    debug!(table = %table, rows = rows, "flush_payload: inserted into target");
    coordinator.ack(&table, &handle, rows, bytes)?;
    info!(
        table = %table,
        rows = rows,
        bytes = bytes,
        "duckling queue payload flushed and acked"
    );
    Ok(())
}

fn attempt_dlq_offload(
    factory: Arc<Mutex<EngineFactory>>,
    settings: &Settings,
    coordinator: &Arc<DqCoordinator>,
    payload: &FlushPayload,
) -> Result<bool, ServerError> {
    let Some(target_root) = settings.dlq_target.as_ref() else {
        return Ok(false);
    };
    let dest = format!(
        "{}/{}/{}-{}.parquet",
        target_root.trim_end_matches('/'),
        payload.table,
        Utc::now().timestamp_millis(),
        uuid::Uuid::new_v4()
    );
    let temp_path = std::env::temp_dir().join(format!(
        "duckling_queue_dlq-{}-{}.parquet",
        payload.table,
        uuid::Uuid::new_v4()
    ));

    write_payload_parquet(&temp_path, &payload.schema, &payload.batches)?;

    if dest.contains("://") {
        let conn = factory.lock().unwrap().create_connection()?;
        let app_conn = conn
            .conn
            .lock()
            .map_err(|_| ServerError::Internal("connection mutex poisoned".to_string()))?;
        copy_parquet_to_dest(&temp_path, &dest, &app_conn)?;
    } else {
        create_parent_dir_if_local(&dest)?;
        std::fs::rename(&temp_path, &dest)
            .or_else(|_| std::fs::copy(&temp_path, &dest).map(|_| ()))
            .map_err(|err| {
                ServerError::Internal(format!(
                    "failed to move duckling queue DLQ file to {}: {err}",
                    dest
                ))
            })?;
    }

    coordinator.ack(&payload.table, &payload.handle, payload.rows, payload.bytes)?;
    info!(
        table = %payload.table,
        target = %dest,
        "duckling queue payload offloaded to DLQ target"
    );
    Ok(true)
}

fn write_payload_parquet(
    path: &std::path::Path,
    schema: &SchemaRef,
    batches: &[RecordBatch],
) -> Result<(), ServerError> {
    let file = std::fs::File::create(path).map_err(|err| {
        ServerError::Internal(format!(
            "failed to create DLQ parquet file {}: {err}",
            path.display()
        ))
    })?;
    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .build();
    let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props)).map_err(|err| {
        ServerError::Internal(format!("failed to create DLQ parquet writer: {err}"))
    })?;
    for batch in batches {
        writer.write(batch).map_err(|err| {
            ServerError::Internal(format!("failed to write DLQ parquet batch: {err}"))
        })?;
    }
    writer.close().map_err(|err| {
        ServerError::Internal(format!("failed to finish DLQ parquet file: {err}"))
    })?;
    Ok(())
}

fn copy_parquet_to_dest(
    src_path: &std::path::Path,
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
