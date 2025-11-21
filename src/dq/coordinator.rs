use std::collections::HashMap;
use std::sync::{mpsc, Arc, Condvar, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use arrow_array::RecordBatch;
use arrow_schema::{Schema, SchemaRef};
use tokio::sync::mpsc::UnboundedSender;
use tracing::{debug, info, warn};

use crate::dq::config::Settings;
use crate::dq::storage::{DurableChunk, DurableStorage};
use crate::error::ServerError;

#[derive(Debug)]
pub struct FlushTracker {
    pending: Mutex<usize>,
    signal: Condvar,
}

impl FlushTracker {
    pub fn new() -> Self {
        Self {
            pending: Mutex::new(0),
            signal: Condvar::new(),
        }
    }

    pub fn track(&self) {
        let mut guard = self.pending.lock().expect("flush tracker mutex poisoned");
        *guard += 1;
    }

    pub fn complete(&self) {
        let mut guard = self.pending.lock().expect("flush tracker mutex poisoned");
        if *guard > 0 {
            *guard -= 1;
            if *guard == 0 {
                self.signal.notify_all();
            }
        }
    }

    pub fn wait_for_idle(&self) {
        let mut guard = self.pending.lock().expect("flush tracker mutex poisoned");
        while *guard > 0 {
            guard = self
                .signal
                .wait(guard)
                .expect("flush tracker condvar poisoned");
        }
    }
}

#[derive(Debug, Clone)]
pub struct FlushPayload {
    pub table: String,
    pub schema: SchemaRef,
    chunks: Vec<BufferedChunk>,
}

impl FlushPayload {
    pub fn is_empty(&self) -> bool {
        self.chunks.is_empty()
    }

    pub fn total_rows(&self) -> usize {
        self.chunks.iter().map(|chunk| chunk.rows).sum()
    }

    pub fn chunk_handles(&self) -> Vec<DurableChunk> {
        self.chunks
            .iter()
            .map(|chunk| chunk.handle.clone())
            .collect()
    }

    pub fn into_batches_and_chunks(self) -> (Vec<RecordBatch>, Vec<DurableChunk>) {
        let mut batches = Vec::new();
        let mut handles = Vec::new();
        for mut chunk in self.chunks {
            handles.push(chunk.handle);
            batches.append(&mut chunk.batches);
        }
        (batches, handles)
    }
}

#[derive(Debug, Clone)]
struct BufferedChunk {
    handle: DurableChunk,
    batches: Vec<RecordBatch>,
    rows: usize,
    bytes: u64,
}

impl BufferedChunk {
    fn new(handle: DurableChunk, batches: Vec<RecordBatch>) -> Self {
        let mut rows = 0usize;
        let mut bytes = 0u64;
        for batch in &batches {
            rows += batch.num_rows();
            bytes += batch.get_array_memory_size() as u64;
        }
        Self {
            handle,
            batches,
            rows,
            bytes,
        }
    }
}

struct BufferedTable {
    schema: SchemaRef,
    chunks: Vec<BufferedChunk>,
    total_rows: usize,
    total_bytes: u64,
    last_flush: Instant,
    last_activity: Instant,
}

impl BufferedTable {
    fn new(schema: SchemaRef) -> Self {
        let now = Instant::now();
        Self {
            schema,
            chunks: Vec::new(),
            total_rows: 0,
            total_bytes: 0,
            last_flush: now,
            last_activity: now,
        }
    }

    fn ensure_schema(&self, schema: &SchemaRef) -> Result<(), ServerError> {
        if self.schema.as_ref() != schema.as_ref() {
            return Err(ServerError::Internal(
                "duckling queue received mismatched schema for buffered table".to_string(),
            ));
        }
        Ok(())
    }

    fn push_chunk(&mut self, chunk: BufferedChunk) -> usize {
        self.total_rows += chunk.rows;
        self.total_bytes += chunk.bytes;
        self.last_activity = Instant::now();
        let inserted = chunk.rows;
        self.chunks.push(chunk);
        inserted
    }

    fn should_flush(&self, max_rows: usize, max_bytes: u64) -> bool {
        if max_rows > 0 && self.total_rows >= max_rows {
            return true;
        }
        if max_bytes > 0 && self.total_bytes >= max_bytes {
            return true;
        }
        false
    }

    fn drain_payload(&mut self, table: &str) -> Option<FlushPayload> {
        if self.total_rows == 0 {
            return None;
        }
        let chunks = std::mem::take(&mut self.chunks);
        let payload = FlushPayload {
            table: table.to_string(),
            schema: self.schema.clone(),
            chunks,
        };
        self.total_rows = 0;
        self.total_bytes = 0;
        self.last_flush = Instant::now();
        self.last_activity = self.last_flush;
        Some(payload)
    }
}

struct CoordinatorInner {
    tables: HashMap<String, BufferedTable>,
}

/// Coordinates in-memory buffering of duckling_queue inserts and schedules flushes.
#[derive(Clone)]
pub struct DqCoordinator {
    settings: Settings,
    storage: Arc<DurableStorage>,
    tracker: Arc<FlushTracker>,
    flush_tx: UnboundedSender<FlushPayload>,
    persist_tx: mpsc::Sender<PersistJob>,
    inner: Arc<Mutex<CoordinatorInner>>,
}

impl DqCoordinator {
    pub fn new(
        settings: Settings,
        storage: Arc<DurableStorage>,
        tracker: Arc<FlushTracker>,
        flush_tx: UnboundedSender<FlushPayload>,
    ) -> Result<Self, ServerError> {
        let inner = CoordinatorInner {
            tables: HashMap::new(),
        };
        let persist_tx = spawn_persist_worker(storage.clone());
        let coordinator = Self {
            settings,
            storage,
            tracker,
            flush_tx,
            persist_tx,
            inner: Arc::new(Mutex::new(inner)),
        };
        let restored = coordinator.restore_from_storage()?;
        if restored > 0 {
            info!(restored, "restored duckling queue batches from disk");
        }
        Ok(coordinator)
    }

    /// Buffer new RecordBatches for a duckling_queue table.
    pub fn enqueue(
        &self,
        table: &str,
        schema: Schema,
        batches: Vec<RecordBatch>,
    ) -> Result<usize, ServerError> {
        if batches.is_empty() {
            return Ok(0);
        }
        let schema_ref: SchemaRef = Arc::new(schema);
        let table_key = table.to_string();

        {
            let mut guard = self.inner.lock().expect("dq coordinator mutex poisoned");
            let entry = guard
                .tables
                .entry(table_key.clone())
                .or_insert_with(|| BufferedTable::new(schema_ref.clone()));
            entry.ensure_schema(&schema_ref)?;
        }

        let chunk = self.persist_chunk(table_key.clone(), schema_ref, batches)?;
        let mut maybe_payload = None;
        let inserted = {
            let mut guard = self.inner.lock().expect("dq coordinator mutex poisoned");
            let entry = guard
                .tables
                .get_mut(&table_key)
                .expect("duckling table entry missing during enqueue");
            let inserted = entry.push_chunk(chunk);
            if entry.should_flush(
                self.settings.buffer_max_rows,
                self.settings.buffer_max_bytes,
            ) {
                maybe_payload = entry.drain_payload(&table_key);
            }
            inserted
        };

        if let Some(payload) = maybe_payload {
            self.dispatch_payload(payload);
        }

        Ok(inserted)
    }

    /// Flush tables whose buffered data exceeded the max age.
    pub fn flush_stale_buffers(&self) {
        if self.settings.buffer_max_age == Duration::ZERO {
            return;
        }
        let mut payloads = Vec::new();
        {
            let mut guard = self.inner.lock().expect("dq coordinator mutex poisoned");
            let now = Instant::now();
            for (table, info) in guard.tables.iter_mut() {
                if info.total_rows == 0 {
                    continue;
                }
                if now.duration_since(info.last_activity) >= self.settings.buffer_max_age {
                    if let Some(payload) = info.drain_payload(table) {
                        payloads.push(payload);
                    }
                }
            }
        }

        if !payloads.is_empty() {
            debug!(
                count = payloads.len(),
                "flushing stale duckling queue buffers due to age"
            );
        }

        for payload in payloads {
            self.dispatch_payload(payload);
        }
    }

    /// Force flush every buffered table.
    pub fn force_flush_all(&self) {
        let payloads = self.take_all_payloads();
        if !payloads.is_empty() {
            info!(
                count = payloads.len(),
                "force flushing all duckling queue buffers"
            );
        }
        for payload in payloads {
            self.dispatch_payload(payload);
        }
        self.wait_for_idle();
    }

    /// Re-queue payload data when a flush task fails.
    pub fn requeue(&self, payload: FlushPayload) {
        let mut guard = self.inner.lock().expect("dq coordinator mutex poisoned");
        let entry = guard
            .tables
            .entry(payload.table.clone())
            .or_insert_with(|| BufferedTable::new(payload.schema.clone()));
        if let Err(err) = entry.ensure_schema(&payload.schema) {
            warn!(error = %err, table = %payload.table, "failed to requeue duckling buffer due to schema mismatch");
            return;
        }
        for chunk in payload.chunks {
            entry.push_chunk(chunk);
        }
    }

    fn take_all_payloads(&self) -> Vec<FlushPayload> {
        let mut guard = self.inner.lock().expect("dq coordinator mutex poisoned");
        let mut payloads = Vec::new();
        for (table, info) in guard.tables.iter_mut() {
            if let Some(payload) = info.drain_payload(table) {
                payloads.push(payload);
            }
        }
        payloads
    }

    fn dispatch_payload(&self, payload: FlushPayload) {
        self.tracker.track();
        if self.flush_tx.send(payload).is_err() {
            self.tracker.complete();
            warn!("duckling queue flush channel dropped; dropping payload");
        }
    }

    #[cfg(test)]
    pub fn take_all_payloads_for_test(&self) -> Vec<FlushPayload> {
        self.take_all_payloads()
    }

    pub fn wait_for_idle(&self) {
        self.tracker.wait_for_idle();
    }

    fn restore_from_storage(&self) -> Result<usize, ServerError> {
        let persisted = self.storage.load_pending()?;
        if persisted.is_empty() {
            return Ok(0);
        }
        let mut guard = self.inner.lock().expect("dq coordinator mutex poisoned");
        let mut restored = 0usize;
        for chunk in persisted {
            let entry = guard
                .tables
                .entry(chunk.table.clone())
                .or_insert_with(|| BufferedTable::new(chunk.schema.clone()));
            entry.ensure_schema(&chunk.schema)?;
            entry.push_chunk(BufferedChunk::new(chunk.handle, chunk.batches));
            restored += 1;
        }
        Ok(restored)
    }

    fn persist_chunk(
        &self,
        table: String,
        schema: SchemaRef,
        batches: Vec<RecordBatch>,
    ) -> Result<BufferedChunk, ServerError> {
        let (tx, rx) = mpsc::channel();
        self.persist_tx
            .send(PersistJob {
                table,
                schema,
                batches,
                respond_to: tx,
            })
            .map_err(|_| {
                ServerError::Internal(
                    "duckling queue persistence worker stopped unexpectedly".to_string(),
                )
            })?;
        rx.recv().map_err(|_| {
            ServerError::Internal("duckling queue persistence worker response dropped".to_string())
        })?
    }
}

struct PersistJob {
    table: String,
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
    respond_to: mpsc::Sender<Result<BufferedChunk, ServerError>>,
}

fn spawn_persist_worker(storage: Arc<DurableStorage>) -> mpsc::Sender<PersistJob> {
    let (tx, rx) = mpsc::channel::<PersistJob>();
    thread::Builder::new()
        .name("dq_persist_worker".to_string())
        .spawn(move || {
            while let Ok(job) = rx.recv() {
                let result = storage
                    .persist_chunk(&job.table, &job.schema, &job.batches)
                    .map(|handle| BufferedChunk::new(handle, job.batches));
                let _ = job.respond_to.send(result);
            }
        })
        .expect("failed to spawn duckling queue persist worker");
    tx
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{ArrayRef, Int32Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use std::path::PathBuf;
    use std::sync::Arc;
    use tempfile::TempDir;

    fn test_settings(root: PathBuf) -> Settings {
        Settings {
            buffer_max_rows: usize::MAX,
            buffer_max_bytes: u64::MAX,
            buffer_max_age: Duration::from_secs(60),
            flush_interval: Duration::from_secs(60),
            max_parallel_flushes: 1,
            target_catalog: "swanlake".to_string(),
            root_dir: root,
            dlq_target: None,
        }
    }

    fn sample_batch(schema: &Arc<Schema>) -> RecordBatch {
        let values: ArrayRef = Arc::new(Int32Array::from(vec![1, 2]));
        RecordBatch::try_new(schema.clone(), vec![values]).expect("valid record batch")
    }

    #[test]
    fn restores_chunks_after_restart() {
        let temp = TempDir::new().expect("tempdir");
        let settings = test_settings(temp.path().join("dq-root"));
        let storage = Arc::new(DurableStorage::new(settings.root_dir.clone()).expect("storage"));
        let tracker = Arc::new(FlushTracker::new());
        let (flush_tx, _flush_rx) = tokio::sync::mpsc::unbounded_channel();
        let coordinator = DqCoordinator::new(settings.clone(), storage.clone(), tracker, flush_tx)
            .expect("coordinator");

        let schema = Arc::new(Schema::new(vec![Field::new("i", DataType::Int32, false)]));
        let batch = sample_batch(&schema);
        let rows = coordinator
            .enqueue("dq_restart_test", schema.as_ref().clone(), vec![batch])
            .expect("enqueue");
        assert_eq!(rows, 2);
        drop(coordinator);

        let tracker2 = Arc::new(FlushTracker::new());
        let (flush_tx2, _flush_rx2) = tokio::sync::mpsc::unbounded_channel();
        let coordinator2 = DqCoordinator::new(settings, storage, tracker2, flush_tx2)
            .expect("recreated coordinator");

        let payloads = coordinator2.take_all_payloads_for_test();
        assert_eq!(payloads.len(), 1);
        assert_eq!(payloads[0].table, "dq_restart_test");
        assert_eq!(payloads[0].total_rows(), 2);
    }
}
