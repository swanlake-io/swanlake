use std::collections::HashMap;
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use arrow_array::RecordBatch;
use arrow_schema::{Schema, SchemaRef};
use tokio::sync::mpsc::UnboundedSender;
use tracing::{debug, info, warn};

use crate::dq::config::Settings;
use crate::dq::duckdb_buffer::{DuckDbBuffer, FlushHandle, SelectedPayload};
use crate::engine::EngineFactory;
use crate::error::ServerError;

#[derive(Debug)]
pub struct FlushTracker {
    pending: Mutex<usize>,
    signal: Condvar,
}

impl Default for FlushTracker {
    fn default() -> Self {
        Self::new()
    }
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
    pub batches: Vec<RecordBatch>,
    pub handle: FlushHandle,
    pub rows: usize,
    pub bytes: u64,
}

impl FlushPayload {
    pub fn is_empty(&self) -> bool {
        self.rows == 0
    }
    pub fn total_rows(&self) -> usize {
        self.rows
    }
    pub fn total_bytes(&self) -> u64 {
        self.bytes
    }
}

struct BufferedTable {
    schema: SchemaRef,
    total_rows: usize,
    total_bytes: u64,
    last_flush: Instant,
    last_activity: Instant,
    inflight: bool,
}

impl BufferedTable {
    fn new(schema: SchemaRef, row_count: usize) -> Self {
        let now = Instant::now();
        Self {
            schema,
            total_rows: row_count,
            total_bytes: 0,
            last_flush: now,
            last_activity: now,
            inflight: false,
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

    fn record_enqueue(&mut self, rows: usize, bytes: u64) {
        self.total_rows += rows;
        self.total_bytes += bytes;
        self.last_activity = Instant::now();
    }

    fn should_flush(&self, max_rows: usize, max_bytes: u64) -> bool {
        if self.inflight {
            return false;
        }
        if max_rows > 0 && self.total_rows >= max_rows {
            return true;
        }
        if max_bytes > 0 && self.total_bytes >= max_bytes {
            return true;
        }
        false
    }

    fn mark_inflight(&mut self) {
        self.inflight = true;
    }

    fn mark_failed(&mut self) {
        self.inflight = false;
    }

    fn mark_flushed(&mut self, rows: usize, bytes: u64) {
        self.inflight = false;
        self.last_flush = Instant::now();
        self.last_activity = self.last_flush;
        self.total_rows = self.total_rows.saturating_sub(rows);
        self.total_bytes = self.total_bytes.saturating_sub(bytes);
    }
}

struct CoordinatorInner {
    tables: HashMap<String, BufferedTable>,
}

/// Coordinates buffering of duckling_queue inserts and schedules flushes.
#[derive(Clone)]
pub struct DqCoordinator {
    settings: Settings,
    buffer: Arc<DuckDbBuffer>,
    factory: Arc<Mutex<EngineFactory>>,
    tracker: Arc<FlushTracker>,
    flush_tx: UnboundedSender<FlushPayload>,
    inner: Arc<Mutex<CoordinatorInner>>,
}

impl DqCoordinator {
    pub fn new(
        settings: Settings,
        buffer: Arc<DuckDbBuffer>,
        factory: Arc<Mutex<EngineFactory>>,
        tracker: Arc<FlushTracker>,
        flush_tx: UnboundedSender<FlushPayload>,
    ) -> Result<Self, ServerError> {
        let mut tables = HashMap::new();
        for state in buffer.load_table_states()? {
            tables.insert(
                state.table.clone(),
                BufferedTable::new(state.schema.clone(), state.row_count),
            );
        }
        let coordinator = Self {
            settings,
            buffer,
            factory,
            tracker,
            flush_tx,
            inner: Arc::new(Mutex::new(CoordinatorInner { tables })),
        };
        coordinator.dispatch_pending_on_startup();
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

        self.ensure_schema_ready(&table_key, &schema_ref)?;

        let (inserted_rows, inserted_bytes) =
            self.buffer.enqueue(&table_key, &schema_ref, &batches)?;

        let mut should_dispatch = false;
        {
            let mut guard = self.inner.lock().expect("dq coordinator mutex poisoned");
            let entry = guard
                .tables
                .entry(table_key.clone())
                .or_insert_with(|| BufferedTable::new(schema_ref.clone(), 0));
            entry.ensure_schema(&schema_ref)?;
            entry.record_enqueue(inserted_rows, inserted_bytes);
            if entry.should_flush(
                self.settings.buffer_max_rows,
                self.settings.buffer_max_bytes,
            ) {
                should_dispatch = true;
            }
        }

        if should_dispatch {
            self.try_dispatch_table(table_key);
        }

        // If buffer file grew beyond limit, trigger best-effort flush attempts.
        if self.settings.max_db_bytes > 0 {
            if let Ok(size) = self.buffer.database_size_bytes() {
                if size > self.settings.max_db_bytes {
                    self.try_dispatch_table(table.to_string());
                }
            }
        }

        Ok(inserted_rows)
    }

    /// Flush tables whose buffered data exceeded the max age.
    pub fn flush_stale_buffers(&self) {
        if self.settings.buffer_max_age == Duration::ZERO {
            return;
        }
        let mut candidates = Vec::new();
        {
            let guard = self.inner.lock().expect("dq coordinator mutex poisoned");
            let now = Instant::now();
            for (table, info) in guard.tables.iter() {
                if info.total_rows == 0 || info.inflight {
                    continue;
                }
                if now.duration_since(info.last_activity) >= self.settings.buffer_max_age {
                    candidates.push(table.clone());
                }
            }
        }

        if !candidates.is_empty() {
            debug!(
                count = candidates.len(),
                "flushing stale duckling queue buffers due to age"
            );
        }

        for table in candidates {
            self.try_dispatch_table(table);
        }
    }

    /// Force flush every buffered table.
    pub fn force_flush_all(&self) {
        let tables: Vec<String> = {
            let guard = self.inner.lock().expect("dq coordinator mutex poisoned");
            guard.tables.keys().cloned().collect()
        };
        if !tables.is_empty() {
            info!(
                count = tables.len(),
                "force flushing all duckling queue buffers"
            );
        }
        for table in tables {
            self.try_dispatch_table(table);
        }
        self.wait_for_idle();
    }

    /// Re-dispatch a table after a failed flush attempt.
    pub fn requeue(&self, table: &str) {
        self.mark_failed(table);
        self.try_dispatch_table(table.to_string());
    }

    /// Called by flush runtime when a flush attempt failed.
    pub fn mark_failed(&self, table: &str) {
        let mut guard = self.inner.lock().expect("dq coordinator mutex poisoned");
        if let Some(state) = guard.tables.get_mut(table) {
            state.mark_failed();
        }
    }

    /// Called by flush runtime when a payload has been durably flushed.
    pub fn ack(
        &self,
        table: &str,
        handle: &FlushHandle,
        rows: usize,
        bytes: u64,
    ) -> Result<(), ServerError> {
        self.buffer.ack(handle)?;
        let mut guard = self.inner.lock().expect("dq coordinator mutex poisoned");
        if let Some(state) = guard.tables.get_mut(table) {
            state.mark_flushed(rows, bytes);
        }
        Ok(())
    }

    fn ensure_schema_ready(&self, table: &str, incoming: &SchemaRef) -> Result<(), ServerError> {
        if let Some((stored, row_count)) = self.buffer.table_info(table)? {
            if stored.as_ref() == incoming.as_ref() {
                return Ok(());
            }
            // Try to refresh from SwanLake; only allow change if remote matches incoming.
            let remote = self.fetch_remote_schema(table)?;
            if remote.as_ref() != incoming.as_ref() {
                return Err(ServerError::Internal(format!(
                    "duckling queue schema mismatch for {table}; remote schema does not match incoming"
                )));
            }
            if row_count > 0 {
                self.flush_table_and_wait(table)?;
                let remaining = {
                    let guard = self.inner.lock().expect("dq coordinator mutex poisoned");
                    guard.tables.get(table).map(|t| t.total_rows).unwrap_or(0)
                };
                if remaining > 0 {
                    return Err(ServerError::Internal(format!(
                        "duckling queue schema change for {table} blocked; buffered rows could not be flushed"
                    )));
                }
            }
            self.buffer.replace_table_schema(table, incoming.as_ref())?;
            let mut guard = self.inner.lock().expect("dq coordinator mutex poisoned");
            let entry = guard
                .tables
                .entry(table.to_string())
                .or_insert_with(|| BufferedTable::new(incoming.clone(), 0));
            entry.schema = incoming.clone();
            entry.total_rows = 0;
            entry.total_bytes = 0;
            entry.inflight = false;
            return Ok(());
        }

        // No existing table; create on first use.
        self.buffer.ensure_table(table, incoming.as_ref())
    }

    fn fetch_remote_schema(&self, table: &str) -> Result<SchemaRef, ServerError> {
        let conn = self.factory.lock().unwrap().create_connection()?;
        let qualified = format!("{}.{}", self.settings.target_catalog, table);
        let schema = conn.table_schema(&qualified)?;
        Ok(Arc::new(schema))
    }

    fn flush_table_and_wait(&self, table: &str) -> Result<(), ServerError> {
        let mut attempts = 0;
        loop {
            let selection = self.buffer.select_for_flush(table)?;
            if let Some(sel) = selection {
                self.dispatch_payload(sel.into());
                self.wait_for_idle();
            }
            let remaining = {
                let guard = self.inner.lock().expect("dq coordinator mutex poisoned");
                guard.tables.get(table).map(|t| t.total_rows).unwrap_or(0)
            };
            if remaining == 0 {
                return Ok(());
            }
            attempts += 1;
            if attempts >= 3 {
                return Err(ServerError::Internal(format!(
                    "duckling queue failed to drain buffered rows for {table} after retries"
                )));
            }
        }
    }

    fn dispatch_pending_on_startup(&self) {
        let tables: Vec<String> = {
            let guard = self.inner.lock().expect("dq coordinator mutex poisoned");
            guard.tables.keys().cloned().collect()
        };
        for table in tables {
            self.try_dispatch_table(table);
        }
    }

    fn try_dispatch_table(&self, table: String) {
        let selection = match self.buffer.select_for_flush(&table) {
            Ok(Some(payload)) => payload,
            Ok(None) => return,
            Err(err) => {
                warn!(error = %err, table = %table, "failed to select duckling queue payload");
                return;
            }
        };

        let mut guard = self.inner.lock().expect("dq coordinator mutex poisoned");
        let Some(table_state) = guard.tables.get_mut(&table) else {
            return;
        };
        if table_state.inflight {
            return;
        }
        table_state.mark_inflight();
        drop(guard);

        self.dispatch_payload(selection.into());
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
        let mut out = Vec::new();
        let tables: Vec<String> = {
            let guard = self.inner.lock().expect("dq coordinator mutex poisoned");
            guard.tables.keys().cloned().collect()
        };
        for table in tables {
            if let Ok(Some(selection)) = self.buffer.select_for_flush(&table) {
                out.push(selection.into());
            }
        }
        out
    }

    pub fn wait_for_idle(&self) {
        self.tracker.wait_for_idle();
    }
}

impl From<SelectedPayload> for FlushPayload {
    fn from(value: SelectedPayload) -> Self {
        Self {
            table: value.table,
            schema: value.schema,
            batches: value.batches,
            handle: value.handle,
            rows: value.rows,
            bytes: value.bytes,
        }
    }
}
