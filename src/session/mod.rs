//! Session management module.
//!
//! This module provides:
//! - `Session`: Client session with dedicated DuckDB connection and state
//! - `SessionRegistry`: Registry for managing all active sessions
//! - `SessionId`: Unique identifier for sessions
//! - Transaction and prepared statement management per session

pub mod id;
pub mod registry;

pub use id::SessionId;

// Session management - each client connection gets a dedicated session.
//
// A Session owns:
// - A dedicated DuckDB connection (persistent state)
// - Transaction state
// - Prepared statements
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use arrow_array::RecordBatch;
use arrow_schema::{Field, Schema};
use duckdb::types::Value;
use tracing::{debug, info, instrument, warn};

use crate::dq::DqCoordinator;
use crate::engine::{DuckDbConnection, QueryResult};
use crate::error::ServerError;
use crate::session::id::{
    StatementHandle, StatementHandleGenerator, TransactionId, TransactionIdGenerator,
};
use crate::sql_parser::ParsedStatement;

/// Metadata persisted alongside each prepared/ephemeral handle.
///
/// This reflects the authoritative view of a statement that can be
/// executed later (SQL text, schema if known, flags, etc.).
#[derive(Debug, Clone)]
pub struct PreparedStatementMeta {
    pub sql: String,
    pub is_query: bool,
    pub schema: Option<Schema>,
    pub ephemeral: bool,
}

fn contains_duckling_queue_keyword(sql: &str) -> bool {
    contains_case_insensitive(sql, "duckling_queue")
}

fn contains_case_insensitive(haystack: &str, needle: &str) -> bool {
    if needle.is_empty() {
        return true;
    }

    let haystack_bytes = haystack.as_bytes();
    let needle_bytes = needle.as_bytes();
    if haystack_bytes.len() < needle_bytes.len() {
        return false;
    }

    'outer: for window in haystack_bytes.windows(needle_bytes.len()) {
        for (candidate, target) in window.iter().zip(needle_bytes.iter()) {
            if !candidate.eq_ignore_ascii_case(target) {
                continue 'outer;
            }
        }
        return true;
    }

    false
}

/// Builder-style options passed in when *creating* a prepared statement.
///
/// These options capture contextual data available up front (e.g. a schema
/// computed in the handler) without polluting the long-lived metadata struct.
/// Once the statement is registered, the selected options are copied into
/// [`PreparedStatementMeta`].
#[derive(Debug, Default)]
pub struct PreparedStatementOptions {
    pub cached_schema: Option<Schema>,
    pub ephemeral: bool,
}

impl PreparedStatementOptions {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_cached_schema(mut self, schema: Option<Schema>) -> Self {
        self.cached_schema = schema;
        self
    }

    pub fn ephemeral(mut self) -> Self {
        self.ephemeral = true;
        self
    }
}

/// State for a prepared statement including pending parameters
#[derive(Debug)]
struct PreparedStatementState {
    meta: PreparedStatementMeta,
    pending_parameters: Option<Vec<Value>>,
}

impl PreparedStatementState {
    fn new(meta: PreparedStatementMeta) -> Self {
        Self {
            meta,
            pending_parameters: None,
        }
    }
}

/// Transaction state
#[derive(Debug)]
struct Transaction {
    _id: TransactionId,
    _started_at: Instant,
    // Future: add transaction-specific state
}

impl Transaction {
    fn new(id: TransactionId) -> Self {
        Self {
            _id: id,
            _started_at: Instant::now(),
        }
    }
}

/// A client session with dedicated connection and state
pub struct Session {
    id: SessionId,
    connection: DuckDbConnection,
    transactions: Arc<Mutex<HashMap<TransactionId, Transaction>>>,
    aborted_transactions: Arc<Mutex<HashSet<TransactionId>>>,
    prepared_statements: Arc<Mutex<HashMap<StatementHandle, PreparedStatementState>>>,
    transaction_id_gen: Arc<TransactionIdGenerator>,
    statement_handle_gen: Arc<StatementHandleGenerator>,
    last_activity: Arc<Mutex<Instant>>,
    dq_coordinator: Option<Arc<DqCoordinator>>,
}

impl Session {
    /// Create a new session with a specific ID (for connection-based persistence)
    #[instrument(skip(connection))]
    pub fn new_with_id(id: SessionId, connection: DuckDbConnection) -> Self {
        debug!(session_id = %id, "created new session with specific ID");

        Self {
            id,
            connection,
            transactions: Arc::new(Mutex::new(HashMap::new())),
            aborted_transactions: Arc::new(Mutex::new(HashSet::new())),
            prepared_statements: Arc::new(Mutex::new(HashMap::new())),
            transaction_id_gen: Arc::new(TransactionIdGenerator::new()),
            statement_handle_gen: Arc::new(StatementHandleGenerator::new()),
            last_activity: Arc::new(Mutex::new(Instant::now())),
            dq_coordinator: None,
        }
    }

    /// Create a new session with duckling queue support.
    /// The queue is attached immediately on session creation.
    #[instrument(skip(connection, dq_coordinator))]
    pub fn new_with_id_and_dq(
        id: SessionId,
        connection: DuckDbConnection,
        dq_coordinator: Arc<DqCoordinator>,
    ) -> Result<Self, ServerError> {
        debug!(session_id = %id, "creating new session with duckling queue support");
        Ok(Self {
            id,
            connection,
            transactions: Arc::new(Mutex::new(HashMap::new())),
            aborted_transactions: Arc::new(Mutex::new(HashSet::new())),
            prepared_statements: Arc::new(Mutex::new(HashMap::new())),
            transaction_id_gen: Arc::new(TransactionIdGenerator::new()),
            statement_handle_gen: Arc::new(StatementHandleGenerator::new()),
            last_activity: Arc::new(Mutex::new(Instant::now())),
            dq_coordinator: Some(dq_coordinator),
        })
    }

    /// Get time since last activity
    pub fn idle_duration(&self) -> Duration {
        let last = self
            .last_activity
            .lock()
            .expect("last_activity mutex poisoned");
        last.elapsed()
    }

    /// Update last activity timestamp
    fn touch(&self) {
        let mut last = self
            .last_activity
            .lock()
            .expect("last_activity mutex poisoned");
        *last = Instant::now();
    }

    /// Run an operation and automatically roll back if DuckDB reports an aborted transaction.
    /// After rollback, retry the operation once.
    fn with_transaction_recovery<T, F>(&self, op: F) -> Result<T, ServerError>
    where
        F: Fn() -> Result<T, ServerError>,
    {
        match op() {
            Ok(value) => Ok(value),
            Err(err) => {
                if Self::is_transaction_abort_error(&err) {
                    self.recover_from_transaction_abort(&err);
                    // Retry once after rollback
                    op()
                } else {
                    Err(err)
                }
            }
        }
    }

    /// Run an operation without retry on transaction abort (for operations that move data).
    fn with_transaction_recovery_no_retry<T, F>(&self, op: F) -> Result<T, ServerError>
    where
        F: FnOnce() -> Result<T, ServerError>,
    {
        match op() {
            Ok(value) => Ok(value),
            Err(err) => {
                self.recover_from_transaction_abort(&err);
                Err(err)
            }
        }
    }

    /// Detect the "transaction aborted" state and roll back so the session can be reused.
    fn recover_from_transaction_abort(&self, err: &ServerError) {
        if !Self::is_transaction_abort_error(err) {
            return;
        }

        warn!(error = %err, "transaction aborted; rolling back session state");

        match self.connection.execute_batch("ROLLBACK") {
            Ok(()) => {
                let (cleared, cleared_ids) = {
                    let mut txs = self
                        .transactions
                        .lock()
                        .expect("transactions mutex poisoned");
                    let cleared = txs.len();
                    let ids = txs.keys().copied().collect::<Vec<_>>();
                    txs.clear();
                    (cleared, ids)
                };
                if !cleared_ids.is_empty() {
                    let mut aborted = self
                        .aborted_transactions
                        .lock()
                        .expect("aborted_transactions mutex poisoned");
                    for id in cleared_ids {
                        aborted.insert(id);
                    }
                }
                info!(
                    cleared_transactions = cleared,
                    "auto-rolled back aborted transaction"
                );
            }
            Err(rollback_err) => {
                warn!(error = %rollback_err, "failed to rollback aborted transaction");
            }
        }
    }

    fn is_transaction_abort_error(err: &ServerError) -> bool {
        match err {
            ServerError::DuckDb(duck_err) => {
                let msg = duck_err.to_string();
                msg.contains("Current transaction is aborted")
                    || msg.contains("TransactionContext Error")
            }
            _ => false,
        }
    }

    fn transaction_absent_error(&self, transaction_id: TransactionId) -> Result<(), ServerError> {
        let mut aborted = self
            .aborted_transactions
            .lock()
            .expect("aborted_transactions mutex poisoned");
        if aborted.remove(&transaction_id) {
            return Err(ServerError::TransactionAborted);
        }
        Err(ServerError::TransactionNotFound)
    }

    /// Execute a SELECT query
    #[instrument(skip(self), fields(session_id = %self.id, sql = %sql))]
    pub fn execute_query(&self, sql: &str) -> Result<QueryResult, ServerError> {
        self.ensure_duckling_queue_usage_allowed(sql)?;
        self.touch();
        self.with_transaction_recovery(|| self.connection.execute_query(sql))
    }

    /// Execute a query with parameters
    #[instrument(skip(self, params), fields(session_id = %self.id, sql = %sql))]
    pub fn execute_query_with_params(
        &self,
        sql: &str,
        params: &[Value],
    ) -> Result<QueryResult, ServerError> {
        self.ensure_duckling_queue_usage_allowed(sql)?;
        self.touch();
        self.with_transaction_recovery(|| self.connection.execute_query_with_params(sql, params))
    }

    /// Execute a statement (DDL/DML)
    #[instrument(skip(self), fields(session_id = %self.id, sql = %sql))]
    pub fn execute_statement(&self, sql: &str) -> Result<i64, ServerError> {
        self.ensure_duckling_queue_usage_allowed(sql)?;
        if let Some(result) = self.try_handle_dq_command(sql)? {
            return Ok(result);
        }
        if let Some(result) = self.try_handle_dq_insert(sql, None)? {
            return Ok(result);
        }

        self.touch();
        self.with_transaction_recovery(|| self.connection.execute_statement(sql))
    }

    /// Try to handle duckling queue administrative commands.
    /// Returns Some(affected_rows) if it's a DQ command, None otherwise.
    ///
    /// Supported commands:
    /// - `PRAGMA duckling_queue.flush` / `CALL duckling_queue_flush()` - Force flush all buffered data
    fn try_handle_dq_command(&self, sql: &str) -> Result<Option<i64>, ServerError> {
        let trimmed = sql.trim();
        let normalized = trimmed.trim_end_matches(';').trim().to_ascii_lowercase();

        if normalized == "pragma duckling_queue.flush"
            || normalized == "call duckling_queue_flush()"
        {
            let dq = self
                .dq_coordinator
                .as_ref()
                .ok_or(ServerError::DucklingQueueDisabled)?;
            dq.force_flush_all();
            Ok(Some(0))
        } else {
            Ok(None)
        }
    }

    /// Handle duckling_queue INSERT statements by executing the source query
    /// and enqueuing the resulting Arrow batches.
    ///
    /// This method is used for SQL-based INSERTs like:
    /// - `INSERT INTO duckling_queue.events SELECT ...`
    /// - `INSERT INTO duckling_queue.events VALUES (42)`
    /// - `INSERT INTO duckling_queue.events (id) VALUES (?)`
    ///
    /// For Arrow batch-based INSERTs (via DoPut), use `enqueue_duckling_batches` instead
    /// to avoid unnecessary conversions.
    ///
    /// Flow:
    /// 1. Parse the INSERT statement to extract table name
    /// 2. Extract the source SQL (the SELECT or VALUES clause)
    /// 3. Execute the source as a query to get Arrow batches
    /// 4. Enqueue the batches to the duckling_queue coordinator
    fn try_handle_dq_insert(
        &self,
        sql: &str,
        params: Option<&[Value]>,
    ) -> Result<Option<i64>, ServerError> {
        // Parse and validate this is an INSERT INTO duckling_queue.<table>
        let parsed = match ParsedStatement::parse(sql) {
            Some(stmt) if stmt.is_insert() => stmt,
            _ => return Ok(None), // Not an INSERT, let caller handle it
        };

        let table_ref = match parsed.get_insert_table() {
            Some(table) => table,
            None => return Ok(None),
        };

        // Check if this is duckling_queue schema (format: duckling_queue.<table>)
        let parts = table_ref.parts();
        if parts.len() != 2 || !parts[0].eq_ignore_ascii_case("duckling_queue") {
            return Ok(None); // Not a duckling_queue table
        }

        // Extract the source SQL: the SELECT or VALUES clause
        // Example: "INSERT INTO t SELECT ..." -> "SELECT ..."
        // Example: "INSERT INTO t VALUES (1)" -> "VALUES (1)"
        let source_sql = match parsed.insert_source_sql() {
            Some(sql) => sql,
            None => return Ok(None),
        };
        let insert_columns = parsed.get_insert_columns();

        // Ensure duckling_queue coordinator is available
        let dq = self
            .dq_coordinator
            .as_ref()
            .ok_or(ServerError::DucklingQueueDisabled)?;

        // Execute the source SQL as a query to get Arrow batches
        // This converts "VALUES (?)" or "SELECT ..." into Arrow format
        let result = if let Some(values) = params {
            self.with_transaction_recovery(|| {
                self.connection
                    .execute_query_with_params(&source_sql, values)
            })?
        } else {
            self.with_transaction_recovery(|| self.connection.execute_query(&source_sql))?
        };

        if result.total_rows == 0 {
            return Ok(Some(0));
        }

        // Extract target table name (e.g., "events" from "duckling_queue.events")
        let target_table = parts[1].clone();
        let QueryResult {
            schema,
            batches,
            total_rows,
            ..
        } = result;

        // If the INSERT specified column names, rename the produced batches so they align
        // with the target table on flush (duckling_queue doesn't carry SQL, only Arrow).
        let batches = if let Some(cols) = insert_columns.as_ref() {
            batches
                .into_iter()
                .map(|batch| Self::rename_batch_columns(batch, cols))
                .collect::<Result<Vec<_>, ServerError>>()?
        } else {
            batches
        };

        // Enqueue the Arrow batches for async processing
        dq.enqueue(&target_table, schema, batches)?;
        Ok(Some(total_rows as i64))
    }

    /// Ensure duckling_queue tables are only used for writes (INSERT), not reads (SELECT).
    ///
    /// This validation allows:
    /// - `INSERT INTO duckling_queue.<table> ...` (write operations)
    /// - `PRAGMA duckling_queue.flush` (admin commands)
    ///
    /// This rejects:
    /// - `SELECT FROM duckling_queue.<table>` (read operations)
    /// - `UPDATE/DELETE duckling_queue.<table>` (modification operations)
    fn ensure_duckling_queue_usage_allowed(&self, sql: &str) -> Result<(), ServerError> {
        if !contains_duckling_queue_keyword(sql) {
            return Ok(());
        }

        if let Some(parsed) = ParsedStatement::parse(sql) {
            // Allow INSERT INTO duckling_queue.<table>
            if parsed.is_insert() {
                if let Some(table_ref) = parsed.get_insert_table() {
                    let parts = table_ref.parts();
                    if parts.len() == 2 && parts[0].eq_ignore_ascii_case("duckling_queue") {
                        // This is INSERT INTO duckling_queue.<table>, allow it
                        return Ok(());
                    }
                }
            }

            // Check if the statement references duckling_queue in other contexts (SELECT, UPDATE, etc.)
            if parsed.references_duckling_queue_relation() {
                return Err(ServerError::DucklingQueueWriteOnly);
            }
        }

        Ok(())
    }

    /// Execute a statement with parameters
    #[instrument(skip(self, params), fields(session_id = %self.id, sql = %sql))]
    pub fn execute_statement_with_params(
        &self,
        sql: &str,
        params: &[Value],
    ) -> Result<usize, ServerError> {
        self.ensure_duckling_queue_usage_allowed(sql)?;
        if let Some(result) = self.try_handle_dq_command(sql)? {
            return Ok(result as usize);
        }
        if let Some(result) = self.try_handle_dq_insert(sql, Some(params))? {
            return Ok(result as usize);
        }

        self.touch();
        self.with_transaction_recovery(|| {
            self.connection.execute_statement_with_params(sql, params)
        })
    }

    /// Get schema for a query
    #[instrument(skip(self), fields(session_id = %self.id, sql = %sql))]
    pub fn schema_for_query(&self, sql: &str) -> Result<arrow_schema::Schema, ServerError> {
        self.ensure_duckling_queue_usage_allowed(sql)?;
        self.touch();
        self.with_transaction_recovery(|| self.connection.schema_for_query(sql))
    }

    /// Insert data using appender API with RecordBatches.
    ///
    /// This is an optimized path for INSERT statements that avoids
    /// converting RecordBatches to individual parameter values.
    #[instrument(skip(self, batches), fields(session_id = %self.id, catalog_name = %catalog_name, table_name = %table_name, rows = batches.iter().map(|b| b.num_rows()).sum::<usize>()))]
    pub fn insert_with_appender(
        &self,
        catalog_name: &str,
        table_name: &str,
        batches: Vec<arrow_array::RecordBatch>,
    ) -> Result<usize, ServerError> {
        self.touch();
        self.with_transaction_recovery_no_retry(|| {
            self.connection
                .insert_with_appender(catalog_name, table_name, batches)
        })
    }

    /// Get the schema of a table
    pub fn table_schema(&self, table_name: &str) -> Result<arrow_schema::Schema, ServerError> {
        self.with_transaction_recovery(|| self.connection.table_schema(table_name))
    }

    /// Return the current catalog selected for this session.
    pub fn current_catalog(&self) -> Result<String, ServerError> {
        self.with_transaction_recovery(|| self.connection.current_catalog())
    }

    fn rename_batch_columns(
        batch: RecordBatch,
        columns: &[String],
    ) -> Result<RecordBatch, ServerError> {
        if batch.num_columns() != columns.len() {
            return Err(ServerError::Internal(format!(
                "column count mismatch: batch has {} columns but INSERT specified {} columns",
                batch.num_columns(),
                columns.len()
            )));
        }
        let new_fields = batch
            .schema()
            .fields()
            .iter()
            .zip(columns.iter())
            .map(|(field, name)| Field::new(name, field.data_type().clone(), field.is_nullable()))
            .collect::<Vec<_>>();
        let new_schema = Arc::new(arrow_schema::Schema::new(new_fields));
        RecordBatch::try_new(new_schema, batch.columns().to_vec()).map_err(ServerError::Arrow)
    }

    /// Resolve catalog/table for a parsed table reference, respecting the session's current catalog.
    ///
    /// - If the reference is unqualified (single part), use the current catalog when it is set
    ///   to a real catalog (not DuckDB's default "memory"); otherwise fall back to `default_catalog`.
    /// - If the reference is qualified, return the provided catalog and table parts.
    pub fn resolve_catalog_and_table(
        &self,
        parts: &[String],
        default_catalog: &str,
    ) -> (String, String) {
        if parts.len() == 1 {
            let catalog = self
                .current_catalog()
                .ok()
                .filter(|c| !c.eq_ignore_ascii_case("memory"))
                .unwrap_or_else(|| default_catalog.to_string());
            (catalog, parts[0].clone())
        } else {
            (parts[0].clone(), parts[1].clone())
        }
    }

    /// Directly enqueue Arrow batches to duckling_queue without conversion.
    ///
    /// This is an optimized path for DoPut operations where the client sends
    /// Arrow batches for `INSERT INTO duckling_queue.<table>` prepared statements.
    ///
    /// Instead of converting Arrow → params → VALUES → Arrow (wasteful),
    /// we directly enqueue the original Arrow batches.
    ///
    /// # Arguments
    /// * `target_table` - The table name (without duckling_queue prefix)
    /// * `batches` - Arrow batches to enqueue
    ///
    /// # Example
    /// For `INSERT INTO duckling_queue.events (id, msg) VALUES (?, ?)`,
    /// when client sends Arrow batches via DoPut, this method enqueues them directly.
    #[instrument(skip(self, batches), fields(session_id = %self.id, target_table = %target_table, rows = batches.iter().map(|b| b.num_rows()).sum::<usize>()))]
    pub fn enqueue_duckling_batches(
        &self,
        target_table: &str,
        batches: Vec<arrow_array::RecordBatch>,
    ) -> Result<i64, ServerError> {
        // Ensure duckling_queue coordinator is available
        let dq = self
            .dq_coordinator
            .as_ref()
            .ok_or(ServerError::DucklingQueueDisabled)?;

        if batches.is_empty() {
            return Ok(0);
        }

        // Extract schema from the first batch
        let schema = batches[0].schema();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();

        // Enqueue the Arrow batches directly for async processing
        // Note: dq.enqueue expects owned Schema, so we dereference the Arc
        dq.enqueue(target_table, (*schema).clone(), batches)?;

        self.touch();
        Ok(total_rows as i64)
    }

    // === Prepared Statements ===

    /// Create a prepared statement and return its handle
    #[instrument(skip(self), fields(session_id = %self.id, sql = %sql))]
    pub fn create_prepared_statement(
        &self,
        sql: String,
        is_query: bool,
        options: PreparedStatementOptions,
    ) -> Result<StatementHandle, ServerError> {
        self.touch();

        let handle = self.statement_handle_gen.next();
        let meta = PreparedStatementMeta {
            sql,
            is_query,
            schema: options.cached_schema,
            ephemeral: options.ephemeral,
        };

        let mut prepared = self
            .prepared_statements
            .lock()
            .expect("prepared_statements mutex poisoned");
        prepared.insert(handle, PreparedStatementState::new(meta));

        debug!(handle = %handle, "created prepared statement");
        Ok(handle)
    }

    /// Get prepared statement metadata
    pub fn get_prepared_statement_meta(
        &self,
        handle: StatementHandle,
    ) -> Result<PreparedStatementMeta, ServerError> {
        let prepared = self
            .prepared_statements
            .lock()
            .expect("prepared_statements mutex poisoned");
        prepared
            .get(&handle)
            .map(|state| state.meta.clone())
            .ok_or(ServerError::PreparedStatementNotFound)
    }

    pub fn cache_prepared_statement_schema(
        &self,
        handle: StatementHandle,
        schema: Schema,
    ) -> Result<(), ServerError> {
        let mut prepared = self
            .prepared_statements
            .lock()
            .expect("prepared_statements mutex poisoned");
        let state = prepared
            .get_mut(&handle)
            .ok_or(ServerError::PreparedStatementNotFound)?;
        state.meta.schema = Some(schema);
        Ok(())
    }

    /// Set parameters for a prepared statement
    pub fn set_prepared_statement_parameters(
        &self,
        handle: StatementHandle,
        params: Vec<Value>,
    ) -> Result<(), ServerError> {
        let mut prepared = self
            .prepared_statements
            .lock()
            .expect("prepared_statements mutex poisoned");
        let state = prepared
            .get_mut(&handle)
            .ok_or(ServerError::PreparedStatementNotFound)?;
        state.pending_parameters = Some(params);
        Ok(())
    }

    /// Take (consume) parameters from a prepared statement
    pub fn take_prepared_statement_parameters(
        &self,
        handle: StatementHandle,
    ) -> Result<Option<Vec<Value>>, ServerError> {
        let mut prepared = self
            .prepared_statements
            .lock()
            .expect("prepared_statements mutex poisoned");
        let state = prepared
            .get_mut(&handle)
            .ok_or(ServerError::PreparedStatementNotFound)?;
        Ok(state.pending_parameters.take())
    }

    /// Close a prepared statement
    pub fn close_prepared_statement(&self, handle: StatementHandle) -> Result<(), ServerError> {
        let mut prepared = self
            .prepared_statements
            .lock()
            .expect("prepared_statements mutex poisoned");
        prepared
            .remove(&handle)
            .ok_or(ServerError::PreparedStatementNotFound)?;
        debug!(handle = %handle, "closed prepared statement");
        Ok(())
    }

    // === Transactions ===

    /// Begin a new transaction
    #[instrument(skip(self), fields(session_id = %self.id))]
    pub fn begin_transaction(&self) -> Result<TransactionId, ServerError> {
        self.touch();

        // Execute BEGIN TRANSACTION on the connection
        self.with_transaction_recovery(|| self.connection.execute_batch("BEGIN TRANSACTION"))?;

        let tx_id = self.transaction_id_gen.next();
        let mut transactions = self
            .transactions
            .lock()
            .expect("transactions mutex poisoned");
        transactions.insert(tx_id, Transaction::new(tx_id));

        debug!(transaction_id = %tx_id, "began transaction");
        Ok(tx_id)
    }

    /// Commit a transaction
    #[instrument(skip(self), fields(session_id = %self.id, transaction_id = %transaction_id))]
    pub fn commit_transaction(&self, transaction_id: TransactionId) -> Result<(), ServerError> {
        self.touch();

        // Verify transaction exists (without holding the lock during the commit)
        {
            let transactions = self
                .transactions
                .lock()
                .expect("transactions mutex poisoned");
            if !transactions.contains_key(&transaction_id) {
                // Check if it was aborted before treating as "not found"
                return self.transaction_absent_error(transaction_id);
            }
        }

        // Execute COMMIT on the connection
        self.with_transaction_recovery(|| self.connection.execute_batch("COMMIT"))?;

        let mut transactions = self
            .transactions
            .lock()
            .expect("transactions mutex poisoned");
        transactions.remove(&transaction_id);
        let mut aborted = self
            .aborted_transactions
            .lock()
            .expect("aborted_transactions mutex poisoned");
        aborted.remove(&transaction_id);
        debug!("committed transaction");
        Ok(())
    }

    /// Rollback a transaction
    #[instrument(skip(self), fields(session_id = %self.id, transaction_id = %transaction_id))]
    pub fn rollback_transaction(&self, transaction_id: TransactionId) -> Result<(), ServerError> {
        self.touch();

        // Verify transaction exists (without holding the lock during the rollback)
        {
            let transactions = self
                .transactions
                .lock()
                .expect("transactions mutex poisoned");
            if !transactions.contains_key(&transaction_id) {
                return self.transaction_absent_error(transaction_id);
            }
        }

        // Execute ROLLBACK on the connection
        self.with_transaction_recovery(|| self.connection.execute_batch("ROLLBACK"))?;

        let mut transactions = self
            .transactions
            .lock()
            .expect("transactions mutex poisoned");
        transactions.remove(&transaction_id);
        let mut aborted = self
            .aborted_transactions
            .lock()
            .expect("aborted_transactions mutex poisoned");
        aborted.remove(&transaction_id);
        debug!("rolled back transaction");
        Ok(())
    }
}
