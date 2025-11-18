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
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use arrow_schema::Schema;
use duckdb::types::Value;
use tracing::{debug, instrument};

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

    /// Execute a SELECT query
    #[instrument(skip(self), fields(session_id = %self.id, sql = %sql))]
    pub fn execute_query(&self, sql: &str) -> Result<QueryResult, ServerError> {
        self.touch();
        self.connection.execute_query(sql)
    }

    /// Execute a query with parameters
    #[instrument(skip(self, params), fields(session_id = %self.id, sql = %sql))]
    pub fn execute_query_with_params(
        &self,
        sql: &str,
        params: &[Value],
    ) -> Result<QueryResult, ServerError> {
        self.touch();
        self.connection.execute_query_with_params(sql, params)
    }

    /// Execute a statement (DDL/DML)
    #[instrument(skip(self), fields(session_id = %self.id, sql = %sql))]
    pub fn execute_statement(&self, sql: &str) -> Result<i64, ServerError> {
        // Check if this is a DQ admin command first
        if let Some(result) = self.try_handle_dq_command(sql)? {
            return Ok(result);
        }
        if let Some(result) = self.try_handle_dq_insert(sql, None)? {
            return Ok(result);
        }

        self.touch();
        self.connection.execute_statement(sql)
    }

    /// Try to handle duckling queue administrative commands.
    /// Returns Some(affected_rows) if it's a DQ command, None otherwise.
    fn try_handle_dq_command(&self, sql: &str) -> Result<Option<i64>, ServerError> {
        let trimmed = sql.trim();
        let normalized = trimmed.trim_end_matches(';').trim().to_ascii_lowercase();

        if normalized == "pragma duckling_queue.flush"
            || normalized == "call duckling_queue_flush()"
            || normalized == "pragma duckling_queue.cleanup"
            || normalized == "call duckling_queue_cleanup()"
        {
            let dq = self
                .dq_coordinator
                .as_ref()
                .ok_or_else(|| ServerError::Internal("duckling queue is not enabled".into()))?;
            dq.force_flush_all();
            Ok(Some(0))
        } else {
            Ok(None)
        }
    }

    fn try_handle_dq_insert(
        &self,
        sql: &str,
        params: Option<&[Value]>,
    ) -> Result<Option<i64>, ServerError> {
        let parsed = match ParsedStatement::parse(sql) {
            Some(stmt) if stmt.is_insert() => stmt,
            _ => return Ok(None),
        };

        let table_ref = match parsed.get_insert_table() {
            Some(table) => table,
            None => return Ok(None),
        };

        let parts = table_ref.parts();
        if parts.len() != 2 || !parts[0].eq_ignore_ascii_case("duckling_queue") {
            return Ok(None);
        }

        let source_sql = match parsed.insert_source_sql() {
            Some(sql) => sql,
            None => return Ok(None),
        };

        let dq = self
            .dq_coordinator
            .as_ref()
            .ok_or_else(|| ServerError::Internal("duckling queue is not enabled".into()))?;

        let result = if let Some(values) = params {
            self.connection
                .execute_query_with_params(&source_sql, values)?
        } else {
            self.connection.execute_query(&source_sql)?
        };

        if result.total_rows == 0 {
            return Ok(Some(0));
        }

        let target_table = parts[1].clone();
        let QueryResult {
            schema,
            batches,
            total_rows,
            ..
        } = result;
        dq.enqueue(&target_table, schema, batches)?;
        Ok(Some(total_rows as i64))
    }

    /// Execute a statement with parameters
    #[instrument(skip(self, params), fields(session_id = %self.id, sql = %sql))]
    pub fn execute_statement_with_params(
        &self,
        sql: &str,
        params: &[Value],
    ) -> Result<usize, ServerError> {
        if let Some(result) = self.try_handle_dq_command(sql)? {
            return Ok(result as usize);
        }
        if let Some(result) = self.try_handle_dq_insert(sql, Some(params))? {
            return Ok(result as usize);
        }

        self.touch();
        self.connection.execute_statement_with_params(sql, params)
    }

    /// Get schema for a query
    #[instrument(skip(self), fields(session_id = %self.id, sql = %sql))]
    pub fn schema_for_query(&self, sql: &str) -> Result<arrow_schema::Schema, ServerError> {
        self.touch();
        self.connection.schema_for_query(sql)
    }

    /// Insert data using appender API with RecordBatch.
    ///
    /// This is an optimized path for INSERT statements that avoids
    /// converting RecordBatch to individual parameter values.
    #[instrument(skip(self, batch), fields(session_id = %self.id, table_name = %table_name, rows = batch.num_rows()))]
    pub fn insert_with_appender(
        &self,
        table_name: &str,
        batch: arrow_array::RecordBatch,
    ) -> Result<usize, ServerError> {
        self.touch();
        self.connection.insert_with_appender(table_name, batch)
    }

    /// Get the schema of a table
    pub fn table_schema(&self, table_name: &str) -> Result<arrow_schema::Schema, ServerError> {
        self.connection.table_schema(table_name)
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
        self.connection.execute_batch("BEGIN TRANSACTION")?;

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

        // Verify transaction exists
        let mut transactions = self
            .transactions
            .lock()
            .expect("transactions mutex poisoned");
        if !transactions.contains_key(&transaction_id) {
            return Err(ServerError::TransactionNotFound);
        }

        // Execute COMMIT on the connection
        self.connection.execute_batch("COMMIT")?;

        transactions.remove(&transaction_id);
        debug!("committed transaction");
        Ok(())
    }

    /// Rollback a transaction
    #[instrument(skip(self), fields(session_id = %self.id, transaction_id = %transaction_id))]
    pub fn rollback_transaction(&self, transaction_id: TransactionId) -> Result<(), ServerError> {
        self.touch();

        // Verify transaction exists
        let mut transactions = self
            .transactions
            .lock()
            .expect("transactions mutex poisoned");
        if !transactions.contains_key(&transaction_id) {
            return Err(ServerError::TransactionNotFound);
        }

        // Execute ROLLBACK on the connection
        self.connection.execute_batch("ROLLBACK")?;

        transactions.remove(&transaction_id);
        debug!("rolled back transaction");
        Ok(())
    }
}
