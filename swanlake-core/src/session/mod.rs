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

use std::collections::{HashMap, HashSet};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use arrow_schema::Schema;
use duckdb::types::Value;
use tracing::{debug, info, instrument, warn};

use crate::engine::{DuckDbConnection, QueryResult};
use crate::error::ServerError;
use crate::session::id::{
    StatementHandle, StatementHandleGenerator, TransactionId, TransactionIdGenerator,
};

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

/// A client session with dedicated connection and state
pub struct Session {
    id: SessionId,
    connection: DuckDbConnection,
    transactions: Mutex<HashSet<TransactionId>>,
    aborted_transactions: Mutex<HashSet<TransactionId>>,
    prepared_statements: Mutex<HashMap<StatementHandle, PreparedStatementState>>,
    transaction_id_gen: TransactionIdGenerator,
    statement_handle_gen: StatementHandleGenerator,
    last_activity: Mutex<Instant>,
}

impl Session {
    /// Create a new session with a specific ID (for connection-based persistence)
    #[instrument(skip(connection))]
    pub fn new_with_id(id: SessionId, connection: DuckDbConnection) -> Self {
        debug!(session_id = %id, "created new session with specific ID");

        Self {
            id,
            connection,
            transactions: Mutex::new(HashSet::new()),
            aborted_transactions: Mutex::new(HashSet::new()),
            prepared_statements: Mutex::new(HashMap::new()),
            transaction_id_gen: TransactionIdGenerator::new(),
            statement_handle_gen: StatementHandleGenerator::new(),
            last_activity: Mutex::new(Instant::now()),
        }
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
    fn with_transaction_recovery<T, F>(
        &self,
        mut op: F,
        retry_on_abort: bool,
    ) -> Result<T, ServerError>
    where
        F: FnMut() -> Result<T, ServerError>,
    {
        match op() {
            Ok(value) => Ok(value),
            Err(err) => {
                if Self::is_transaction_abort_error(&err) {
                    self.recover_from_transaction_abort(&err);
                    if retry_on_abort {
                        // Retry once after rollback
                        op()
                    } else {
                        Err(err)
                    }
                } else {
                    Err(err)
                }
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
                    let ids = txs.iter().copied().collect::<Vec<_>>();
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

    fn transaction_absent(&self, transaction_id: TransactionId) -> Result<(), ServerError> {
        let mut aborted = self
            .aborted_transactions
            .lock()
            .expect("aborted_transactions mutex poisoned");
        if aborted.remove(&transaction_id) {
            return Err(ServerError::TransactionAborted);
        }

        debug!(
            transaction_id = %transaction_id,
            "transaction not found; treating as no-op"
        );
        Ok(())
    }

    /// Execute a SELECT query
    #[instrument(skip(self), fields(session_id = %self.id, sql = %sql))]
    pub fn execute_query(&self, sql: &str) -> Result<QueryResult, ServerError> {
        self.touch();
        self.with_transaction_recovery(|| self.connection.execute_query(sql), true)
    }

    /// Execute a query with parameters
    #[instrument(skip(self, params), fields(session_id = %self.id, sql = %sql))]
    pub fn execute_query_with_params(
        &self,
        sql: &str,
        params: &[Value],
    ) -> Result<QueryResult, ServerError> {
        self.touch();
        self.with_transaction_recovery(
            || self.connection.execute_query_with_params(sql, params),
            true,
        )
    }

    /// Execute a statement (DDL/DML)
    #[instrument(skip(self), fields(session_id = %self.id, sql = %sql))]
    pub fn execute_statement(&self, sql: &str) -> Result<i64, ServerError> {
        self.touch();
        self.with_transaction_recovery(|| self.connection.execute_statement(sql), true)
    }

    /// Execute a statement with parameters
    #[instrument(skip(self, params), fields(session_id = %self.id, sql = %sql))]
    pub fn execute_statement_with_params(
        &self,
        sql: &str,
        params: &[Value],
    ) -> Result<usize, ServerError> {
        self.touch();
        self.with_transaction_recovery(
            || self.connection.execute_statement_with_params(sql, params),
            true,
        )
    }

    /// Get schema for a query
    #[instrument(skip(self), fields(session_id = %self.id, sql = %sql))]
    pub fn schema_for_query(&self, sql: &str) -> Result<arrow_schema::Schema, ServerError> {
        self.touch();
        self.with_transaction_recovery(|| self.connection.schema_for_query(sql), true)
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
        self.with_transaction_recovery(
            || {
                self.connection
                    .insert_with_appender(catalog_name, table_name, batches.clone())
            },
            false,
        )
    }

    /// Get the schema of a table
    pub fn table_schema(&self, table_name: &str) -> Result<arrow_schema::Schema, ServerError> {
        self.with_transaction_recovery(|| self.connection.table_schema(table_name), true)
    }

    /// Return the current catalog selected for this session.
    pub fn current_catalog(&self) -> Result<String, ServerError> {
        self.with_transaction_recovery(|| self.connection.current_catalog(), true)
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
        self.with_transaction_recovery(
            || self.connection.execute_batch("BEGIN TRANSACTION"),
            true,
        )?;

        let tx_id = self.transaction_id_gen.next();
        let mut transactions = self
            .transactions
            .lock()
            .expect("transactions mutex poisoned");
        transactions.insert(tx_id);

        debug!(transaction_id = %tx_id, "began transaction");
        Ok(tx_id)
    }

    /// Commit a transaction
    #[instrument(skip(self), fields(session_id = %self.id, transaction_id = %transaction_id))]
    pub fn commit_transaction(&self, transaction_id: TransactionId) -> Result<(), ServerError> {
        self.end_transaction(transaction_id, "COMMIT", "committed")
    }

    /// Rollback a transaction
    #[instrument(skip(self), fields(session_id = %self.id, transaction_id = %transaction_id))]
    pub fn rollback_transaction(&self, transaction_id: TransactionId) -> Result<(), ServerError> {
        self.end_transaction(transaction_id, "ROLLBACK", "rolled back")
    }

    fn end_transaction(
        &self,
        transaction_id: TransactionId,
        sql: &str,
        op_name: &str,
    ) -> Result<(), ServerError> {
        self.touch();

        // Verify transaction exists (without holding the lock during the commit/rollback)
        {
            let transactions = self
                .transactions
                .lock()
                .expect("transactions mutex poisoned");
            if !transactions.contains(&transaction_id) {
                return self.transaction_absent(transaction_id);
            }
        }

        // Execute COMMIT/ROLLBACK on the connection
        self.with_transaction_recovery(|| self.connection.execute_batch(sql), true)?;

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
        debug!(
            transaction_id = %transaction_id,
            operation = op_name,
            "completed transaction"
        );
        Ok(())
    }
}
