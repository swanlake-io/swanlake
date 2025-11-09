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
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use duckdb::types::Value;
use tracing::{debug, instrument};

use crate::dq::{QueueManager, QueueSession};
use crate::engine::{DuckDbConnection, QueryResult};
use crate::error::ServerError;
use crate::session::id::{
    StatementHandle, StatementHandleGenerator, TransactionId, TransactionIdGenerator,
};

/// Metadata for a prepared statement
#[derive(Debug, Clone)]
pub struct PreparedStatementMeta {
    pub sql: String,
    pub is_query: bool,
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
    // Duckling Queue support
    dq_manager: Option<Arc<QueueManager>>,
    dq_queue: Arc<Mutex<Option<QueueSession>>>,
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
            dq_manager: None,
            dq_queue: Arc::new(Mutex::new(None)),
        }
    }

    /// Create a new session with duckling queue support.
    /// The queue is attached immediately on session creation.
    #[instrument(skip(connection, dq_manager))]
    pub fn new_with_id_and_dq(
        id: SessionId,
        connection: DuckDbConnection,
        dq_manager: Arc<QueueManager>,
    ) -> Result<Self, ServerError> {
        debug!(session_id = %id, "creating new session with duckling queue support");

        // Create and attach queue immediately
        let sq = dq_manager
            .open_session_queue(id.clone())
            .map_err(|e| ServerError::Internal(format!("failed to create session queue: {}", e)))?;

        let sql = sq.attach_sql();
        connection
            .execute_batch(&sql)
            .map_err(|e| ServerError::Internal(format!("failed to attach session queue: {}", e)))?;

        Ok(Self {
            id,
            connection,
            transactions: Arc::new(Mutex::new(HashMap::new())),
            prepared_statements: Arc::new(Mutex::new(HashMap::new())),
            transaction_id_gen: Arc::new(TransactionIdGenerator::new()),
            statement_handle_gen: Arc::new(StatementHandleGenerator::new()),
            last_activity: Arc::new(Mutex::new(Instant::now())),
            dq_manager: Some(dq_manager),
            dq_queue: Arc::new(Mutex::new(Some(sq))),
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
        {
            self.force_flush_own_queue()?;
            Ok(Some(0))
        } else if normalized == "pragma duckling_queue.cleanup"
            || normalized == "call duckling_queue_cleanup()"
        {
            let dq_manager = self
                .dq_manager
                .as_ref()
                .ok_or_else(|| ServerError::Internal("no duckling queue manager".into()))?;
            dq_manager.cleanup_flushed_files().map_err(|e| {
                ServerError::Internal(format!("failed to cleanup flushed files: {}", e))
            })?;
            Ok(Some(0))
        } else {
            Ok(None)
        }
    }

    /// Force flush: rotate this session's queue and flush the sealed file immediately.
    fn force_flush_own_queue(&self) -> Result<(), ServerError> {
        // Rotate the current session's queue file
        let sealed_path = self.force_rotate_queue()?;

        // Flush only this session's sealed file synchronously
        let dq_manager = self
            .dq_manager
            .as_ref()
            .ok_or_else(|| ServerError::Internal("no duckling queue manager".into()))?
            .clone();

        // Use session's own connection to flush (it's already in a spawn_blocking context)
        self.connection
            .with_inner(|conn| {
                crate::dq::runtime::flush_sealed_file(&dq_manager, conn, &sealed_path)
            })
            .map_err(|e| ServerError::Internal(format!("failed to flush queue file: {}", e)))?;

        // Re-attach the active queue to ensure duckling_queue is always available
        if let Some(sq) = &*self.dq_queue.lock().expect("dq_queue mutex poisoned") {
            self.connection
                .execute_batch(&sq.attach_sql())
                .map_err(|e| {
                    ServerError::Internal(format!("failed to re-attach active queue: {}", e))
                })?;
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
        self.touch();
        self.connection.execute_statement_with_params(sql, params)
    }

    /// Get schema for a query
    #[instrument(skip(self), fields(session_id = %self.id, sql = %sql))]
    pub fn schema_for_query(&self, sql: &str) -> Result<arrow_schema::Schema, ServerError> {
        self.touch();
        self.connection.schema_for_query(sql)
    }

    // === Prepared Statements ===

    /// Create a prepared statement and return its handle
    #[instrument(skip(self), fields(session_id = %self.id, sql = %sql))]
    pub fn create_prepared_statement(
        &self,
        sql: String,
        is_query: bool,
    ) -> Result<StatementHandle, ServerError> {
        self.touch();

        let handle = self.statement_handle_gen.next();
        let meta = PreparedStatementMeta { sql, is_query };

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

    // === Duckling Queue ===

    /// Check if rotation is needed and rotate if necessary.
    pub fn maybe_rotate_queue(&self) -> Result<(), ServerError> {
        let mut queue = self.dq_queue.lock().expect("dq_queue mutex poisoned");

        if let Some(ref mut sq) = *queue {
            let should_rotate = sq
                .should_rotate()
                .map_err(|e| ServerError::Internal(format!("failed to check rotation: {}", e)))?;

            if should_rotate {
                let sealed = sq
                    .rotate(&self.connection)
                    .map_err(|e| ServerError::Internal(format!("failed to rotate queue: {}", e)))?;
                debug!(session_id = %self.id, sealed_file = %sealed.display(), "session queue rotated");
            }
        }

        Ok(())
    }

    /// Force flush: rotate queue and return sealed file path.
    pub fn force_rotate_queue(&self) -> Result<std::path::PathBuf, ServerError> {
        let mut queue = self.dq_queue.lock().expect("dq_queue mutex poisoned");

        if let Some(ref mut sq) = *queue {
            sq.force_flush(&self.connection)
                .map_err(|e| ServerError::Internal(format!("failed to force flush: {}", e)))
        } else {
            Err(ServerError::Internal("no active queue to flush".into()))
        }
    }

    /// Seal queue file before session cleanup.
    pub fn cleanup_queue(&self) -> Result<(), ServerError> {
        let queue = self
            .dq_queue
            .lock()
            .expect("dq_queue mutex poisoned")
            .take();

        if let Some(sq) = queue {
            sq.seal_on_cleanup().map_err(|e| {
                ServerError::Internal(format!("failed to seal queue on cleanup: {}", e))
            })?;
            debug!(session_id = %self.id, "session queue sealed on cleanup");
        }

        Ok(())
    }

    /// Get session ID (for use by registry).
    pub fn id(&self) -> SessionId {
        self.id.clone()
    }
}
