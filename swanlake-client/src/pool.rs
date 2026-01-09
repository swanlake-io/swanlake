use std::marker::PhantomData;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use adbc_core::{Connection, Statement};
use adbc_driver_manager::{ManagedConnection, ManagedStatement};
use anyhow::{anyhow, Result};
use arrow_array::RecordBatch;

use crate::driver::get_cached_driver;
use crate::internal::{
    begin_transaction, commit_transaction, execute_query, execute_query_with_params,
    execute_statement_query, execute_update, execute_update_with_batch, rollback_transaction,
    run_healthcheck,
};
use crate::pool_shared::{evict_idle, IdleConnection, PoolState};
use crate::{QueryResult, UpdateResult};

/// Configuration options for the Flight SQL connection pool.
#[derive(Debug, Clone)]
pub struct PoolConfig {
    pub min_idle: usize,
    pub max_size: usize,
    pub acquire_timeout_ms: u64,
    pub idle_ttl_ms: u64,
    pub healthcheck_sql: String,
    /// Reserved for future timeout support in the sync pool.
    pub default_query_timeout_ms: Option<u64>,
    pub retry_on_failure: bool,
}

impl PoolConfig {
    pub(crate) fn validate(&self) -> Result<()> {
        if self.max_size == 0 {
            return Err(anyhow!("PoolConfig.max_size must be greater than 0"));
        }
        if self.min_idle > self.max_size {
            return Err(anyhow!(
                "PoolConfig.min_idle ({}) exceeds max_size ({})",
                self.min_idle,
                self.max_size
            ));
        }
        Ok(())
    }
}

impl Default for PoolConfig {
    fn default() -> Self {
        let cpus = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1);
        let scaled = cpus.saturating_mul(2);
        let max_size = scaled.clamp(4, 16);
        Self {
            min_idle: 1,
            max_size,
            acquire_timeout_ms: 30_000,
            idle_ttl_ms: 300_000,
            healthcheck_sql: "SELECT 1".to_string(),
            default_query_timeout_ms: None,
            retry_on_failure: true,
        }
    }
}

/// Per-query overrides for pooled calls.
#[derive(Debug, Clone, Default)]
pub struct QueryOptions {
    /// Reserved for future timeout support in the sync pool.
    pub timeout_ms: Option<u64>,
    pub retry_on_failure: Option<bool>,
}

impl QueryOptions {
    /// Create a new set of query options with defaults.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set a per-query timeout (reserved for future sync timeout support).
    pub fn with_timeout_ms(mut self, timeout_ms: u64) -> Self {
        self.timeout_ms = Some(timeout_ms);
        self
    }

    /// Override retry behavior for idempotent queries.
    pub fn with_retry_on_failure(mut self, retry_on_failure: bool) -> Self {
        self.retry_on_failure = Some(retry_on_failure);
        self
    }
}

struct PoolInner {
    endpoint: String,
    config: PoolConfig,
    driver: Arc<crate::driver::CachedDriver>,
    state: Mutex<PoolState>,
    condvar: Condvar,
    total: AtomicUsize,
}

impl PoolInner {
    fn new(endpoint: &str, config: PoolConfig) -> Result<Self> {
        config.validate()?;
        let driver = get_cached_driver()?;
        let inner = Self {
            endpoint: endpoint.to_string(),
            config,
            driver,
            state: Mutex::new(PoolState { idle: Vec::new() }),
            condvar: Condvar::new(),
            total: AtomicUsize::new(0),
        };

        if inner.config.min_idle > 0 {
            let mut warm = Vec::with_capacity(inner.config.min_idle);
            for _ in 0..inner.config.min_idle {
                warm.push(inner.new_connection()?);
            }
            let mut state = inner
                .state
                .lock()
                .map_err(|e| anyhow!("Flight SQL pool mutex poisoned: {}", e))?;
            for conn in warm {
                inner.total.fetch_add(1, Ordering::SeqCst);
                state.idle.push(IdleConnection {
                    conn,
                    last_used: Instant::now(),
                });
            }
        }

        Ok(inner)
    }

    fn new_connection(&self) -> Result<ManagedConnection> {
        self.driver.new_connection(&self.endpoint)
    }

    fn acquire_connection(&self) -> Result<ManagedConnection> {
        let timeout = Duration::from_millis(self.config.acquire_timeout_ms);
        let deadline = Instant::now()
            .checked_add(timeout)
            .ok_or_else(|| anyhow!("acquire timeout overflow"))?;
        let mut state = self
            .state
            .lock()
            .map_err(|e| anyhow!("Flight SQL pool mutex poisoned: {}", e))?;

        loop {
            self.evict_idle_locked(&mut state);

            if let Some(idle) = state.idle.pop() {
                return Ok(idle.conn);
            }

            let total = self.total.load(Ordering::SeqCst);
            if total < self.config.max_size {
                self.total.fetch_add(1, Ordering::SeqCst);
                drop(state);
                match self.new_connection() {
                    Ok(conn) => return Ok(conn),
                    Err(err) => {
                        self.total.fetch_sub(1, Ordering::SeqCst);
                        return Err(err);
                    }
                }
            }

            let now = Instant::now();
            if now >= deadline {
                return Err(anyhow!(
                    "timed out waiting for pool connection (max_size={}, acquire_timeout_ms={})",
                    self.config.max_size,
                    self.config.acquire_timeout_ms
                ));
            }
            let remaining = deadline.saturating_duration_since(now);
            let (guard, wait_result) = self
                .condvar
                .wait_timeout(state, remaining)
                .map_err(|e| anyhow!("Flight SQL pool mutex poisoned: {}", e))?;
            state = guard;
            if wait_result.timed_out() {
                return Err(anyhow!(
                    "timed out waiting for pool connection (max_size={}, acquire_timeout_ms={})",
                    self.config.max_size,
                    self.config.acquire_timeout_ms
                ));
            }
        }
    }

    fn release_connection(&self, mut conn: ManagedConnection, had_error: bool) {
        if had_error && !self.healthcheck(&mut conn) {
            self.drop_connection(conn);
            return;
        }

        let mut state = match self.state.lock() {
            Ok(state) => state,
            Err(_) => {
                self.drop_connection(conn);
                return;
            }
        };
        self.evict_idle_locked(&mut state);
        state.idle.push(IdleConnection {
            conn,
            last_used: Instant::now(),
        });
        self.condvar.notify_one();
    }

    fn drop_connection(&self, _conn: ManagedConnection) {
        self.total.fetch_sub(1, Ordering::SeqCst);
    }

    fn healthcheck(&self, conn: &mut ManagedConnection) -> bool {
        let sql = self.config.healthcheck_sql.trim();
        if sql.is_empty() {
            return true;
        }
        run_healthcheck(conn, sql).is_ok()
    }

    fn evict_idle_locked(&self, state: &mut PoolState) {
        let removed = evict_idle(state, self.config.idle_ttl_ms);
        if removed > 0 {
            self.total.fetch_sub(removed, Ordering::SeqCst);
        }
    }
}

/// RAII wrapper returning a pooled connection to the pool on drop.
///
/// Use this when you need access to the underlying ADBC connection for
/// custom statements.
pub struct PooledConnection {
    conn: Option<ManagedConnection>,
    pool: Arc<PoolInner>,
    had_error: bool,
}

impl PooledConnection {
    fn new(conn: ManagedConnection, pool: Arc<PoolInner>) -> Self {
        Self {
            conn: Some(conn),
            pool,
            had_error: false,
        }
    }

    /// Get mutable access to the underlying managed connection.
    pub fn connection(&mut self) -> &mut ManagedConnection {
        self.conn.as_mut().expect("pooled connection missing")
    }

    fn mark_error(&mut self) {
        self.had_error = true;
    }

    fn take(&mut self) -> Option<ManagedConnection> {
        self.conn.take()
    }
}

impl Drop for PooledConnection {
    fn drop(&mut self) {
        if let Some(conn) = self.conn.take() {
            self.pool.release_connection(conn, self.had_error);
        }
    }
}

/// Exclusive session handle for stateful workflows (transactions, temp tables).
pub struct SessionHandle {
    pooled: PooledConnection,
}

impl SessionHandle {
    /// Execute a SQL query within this session.
    pub fn query(&mut self, sql: &str) -> Result<QueryResult> {
        execute_query(self.pooled.connection(), sql)
    }

    /// Execute an update/DDL statement within this session.
    pub fn update(&mut self, sql: &str) -> Result<UpdateResult> {
        execute_update(self.pooled.connection(), sql)
    }

    /// Prepare a query tied to this session.
    pub fn prepare_query<'a>(&'a mut self, sql: &str) -> Result<PreparedQuery<'a>> {
        let mut stmt = self.pooled.connection().new_statement()?;
        stmt.set_sql_query(sql)?;
        stmt.prepare()?;
        Ok(PreparedQuery {
            stmt,
            _session: PhantomData,
        })
    }

    /// Begin a transaction by disabling autocommit.
    pub fn begin_transaction(&mut self) -> Result<()> {
        begin_transaction(self.pooled.connection())
    }

    /// Commit the active transaction and re-enable autocommit.
    pub fn commit(&mut self) -> Result<()> {
        commit_transaction(self.pooled.connection())
    }

    /// Roll back the active transaction and re-enable autocommit.
    pub fn rollback(&mut self) -> Result<()> {
        rollback_transaction(self.pooled.connection())
    }

    /// Get mutable access to the underlying managed connection.
    pub fn connection(&mut self) -> &mut ManagedConnection {
        self.pooled.connection()
    }
}

/// Prepared statement handle tied to the session lifetime.
pub struct PreparedQuery<'a> {
    stmt: ManagedStatement,
    _session: PhantomData<&'a mut SessionHandle>,
}

impl<'a> PreparedQuery<'a> {
    /// Bind parameters for the prepared statement.
    pub fn bind(&mut self, params: RecordBatch) -> Result<()> {
        self.stmt.bind(params)?;
        Ok(())
    }

    /// Execute the prepared query and return result batches.
    pub fn query(&mut self) -> Result<QueryResult> {
        execute_statement_query(&mut self.stmt)
    }

    /// Execute the prepared query (alias of `query`).
    pub fn execute(&mut self) -> Result<QueryResult> {
        self.query()
    }

    /// Execute the prepared statement as an update/DDL.
    pub fn update(&mut self) -> Result<UpdateResult> {
        let rows_affected = self.stmt.execute_update()?;
        Ok(UpdateResult { rows_affected })
    }
}

/// Synchronous connection pool for Flight SQL sessions.
#[derive(Clone)]
pub struct FlightSQLPool {
    inner: Arc<PoolInner>,
}

impl FlightSQLPool {
    /// Create a new pool for the given endpoint and configuration.
    pub fn new(endpoint: &str, config: PoolConfig) -> Result<Self> {
        Ok(Self {
            inner: Arc::new(PoolInner::new(endpoint, config)?),
        })
    }

    /// Create a pool with default configuration.
    pub fn with_default(endpoint: &str) -> Result<Self> {
        Self::new(endpoint, PoolConfig::default())
    }

    /// Acquire a pooled connection handle.
    pub fn acquire(&self) -> Result<PooledConnection> {
        let conn = self.inner.acquire_connection()?;
        Ok(PooledConnection::new(conn, self.inner.clone()))
    }

    /// Acquire an exclusive session handle for stateful workflows.
    pub fn acquire_session(&self) -> Result<SessionHandle> {
        Ok(SessionHandle {
            pooled: self.acquire()?,
        })
    }

    /// Execute a SQL query using any available pooled connection.
    pub fn query(&self, sql: &str) -> Result<QueryResult> {
        self.query_with_options(sql, QueryOptions::default())
    }

    /// Execute a SQL statement and return results if any.
    pub fn execute(&self, sql: &str) -> Result<QueryResult> {
        self.query(sql)
    }

    /// Execute an update/DDL statement using any available pooled connection.
    pub fn update(&self, sql: &str) -> Result<UpdateResult> {
        self.update_with_options(sql, QueryOptions::default())
    }

    /// Execute a parameterized query using a prepared statement.
    pub fn query_with_param(&self, sql: &str, params: RecordBatch) -> Result<QueryResult> {
        self.query_with_param_and_options(sql, Some(params), QueryOptions::default())
    }

    /// Execute a batch update/insert using a prepared statement.
    pub fn update_with_record_batch(&self, sql: &str, batch: RecordBatch) -> Result<UpdateResult> {
        self.update_with_batch_and_options(sql, Some(batch), QueryOptions::default())
    }

    /// Execute a query with explicit query options.
    pub fn query_with_options(&self, sql: &str, opts: QueryOptions) -> Result<QueryResult> {
        self.query_with_param_and_options(sql, None, opts)
    }

    /// Execute an update with explicit query options.
    pub fn update_with_options(&self, sql: &str, opts: QueryOptions) -> Result<UpdateResult> {
        self.update_with_batch_and_options(sql, None, opts)
    }

    fn query_with_param_and_options(
        &self,
        sql: &str,
        params: Option<RecordBatch>,
        opts: QueryOptions,
    ) -> Result<QueryResult> {
        let retry_enabled = opts
            .retry_on_failure
            .unwrap_or(self.inner.config.retry_on_failure);
        let _timeout = opts
            .timeout_ms
            .or(self.inner.config.default_query_timeout_ms);

        let mut pooled = self.acquire()?;
        let mut attempt = 0u8;
        loop {
            attempt += 1;
            let result = match &params {
                Some(batch) => execute_query_with_params(pooled.connection(), sql, batch.clone()),
                None => execute_query(pooled.connection(), sql),
            };

            match result {
                Ok(result) => return Ok(result),
                Err(err) => {
                    if retry_enabled && attempt == 1 {
                        let healthy = self.inner.healthcheck(pooled.connection());
                        if !healthy {
                            if let Some(conn) = pooled.take() {
                                self.inner.drop_connection(conn);
                            }
                            pooled = self.acquire()?;
                            continue;
                        }
                    }
                    pooled.mark_error();
                    return Err(err);
                }
            }
        }
    }

    fn update_with_batch_and_options(
        &self,
        sql: &str,
        batch: Option<RecordBatch>,
        opts: QueryOptions,
    ) -> Result<UpdateResult> {
        let _timeout = opts
            .timeout_ms
            .or(self.inner.config.default_query_timeout_ms);

        let mut pooled = self.acquire()?;
        let result = match batch {
            Some(batch) => execute_update_with_batch(pooled.connection(), sql, batch),
            None => execute_update(pooled.connection(), sql),
        };
        match result {
            Ok(result) => Ok(result),
            Err(err) => {
                pooled.mark_error();
                Err(err)
            }
        }
    }
}
