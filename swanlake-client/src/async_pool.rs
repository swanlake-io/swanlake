use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use adbc_driver_manager::ManagedConnection;
use anyhow::{anyhow, Result};
use arrow_array::RecordBatch;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::driver::get_cached_driver;
use crate::internal::{
    begin_transaction, commit_transaction, execute_query, execute_query_with_params, execute_update,
    execute_update_with_batch, rollback_transaction, run_healthcheck,
};
use crate::pool::QueryOptions;
use crate::pool_shared::{evict_idle, IdleConnection, PoolState};
use crate::{PoolConfig, QueryResult, UpdateResult};

struct AsyncPoolInner {
    endpoint: String,
    config: PoolConfig,
    driver: Arc<crate::driver::CachedDriver>,
    state: Mutex<PoolState>,
    total: AtomicUsize,
    semaphore: Arc<Semaphore>,
}

impl AsyncPoolInner {
    async fn new(endpoint: &str, config: PoolConfig) -> Result<Self> {
        config.validate()?;
        let driver = get_cached_driver()?;
        let inner = Self {
            endpoint: endpoint.to_string(),
            semaphore: Arc::new(Semaphore::new(config.max_size)),
            config,
            driver,
            state: Mutex::new(PoolState { idle: Vec::new() }),
            total: AtomicUsize::new(0),
        };

        if inner.config.min_idle > 0 {
            for _ in 0..inner.config.min_idle {
                let conn = inner.new_connection().await?;
                inner.total.fetch_add(1, Ordering::SeqCst);
                let mut state = inner
                    .state
                    .lock()
                    .map_err(|e| anyhow!("Flight SQL async pool mutex poisoned: {}", e))?;
                state.idle.push(IdleConnection {
                    conn,
                    last_used: Instant::now(),
                });
            }
        }

        Ok(inner)
    }

    async fn new_connection(&self) -> Result<ManagedConnection> {
        let driver = self.driver.clone();
        let endpoint = self.endpoint.clone();
        tokio::task::spawn_blocking(move || driver.new_connection(&endpoint))
            .await
            .map_err(|e| anyhow!("connection task failed: {}", e))?
    }

    async fn acquire_connection(self: &Arc<Self>) -> Result<AsyncPooledConnection> {
        let timeout = Duration::from_millis(self.config.acquire_timeout_ms);
        let permit =
            match tokio::time::timeout(timeout, self.semaphore.clone().acquire_owned()).await
        {
            Ok(Ok(permit)) => permit,
            Ok(Err(_)) => {
                return Err(anyhow!(
                    "connection pool closed while waiting for permit"
                ))
            }
            Err(_) => {
                return Err(anyhow!(
                    "timed out waiting for pool connection (max_size={}, acquire_timeout_ms={})",
                    self.config.max_size,
                    self.config.acquire_timeout_ms
                ))
            }
        };

        let (idle_conn, should_create) = {
            let mut state = self
                .state
                .lock()
                .map_err(|e| anyhow!("Flight SQL async pool mutex poisoned: {}", e))?;
            self.evict_idle_locked(&mut state);
            if let Some(idle) = state.idle.pop() {
                (Some(idle.conn), false)
            } else {
                let total = self.total.load(Ordering::SeqCst);
                if total < self.config.max_size {
                    self.total.fetch_add(1, Ordering::SeqCst);
                    (None, true)
                } else {
                    (None, false)
                }
            }
        };

        if let Some(conn) = idle_conn {
            return Ok(AsyncPooledConnection::new(conn, self.clone(), permit));
        }

        if should_create {
            match self.new_connection().await {
                Ok(conn) => Ok(AsyncPooledConnection::new(conn, self.clone(), permit)),
                Err(err) => {
                    self.total.fetch_sub(1, Ordering::SeqCst);
                    Err(err)
                }
            }
        } else {
            Err(anyhow!(
                "connection pool exhausted (max_size={})",
                self.config.max_size
            ))
        }
    }

    fn release_connection(&self, conn: ManagedConnection) {
        let mut state = match self.state.lock() {
            Ok(state) => state,
            Err(_) => {
                self.total.fetch_sub(1, Ordering::SeqCst);
                return;
            }
        };
        self.evict_idle_locked(&mut state);
        state.idle.push(IdleConnection {
            conn,
            last_used: Instant::now(),
        });
    }

    fn drop_connection(&self, _conn: ManagedConnection) {
        self.total.fetch_sub(1, Ordering::SeqCst);
    }

    fn evict_idle_locked(&self, state: &mut PoolState) {
        let removed = evict_idle(state, self.config.idle_ttl_ms);
        if removed > 0 {
            self.total.fetch_sub(removed, Ordering::SeqCst);
        }
    }
}

struct AsyncPooledConnection {
    conn: Option<ManagedConnection>,
    pool: Arc<AsyncPoolInner>,
    _permit: Option<OwnedSemaphorePermit>,
    had_error: bool,
}

impl AsyncPooledConnection {
    fn new(conn: ManagedConnection, pool: Arc<AsyncPoolInner>, permit: OwnedSemaphorePermit) -> Self {
        Self {
            conn: Some(conn),
            pool,
            _permit: Some(permit),
            had_error: false,
        }
    }

    async fn with_connection<F, T>(&mut self, f: F) -> Result<T>
    where
        F: FnOnce(&mut ManagedConnection) -> Result<T> + Send + 'static,
        T: Send + 'static,
    {
        let conn = self
            .conn
            .take()
            .ok_or_else(|| anyhow!("pooled connection missing"))?;
        let join = tokio::task::spawn_blocking(move || {
            let mut conn = conn;
            let result = f(&mut conn);
            (conn, result)
        })
        .await;

        match join {
            Ok((conn, result)) => {
                self.conn = Some(conn);
                result
            }
            Err(err) => {
                self.had_error = true;
                self._permit = None;
                self.pool.total.fetch_sub(1, Ordering::SeqCst);
                Err(anyhow!("blocking task failed: {}", err))
            }
        }
    }

    async fn healthcheck(&mut self, sql: String) -> bool {
        let result = self
            .with_connection(move |conn| {
                run_healthcheck(conn, &sql)?;
                Ok(true)
            })
            .await;
        result.unwrap_or(false)
    }

    fn mark_error(&mut self) {
        self.had_error = true;
    }
}

impl Drop for AsyncPooledConnection {
    fn drop(&mut self) {
        if let Some(conn) = self.conn.take() {
            if self.had_error {
                self.pool.drop_connection(conn);
            } else {
                self.pool.release_connection(conn);
            }
        }
    }
}

/// Async session handle for stateful workflows (transactions, temp tables).
pub struct AsyncSessionHandle {
    pooled: AsyncPooledConnection,
}

impl AsyncSessionHandle {
    /// Execute a SQL query within this session.
    pub async fn query(&mut self, sql: &str) -> Result<QueryResult> {
        let sql = sql.to_string();
        self.pooled
            .with_connection(move |conn| execute_query(conn, &sql))
            .await
    }

    /// Execute an update/DDL statement within this session.
    pub async fn update(&mut self, sql: &str) -> Result<UpdateResult> {
        let sql = sql.to_string();
        self.pooled
            .with_connection(move |conn| execute_update(conn, &sql))
            .await
    }

    /// Begin a transaction by disabling autocommit.
    pub async fn begin_transaction(&mut self) -> Result<()> {
        self.pooled
            .with_connection(begin_transaction)
            .await
    }

    /// Commit the active transaction and re-enable autocommit.
    pub async fn commit(&mut self) -> Result<()> {
        self.pooled
            .with_connection(commit_transaction)
            .await
    }

    /// Roll back the active transaction and re-enable autocommit.
    pub async fn rollback(&mut self) -> Result<()> {
        self.pooled
            .with_connection(rollback_transaction)
            .await
    }
}

/// Async connection pool for Flight SQL sessions.
#[derive(Clone)]
pub struct AsyncFlightSQLPool {
    inner: Arc<AsyncPoolInner>,
}

impl AsyncFlightSQLPool {
    /// Create a new async pool for the given endpoint and configuration.
    pub async fn new(endpoint: &str, config: PoolConfig) -> Result<Self> {
        Ok(Self {
            inner: Arc::new(AsyncPoolInner::new(endpoint, config).await?),
        })
    }

    /// Create an async pool with default configuration.
    pub async fn with_default(endpoint: &str) -> Result<Self> {
        Self::new(endpoint, PoolConfig::default()).await
    }

    async fn acquire(&self) -> Result<AsyncPooledConnection> {
        self.inner.acquire_connection().await
    }

    /// Acquire an exclusive session handle for stateful workflows.
    pub async fn acquire_session(&self) -> Result<AsyncSessionHandle> {
        Ok(AsyncSessionHandle {
            pooled: self.acquire().await?,
        })
    }

    /// Execute a SQL query using any available pooled connection.
    pub async fn query(&self, sql: &str) -> Result<QueryResult> {
        self.query_with_options(sql, QueryOptions::default()).await
    }

    /// Execute a SQL statement and return results if any.
    pub async fn execute(&self, sql: &str) -> Result<QueryResult> {
        self.query(sql).await
    }

    /// Execute an update/DDL statement using any available pooled connection.
    pub async fn update(&self, sql: &str) -> Result<UpdateResult> {
        self.update_with_options(sql, QueryOptions::default()).await
    }

    /// Execute a parameterized query using a prepared statement.
    pub async fn query_with_param(&self, sql: &str, params: RecordBatch) -> Result<QueryResult> {
        self.query_with_param_and_options(sql, Some(params), QueryOptions::default())
            .await
    }

    /// Execute a batch update/insert using a prepared statement.
    pub async fn update_with_record_batch(
        &self,
        sql: &str,
        batch: RecordBatch,
    ) -> Result<UpdateResult> {
        self.update_with_batch_and_options(sql, Some(batch), QueryOptions::default())
            .await
    }

    /// Execute a query with explicit query options.
    pub async fn query_with_options(&self, sql: &str, opts: QueryOptions) -> Result<QueryResult> {
        self.query_with_param_and_options(sql, None, opts).await
    }

    /// Execute an update with explicit query options.
    pub async fn update_with_options(
        &self,
        sql: &str,
        opts: QueryOptions,
    ) -> Result<UpdateResult> {
        self.update_with_batch_and_options(sql, None, opts).await
    }

    async fn query_with_param_and_options(
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
        let params = params;
        let mut attempt = 0u8;

        loop {
            attempt += 1;
            let mut pooled = self.acquire().await?;
            let sql_owned = sql.to_string();
            let params_clone = params.clone();
            let result = pooled
                .with_connection(move |conn| match params_clone {
                    Some(batch) => execute_query_with_params(conn, &sql_owned, batch),
                    None => execute_query(conn, &sql_owned),
                })
                .await;

            match result {
                Ok(result) => return Ok(result),
                Err(err) => {
                    if retry_enabled && attempt == 1 {
                        let healthcheck_sql = self.inner.config.healthcheck_sql.clone();
                        let healthy = pooled.healthcheck(healthcheck_sql).await;
                        if !healthy {
                            pooled.mark_error();
                            continue;
                        }
                    }
                    pooled.mark_error();
                    return Err(err);
                }
            }
        }
    }

    async fn update_with_batch_and_options(
        &self,
        sql: &str,
        batch: Option<RecordBatch>,
        opts: QueryOptions,
    ) -> Result<UpdateResult> {
        let _timeout = opts
            .timeout_ms
            .or(self.inner.config.default_query_timeout_ms);
        let mut pooled = self.acquire().await?;
        let sql_owned = sql.to_string();
        let result = pooled
            .with_connection(move |conn| match batch {
                Some(batch) => execute_update_with_batch(conn, &sql_owned, batch),
                None => execute_update(conn, &sql_owned),
            })
            .await;
        match result {
            Ok(result) => Ok(result),
            Err(err) => {
                pooled.mark_error();
                Err(err)
            }
        }
    }
}
