# SwanLake Client Connection Pool Design

## Problem Statement
SwanLake sessions are tied to the gRPC connection (remote_addr). Each session
owns one DuckDB connection, and DuckDB is guarded by a mutex on the server.
That means a single client connection executes statements sequentially.

To provide parallelism without requiring users to manage multiple client
connections manually, we need a connection pool in `swanlake-client`.

## Goals
- Provide parallel query/update execution by using multiple gRPC connections.
- Preserve session semantics when needed (prepared statements, transactions,
  temp tables, session-scoped settings).
- Keep a simple API that works for most users without extra boilerplate.
- Be safe by default: if a connection is unhealthy, drop and replace it.
- Provide async APIs that integrate with tokio without blocking user tasks.
- Support best-effort cancellation and timeouts.

## Non-Goals
- Cross-connection transactions (impossible without server-side changes).
- Guarantee cancellation on every backend (best-effort only).

## Key Constraints
- ADBC `ManagedConnection` is the unit of session state.
- Prepared statements are tied to a specific connection/session.
- Parallelism requires multiple connections; per-connection work is sequential.

## Proposed Public API

### Pool Types
```
pub struct FlightSQLPool { ... }
pub struct AsyncFlightSQLPool { ... }
pub struct PoolConfig { ... }
pub struct PooledConnection { ... }   // RAII return-to-pool
pub struct SessionHandle { ... }      // Exclusive connection for stateful flows
pub struct AsyncSessionHandle { ... } // Async counterpart
pub struct QueryOptions { ... }       // timeout, retry, cancel behavior
```

### Constructors
```
impl FlightSQLPool {
    pub fn new(endpoint: &str, config: PoolConfig) -> Result<Self>;
    pub fn with_default(endpoint: &str) -> Result<Self>;
}

impl AsyncFlightSQLPool {
    pub async fn new(endpoint: &str, config: PoolConfig) -> Result<Self>;
    pub async fn with_default(endpoint: &str) -> Result<Self>;
}
```

### Stateless Convenience APIs (parallel safe)
```
impl FlightSQLPool {
    pub fn query(&self, sql: &str) -> Result<QueryResult>;
    pub fn execute(&self, sql: &str) -> Result<QueryResult>;
    pub fn update(&self, sql: &str) -> Result<UpdateResult>;
    pub fn query_with_param(&self, sql: &str, params: RecordBatch) -> Result<QueryResult>;
    pub fn update_with_record_batch(&self, sql: &str, batch: RecordBatch) -> Result<UpdateResult>;
    pub fn query_with_options(&self, sql: &str, opts: QueryOptions) -> Result<QueryResult>;
    pub fn update_with_options(&self, sql: &str, opts: QueryOptions) -> Result<UpdateResult>;
}
```

### Stateful API (session affinity)
```
impl FlightSQLPool {
    pub fn acquire_session(&self) -> Result<SessionHandle>;
}

impl SessionHandle {
    pub fn query(&mut self, sql: &str) -> Result<QueryResult>;
    pub fn update(&mut self, sql: &str) -> Result<UpdateResult>;
    pub fn prepare_query(&mut self, sql: &str) -> Result<PreparedQuery>;
    pub fn begin_transaction(&mut self) -> Result<()>;
    pub fn commit(&mut self) -> Result<()>;
    pub fn rollback(&mut self) -> Result<()>;
}
```

### Async APIs
```
impl AsyncFlightSQLPool {
    pub async fn query(&self, sql: &str) -> Result<QueryResult>;
    pub async fn execute(&self, sql: &str) -> Result<QueryResult>;
    pub async fn update(&self, sql: &str) -> Result<UpdateResult>;
    pub async fn query_with_param(&self, sql: &str, params: RecordBatch) -> Result<QueryResult>;
    pub async fn update_with_record_batch(&self, sql: &str, batch: RecordBatch) -> Result<UpdateResult>;
    pub async fn query_with_options(&self, sql: &str, opts: QueryOptions) -> Result<QueryResult>;
    pub async fn update_with_options(&self, sql: &str, opts: QueryOptions) -> Result<UpdateResult>;
    pub async fn acquire_session(&self) -> Result<AsyncSessionHandle>;
}

impl AsyncSessionHandle {
    pub async fn query(&mut self, sql: &str) -> Result<QueryResult>;
    pub async fn update(&mut self, sql: &str) -> Result<UpdateResult>;
    pub async fn begin_transaction(&mut self) -> Result<()>;
    pub async fn commit(&mut self) -> Result<()>;
    pub async fn rollback(&mut self) -> Result<()>;
}
```

Notes:
- `SessionHandle` holds an exclusive connection; user keeps it for workflows
  that rely on session state.
- Stateless APIs use pooled connections and release them after the call.

## PoolConfig
```
pub struct PoolConfig {
    pub min_idle: usize,          // create on startup
    pub max_size: usize,          // cap total connections
    pub acquire_timeout_ms: u64,  // wait for available connection
    pub idle_ttl_ms: u64,         // drop idle connections
    pub healthcheck_sql: String,  // default "SELECT 1"
    pub default_query_timeout_ms: Option<u64>,
    pub retry_on_failure: bool,   // applies to idempotent queries only
}
```

Defaults:
- min_idle: 1
- max_size: max(4, min(16, num_cpus * 2))
- acquire_timeout_ms: 30000
- idle_ttl_ms: 300000
- healthcheck_sql: "SELECT 1"
- default_query_timeout_ms: None
- retry_on_failure: true (queries only)

## Pool Internals

### Data Structures
- `Vec<ManagedConnection>` protected by a mutex.
- `Condvar` for sync waiting; `tokio::sync::Semaphore` for async acquire.
- `AtomicUsize` for total connections (idle + in-use).
- `Instant` tracking last-used per connection (for idle eviction).

### Acquire Flow (stateless)
1. Lock pool.
2. If idle connection exists, pop and return.
3. If total < max_size, create a new connection and return.
4. Otherwise, wait on condvar up to acquire_timeout_ms.
5. If timeout, return a descriptive error.

### Release Flow
1. On failure paths, run health check and retry once for idempotent queries.
2. If health check fails, drop connection and decrement total.
3. Otherwise, push back into idle list and signal condvar/semaphore.

### Idle Eviction
Option 1: Lazy eviction on acquire/release (check timestamps).
Option 2: Background thread that prunes idle connections.
Start with lazy eviction to keep design simple.

## Behavior and Semantics

### Parallelism
Multiple threads calling pool APIs will receive different connections (if
available), enabling true parallel execution on the server.

### Prepared Statements and Transactions
Prepared statements are connection-scoped. Users must use `SessionHandle`
for workflows that create/reuse prepared statements or open transactions.
Stateless APIs should not expose prepared-statement handles.

### Error Handling
If a query returns a transport error or connection-level failure:
- Drop the connection from the pool.
- Return error to caller.
- Next acquire will create a fresh connection if needed.

Idempotent queries (SELECT) get one retry after a health check.
Mutations are never retried by default.

### Backpressure
When the pool is exhausted, `acquire_timeout_ms` prevents unbounded waiting.
Callers can catch the timeout and retry or increase pool size.

## Timeouts and Cancellation

### Client-Side Timeout
- Every query can accept `QueryOptions` with `timeout_ms`, or use the pool
  default.
- If the timeout elapses, the client attempts to cancel and then drops the
  connection as a fallback.

### Best-Effort Cancellation Strategy
Order of attempts:
1. `Statement::cancel()` if the ADBC driver supports it.
2. `Connection::cancel()` as a broader fallback.
3. If server-side cancel API is available, call it (see below).
4. Drop the connection and create a new one.

### Server-Side Cancel (Proposed)
Add a SwanLake Flight Action (e.g. `CancelQuery`) that accepts a session id +
statement handle (or ticket) and interrupts the DuckDB query. The client will
feature-detect this action and call it when local cancel is unsupported.
If the action is missing, the client falls back to dropping the connection.

## Logging and Metrics (optional but recommended)
- `pool.acquire.wait_ms`
- `pool.acquire.timeout`
- `pool.conn.created`
- `pool.conn.dropped`
- `pool.conn.in_use`
- `pool.conn.idle`

Add tracing spans around acquire/release for diagnostics.

## Compatibility and Migration
- Keep `FlightSQLClient` unchanged for single-connection use.
- Add `FlightSQLPool` in a new module (e.g., `swanlake-client/src/pool.rs`).
- Document when to use `FlightSQLClient` vs `FlightSQLPool`.

For async users, expose `AsyncFlightSQLPool` and keep it behind a `tokio`
feature flag if build size becomes a concern.

## Example Usage
```
let pool = FlightSQLPool::with_default("grpc://localhost:4214")?;

// Parallel queries
let q1 = std::thread::spawn({
    let pool = pool.clone();
    move || pool.query("SELECT 1")
});
let q2 = std::thread::spawn({
    let pool = pool.clone();
    move || pool.query("SELECT 2")
});

// Stateful session for prepared statements
let mut session = pool.acquire_session()?;
session.begin_transaction()?;
session.update("INSERT INTO t VALUES (1)")?;
session.commit()?;
```
```
let async_pool = AsyncFlightSQLPool::with_default("grpc://localhost:4214").await?;
let result = async_pool
    .query_with_options(
        "SELECT * FROM t",
        QueryOptions::new().with_timeout_ms(2_000),
    )
    .await?;
```
