# SwanLake Client

A Rust client library and CLI for connecting to Arrow Flight SQL servers, designed for use with SwanLake.

## Installation

Enable the async pool (tokio) feature:

```toml
[dependencies]
swanlake-client = { version = "0.1.1", features = ["tokio"] }
```

Or use the sync-only client/pool:

```toml
[dependencies]
swanlake-client = "0.1.1"
```

## Async Pool Usage (recommended)

```rust
use swanlake_client::{AsyncFlightSQLPool, QueryOptions};

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let pool = AsyncFlightSQLPool::with_default("grpc://localhost:4214").await?;

    let (q1, q2) = tokio::try_join!(
        pool.query("SELECT 1"),
        pool.query_with_options("SELECT 2", QueryOptions::new()),
    )?;
    println!("Rows: {}, {}", q1.total_rows, q2.total_rows);

    let mut session = pool.acquire_session().await?;
    session.begin_transaction().await?;
    session.update("CREATE TABLE t (id INTEGER)").await?;
    session.commit().await?;

    Ok(())
}
```

## Sync Pool and Client

```rust
use swanlake_client::FlightSQLPool;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let pool = FlightSQLPool::with_default("grpc://localhost:4214")?;
    let result = pool.query("SELECT 1 as col")?;
    println!("Total rows: {}", result.total_rows);
    Ok(())
}
```

`FlightSQLClient` is still available for single-connection workflows.

## Connection Pool Design

### Problem Statement
SwanLake sessions are tied to the gRPC connection. Each session owns one DuckDB
connection guarded by a mutex on the server, so statements on a single client
connection execute sequentially. A pool provides parallelism without forcing
users to manage multiple client connections.

### Goals
- Provide parallel query/update execution by using multiple gRPC connections.
- Preserve session semantics when needed (prepared statements, transactions, temp tables, session-scoped settings).
- Keep a simple API that works for most users without extra boilerplate.
- Be safe by default: if a connection is unhealthy, drop and replace it.
- Provide async APIs that integrate with tokio without blocking user tasks.
- Support best-effort cancellation and timeouts (planned).

### Non-Goals
- Cross-connection transactions (requires server-side changes).
- Guarantee cancellation on every backend (best-effort only).

### Key Constraints
- ADBC `ManagedConnection` is the unit of session state.
- Prepared statements are tied to a specific connection/session.
- Parallelism requires multiple connections; per-connection work is sequential.

### API Overview
- `FlightSQLPool` and `AsyncFlightSQLPool` provide stateless query/update helpers.
- `SessionHandle` and `AsyncSessionHandle` provide exclusive, session-affine access.
- `PreparedQuery` (sync) is tied to a `SessionHandle` lifetime.

### PoolConfig

```rust
pub struct PoolConfig {
    pub min_idle: usize,
    pub max_size: usize,
    pub acquire_timeout_ms: u64,
    pub idle_ttl_ms: u64,
    pub healthcheck_sql: String,
    pub default_query_timeout_ms: Option<u64>,
    pub retry_on_failure: bool,
}
```

Defaults:
- min_idle: 1
- max_size: max(4, min(16, num_cpus * 2))
- acquire_timeout_ms: 30000
- idle_ttl_ms: 300000
- healthcheck_sql: "SELECT 1"
- default_query_timeout_ms: None (reserved)
- retry_on_failure: true (queries only)

### Behavior and Semantics

Parallelism
- Multiple callers receive different connections (if available), enabling server-side parallel execution.

Prepared Statements and Transactions
- Prepared statements are connection-scoped. Use `SessionHandle` for workflows that prepare and reuse statements.
- Async sessions currently expose transactional flows but do not expose prepared statement handles yet.

Error Handling
- If a query returns a transport or connection-level failure, the connection is dropped and replaced.
- Idempotent queries (`SELECT`) get one retry after a health check.
- Mutations are never retried by default.

Backpressure
- When the pool is exhausted, `acquire_timeout_ms` prevents unbounded waiting.

Timeouts and Cancellation
- `QueryOptions::timeout_ms` and `PoolConfig::default_query_timeout_ms` are reserved for future use.
- Best-effort cancellation is planned: statement cancel, connection cancel, and an optional server-side cancel action, with connection drop as the fallback.

### Internals (implementation notes)
- Sync pool uses a mutex-protected idle list with a condvar for acquisition waits.
- Async pool uses a tokio semaphore to cap concurrency and a mutex-protected idle list.
- Idle eviction is lazy (checked on acquire/release) to keep the implementation simple.

### Compatibility and Migration
- `FlightSQLClient` remains unchanged for single-connection use.
- `FlightSQLPool` is synchronous; `AsyncFlightSQLPool` is behind the `tokio` feature.

### Logging and Metrics (optional)
- pool.acquire.wait_ms
- pool.acquire.timeout
- pool.conn.created
- pool.conn.dropped
- pool.conn.in_use
- pool.conn.idle

## CLI Usage

You can use the provided interactive CLI to connect to a SwanLake server.

To build and run the CLI:

```bash
cargo run --bin swanlake-cli --features="cli"
```

By default, it will attempt to connect to `grpc://127.0.0.1:4214`. You can specify a different endpoint:

```bash
cargo run --bin swanlake-cli -- --endpoint grpc://remote-host:4214
```

## License

Licensed under the MIT License.
