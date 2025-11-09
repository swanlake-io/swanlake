# Session Architecture

SwanLake implements a **connection-based session architecture** where each gRPC connection gets a dedicated DuckDB session with persistent state. This enables correct semantics for prepared statements, transactions, and connection-scoped settings.

## Overview

### Key Concepts

- **Session**: A stateful context with a dedicated DuckDB connection
- **Session ID**: Derived from the client's remote address (`ip:port`)
- **Session Persistence**: Sessions persist across multiple requests from the same gRPC connection
- **Automatic Cleanup**: Idle sessions are automatically removed after timeout (default: 30 minutes)

### Architecture Diagram

```
┌─────────────────────────────────────────────────────────┐
│  Client (ip:port = 192.168.1.100:54321)                │
└────────────────────┬────────────────────────────────────┘
                     │ gRPC Connection
                     ▼
┌─────────────────────────────────────────────────────────┐
│  SwanFlightSqlService                                   │
│  ┌──────────────────────────────────────────────────┐  │
│  │  SessionRegistry                                  │  │
│  │  ┌────────────────────────────────────────────┐  │  │
│  │  │  Session (id: "192.168.1.100:54321")       │  │  │
│  │  │  ├─ DuckDB Connection                       │  │  │
│  │  │  ├─ Prepared Statements                     │  │  │
│  │  │  ├─ Active Transactions                     │  │  │
│  │  │  └─ Last Activity Timestamp                 │  │  │
│  │  └────────────────────────────────────────────┘  │  │
│  └──────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────┐
│  DuckDB (in-memory with extensions)                     │
└─────────────────────────────────────────────────────────┘
```

## How It Works

### Session Lifecycle

1. **Client Connects**: Opens a gRPC connection to SwanLake
2. **First Request**:
   - SessionID generated from `remote_addr()` (e.g., `"192.168.1.100:54321"`)
   - New session created with dedicated DuckDB connection
   - Extensions loaded (ducklake, httpfs, aws, postgres)
   - `ducklake_init_sql` executed (ATTACH statements, etc.)
   - UI server started (first connection only)
3. **Subsequent Requests**: Same SessionID → reuses existing session
4. **Client Disconnects**: Session remains for `session_timeout_seconds`
5. **Cleanup**: Idle sessions automatically removed by periodic task

### Session Identification

Sessions are identified by the client's remote address:

```rust
fn extract_session_id<T>(request: &Request<T>) -> SessionId {
    if let Some(addr) = request.remote_addr() {
        SessionId::from_string(addr.to_string())  // "192.168.1.100:54321"
    } else {
        SessionId::from_string(Uuid::new_v4().to_string())  // Fallback
    }
}
```

**Why remote_addr?**
- ✅ Unique per TCP connection (includes client port)
- ✅ No client changes required
- ✅ Works transparently with existing clients
- ✅ Automatic session isolation
- ✅ Session lifetime tied to connection lifetime

## Session State

Each session maintains:

### 1. DuckDB Connection

A dedicated, persistent connection to DuckDB:

```rust
pub struct Session {
    id: SessionId,
    connection: DuckDbConnection,
    // ...
}
```

**Benefits:**
- Connection-scoped settings persist
- Temporary tables available across requests
- Better resource management

### 2. Prepared Statements

Stored in session-scoped HashMap:

```rust
prepared_statements: Arc<Mutex<HashMap<StatementHandle, PreparedStatementState>>>
```

**Workflow:**
```
Request 1: CreatePreparedStatement → handle = StatementHandle::new(1)
Request 2: BindParameters(handle)  → Same session, finds statement ✅
Request 3: Execute(handle)         → Same session, executes ✅
```

Handles are lightweight wrappers around `u64` values; they serialize to big-endian bytes when crossing the Flight wire format.

### 3. Transactions

Tracked per session:

```rust
transactions: Arc<Mutex<HashMap<TransactionId, Transaction>>>
```

**Workflow:**
```
Request 1: BeginTransaction     → txn_id = TransactionId::new(1)
Request 2: Execute in txn       → Same session, same transaction ✅
Request 3: Commit(txn_id)       → Same session, commits ✅
```

`TransactionId` follows the same wrapping/serialization approach as `StatementHandle`, keeping logs readable while maintaining a compact protocol payload.

### 4. Activity Tracking

```rust
last_activity: Arc<Mutex<Instant>>
```

Updated on every request to prevent premature cleanup.

## Configuration

### Server Configuration

```toml
# Session limits
max_sessions = 100                  # Maximum concurrent sessions
session_timeout_seconds = 1800      # 30 minutes

ducklake_init_sql = """
  ATTACH 's3://my-bucket/data' AS mydata;
  ATTACH 'postgresql://host/db' AS pgdb;
"""
```

### Environment Variables

```bash
export SWANLAKE_MAX_SESSIONS=100
export SWANLAKE_SESSION_TIMEOUT_SECONDS=1800
export SWANLAKE_DUCKLAKE_INIT_SQL="ATTACH 's3://bucket/data' AS mydata;"
```

## Connection Initialization

Every new session connection is initialized with:

1. **Extensions**:
   ```sql
   INSTALL ducklake; INSTALL httpfs; INSTALL aws; INSTALL postgres;
   LOAD ducklake; LOAD httpfs; LOAD aws; LOAD postgres;
   ```

2. **User SQL** (from `ducklake_init_sql`):
   ```sql
   ATTACH 's3://my-bucket/data' AS mydata;
   ATTACH 'postgresql://host/db' AS pgdb;
   ```



## Session Management

### SessionRegistry

Central registry managing all active sessions:

```rust
pub struct SessionRegistry {
    sessions: HashMap<SessionId, Arc<Session>>,
    factory: EngineFactory,
    max_sessions: usize,
    session_timeout: Duration,
}
```

**Key Methods:**

- `get_or_create_by_id(session_id)` - Get existing or create new session
- `cleanup_idle_sessions()` - Remove sessions exceeding timeout
- `session_count()` - Get current session count

### Automatic Cleanup

Periodic task runs every 5 minutes:

```rust
tokio::spawn(async move {
    let mut interval = tokio::time::interval(Duration::from_secs(300));
    loop {
        interval.tick().await;
        registry.cleanup_idle_sessions();
    }
});
```

Sessions are removed if `idle_duration() > session_timeout`.

## Implementation Details

### Core Components

#### 1. Session (`src/session/session.rs`)

```rust
pub struct Session {
    id: SessionId,
    connection: DuckDbConnection,
    transactions: Arc<Mutex<HashMap<TransactionId, Transaction>>>,
    prepared_statements: Arc<Mutex<HashMap<StatementHandle, PreparedStatementState>>>,
    last_activity: Arc<Mutex<Instant>>,
}

impl Session {
    pub fn execute_query(&self, sql: &str) -> Result<QueryResult, ServerError>;
    pub fn begin_transaction(&self) -> Result<TransactionId, ServerError>;
    pub fn create_prepared_statement(&self, sql: String, is_query: bool) -> Result<StatementHandle, ServerError>;
    // ...
}
```

#### 2. SessionRegistry (`src/session/registry.rs`)

```rust
pub struct SessionRegistry {
    inner: Arc<RwLock<RegistryInner>>,
    factory: EngineFactory,
    max_sessions: usize,
    session_timeout: Duration,
}

impl SessionRegistry {
    pub fn get_or_create_by_id(&self, session_id: &SessionId) -> Result<Arc<Session>, ServerError>;
    pub fn cleanup_idle_sessions(&self) -> usize;
}
```

#### 3. EngineFactory (`src/engine/factory.rs`)

```rust
pub struct EngineFactory {
    init_sql: String,
}

impl EngineFactory {
    pub fn create_connection(&self) -> Result<DuckDbConnection, ServerError>;
}
```

### Service Integration

Flight SQL service handler pattern:

```rust
async fn handler(
    &self,
    query: Command,
    request: Request<T>,
) -> Result<Response<R>, Status> {
    // Extract session ID from connection
    let session_id = Self::extract_session_id(&request);

    // Get or create session
    let session = self.registry
        .get_or_create_by_id(&session_id)
        .map_err(Self::status_from_error)?;

    // Use session to execute operation
    let result = session.execute_query(&sql)?;

    // Return response
    Ok(Response::new(result))
}
```

## Benefits

### 1. Correct Semantics

- ✅ **Prepared Statements**: Work across multiple requests
- ✅ **Transactions**: Multi-statement transactions supported
- ✅ **Temporary Tables**: Persist within session
- ✅ **Connection Settings**: Maintained across requests

### 2. Performance

- ✅ **Connection Reuse**: No connection setup overhead per request
- ✅ **Prepared Statement Caching**: Statements parsed once
- ✅ **Resource Efficiency**: Shared connection pool avoided

### 3. Isolation

- ✅ **Per-Connection State**: Complete isolation between clients
- ✅ **No Cross-Talk**: Sessions cannot interfere with each other
- ✅ **Security**: Transaction data not leaked across sessions

### 4. Simplicity

- ✅ **No Client Changes**: Works transparently
- ✅ **Clean Code**: Single `registry` field in service
- ✅ **Easy Testing**: Session behavior predictable

## Edge Cases

### 1. Connection Reconnect

**Scenario:** Client disconnects and reconnects

**Behavior:**
- New connection → different client port → new SessionID
- New session created with fresh state
- Old session cleaned up after timeout

**Rationale:** Correct behavior - client reconnect should start fresh

### 2. Multiple Concurrent Connections

**Scenario:** Same client IP, multiple connections

**Behavior:**
- Each connection gets unique port → unique SessionID
- Each connection gets separate session
- Complete isolation between connections

**Rationale:** Correct behavior - concurrent connections should be isolated

### 3. Session Limit Reached

**Scenario:** `max_sessions` limit hit

**Behavior:**
- `SessionRegistry::get_or_create_by_id()` returns `Err(ServerError::MaxSessionsReached)`
- Client receives error status
- Cleanup task runs to free idle sessions

**Mitigation:**
- Increase `max_sessions` in config
- Decrease `session_timeout_seconds` for faster cleanup
- Monitor session count with `session_count()`

### 4. Port Reuse

**Scenario:** Client port reused before session timeout

**Behavior:**
- Extremely rare (30 min timeout >> typical port reuse time)
- If happens: old session reused
- Session state may be stale but valid

**Mitigation:** Not needed in practice; timeout prevents this

## Migration from Old Architecture

The migration from global state to session-scoped state involved:

### Before (Phase 1)

```rust
pub struct SwanFlightSqlService {
    registry: Arc<SessionRegistry>,
    engine: Arc<DuckDbEngine>,
    transactions: Arc<TransactionManager>,      // Global!
    prepared: Arc<PreparedStatementStore>,      // Global!
    next_statement_id: Arc<AtomicU64>,
}
```

**Problems:**
- Prepared statements shared across all clients
- Transactions used separate connections
- No state persistence across requests
- Complex global state management

### After (Phase 2)

```rust
pub struct SwanFlightSqlService {
    registry: Arc<SessionRegistry>,  // Just one field!
}
```

**Benefits:**
- All state in sessions (prepared statements, transactions)
- Connection-based persistence
- ~250 lines of code removed
- Simpler, cleaner architecture

## Future Enhancements

### Header-Based Session IDs (Optional)

Support explicit session IDs via headers:

```
x-swanlake-session-id: custom-session-id-123
```

**Use Case:** Client wants explicit session control

**Implementation:**
```rust
fn extract_session_id<T>(request: &Request<T>) -> SessionId {
    // Try custom header first
    if let Some(header) = request.metadata().get("x-swanlake-session-id") {
        return SessionId::from_string(header.to_str().unwrap().to_string());
    }

    // Fall back to connection-based
    if let Some(addr) = request.remote_addr() {
        return SessionId::from_string(addr.to_string());
    }

    // Last resort: random
    SessionId::new()
}
```

### Session Metrics

Expose session metrics:

- Active session count
- Session creation rate
- Session cleanup rate
- Average session lifetime

### Session Pooling

Pre-create session pool for faster first request:

```rust
impl SessionRegistry {
    pub fn warmup(&self, count: usize) -> Result<(), ServerError> {
        for _ in 0..count {
            self.create_session()?;
        }
        Ok(())
    }
}
```

## Troubleshooting

### Sessions Not Persisting

**Symptom:** Prepared statements fail on second request

**Causes:**
- Load balancer creating new connection per request
- Client library not reusing connections
- Proxy between client and server

**Solution:**
- Check connection reuse in client
- Verify `remote_addr()` stability
- Consider header-based session IDs

### Too Many Sessions

**Symptom:** `MaxSessionsReached` errors

**Causes:**
- Too many clients
- Clients not closing connections
- `session_timeout_seconds` too high

**Solution:**
- Increase `max_sessions`
- Decrease `session_timeout_seconds`
- Fix client connection leaks



## References

- [Arrow Flight SQL Protocol](https://arrow.apache.org/docs/format/FlightSql.html)
- [DuckDB Extensions](https://duckdb.org/docs/extensions/overview)
- [DuckLake Extension](https://github.com/duckdb/duckdb)
- SwanLake README: `../README.md`
- Configuration: `../config.toml.example`
