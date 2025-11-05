# SwanLake - Agent Guide

> **"It's not just a duck — it's a swan in flight."**

This guide consolidates key information from all project documentation for AI assistants and developers working with the SwanLake codebase.

## What is SwanLake?

SwanLake is an **Apache Arrow Flight SQL server** powered by **DuckDB** with optional **DuckLake** extension support. It provides a high-performance, Flight SQL-compliant interface to DuckDB's analytical capabilities.

### Key Features

- ✅ Arrow Flight SQL protocol implementation
- ✅ DuckDB-powered query execution
- ✅ DuckLake extension for data lake access (Iceberg, Delta Lake, Hudi)
- ✅ Session-based architecture with dedicated connections
- ✅ Prepared statement support per session
- ✅ Schema extraction optimization (~50% performance improvement)
- ✅ ADBC client compatible

## Architecture Overview

```
┌─────────────┐
│   Client    │ (ADBC/Flight SQL)
└──────┬──────┘
       │ Arrow Flight
       ▼
┌─────────────────────────────────────────┐
│  SwanLake Flight SQL Server (Rust)        │
│  ┌────────────────────────────────────┐ │
│  │  service.rs - FlightSqlService     │ │
│  │  - CommandStatementQuery           │ │
│  │  - CommandStatementUpdate          │ │
│  │  - Prepared Statement Flow         │ │
│  └────────────────────────────────────┘ │
│  ┌────────────────────────────────────┐ │
│  │  session/ - Session Management     │ │
│  │  - Session Registry (max 100)      │ │
│  │  - Per-Connection Sessions         │ │
│  │  - Dedicated Connections           │ │
│  │  - Idle Session Cleanup            │ │
│  └────────────────────────────────────┘ │
│  ┌────────────────────────────────────┐ │
│  │  engine/ - Connection Factory      │ │
│  │  - Schema Extraction (LIMIT 0)     │ │
│  │  - Async Task Offload              │ │
│  └────────────────────────────────────┘ │
│  ┌────────────────────────────────────┐ │
│  │  config.rs - Configuration         │ │
│  │  - .env / config.toml / CLI args   │ │
│  └────────────────────────────────────┘ │
└─────────────────────────────────────────┘
       │
       ▼
┌─────────────┐
│   DuckDB    │ (Native Library v1.4.1)
└─────────────┘
```

### Core Components

1. **`config`** - Loads runtime configuration from environment variables, config.toml, or CLI args
2. **`engine/`** - Connection factory and DuckDB wrapper with schema extraction optimization
3. **`session/`** - Session management with per-connection sessions, dedicated connections, prepared statements, and transactions
4. **`service`** - Flight SQL service implementation with session registry for queries and statements
5. **`main`** - Entry point that wires everything together with idle session cleanup

## Critical Performance Optimizations

### 1. Schema Extraction with LIMIT 0

**Problem:** Naive implementation executes queries twice (once for schema, once for results).

**Solution:** Wrap queries in `SELECT * FROM (query) LIMIT 0` to get schema without data scan.

```rust
// src/duckdb.rs
pub fn schema_for_query(&self, sql: &str) -> Result<Schema, ServerError> {
    let schema_query = format!("SELECT * FROM ({}) LIMIT 0", sql);
    let mut stmt = conn.prepare(&schema_query)?;
    let arrow = stmt.query_arrow([])?;  // Planning only, no execution!
    Ok(arrow.get_schema().as_ref().clone())
}
```

**Impact:** ~50% performance improvement for typical query workflows

**Trade-off:** Uses LIMIT 0 wrapper due to DuckDB-rs API constraint (Statement doesn't expose schema without execution)

### 2. Session-Based Architecture

Each gRPC connection gets a dedicated session with a DuckDB connection, persistent state for transactions and temp tables. Sessions are automatically cleaned up after 30 minutes of inactivity. DuckLake extensions and user SQL initialize per session; UI server initializes once on first connection.

### 3. Async Task Offload

All DuckDB operations run in `tokio::task::spawn_blocking` to avoid blocking the async runtime.

### 4. Arrow Zero-Copy

Results use `query_arrow()` for efficient columnar data transfer.

### 5. Session State Persistence

Prepared statements and transactions persist within each session's dedicated connection, enabling complex multi-statement workflows.

## Prepared Statement Design Decisions

### The Constraint

**DuckDB-rs API Limitation:** `Statement` struct doesn't expose schema without execution.

```rust
// ❌ Doesn't exist:
let stmt = conn.prepare(sql)?;
let schema = stmt.schema()?;

// ✅ Must execute:
let arrow = stmt.query_arrow([])?;
let schema = arrow.get_schema();
```

### Current Implementation

**Handle Format:** `StatementHandle` type wrapping a u64 ID, convertible to/from big-endian bytes

**Flow:**
1. `CreatePreparedStatement` → Returns `StatementHandle` + schema (via LIMIT 0)
2. `GetFlightInfo` → Returns schema + ticket (total_records = -1)
3. `DoGet` → Decodes handle, re-prepares, executes, streams results

**Trade-offs:**
- ✅ Enables transaction state persistence
- ✅ Simpler lifecycle management

**Logging:** Handles are logged as readable u64 numbers via `StatementHandle`'s `Display` implementation.

**Transaction IDs:** Similarly, `TransactionId` type wraps a u64 ID for transaction management, with the same conversion and logging benefits.

**Future Improvement:** If DuckDB-rs adds `Statement::schema()`, we can remove LIMIT 0 optimization.

### Protocol Compliance

✅ Fully Flight SQL compliant:
- Correct schema returned
- `total_records = -1` (unknown) per spec
- Schema consistent across all calls
- Protocol doesn't mandate HOW you get schema

## Flight SQL Operations Supported

### Query Path (SELECT)

```
Client calls:                    Server methods:
1. CommandStatementQuery     →   get_flight_info_statement
2. DoGet(ticket)            →   do_get_statement

OR (Prepared):
1. CreatePreparedStatement   →   do_action_create_prepared_statement
2. GetFlightInfo(handle)    →   get_flight_info_prepared_statement
3. DoGet(ticket)            →   do_get_prepared_statement
```

### Statement Path (DDL/DML)

```
Client calls:                    Server methods:
1. CommandStatementUpdate    →   do_put_statement_update

OR (Prepared):
1. CreatePreparedStatement   →   do_action_create_prepared_statement
2. DoPut(handle)            →   do_put_prepared_statement_update
```

### Query Type Detection

Keyword-based analysis in `is_query_statement()`:
- **Queries:** SELECT, WITH, SHOW, DESCRIBE, EXPLAIN, VALUES, TABLE, PRAGMA
- **Statements:** ATTACH, CREATE, DROP, INSERT, UPDATE, DELETE, etc.
- Handles SQL comments (`--` and `/* */`) correctly

## Configuration

### Environment Variables

| Variable | Purpose | Default |
|----------|---------|---------|
| `SWANLAKE_HOST` | Server bind address | `127.0.0.1` |
| `SWANLAKE_PORT` | TCP port | `4214` |
| `SWANLAKE_DUCKDB_PATH` | Database file path | _in-memory_ |
| `SWANLAKE_MAX_SESSIONS` | Maximum concurrent sessions | `100` |
| `SWANLAKE_SESSION_TIMEOUT_SECONDS` | Session idle timeout | `1800` |
| `SWANLAKE_ENABLE_DUCKLAKE` | Load DuckLake extension | `true` |
| `SWANLAKE_DUCKLAKE_INIT_SQL` | SQL after extension loads | _unset_ |

### Configuration Precedence

1. Environment variables (highest priority)
2. Command-line config file (`--config custom.toml`)
3. `.env` file (loaded automatically via dotenvy)

## Quick Start

### Setup DuckDB Native Library

```bash
# Download DuckDB v1.4.1 and configure environment
scripts/setup_duckdb.sh
source .duckdb/env.sh
```

### Run Server

```bash
cargo run
# Or with custom config:
cargo run -- --config path/to/custom.toml
```

### Run Tests

```bash
# Full integration test (builds server + runs Go client tests)
./scripts/test-integration.sh

# Manual testing:
# Terminal 1:
SWANLAKE_PORT=50051 cargo run

# Terminal 2:
cd examples/go
SWANLAKE_PORT=50051 go run main.go
```

## Testing Framework

### Go Client (examples/go/)

ADBC-based testing framework with 6 test scenarios:

1. ✅ Simple SELECT query
2. ✅ DDL statement (CREATE TABLE)
3. ✅ DML statement (INSERT)
4. ✅ Query with ORDER BY
5. ✅ Prepared statement flow
6. ✅ Schema optimization (complex query with CROSS JOIN)

### Test Scripts

```bash
# Run all tests
./scripts/test-integration.sh

# Go client only
cd examples/go && ./test.sh

# Benchmarks
cd examples/go && go test -bench=. -benchmem
```

## Current Limitations

1. **No parameter binding** - Prepared statements don't support `?` or `$1` placeholders
2. **No statement caching** - Each execution re-prepares (acceptable ~1-2ms overhead)
3. **No transactions** - Each query runs in its own transaction
4. **Limited metadata** - No `get_catalogs`, `get_db_schemas`, `get_tables` yet

See `examples/go/ROADMAP.md` for implementation priorities.

## Key Files to Know

### Source Code
- `src/main.rs` - Entry point, server setup with session cleanup
- `src/config.rs` - Configuration loading
- `src/engine/` - Connection factory and DuckDB wrapper
- `src/session/` - Session management and registry
- `src/service.rs` - Flight SQL service implementation
- `src/error.rs` - Error types

### Documentation
- `README.md` - Main project documentation
- `SESSION_REFACTOR_DESIGN.md` - Session architecture design and migration
- `OPTIMIZATIONS.md` - Detailed performance optimization docs
- `PREPARED_STATEMENT_DECISION.md` - Design decisions and trade-offs
- `IMPLEMENTATION_SUMMARY.md` - Implementation status and history

### Testing
- `test-integration.sh` - Automated integration test runner
- `examples/go/main.go` - ADBC-based test suite
- `examples/go/test.sh` - Go client test runner

### Configuration
- `config.toml` - Configuration file
- `.env` - Environment variables (git-ignored)
- `scripts/setup_duckdb.sh` - DuckDB native library setup

### Session Management
- `src/session/registry.rs` - Session registry with cleanup
- `src/session/session.rs` - Session implementation with state
- `src/session/id.rs` - ID generators for sessions and statements

## Common Tasks

### Add a New Flight SQL Operation

1. Add method to `SwanFlightSqlService` in `src/service.rs`
2. Implement DuckDB logic in `src/duckdb.rs` if needed
3. Add test case to `examples/go/main.go`
4. Run `./scripts/test-integration.sh` to verify

### Debug Performance Issues

```bash
# Enable debug logging
RUST_LOG=debug cargo run

# Look for:
# - "retrieved schema (optimized with LIMIT 0)"
# - Query execution times
# - Connection pool activity
```

### Add New Configuration

1. Add field to `Config` struct in `src/config.rs`
2. Add environment variable name constant
3. Update `load()` method to read from env
4. Update README.md configuration table

## Important Design Principles

1. **Fail Fast** - Return errors immediately, don't catch/ignore
2. **Minimal Code** - Keep implementation simple and focused
3. **Session-Based** - One connection per client session for state persistence
4. **Async Offload** - All DuckDB work in `spawn_blocking`
5. **Zero-Copy** - Use Arrow format throughout
6. **Documented Trade-offs** - Explain WHY when deviating from ideal patterns

## Performance Benchmarks

### Expected Results

```
BenchmarkSimpleQuery-8                500     2.1ms/op
BenchmarkDirectQuery-8                100    12.5ms/op
BenchmarkPreparedStatementFlow-8      150    10.2ms/op
BenchmarkSchemaExtraction-8          2000     0.5ms/op  # LIMIT 0 optimization
```

**Key Metric:** Schema extraction is ~20x faster than full execution.

## Debugging Tips

### Check Server Health

```bash
# Server logs should show:
# "session registry initialized"
# "SwanLake Flight SQL server listening on 127.0.0.1:4214"
# "Cleaned up X idle sessions" (every 5 minutes if sessions were cleaned)
```

### Verify Optimization

```bash
RUST_LOG=debug cargo run 2>&1 | grep "LIMIT 0"
# Should see: "retrieved schema (optimized with LIMIT 0)"
```

### Test Sessions

```bash
# Check session creation
RUST_LOG=info cargo run 2>&1 | grep "session created"

# Check session cleanup
RUST_LOG=info cargo run 2>&1 | grep "idle sessions"
```

## Summary

SwanLake is a production-ready Arrow Flight SQL server with a per-connection session-based architecture that provides persistent state for complex client workflows. The LIMIT 0 schema optimization delivers ~50% performance improvements while maintaining full Flight SQL protocol compliance. Sessions automatically manage connection lifecycle, prepared statements, and transactions with configurable limits and timeouts.

---

**For more details:**
- Session Architecture: See `SESSION_REFACTOR_DESIGN.md`
- Performance: See `OPTIMIZATIONS.md`
- Prepared Statements: See `PREPARED_STATEMENT_DECISION.md`
- Implementation History: See `IMPLEMENTATION_SUMMARY.md`
- Getting Started: See `README.md`
