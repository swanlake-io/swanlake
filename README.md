# SwanDB - It's not just a duck — it's a swan in flight.

> **For AI Assistants/Developers:** See [AGENT.md](AGENT.md) for a comprehensive guide consolidating all project documentation.

An Arrow Flight SQL server powered by DuckDB with DuckLake extension support.

## Overview

SwanDB is a arrow flight sql server:
https://github.com/apache/arrow-rs/blob/main/arrow-flight/src/sql/server.rs

that powered by duckdb https://duckdb.org/docs/stable/clients/rust and ducklake https://ducklake.select/docs/stable/duckdb/introduction

help me build the very basic version of it.

minimum code, fail fast

dependencies I can think of:

- arrow-flight
- duckdb-rs
- thiserror
- anyhow
- tracing
- dotenv
- serde
- prost
- tokio maybe

examples:

* https://github.com/apache/datafusion/blob/main/datafusion-examples/examples/flight/flight_sql_server.rs
* https://github.com/apache/arrow-rs/blob/main/arrow-flight/examples/flight_sql_server.rs
* https://github.com/datafusion-contrib/datafusion-flight-sql-server/blob/main/datafusion-flight-sql-server/src/service.rs

## Getting started

```bash
cargo run
# override config file
cargo run -- --config path/to/custom.toml
```

The server listens on `127.0.0.1:4214` by default and can be configured through environment variables:

| Variable | Purpose | Default |
| --- | --- | --- |
| `SWANDB_HOST` | Address to bind the Flight SQL server | `127.0.0.1` |
| `SWANDB_PORT` | TCP port for the Flight SQL server | `4214` |
| `SWANDB_DUCKDB_PATH` | Optional path to a DuckDB database file (in-memory when omitted) | _in-memory_ |
| `SWANDB_MAX_SESSIONS` | Maximum number of concurrent client sessions | `100` |
| `SWANDB_SESSION_TIMEOUT_SECONDS` | Idle session timeout in seconds | `1800` |
| `SWANDB_ENABLE_DUCKLAKE` | Toggle automatic `ducklake` extension install/load | `true` |
| `SWANDB_DUCKLAKE_INIT_SQL` | Optional SQL executed after the extension loads (e.g. ATTACH commands) | _unset_ |

`.env` files are loaded automatically via `dotenvy`. You can also point the binary at a custom `config.toml` with `--config`; environment variables always take precedence.

### Testing with the Go Client

A Go client using Apache Arrow ADBC is available in the `examples/go/` directory for testing the Flight SQL server:

```bash
# Run automated integration test (builds server + runs tests)
./scripts/test-integration.sh

# Or manually:
# Terminal 1: Start the server
SWANDB_PORT=50051 cargo run

# Terminal 2: Run the Go client tests
cd examples/go
SWANDB_PORT=50051 ./test.sh
```

The Go client tests:
- Simple SELECT queries
- DDL statements (CREATE TABLE)
- DML statements (INSERT, UPDATE, DELETE)
- Prepared statement flow (CREATE → PREPARE → EXECUTE)
- Query result reading with Arrow types

See [examples/go/README.md](examples/go/README.md) for detailed usage and examples.

### DuckDB native library

We link against the official DuckDB binary instead of compiling C++ sources on every build. Run the helper script once to download the correct archive for your platform and create an env file with the necessary exports:

```bash
# grabs v1.4.1 into .duckdb/ and generates .duckdb/env.sh
scripts/setup_duckdb.sh

# enable the environment in your shell
source .duckdb/env.sh

# now build or run as usual
cargo check
```

The script stores the archive inside `.duckdb/<version>` (ignored by git) and keeps `DUCKDB_LIB_DIR`, `DUCKDB_INCLUDE_DIR`, and the appropriate loader path in `.duckdb/env.sh`. CI and other tooling can simply `source` the same file before invoking Cargo.

## Performance

SwanDB includes several optimizations for efficient query execution:

### Schema Extraction Optimization

When clients request query schemas (via `GetFlightInfo`), SwanDB uses a `LIMIT 0` optimization to avoid executing expensive queries:

```sql
-- Instead of executing the full query twice:
SELECT * FROM large_table JOIN other_table  -- executed for schema
SELECT * FROM large_table JOIN other_table  -- executed for results

-- We wrap the query with LIMIT 0 for schema extraction:
SELECT * FROM (SELECT * FROM large_table JOIN other_table) LIMIT 0  -- planning only
SELECT * FROM large_table JOIN other_table  -- full execution
```

**Impact:** ~50% performance improvement for typical query workflows by eliminating duplicate query execution.

See [OPTIMIZATIONS.md](OPTIMIZATIONS.md) for detailed performance documentation.

## Architecture overview

- `config`: loads runtime configuration from the environment.
- `engine/`: connection factory that creates initialized DuckDB connections with extensions and optimizations.
- `session/`: session management with per-connection sessions, dedicated connections, prepared statements, transactions, and automatic cleanup.
- `service`: minimal [`FlightSqlService`](https://docs.rs/arrow-flight/latest/arrow_flight/sql/server/trait.FlightSqlService.html) implementation that handles `CommandStatementQuery` (SELECT queries) and `CommandStatementUpdate` (DDL/DML statements) and streams results via Arrow Flight.
- `main`: wires configuration, logging, the Flight SQL gRPC server, and idle session cleanup together.

The implementation uses a per-connection session-based architecture where each gRPC connection gets a dedicated session with a DuckDB connection and persistent state. No global state is used. Queries are executed directly and the resulting `RecordBatch`es are converted to Flight `FlightData`. DuckLake extensions and user SQL initialize per session; UI server initializes once on first connection. Sessions are automatically cleaned up after 30 minutes of inactivity.

### Flight SQL Operations

The server supports the standard Flight SQL prepared statement flow with automatic query vs. statement detection:

#### Recommended Flow: Prepared Statements with Auto-Detection

1. **Client calls:** `DoAction("CreatePreparedStatement")` with SQL
   - Server analyzes SQL using DuckDB's column count detection
   - Returns `ActionCreatePreparedStatementResult` with:
     - `dataset_schema` - **non-empty for queries (SELECT), empty for statements (DDL/DML)**
     - `prepared_statement_handle` - opaque handle (currently the SQL itself)

2. **Client inspects `dataset_schema` and routes accordingly:**
   - If non-empty schema → Query path: `GetFlightInfo` + `DoGet`
   - If empty schema → Update path: `DoPut`

#### Direct Execution (Alternative)

You can also execute SQL directly without the prepared statement flow:

1. **Queries (SELECT)** - Use `CommandStatementQuery` via `get_flight_info_statement` + `do_get_statement`
   - Returns result sets as Arrow batches
   - Example: `SELECT * FROM table`

2. **Statements (DDL/DML)** - Use `CommandStatementUpdate` via `do_put_statement_update`
   - Executes statements that don't return results (ATTACH, CREATE, DROP, INSERT, UPDATE, DELETE)
   - Returns affected row count (0 for DDL statements)
   - Example: `ATTACH 'ducklake:postgres:dbname=test' AS ducklake (DATA_PATH 'r2://bucket/')`

**Detection Method:** The server uses keyword-based analysis to avoid executing statements prematurely:
- Keywords like `SELECT`, `WITH`, `SHOW`, `DESCRIBE`, `EXPLAIN`, `VALUES`, `TABLE`, `PRAGMA` → Query
- All other keywords (`ATTACH`, `CREATE`, `DROP`, `INSERT`, `UPDATE`, `DELETE`, etc.) → Statement
- Handles SQL comments (`--` and `/* */`) correctly
