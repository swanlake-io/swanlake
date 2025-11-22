## Handler Overview

This directory contains the Flight SQL handler implementations that plug into `FlightSqlService` for the Swanlake server. Handlers are split by feature area (statements, prepared statements, tickets, SQL info metadata, and transactions) and mostly delegate into `SwanFlightSqlService`/session helpers.

### Architecture (happy-path)

```
CommandStatementQuery / CommandPreparedStatementQuery
                │
                ▼
     get_flight_info_* (plan schema if query; ticket includes returns_rows)
                │
                ▼
     DoGet statement/prepared
       ├─ returns_rows=true  → execute_prepared_query_handle (stream batches)
       └─ returns_rows=false → execute_prepared_update_handle (empty stream + affected rows)

CommandStatementUpdate / CommandPreparedStatementUpdate
                │
                ▼
            DoPut update (handles parameters, appender/duckling queue fast paths)
```

### Statement Handlers (`statement.rs`)
- `get_flight_info_statement`: Plans a schema for an ad-hoc SQL string and returns a `FlightInfo`/ticket; supports both queries (planned schema) and commands (empty schema) so ExecuteQuery callers can send any SQL.
- `do_get_statement`: Resolves a ticket and streams results for a prepared or ephemeral statement; executes non-query tickets and reports affected rows when applicable. Falls back to SQL embedded in the ticket if a handle is missing.
- `do_put_statement_update`: Executes an ad-hoc update statement via DoPut (no result set), returning affected rows.

### Prepared Statement Handlers (`prepared.rs`)
- `do_action_create_prepared_statement`: Creates a prepared statement, infers whether it is a query, and caches schema when possible.
- `get_flight_info_prepared_statement`/`do_get_prepared_statement`: Fetch schema and stream results for query prepared statements; execute updates/DDL via DoGet with affected-row metadata when a prepared statement does not return rows.
- `do_put_prepared_statement_query`: Binds parameters for prepared statements (query or command) without executing them.
- `do_put_prepared_statement_update`: Executes prepared statements that mutate data/schema, with optimized paths for inserts (duckling queue and regular tables) and fallback parameter batching for other updates/DDL.
- `do_action_close_prepared_statement`: Closes a prepared statement handle.

### Ticket Helpers (`ticket.rs`)
- Defines the serialized payload carried in Flight tickets, including prepared vs. ephemeral handles, optional fallback SQL, and whether the statement returns rows.

### SQL Info Handlers (`sql_info.rs`)
- Serves static SQL capability metadata via Flight's SqlInfo endpoints.

### Transaction Handlers (`transaction.rs`)
- Starts and completes transactions (commit/rollback) for a session, tolerating autocommit no-ops.
