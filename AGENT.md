# SwanLake — LLM Field Guide

Concise reference for language-model agents working on the SwanLake codebase.

## Snapshot
- **Purpose**: Arrow Flight SQL server backed by DuckDB with optional DuckLake extensions.
- **Runtime**: Rust async service (`tokio`) exposing prepared statements, streaming results, and session-scoped state.
- **Sessions**: Each gRPC connection owns a DuckDB connection; idle sessions auto-expire (default 30 min).
- **Performance**: Schema discovery executes queries as-is, relying on DuckDB's lazy streaming to avoid unnecessary data scanning.

## Code Map
- `swanlake-server/src/main.rs` — bootstrap: config load, tracing, gRPC server, session janitor.
- `swanlake-core/src/config.rs` — layered config (env > CLI `--config` > `.env`/defaults).
- `swanlake-core/src/session/` — session registry, per-session state, ID helpers.
- `swanlake-core/src/service/` — Arrow Flight SQL handlers (queries, updates, prepared flow).
- `swanlake-core/src/engine/` — DuckDB connection factory, schema extraction helpers.
- `tests/runner/` — integration harness using the same Flight API as clients.
- `examples/go-*/` — Go ADBC and `sqlx` samples that double as smoke tests.

## Build & Run
```bash
# run the server
RUST_LOG=info cargo run --bin swanlake
```

## Configuration Cheatsheet
All env vars use the `SWANLAKE_` prefix. See [Configuration.md](Configuration.md) for the up-to-date
table covering defaults and descriptions. Quick reminders:
- Host/port/pool sizes influence the Flight endpoint + DuckDB pools.
- `ENABLE_WRITES`, `MAX_SESSIONS`, and `SESSION_TIMEOUT_SECONDS` gate write workloads and cleanup.
- `DUCKLAKE_INIT_SQL` runs immediately after DuckDB boots (attach remote storage, etc.).
- Duckling Queue knobs (`DUCKLING_QUEUE_*`) control staging paths plus rotation/flush behavior.

Precedence: env > CLI `--config` > `config.toml` > `.env`.

## Flight SQL Behaviour
- **Queries**: `CommandStatementQuery` ➜ `get_flight_info_statement` ➜ `do_get_statement`.
- **Updates/DDL**: `CommandStatementUpdate` ➜ `do_put_statement_update`.
- **Prepared flow**: `CreatePreparedStatement` yields handle + schema, followed by `GetFlightInfo`/`DoGet` (queries) or `DoPut` (updates).
- **Tickets**: `TicketStatementQuery` payloads carry `{version, kind, returns_rows, statement_handle? , fallback_sql?}` so DoGet knows whether to stream rows or execute a command.
- **Ephemeral handles**: `get_flight_info_statement` registers ad-hoc queries with ephemeral handles; `do_get_statement` executes and auto-closes them.
- **Metadata**: Responses attach `x-swanlake-total-rows` / `x-swanlake-total-bytes` (queries) and `x-swanlake-affected-rows` (commands) when available.
- **Duckling Queue**: inserts via SQL or Arrow batches; optimized paths live in `do_put_prepared_statement_update`.
- See `src/service/handlers/README.md` for handler entrypoints and flow, and `tests/sql/README.md` for the SQL test format.

## Testing & Tooling
- `./scripts/run-integration-tests.sh` — end-to-end (builds server, runs Go client tests).
- `examples/go-adbc/main.go` — quick manual smoke test (`SWANLAKE_PORT=50051 go run main.go`).

## Common Agent Tasks
- **Add config option**: extend `ServerConfig` in `src/config.rs`, wire defaults + docs (`README.md`, possibly `AGENT.md`).
- **Extend Flight endpoints**: implement handler in `src/service/…`, add DuckDB helper if required, cover with Go example/Rust test.
- **Investigate performance**: run with `RUST_LOG=debug`, inspect schema-optimization logs, verify DuckDB calls use `spawn_blocking`.

## Prepared-Statement Internals
- `PreparedStatementOptions` is a builder used *only at creation time* so handlers can pass ephemeral/cached-schema hints without mutating session state directly.
- `PreparedStatementMeta` is the long-lived record stored in the session map (SQL text, cached schema, flags). Any runtime change (e.g., caching a schema later) updates this struct via helper methods such as `cache_prepared_statement_schema`.
- Ephemeral statements set the option flag and are auto-closed inside `execute_prepared_query_handle` after a DoGet finishes.

## Further Reading
- `README.md` — project overview, highlights, license.
- `docs/session.md` — session lifecycle & metadata contract.

Keep responses crisp, prefer concrete file references, and preserve the session-centric design.
