# SwanLake — LLM Field Guide

Concise reference for language-model agents working on the SwanLake codebase.

## Snapshot
- **Purpose**: Arrow Flight SQL server backed by DuckDB with optional DuckLake extensions.
- **Runtime**: Rust async service (`tokio`) exposing prepared statements, streaming results, and session-scoped state.
- **Sessions**: Each gRPC connection owns a DuckDB connection; idle sessions auto-expire (default 30 min).
- **Performance**: Schema discovery executes queries as-is, relying on DuckDB's lazy streaming to avoid unnecessary data scanning.

## Code Map
- `src/main.rs` — bootstrap: config load, tracing, gRPC server, session janitor.
- `src/config.rs` — layered config (env > CLI `--config` > `.env`/defaults).
- `src/session/` — session registry, per-session state, ID helpers.
- `src/service/` — Arrow Flight SQL handlers (queries, updates, prepared flow).
- `src/engine/` — DuckDB connection factory, schema extraction helpers.
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
- **Tickets**: all `TicketStatementQuery` payloads are our own protobuf (`TicketStatementPayload`). They carry `{version, kind, statement_handle}` only—the SQL/schema stay server-side. `kind` distinguishes long-lived prepared statements from one-shot "ephemeral" handles created for ad-hoc queries so FlightInfo + DoGet share the same cached schema/query plan.
- **Ephemeral handles**: `get_flight_info_statement` now registers every SELECT as an ephemeral prepared statement with cached schema. `do_get_statement` executes via the same handle and immediately closes it, ensuring the query runs exactly once.
- **Detection**: `is_query_statement()` strips comments and inspects the first keyword; SELECT/WITH/SHOW/etc. route to query path, everything else goes to update path.
- **Metadata**: Responses attach `x-swanlake-total-rows` / `x-swanlake-total-bytes` when available.
- **Health checks**: gRPC health service available at `grpc.health.v1.Health` endpoint for container orchestration (ECS, Kubernetes).
- **Duckling Queue admin command**: `PRAGMA duckling_queue.flush;` bypasses the async worker by rotating the active file and flushing every sealed DB immediately (handy for CI/tests).

## Testing & Tooling
- `./scripts/run-integration-tests.sh` — end-to-end (builds server, runs Go client tests).
- `cargo test` — Rust unit/integration suite.
- `examples/go-adbc/main.go` — quick manual smoke test (`SWANLAKE_PORT=50051 go run main.go`).
- Logs to watch: `"SwanLake Flight SQL server listening..."`, `"session created"`, `"Cleaned up X idle sessions"`.

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
