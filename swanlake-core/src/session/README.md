# Session Architecture

SwanLake keeps all database state inside **connection-scoped sessions**. Each gRPC connection owns a DuckDB connection, so prepared statements, transactions, temporary tables, and connection settings persist for the lifetime of that connection.

## Lifecycle

- Session ID comes from `Request::remote_addr()` (e.g., `ip:port`); if unavailable, a random UUID is used.
- On first use, the `SessionRegistry` creates a DuckDB connection via `EngineFactory`, installs DuckLake/HTTPFS/AWS/Postgres, and runs any configured `ducklake_init_sql`.
- Subsequent requests on the same connection reuse the existing session.
- Idle sessions are removed when `idle_duration() > session_timeout_seconds` (default 900s / 15 min). The janitor runs every 5 minutes.
- The registry enforces `max_sessions` (default 100). Errors surface as `ServerError::MaxSessionsReached`.

## Session State

`Session` (see `swanlake-core/src/session/session.rs`) holds:
- DuckDB connection with loaded extensions and optional init SQL.
- Prepared statement map keyed by `StatementHandle` (u64 wrapper carried in tickets).
- Transaction map keyed by `TransactionId`.
- Last-activity timestamp used by cleanup.

## Registry + Service Integration

- `SessionRegistry::get_or_create_by_id` returns or creates the session for the provided ID and applies limits/timeouts configured via environment variables (see `CONFIGURATION.md`).
- The periodic cleanup in `swanlake-server/src/main.rs` calls `cleanup_idle_sessions()` every 300s.
- `SwanFlightSqlService::prepare_request` (in `swanlake-core/src/service/mod.rs`) extracts the session ID, records it on the tracing span, and rehydrates the session before handing off to handlers.

## Troubleshooting

- **State not persisting**: ensure the client keeps a single gRPC connection alive; some HTTP/gRPC gateways open a new connection per request.
- **Max sessions reached**: raise `SWANLAKE_MAX_SESSIONS` or lower `SWANLAKE_SESSION_TIMEOUT_SECONDS`; verify clients close idle connections.
- **Port reuse edge cases**: if a client reconnects with the same `ip:port` before timeout expires, the old session is reused. Lower the timeout if this is undesirable.

## References

- Architecture entrypoints: `swanlake-core/src/session/registry.rs`, `swanlake-core/src/session/session.rs`, `swanlake-core/src/service/mod.rs`
- Configuration defaults: `CONFIGURATION.md`
- Flight SQL protocol: https://arrow.apache.org/docs/format/FlightSql.html
