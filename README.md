[![codecov](https://codecov.io/gh/swanlake-io/swanlake/graph/badge.svg)](https://codecov.io/gh/swanlake-io/swanlake)

# SwanLake

SwanLake is a Rust-based Arrow Flight SQL server backed by DuckDB with optional DuckLake extensions. It delivers per-connection sessions, streaming analytics, and a compact deployment footprint.

## Highlights
- Arrow Flight SQL endpoint with prepared statement and streaming result support
- Session-scoped DuckDB connections for predictable state management
- Optional DuckLake extension loading and initialization hooks
- Duckling Queue staging layer with `PRAGMA duckling_queue.flush` for crash-resilient write buffering
- Structured logging and configurability via environment variables or `config.toml`

## Quick Start
```bash
# (optional) Download the prebuilt DuckDB libraries and export env vars
scripts/setup_duckdb.sh
source .duckdb/env.sh

# Run SwanLake
cargo run
```

## Configuration
All configuration options (env vars + defaults) live in [Configuration.md](Configuration.md).
Highlights:
- `SWANLAKE_HOST` / `SWANLAKE_PORT` control the Flight endpoint bind address.
- `SWANLAKE_ENABLE_WRITES`, `SWANLAKE_MAX_SESSIONS`, and `SWANLAKE_SESSION_TIMEOUT_SECONDS`
  gate write access and session lifecycle.
- `SWANLAKE_DUCKLAKE_INIT_SQL` runs custom SQL immediately after DuckDB starts.
- Duckling Queue settings (root path, rotation/flush thresholds, target schema) are also covered there.

`.env` files are read automatically via `dotenvy`. Command-line flags always override file-based configuration.

Every session can create or insert into `duckling_queue.*` tables. Use
`PRAGMA duckling_queue.flush;` to force the active file to rotate and flush immediately (handy for tests and CI).

## Testing

### Integration Tests
- `./scripts/test-integration.sh` builds the server and runs the Go ADBC client flow.
- `examples/go-adbc` and `examples/go-sqlx` provide minimal client samples for manual testing.

### SQL Logic Tests
The standalone test runner (`tests/runner`) executes SQL logic tests against a running SwanLake server:

```bash
bash ./scripts/run-integration-tests.sh
```

## License

Licensed under the MIT License. See [LICENSE](LICENSE) for details.
