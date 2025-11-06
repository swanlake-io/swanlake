[![codecov](https://codecov.io/gh/swanlake-io/swanlake/graph/badge.svg)](https://codecov.io/gh/swanlake-io/swanlake)


# SwanLake

SwanLake is a Rust-based Arrow Flight SQL server backed by DuckDB with optional DuckLake extensions. It delivers per-connection sessions, streaming analytics, and a compact deployment footprint.

## Highlights
- Arrow Flight SQL endpoint with prepared statement and streaming result support
- Session-scoped DuckDB connections for predictable state management
- Optional DuckLake extension loading and initialization hooks
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
Key environment variables (all prefixed with `SWANLAKE_`):

| Variable | Description | Default |
| --- | --- | --- |
| `HOST` | Bind address | `127.0.0.1` |
| `PORT` | gRPC port | `4214` |
| `DUCKDB_PATH` | DuckDB database path (blank = in-memory) | _unset_ |
| `MAX_SESSIONS` | Concurrent session limit | `100` |
| `SESSION_TIMEOUT_SECONDS` | Idle session timeout | `1800` |
| `DUCKLAKE_ENABLE` | Auto-load DuckLake extension | `true` |
| `DUCKLAKE_INIT_SQL` | SQL executed after DuckLake loads | _unset_ |
| `LOG_FORMAT` | `compact` or `json` | `compact` |
| `LOG_ANSI` | Enable ANSI colors | `true` |

`.env` files are read automatically via `dotenvy`. Command-line flags always override file-based configuration.

## Testing

### Integration Tests
- `./scripts/test-integration.sh` builds the server and runs the Go ADBC client flow.
- `examples/go-adbc` and `examples/go-sqlx` provide minimal client samples for manual testing.

### SQL Logic Tests
The standalone test runner (`tests/runner`) executes SQL logic tests against a running SwanLake server:

```bash
# Start the server
cargo run

# In another terminal, run SQL tests
cd tests/runner
cargo run -- --endpoint grpc://127.0.0.1:4214 ../../tests/sql/ducklake_basic.test

# Or use the helper script
./scripts/run_ducklake_tests.sh
```

The test runner uses Arrow 56.x for ADBC compatibility, while the main project uses Arrow 57.x.

## Documentation
- `AGENT.md` offers a guided tour of the architecture for contributors.
- `docs/session.md` explains session lifecycle, metadata, and protocol handling.

## License

Licensed under the MIT License. See [LICENSE](LICENSE) for details.
