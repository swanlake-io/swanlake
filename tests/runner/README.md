# SwanLake Test Runner

Rust-based runner that drives the SwanLake Flight SQL endpoint and our Go-in-the-loop scenarios.

## What it Runs
- **SQL scripts** in `tests/sql/*.test`: simple format (`statement ok/error` and `query <types|error>`). See `tests/sql/README.md` for details.
- **Scenarios** in `tests/runner/src/scenarios`: Rust integration checks for duckling_queue, appender, prepared statements, etc.

## Usage
```bash
# from repo root (server should be running on the endpoint)
cargo run --manifest-path tests/runner/Cargo.toml -- \
  --endpoint grpc://127.0.0.1:4214 \
  --test-dir target/ducklake-tests
```

Flags:
- `--endpoint` (default `grpc://127.0.0.1:4214`)
- `--test-dir` (required for SQL scripts; used for `__TEST_DIR__` substitution)
- `--var KEY=VALUE` to add extra substitutions in SQL scripts
- positional args: optional list of `.test` files; if omitted, all `tests/sql/*.test` are run

## Notes
- The runner talks to the live server via `flight_sql_client` (ADBC Flight SQL). It normalizes query outputs to tab-delimited strings to match expected rows.
- Arrow/ADBC versions here are kept in sync with the main repo (no separate sqllogictest harness).
