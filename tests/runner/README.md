# SwanLake Test Runner

Standalone SQL logic test runner for SwanLake using Arrow Flight SQL and ADBC.

## Overview

This is an independent Rust project that runs SQL logic tests against a SwanLake server. It uses Arrow 56.x to maintain compatibility with ADBC drivers, while the main SwanLake project uses Arrow 57.x.

## Dependencies

- **Arrow 56.x** - Compatible with ADBC 0.20.0
- **ADBC** - Arrow Database Connectivity for connecting to Flight SQL servers
- **sqllogictest** - SQL logic test framework

## Building

```bash
cd tests/runner
cargo build --release
```

## Running Tests

Start the SwanLake server first, then run:

```bash
cargo run -- \
  --endpoint grpc://127.0.0.1:4214 \
  --test-dir ../../tests/sqllogictest
```

### Command Line Options

- `--endpoint` - SwanLake Flight SQL endpoint (default: `grpc://127.0.0.1:4214`)
- `--test-files` - Specific test files to run (optional)
- `--labels` - Test labels to filter (optional)
- `--vars` - Variable substitutions in format `key=value` (optional)
- `--test-dir` - Directory containing SQL logic test files (default: `../../tests/sqllogictest`)

## Example

```bash
# Run all tests
cargo run

# Run specific test file
cargo run -- --test-files basic_queries.slt

# Run with substitutions
cargo run -- --vars "table_name=my_table"
```

## Architecture

The runner connects to SwanLake via ADBC's Flight SQL driver and executes SQL logic tests, comparing actual results against expected outputs defined in `.slt` files.
