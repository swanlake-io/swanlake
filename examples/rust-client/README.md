# SwanLake Rust Client

A sample CLI tool for connecting to SwanLake Flight SQL servers, built on top of the reusable `flight-sql-client` crate placed in the repository root.

## Overview

This package provides:

1. **`flight-sql-client` crate** *(at the repository root)* - Shared Flight SQL client utilities
2. **`swanlake-cli` binary** - An interactive SQL client demonstrating the shared crate

## Installation

### Build from source

```bash
cd examples/rust-client
cargo build --release
```

The CLI binary will be available at `target/release/swanlake-cli`.

## Library Usage

To use the shared client primitives directly, depend on the root-level crate:

```toml
[dependencies]
flight-sql-client = { path = "../../flight-sql-client" }
```

### Basic Example

```rust
use flight_sql_client::FlightSQLClient;
use anyhow::Result;

fn main() -> Result<()> {
    // Connect to SwanLake server
    let mut client = FlightSQLClient::connect("grpc://localhost:4214")?;

    // Create a table
    client.execute_update("CREATE TABLE users (id INT, name VARCHAR)")?;

    // Insert data
    client.execute_update("INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')")?;

    // Query data
    let result = client.execute("SELECT * FROM users")?;
    println!("Found {} rows", result.total_rows);

    // Query with parameters
    let result = client.execute_with_params(
        "SELECT * FROM users WHERE name = ?",
        vec!["Alice".to_string()]
    )?;

    Ok(())
}
```

### API Reference

#### `FlightSQLClient`

**Methods:**

- `connect(endpoint: &str) -> Result<Self>` - Connect to a SwanLake server
- `execute(sql: &str) -> Result<QueryResult>` - Execute a SELECT query
- `execute_update(sql: &str) -> Result<UpdateResult>` - Execute DDL/DML statements
- `execute_with_params(sql: &str, params: Vec<String>) -> Result<QueryResult>` - Execute parameterized query
- `execute_batch_update(sql: &str, batch: RecordBatch) -> Result<UpdateResult>` - Batch insert with Arrow RecordBatch

#### `QueryResult`

Contains the results of a query:

- `batches: Vec<RecordBatch>` - Arrow record batches
- `total_rows: usize` - Total number of rows
- `schema() -> Option<Arc<Schema>>` - Get the result schema
- `is_empty() -> bool` - Check if result is empty

## CLI Usage

### Interactive Mode

Start an interactive SQL shell:

```bash
# Connect to default endpoint (localhost:4214)
./target/release/swanlake-cli

# Connect to custom endpoint
./target/release/swanlake-cli --endpoint grpc://remote-host:4214
```

Example session:

```
🦢 Connecting to SwanLake at grpc://localhost:4214...
✅ Connected successfully!

SwanLake Interactive SQL Shell
Type your SQL queries and press Enter. Type 'exit' or 'quit' to exit.
Press Ctrl-C twice to exit.

swanlake> CREATE TABLE test (id INT, name VARCHAR);
✓ 0 rows affected
⏱️  0.045s

swanlake> INSERT INTO test VALUES (1, 'Alice'), (2, 'Bob');
✓ 2 rows affected
⏱️  0.023s

swanlake> SELECT * FROM test;
╭────┬───────╮
│ id │ name  │
├────┼───────┤
│ 1  │ Alice │
│ 2  │ Bob   │
╰────┴───────╯
📊 2 rows in 0.015s

swanlake> exit
Goodbye! 👋
```

### Non-Interactive Mode

Execute a single query:

```bash
./target/release/swanlake-cli --query "SELECT * FROM my_table"
```

### Options

- `-e, --endpoint <ENDPOINT>` - Server endpoint (default: `grpc://localhost:4214`)
- `-q, --query <QUERY>` - Execute a single query and exit
- `-d, --debug` - Enable debug logging

### Interactive Controls

- Type `exit` or `quit` to exit
- Press **Ctrl-C twice** to exit (first Ctrl-C shows a reminder)
- Press **Ctrl-D** (EOF) to exit

### History

The CLI automatically saves command history to `~/.swanlake_history`.

## Features

- ✅ Simple, ergonomic API
- ✅ Prepared statements with parameter binding
- ✅ Batch inserts with Arrow RecordBatch
- ✅ Interactive SQL shell with readline support
- ✅ Beautiful table output with UTF-8 borders
- ✅ Command history
- ✅ Automatic query/update detection
- ✅ Comprehensive error handling

## Requirements

- Rust 1.70+
- SwanLake server running (see main README)
- ADBC Flight SQL driver (automatically downloaded)

## License

Same as SwanLake parent project.
