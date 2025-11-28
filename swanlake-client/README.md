# SwanLake Client

A Rust client library and CLI for connecting to Arrow Flight SQL servers, designed for use with [SwanLake](https://github.com/swanlake-io/swanlake).

## Add to your project

Add this to your `Cargo.toml`:

```toml
[dependencies]
swanlake-client = "0.1.1"
```

## API overview

- `connect(endpoint) -> FlightSQLClient`: open a connection to a Flight SQL endpoint.
- `query(sql) -> QueryResult`: run a read-only statement. `execute(sql)` is an alias.
- `update(sql) -> UpdateResult`: run INSERT/UPDATE/DELETE/DDL.
- `query_with_params(sql, params)`, `update_with_params(sql, params)`: prepared statements for both reads and writes (pass `None` or an empty vec for simple statements).
- `begin_transaction()`, `commit()`, `rollback()`: simple transaction helpers.
- Scalar helper methods were removed; use the general result types.

`QueryResult` holds the Arrow batches and row count. `UpdateResult` holds `rows_affected` when the server returns it.

### Query data

```rust
use swanlake_client::FlightSQLClient;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = FlightSQLClient::connect("grpc://localhost:4214")?;

    // Simple query; execute() is an alias of query()
    let result = client.query("SELECT 1 as col")?;
    println!("Total rows: {}", result.total_rows);

    Ok(())
}
```

### Parameterized queries

```rust
use swanlake_client::FlightSQLClient;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = FlightSQLClient::connect("grpc://localhost:4214")?;
    let params = vec!["alice".to_string()];
    let result = client.query_with_params(
        "SELECT * FROM users WHERE name = ?",
        Some(params),
    )?;
    println!("Rows: {}", result.total_rows);
    Ok(())
}
```

### Updates

```rust
use swanlake_client::FlightSQLClient;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = FlightSQLClient::connect("grpc://localhost:4214")?;
    let affected = client.update("CREATE TABLE t(id INT)")?;
    println!("Rows affected: {:?}", affected.rows_affected);
    Ok(())
}
```

### Transactions

```rust
use swanlake_client::FlightSQLClient;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = FlightSQLClient::connect("grpc://localhost:4214")?;

    client.begin_transaction()?;
    client.update("INSERT INTO t VALUES (1)")?;
    client.update("INSERT INTO t VALUES (2)")?;
    client.commit()?;

    Ok(())
}
```

## CLI usage

You can use the provided interactive CLI to connect to a SwanLake server.

To build and run the CLI:

```bash
cargo run --bin swanlake-cli --features="cli"
```

By default, it will attempt to connect to `grpc://127.0.0.1:4214`. You can specify a different endpoint:

```bash
cargo run --bin swanlake-cli -- --endpoint grpc://remote-host:4214
```

## Notes

- The CLI prints Arrow batches via `arrow-cast`; the previous `src/arrow.rs` helpers have been removed.
- The API surface aims to stay small and general; prefer `query`/`update` over type-specific helpers.

## License

Licensed under the MIT License.
