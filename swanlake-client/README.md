# SwanLake Client

A Rust client library and CLI for connecting to Arrow Flight SQL servers, designed for use with [SwanLake](https://github.com/swanlake-io/swanlake).

## Library Usage

Add this to your `Cargo.toml`:

```toml
[dependencies]
swanlake-client = "0.1.1"
```

Connect to a SwanLake server and execute a query:

```rust
use swanlake_client::FlightSQLClient;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Connect to the server
    let mut client = FlightSQLClient::connect("grpc://localhost:4214")?;

    // Execute a query
    let result = client.query("SELECT 1 as col")?;
    println!("Total rows: {}", result.total_rows);

    // Execute an update
    let update_result = client.update("CREATE TABLE test (id INTEGER)")?;
    println!("Rows affected: {:?}", update_result.rows_affected);

    Ok(())
}
```

## CLI Usage

You can use the provided interactive CLI to connect to a SwanLake server.

To build and run the CLI:

```bash
cargo run --bin swanlake-cli --features="cli"
```

By default, it will attempt to connect to `grpc://127.0.0.1:4214`. You can specify a different endpoint:

```bash
cargo run --bin swanlake-cli -- --endpoint grpc://remote-host:4214
```

## License

Licensed under the MIT License.
