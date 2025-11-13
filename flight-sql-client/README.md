# flight-sql-client

A Rust client library for connecting to Arrow Flight SQL servers, built on top of ADBC drivers. Designed for use with [SwanLake](https://github.com/swanlake-io/swanlake), an Arrow Flight SQL server backed by DuckDB.

## Installation

Add this to your `Cargo.toml`:

```toml
[dependencies]
flight-sql-client = "0.1.0"
```

## Usage

Connect to a SwanLake server and execute a query:

```rust
use flight_sql_client::FlightSQLClient;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Connect to the server
    let mut client = FlightSQLClient::connect("grpc://localhost:4214")?;

    // Execute a query
    let result = client.execute("SELECT 1 as col")?;
    println!("Total rows: {}", result.total_rows);

    // Execute an update
    let update_result = client.execute_update("CREATE TABLE test (id INTEGER)")?;
    println!("Rows affected: {:?}", update_result.rows_affected);

    Ok(())
}
```

## API Overview

- `FlightSQLClient`: Main client for executing queries and updates.
- `QueryResult`: Holds query results with record batches.
- `UpdateResult`: Holds update results with affected rows.
- `FlightSqlConnectionBuilder`: Builder for custom connections.

See the [SwanLake repository](https://github.com/swanlake-io/swanlake) for more examples and server setup.

## License

Licensed under the MIT License.