[![codecov](https://codecov.io/gh/swanlake-io/swanlake/graph/badge.svg)](https://codecov.io/gh/swanlake-io/swanlake)

# SwanLake

SwanLake is an Arrow Flight SQL server backed by DuckDB, enabling fast data analytics and ingestion with datalake support.

<!--![SwanLake](swanlake.jpeg)-->
<img src="swanlake.jpeg" width="512" alt="SwanLake">

## Architecture

- **Arrow Flight SQL Server**: High-performance SQL interface over gRPC for efficient querying.
- **Duckling Queue**: Staging layer for fast insertion of small files, ensuring crash-resilient buffering.
- **DuckLake**: Extensions for datalake integrations, supporting scalable storage solutions.

## Use Cases

- Building datalakes on Postgres and S3 for unified data access.
- Rapid ingestion of logs, metrics, and streaming data.
- High-speed querying using DuckDB and Arrow for analytics.

## Client Libraries and Examples

SwanLake can be accessed using standard Arrow Flight SQL clients or our custom Rust client library.

### Rust Client

The `swanlake-client` library provides an ergonomic API for connecting to SwanLake servers:

```bash
cd examples/rust-client
cargo build --release
```

**Interactive CLI:**
```bash
./target/release/swanlake-cli --endpoint grpc://localhost:4214
```

**Library Usage:**
```rust
use swanlake_client::FlightSQLClient;

let mut client = FlightSQLClient::connect("grpc://localhost:4214")?;
client.execute_update("CREATE TABLE test (id INT, name VARCHAR)")?;
let result = client.execute("SELECT * FROM test")?;
```

See [examples/rust-client/README.md](examples/rust-client/README.md) for full documentation.

### Other Examples

- **Go + ADBC**: [examples/go-adbc/](examples/go-adbc/)
- **Go + sqlx**: [examples/go-sqlx/](examples/go-sqlx/)
- **Rust + ADBC**: [examples/rust-adbc/](examples/rust-adbc/)

## Deployment

SwanLake supports serverless deployment via Docker. Pull the latest image from [GitHub Container Registry](https://github.com/swanlake-io/swanlake/pkgs/container/swanlake).

Customize with environment variables; see [CONFIGURATION.md](CONFIGURATION.md) for details.



## License

Licensed under the MIT License. See [LICENSE](LICENSE) for details.
