swandb - It’s not just a duck — it’s a swan in flight.

I want to build a arrow flight sql server:
https://github.com/apache/arrow-rs/blob/main/arrow-flight/src/sql/server.rs

that powered by duckdb https://duckdb.org/docs/stable/clients/rust and ducklake https://ducklake.select/docs/stable/duckdb/introduction

help me build the very basic version of it.

minimum code, fail fast

dependencies I can think of:

- arrow-flight
- duckdb-rs
- thiserror
- anyhow
- tracing
- dotenv
- serde
- prost
- tokio maybe

examples:

* https://github.com/apache/datafusion/blob/main/datafusion-examples/examples/flight/flight_sql_server.rs
* https://github.com/apache/arrow-rs/blob/main/arrow-flight/examples/flight_sql_server.rs
* https://github.com/datafusion-contrib/datafusion-flight-sql-server/blob/main/datafusion-flight-sql-server/src/service.rs

## Getting started

```bash
cargo run
# override config file
cargo run -- --config path/to/custom.toml
```

The server listens on `127.0.0.1:50051` by default and can be configured through environment variables:

| Variable | Purpose | Default |
| --- | --- | --- |
| `SWANDB_HOST` | Address to bind the Flight SQL server | `127.0.0.1` |
| `SWANDB_PORT` | TCP port for the Flight SQL server | `50051` |
| `SWANDB_DUCKDB_PATH` | Optional path to a DuckDB database file (in-memory when omitted) | _in-memory_ |
| `SWANDB_POOL_SIZE` | Maximum number of pooled DuckDB connections | `4` |
| `SWANDB_ENABLE_DUCKLAKE` | Toggle automatic `ducklake` extension install/load | `true` |
| `SWANDB_DUCKLAKE_INIT_SQL` | Optional SQL executed after the extension loads (e.g. ATTACH commands) | _unset_ |

`.env` files are loaded automatically via `dotenvy`. You can also point the binary at a custom `config.toml` with `--config`; environment variables always take precedence.

## Architecture overview

- `config`: loads runtime configuration from the environment.
- `duckdb`: thin query engine that fans out synchronous DuckDB work into blocking tasks, manages an r2d2-backed DuckDB pool, installs/loads the DuckLake extension, and exposes results as Arrow batches.
- `service`: minimal [`FlightSqlService`](https://docs.rs/arrow-flight/latest/arrow_flight/sql/server/trait.FlightSqlService.html) implementation that handles `CommandStatementQuery` and streams results via Arrow Flight.
- `main`: wires configuration, logging, and the Flight SQL gRPC server together.

The implementation keeps the DuckDB engine simple on purpose—queries are executed directly and the resulting `RecordBatch`es are converted to Flight `FlightData`. DuckLake is handled as a DuckDB extension: we install/load it at startup (unless disabled) and optionally run custom SQL afterwards so we fail fast if attachment fails. Connection management is delegated to an r2d2 pool so concurrent RPCs get cheap cloned handles.
