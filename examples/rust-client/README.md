# SwanLake Rust Client

A simple CLI tool for connecting to SwanLake Flight SQL servers.

## Run

Make sure you have a SwanLake server running locally or remotely.

From the repository root:

```bash
cargo run-cli

# or 
cargo run --manifest-path examples/rust-client/Cargo.toml -- --endpoint grpc://127.0.0.1:4214
```
