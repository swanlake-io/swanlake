# SwanLake Core

This crate contains the core logic for the SwanLake Arrow Flight SQL server. It implements the main services, session management, query engine, and other foundational components.

The primary components include:

- **`service`**: Implements the Arrow Flight SQL gRPC service.
- **`session`**: Manages client sessions, including state for prepared statements and transactions.
- **`engine`**: Provides the DuckDB query execution engine.
- **`dq`**: Implements the Duckling Queue for resilient data ingestion.
- **`config`**: Handles server configuration.

This crate is used by `swanlake-server` to build the main server binary.
