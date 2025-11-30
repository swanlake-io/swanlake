//! Shared Flight SQL client utilities for SwanLake examples and tests.
//!
//! This crate wraps the dynamic Flight SQL driver loading logic so that
//! consumers do not need to duplicate the boilerplate required to obtain
//! `ManagedConnection` instances or to work with Arrow record batches.
//!
//! ## Example
//!
//! ```rust,ignore
//! use flight_sql_client::FlightSQLClient;
//!
//! let mut client = FlightSQLClient::connect("grpc://localhost:4214")?;
//! let result = client.execute("SELECT 1")?;
//! println!("Rows: {}", result.total_rows);
//! # Ok::<(), anyhow::Error>(())
//! ```

pub mod client;

/// A Flight SQL client for connecting to SwanLake servers.
///
/// See [`FlightSQLClient`] for details.
pub use client::FlightSQLClient;

/// Result of executing a query.
///
/// See [`QueryResult`] for details.
pub use client::QueryResult;

/// Result of executing an update/DDL statement.
///
/// See [`UpdateResult`] for details.
pub use client::UpdateResult;
