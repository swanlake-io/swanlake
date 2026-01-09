//! Shared Flight SQL client utilities for SwanLake examples and tests.
//!
//! This crate wraps the dynamic Flight SQL driver loading logic so that
//! consumers do not need to duplicate the boilerplate required to obtain
//! `ManagedConnection` instances or to work with Arrow record batches.
//!
//! ## Example
//!
//! ```rust,ignore
//! use swanlake_client::FlightSQLClient;
//!
//! let mut client = FlightSQLClient::connect("grpc://localhost:4214")?;
//! let result = client.execute("SELECT 1")?;
//! println!("Rows: {}", result.total_rows);
//! # Ok::<(), anyhow::Error>(())
//! ```

mod driver;
mod internal;
mod pool_shared;
pub mod client;
pub mod pool;

#[cfg(feature = "tokio")]
pub mod async_pool;

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

/// Connection pool for SwanLake Flight SQL connections.
pub use pool::FlightSQLPool;

/// Pool configuration options.
pub use pool::PoolConfig;

/// RAII pooled connection handle.
pub use pool::PooledConnection;

/// Session handle for stateful workflows.
pub use pool::SessionHandle;

/// Prepared statement handle tied to a session.
pub use pool::PreparedQuery;

/// Options for pooled queries/updates.
pub use pool::QueryOptions;

#[cfg(feature = "tokio")]
/// Async connection pool for SwanLake Flight SQL connections.
pub use async_pool::AsyncFlightSQLPool;

#[cfg(feature = "tokio")]
/// Async session handle for stateful workflows.
pub use async_pool::AsyncSessionHandle;
