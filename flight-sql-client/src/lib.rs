//! Shared Flight SQL client utilities for SwanLake examples and tests.
//!
//! This crate wraps the dynamic Flight SQL driver loading logic so that
//! consumers do not need to duplicate the boilerplate required to obtain
//! `ManagedConnection` instances or to work with Arrow record batches.

pub mod arrow;
pub mod client;
pub mod connection;

pub use client::{FlightSQLClient, QueryResult, UpdateResult};
pub use connection::{connect, FlightSqlConnectionBuilder};
