//! DuckDB engine module - connection management and query execution.
//!
//! This module provides:
//! - `DuckDbConnection`: Wrapper around duckdb::Connection with execution methods
//! - `EngineFactory`: Factory for creating initialized connections
//! - `QueryResult`: Query execution results

pub(crate) mod batch;
pub mod connection;
mod factory;

pub use connection::{DuckDbConnection, QueryResult};
pub use factory::EngineFactory;
