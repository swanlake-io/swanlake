//! DuckDB engine module - connection management and query execution.
//!
//! This module provides:
//! - `DuckDbConnection`: Wrapper around duckdb::Connection with execution methods
//! - `EnginePool`: Pool for creating initialized connections
//! - `QueryResult`: Query execution results

pub mod batch;
pub mod connection;
mod factory;

pub use connection::{DuckDbConnection, QueryResult};
pub use factory::EnginePool;
