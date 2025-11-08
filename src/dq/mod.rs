//! Duckling Queue module - staging local DuckDB files before flushing to DuckLake.
//!
//! This module provides:
//! - `manager`: attach/rotation helpers
//! - `runtime`: background tasks for rotation + flushing
//! - `lock`: cross-host file locking

pub mod lock;
pub mod manager;
pub mod runtime;

pub use manager::DucklingQueueManager;
pub use runtime::DucklingQueueRuntime;
