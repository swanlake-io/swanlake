//! Duckling Queue module - staging local DuckDB files before flushing to DuckLake.
//!
//! This module provides:
//! - `settings`: configuration and directory management
//! - `manager`: per-session queue file management
//! - `runtime`: background tasks for rotation + flushing
//! - `lock`: cross-host file locking

pub mod lock;
pub mod manager;
pub mod runtime;
pub mod settings;

pub use manager::SessionQueue;
pub use runtime::DucklingQueueRuntime;
pub use settings::DucklingQueueSettings as DucklingQueueManager;
