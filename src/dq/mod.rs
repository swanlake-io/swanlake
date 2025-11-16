//! Duckling Queue module - staging local DuckDB files before flushing to DuckLake.
//!
//! This module provides:
//! - `config`: configuration and directory management
//! - `manager`: global queue orchestration
//! - `session_queue`: per-session queue handles
//! - `runtime`: background tasks for rotation + flushing

pub mod config;
pub mod manager;
pub mod runtime;
pub mod session;

pub use manager::QueueManager;
pub use runtime::QueueRuntime;
pub use session::QueueSession;
