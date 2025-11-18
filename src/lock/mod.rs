#![allow(dead_code)]
//! Distributed locking module for SwanLake.
//!
//! This module provides distributed locking capabilities that can work across multiple
//! hosts, replacing the file-based locking mechanism. It supports PostgreSQL advisory
//! locks for distributed coordination.

mod postgres;

#[allow(unused_imports)]
pub use postgres::PostgresLock;

use std::path::Path;
use std::time::Duration;

use anyhow::Result;

/// Trait for distributed lock implementations.
pub trait DistributedLock: Send + Sync {
    /// Try to acquire a lock for the given target with a TTL and optional owner.
    ///
    /// Returns `Ok(Some(lock))` if the lock was acquired, `Ok(None)` if the lock
    /// is held by another process, or an error if the operation failed.
    fn try_acquire(
        target: &Path,
        ttl: Duration,
        owner: Option<&str>,
    ) -> impl std::future::Future<Output = Result<Option<Self>>> + Send
    where
        Self: Sized;
}
