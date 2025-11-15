//! Example demonstrating distributed lock usage with PostgreSQL advisory locks.
//!
//! This is a documentation example showing how to use SwanLake's distributed lock module.
//! Since SwanLake is a binary crate, this example demonstrates the usage pattern
//! for the distributed lock module.
//!
//! ## Setup
//!
//! 1. Ensure PostgreSQL is running and accessible
//! 2. Set environment variables:
//!    ```
//!    export PGHOST=localhost
//!    export PGPORT=5432
//!    export PGUSER=postgres
//!    export PGPASSWORD=postgres
//!    export PGDATABASE=postgres
//!    ```
//!
//! ## Usage Pattern
//!
//! The distributed lock module provides a `PostgresLock` type that implements
//! the `DistributedLock` trait for coordinating access across multiple hosts.
//!
//! ```rust,no_run
//! use std::path::Path;
//! use std::time::Duration;
//! // In actual usage within swanlake:
//! // use crate::lock::{DistributedLock, PostgresLock};
//!
//! async fn example() -> anyhow::Result<()> {
//!     let target = Path::new("/tmp/swanlake-resource.db");
//!     let ttl = Duration::from_secs(600);
//!     let worker_id = "worker-1";
//!
//!     // Try to acquire the lock
//!     // if let Some(lock) = PostgresLock::try_acquire(target, ttl, Some(worker_id)).await? {
//!     //     println!("Lock acquired!");
//!     //     
//!     //     // Do work while holding the lock
//!     //     // ...
//!     //     
//!     //     // Optionally refresh to keep the connection alive
//!     //     lock.refresh().await?;
//!     //     
//!     //     // Lock is automatically released when dropped
//!     //     drop(lock);
//!     //     println!("Lock released");
//!     // } else {
//!     //     println!("Lock is held by another process");
//!     // }
//!
//!     Ok(())
//! }
//! ```
//!
//! ## Key Features
//!
//! - **Distributed**: Works across multiple hosts via PostgreSQL
//! - **Automatic cleanup**: Locks released on drop or session termination
//! - **Simple API**: Similar to file-based locks but distributed
//! - **No tables needed**: Uses PostgreSQL advisory locks (built-in feature)
//!
//! ## Configuration
//!
//! The lock uses PostgreSQL connection info from environment variables:
//! - `SWANLAKE_LOCK_POSTGRES_CONNECTION` (full connection string)
//! - Or individual: `PGHOST`, `PGPORT`, `PGUSER`, `PGPASSWORD`, `PGDATABASE`
//!
//! See `src/lock/README.md` for more details.

fn main() {
    println!("This is a documentation example. See the source code and src/lock/README.md for usage details.");
}
