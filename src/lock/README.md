# Distributed Lock Module

Provides PostgreSQL-based distributed locking for coordinating access across multiple SwanLake hosts using PostgreSQL advisory locks.

## Overview

Replaces file-based locks with PostgreSQL advisory locks that work across multiple hosts sharing a PostgreSQL database. Uses `tokio-postgres` for lightweight connection management.

## Features

- **Distributed coordination** across multiple hosts via PostgreSQL
- **Automatic cleanup** on session termination or crash
- **Session-based locks** using `pg_try_advisory_lock()` / `pg_advisory_unlock()`
- **Lightweight connections** via `tokio-postgres` (each lock creates its own connection)
- **Optimized configuration** built once from environment variables and reused

## Configuration

Uses standard PostgreSQL environment variables (configuration is built once at startup):

```bash
export PGHOST=localhost
export PGPORT=5432
export PGUSER=postgres
export PGDATABASE=swanlake
export PGPASSWORD=secret
export PGSSLMODE=disable  # Options: disable, require, verify-ca, verify-full
```

## Usage

```rust
use crate::lock::{DistributedLock, PostgresLock};

if let Some(lock) = PostgresLock::try_acquire(&path, ttl, owner).await? {
    // Work while holding lock
    lock.refresh().await?;
    // Lock auto-released on drop
}
```

## Implementation

- Resource paths are hashed to 64-bit lock keys
- Locks are held for the database session lifetime
- PostgreSQL automatically releases locks if the session crashes
- Each lock creates a fresh tokio-postgres connection for lightweight operation
- Connection configuration is built once from environment variables using `OnceLock`
- Connection is maintained only while lock is held

## Lock Key Generation

```rust
fn path_to_lock_key(path: &Path) -> i64 {
    // Hash path to deterministic 64-bit integer
    let mut hasher = DefaultHasher::new();
    path.hash(&mut hasher);
    hasher.finish() as i64
}
```
