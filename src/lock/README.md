# Distributed Lock Module

Provides PostgreSQL-based distributed locking for coordinating access across multiple SwanLake hosts using PostgreSQL advisory locks.

## Overview

Replaces file-based locks with PostgreSQL advisory locks that work across multiple hosts sharing a PostgreSQL database. Uses DuckDB's postgres extension to connect, avoiding additional dependencies.

## Features

- **Distributed coordination** across multiple hosts via PostgreSQL
- **Automatic cleanup** on session termination or crash
- **Session-based locks** using `pg_try_advisory_lock()` / `pg_advisory_unlock()`
- **Zero additional dependencies** - uses DuckDB's built-in postgres extension

## Configuration

Uses standard PostgreSQL environment variables:

```bash
export PGHOST=localhost
export PGPORT=5432
export PGUSER=postgres
export PGDATABASE=swanlake
export PGPASSWORD=secret
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
- Each lock creates a fresh DuckDB connection that attaches to PostgreSQL

## Lock Key Generation

```rust
fn path_to_lock_key(path: &Path) -> i64 {
    // Hash path to deterministic 64-bit integer
    let mut hasher = DefaultHasher::new();
    path.hash(&mut hasher);
    hasher.finish() as i64
}
```
