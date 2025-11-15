# Distributed Lock Module

This module provides distributed locking capabilities for SwanLake that can coordinate across multiple hosts, replacing the file-based locking mechanism.

## Overview

The distributed lock module supports PostgreSQL advisory locks for distributed coordination. Unlike file-based locks which only work on a single filesystem, these locks can coordinate access across multiple hosts as long as they share access to the same PostgreSQL database.

## PostgreSQL Advisory Locks

PostgreSQL advisory locks are a built-in feature that provides application-level locking using the database. They are:

- **Lightweight**: No tables or rows need to be created
- **Session-based**: Automatically released when the database session ends
- **Distributed**: Work across multiple hosts connecting to the same database
- **Fast**: Minimal overhead compared to row-level locks

### How It Works

1. Each resource (e.g., a file path) is mapped to a unique 64-bit integer lock key by hashing
2. When trying to acquire a lock, we call `pg_try_advisory_lock(key)` which returns true if the lock was acquired
3. The lock is held for the lifetime of the database session
4. When the lock object is dropped, we call `pg_advisory_unlock(key)` to release it
5. If a session crashes, PostgreSQL automatically releases all advisory locks held by that session

## Usage Example

```rust
use swanlake::lock::{PostgresLock, DistributedLock};
use std::path::Path;
use std::time::Duration;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let target = Path::new("/tmp/my-resource.db");
    let ttl = Duration::from_secs(600);
    
    // Try to acquire the lock
    if let Some(lock) = PostgresLock::try_acquire(target, ttl, Some("worker-1")).await? {
        println!("Lock acquired!");
        
        // Do work while holding the lock
        // ...
        
        // Lock is automatically released when dropped
    } else {
        println!("Lock is held by another process");
    }
    
    Ok(())
}
```

## Configuration

The PostgreSQL connection is configured via environment variables:

### Option 1: Full Connection String
```bash
export SWANLAKE_LOCK_POSTGRES_CONNECTION="host=localhost port=5432 user=postgres dbname=swanlake password=secret"
```

### Option 2: Individual Environment Variables
```bash
export PGHOST=localhost
export PGPORT=5432
export PGUSER=postgres
export PGDATABASE=swanlake
export PGPASSWORD=secret
```

The module will use `SWANLAKE_LOCK_POSTGRES_CONNECTION` if set, otherwise fall back to `PGCONNECTION`, and finally construct a connection string from the individual `PG*` environment variables.

## Comparison with File-Based Locks

| Feature | File Lock | PostgreSQL Lock |
|---------|-----------|-----------------|
| **Multi-host** | ❌ Only works on shared filesystem | ✅ Works across any hosts with DB access |
| **Crash recovery** | ⚠️ Requires TTL + stale detection | ✅ Automatic via session termination |
| **Performance** | Fast (local filesystem) | Fast (single DB roundtrip) |
| **Dependencies** | None | PostgreSQL database |
| **Complexity** | Simple | Moderate |

## Architecture

```
┌─────────────────┐
│   SwanLake      │
│   Process 1     │
│                 │
│  ┌──────────┐   │
│  │PostgresLock│  │────┐
│  └──────────┘   │    │
└─────────────────┘    │
                       │
                       ▼
                  ┌─────────────┐
                  │ PostgreSQL  │
                  │  Advisory   │
                  │   Locks     │
                  └─────────────┘
                       ▲
                       │
┌─────────────────┐    │
│   SwanLake      │    │
│   Process 2     │    │
│                 │    │
│  ┌──────────┐   │    │
│  │PostgresLock│  │────┘
│  └──────────┘   │
└─────────────────┘
```

## Trait-Based Design

The module uses a `DistributedLock` trait to allow for future implementations:

```rust
pub trait DistributedLock: Send + Sync {
    async fn try_acquire(
        target: &Path,
        ttl: Duration,
        owner: Option<&str>,
    ) -> Result<Option<Self>> where Self: Sized;
    
    async fn refresh(&self) -> Result<()>;
}
```

This design allows for:
- Future implementations (Redis, etcd, etc.)
- Testing with mock implementations
- Easy migration between lock backends

## Future Enhancements

Possible future additions to this module:

1. **Additional Backends**: Redis (Redlock), etcd, Consul
2. **Lock Pooling**: Connection pooling for better performance
3. **Observability**: Metrics for lock acquisition times and contention
4. **Lock Renewal**: Automatic background refresh for long-running operations
5. **Fair Queuing**: FIFO ordering for lock acquisition
