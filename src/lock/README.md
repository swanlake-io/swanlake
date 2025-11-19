# Distributed Lock Module

Provides PostgreSQL-based distributed locking for coordinating access across multiple SwanLake hosts using PostgreSQL advisory locks.

> **Note:** The lock subsystem is experimental and gated behind the `distributed-locks` Cargo
> feature. Enable it with `cargo build --features distributed-locks` once you are ready to
> integrate it; it is disabled by default to keep unused code out of coverage reports.

## Overview

- Locks reuse a single shared PostgreSQL session per SwanLake process, so acquiring a lock does **not** create a new long-lived database connection.
- PostgreSQL releases all locks automatically if the process exits or the connection drops.
- Advisory locks are scoped to 64-bit integers derived from the resource path, so the lock state survives SwanLake restarts without extra metadata files.

## Features

- **Distributed coordination** across hosts via PostgreSQL advisory locks
- **Automatic cleanup** on process exit or crash (PostgreSQL releases locks)
- **No per-lock connections:** one PostgreSQL session is shared for all locks
- **Configurable TLS** via `PGSSLMODE`
- **RAII unlocking** – locks release automatically when the guard is dropped

## Configuration

Standard PostgreSQL environment variables are honored:

```bash
export PGHOST=localhost
export PGPORT=5432
export PGUSER=postgres
export PGDATABASE=swanlake
export PGPASSWORD=secret
# PGSSLMODE: disable | require | verify-ca | verify-full
```

`PGSSLMODE` controls TLS behavior:

- `disable`: plaintext
- `require`: TLS without certificate/hostname verification
- `verify-ca`: TLS verifying the CA but allowing hostname mismatches
- `verify-full`: TLS with full certificate + hostname verification

## Usage

```rust
use crate::lock::{DistributedLock, PostgresLock};

if let Some(_lock) = PostgresLock::try_acquire(&path, ttl, owner).await? {
    // Critical section while holding the advisory lock
    // Lock is released automatically when `_lock` is dropped.
}
```

`ttl` is accepted to satisfy the `DistributedLock` trait, but PostgreSQL handles leases automatically so the value is ignored.

## Implementation

- Resource paths are hashed with `SipHasher13` seeded with zeros to produce deterministic 64-bit lock keys.
- A single `tokio_postgres::Client` is created lazily and reused for every lock acquisition and release.
- TLS connectors are built based on `PGSSLMODE` and cached for the lifetime of the process.
- Releasing a lock happens asynchronously on drop via `pg_advisory_unlock`.

## Lock Key Generation

```rust
fn path_to_lock_key(path: &Path) -> i64 {
    let mut hasher = SipHasher13::new_with_key(&[0u8; 16]);
    path.hash(&mut hasher);
    hasher.finish() as i64
}
```
